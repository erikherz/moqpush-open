# Custom MoQT Player — Technical Deep Dive

A vanilla JavaScript MoQ Transport client that connects to Cloudflare's MoQ relay via WebTransport, implements the draft-14 wire protocol, and feeds CMAF fragments directly into MSE SourceBuffers for sub-second live latency.

## Architecture

```
┌─────────────┐    WebTransport/QUIC    ┌───────────────────┐
│  Publisher   │ ─────────────────────▶ │  Cloudflare Relay  │
│ (moqpush-app)│                        │ (mediaoverquic.com)│
└─────────────┘                        └────────┬──────────┘
                                                │
                                   WebTransport │ (H3/QUIC)
                                                │
                                       ┌────────▼──────────┐
                                       │  Custom MoQT      │
                                       │  Player (browser)  │
                                       │                    │
                                       │  moqt-player.js    │
                                       │  fragment-appender  │
                                       │  MSE SourceBuffers │
                                       └───────────────────┘
```

Two source files, ~1000 lines total, zero dependencies:

| File | Lines | Role |
|------|-------|------|
| `moqt-player.js` | ~770 | MoQT wire protocol, WebTransport, track subscription, data stream handling |
| `fragment-appender.js` | ~300 | MSE SourceBuffer management, codec auto-detection, fragment batching |

## Connection Lifecycle

### 1. WebTransport Connect

```js
this.wt = new WebTransport(relayUrl, {
  allowPooling: false,
  congestionControl: 'low-latency',
  protocols: ['moq-lite-03', 'moql', 'moqt-16', 'moqt-15'],
});
await this.wt.ready;
```

The `congestionControl: 'low-latency'` hint tells the browser to optimize for latency over throughput. Protocol negotiation uses the ALPN list matching Cloudflare's relay.

### 2. Control Stream (Bidirectional)

A single bidirectional QUIC stream carries all control messages:

```js
const bidi = await this.wt.createBidirectionalStream();
this.controlWriter = new MoqWriter(bidi.writable);
this.controlReader = new MoqReader(bidi.readable);
```

All control messages are framed as:

```
┌──────────────┬─────────────┬────────────┐
│ varint(type) │ u16(length) │   body     │
└──────────────┴─────────────┴────────────┘
```

### 3. SETUP Exchange

The client sends `CLIENT_SETUP` (0x20) offering three protocol versions:

```js
const body = new MsgBuilder()
  .varint(3)                        // num_versions
  .varint(0xff0dad02)               // moq-lite draft-02
  .varint(0xff0dad01)               // moq-lite draft-01
  .varint(0xff00000e)               // moq-transport draft-14
  .varint(2)                        // num_params
  .varint(2)                        // param: MaxRequestId (even=varint)
  .varint(1000)                     // allow server 1000 request IDs
  .varint(7)                        // param: Implementation (odd=bytes)
  .string('moqpush-custom-player')  // implementation name
  .finish();

await this.controlWriter.writeControlMessage(0x20, body);
```

The server responds with `SERVER_SETUP` (0x21) containing the negotiated version and its own `MaxRequestId` parameter, which limits how many concurrent subscriptions the client can make.

## QUIC Variable-Length Integers

All varints follow RFC 9000 Section 16:

```js
function encodeVarint(v) {
  if (v <= 63)         return new Uint8Array([v]);                    // 1 byte,  6-bit
  if (v <= 16383)      return u16(v | 0x4000);                       // 2 bytes, 14-bit
  if (v <= 1073741823) return u32((v | 0x80000000) >>> 0);           // 4 bytes, 30-bit
  return u64(BigInt(v) | 0xc000000000000000n);                       // 8 bytes, 62-bit
}
```

The top 2 bits encode the length: `00`=1B, `01`=2B, `10`=4B, `11`=8B. Decoding masks off those bits after reading the appropriate number of bytes.

## Track Subscription

### SUBSCRIBE Message (0x03)

Draft-14 wire format for subscribing to a track:

```js
body.varint(requestId);               // even IDs for client-initiated (0, 2, 4, ...)
body.varint(nsParts.length);           // namespace: array of strings
for (const part of nsParts) {
  body.string(part);                   // varint(len) + UTF-8 bytes
}
body.string(trackName);               // e.g. "video2.m4s"
body.u8(priority);                     // subscriber_priority (128=video, 64=audio)
body.u8(0x02);                         // group_order = descending (newest first)
body.bool(true);                       // forward = true (want future groups)
body.varint(startFilter);              // filter type (default: LatestGroup)
body.varint(0);                        // num_parameters = 0
```

### Start Filters

The player supports configurable start filters via the `start` URL parameter:

| Filter | Value | Behavior |
|--------|-------|----------|
| `latest_group` | 0x01 | Start from the newest group (default) |
| `largest_object` | 0x02 | Start from the largest object in the current group |

Default is `latest_group` for lowest startup latency. Override with `?start=largest_object`.

### Subscription Flow

```
Client                                  Relay
  │                                       │
  │── SUBSCRIBE(id=0, "catalog") ────────▶│
  │◀─ SUBSCRIBE_OK(id=0, alias=0) ───────│
  │                                       │
  │◀═══ Unidirectional: GROUP(alias=0) ══▶│  ← catalog JSON arrives
  │                                       │
  │── SUBSCRIBE(id=2, "video2.m4s") ─────▶│  ← parallel with audio
  │── SUBSCRIBE(id=4, "audio0.m4s") ─────▶│
  │◀─ SUBSCRIBE_OK(id=2, alias=2) ───────│
  │◀─ SUBSCRIBE_OK(id=4, alias=4) ───────│
  │                                       │
  │◀═══ Unidirectional: GROUP(alias=2) ══▶│  ← video CMAF fragments
  │◀═══ Unidirectional: GROUP(alias=4) ══▶│  ← audio CMAF fragments
```

After catalog parsing, video and audio subscriptions are issued in parallel via `Promise.all()` to avoid serialization delay.

### Atomic Write Lock

The `MoqWriter` serializes all control stream writes using a promise-chain lock. Each message is combined into a single `write()` call:

```js
async writeControlMessage(type, body) {
  const prev = this.#lock;
  let resolve;
  this.#lock = new Promise(r => { resolve = r; });
  await prev;
  try {
    const typeBytes = encodeVarint(type);
    const sizeBytes = new Uint8Array(2);  // u16 big-endian
    new DataView(sizeBytes.buffer).setUint16(0, body.byteLength);
    // Single atomic write: type + size + body
    const combined = new Uint8Array(typeBytes.byteLength + 2 + body.byteLength);
    combined.set(typeBytes, 0);
    combined.set(sizeBytes, typeBytes.byteLength);
    combined.set(body, typeBytes.byteLength + 2);
    await this.#rawWrite(combined);
  } finally {
    resolve();
  }
}
```

This is critical because the control loop (responding to `PUBLISH_NAMESPACE` with `PUBLISH_NAMESPACE_OK`) and the subscription logic both write to the same bidirectional stream from different async contexts.

## MSF Catalog Parsing

The first subscription is always to the `"catalog"` track. The catalog is an MSF (Media over QUIC Streaming Format) JSON document delivered as a GROUP on a unidirectional stream:

```json
{
  "version": 1,
  "generatedAt": "2026-03-12T18:01:49.731Z",
  "tracks": [
    {
      "name": "video0.m4s",
      "selectionParams": { "mimeType": "video/mp4", ... },
      "initData": "<base64-encoded ftyp+moov>",
      "renderGroup": 0
    },
    {
      "name": "video1.m4s",
      "initData": "<base64>",
      "renderGroup": 0
    },
    {
      "name": "video2.m4s",
      "initData": "<base64>",
      "renderGroup": 0
    },
    {
      "name": "audio0.m4s",
      "selectionParams": { "mimeType": "audio/mp4", ... },
      "initData": "<base64>"
    },
    {
      "name": "sap-timeline"
    }
  ]
}
```

The stats overlay displays "Catalog MSF draft-00 / CMSF draft-00" to indicate the catalog format version.

### Track Selection by Resolution

The player selects the video track closest to the desired resolution. When multiple video tracks exist with different resolutions, selection is based on pixel count (width × height):

```js
const videoTracks = [];
const audioTracks = [];
for (const track of catalog.tracks) {
  const mime = track.selectionParams?.mimeType || '';
  if (mime.startsWith('audio') || track.name.includes('audio')) {
    audioTracks.push(track);
  } else if (mime.startsWith('video') || track.name.includes('video')) {
    videoTracks.push(track);
  }
}
// Sort by resolution (width*height), select highest
const selectedVideo = videoTracks.reduce((best, t) => {
  const bp = best.selectionParams || {};
  const tp = t.selectionParams || {};
  return (tp.width * tp.height) > (bp.width * bp.height) ? t : best;
});
const selectedAudio = audioTracks[0];
```

Init segments are extracted from the catalog's base64 `initData` field and immediately fed to `FragmentAppender`, which triggers MSE SourceBuffer creation.

## Data Stream Processing (Unidirectional Streams)

Media data arrives on server-initiated unidirectional QUIC streams. Each stream carries one GROUP:

### GROUP Header

```
┌──────────────┬─────────────┬──────────┬──────────────┬──────────┐
│ varint(type) │ varint(alias)│ varint(g)│ [varint(sub)]│ [u8(pri)]│
└──────────────┴─────────────┴──────────┴──────────────┴──────────┘
```

The `type` field encodes both the group type and feature flags:

```js
// type 0x10-0x1f: has priority byte
// type 0x30-0x3f: no priority byte
const hasExtensions     = (baseId & 0x01) !== 0;
const hasSubgroupObject = (baseId & 0x02) !== 0;
const hasSubgroup       = (baseId & 0x04) !== 0;
const hasEnd            = (baseId & 0x08) !== 0;
```

Common type seen from Cloudflare relay: `0x15` = has priority + has subgroup + has extensions.

### FRAME Reading

Within a GROUP, frames are read until the stream ends:

```js
while (true) {
  const streamDone = await r.done();
  if (streamDone) break;

  const delta = await r.varint();          // id_delta (usually 0)

  if (hasExtensions) {
    const extLen = await r.varint();       // extensions byte length
    if (extLen > 0) await r.read(extLen);  // skip extension data
  }

  const payloadLen = await r.varint();

  if (payloadLen > 0) {
    const payload = await r.read(payloadLen);
    this._onFrame(track, payload, groupId);
  } else {
    const status = await r.varint();       // 0x03 = GROUP_END
    if (status === 0x03) break;
  }
}
```

### Frame Classification

Each frame payload is classified by inspecting its MP4 box structure:

```js
_onFrame(trackInfo, payload, groupId) {
  const type = trackInfo.type; // 'video' or 'audio'

  if (hasMoov(payload)) {
    // Init segment (moov box) — reconfigure SourceBuffer
    this.appender.setInitSegment(type, payload);
  } else if (hasMoof(payload)) {
    // Media fragment (moof+mdat) — append to SourceBuffer
    this.appender.append(type, payload);
  }
}
```

`hasMoov()` and `hasMoof()` scan the top-level MP4 boxes by reading the 4-byte size + 4-byte type header at each offset.

## MSE / FragmentAppender

### Codec Auto-Detection

Rather than hardcoding codec strings, `FragmentAppender` parses them from the init segment's `moov` box:

```
moov → trak → mdia → minf → stbl → stsd → avc1 → avcC  (video)
moov → trak → mdia → minf → stbl → stsd → mp4a → esds  (audio)
```

For video (H.264), the codec string is extracted from the `avcC` box:

```js
const off = avcC.contentOffset;
return `avc1.${hex2(data[off+1])}${hex2(data[off+2])}${hex2(data[off+3])}`;
// e.g. "avc1.4d4028" (Main profile, level 4.0)
```

For audio (AAC), the `esds` descriptor chain is walked:

```js
// ES_Descriptor (tag 0x03) → DecoderConfigDescriptor (tag 0x04)
// → objectTypeIndication + DecoderSpecificInfo (tag 0x05)
// → audioObjectType from first 5 bits
return `mp4a.${hex2(objectType)}.${audioObjectType}`;
// e.g. "mp4a.40.2" (AAC-LC)
```

### SourceBuffer Creation

SourceBuffers are created once both video and audio codecs are known:

```js
_createSourceBuffers(videoCodec, audioCodec) {
  this.mediaSource.duration = Infinity;  // live stream

  const videoSb = this.mediaSource.addSourceBuffer(`video/mp4; codecs="${videoCodec}"`);
  const audioSb = this.mediaSource.addSourceBuffer(`audio/mp4; codecs="${audioCodec}"`);

  videoSb.mode = 'segments';  // timestamps from moof boxes
  audioSb.mode = 'segments';

  // Drain queues on updateend
  videoSb.addEventListener('updateend', () => this._processQueue('video'));
  audioSb.addEventListener('updateend', () => this._processQueue('audio'));
}
```

### Buffer Queue Processing

Each SourceBuffer has an append queue. Since `appendBuffer()` is asynchronous and throws if called while updating, the queue drains one buffer at a time:

```js
_processQueue(type) {
  const sb = this.sourceBuffers[type];
  const queue = this.queues[type];
  if (!sb || sb.updating || queue.length === 0) return;

  const data = queue.shift();
  try {
    sb.appendBuffer(data);
  } catch (e) {
    if (e.name === 'QuotaExceededError') {
      queue.unshift(data);  // retry after trim
    } else if (e.name === 'InvalidStateError') {
      this.errored = true;  // unrecoverable
    }
  }
}
```

### Fragment Batching

For initial startup, `FragmentAppender` can batch multiple fragments into a single `appendBuffer()` call to reduce MSE overhead. Before the first decoded frame, incoming fragments are accumulated and flushed as one concatenated buffer:

```js
append(type, data) {
  if (!this._firstDecodeFired[type]) {
    this._batchBuffer[type].push(buf);
    if (this._batchBuffer[type].length >= this.batchSize) {
      this._flushBatch(type);
    } else if (this._batchBuffer[type].length === 1) {
      setTimeout(() => this._flushBatch(type), 250);  // flush after 250ms
    }
    return;
  }
  // After first decode, append immediately
  this.queues[type].push(buf);
  this._processQueue(type);
}
```

Default batch size is 1 (no batching). Configurable via `?batch=N` URL parameter. Testing showed batching reduces the number of `appendBuffer()` calls but does not meaningfully improve TTFF — the seek-to-live-edge fix (below) was the actual bottleneck.

## Time to First Frame (TTFF) Optimization

### The Problem

Out of the box, Chrome MSE exhibits a ~1 second delay between the first `appendBuffer()` call and the first decoded frame (`loadeddata` event). This delay persisted regardless of:
- IDR keyframe frequency (tested 1, 2, 4 per second)
- Fragment size (per-frame CMAF fragments via `gpac cdur=0.033`)
- Fragment batching (1, 8, 16 fragments per append)

### Root Cause: currentTime vs. Buffered Range Mismatch

When a `<video>` element is created, `video.currentTime` defaults to **0**. But live stream data arrives at the live edge — e.g., timestamp 52 seconds. Chrome's MSE pipeline transitions through readyState levels:

1. `HAVE_NOTHING` → `HAVE_METADATA` — triggers when init segment (moov) is appended
2. `HAVE_METADATA` → `HAVE_CURRENT_DATA` — triggers when buffered data **covers `currentTime`**

Since `currentTime=0` and buffered data starts at ~52s, Chrome stays stuck at `HAVE_METADATA`. It has the media data but won't fire `loadeddata` or begin decoding because the data doesn't cover the playback position. After ~1 second, Chrome's internal heuristics eventually resolve this, but that's the bottleneck.

### The Fix: Seek to Buffered Start

Immediately after the first media fragment is appended, seek `video.currentTime` to `buffered.start(0)`:

```js
// In _triggerPlay(), after play() is called:
const seekToBuffered = () => {
  const b = this.video.buffered;
  if (b.length > 0 && this.video.currentTime < b.start(0)) {
    this.video.currentTime = b.start(0);
  }
};
seekToBuffered();
setTimeout(seekToBuffered, 10);
setTimeout(seekToBuffered, 50);

// Also on first appendBuffer updateend:
sb.addEventListener('updateend', () => {
  const b = this.video.buffered;
  if (b.length > 0 && this.video.currentTime < b.start(0)) {
    this.video.currentTime = b.start(0);
  }
}, { once: true });
```

This immediately satisfies Chrome's `HAVE_CURRENT_DATA` requirement, triggering `loadeddata` and first frame decode within one frame interval.

### Results

| Metric | Before Fix | After Fix |
|--------|-----------|-----------|
| Append → Decode | ~1000ms | **16-21ms** |
| Total TTFF | ~1400ms | **322-353ms** |

The TTFF waterfall (displayed in the stats overlay):

| Stage | Typical Time |
|-------|-------------|
| SETUP (WebTransport + MoQT handshake) | ~100ms |
| Catalog (subscribe + parse) | ~50ms |
| 1st Fragment (subscribe + receive) | ~100ms |
| TTFF (total to first decoded frame) | **~330ms** |

### Confirmed by Research

The L3D-DASH paper (Fraunhofer HHI / Netflix / Comcast, ACM MMSys 2024) independently confirmed that increasing IDR frequency does not reduce MSE startup latency. Their finding aligns with our root cause analysis — the bottleneck is Chrome's readyState transition, not the codec layer.

## Encoding Pipeline Requirements

Source videos must be encoded with **true IDR frames** (H.264 NAL unit type 5) for reliable keyframe detection:

```bash
ffmpeg -i source.mp4 -c:v libx264 -preset veryfast -b:v 800k \
  -forced-idr 1 -bf 0 -g 30 \
  -vf "drawtext=text='%{pts\\:hms}':fontsize=48:fontcolor=white:x=10:y=10" \
  -c:a aac -b:a 128k output.mp4
```

Key flags:
- `-forced-idr 1` — Forces keyframes to be true IDR (NAL type 5), not recovery-point I-frames (NAL type 1 with SEI recovery point)
- `-bf 0` — Removes B-frames for simpler decode dependency chain and lower latency
- `-g 30` — One IDR per second at 30fps
- `drawtext` — Burns timecodes for visual latency comparison

The publisher (`moqpush-app`) detects IDR frames via `tfhd default_sample_flags` in CMAF fragments. Without `-forced-idr 1`, keyframes appear as NAL type 1 and are not detected as sync points.

CMAF fragmentation is done by gpac:

```bash
gpac -i source.mp4 -o pipe://moqpush:cmaf:cdur=0.033:noinit
```

`cdur=0.033` produces per-frame CMAF fragments (~33ms each at 30fps), giving the finest possible granularity for low-latency delivery.

## Live Edge Management

### Buffer Trimming

Every 1 second, old data is removed to prevent unbounded memory growth:

```js
trimBuffer(currentTime, keep) {
  for (const type of ['video', 'audio']) {
    const sb = this.sourceBuffers[type];
    if (!sb || sb.updating) continue;
    if (sb.buffered.length > 0) {
      const start = sb.buffered.start(0);
      const removeEnd = currentTime - keep;  // keep 3 seconds behind
      if (removeEnd > start + 1) {
        sb.remove(start, removeEnd);
      }
    }
  }
}
```

### Auto-Seek to Live Edge

If playback falls more than 2 seconds behind the buffer edge, the player jumps forward:

```js
const edge = b.end(b.length - 1);
const behind = edge - this.video.currentTime;
if (behind > 2.0 && !this.video.paused) {
  this.video.currentTime = edge - 0.1;  // 100ms behind edge
}
```

This keeps playback locked to the live edge without manual intervention.

## Performance vs. Shaka Player

Measured side-by-side on the same stream (720p H.264 + AAC, `-forced-idr 1 -bf 0`):

| Metric | Custom MoQT + MSE | Shaka Player |
|---|---|---|
| TTFF | **322-353ms** | ~1290ms |
| Steady-state latency | **~0.2s buffer** | ~1.6s reported |
| Dropped frames | 0 | 0–1 |
| Resolution | 1280x720 | 1280x720 |

The custom player achieves **sub-400ms TTFF** — nearly 4x faster than Shaka Player on the same stream. The latency advantage comes from:

1. **Seek-to-buffered-start** — Eliminates Chrome's ~1s MSE readyState stall by seeking `currentTime` to match buffered data
2. **No ABR overhead** — Single track, no bandwidth estimation or quality switching
3. **No segment request/response cycle** — Data pushed via QUIC streams, not pulled via HTTP
4. **Per-frame CMAF fragments** — 33ms granularity via gpac `cdur=0.033`
5. **Parallel subscribes** — Video and audio subscriptions issued simultaneously via `Promise.all()`
6. **QUIC low-latency congestion control** — `congestionControl: 'low-latency'` hint
7. **`latest_group` start filter** — Begins from the newest group, skipping stale data

## URL Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `start` | `latest_group` | Start filter: `latest_group` or `largest_object` |
| `batch` | `1` | Number of fragments to batch before first decode |
| `bufferBehind` | `3` | Seconds of buffer to keep behind playhead |
| `targetLatency` | `0.5` | Target latency in seconds for Shaka player |

## Stats Overlay

The player displays a 4-row TTFF waterfall in the top-left corner:

```
SETUP         102ms    ← WebTransport connect + MoQT handshake
Catalog        48ms    ← Catalog subscribe + parse
1st Fragment   97ms    ← First media fragment received
TTFF          322ms    ← First decoded frame rendered

Catalog MSF draft-00 / CMSF draft-00
```

## Control Message Reference

| Type | ID | Direction | Purpose |
|------|------|-----------|---------|
| CLIENT_SETUP | 0x20 | C→S | Version negotiation + parameters |
| SERVER_SETUP | 0x21 | S→C | Selected version + MaxRequestId |
| SUBSCRIBE | 0x03 | C→S | Request a track by namespace + name |
| SUBSCRIBE_OK | 0x04 | S→C | Subscription accepted, returns trackAlias |
| SUBSCRIBE_ERROR | 0x05 | S→C | Subscription rejected |
| PUBLISH_NAMESPACE | 0x06 | S→C | Server announces available namespace |
| PUBLISH_NAMESPACE_OK | 0x07 | C→S | Client accepts namespace |
| PUBLISH | 0x1d | S→C | Server offers to publish a track |
| PUBLISH_ERROR | 0x1f | C→S | Client rejects publish (subscriber-only) |
| PUBLISH_DONE | 0x0b | S→C | Track publication ended |
| MAX_REQUEST_ID | 0x15 | S→C | Updates the client's subscription limit |
| GOAWAY | 0x10 | S→C | Server is shutting down |

## File Layout

```
moqpush-worker/public/
├── js/
│   ├── moqt-player.js        # MoQT protocol + MoqtPlayer class
│   └── fragment-appender.js   # MSE SourceBuffer management + batching
└── player.html                # Routes ?custom-player=true to MoqtPlayer
```

The player is activated by adding `?custom-player=true` to any player URL. Without this parameter, Shaka Player is used.
