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
| `fragment-appender.js` | ~265 | MSE SourceBuffer management, codec auto-detection from MP4 boxes |

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
body.varint(0x02);                     // filter = LargestObject
body.varint(0);                        // num_parameters = 0
```

### Subscription Flow

```
Client                                  Relay
  │                                       │
  │── SUBSCRIBE(id=0, "catalog") ────────▶│
  │◀─ SUBSCRIBE_OK(id=0, alias=0) ───────│
  │                                       │
  │◀═══ Unidirectional: GROUP(alias=0) ══▶│  ← catalog JSON arrives
  │                                       │
  │── SUBSCRIBE(id=2, "video2.m4s") ─────▶│
  │◀─ SUBSCRIBE_OK(id=2, alias=2) ───────│
  │                                       │
  │── SUBSCRIBE(id=4, "audio0.m4s") ─────▶│
  │◀─ SUBSCRIBE_OK(id=4, alias=4) ───────│
  │                                       │
  │◀═══ Unidirectional: GROUP(alias=2) ══▶│  ← video CMAF fragments
  │◀═══ Unidirectional: GROUP(alias=4) ══▶│  ← audio CMAF fragments
```

Subscriptions are serialized — each `await this._subscribe()` blocks until `SUBSCRIBE_OK` arrives on the control stream. This prevents interleaved writes from corrupting the control stream framing.

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

### Track Selection

The player selects one video track (highest quality = last in array) and one audio track:

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
  // skip non-media tracks like "sap-timeline"
}
const selectedVideo = videoTracks[videoTracks.length - 1]; // highest quality
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

Measured side-by-side on the same stream (1080p H.264 + AAC):

| Metric | Custom MoQT + MSE | Shaka Player |
|---|---|---|
| Buffer health | **0.35s** | 0.34s |
| Clock offset | **+261ms ahead** | baseline |
| Reported latency | — | 1.61s |
| Dropped frames | 0 | 1 |
| Resolution | 1920x1080 | 1920x1080 |
| Data received | 8.2 MB / 148 frames (31s) | — (8s) |

The custom player displays video **261ms ahead** of Shaka (19:13:30.197 vs 19:13:29.936 on the in-stream clock overlay) despite near-identical buffer depths. The latency advantage comes from:

1. **No ABR overhead** — single track, no bandwidth estimation or quality switching
2. **No segment request/response cycle** — data pushed via QUIC streams, not pulled via HTTP
3. **Minimal buffering** — fragments appended immediately on arrival, 0.35s buffer
4. **QUIC low-latency congestion control** — `congestionControl: 'low-latency'` hint

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
│   └── fragment-appender.js   # MSE SourceBuffer management
└── player.html                # Routes ?custom-player=true to MoqtPlayer
```

The player is activated by adding `?custom-player=true` to any player URL. Without this parameter, Shaka Player is used.
