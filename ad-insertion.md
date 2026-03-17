# MoQpush Ad Insertion

Server-side ad insertion for MoQ live streams. Pre-encoded CMAF ads are loaded from disk and inserted into live tracks at real-time pace, with automatic codec switching and timestamp continuity. Works with any ad content regardless of encoder.

## Quick Start

### 1. Prepare Ad Content

Encode your ad as CMAF DASH segments at each quality level matching your live stream. Organize into a directory:

```
/path/to/ads/
  my-ad/
    video-720-init.mp4       # ftyp+moov for 720p
    video-720-00001.m4s      # moof+mdat segment 1
    video-720-00002.m4s      # moof+mdat segment 2
    ...
    video-480-init.mp4
    video-480-00001.m4s
    ...
    video-1080-init.mp4
    video-1080-00001.m4s
    ...
    audio-init.mp4
    audio-00001.m4s
    ...
```

**File naming rules:**
- Init segments: filename must contain `init` and end in `.mp4`
- Media segments: must end in `.m4s`
- Files are grouped by prefix (everything before the first digit or "init")
- Video vs audio is auto-detected from the init segment's handler type (`vide`/`soun`)
- Zero-pad segment numbers for correct sort order (e.g. `00001`, `00002`)
- Each subdirectory under `--ad-dir` is one ad, named by the directory name

### 2. Start MoQpush with Ads

```bash
moqpush-app \
  --push-key <key> \
  --ad-dir /path/to/ads \
  --tracks 3v1a \
  --target-latency 250
```

On startup, the ad manager scans `--ad-dir` for subdirectories and loads all init segments and media fragments into memory.

### 3. Trigger an Ad

```bash
curl -X POST 'http://localhost:8888/admin/ad?name=my-ad'
```

### 4. List Available Ads

```bash
curl http://localhost:8888/admin/ads
# Returns: { "ads": ["my-ad", "another-ad"] }
```

## API Reference

### `POST /admin/ad?name=<ad_name>`

Triggers ad insertion on the live stream.

**Parameters:**
| Param | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Directory name of the ad under `--ad-dir` |

**Response:**
```json
{
  "ok": true,
  "ad": "my-ad",
  "slots_published": 19,
  "fragments_published": 75
}
```

The response returns after the full ad has played (real-time duration). `slots_published` includes the init slot (slot 0) plus all media slots.

### `GET /admin/ads`

Lists all loaded ads.

**Response:**
```json
{
  "ads": ["my-ad", "another-ad"]
}
```

## How It Works

### Ad Loading (Startup)

The `AdManager` (`ad_manager.rs`) scans the `--ad-dir` directory at startup:

1. Each subdirectory is one ad
2. Files are grouped by prefix using `extract_prefix()` (e.g. `video-720-init.mp4` and `video-720-00001.m4s` share prefix `video-720-`)
3. Init segments (`.mp4` files containing `init` in the name) are parsed to extract handler type (`vide`/`soun`) and resolution (height)
4. Media segments (`.m4s` files) are loaded into memory as `Vec<Bytes>`
5. Video tracks are keyed by resolution height (e.g. 240, 480, 720, 1080) for matching to live tracks

### Ad Trigger Flow

When `POST /admin/ad` is called, the following sequence executes:

#### Phase 1: Preparation (`prepare_ad()` in `publisher.rs`)

1. **Track matching** — Ad video tracks are matched to publisher video tracks by resolution. The publisher extracts dimensions from its stored init segments and looks up the corresponding ad track by height.

2. **BDT offset computation** — For each track, the last live `baseMediaDecodeTime` plus one fragment duration gives the starting BDT for the ad. This ensures timestamp continuity.

3. **Live group closure** — All active live groups on all tracks are finished.

4. **Timeslot construction** — Ad fragments are organized into timeslots:
   - **Slot 0**: Ad init segments (moov) for each matched track
   - **Slots 1..N**: Rebased media fragments (moof+mdat) with timestamps adjusted for continuity

5. **Ad group creation** — New MoQ groups are opened on each track for the ad content.

6. **Pace calculation** — The pacing interval is computed from BDT deltas between the first two ad segments (typically ~1000ms for 1-second segments).

7. **State transition** — `AdState` is set to `PlayingAd`, which causes `send_fragment()` and `start_segment()` to silently drop incoming live fragments.

#### Phase 2: Paced Publishing (`handle_ad_trigger()` in `http_ingest.rs`)

1. Slot 0 (init segments) is published immediately with no delay
2. Each subsequent media slot is published with real-time pacing (`tokio::time::sleep` between slots)
3. The publisher mutex is released between each slot, allowing the system to remain responsive (though live fragments are dropped by the `PlayingAd` state check)

#### Phase 3: Live Resume (`finish_ad()` in `publisher.rs`)

1. Ad groups are closed on all tracks
2. New groups are opened on each track
3. **Live init segments are sent as moov frames** on the data stream — this tells the player to reinitialize its decoder with the live codec parameters
4. `AdState` returns to `Live`, and incoming live fragments resume flowing

### Init-as-Frame Codec Switching

The key innovation is sending init segments (moov boxes) as regular data frames on the MoQ track, rather than updating the MSF catalog.

**Why not use the catalog?**
- The catalog heartbeat republishes every 2 seconds
- During an 18-second ad, that's ~9 catalog updates, each triggering init change detection
- The player would process init changes from all tracks in the catalog (not just the subscribed one)
- This causes rapid codec flipping and `InvalidStateError` from `changeType()` calls while the SourceBuffer is busy

**How init-as-frame works:**
1. The publisher writes the moov bytes as an object in the MoQ group
2. The player's `_onFrame()` handler checks every incoming payload with `hasMoov()`
3. When a moov is detected, it calls `appender.setInitSegment(type, payload)`
4. The `FragmentAppender` detects codec changes, queues a `changeType()` marker, then queues the new init data
5. The `_processQueue()` method processes the `changeType` marker when the SourceBuffer is idle, then appends the init segment
6. Subsequent moof+mdat frames are decoded with the new codec parameters

This approach:
- Works with any ad content regardless of encoder (ffmpeg, GPAC, etc.)
- Requires no `use_init` parameter — it's always automatic
- Avoids catalog heartbeat interference
- Processes codec changes in the correct order via the SourceBuffer queue

### Timestamp Rebasing

Each ad fragment's `baseMediaDecodeTime` (BDT) in the `tfdt` box is rewritten using `set_decode_time()` to continue from where the live stream left off:

```
ad_desired_bdt = live_last_bdt + live_frag_duration + ad_original_bdt
```

This is done at the raw MP4 byte level by locating the `tfdt` box within `moof > traf` and overwriting the 4-byte or 8-byte BDT field in place.

### Live Fragment Dropping

While `AdState::PlayingAd` is active:
- `send_fragment()` returns immediately without writing (drops the fragment)
- `start_segment()` returns immediately (prevents new live group creation)

This prevents live and ad content from interleaving on the same track. When the ad finishes and state returns to `Live`, the next live fragment creates a new group and continues normally.

## Architecture

### Code Map

| Component | File | Purpose |
|-----------|------|---------|
| Ad loading | `moqpush-app/src/ad_manager.rs` | Scan directories, parse inits, load fragments |
| Ad state machine | `moqpush-app/src/publisher.rs` | `prepare_ad()`, `publish_ad_slot()`, `finish_ad()`, fragment dropping |
| HTTP trigger | `moqpush-app/src/http_ingest.rs` | `handle_ad_trigger()`, pacing loop |
| BDT rewriting | `moqpush-app/src/mp4/parser.rs` | `set_decode_time()`, `rebase_decode_time()` |
| Player init detection | `moqpush-worker/public/js/moqt-player.js` | `_onFrame()` moov detection, catalog init change tracking |
| MSE codec switching | `moqpush-worker/public/js/fragment-appender.js` | `setInitSegment()`, `changeType()` queuing |

### Data Flow

```
curl POST /admin/ad
  |
  v
handle_ad_trigger()
  |
  +-- prepare_ad()
  |     |-- Match ad tracks to live tracks by resolution
  |     |-- Compute BDT offsets for timestamp continuity
  |     |-- Build timeslots: [init_slot, media_slot_1, ..., media_slot_N]
  |     |-- Set AdState::PlayingAd (drops live fragments)
  |     +-- Open new MoQ groups for ad
  |
  +-- Pacing loop (1 slot per ~1000ms)
  |     |-- Slot 0: publish ad init (moov) frames immediately
  |     |-- Slot 1..N: publish rebased media (moof+mdat) with sleep
  |     +-- Live fragments silently dropped by send_fragment()
  |
  +-- finish_ad()
        |-- Close ad groups
        |-- Open new groups, send live init (moov) as frames
        +-- Set AdState::Live (resumes live fragments)
```

### Player-Side Flow

```
Incoming MoQ object
  |
  v
_onFrame()
  |-- hasMoov(payload)?
  |     YES --> appender.setInitSegment(type, payload)
  |               |-- Parse codec from moov
  |               |-- If codec changed: queue {_changeType: newMime}
  |               +-- Queue init data for appendBuffer
  |
  |-- hasMoof(payload)?
        YES --> appender.append(type, payload)
                  +-- Queue moof+mdat for appendBuffer

_processQueue()
  |-- Next item is _changeType marker?
  |     YES --> sb.changeType(mime)  // synchronous when idle
  |             process next item immediately
  |
  |-- Next item is data?
        YES --> sb.appendBuffer(data)  // async, triggers updateend
```

## Preparing Ad Content

### From an Existing Video File

Use ffmpeg to create multi-quality CMAF DASH segments:

```bash
# 720p
ffmpeg -i source.mp4 -vf scale=1280:720 -c:v libx264 -b:v 2000k \
  -g 30 -keyint_min 30 -sc_threshold 0 \
  -c:a aac -ar 48000 -ac 2 -b:a 128k \
  -f dash -seg_duration 1 \
  -init_seg_name 'video-720-init.mp4' \
  -media_seg_name 'video-720-$Number%05d$.m4s' \
  /path/to/ads/my-ad/manifest.mpd
```

Repeat for each quality level (adjust `-vf scale`, bitrate, and filename prefix).

For audio-only (if separate from video):
```bash
ffmpeg -i source.mp4 -vn -c:a aac -ar 48000 -ac 2 -b:a 128k \
  -f dash -seg_duration 1 \
  -init_seg_name 'audio-init.mp4' \
  -media_seg_name 'audio-$Number%05d$.m4s' \
  /path/to/ads/my-ad/manifest_audio.mpd
```

### From Existing DASH Output

If you already have DASH segments (e.g. from `chunk-stream0-00001.m4s`):

1. Probe the init segments to identify which stream is which:
   ```bash
   for i in 0 1 2 3; do
     echo "=== stream$i ==="
     ffprobe -v quiet -show_entries stream=codec_type,width,height,sample_rate \
       init-stream$i.m4s
   done
   ```

2. Rename to the expected convention:
   ```bash
   mv init-stream0.m4s video-720-init.mp4
   for f in chunk-stream0-*.m4s; do
     mv "$f" "video-720-${f#chunk-stream0-}"
   done
   ```

### Matching Live Encoder Settings

For the smoothest playback, match these parameters between ad and live:
- **Timescale** (usually 15360 for video, 48000 for audio)
- **Sample rate and channels** (audio)
- **Resolution** (must match exactly for track matching)

The codec profile/level and encoder don't need to match — the init-as-frame approach handles codec switching automatically.

## Limitations

- **Single ad at a time** — Triggering a new ad while one is playing is not supported
- **No skip/cancel** — Once triggered, the ad plays to completion
- **Resolution matching only** — Ad tracks are matched to live tracks by height; if no matching resolution exists, that quality level gets no ad
- **No ad scheduling** — Ads are triggered manually via API; there's no built-in SCTE-35 or scheduling system
- **Segment count mismatch** — If ad qualities have different segment counts (e.g. 17 vs 18), the shorter quality finishes early while others continue
