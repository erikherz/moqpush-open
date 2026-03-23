# FFmpeg + GPAC: Creating CMAF-IF Sources for moqpush

moqpush accepts HTTP CMAF-IF input from any encoder. This guide shows how to create CMAF-compliant source files with FFmpeg and push them to moqpush using GPAC.

## Create CMAF Source Files with FFmpeg

Source files must be fragmented MP4 with CMAF brands. Each track (video resolution, audio) is a separate file.

### From an existing video

```bash
INPUT="/root/vid/source.mp4"

# 1080p video
ffmpeg -i "$INPUT" \
  -vf scale=1920:1080 -c:v libx264 -profile:v high -level 4.2 \
  -g 25 -keyint_min 25 -sc_threshold 0 \
  -b:v 4M -maxrate 5M -bufsize 8M \
  -an -movflags +frag_keyframe+empty_moov+default_base_moof+cmaf \
  -f mp4 /root/vid/source_1080.mp4

# 720p video
ffmpeg -i "$INPUT" \
  -vf scale=1280:720 -c:v libx264 -profile:v main -level 3.1 \
  -g 25 -keyint_min 25 -sc_threshold 0 \
  -b:v 2M -maxrate 2.5M -bufsize 4M \
  -an -movflags +frag_keyframe+empty_moov+default_base_moof+cmaf \
  -f mp4 /root/vid/source_720.mp4

# 480p video
ffmpeg -i "$INPUT" \
  -vf scale=854:480 -c:v libx264 -profile:v main -level 3.0 \
  -g 25 -keyint_min 25 -sc_threshold 0 \
  -b:v 1M -maxrate 1.2M -bufsize 2M \
  -an -movflags +frag_keyframe+empty_moov+default_base_moof+cmaf \
  -f mp4 /root/vid/source_480.mp4

# Audio only
ffmpeg -i "$INPUT" \
  -vn -c:a aac -b:a 128k -ar 48000 -ac 2 \
  -movflags +frag_keyframe+empty_moov+default_base_moof+cmaf \
  -f mp4 /root/vid/source_audio.mp4
```

### Test pattern (no source file needed)

```bash
# 60-second color bars at 720p
ffmpeg -f lavfi -i "testsrc2=size=1280x720:rate=25:duration=60" \
  -c:v libx264 -profile:v main -g 25 -keyint_min 25 -sc_threshold 0 \
  -b:v 2M -an \
  -movflags +frag_keyframe+empty_moov+default_base_moof+cmaf \
  -f mp4 /root/vid/test_720.mp4

# Test audio (1kHz tone)
ffmpeg -f lavfi -i "sine=frequency=1000:sample_rate=48000:duration=60" \
  -c:a aac -b:a 128k \
  -movflags +frag_keyframe+empty_moov+default_base_moof+cmaf \
  -f mp4 /root/vid/test_audio.mp4
```

### Key FFmpeg flags explained

| Flag | Purpose |
|------|---------|
| `-g 25 -keyint_min 25` | Fixed GOP length (1 second at 25fps) |
| `-sc_threshold 0` | Disable scene-change IDR insertion |
| `+frag_keyframe` | Start new fragment at each keyframe |
| `+empty_moov` | Init segment (moov) contains no samples |
| `+default_base_moof` | Required for CMAF `cmfc` brand |
| `+cmaf` | Sets CMAF brand in ftyp box |
| `-an` / `-vn` | Separate video and audio into individual files |

## Push to moqpush with GPAC

GPAC reads the CMAF source files, plays them back in real-time, and pushes via HTTP CMAF-IF to moqpush.

### 3 video + 1 audio (ABR)

```bash
gpac \
  -i /root/vid/source_1080.mp4 \
  -i /root/vid/source_720.mp4 \
  -i /root/vid/source_480.mp4 \
  -i /root/vid/source_audio.mp4 \
  reframer:rt=on \
  -o http://localhost:9078/manifest.mpd:gpac:hmode=push:cdur=0.033:cmaf=cmfc
```

### 1 video + 1 audio (single quality)

```bash
gpac \
  -i /root/vid/source_720.mp4 \
  -i /root/vid/source_audio.mp4 \
  reframer:rt=on \
  -o http://localhost:9078/manifest.mpd:gpac:hmode=push:cdur=0.033:cmaf=cmfc
```

### GPAC flags explained

| Flag | Purpose |
|------|---------|
| `reframer:rt=on` | Real-time playback (simulates live encoder) |
| `hmode=push` | HTTP push mode (POST/PUT to server) |
| `cdur=0.033` | ~33ms CMAF chunks (one frame at 30fps) |
| `cmaf=cmfc` | CMAF fragment output format |

Adjust `cdur` for your framerate: `0.04` for 25fps, `0.033` for 30fps.

### With moqpush standalone mode

```bash
# Terminal 1: start moqpush
RUST_LOG=info moqpush-app --namespace my-stream --tracks 3v1a --target-latency 500 --port 9078

# Terminal 2: push with GPAC
gpac \
  -i /root/vid/source_1080.mp4 \
  -i /root/vid/source_720.mp4 \
  -i /root/vid/source_480.mp4 \
  -i /root/vid/source_audio.mp4 \
  reframer:rt=on \
  -o http://localhost:9078/manifest.mpd:gpac:hmode=push:cdur=0.033:cmaf=cmfc
```

## Verify CMAF Structure with ffprobe

```bash
# Check ftyp brands (should show cmfc)
ffprobe -v error -show_format /root/vid/source_720.mp4 2>&1 | grep -i brand

# Check keyframe interval
ffprobe -v error -select_streams v:0 \
  -show_entries packet=pts_time,flags \
  -of csv=p=0 /root/vid/source_720.mp4 | grep K | head -10

# Verify fragmentation (look for moov, moof, mdat boxes)
ffprobe -v trace /root/vid/source_720.mp4 2>&1 | grep -E "moov|moof|mdat|ftyp" | head -20

# Check codec and resolution
ffprobe -v error -select_streams v:0 \
  -show_entries stream=codec_name,width,height,r_frame_rate \
  /root/vid/source_720.mp4
```

## Troubleshooting

**moqpush doesn't detect init segments:** Source files must have `empty_moov` — the moov box should contain track metadata but no sample data. Re-encode with `+empty_moov`.

**GPAC errors about fragmentation:** Make sure source files are fragmented MP4, not regular MP4. The `+frag_keyframe` flag is required.

**Playback stutters:** Check that `-g` and `-keyint_min` match (fixed GOP). Scene-change IDRs (`-sc_threshold 0` disables them) can cause irregular fragment sizes.

**Audio/video sync issues:** Ensure all tracks use the same source and have aligned timestamps. Encoding them from the same input file in one FFmpeg session helps.
