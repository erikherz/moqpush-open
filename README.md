# MoQpush

Global MoQ CDN for Low Latency CMAF.

MoQpush accepts CMAF segments from your encoder via HTTP PUT, publishes them to Cloudflare's MoQ relay network, and orchestrates edge servers to warm relays worldwide. Viewers watch via Shaka Player with MoQ streaming support in a browser.

## Components

| Component | Description |
|---|---|
| **moqpush-app** | Publisher binary. Accepts CMAF-IF ingest from your encoder, authenticates with the worker, and publishes to the Cloudflare MoQ relay. |
| **moqpush-puller** | Edge pull server. Connects to the worker via WebSocket, receives pull/stop commands, and subscribes to the relay to warm the network for a region. |
| **moqpush-worker** | Cloudflare Worker + Durable Objects. Handles auth, namespace management, orchestration, stats, and serves the web UI. |

## moqpush-app

```
./moqpush-app --push-key YOUR_PUSH_KEY_HERE
```

| Param | Description | Default |
|---|---|---|
| `--push-key` | Push key from moqpush admin | env: `MOQPUSH_KEY` |
| `--worker-url` | Worker URL for auth + heartbeat + stats | `https://moqpush.com` |
| `--port` | Port for HTTP CMAF-IF ingest | `8888` |
| `--test` | Accept and print incoming data without connecting to worker or relay | |

## moqpush-puller

```
./moqpush-puller --node naw --region us-west --secret YOUR_SECRET
```

| Param | Description | Default |
|---|---|---|
| `--node` | Node name (e.g. `naw`, `nac`, `nae`, `eu1`, `in1`) | env: `MOQPUSH_NODE` |
| `--region` | Region label (e.g. `us-west`, `us-central`, `us-east`, `eu-west`, `ap-south`) | env: `MOQPUSH_REGION` |
| `--worker-url` | Worker URL for WebSocket orchestration | `https://moqpush.com` |
| `--port` | Health check HTTP port | `8080` |
| `--secret` | Puller secret for authentication | env: `MOQPUSH_PULLER_SECRET` |

## Building

```
cargo build --release -p moqpush-app
cargo build --release -p moqpush-puller
```

Binaries are output to `target/release/`.

## Architecture

```
Encoder (CMAF-IF HTTP PUT)
    |
    v
moqpush-app ---> Cloudflare MoQ Relay ---> moqpush-puller (per region)
    |                                           |
    v                                           v
moqpush-worker (auth, orchestration)     Warm relay cache
    |
    v
Shaka Player (MoQ/WebTransport in browser)
```

1. Sign in at [moqpush.com/admin](https://moqpush.com/admin) and create a namespace
2. Run `moqpush-app` with your push key
3. Point your encoder (Ateme Titan Live, FFmpeg, GPAC) to `http://localhost:8888`
4. Share `https://moqpush.com/your-namespace` with viewers
