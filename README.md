# moqpush

Open source MoQ publisher. Takes CMAF-IF input from any encoder (Ateme, GPAC, FFmpeg), publishes to any MoQ relay via MoQ Transport.

## Quick Start

### Standalone (no MoQcdn account, own MoQ relay)

```bash
# Publish
moqpush-app --relay-url https://your-relay:443 --namespace my-stream --tracks 2v1a --target-latency 500 --port 9078

# Point your encoder's HTTP CMAF-IF output at the specified port
```

### Test mode (verify encoder output, no relay needed)

```bash
moqpush-app --test

# Point your encoder at port 9078 — fragment info printed to console
```

### Managed hosting via moqcdn.net (free tier)

1. Create an account at [moqcdn.net](https://moqcdn.net)
2. Create a namespace → get a push key
3. Run the publisher:

```bash
moqpush-app --push-key mpk_XXX --worker-url https://moqcdn.net --tracks 2v1a --target-latency 500
```

4. Watch at `moqcdn.net/{namespace}`

Free tier uses Cloudflare's MoQ relay with Shaka player. No relay to run.

### Premium CDN (moqcdn.net)

Same command, same binary. Premium namespaces automatically route to the moqcdn global relay network with Viper player, ABR, relay racing, and sub-second latency.

## Options

| Flag | Default | Description |
|------|---------|-------------|
| --test | | Test mode: print fragment info, no relay |
| --relay-url | — | Relay URL (standalone mode, no Worker) |
| --namespace | — | Namespace (required with --relay-url) |
| --push-key | — | Push key (managed mode, from moqcdn.net) |
| --worker-url | moqcdn.net | Worker URL (managed mode) |
| --tracks | — | Wait for N video + M audio inits (e.g. `2v1a`) |
| --target-latency | 2000 | Target latency in ms (published in catalog) |
| --port | 9078 | HTTP CMAF-IF ingest port |
| --tls-disable-verify | false | Skip TLS cert verification (self-signed relay certs) |

## Player

The `player/` directory contains standalone HTML players that work from `file://` in Chrome — no server needed:

| File | Description |
|------|-------------|
| `moq-player.html` | Interactive: enter relay URL + namespace, click Play |
| `moq-player-local.html` | Same but with Shaka JS embedded (~2MB, zero internet needed) |
| `player-remote.html` | Edit two constants, loads Shaka from CDN |
| `player-local.html` | Edit two constants, uses bundled `shaka-player.js` |
| `shaka-player.js` | Shaka Player experimental build with MoQ/MSF support |

### Minimal player example

```html
<script src="https://shaka-project.github.io/shaka-player/dist/shaka-player.experimental.debug.js"></script>
<video id="v" controls autoplay muted></video>
<script>
  shaka.polyfill.installAll();
  const player = new shaka.Player();
  player.attach(document.getElementById('v'));
  player.configure({
    streaming: { lowLatencyMode: true },
    manifest: { msf: { namespaces: ['YOUR_NAMESPACE'] } }
  });
  player.load('https://YOUR_RELAY/', undefined, 'application/msf');
</script>
```

Works from a local HTML file. No HTTPS hosting required — WebTransport handles encryption.

## How It Works

```
Encoder (Ateme/GPAC/FFmpeg)
     | HTTP PUT (CMAF-IF, chunked transfer)
     v
moqpush-app (this binary)
     | MoQ Transport (QUIC/WebTransport)
     v
MoQ Relay (Cloudflare, moqcdn, or self-hosted)
     | MoQ Transport (WebTransport)
     v
Browser (Shaka Player or Viper Player)
```

- Encoder sends CMAF-IF segments via HTTP PUT with chunked transfer
- Each PUT is one segment (~1.8s), containing ~16 fragments streamed in real-time
- moqpush-app parses fragments as they arrive and publishes immediately via MoQ
- MSF catalog with codec info, init segments, and target latency published automatically
- `--tracks` waits for all init segments before connecting (multi-quality ABR support)
- Protocol auto-negotiated with relay (moq-transport draft-14, moq-lite-02/03)

## Building

```bash
git clone https://github.com/erikherz/moqpush.git
cd moqpush
cargo build --release
# Binary at target/release/moqpush-app
```

## Relay

For standalone use, run Luke Curley's open source MoQ relay:

```bash
git clone https://github.com/moq-dev/moq.git
cd moq
cargo build --release --bin moq-relay
./target/release/moq-relay relay.toml
```

## License

Apache 2.0
