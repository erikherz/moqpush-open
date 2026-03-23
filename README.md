# moqpush

Open source MoQ publisher. Takes CMAF-IF input and publishes to any MoQ relay via MoQ Transport. Built on [moq-dev/moq](https://github.com/moq-dev/moq) by [Luke Curley](https://github.com/kixelated).

## Quick Start

### Standalone (free, uses Cloudflare's public MoQ relay)

```bash
RUST_LOG=info moqpush-app --namespace my-stream --tracks 2v1a --target-latency 500 --port 9078

# Point your encoder's HTTP CMAF-IF output at port 9078
```

Connects to Cloudflare's public MoQ relay by default. Use `--relay-url` to override.

### Test mode (verify encoder output, no relay needed)

```bash
moqpush-app --test --port 9078

# Point your encoder at port 9078 — fragment info printed to console
```

### Player

Save this as an HTML file and open in Chrome — no server needed:

```html
<script src="https://shaka-project.github.io/shaka-player/dist/shaka-player.experimental.debug.js"></script>
<video id="v" controls autoplay muted></video>
<script>
  shaka.polyfill.installAll();
  const player = new shaka.Player();
  player.attach(document.getElementById('v'));
  player.configure({
    streaming: { lowLatencyMode: true },
    manifest: { msf: { namespaces: ['my-stream'] } }
  });
  player.load('https://draft-14.cloudflare.mediaoverquic.com/', undefined, 'application/msf');
</script>
```

Replace `my-stream` with your namespace. Works from `file://` — WebTransport handles encryption.

---

## Managed CDN via moqcdn.net

1. Create an account at [moqcdn.net](https://moqcdn.net)
2. Create a namespace → get a push key
3. Run the publisher:

```bash
moqpush-app --push-key mpk_XXX --tracks 2v1a --target-latency 500 --port 9078
```

4. Watch at `moqcdn.net/{namespace}`

No relay to run. Global relay network with Viper player, ABR, relay racing, and sub-second latency.

---

## Options

| Flag | Default | Description |
|------|---------|-------------|
| --test | | Test mode: print fragment info, no relay |
| --namespace | — | Namespace (standalone mode, no Worker) |
| --relay-url | Cloudflare | Relay URL (standalone mode, override default) |
| --push-key | — | Push key (managed mode, from moqcdn.net) |
| --worker-url | moqcdn.net | Worker URL (managed mode) |
| --tracks | — | Wait for N video + M audio inits (e.g. `2v1a`) |
| --target-latency | 2000 | Target latency in ms (published in catalog) |
| --port | 9078 | HTTP CMAF-IF ingest port |
| --tls-disable-verify | false | Skip TLS cert verification (self-signed relay certs) |

## Creating Source Files

See [ffmpeg-gpac-how-to.md](ffmpeg-gpac-how-to.md) for creating CMAF-IF source files with FFmpeg and pushing them to moqpush with GPAC.

## Building

```bash
git clone https://github.com/erikherz/moqpush-open.git
cd moqpush-open
cargo build --release
# Binary at target/release/moqpush-app
```

## Acknowledgments

moqpush is built on [moq-dev/moq](https://github.com/moq-dev/moq), Luke Curley's MoQ Transport implementation in Rust. The `moq-lite`, `moq-mux`, `moq-msf`, and `moq-native` crates handle all MoQ protocol, catalog, and QUIC transport logic.

Luke's `moq-cli publish` command supports file and pipe input via its `Fmp4` importer. moqpush adds an HTTP CMAF-IF ingest server on top — accepting concurrent PUT/POST requests from encoders (Ateme, FFmpeg, GPAC) and publishing to any MoQ relay. The custom MP4 parser and publisher were needed because HTTP ingest requires handling multiple concurrent track uploads, init segment coordination, and catalog generation before connecting to the relay — patterns that don't map cleanly to the CLI's single-stream AsyncRead model.

## License

Apache 2.0
