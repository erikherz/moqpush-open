use anyhow::Result;
use clap::Parser;
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Notify};
use tracing::{error, info, warn};

use moq_lite::Origin;
use moq_mux::CatalogProducer;

mod http_ingest;
mod mp4;
mod publisher;

use publisher::{Publisher, PublisherStats};

/// Default relay (Cloudflare public MoQ relay)
const DEFAULT_RELAY: &str = "https://draft-14.cloudflare.mediaoverquic.com";

#[derive(Parser, Debug)]
#[command(name = "moqpush-app")]
#[command(about = "MoQ push publisher — accepts HTTP CMAF-IF from encoder and publishes to any MoQ relay")]
struct Args {
    /// Push key (managed mode: from moqcdn.net admin)
    #[arg(long, env = "MOQPUSH_KEY")]
    push_key: Option<String>,

    /// Worker URL for auth + heartbeat (managed mode)
    #[arg(long, default_value = "https://moqcdn.net")]
    worker_url: String,

    /// Relay URL (standalone mode: defaults to Cloudflare public relay)
    #[arg(long)]
    relay_url: Option<String>,

    /// Namespace (standalone mode: connect directly, no Worker auth)
    #[arg(long)]
    namespace: Option<String>,

    /// Port for HTTP CMAF-IF ingest
    #[arg(long, default_value_t = 9078)]
    port: u16,

    /// Target latency in milliseconds for the MSF catalog (default: 2000)
    #[arg(long)]
    target_latency: Option<u64>,

    /// Expected track counts before publishing catalog, e.g. "3v1a" for 3 video + 1 audio.
    /// Without this, the catalog publishes as soon as any video + audio init arrives.
    #[arg(long)]
    tracks: Option<String>,

    /// Test mode: accept and print incoming data without connecting to worker or relay
    #[arg(long)]
    test: bool,

    /// Skip TLS certificate verification (for self-signed relay certs in testing)
    #[arg(long)]
    tls_disable_verify: bool,

}

/// Parse a track spec like "3v1a" into (video_count, audio_count).
fn parse_track_spec(spec: &str) -> Result<(u32, u32)> {
    let spec = spec.to_lowercase();
    let mut video = None;
    let mut audio = None;
    let mut num_buf = String::new();

    for c in spec.chars() {
        match c {
            '0'..='9' => num_buf.push(c),
            'v' => {
                video = Some(num_buf.parse::<u32>().unwrap_or(1));
                num_buf.clear();
            }
            'a' => {
                audio = Some(num_buf.parse::<u32>().unwrap_or(1));
                num_buf.clear();
            }
            _ => {}
        }
    }

    match (video, audio) {
        (Some(v), Some(a)) => Ok((v, a)),
        (Some(v), None) => Ok((v, 0)),
        (None, Some(a)) => Ok((0, a)),
        _ => Err(anyhow::anyhow!("invalid --tracks format '{}', expected e.g. '3v1a'", spec)),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("moqpush-app v{}", env!("CARGO_PKG_VERSION"));

    let args = Args::parse();

    if args.test {
        info!("TEST MODE — accepting CMAF-IF on port {}, printing and dropping data", args.port);
        run_test_mode(args.port).await?;
        return Ok(());
    }

    // Determine mode: standalone (--namespace) or managed (--push-key)
    // managed_info holds (push_key, instance_id) when in managed mode
    let (namespace, relay_url, jwt, managed_info) = if let Some(namespace) = args.namespace {
        // Standalone mode: direct relay connection, no Worker
        let relay_url = args.relay_url.unwrap_or_else(|| DEFAULT_RELAY.to_string());
        info!("Standalone mode: namespace='{}', relay='{}'", namespace, relay_url);
        (namespace, relay_url, String::new(), None)
    } else {
        // Managed mode: authenticate with Worker
        let push_key = args.push_key
            .ok_or_else(|| anyhow::anyhow!("--push-key required (managed mode), or use --namespace (standalone) or --test"))?;

        let instance_id: String = {
            let mut rng = rand::rng();
            (0..16).map(|_| format!("{:x}", rng.random_range(0..16u8))).collect()
        };
        info!("Instance ID: {}", instance_id);

        info!("Authenticating with worker at {}...", args.worker_url);
        let client = reqwest::Client::new();

        let auth_resp = client
            .post(format!("{}/api/push/auth", args.worker_url))
            .json(&serde_json::json!({
                "push_key": push_key,
                "instance_id": instance_id,
            }))
            .send()
            .await?;

        let status = auth_resp.status();
        if !status.is_success() {
            let body = auth_resp.text().await.unwrap_or_default();
            if status.as_u16() == 409 {
                return Err(anyhow::anyhow!("Namespace already in use by another instance: {}", body));
            }
            return Err(anyhow::anyhow!("Auth failed ({}): {}", status, body));
        }

        let auth_body: serde_json::Value = auth_resp.json().await?;
        let namespace = auth_body["namespace"].as_str().unwrap_or("").to_string();
        let relay_url = auth_body["relay_url"]
            .as_str()
            .unwrap_or(DEFAULT_RELAY)
            .to_string();
        let jwt = auth_body["jwt"].as_str().unwrap_or("").to_string();

        if jwt.is_empty() {
            info!("Authenticated: namespace='{}', relay='{}' (no JWT)", namespace, relay_url);
        } else {
            info!("Authenticated: namespace='{}', relay='{}' (JWT received)", namespace, relay_url);
        }
        (namespace, relay_url, jwt, Some((push_key, instance_id)))
    };

    // Create shutdown channel
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    // Spawn push key heartbeat (lock renewal) — managed mode only
    if let Some((ref push_key, ref instance_id)) = managed_info {
        let hb_worker_url = args.worker_url.clone();
        let hb_key = push_key.clone();
        let hb_instance = instance_id.clone();
        tokio::spawn(async move {
            run_push_heartbeat(hb_worker_url, hb_key, hb_instance, shutdown_tx).await;
        });
    }

    // Create moq-lite content model
    let origin = Origin::produce();
    let mut broadcast = origin.create_broadcast(&namespace)
        .ok_or_else(|| anyhow::anyhow!("failed to create broadcast for namespace '{}'", namespace))?;
    let catalog = CatalogProducer::new(&mut broadcast)
        .map_err(|e| anyhow::anyhow!("failed to create catalog: {}", e))?;
    let pub_stats = PublisherStats::new();
    let stats_ref = pub_stats.clone();
    let mut publisher = Publisher::new(broadcast, catalog, pub_stats.clone());
    if let Some(latency) = args.target_latency {
        publisher.set_target_latency_ms(latency);
    }
    if let Some(ref spec) = args.tracks {
        let (expected_v, expected_a) = parse_track_spec(spec)?;
        publisher.set_expected_tracks(expected_v, expected_a);
        info!("Waiting for {} video + {} audio init segments before publishing catalog", expected_v, expected_a);
    }

    let first_init_notify = Arc::new(Notify::new());

    // Spawn HTTP ingest server
    info!("HTTP ingest starting on port {}", args.port);
    let http_shutdown = shutdown_rx.clone();
    let http_notify = first_init_notify.clone();
    tokio::spawn(async move {
        if let Err(e) = http_ingest::run(args.port, publisher, http_notify, http_shutdown).await {
            error!("HTTP ingest error: {}", e);
        }
    });

    // Wait for complete catalog (all expected init segments) before connecting to relay
    let wait_msg = if args.tracks.is_some() {
        format!("Waiting for all init segments ({})...", args.tracks.as_ref().unwrap())
    } else {
        "Waiting for first CMAF init segment...".to_string()
    };
    info!("{}", wait_msg);
    tokio::time::timeout(
        Duration::from_secs(300),
        first_init_notify.notified(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("Timeout: init segments not received within 300s"))?;

    // Connect to relay as publisher
    info!("All init segments received — connecting to relay at {}...", relay_url);

    let mut relay_url_parsed: url::Url = relay_url.parse()?;
    // In standalone mode, append namespace to URL path for relay auth matching
    if managed_info.is_none() {
        let path = relay_url_parsed.path().trim_end_matches('/').to_string();
        relay_url_parsed.set_path(&format!("{}/{}", path, namespace));
    }
    if !jwt.is_empty() {
        relay_url_parsed.query_pairs_mut().append_pair("jwt", &jwt);
    }
    let client_config = moq_native::ClientConfig::default();

    let client = client_config.init()?;
    let session = client
        .with_publish(origin.consume())
        .connect(relay_url_parsed.clone())
        .await?;

    info!("Connected to relay");

    // Announce broadcast + spawn stats loop — managed mode only
    if let Some((ref push_key, ref instance_id)) = managed_info {
        let dir_client = reqwest::Client::new();
        let announce_resp = dir_client
            .post(format!("{}/api/push/announce", args.worker_url))
            .json(&serde_json::json!({
                "push_key": push_key,
                "namespace": namespace,
                "relay_url": relay_url,
                "instance_id": instance_id,
            }))
            .send()
            .await?;

        if announce_resp.status().is_success() {
            info!("Broadcast announced: {} -> {}", namespace, relay_url);
        } else {
            warn!("Failed to announce broadcast: {}", announce_resp.status());
        }

        let hb_worker = args.worker_url.clone();
        let hb_key = push_key.clone();
        let hb_ns = namespace.clone();
        let hb_instance = instance_id.clone();
        tokio::spawn(async move {
            run_stats_loop(hb_worker, hb_key, hb_ns, hb_instance, stats_ref).await;
        });
    }

    // Run until session closes or shutdown, polling transport stats every second
    let mut transport_interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            result = session.closed() => {
                match result {
                    Ok(()) => info!("Relay session closed normally"),
                    Err(e) => warn!("Relay session closed with error: {}", e),
                }
                break;
            }
            _ = shutdown_rx.changed() => {
                info!("Shutdown signal received");
                break;
            }
            _ = transport_interval.tick() => {
                let t = session.stats();
                *pub_stats.transport.lock().unwrap() = Some(serde_json::json!({
                    "rtt_ms": t.rtt.map(|d| d.as_secs_f64() * 1000.0),
                    "bytes_sent": t.bytes_sent,
                    "bytes_received": t.bytes_received,
                    "bytes_lost": t.bytes_lost,
                    "packets_sent": t.packets_sent,
                    "packets_received": t.packets_received,
                    "packets_lost": t.packets_lost,
                    "estimated_send_rate_mbps": t.estimated_send_rate.map(|r| r as f64 / 1e6),
                }));
            }
        }
    }

    // Clean up — managed mode only
    if let Some((ref push_key, _)) = managed_info {
        let client = reqwest::Client::new();
        let _ = client
            .delete(format!("{}/api/push/announce", args.worker_url))
            .json(&serde_json::json!({
                "push_key": push_key,
                "namespace": namespace,
            }))
            .send()
            .await;
        info!("Broadcast removed from directory");
    }

    info!("moqpush-app shutting down");
    Ok(())
}

/// Test mode: accept HTTP PUT/POST, print info about incoming data, drop it
async fn run_test_mode(port: u16) -> Result<()> {
    use bytes::BytesMut;
    use http_body_util::BodyExt;
    use hyper::body::Incoming;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Method, Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use std::net::SocketAddr;
    use tokio::net::TcpListener;

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;
    info!("TEST ingest server listening on http://0.0.0.0:{}", port);

    loop {
        let (stream, remote) = listener.accept().await?;
        let io = TokioIo::new(stream);

        tokio::spawn(async move {
            let service = service_fn(move |req: Request<Incoming>| {
                let remote = remote;
                async move {
                    if req.method() != Method::PUT && req.method() != Method::POST {
                        return Ok::<_, hyper::Error>(Response::builder()
                            .status(StatusCode::METHOD_NOT_ALLOWED)
                            .body("Method not allowed".to_string())
                            .unwrap());
                    }

                    let method = req.method().clone();
                    let path = req.uri().path().to_string();
                    let mut body = req.into_body();
                    let mut buf = BytesMut::new();

                    while let Some(frame) = body.frame().await {
                        if let Ok(f) = frame {
                            if let Some(chunk) = f.data_ref() {
                                buf.extend_from_slice(chunk);
                            }
                        }
                    }

                    let is_init = mp4::has_moov(&buf);
                    let is_media = mp4::has_moof(&buf);
                    let kind = if is_init { "INIT" } else if is_media { "MEDIA" } else { "OTHER" };

                    info!("TEST {} {} from {} — {} bytes [{}]", method, path, remote, buf.len(), kind);

                    if is_init {
                        if let Some(handler) = mp4::parse_handler_type(&buf) {
                            let codec = mp4::parse_codec_from_init(&buf).unwrap_or_default();
                            let timescale = mp4::parse_timescale(&buf).unwrap_or(0);
                            info!("  INIT: handler={} codec={} timescale={}", handler, codec, timescale);
                        }
                    }
                    if is_media {
                        if let Some(bdt) = mp4::parse_base_decode_time(&buf) {
                            let is_idr = mp4::fragment_starts_with_idr(&buf).unwrap_or(false);
                            info!("  MEDIA: bdt={} idr={}", bdt, is_idr);
                        }
                    }

                    Ok(Response::builder()
                        .status(StatusCode::OK)
                        .body("OK".to_string())
                        .unwrap())
                }
            });

            if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                if !e.is_incomplete_message() {
                    warn!("HTTP connection error: {}", e);
                }
            }
        });
    }
}

/// Push key heartbeat — renews lock every 10s
async fn run_push_heartbeat(
    worker_url: String,
    push_key: String,
    instance_id: String,
    shutdown_tx: watch::Sender<bool>,
) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("failed to build HTTP client");

    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;

        let result = client
            .post(format!("{}/api/push/heartbeat", worker_url))
            .json(&serde_json::json!({
                "push_key": push_key,
                "instance_id": instance_id,
            }))
            .send()
            .await;

        match result {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    info!("Heartbeat OK");
                } else if status.as_u16() == 401 || status.as_u16() == 409 {
                    error!("Heartbeat rejected ({}), shutting down", status);
                    let _ = shutdown_tx.send(true);
                    return;
                } else {
                    warn!("Heartbeat unexpected status {}", status);
                }
            }
            Err(e) => warn!("Heartbeat failed: {}", e),
        }
    }
}

/// Stats push (every 1s) + directory heartbeat (every 10s)
async fn run_stats_loop(
    worker_url: String,
    push_key: String,
    namespace: String,
    instance_id: String,
    stats: Arc<PublisherStats>,
) {
    use std::sync::atomic::Ordering::Relaxed;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("failed to build HTTP client");

    let start_time = std::time::Instant::now();
    let mut tick: u64 = 0;

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        tick += 1;

        let uptime = start_time.elapsed().as_secs();
        let video_codec = stats.video_codec.lock().unwrap().clone();
        let audio_codec = stats.audio_codec.lock().unwrap().clone();
        let catalog = stats.catalog_json.lock().unwrap().clone();
        let transport = stats.transport.lock().unwrap().clone();
        let video_structure = stats.video_structure.lock().unwrap().clone();

        // Push stats every second
        let _ = client
            .post(format!("{}/api/stats", worker_url))
            .json(&serde_json::json!({
                "push_key": push_key,
                "namespace": namespace,
                "role": "publisher",
                "uptime_secs": uptime,
                "tracks": stats.track_count.load(Relaxed),
                "bytes_published": stats.bytes_published.load(Relaxed),
                "frames_sent": stats.frames_sent.load(Relaxed),
                "segments_sent": stats.segments_sent.load(Relaxed),
                "video_width": stats.video_width.load(Relaxed),
                "video_height": stats.video_height.load(Relaxed),
                "video_codec": if video_codec.is_empty() { None } else { Some(video_codec) },
                "audio_codec": if audio_codec.is_empty() { None } else { Some(audio_codec) },
                "catalog": catalog,
                "transport": transport,
                "video_structure": video_structure.map(|vs| serde_json::json!({
                    "segment_duration_ms": vs.segment_duration_ms,
                    "fragments_per_segment": vs.fragments_per_segment,
                    "fragment_duration_ms": (vs.fragment_duration_ms * 100.0).round() / 100.0,
                    "fps": (vs.fps * 100.0).round() / 100.0,
                    "timescale": vs.timescale,
                    "default_sample_duration": vs.default_sample_duration,
                })),
            }))
            .send()
            .await;

        // Directory heartbeat every 10s
        if tick % 10 == 0 {
            let _ = client
                .post(format!("{}/api/push/heartbeat", worker_url))
                .json(&serde_json::json!({
                    "push_key": push_key,
                    "instance_id": instance_id,
                }))
                .send()
                .await;

            let _ = client
                .post(format!("{}/api/push/directory-heartbeat", worker_url))
                .json(&serde_json::json!({
                    "push_key": push_key,
                    "namespace": namespace,
                }))
                .send()
                .await;

            info!("Heartbeat OK (uptime: {}s)", uptime);
        }
    }
}

