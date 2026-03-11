//! moqpush-puller — MoQ pull server for stream pre-positioning.
//!
//! Runs on each moqpush edge server (naw.moqpush.com, nac.moqpush.com, etc.).
//! Connects to the moqpush worker via WebSocket for real-time orchestration.
//! When told to pull: connects to Cloudflare relay as subscriber, drains all
//! tracks to keep Cloudflare's edge warm for that region.

use anyhow::Result;
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
#[command(name = "moqpush-puller")]
#[command(about = "MoQ pull server — pre-positions streams by pulling through Cloudflare relay")]
struct Args {
    /// Node name (e.g. "naw", "nac", "nae", "eu1", "in1")
    #[arg(long, env = "MOQPUSH_NODE")]
    node: String,

    /// Region label (e.g. "us-west", "us-central", "us-east", "eu-west", "ap-south")
    #[arg(long, env = "MOQPUSH_REGION")]
    region: String,

    /// Worker URL for WebSocket orchestration
    #[arg(long, default_value = "https://moqpush.com")]
    worker_url: String,

    /// Port for health check HTTP API
    #[arg(long, default_value_t = 8080)]
    port: u16,

    /// Disable TLS certificate verification
    #[arg(long)]
    tls_disable_verify: bool,

    /// Puller secret (shared with worker for authentication)
    #[arg(long, env = "MOQPUSH_PULLER_SECRET")]
    secret: Option<String>,
}

/// Active pull session for a namespace
struct PullSession {
    handle: JoinHandle<()>,
}

type ActivePulls = Arc<RwLock<HashMap<String, PullSession>>>;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("moqpush-puller v{}", env!("CARGO_PKG_VERSION"));

    let args = Args::parse();
    info!("Node: {} ({})", args.node, args.region);

    let active_pulls: ActivePulls = Arc::new(RwLock::new(HashMap::new()));

    // Spawn health check HTTP server
    let health_pulls = active_pulls.clone();
    let port = args.port;
    tokio::spawn(async move {
        if let Err(e) = run_health_server(port, health_pulls).await {
            error!("Health server error: {}", e);
        }
    });

    // Main loop: connect to worker WebSocket, process commands, reconnect on failure
    let tls_disable_verify = args.tls_disable_verify;
    loop {
        info!("Connecting to worker WebSocket...");
        match run_ws_session(&args, active_pulls.clone(), tls_disable_verify).await {
            Ok(()) => info!("WebSocket session closed normally"),
            Err(e) => warn!("WebSocket session error: {}", e),
        }

        // Stop all active pulls on disconnect
        {
            let mut pulls = active_pulls.write().await;
            for (ns, session) in pulls.drain() {
                info!("Stopping pull for '{}' (disconnected)", ns);
                session.handle.abort();
            }
        }

        info!("Reconnecting in 3s...");
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}

async fn run_ws_session(
    args: &Args,
    active_pulls: ActivePulls,
    tls_disable_verify: bool,
) -> Result<()> {
    // Build WebSocket URL
    let ws_base = args.worker_url
        .replace("https://", "wss://")
        .replace("http://", "ws://");

    let mut ws_url = format!(
        "{}/api/pullers/ws?node={}&region={}",
        ws_base.trim_end_matches('/'),
        args.node,
        args.region,
    );
    if let Some(ref secret) = args.secret {
        ws_url.push_str(&format!("&secret={}", secret));
    }

    let (ws_stream, _) = tokio_tungstenite::connect_async(&ws_url).await?;
    info!("Connected to worker WebSocket");

    let (mut ws_write, mut ws_read) = ws_stream.split();

    while let Some(msg) = ws_read.next().await {
        let msg = msg?;
        match msg {
            Message::Text(text) => {
                let data: serde_json::Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                let msg_type = data["type"].as_str().unwrap_or("");

                match msg_type {
                    "pull" => {
                        let namespace = data["namespace"].as_str().unwrap_or("").to_string();
                        let relay_url = data["relay_url"].as_str().unwrap_or("").to_string();

                        if namespace.is_empty() || relay_url.is_empty() {
                            warn!("Invalid pull command: missing namespace or relay_url");
                            continue;
                        }

                        // Check if already pulling
                        {
                            let pulls = active_pulls.read().await;
                            if pulls.contains_key(&namespace) {
                                info!("Already pulling '{}'", namespace);
                                continue;
                            }
                        }

                        info!("Pull command: '{}' via {}", namespace, relay_url);
                        let ns = namespace.clone();
                        let pulls = active_pulls.clone();
                        let tls_dv = tls_disable_verify;

                        let handle = tokio::spawn(async move {
                            match run_pull_session(&relay_url, &ns, tls_dv).await {
                                Ok(()) => info!("Pull session for '{}' ended normally", ns),
                                Err(e) => warn!("Pull session for '{}' error: {}", ns, e),
                            }
                            pulls.write().await.remove(&ns);
                        });

                        active_pulls.write().await.insert(
                            namespace.clone(),
                            PullSession { handle },
                        );

                        // Report warm status
                        let warm_msg = serde_json::json!({
                            "type": "warm",
                            "namespace": namespace,
                        });
                        let _ = ws_write.send(Message::Text(warm_msg.to_string())).await;
                    }

                    "stop" => {
                        let namespace = data["namespace"].as_str().unwrap_or("").to_string();
                        if namespace.is_empty() {
                            continue;
                        }

                        info!("Stop command: '{}'", namespace);
                        let removed = active_pulls.write().await.remove(&namespace);
                        if let Some(session) = removed {
                            session.handle.abort();
                        }

                        let cold_msg = serde_json::json!({
                            "type": "cold",
                            "namespace": namespace,
                        });
                        let _ = ws_write.send(Message::Text(cold_msg.to_string())).await;
                    }

                    "ping" => {
                        let pong = serde_json::json!({ "type": "pong" });
                        let _ = ws_write.send(Message::Text(pong.to_string())).await;
                    }

                    _ => {}
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    Ok(())
}

/// Connect to Cloudflare relay and drain a namespace (keeps stream flowing)
async fn run_pull_session(relay_url: &str, namespace: &str, tls_disable_verify: bool) -> Result<()> {
    info!("Connecting to {} to pull '{}'...", relay_url, namespace);

    let origin = moq_lite::Origin::produce();
    let consumer = origin.consume();

    let mut client_config = moq_native::ClientConfig::default();
    if tls_disable_verify {
        client_config.tls.disable_verify = Some(true);
    }

    let client = client_config.init()?;

    let session = client
        .with_consume(origin)
        .connect(relay_url.parse()?)
        .await?;

    info!("Connected to relay, subscribing to '{}'", namespace);

    let mut announcements = consumer;
    let _broadcast = loop {
        match announcements.announced().await {
            Some((path, Some(bc))) => {
                let announced = path.as_str();
                if announced == namespace || announced.starts_with(&format!("{}/", namespace)) {
                    info!("Got broadcast for '{}'", announced);
                    break bc;
                }
            }
            Some((_, None)) => continue,
            None => return Err(anyhow::anyhow!("no announcements received")),
        }
    };

    info!("Draining all tracks for '{}'...", namespace);
    session.closed().await?;
    Ok(())
}

async fn run_health_server(port: u16, pulls: ActivePulls) -> Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;
    info!("Health API listening on http://0.0.0.0:{}", port);

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let pulls = pulls.clone();

        tokio::spawn(async move {
            let service = service_fn(move |_req: Request<hyper::body::Incoming>| {
                let pulls = pulls.clone();
                async move {
                    let active = pulls.read().await;
                    let namespaces: Vec<&str> = active.keys().map(|s| s.as_str()).collect();
                    let body = serde_json::json!({
                        "status": "ok",
                        "active_pulls": namespaces.len(),
                        "namespaces": namespaces,
                    });
                    Ok::<_, hyper::Error>(Response::builder()
                        .status(StatusCode::OK)
                        .header("Content-Type", "application/json")
                        .body(Full::new(Bytes::from(body.to_string())))
                        .unwrap())
                }
            });

            if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                if !e.is_incomplete_message() {
                    warn!("HTTP error: {}", e);
                }
            }
        });
    }
}
