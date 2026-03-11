use anyhow::Result;
use clap::Parser;
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Notify};
use tracing::{error, info, warn};

use moq_lite::Origin;
use moq_mux::CatalogProducer;

use moqcdn_ingest::heartbeat;
use moqcdn_ingest::http_ingest;
use moqcdn_ingest::publisher::Publisher;

/// Default Cloudflare MoQ relay (used for --no-auth or explicit override)
const DEFAULT_RELAY: &str = "https://draft-14.cloudflare.mediaoverquic.com";

#[derive(Parser, Debug)]
#[command(name = "moqpush-app")]
#[command(about = "MoQ push publisher — accepts HTTP CMAF-IF from encoder and publishes to Cloudflare relay")]
struct Args {
    /// Push key (from moqpush admin)
    #[arg(long, env = "MOQPUSH_KEY")]
    push_key: String,

    /// Worker URL for auth + heartbeat + stats + orchestration
    #[arg(long, default_value = "https://moqpush.com")]
    worker_url: String,

    /// Port for HTTP CMAF-IF ingest
    #[arg(long, default_value_t = 9078)]
    port: u16,

    /// Override relay URL (default: Cloudflare relay)
    #[arg(long)]
    relay_url: Option<String>,

    /// Disable TLS certificate verification (for local testing)
    #[arg(long)]
    tls_disable_verify: bool,

    /// Skip worker authentication (for local testing)
    #[arg(long)]
    no_auth: bool,

    /// Target latency in milliseconds for MSF catalog (default: 2000)
    #[arg(long)]
    target_latency: Option<u64>,
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

    let instance_id: String = {
        let mut rng = rand::rng();
        (0..16).map(|_| format!("{:x}", rng.random_range(0..16u8))).collect()
    };
    info!("Instance ID: {}", instance_id);

    // Authenticate with the moqpush worker
    let namespace: String;
    let relay_url: String;

    if !args.no_auth {
        info!("Authenticating with worker at {}...", args.worker_url);
        let client = reqwest::Client::new();

        let auth_resp = client
            .post(format!("{}/api/push/auth", args.worker_url))
            .json(&serde_json::json!({
                "push_key": args.push_key,
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
        namespace = auth_body["namespace"].as_str().unwrap_or("").to_string();
        // Use CLI override > worker DB relay_url > default
        relay_url = args.relay_url.unwrap_or_else(|| {
            auth_body["relay_url"]
                .as_str()
                .unwrap_or(DEFAULT_RELAY)
                .to_string()
        });

        info!("Authenticated: namespace='{}', relay='{}'", namespace, relay_url);
    } else {
        info!("Skipping worker auth (--no-auth)");
        namespace = "local-test".to_string();
        relay_url = args.relay_url.unwrap_or_else(|| DEFAULT_RELAY.to_string());
    }

    // Create shutdown channel
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    // Spawn push key heartbeat (lock renewal)
    if !args.no_auth {
        let hb_worker_url = args.worker_url.clone();
        let hb_key = args.push_key.clone();
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
    let publisher = Publisher::new(broadcast, catalog, namespace.clone())
        .with_target_latency(args.target_latency);

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

    // Wait for first init segment before connecting to relay
    info!("Waiting for first CMAF init segment...");
    tokio::time::timeout(
        Duration::from_secs(120),
        first_init_notify.notified(),
    )
    .await
    .map_err(|_| anyhow::anyhow!("Timeout: no init segment received within 120s"))?;

    // Connect to Cloudflare relay as publisher
    info!("First init received — connecting to Cloudflare relay at {}...", relay_url);

    let relay_url_parsed: url::Url = relay_url.parse()?;
    let mut client_config = moq_native::ClientConfig::default();
    if args.tls_disable_verify {
        client_config.tls.disable_verify = Some(true);
    }

    let client = client_config.init()?;
    let session = client
        .with_publish(origin.consume())
        .connect(relay_url_parsed.clone())
        .await?;

    info!("Connected to Cloudflare relay");

    // Announce broadcast to the worker
    if !args.no_auth {
        let dir_client = reqwest::Client::new();
        let announce_resp = dir_client
            .post(format!("{}/api/push/announce", args.worker_url))
            .json(&serde_json::json!({
                "push_key": args.push_key,
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

        // Spawn stats + heartbeat loop
        let hb_worker = args.worker_url.clone();
        let hb_key = args.push_key.clone();
        let hb_ns = namespace.clone();
        let hb_instance = instance_id.clone();
        tokio::spawn(async move {
            run_stats_loop(hb_worker, hb_key, hb_ns, hb_instance).await;
        });
    }

    // Run until session closes or shutdown
    tokio::select! {
        result = session.closed() => {
            match result {
                Ok(()) => info!("Relay session closed normally"),
                Err(e) => warn!("Relay session closed with error: {}", e),
            }
        }
        _ = shutdown_rx.changed() => {
            info!("Shutdown signal received");
        }
    }

    // Clean up
    if !args.no_auth {
        let client = reqwest::Client::new();
        let _ = client
            .delete(format!("{}/api/push/announce", args.worker_url))
            .json(&serde_json::json!({
                "push_key": args.push_key,
                "namespace": namespace,
            }))
            .send()
            .await;
        info!("Broadcast removed from directory");
    }

    info!("moqpush-app shutting down");
    Ok(())
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
) {
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

        // Push stats every second
        let _ = client
            .post(format!("{}/api/stats", worker_url))
            .json(&serde_json::json!({
                "push_key": push_key,
                "namespace": namespace,
                "role": "publisher",
                "uptime_secs": uptime,
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
