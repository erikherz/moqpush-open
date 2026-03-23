//! HTTP CMAF-IF ingest server.
//!
//! Accepts PUT/POST of init segments (.mp4/moov) and media segments (.m4s/moof+mdat).
//! Auto-detects tracks from content (muxed and separate init support).
//! Streams moof+mdat pairs immediately via chunked transfer encoding.

use anyhow::Result;
use bytes::BytesMut;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{watch, Mutex, Notify};
use tracing::{debug, error, info, warn};

use crate::mp4::{
    has_moof, has_moov, parse_handler_type, parse_moof_mdat_ranges,
    fragment_starts_with_idr, parse_base_decode_time,
};
use crate::publisher::Publisher;

/// Extract a URL stem for track routing.
/// Handles Ateme Titan URL patterns:
///   Init:  /ingest/Video1_2-init.mp4  → stem "Video1_2-"
///   Media: /ingest/Video1_2-885603205.mp4 → stem "Video1_2-"
///   Media: /ingest/Video1_2-000001.m4s → stem "Video1_2-"
fn extract_url_stem(path: &str) -> Option<String> {
    let filename = path.rsplit('/').next()?;
    // Init segments: strip "init.mp4" suffix
    if let Some(stem) = filename.strip_suffix("init.mp4") {
        if !stem.is_empty() {
            return Some(stem.to_string());
        }
    }
    // Media segments: strip .m4s or .mp4 extension, then trailing digits
    for ext in &[".m4s", ".mp4"] {
        if let Some(without_ext) = filename.strip_suffix(ext) {
            let stem = without_ext.trim_end_matches(|c: char| c.is_ascii_digit());
            if !stem.is_empty() {
                // Don't match bare "init" (already handled above)
                if stem == "init." || stem.ends_with("init") {
                    continue;
                }
                return Some(stem.to_string());
            }
        }
    }
    None
}

/// Track resolution: maps URL stems to our internal track names.
struct TrackResolver {
    url_stem_map: HashMap<String, String>,
    video_count: u32,
    audio_count: u32,
}

impl TrackResolver {
    fn new() -> Self {
        Self {
            url_stem_map: HashMap::new(),
            video_count: 0,
            audio_count: 0,
        }
    }

    fn assign_track_name(&mut self, handler_type: &str) -> String {
        match handler_type {
            "vide" => {
                self.video_count += 1;
                if self.video_count == 1 { "video".to_string() } else { format!("video-{}", self.video_count) }
            }
            _ => {
                self.audio_count += 1;
                if self.audio_count == 1 { "audio".to_string() } else { format!("audio-{}", self.audio_count) }
            }
        }
    }

    /// Returns (name, handler, is_new) — is_new=false means this track was already registered.
    fn register_init(&mut self, data: &[u8], url_path: &str) -> Option<(String, String, bool)> {
        let handler = parse_handler_type(data)?;
        let stem = extract_url_stem(url_path);

        // Dedup via URL stem
        if let Some(ref s) = stem {
            if let Some(name) = self.url_stem_map.get(s) {
                return Some((name.clone(), handler, false));
            }
        }

        let name = self.assign_track_name(&handler);
        if let Some(s) = stem {
            self.url_stem_map.insert(s, name.clone());
        }

        Some((name, handler, true))
    }

    /// Remap a placeholder track name to the real name assigned by the publisher/catalog.
    fn remap_track(&mut self, old_name: &str, new_name: &str) {
        if old_name == new_name {
            return;
        }
        for v in self.url_stem_map.values_mut() {
            if v == old_name {
                *v = new_name.to_string();
            }
        }
    }

    fn resolve_fragment(&mut self, _data: &[u8], url_path: &str) -> Option<String> {
        // Resolve by URL stem only — no track_id fallback.
        // track_id fallback is dangerous: multiple quality levels share track_id=1,
        // which would mix different resolutions into one track and corrupt the decoder.
        if let Some(stem) = extract_url_stem(url_path) {
            if let Some(track_name) = self.url_stem_map.get(&stem).cloned() {
                return Some(track_name);
            }
            // Stem extracted but not yet in map — init hasn't registered yet.
            // Drop the fragment; it can't be decoded without the init anyway.
            debug!("resolve: {} stem='{}' not registered yet, dropping", url_path, stem);
        } else {
            debug!("resolve: {} no stem extracted, dropping", url_path);
        }
        None
    }
}

type SharedState = Arc<Mutex<(TrackResolver, Publisher)>>;

async fn handle_request(
    req: Request<Incoming>,
    state: SharedState,
    first_init_notify: Arc<Notify>,
    remote_addr: SocketAddr,
) -> Result<Response<String>, hyper::Error> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    if method != Method::PUT && method != Method::POST {
        return Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body("Method not allowed".to_string())
            .unwrap());
    }

    // Discard manifests
    if path.ends_with(".mpd") {
        return Ok(Response::builder()
            .status(StatusCode::OK)
            .body("OK".to_string())
            .unwrap());
    }

    // Read body, detect content type, process
    let mut body = req.into_body();
    let mut buf = BytesMut::new();
    let mut is_init = false;
    let mut is_media = false;
    let mut track_name: Option<String> = None;
    let mut fragments_sent = 0u32;
    let segment_start = std::time::Instant::now();
    let mut first_bdt: Option<u64> = None;
    let mut last_bdt: Option<u64> = None;

    while let Some(frame_result) = body.frame().await {
        let frame = match frame_result {
            Ok(f) => f,
            Err(e) => {
                error!("Failed to read body frame: {}", e);
                break;
            }
        };

        if let Some(chunk) = frame.data_ref() {
            buf.extend_from_slice(chunk);
        } else {
            continue;
        }

        // Detect content type
        if !is_init && !is_media && buf.len() >= 8 {
            if has_moov(&buf) {
                is_init = true;
            } else if has_moof(&buf) {
                is_media = true;
                let mut guard = state.lock().await;
                let (ref mut resolver, ref mut publisher) = *guard;
                if let Some(name) = resolver.resolve_fragment(&buf, &path) {
                    // Signal new segment (HTTP PUT boundary) so publisher creates a new MoQ group
                    publisher.start_segment(&name);
                    track_name = Some(name);
                }
            }
        }

        // Stream complete moof+mdat pairs as they arrive.
        // Only process ranges where the full mdat is in the buffer — incomplete
        // pairs stay buffered until more HTTP body chunks arrive.
        if is_media && track_name.is_some() {
            let ranges = parse_moof_mdat_ranges(&buf).unwrap_or_default();
            let mut consumed = 0usize;
            if !ranges.is_empty() {
                let mut guard = state.lock().await;
                let (ref _resolver, ref mut publisher) = *guard;
                let tn = track_name.as_ref().unwrap();
                for range in &ranges {
                    let start = range.start as usize;
                    let end = range.end as usize + 1;
                    if end > buf.len() {
                        break; // mdat not fully received yet, wait for more data
                    }
                    let frag_data = &buf[start..end];
                    let is_idr = fragment_starts_with_idr(frag_data).unwrap_or(false);
                    let bdt = parse_base_decode_time(frag_data);
                    let elapsed_ms = segment_start.elapsed().as_millis();
                    debug!("FRAG {} track={} frag={} size={}B idr={} bdt={:?} elapsed={}ms",
                        path, tn, fragments_sent, frag_data.len(), is_idr, bdt, elapsed_ms);
                    // Track BDT range for accurate segment duration
                    if let Some(b) = bdt {
                        if first_bdt.is_none() { first_bdt = Some(b); }
                        last_bdt = Some(b);
                    }
                    // On first fragment, try to discover default_sample_duration from tfhd
                    if fragments_sent == 0 {
                        publisher.update_sample_duration_from_fragment(tn, frag_data);
                    }
                    if let Err(e) = publisher.send_fragment(tn, frag_data) {
                        warn!("Failed to send fragment: {}", e);
                    }
                    fragments_sent += 1;
                    consumed = end;
                }
            }
            if consumed > 0 {
                let _ = buf.split_to(consumed);
            }
        }
    }

    // Handle init segments (full body needed)
    if is_init {
        let data = buf.freeze();
        let mut guard = state.lock().await;
        let (ref mut resolver, ref mut publisher) = *guard;

        if let Some((placeholder_name, handler, is_new)) = resolver.register_init(&data, &path) {
            if is_new {
                match publisher.register_init(&handler, &data) {
                    Ok(real_name) => {
                        resolver.remap_track(&placeholder_name, &real_name);
                        info!("Registered init for track '{}' from {} (source: {})", real_name, path, remote_addr.ip());
                        if publisher.has_complete_catalog() {
                            publisher.publish_msf_catalog();
                            info!("Published MSF catalog ({} tracks) — source IP: {}", publisher.track_count(), remote_addr.ip());
                            first_init_notify.notify_one();
                        }
                    }
                    Err(e) => error!("Failed to register init: {}", e),
                }
            } else {
                debug!("Skipping duplicate init for '{}' from {}", placeholder_name, path);
            }
        }
    } else if is_media && fragments_sent > 0 {
        let seg_duration_ms = segment_start.elapsed().as_millis() as u64;
        debug!("SEGMENT_END {} track={} frags={} wall={}ms first_bdt={:?} last_bdt={:?}",
            path, track_name.as_deref().unwrap_or("?"), fragments_sent, seg_duration_ms, first_bdt, last_bdt);
        // Record video structure for stats reporting (using BDT-based duration)
        if let Some(ref tn) = track_name {
            let guard = state.lock().await;
            let (ref _resolver, ref publisher) = *guard;
            publisher.record_segment_structure(tn, fragments_sent, first_bdt, last_bdt);
        }
    } else if !is_init && !is_media && !buf.is_empty() {
        // Fallback: full-body detection
        let data = buf.freeze();
        let mut guard = state.lock().await;
        let (ref mut resolver, ref mut publisher) = *guard;

        if has_moov(&data) {
            if let Some((placeholder_name, handler, is_new)) = resolver.register_init(&data, &path) {
                if is_new {
                    match publisher.register_init(&handler, &data) {
                        Ok(real_name) => {
                            resolver.remap_track(&placeholder_name, &real_name);
                            info!("Registered init for track '{}' from {} (source: {})", real_name, path, remote_addr.ip());
                            if publisher.has_complete_catalog() {
                                publisher.publish_msf_catalog();
                                info!("Published MSF catalog ({} tracks) — source IP: {}", publisher.track_count(), remote_addr.ip());
                                first_init_notify.notify_one();
                            }
                        }
                        Err(e) => error!("Failed to register init: {}", e),
                    }
                } else {
                    debug!("Skipping duplicate init for '{}' from {}", placeholder_name, path);
                }
            }
        } else if has_moof(&data) {
            if let Some(tn) = resolver.resolve_fragment(&data, &path) {
                if let Err(e) = publisher.send_fragment(&tn, &data) {
                    warn!("Failed to send fragment: {}", e);
                }
            }
        }
    }

    Ok(Response::builder()
        .status(StatusCode::OK)
        .body("OK".to_string())
        .unwrap())
}

pub async fn run(
    port: u16,
    publisher: Publisher,
    first_init_notify: Arc<Notify>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(addr).await?;
    info!("CMAF-IF ingest server listening on http://0.0.0.0:{}", port);

    let resolver = TrackResolver::new();
    let state: SharedState = Arc::new(Mutex::new((resolver, publisher)));

    // Periodically re-publish the MSF catalog so late subscribers (NextGroup filter) get it
    let catalog_state = state.clone();
    let mut catalog_shutdown = shutdown.clone();
    tokio::spawn(async move {
        // Wait briefly for tracks to register before starting catalog heartbeat
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        loop {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {
                    let mut guard = catalog_state.lock().await;
                    let (ref _resolver, ref mut publisher) = *guard;
                    if publisher.has_complete_catalog() {
                        publisher.publish_msf_catalog();
                        debug!("Catalog heartbeat: republished MSF catalog ({} tracks)", publisher.track_count());
                    }
                }
                _ = catalog_shutdown.changed() => break,
            }
        }
    });

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (stream, remote_addr) = accept_result?;
                let io = TokioIo::new(stream);
                let state = state.clone();
                let notify = first_init_notify.clone();

                tokio::spawn(async move {
                    let service = service_fn(move |req| {
                        let state = state.clone();
                        let notify = notify.clone();
                        async move { handle_request(req, state, notify, remote_addr).await }
                    });

                    if let Err(e) = http1::Builder::new()
                        .serve_connection(io, service)
                        .await
                    {
                        if !e.is_incomplete_message() {
                            warn!("HTTP connection error: {}", e);
                        }
                    }
                });
            }
            _ = shutdown.changed() => {
                info!("Shutdown signal received, stopping ingest server");
                break;
            }
        }
    }

    Ok(())
}
