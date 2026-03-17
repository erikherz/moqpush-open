//! MoQ publisher: wraps moq-lite content model (Broadcast/Track/Group/Catalog).
//! Cherry-picked from wowza-sender/src/publisher.rs, adapted for moqcdn-ingest.
//!
//! register_init() creates tracks + updates hang/MSF catalogs
//! send_fragment() writes moof+mdat frames to track groups

use anyhow::{anyhow, Result};
use base64::Engine;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicU32, Ordering};
use tracing::{debug, info, warn};

use moq_lite::{BroadcastProducer, Group, GroupProducer, Track, TrackProducer};
use moq_mux::CatalogProducer;

use crate::ad_manager::Ad;
use crate::mp4;

/// Ad insertion state.
#[derive(Debug, Clone, PartialEq)]
enum AdState {
    Live,
    PlayingAd,
}

/// Video structure snapshot from the most recent segment.
#[derive(Clone, Default)]
pub struct VideoStructure {
    pub segment_duration_ms: u64,
    pub fragments_per_segment: u32,
    pub fragment_duration_ms: f64,
    pub fps: f64,
    pub timescale: u32,
    pub default_sample_duration: Option<u32>,
}

/// Shared publisher stats readable from the stats loop via Arc.
pub struct PublisherStats {
    pub bytes_published: AtomicU64,
    pub frames_sent: AtomicU64,
    pub segments_sent: AtomicU64,
    pub track_count: AtomicU32,
    pub video_width: AtomicU32,
    pub video_height: AtomicU32,
    pub video_codec: std::sync::Mutex<String>,
    pub audio_codec: std::sync::Mutex<String>,
    /// Latest catalog JSON with initData stripped out.
    pub catalog_json: std::sync::Mutex<Option<serde_json::Value>>,
    /// Transport-level stats (QUIC/WebTransport), updated periodically.
    pub transport: std::sync::Mutex<Option<serde_json::Value>>,
    /// Video structure from the latest completed segment.
    pub video_structure: std::sync::Mutex<Option<VideoStructure>>,
}

impl PublisherStats {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            bytes_published: AtomicU64::new(0),
            frames_sent: AtomicU64::new(0),
            segments_sent: AtomicU64::new(0),
            track_count: AtomicU32::new(0),
            video_width: AtomicU32::new(0),
            video_height: AtomicU32::new(0),
            video_codec: std::sync::Mutex::new(String::new()),
            audio_codec: std::sync::Mutex::new(String::new()),
            catalog_json: std::sync::Mutex::new(None),
            transport: std::sync::Mutex::new(None),
            video_structure: std::sync::Mutex::new(None),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrackType {
    Video,
    Audio,
}

struct TrackState {
    track: TrackProducer,
    timescale: u32,
    default_sample_duration: Option<u32>,
    group: Option<GroupProducer>,
    /// First baseMediaDecodeTime seen for this track (used to rebase to 0).
    time_base: Option<u64>,
    track_type: TrackType,
    /// Signal from HTTP ingest: new segment (HTTP PUT) started, force new group.
    new_segment: bool,
    /// Last baseMediaDecodeTime seen (pre-rebase), for ad timestamp continuity.
    last_bdt: Option<u64>,
    /// Last fragment's duration in timescale ticks (estimated from BDT deltas).
    last_frag_duration: Option<u64>,
}

/// Shared time origin across all tracks for synchronized rebasing.
/// When the first track's BDT arrives, we record (bdt, timescale) as the
/// wall-clock reference. All other tracks compute their time_base relative
/// to this same origin, ensuring audio and video timestamps stay in sync.
struct SharedTimeOrigin {
    /// Wall-clock time (seconds) of the first BDT seen across all tracks.
    origin_seconds: f64,
}

pub struct Publisher {
    broadcast: BroadcastProducer,
    catalog: CatalogProducer,
    tracks: HashMap<String, TrackState>,
    init_segments: HashMap<String, Vec<u8>>,
    video_count: u32,
    audio_count: u32,
    /// CMSF SAP-type event timeline track (one shared across all media tracks).
    sap_track: Option<TrackProducer>,
    sap_group: Option<GroupProducer>,
    catalog_last_logged: Option<std::time::Instant>,
    /// Shared time origin for synchronized A/V timestamp rebasing.
    time_origin: Option<SharedTimeOrigin>,
    /// Target latency in ms for MSF catalog (default 2000).
    target_latency_ms: Option<u64>,
    /// When the first video track was registered (for audio-wait timeout).
    first_video_at: Option<std::time::Instant>,
    /// Expected track counts from --tracks flag (e.g. 3v1a).
    expected_video: Option<u32>,
    expected_audio: Option<u32>,
    /// Shared stats counters readable from the stats loop.
    pub stats: Arc<PublisherStats>,
    /// Ad insertion state.
    ad_state: AdState,
}

impl Publisher {
    pub fn new(broadcast: BroadcastProducer, catalog: CatalogProducer, stats: Arc<PublisherStats>) -> Self {
        Self {
            broadcast,
            catalog,
            tracks: HashMap::new(),
            init_segments: HashMap::new(),
            video_count: 0,
            audio_count: 0,
            sap_track: None,
            sap_group: None,
            catalog_last_logged: None,
            time_origin: None,
            target_latency_ms: None,
            first_video_at: None,
            expected_video: None,
            expected_audio: None,
            stats,
            ad_state: AdState::Live,
        }
    }

    pub fn set_target_latency_ms(&mut self, ms: u64) {
        self.target_latency_ms = Some(ms);
    }

    pub fn set_expected_tracks(&mut self, video: u32, audio: u32) {
        self.expected_video = Some(video);
        self.expected_audio = Some(audio);
    }

    pub fn register_init(&mut self, handler_type: &str, init_data: &[u8]) -> Result<String> {
        let track_type = match handler_type {
            "vide" => TrackType::Video,
            "soun" => TrackType::Audio,
            other => return Err(anyhow!("unknown handler type: {}", other)),
        };

        let codec_str = mp4::parse_codec_from_init(init_data)
            .unwrap_or_else(|| if track_type == TrackType::Video { "avc1".to_string() } else { "mp4a.40.2".to_string() });
        let timescale = mp4::parse_timescale(init_data).unwrap_or(90000);
        let track_id = mp4::parse_track_id_from_init(init_data).unwrap_or(1);
        let default_sample_duration = mp4::extract_default_sample_duration(init_data)
            .filter(|&dur| {
                // Reject unreasonable values: if default_sample_duration >= timescale,
                // it means ≥1 second per sample (e.g. Ateme trex placeholder of 30000
                // at timescale=30000). Injecting this would corrupt timestamps.
                if dur >= timescale {
                    info!("Ignoring trex default_sample_duration={} (>= timescale={}), likely placeholder", dur, timescale);
                    false
                } else {
                    true
                }
            });

        let track_name = match track_type {
            TrackType::Video => {
                let description = mp4::extract_avcc_bytes(init_data);
                let (width, height) = mp4::extract_video_dimensions(init_data).unwrap_or((0, 0));

                let video_codec: hang::catalog::VideoCodec = codec_str.parse()
                    .unwrap_or(hang::catalog::VideoCodec::Unknown(codec_str.clone()));

                let config = hang::catalog::VideoConfig {
                    codec: video_codec,
                    description: description.map(Bytes::from),
                    coded_width: if width > 0 { Some(width) } else { None },
                    coded_height: if height > 0 { Some(height) } else { None },
                    display_ratio_width: None,
                    display_ratio_height: None,
                    bitrate: None,
                    framerate: None,
                    optimize_for_latency: Some(true),
                    container: hang::catalog::Container::Cmaf {
                        timescale: timescale as u64,
                        track_id,
                    },
                    jitter: None,
                };

                let mut cat = self.catalog.lock();
                let track_info: Track = cat.video.create_track("m4s", config);
                let name = track_info.name.clone();

                let track_producer = self.broadcast.create_track(track_info)
                    .map_err(|e| anyhow!("failed to create video track: {}", e))?;

                self.video_count += 1;
                if self.first_video_at.is_none() {
                    self.first_video_at = Some(std::time::Instant::now());
                }
                self.init_segments.insert(name.clone(), init_data.to_vec());
                info!("Registered video track '{}' codec={} {}x{} timescale={} default_sample_duration={:?}",
                    name, codec_str, width, height, timescale, default_sample_duration);

                // Update shared stats
                self.stats.video_width.store(width, Ordering::Relaxed);
                self.stats.video_height.store(height, Ordering::Relaxed);
                *self.stats.video_codec.lock().unwrap() = codec_str.clone();

                self.tracks.insert(name.clone(), TrackState {
                    track: track_producer,
                    timescale,
                    default_sample_duration,
                    group: None,
                    time_base: None,
                    track_type: TrackType::Video,
                    new_segment: false,
                    last_bdt: None,
                    last_frag_duration: None,
                });

                // Create SAP event timeline track on first video track
                if self.sap_track.is_none() {
                    let sap_info = Track::new("sap-timeline");
                    match self.broadcast.create_track(sap_info) {
                        Ok(producer) => {
                            info!("Created CMSF SAP event timeline track");
                            self.sap_track = Some(producer);
                        }
                        Err(e) => info!("Failed to create SAP timeline track: {}", e),
                    }
                }

                name
            }
            TrackType::Audio => {
                let description = mp4::extract_esds_bytes(init_data);
                let sample_rate = mp4::extract_audio_sample_rate(init_data).unwrap_or(48000);
                let channels = mp4::extract_audio_channels(init_data).unwrap_or(2);

                let audio_codec: hang::catalog::AudioCodec = codec_str.parse()
                    .unwrap_or(hang::catalog::AudioCodec::Unknown(codec_str.clone()));

                let config = hang::catalog::AudioConfig {
                    codec: audio_codec,
                    description: description.map(Bytes::from),
                    sample_rate,
                    channel_count: channels as u32,
                    bitrate: None,
                    container: hang::catalog::Container::Cmaf {
                        timescale: timescale as u64,
                        track_id,
                    },
                    jitter: None,
                };

                let mut cat = self.catalog.lock();
                let track_info: Track = cat.audio.create_track("m4s", config);
                let name = track_info.name.clone();

                let track_producer = self.broadcast.create_track(track_info)
                    .map_err(|e| anyhow!("failed to create audio track: {}", e))?;

                self.audio_count += 1;
                self.init_segments.insert(name.clone(), init_data.to_vec());
                info!("Registered audio track '{}' codec={} sr={} ch={} timescale={} default_sample_duration={:?}",
                    name, codec_str, sample_rate, channels, timescale, default_sample_duration);

                // Update shared stats
                *self.stats.audio_codec.lock().unwrap() = codec_str.clone();

                self.tracks.insert(name.clone(), TrackState {
                    track: track_producer,
                    timescale,
                    default_sample_duration,
                    group: None,
                    time_base: None,
                    track_type: TrackType::Audio,
                    new_segment: false,
                    last_bdt: None,
                    last_frag_duration: None,
                });

                name
            }
        };

        self.stats.track_count.store(self.tracks.len() as u32, Ordering::Relaxed);

        // Only publish catalog once we have all expected tracks.
        if self.has_complete_catalog() {
            self.publish_msf_catalog();
        } else {
            let expected = match (self.expected_video, self.expected_audio) {
                (Some(ev), Some(ea)) => format!("expected {}v{}a", ev, ea),
                _ => "need video+audio".to_string(),
            };
            info!("Deferring catalog publish ({}, have {}v{}a)",
                expected, self.video_count, self.audio_count);
        }

        Ok(track_name)
    }

    pub fn publish_msf_catalog(&mut self) {
        let cat = self.catalog.lock();
        let mut msf = moq_mux::msf::to_msf_with_namespace(&cat, None);
        drop(cat);

        // Set target latency at track level per MSF draft-00 §5.1.16
        if let Some(latency) = self.target_latency_ms {
            for track in &mut msf.tracks {
                track.target_latency = Some(latency);
            }
        }

        let b64 = base64::engine::general_purpose::STANDARD;
        for track in &mut msf.tracks {
            if let Some(init) = self.init_segments.get(&track.name) {
                track.init_data = Some(b64.encode(init));
            }
        }

        // Add CMSF SAP event timeline track to catalog if created
        if self.sap_track.is_some() {
            msf.tracks.push(moq_mux::msf::sap_timeline_track("sap-timeline", None));
        }

        match msf.to_string() {
            Ok(json) => {
                let json_len = json.len();

                // Store catalog snapshot (without initData) in shared stats
                if let Ok(mut val) = serde_json::from_str::<serde_json::Value>(&json) {
                    if let Some(tracks) = val.get_mut("tracks").and_then(|t| t.as_array_mut()) {
                        for track in tracks.iter_mut() {
                            if let Some(obj) = track.as_object_mut() {
                                obj.remove("initData");
                            }
                        }
                    }
                    *self.stats.catalog_json.lock().unwrap() = Some(val);
                }

                // Log full catalog JSON periodically for debugging
                let should_log = self.catalog_last_logged
                    .map(|t| t.elapsed().as_secs() >= 60)
                    .unwrap_or(true);
                if should_log {
                    info!("MSF catalog JSON: {}", json);
                    self.catalog_last_logged = Some(std::time::Instant::now());
                }
                match self.catalog.msf_track.append_group() {
                    Ok(mut group) => {
                        match group.write_frame(json) {
                            Ok(_) => debug!("MSF catalog published ({} bytes, {} tracks)", json_len, msf.tracks.len()),
                            Err(e) => info!("MSF catalog write_frame failed: {}", e),
                        }
                        let _ = group.finish();
                    }
                    Err(e) => info!("MSF catalog append_group failed: {}", e),
                }
            }
            Err(e) => info!("MSF catalog to_string failed: {}", e),
        }
    }

    pub fn send_fragment(
        &mut self,
        track_name: &str,
        data: &[u8],
    ) -> Result<()> {
        // Drop live fragments while ad is playing
        if self.ad_state == AdState::PlayingAd {
            return Ok(());
        }

        let state = self.tracks.get_mut(track_name)
            .ok_or_else(|| anyhow!("track not found: {}", track_name))?;

        let bdt = mp4::parse_base_decode_time(data);
        let timescale = state.timescale;
        let track_type = state.track_type;

        // Track last BDT for ad insertion timestamp continuity
        if let Some(bdt_val) = bdt {
            if let Some(prev) = state.last_bdt {
                if bdt_val > prev {
                    state.last_frag_duration = Some(bdt_val - prev);
                }
            }
            state.last_bdt = Some(bdt_val);
        }

        // Determine whether to start a new group.
        // Groups are aligned with HTTP PUT boundaries (segments from the encoder).
        // http_ingest calls start_segment() at the start of each PUT, which sets
        // new_segment=true. We create a new MoQ group on that first fragment.
        let need_new_group = if state.group.is_none() {
            true
        } else {
            state.new_segment
        };
        let is_idr = need_new_group && track_type == TrackType::Video;
        if state.new_segment {
            state.new_segment = false;
        }

        if need_new_group {
            let first_group = state.group.is_none();
            if let Some(mut group) = state.group.take() {
                let _ = group.finish();
            }
            let group = if first_group {
                // MSF draft-00: first group ID = milliseconds since Unix epoch
                let epoch_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                state.track.create_group(Group { sequence: epoch_ms })
                    .map_err(|e| anyhow!("failed to create first group: {}", e))?
            } else {
                state.track.append_group()
                    .map_err(|e| anyhow!("failed to create group: {}", e))?
            };
            state.group = Some(group);
        }

        // Rebase timestamps: subtract a synchronized base so audio and video start near 0
        // with matching wall-clock alignment. The first BDT from ANY track establishes a
        // shared time origin; all other tracks compute their base from the same origin.
        let working_data = if let Some(bdt_val) = bdt {
            if state.time_base.is_none() {
                let bdt_seconds = bdt_val as f64 / timescale as f64;
                let base = if let Some(ref origin) = self.time_origin {
                    // Compute this track's base so its rebased time aligns with the origin.
                    let rebased_seconds = bdt_seconds - origin.origin_seconds;
                    let rebased_ticks = (rebased_seconds * timescale as f64).round() as i64;
                    bdt_val.saturating_sub(rebased_ticks.max(0) as u64)
                } else {
                    // First track to arrive: this BDT becomes the wall-clock origin.
                    self.time_origin = Some(SharedTimeOrigin {
                        origin_seconds: bdt_seconds,
                    });
                    bdt_val
                };
                state.time_base = Some(base);
            }
            let base = state.time_base.unwrap();

            // Log at group boundaries (after time_base is computed so values are accurate)
            if need_new_group {
                let rebased_ms = ((bdt_val.saturating_sub(base)) * 1000) / timescale as u64;
                let bdt_ms = (bdt_val * 1000) / timescale as u64;
                let base_ms = (base * 1000) / timescale as u64;
                debug!("{} '{}': NEW_GROUP bdt={}ms base={}ms rebased={}ms",
                    if track_type == TrackType::Video { "Video" } else { "Audio" },
                    track_name, bdt_ms, base_ms, rebased_ms);
            }

            mp4::rebase_decode_time(data, base)
        } else {
            data.to_vec()
        };

        let frame_data = if let Some(dur) = state.default_sample_duration {
            Bytes::from(mp4::inject_trun_duration(&working_data, dur))
        } else {
            Bytes::from(working_data)
        };

        if let Some(ref mut group) = state.group {
            let len = frame_data.len() as u64;
            group.write_frame(frame_data)
                .map_err(|e| anyhow!("failed to write frame: {}", e))?;
            self.stats.bytes_published.fetch_add(len, Ordering::Relaxed);
            self.stats.frames_sent.fetch_add(1, Ordering::Relaxed);
        }

        // Emit CMSF SAP event for video fragments
        if track_type == TrackType::Video {
            if let Some(ref mut sap_track) = self.sap_track {
                let sap_type: u32 = if is_idr { 1 } else { 0 };
                let ept_ms = if let Some(bdt_val) = bdt {
                    let base = self.tracks.get(track_name)
                        .and_then(|s| s.time_base)
                        .unwrap_or(0);
                    let rebased = bdt_val.saturating_sub(base);
                    (rebased * 1000) / timescale as u64
                } else {
                    0
                };

                let json = moq_mux::msf::sap_event_json(sap_type, ept_ms);

                // New group on IDR (aligns with media group boundaries)
                if is_idr {
                    if let Some(mut g) = self.sap_group.take() {
                        let _ = g.finish();
                    }
                    match sap_track.append_group() {
                        Ok(g) => self.sap_group = Some(g),
                        Err(e) => debug!("SAP timeline append_group failed: {}", e),
                    }
                }

                if let Some(ref mut group) = self.sap_group {
                    if let Err(e) = group.write_frame(Bytes::from(json)) {
                        debug!("SAP timeline write_frame failed: {}", e);
                    }
                }
            }
        }

        Ok(())
    }

    /// Signal that a new HTTP PUT (segment) has started for this track.
    /// The next call to send_fragment() will create a new MoQ group.
    pub fn start_segment(&mut self, track_name: &str) {
        if self.ad_state == AdState::PlayingAd {
            return; // Drop live segments during ad playback
        }
        if let Some(state) = self.tracks.get_mut(track_name) {
            state.new_segment = true;
            self.stats.segments_sent.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Update default_sample_duration from a tfhd box in a moof fragment.
    /// Called on first fragment when the trex value was rejected as a placeholder.
    pub fn update_sample_duration_from_fragment(&mut self, track_name: &str, data: &[u8]) {
        let state = match self.tracks.get_mut(track_name) {
            Some(s) => s,
            None => return,
        };
        // Only update if we don't already have a valid value
        if state.default_sample_duration.is_some() {
            return;
        }
        if let Some(dur) = mp4::parse_tfhd_sample_duration(data) {
            info!("Discovered default_sample_duration={} from tfhd for track '{}' (timescale={})",
                dur, track_name, state.timescale);
            state.default_sample_duration = Some(dur);
        }
    }

    /// Record video structure from a completed segment.
    /// `first_bdt` and `last_bdt` are the baseMediaDecodeTime of the first and last
    /// fragments, used to compute accurate media-time segment duration.
    pub fn record_segment_structure(
        &self,
        track_name: &str,
        fragment_count: u32,
        first_bdt: Option<u64>,
        last_bdt: Option<u64>,
    ) {
        // Only track video segments
        let state = match self.tracks.get(track_name) {
            Some(s) if s.track_type == TrackType::Video => s,
            _ => return,
        };

        let timescale = state.timescale;

        // Compute FPS from timescale and default_sample_duration
        let (fps, sample_dur) = if let Some(dur) = state.default_sample_duration {
            if dur > 0 { (timescale as f64 / dur as f64, Some(dur)) } else { (0.0, None) }
        } else {
            (0.0, None)
        };

        // Compute segment duration from BDT span + one fragment duration
        // first_bdt..last_bdt covers (fragment_count - 1) fragments,
        // so total duration = (last_bdt - first_bdt + fragment_dur) / timescale * 1000
        let segment_duration_ms = if let (Some(first), Some(last)) = (first_bdt, last_bdt) {
            if last >= first && timescale > 0 {
                let bdt_span_ticks = last - first;
                // Add one fragment's worth of ticks to cover the last fragment's duration
                let frag_ticks = if fragment_count > 1 {
                    bdt_span_ticks / (fragment_count as u64 - 1)
                } else if let Some(dur) = sample_dur {
                    // Single fragment: estimate from sample duration × samples
                    // For now just use the BDT span (which is 0 for 1 fragment)
                    dur as u64
                } else {
                    0
                };
                let total_ticks = bdt_span_ticks + frag_ticks;
                (total_ticks * 1000) / timescale as u64
            } else {
                0
            }
        } else {
            0
        };

        let fragment_duration_ms = if fragment_count > 0 && segment_duration_ms > 0 {
            segment_duration_ms as f64 / fragment_count as f64
        } else {
            0.0
        };

        let vs = VideoStructure {
            segment_duration_ms,
            fragments_per_segment: fragment_count,
            fragment_duration_ms,
            fps,
            timescale,
            default_sample_duration: state.default_sample_duration,
        };

        *self.stats.video_structure.lock().unwrap() = Some(vs);
    }

    pub fn track_count(&self) -> usize {
        self.tracks.len()
    }

    /// Returns true once all expected tracks have been registered.
    /// With --tracks, waits for the exact counts (e.g. 3v1a).
    /// Without --tracks, waits for any video + audio, or video-only after 5s timeout.
    pub fn has_complete_catalog(&self) -> bool {
        if let (Some(ev), Some(ea)) = (self.expected_video, self.expected_audio) {
            let video_ready = self.video_count >= ev;
            let audio_ready = if ea > 0 { self.audio_count >= ea } else { true };
            return video_ready && audio_ready;
        }

        // Default behavior: any video + any audio
        if self.video_count > 0 && self.audio_count > 0 {
            return true;
        }
        // Timeout: publish video-only catalog if no audio after 5s
        if self.video_count > 0 {
            if let Some(t) = self.first_video_at {
                return t.elapsed() > std::time::Duration::from_secs(5);
            }
        }
        false
    }

    /// Prepare ad for insertion: match tracks, compute offsets, build timeslots.
    ///
    /// Ad init segments are sent as frames on the data stream (not via catalog),
    /// so the player's moov detection in _onFrame handles codec switching.
    /// This works with any ad content regardless of encoder.
    ///
    /// Returns (timeslots, pace_ms) — prepared fragment data grouped by timeslot,
    /// and the recommended pacing interval in milliseconds between slots.
    /// Timeslot 0 contains ad init segments (moov) for each track.
    pub fn prepare_ad(&mut self, ad: &Ad) -> Result<(Vec<Vec<(String, Bytes)>>, u64)> {
        info!("AD_INSERT: preparing ad '{}' (video_tracks={}, audio_frags={})",
            ad.name, ad.video_tracks.len(), ad.audio_fragments.len());

        // Compute BDT offsets in seconds (f64) for timescale-independent continuity.
        // These will be converted to ad timescale when building each slot.
        let mut bdt_offset_secs: HashMap<String, f64> = HashMap::new();
        for (name, state) in self.tracks.iter() {
            let last = state.last_bdt.unwrap_or(0);
            let dur = state.last_frag_duration.unwrap_or(0);
            let base = state.time_base.unwrap_or(0);
            let offset_ticks = (last + dur).saturating_sub(base);
            let ts = state.timescale.max(1) as f64;
            let offset_secs = offset_ticks as f64 / ts;
            bdt_offset_secs.insert(name.clone(), offset_secs);
            info!("AD_INSERT: {} offset={:.3}s (last_bdt={}, dur={}, base={}, timescale={})",
                name, offset_secs, last, dur, base, state.timescale);
        }

        // Match ad video tracks to publisher video tracks by resolution.
        let mut video_matches: Vec<(String, &crate::ad_manager::AdVideoTrack)> = Vec::new();
        let publisher_video_tracks: Vec<(String, u32)> = self.tracks.iter()
            .filter(|(_, s)| s.track_type == TrackType::Video)
            .map(|(n, _s)| {
                let height = self.init_segments.get(n)
                    .and_then(|init| mp4::extract_video_dimensions(init))
                    .map(|(_, h)| h)
                    .unwrap_or(0);
                (n.clone(), height)
            })
            .collect();

        for (track_name, height) in &publisher_video_tracks {
            if let Some(ad_track) = ad.video_tracks.get(height) {
                info!("AD_INSERT: matched {}p ad video → publisher track '{}'", height, track_name);
                video_matches.push((track_name.clone(), ad_track));
            } else {
                warn!("AD_INSERT: no {}p ad video found for publisher track '{}'", height, track_name);
            }
        }

        let audio_track = self.tracks.iter()
            .find(|(_, s)| s.track_type == TrackType::Audio)
            .map(|(n, _)| n.clone());

        // Finish current live groups on ALL tracks
        for (_name, state) in self.tracks.iter_mut() {
            if let Some(mut group) = state.group.take() {
                let _ = group.finish();
            }
        }

        // Build timeslots. Slot 0 = ad init segments (moov frames).
        // Slots 1..N = rebased media fragments.
        let max_frags = video_matches.iter()
            .map(|(_, t)| t.fragments.len())
            .max()
            .unwrap_or(0)
            .max(ad.audio_fragments.len());

        let mut timeslots: Vec<Vec<(String, Bytes)>> = Vec::with_capacity(max_frags + 1);

        // Slot 0: ad init segments as frames
        {
            let mut init_slot: Vec<(String, Bytes)> = Vec::new();
            for (track_name, ad_track) in &video_matches {
                init_slot.push((track_name.clone(), ad_track.init.clone()));
                info!("AD_INSERT: queued ad init for '{}' ({}B)", track_name, ad_track.init.len());
            }
            if let Some(ref at) = audio_track {
                if let Some(ref init) = ad.audio_init {
                    init_slot.push((at.clone(), init.clone()));
                    info!("AD_INSERT: queued ad audio init for '{}' ({}B)", at, init.len());
                }
            }
            timeslots.push(init_slot);
        }

        // Slots 1..N: rebased media fragments
        for i in 0..max_frags {
            let mut slot: Vec<(String, Bytes)> = Vec::new();

            for (track_name, ad_track) in &video_matches {
                if i >= ad_track.fragments.len() {
                    continue;
                }
                let frag = &ad_track.fragments[i];
                let offset_secs = *bdt_offset_secs.get(track_name).unwrap_or(&0.0);
                let ad_ts = ad_track.timescale.max(1) as f64;
                let offset_ticks = (offset_secs * ad_ts).round() as u64;
                let default_dur = self.tracks.get(track_name).and_then(|s| s.default_sample_duration);

                let ad_bdt = mp4::parse_base_decode_time(frag).unwrap_or(0);
                let desired_bdt = offset_ticks + ad_bdt;
                let rebased = mp4::set_decode_time(frag, desired_bdt);

                let frame_data = if let Some(dur) = default_dur {
                    Bytes::from(mp4::inject_trun_duration(&rebased, dur))
                } else {
                    Bytes::from(rebased)
                };

                slot.push((track_name.clone(), frame_data));
            }

            if let Some(ref at) = audio_track {
                if i < ad.audio_fragments.len() {
                    let frag = &ad.audio_fragments[i];
                    let offset_secs = *bdt_offset_secs.get(at).unwrap_or(&0.0);
                    let ad_audio_ts = ad.audio_timescale.max(1) as f64;
                    let offset_ticks = (offset_secs * ad_audio_ts).round() as u64;
                    let default_dur = self.tracks.get(at).and_then(|s| s.default_sample_duration);

                    let ad_bdt = mp4::parse_base_decode_time(frag).unwrap_or(0);
                    let desired_bdt = offset_ticks + ad_bdt;
                    let rebased = mp4::set_decode_time(frag, desired_bdt);

                    let frame_data = if let Some(dur) = default_dur {
                        Bytes::from(mp4::inject_trun_duration(&rebased, dur))
                    } else {
                        Bytes::from(rebased)
                    };

                    slot.push((at.clone(), frame_data));
                }
            }

            timeslots.push(slot);
        }

        // Groups are NOT opened here — publish_ad_slot opens/closes a group per slot,
        // matching the live pattern (one group per segment). This ensures the relay
        // forwards each ad fragment as a discrete group rather than one long-lived stream.

        // Compute pacing interval from the first ad video track's BDT deltas.
        // Uses the AD's timescale (not the live track's timescale).
        let pace_ms = {
            let mut pace: u64 = 1000; // default 1s
            for (_track_name, ad_track) in &video_matches {
                if ad_track.fragments.len() >= 2 {
                    let bdt0 = mp4::parse_base_decode_time(&ad_track.fragments[0]).unwrap_or(0);
                    let bdt1 = mp4::parse_base_decode_time(&ad_track.fragments[1]).unwrap_or(0);
                    let ad_ts = ad_track.timescale.max(1);
                    info!("AD_INSERT: pace calc: bdt0={} bdt1={} delta={} ad_timescale={}",
                        bdt0, bdt1, bdt1.saturating_sub(bdt0), ad_ts);
                    if bdt1 > bdt0 {
                        pace = ((bdt1 - bdt0) * 1000) / ad_ts as u64;
                        if pace > 0 {
                            break;
                        }
                    }
                }
            }
            pace.max(10) // floor at 10ms
        };

        self.ad_state = AdState::PlayingAd;
        // +1 for the init slot
        info!("AD_INSERT: prepared {} timeslots ({} media + 1 init) for ad '{}', pace={}ms",
            timeslots.len(), max_frags, ad.name, pace_ms);

        Ok((timeslots, pace_ms))
    }

    /// Publish one timeslot of ad fragments. Each slot gets its own group per track
    /// (matching the live pattern: one group per segment), so the relay forwards
    /// each ad fragment as a discrete group rather than one long-lived stream.
    pub fn publish_ad_slot(&mut self, slot: &[(String, Bytes)]) -> Result<u32> {
        let mut count: u32 = 0;
        for (track_name, frame_data) in slot {
            if let Some(state) = self.tracks.get_mut(track_name) {
                // Close previous group if open
                if let Some(mut prev) = state.group.take() {
                    let _ = prev.finish();
                }
                // Open new group, write frame, leave open (finished on next slot or finish_ad)
                let mut group = state.track.append_group()
                    .map_err(|e| anyhow!("failed to create ad group for '{}': {}", track_name, e))?;
                let len = frame_data.len() as u64;
                group.write_frame(frame_data.clone())
                    .map_err(|e| anyhow!("failed to write ad frame for '{}': {}", track_name, e))?;
                self.stats.bytes_published.fetch_add(len, Ordering::Relaxed);
                self.stats.frames_sent.fetch_add(1, Ordering::Relaxed);
                count += 1;
                // Finish the group immediately so relay delivers it as a discrete unit
                let _ = group.finish();
            }
        }
        Ok(count)
    }

    /// Finish ad: close ad groups, open new live groups, send live init
    /// segments as frames so the player reinitializes its decoder, then
    /// resume accepting live fragments.
    pub fn finish_ad(&mut self) {
        // Close ad groups
        for (_name, state) in self.tracks.iter_mut() {
            if let Some(mut group) = state.group.take() {
                let _ = group.finish();
            }
        }

        // Open new groups and send live init segments as frames.
        // The player detects moov boxes in incoming frames and calls
        // setInitSegment → changeType, reinitializing the decoder.
        let track_names: Vec<String> = self.tracks.keys().cloned().collect();
        for track_name in &track_names {
            if let Some(init_data) = self.init_segments.get(track_name).cloned() {
                if let Some(state) = self.tracks.get_mut(track_name) {
                    match state.track.append_group() {
                        Ok(mut group) => {
                            match group.write_frame(Bytes::from(init_data)) {
                                Ok(_) => info!("AD_INSERT: sent live init for '{}' as frame", track_name),
                                Err(e) => warn!("AD_INSERT: failed to write live init for '{}': {}", track_name, e),
                            }
                            // Finish this group so relay delivers init as discrete unit.
                            // Next live fragment will create a new group via start_segment.
                            let _ = group.finish();
                        }
                        Err(e) => warn!("AD_INSERT: failed to create resume group for '{}': {}", track_name, e),
                    }
                }
            }
        }

        self.ad_state = AdState::Live;
        info!("AD_INSERT: ad finished, live init segments sent, resuming live");
    }

}
