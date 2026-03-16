//! Ad manager: loads pre-encoded CMAF ads from disk and provides them for insertion.
//!
//! Ad directory structure (multi-quality ABR):
//!   ads/
//!     my-ad/
//!       video-720-init.mp4     (ftyp+moov for 720p video)
//!       video-720-00001.m4s    (moof+mdat fragments, sorted)
//!       video-720-00002.m4s
//!       video-480-init.mp4     (ftyp+moov for 480p video)
//!       video-480-00001.m4s
//!       video-240-init.mp4     (ftyp+moov for 240p video)
//!       video-240-00001.m4s
//!       audio-init.mp4         (ftyp+moov for audio)
//!       audio-00001.m4s
//!
//! Init filenames must contain "init" and end in .mp4.
//! Media filenames must end in .m4s.
//! Files are grouped by prefix (everything before the first digit or "init").
//! Video vs audio is auto-detected from the init segment's handler type.
//! Video tracks are keyed by resolution (e.g. "720", "480", "240") parsed
//! from the filename prefix (video-720-*).

use anyhow::{anyhow, Result};
use bytes::Bytes;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

use crate::mp4;

/// A single video quality within an ad.
#[derive(Clone)]
pub struct AdVideoTrack {
    /// Resolution label from filename (e.g. "720", "480", "240").
    pub label: String,
    /// Width from init segment.
    pub width: u32,
    /// Height from init segment.
    pub height: u32,
    /// Init segment (ftyp+moov).
    pub init: Bytes,
    /// Timescale from init segment.
    pub timescale: u32,
    /// Media fragments (moof+mdat), in playback order.
    pub fragments: Vec<Bytes>,
}

/// A single loaded ad, ready for insertion.
#[derive(Clone)]
pub struct Ad {
    pub name: String,
    /// Video tracks keyed by resolution height (e.g. 240, 480, 720).
    pub video_tracks: HashMap<u32, AdVideoTrack>,
    /// Audio init segment (ftyp+moov). None if ad has no audio.
    pub audio_init: Option<Bytes>,
    /// Audio timescale (from init segment).
    pub audio_timescale: u32,
    /// Audio media fragments (moof+mdat), in playback order.
    pub audio_fragments: Vec<Bytes>,
}

/// Manages all loaded ads.
pub struct AdManager {
    ads: HashMap<String, Ad>,
    ad_dir: PathBuf,
}

impl AdManager {
    /// Create a new AdManager and load all ads from the given directory.
    pub fn load(ad_dir: &Path) -> Result<Self> {
        let mut manager = Self {
            ads: HashMap::new(),
            ad_dir: ad_dir.to_path_buf(),
        };
        manager.scan_ads()?;
        Ok(manager)
    }

    fn scan_ads(&mut self) -> Result<()> {
        if !self.ad_dir.exists() {
            return Err(anyhow!("Ad directory does not exist: {}", self.ad_dir.display()));
        }

        let entries = std::fs::read_dir(&self.ad_dir)?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            let name = path.file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();

            if name.is_empty() || name.starts_with('.') {
                continue;
            }

            match self.load_ad(&path, &name) {
                Ok(ad) => {
                    let video_info: Vec<String> = ad.video_tracks.values()
                        .map(|t| format!("{}p({} frags)", t.height, t.fragments.len()))
                        .collect();
                    info!("Loaded ad '{}': video=[{}], audio={} frags",
                        name, video_info.join(", "), ad.audio_fragments.len());
                    self.ads.insert(name, ad);
                }
                Err(e) => {
                    warn!("Failed to load ad from {}: {}", path.display(), e);
                }
            }
        }

        info!("AdManager: loaded {} ads from {}", self.ads.len(), self.ad_dir.display());
        Ok(())
    }

    fn load_ad(&self, dir: &Path, name: &str) -> Result<Ad> {
        // Group files by prefix. A prefix is everything before "init" or before
        // the numeric segment number. E.g.:
        //   "video-720-init.mp4"   → prefix "video-720-"
        //   "video-720-00001.m4s"  → prefix "video-720-"
        //   "audio-init.mp4"       → prefix "audio-"
        //   "audio-00001.m4s"      → prefix "audio-"

        let mut files: Vec<PathBuf> = std::fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.is_file())
            .collect();
        files.sort();

        // Collect init segments by prefix
        struct InitInfo {
            data: Vec<u8>,
            handler: String, // "vide" or "soun"
            width: u32,
            height: u32,
            timescale: u32,
        }
        let mut inits: HashMap<String, InitInfo> = HashMap::new();

        for path in &files {
            let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if !filename.contains("init") || !filename.ends_with(".mp4") {
                continue;
            }
            let data = std::fs::read(path)?;
            if !mp4::has_moov(&data) {
                warn!("Init file {} has no moov box, skipping", path.display());
                continue;
            }
            let handler = match mp4::parse_handler_type(&data) {
                Some(h) => h,
                None => {
                    warn!("No handler type in {}, skipping", path.display());
                    continue;
                }
            };
            let timescale = mp4::parse_timescale(&data).unwrap_or(90000);
            let (width, height) = mp4::extract_video_dimensions(&data).unwrap_or((0, 0));

            // Extract prefix: everything up to and including the dash before "init"
            let prefix = extract_prefix(filename);
            info!("  Init: {} → prefix='{}' handler={} {}x{} ts={}",
                filename, prefix, handler, width, height, timescale);

            inits.insert(prefix, InitInfo { data, handler, width, height, timescale });
        }

        // Collect media fragments by prefix
        let mut frag_groups: HashMap<String, Vec<(String, Bytes)>> = HashMap::new();

        for path in &files {
            let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if !filename.ends_with(".m4s") {
                continue;
            }
            let data = std::fs::read(path)?;
            if !mp4::has_moof(&data) {
                warn!("Media file {} has no moof box, skipping", path.display());
                continue;
            }

            let prefix = extract_prefix(filename);
            frag_groups.entry(prefix)
                .or_default()
                .push((filename.to_string(), Bytes::from(data)));
        }

        // Sort fragments within each group
        for frags in frag_groups.values_mut() {
            frags.sort_by(|a, b| a.0.cmp(&b.0));
        }

        // Build the Ad struct
        let mut video_tracks: HashMap<u32, AdVideoTrack> = HashMap::new();
        let mut audio_init: Option<Bytes> = None;
        let mut audio_timescale: u32 = 48000;
        let mut audio_fragments: Vec<Bytes> = Vec::new();

        for (prefix, init_info) in &inits {
            let frags: Vec<Bytes> = frag_groups.remove(prefix)
                .unwrap_or_default()
                .into_iter()
                .map(|(_, d)| d)
                .collect();

            match init_info.handler.as_str() {
                "vide" => {
                    let height = init_info.height;
                    let label = if height > 0 {
                        format!("{}", height)
                    } else {
                        prefix.trim_end_matches('-').to_string()
                    };
                    video_tracks.insert(height, AdVideoTrack {
                        label,
                        width: init_info.width,
                        height,
                        init: Bytes::from(init_info.data.clone()),
                        timescale: init_info.timescale,
                        fragments: frags,
                    });
                }
                "soun" => {
                    audio_timescale = init_info.timescale;
                    audio_init = Some(Bytes::from(init_info.data.clone()));
                    audio_fragments = frags;
                }
                _ => {}
            }
        }

        // Check for unmatched fragment groups (fragments with no init)
        for (prefix, frags) in &frag_groups {
            if !frags.is_empty() {
                warn!("Ad '{}': {} fragments with prefix '{}' have no matching init segment",
                    name, frags.len(), prefix);
            }
        }

        if video_tracks.is_empty() && audio_fragments.is_empty() {
            return Err(anyhow!("No media found in {}", dir.display()));
        }

        Ok(Ad {
            name: name.to_string(),
            video_tracks,
            audio_init,
            audio_timescale,
            audio_fragments,
        })
    }

    /// Get a loaded ad by name.
    pub fn get(&self, name: &str) -> Option<&Ad> {
        self.ads.get(name)
    }

    /// List all loaded ad names.
    pub fn list(&self) -> Vec<&str> {
        self.ads.keys().map(|s| s.as_str()).collect()
    }
}

/// Extract the prefix from a filename — everything before "init" or before
/// the first run of digits at the end.
/// Examples:
///   "video-720-init.mp4"   → "video-720-"
///   "video-720-00001.m4s"  → "video-720-"
///   "audio-init.mp4"       → "audio-"
///   "audio-00001.m4s"      → "audio-"
///   "video-init.mp4"       → "video-"
///   "video-00001.m4s"      → "video-"
fn extract_prefix(filename: &str) -> String {
    // Strip extension
    let base = if let Some(pos) = filename.rfind('.') {
        &filename[..pos]
    } else {
        filename
    };

    // If contains "init", prefix is everything before "init"
    if let Some(pos) = base.find("init") {
        return base[..pos].to_string();
    }

    // Otherwise, strip trailing digits
    let prefix = base.trim_end_matches(|c: char| c.is_ascii_digit());
    prefix.to_string()
}
