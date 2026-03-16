//! Ad manager: loads pre-encoded CMAF ads from disk and provides them for insertion.
//!
//! Ad directory structure:
//!   ads/
//!     my-ad/
//!       video-init.mp4    (ftyp+moov for video)
//!       audio-init.mp4    (ftyp+moov for audio)
//!       video-001.m4s     (moof+mdat fragments, in order)
//!       video-002.m4s
//!       ...
//!       audio-001.m4s
//!       audio-002.m4s
//!       ...
//!
//! Init filenames must contain "init" and end in .mp4.
//! Media filenames must end in .m4s. They are sorted lexicographically
//! so zero-padded numbering (001, 002, ...) is recommended.
//!
//! Video and audio are distinguished by parsing the handler type from
//! the init segment (hdlr box: "vide" or "soun").

use anyhow::{anyhow, Result};
use bytes::Bytes;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tracing::{info, warn};

use crate::mp4;

/// A single loaded ad, ready for insertion.
#[derive(Clone)]
pub struct Ad {
    pub name: String,
    /// Video init segment (ftyp+moov). None if ad has no video.
    pub video_init: Option<Bytes>,
    /// Audio init segment (ftyp+moov). None if ad has no audio.
    pub audio_init: Option<Bytes>,
    /// Video media fragments (moof+mdat), in playback order.
    pub video_fragments: Vec<Bytes>,
    /// Audio media fragments (moof+mdat), in playback order.
    pub audio_fragments: Vec<Bytes>,
    /// Video timescale (from init segment).
    pub video_timescale: u32,
    /// Audio timescale (from init segment).
    pub audio_timescale: u32,
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
                    info!("Loaded ad '{}': {} video frags, {} audio frags",
                        name, ad.video_fragments.len(), ad.audio_fragments.len());
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
        let mut video_init: Option<Bytes> = None;
        let mut audio_init: Option<Bytes> = None;
        let mut video_fragments: Vec<(String, Bytes)> = Vec::new();
        let mut audio_fragments: Vec<(String, Bytes)> = Vec::new();
        let mut video_timescale: u32 = 90000;
        let mut audio_timescale: u32 = 48000;

        // Collect all files
        let mut files: Vec<PathBuf> = std::fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.is_file())
            .collect();
        files.sort();

        // First pass: load init segments
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
            let handler = mp4::parse_handler_type(&data)
                .ok_or_else(|| anyhow!("No handler type in {}", path.display()))?;
            let timescale = mp4::parse_timescale(&data).unwrap_or(90000);

            match handler.as_str() {
                "vide" => {
                    video_timescale = timescale;
                    video_init = Some(Bytes::from(data));
                }
                "soun" => {
                    audio_timescale = timescale;
                    audio_init = Some(Bytes::from(data));
                }
                other => warn!("Unknown handler '{}' in {}, skipping", other, path.display()),
            }
        }

        // Second pass: load media fragments
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

            // Determine video vs audio from filename prefix
            let is_video = filename.starts_with("video") || filename.starts_with("Video");
            let is_audio = filename.starts_with("audio") || filename.starts_with("Audio");

            if is_video {
                video_fragments.push((filename.to_string(), Bytes::from(data)));
            } else if is_audio {
                audio_fragments.push((filename.to_string(), Bytes::from(data)));
            } else {
                // Try to detect from track_id matching init segments
                warn!("Can't determine type for {}, skipping (prefix with 'video-' or 'audio-')", filename);
            }
        }

        // Sort by filename to ensure correct order
        video_fragments.sort_by(|a, b| a.0.cmp(&b.0));
        audio_fragments.sort_by(|a, b| a.0.cmp(&b.0));

        if video_fragments.is_empty() && audio_fragments.is_empty() {
            return Err(anyhow!("No media fragments found in {}", dir.display()));
        }

        Ok(Ad {
            name: name.to_string(),
            video_init,
            audio_init,
            video_fragments: video_fragments.into_iter().map(|(_, d)| d).collect(),
            audio_fragments: audio_fragments.into_iter().map(|(_, d)| d).collect(),
            video_timescale,
            audio_timescale,
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
