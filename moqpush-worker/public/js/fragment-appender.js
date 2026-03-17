/*! Copyright © 2026 Erik Herz. All rights reserved. */

/**
 * FragmentAppender — Direct MSE management for sub-second latency.
 * Adapted from viper project. Strips diagnostics, keeps core MSE logic.
 *
 * Auto-detects codecs from init segments (parses moov/avcC for video,
 * moov/esds for audio). SourceBuffers are created automatically once
 * both init segments have been received and parsed.
 */

// --- MP4 box parsing ---

function findBox(data, start, end, type) {
  const t0 = type.charCodeAt(0), t1 = type.charCodeAt(1);
  const t2 = type.charCodeAt(2), t3 = type.charCodeAt(3);
  let offset = start;
  while (offset + 8 <= end) {
    const size = ((data[offset] << 24) | (data[offset+1] << 16) |
                  (data[offset+2] << 8) | data[offset+3]) >>> 0;
    let boxSize = size;
    let headerSize = 8;
    if (size === 1 && offset + 16 <= end) {
      boxSize = ((data[offset+8] << 24) | (data[offset+9] << 16) |
                 (data[offset+10] << 8) | data[offset+11]) * 0x100000000 +
                (((data[offset+12] << 24) | (data[offset+13] << 16) |
                  (data[offset+14] << 8) | data[offset+15]) >>> 0);
      headerSize = 16;
    } else if (size === 0) {
      boxSize = end - offset;
    }
    if (boxSize < headerSize || offset + boxSize > end) break;
    if (data[offset+4] === t0 && data[offset+5] === t1 &&
        data[offset+6] === t2 && data[offset+7] === t3) {
      return { contentOffset: offset + headerSize, end: offset + boxSize };
    }
    offset += boxSize;
  }
  return null;
}

const hex2 = n => n.toString(16).padStart(2, '0');

function parseCodecFromInitSegment(data) {
  const moov = findBox(data, 0, data.length, 'moov');
  if (!moov) return null;
  const trak = findBox(data, moov.contentOffset, moov.end, 'trak');
  if (!trak) return null;
  const mdia = findBox(data, trak.contentOffset, trak.end, 'mdia');
  if (!mdia) return null;
  const minf = findBox(data, mdia.contentOffset, mdia.end, 'minf');
  if (!minf) return null;
  const stbl = findBox(data, minf.contentOffset, minf.end, 'stbl');
  if (!stbl) return null;
  const stsd = findBox(data, stbl.contentOffset, stbl.end, 'stsd');
  if (!stsd) return null;

  const entryStart = stsd.contentOffset + 8;

  // Video: avc1 → avcC
  const avc1 = findBox(data, entryStart, stsd.end, 'avc1');
  if (avc1) {
    const avcC = findBox(data, avc1.contentOffset + 78, avc1.end, 'avcC');
    if (avcC && avcC.contentOffset + 4 <= data.length) {
      const off = avcC.contentOffset;
      return `avc1.${hex2(data[off+1])}${hex2(data[off+2])}${hex2(data[off+3])}`;
    }
  }

  // Audio: mp4a → esds
  const mp4a = findBox(data, entryStart, stsd.end, 'mp4a');
  if (mp4a) {
    const esds = findBox(data, mp4a.contentOffset + 28, mp4a.end, 'esds');
    if (esds) {
      return parseEsdsCodec(data, esds.contentOffset + 4, esds.end);
    }
  }

  return null;
}

function skipDescriptorLength(data, offset) {
  let i = offset;
  while (i < data.length && (data[i] & 0x80)) i++;
  return i + 1;
}

function parseEsdsCodec(data, start, end) {
  let i = start;
  if (i >= end || data[i] !== 0x03) return null;
  i = skipDescriptorLength(data, i + 1);
  i += 3;
  if (i >= end || data[i] !== 0x04) return null;
  i = skipDescriptorLength(data, i + 1);
  const objectType = data[i];
  i += 13;
  if (i >= end || data[i] !== 0x05) return `mp4a.${hex2(objectType)}.2`;
  i = skipDescriptorLength(data, i + 1);
  if (i >= end) return `mp4a.${hex2(objectType)}.2`;
  let audioObjectType = data[i] >> 3;
  if (audioObjectType === 31 && i + 1 < end) {
    audioObjectType = 32 + ((data[i] & 0x07) << 3) | (data[i+1] >> 5);
  }
  return `mp4a.${hex2(objectType)}.${audioObjectType}`;
}

// --- Helper: detect box types ---

function hasMoov(data) {
  return findBox(data, 0, data.length, 'moov') !== null;
}

function hasMoof(data) {
  return findBox(data, 0, data.length, 'moof') !== null;
}

// --- FragmentAppender ---

class FragmentAppender {
  constructor() {
    this.mediaSource = new MediaSource();
    this.sourceBuffers = {};
    this.queues = { video: [], audio: [] };
    this.initSegments = { video: null, audio: null };
    this.codecs = { video: null, audio: null };
    this.initialized = false;
    this.errored = false;
    this.onAppend = null; // callback(type, data, isInit) — fires when appendBuffer() is actually called
    this._firstDecodeFired = { video: false, audio: false };
    this._batchBuffer = { video: [], audio: [] };
    this.batchSize = 8; // batch first N video/audio fragments into one appendBuffer call

    this._openPromise = new Promise(resolve => {
      this.mediaSource.addEventListener('sourceopen', resolve, { once: true });
    });
  }

  getObjectURL() {
    return URL.createObjectURL(this.mediaSource);
  }

  setInitSegment(type, data) {
    const buf = data instanceof ArrayBuffer ? new Uint8Array(data) : data;
    this.initSegments[type] = buf;

    const newCodec = parseCodecFromInitSegment(buf);
    const oldCodec = this.codecs[type];
    if (newCodec) {
      this.codecs[type] = newCodec;
      console.log(`[MSE] ${type} init: ${buf.byteLength}B, codec=${newCodec}`);
    } else {
      this.codecs[type] = type === 'video' ? 'avc1.4d401f' : 'mp4a.40.2';
      console.warn(`[MSE] ${type} init: codec detection failed, fallback=${this.codecs[type]}`);
    }

    if (this.initialized && this.sourceBuffers[type]) {
      // Always call changeType when init bytes differ — even if the codec string
      // is the same, the decoder config (SPS/PPS, AudioSpecificConfig) may differ
      // between encoders, requiring a full decoder reinit.
      const effectiveNewCodec = newCodec || this.codecs[type];
      const initChanged = this._lastInitBytes && this._lastInitBytes[type] &&
        buf.byteLength !== this._lastInitBytes[type].byteLength;
      if (oldCodec && (effectiveNewCodec !== oldCodec || initChanged)) {
        const mime = `${type}/mp4; codecs="${effectiveNewCodec}"`;
        this.queues[type].push({ _changeType: mime, _oldCodec: oldCodec || effectiveNewCodec, _newCodec: effectiveNewCodec });
      }
      if (!this._lastInitBytes) this._lastInitBytes = {};
      this._lastInitBytes[type] = buf;
      this.queues[type].push(buf);
      this._processQueue(type);
      return;
    }

    if (!this._lastInitBytes) this._lastInitBytes = {};
    this._lastInitBytes[type] = buf;

    if (this.codecs.video && this.codecs.audio && !this.initialized) {
      this._autoInit().catch(e => console.error('[MSE] autoInit error:', e));
    }
  }

  /*! Copyright © 2026 Erik Herz. All rights reserved. — Buffer Management */
  append(type, data) {
    if (this.errored) return;
    const buf = data instanceof ArrayBuffer ? new Uint8Array(data) : data;

    // Before first decode, accumulate fragments and flush as one big append
    if (!this._firstDecodeFired[type]) {
      this._batchBuffer[type].push(buf);
      if (this._batchBuffer[type].length >= this.batchSize) {
        this._flushBatch(type);
      } else if (this._batchBuffer[type].length === 1) {
        // Start a timeout — flush whatever we have after 250ms
        setTimeout(() => {
          if (!this._firstDecodeFired[type] && this._batchBuffer[type].length > 0) {
            this._flushBatch(type);
          }
        }, 250);
      }
      return;
    }

    this.queues[type].push(buf);
    if (this.sourceBuffers[type]) {
      this._processQueue(type);
    }
  }

  _flushBatch(type) {
    const chunks = this._batchBuffer[type];
    if (chunks.length === 0) return;
    let totalLen = 0;
    for (const c of chunks) totalLen += c.byteLength;
    const merged = new Uint8Array(totalLen);
    let off = 0;
    for (const c of chunks) { merged.set(c, off); off += c.byteLength; }
    console.log(`[MSE] Batched ${chunks.length} ${type} fragments (${totalLen}B) into single append`);
    this._batchBuffer[type] = [];
    this.queues[type].push(merged);
    if (this.sourceBuffers[type]) {
      this._processQueue(type);
    }
  }

  async _autoInit() {
    if (this._autoInitStarted || this.initialized) return;
    this._autoInitStarted = true;
    await this._openPromise;
    if (this.initialized) return;
    this._createSourceBuffers(this.codecs.video, this.codecs.audio);
  }

  _createSourceBuffers(videoCodec, audioCodec) {
    if (this.initialized) return;
    const videoMime = `video/mp4; codecs="${videoCodec}"`;
    const audioMime = `audio/mp4; codecs="${audioCodec}"`;
    console.log(`[MSE] SourceBuffers: ${videoMime} | ${audioMime}`);

    this.mediaSource.duration = Infinity;

    const videoSb = this.mediaSource.addSourceBuffer(videoMime);
    const audioSb = this.mediaSource.addSourceBuffer(audioMime);

    videoSb.mode = 'segments';
    audioSb.mode = 'segments';

    videoSb.addEventListener('updateend', () => this._processQueue('video'));
    audioSb.addEventListener('updateend', () => this._processQueue('audio'));

    videoSb.addEventListener('error', (e) => {
      console.error('[MSE] Video SourceBuffer error:', e);
    });
    audioSb.addEventListener('error', (e) => {
      console.error('[MSE] Audio SourceBuffer error:', e);
    });

    this.sourceBuffers.video = videoSb;
    this.sourceBuffers.audio = audioSb;
    this.initialized = true;

    for (const type of ['video', 'audio']) {
      if (this.initSegments[type]) {
        this.queues[type].unshift(this.initSegments[type]);
      }
      this._processQueue(type);
    }
  }

  /*! Copyright © 2026 Erik Herz. All rights reserved. — Buffer Queue Processing */
  _processQueue(type) {
    const sb = this.sourceBuffers[type];
    const queue = this.queues[type];
    if (!sb || sb.updating || queue.length === 0) return;
    if (this.mediaSource.readyState !== 'open') return;
    if (this.errored) { queue.length = 0; return; }

    const data = queue.shift();

    // Handle changeType marker (queued by setInitSegment when codec/init changes)
    if (data && data._changeType) {
      try {
        sb.abort();
        sb.changeType(data._changeType);
        console.log(`[MSE] ${type} abort+changeType: ${data._oldCodec} → ${data._newCodec}`);
      } catch (e) {
        console.error(`[MSE] ${type} changeType failed:`, e);
      }
      // changeType is synchronous when sb is idle — process next item immediately
      this._processQueue(type);
      return;
    }

    try {
      if (this.onAppend) this.onAppend(type, data);
      sb.appendBuffer(data);
    } catch (e) {
      console.error(`[MSE] appendBuffer error (${type}):`, e.name, e.message);
      if (e.name === 'QuotaExceededError') {
        queue.unshift(data);
      } else if (e.name === 'InvalidStateError') {
        this.errored = true;
        this.queues.video.length = 0;
        this.queues.audio.length = 0;
      }
    }
  }

  /*! Copyright © 2026 Erik Herz. All rights reserved. — Buffer Trimming */
  trimBuffer(currentTime, keep) {
    for (const type of ['video', 'audio']) {
      const sb = this.sourceBuffers[type];
      if (!sb || sb.updating) continue;
      if (sb.buffered.length > 0) {
        const start = sb.buffered.start(0);
        const removeEnd = currentTime - keep;
        if (removeEnd > start + 1) {
          try { sb.remove(start, removeEnd); } catch (e) { /* ignore */ }
        }
      }
    }
  }

  destroy() {
    if (this.mediaSource.readyState === 'open') {
      try { this.mediaSource.endOfStream(); } catch (e) { /* ignore */ }
    }
    this.sourceBuffers = {};
    this.queues = { video: [], audio: [] };
    this.initSegments = { video: null, audio: null };
    this.codecs = { video: null, audio: null };
    this.initialized = false;
    this._autoInitStarted = false;
    this._firstDecodeFired = { video: false, audio: false };
    this._batchBuffer = { video: [], audio: [] };
  }
}

// Expose to window for non-module usage
window.FragmentAppender = FragmentAppender;
window.parseCodecFromInitSegment = parseCodecFromInitSegment;
window.hasMoov = hasMoov;
window.hasMoof = hasMoof;
