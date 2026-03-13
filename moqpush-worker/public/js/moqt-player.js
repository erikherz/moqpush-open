// Copyright © 2026 Erik Herz. All rights reserved.

/**
 * moqt-player.js — Custom low-latency MoQT player.
 *
 * Connects to a Cloudflare MoQ relay via WebTransport, subscribes to
 * tracks using MoQ Transport draft-14 wire format, and feeds CMAF
 * fragments directly into MSE SourceBuffers for sub-second latency.
 *
 * Depends on fragment-appender.js (window.FragmentAppender, window.hasMoov, window.hasMoof)
 */

// --- §1 QUIC Variable-Length Integer (RFC 9000 §16) ---

function encodeVarint(v) {
  if (v < 0) throw new Error('negative varint');
  if (v <= 63) return new Uint8Array([v]);
  if (v <= 16383) {
    const b = new Uint8Array(2);
    new DataView(b.buffer).setUint16(0, v | 0x4000);
    return b;
  }
  if (v <= 1073741823) {
    const b = new Uint8Array(4);
    new DataView(b.buffer).setUint32(0, (v | 0x80000000) >>> 0);
    return b;
  }
  const b = new Uint8Array(8);
  new DataView(b.buffer).setBigUint64(0, BigInt(v) | 0xc000000000000000n);
  return b;
}

// --- §2 Stream Reader / Writer ---

class MoqReader {
  #buf = new Uint8Array(0);
  #reader;

  constructor(readable) {
    if (readable instanceof ReadableStream) {
      this.#reader = readable.getReader();
    } else {
      // readable is a Uint8Array buffer (for message body decoding)
      this.#buf = readable;
      this.#reader = null;
    }
  }

  async #fill() {
    if (!this.#reader) return false;
    const { done, value } = await this.#reader.read();
    if (done || !value) return false;
    const chunk = new Uint8Array(value);
    if (this.#buf.byteLength === 0) {
      this.#buf = chunk;
    } else {
      const merged = new Uint8Array(this.#buf.byteLength + chunk.byteLength);
      merged.set(this.#buf);
      merged.set(chunk, this.#buf.byteLength);
      this.#buf = merged;
    }
    return true;
  }

  async #fillTo(n) {
    while (this.#buf.byteLength < n) {
      if (!(await this.#fill())) throw new Error('unexpected end of stream');
    }
  }

  #slice(n) {
    const result = new Uint8Array(this.#buf.buffer, this.#buf.byteOffset, n);
    this.#buf = new Uint8Array(this.#buf.buffer, this.#buf.byteOffset + n, this.#buf.byteLength - n);
    return result;
  }

  async read(n) {
    if (n === 0) return new Uint8Array(0);
    await this.#fillTo(n);
    return this.#slice(n);
  }

  async u8() {
    await this.#fillTo(1);
    return this.#slice(1)[0];
  }

  async u16() {
    await this.#fillTo(2);
    const view = new DataView(this.#buf.buffer, this.#buf.byteOffset, 2);
    const val = view.getUint16(0);
    this.#slice(2);
    return val;
  }

  async varint() {
    await this.#fillTo(1);
    const tag = (this.#buf[0] & 0xc0) >> 6;
    const size = 1 << tag;
    await this.#fillTo(size);
    const slice = this.#slice(size);
    const view = new DataView(slice.buffer, slice.byteOffset, size);
    if (size === 1) return slice[0] & 0x3f;
    if (size === 2) return view.getUint16(0) & 0x3fff;
    if (size === 4) return view.getUint32(0) & 0x3fffffff;
    return Number(view.getBigUint64(0) & 0x3fffffffffffffffn);
  }

  async string() {
    const len = await this.varint();
    const bytes = await this.read(len);
    return new TextDecoder().decode(bytes);
  }

  async bool() {
    return (await this.u8()) === 1;
  }

  async done() {
    if (this.#buf.byteLength > 0) return false;
    if (!this.#reader) return true;
    return !(await this.#fill());
  }

  cancel() {
    if (this.#reader) this.#reader.cancel().catch(() => {});
  }
}

// --- Message Builder ---

class MsgBuilder {
  #chunks = [];
  #size = 0;

  varint(v) {
    const b = encodeVarint(v);
    this.#chunks.push(b);
    this.#size += b.byteLength;
    return this;
  }

  u8(v) {
    this.#chunks.push(new Uint8Array([v]));
    this.#size += 1;
    return this;
  }

  string(s) {
    const data = new TextEncoder().encode(s);
    this.varint(data.byteLength);
    this.#chunks.push(data);
    this.#size += data.byteLength;
    return this;
  }

  bool(v) { return this.u8(v ? 1 : 0); }

  bytes(data) {
    this.#chunks.push(new Uint8Array(data));
    this.#size += data.byteLength;
    return this;
  }

  finish() {
    const result = new Uint8Array(this.#size);
    let off = 0;
    for (const c of this.#chunks) { result.set(c, off); off += c.byteLength; }
    return result;
  }
}

// --- Writer wrapper ---

class MoqWriter {
  #writer;
  #lock = Promise.resolve();

  constructor(writable) {
    this.#writer = writable.getWriter();
  }

  async #rawWrite(data) { await this.#writer.write(data); }

  /** Atomically write a control message: varint(type) + u16(size) + body */
  async writeControlMessage(type, body) {
    const prev = this.#lock;
    let resolve;
    this.#lock = new Promise(r => { resolve = r; });
    await prev;
    try {
      const typeBytes = encodeVarint(type);
      const sizeBytes = new Uint8Array(2);
      new DataView(sizeBytes.buffer).setUint16(0, body.byteLength);
      // Combine into single write for atomicity
      const combined = new Uint8Array(typeBytes.byteLength + 2 + body.byteLength);
      combined.set(typeBytes, 0);
      combined.set(sizeBytes, typeBytes.byteLength);
      combined.set(body, typeBytes.byteLength + 2);
      await this.#rawWrite(combined);
    } finally {
      resolve();
    }
  }

  close() { this.#writer.close().catch(() => {}); }
}

// --- §3 MoQT Protocol Constants (draft-14) ---

const MOQT_VERSION_DRAFT14 = 0xff00000e;
const MOQT_LITE_V02        = 0xff0dad02;
const MOQT_LITE_V01        = 0xff0dad01;

const MSG_CLIENT_SETUP   = 0x20;
const MSG_SERVER_SETUP   = 0x21;
const MSG_SUBSCRIBE      = 0x03;
const MSG_SUBSCRIBE_OK   = 0x04;
const MSG_SUBSCRIBE_ERROR = 0x05;
const MSG_GOAWAY         = 0x10;
const MSG_MAX_REQUEST_ID = 0x15;

const PARAM_MAX_REQUEST_ID  = 2;
const PARAM_IMPLEMENTATION  = 7;

const GROUP_ORDER_DESCENDING = 0x02;
const FILTER_LATEST_GROUP    = 0x01;
const FILTER_LARGEST_OBJECT  = 0x02;
const GROUP_END_STATUS       = 0x03;

const MSG_PUBLISH_NAMESPACE     = 0x06;
const MSG_PUBLISH_NAMESPACE_OK  = 0x07;
const MSG_PUBLISH_DONE          = 0x0b;
const MSG_PUBLISH               = 0x1d;
const MSG_PUBLISH_ERROR         = 0x1f;

// --- §4 MoqtPlayer ---

class MoqtPlayer {
  constructor(video, relayUrl, namespace, opts = {}) {
    this.video = video;
    this.relayUrl = relayUrl;
    this.namespace = namespace;
    this.onStatus = opts.onStatus || (() => {});
    this.onCatalog = opts.onCatalog || (() => {});
    this.startFilter = opts.startFilter === 'latest_group' ? FILTER_LATEST_GROUP : FILTER_LARGEST_OBJECT;

    this.appender = new window.FragmentAppender();
    if (opts.batchSize !== undefined) this.appender.batchSize = opts.batchSize;
    this.trackAliasMap = new Map();   // trackAlias → { name, type:'video'|'audio' }
    this.subscribeCallbacks = new Map(); // requestId → { resolve, reject }
    this.nextReqId = 0;
    this.wt = null;
    this.controlWriter = null;
    this.controlReader = null;
    this.catalogReceived = false;
    this.stats = { framesReceived: 0, bytesReceived: 0, startTime: 0 };
    this._playTriggered = false;
    this._initialSeekDone = false;

    // Pipeline timing instrumentation
    this.timing = {};
  }


  async connect() {
    this.timing.connectStart = performance.now();
    this.onStatus('Connecting WebTransport...');

    // 1. Connect WebTransport
    this.wt = new WebTransport(this.relayUrl, {
      allowPooling: false,
      congestionControl: 'low-latency',
      protocols: ['moq-lite-03', 'moql', 'moqt-16', 'moqt-15'],
    });
    await this.wt.ready;
    this.timing.connectDone = performance.now();
    console.log('[MoQT] WebTransport connected, protocol:', this.wt.protocol ?? '(none)');
    this.wt.closed.then(info => {
      console.log('[MoQT] WebTransport closed:', info);
    }).catch(e => {
      console.error('[MoQT] WebTransport closed with error:', e);
    });

    // 2. Attach MSE to video element + listen for decode/render events
    this.video.src = this.appender.getObjectURL();
    this.stats.startTime = Date.now();
    this.appender.mediaSource.addEventListener('sourceopen', () => {
      if (!this.timing.mseOpen) this.timing.mseOpen = performance.now();
    }, { once: true });
    // Track every video appendBuffer() call until first decode
    this._videoAppends = [];
    this.appender.onAppend = (type, data) => {
      if (type === 'video' && !this.timing.firstFrameDecoded) {
        const now = performance.now();
        const isInit = window.hasMoov(data);
        this._videoAppends.push({ t: now, bytes: data.byteLength, isInit });
        if (!isInit && !this.timing.firstVideoAppend) {
          this.timing.firstVideoAppend = now;
        }
        console.log(`[MoQT] Video appendBuffer #${this._videoAppends.length}: ${data.byteLength}B ${isInit ? '(init)' : '(moof)'} at ${(now - this.timing.connectStart).toFixed(0)}ms`);

        // After first media append, seek to buffered start on updateend
        if (!isInit && !this._seekOnFirstAppend) {
          this._seekOnFirstAppend = true;
          const sb = this.appender.sourceBuffers.video;
          if (sb) {
            sb.addEventListener('updateend', () => {
              const b = this.video.buffered;
              if (b.length > 0 && this.video.currentTime < b.start(0)) {
                console.log(`[MoQT] Seeking to buffered start on updateend: ${b.start(0).toFixed(3)}s`);
                this.video.currentTime = b.start(0);
              }
            }, { once: true });
          }
        }
      }
    };
    this.video.addEventListener('loadeddata', () => {
      if (!this.timing.firstFrameDecoded) {
        this.timing.firstFrameDecoded = performance.now();
        // Stop batching — flush any remaining buffered fragments, switch to per-fragment appends
        this.appender._firstDecodeFired.video = true;
        this.appender._firstDecodeFired.audio = true;
        this.appender._flushBatch('video');
        this.appender._flushBatch('audio');
        this._logTiming();
      }
    }, { once: true });
    this.video.addEventListener('playing', () => {
      if (!this.timing.firstBlit) {
        this.timing.firstBlit = performance.now();
        this._logTiming();
        // Aggressive initial seek — snap to live edge once playback starts
        setTimeout(() => this._initialSeek(), 100);
      }
    }, { once: true });

    // 3. SETUP exchange
    this.onStatus('MoQT SETUP...');
    const bidi = await this.wt.createBidirectionalStream();
    this.controlWriter = new MoqWriter(bidi.writable);
    this.controlReader = new MoqReader(bidi.readable);
    await this._doSetup();
    this.timing.setupDone = performance.now();

    // 4. Start background loops
    this._readControlLoop();
    this._readDataStreams();

    // 5. Subscribe to catalog
    this.onStatus('Subscribing to catalog...');
    this.timing.catalogSubSent = performance.now();
    await this._subscribe('catalog', 128);
    this.timing.catalogSubOk = performance.now();

    // 6. Buffer trimming loop
    this._trimLoop();

    console.log('[MoQT] Waiting for catalog...');
  }

  // --- SETUP Exchange ---

  async _doSetup() {
    // CLIENT_SETUP (draft-14 encoding)
    const body = new MsgBuilder()
      .varint(3)                        // num_versions
      .varint(MOQT_LITE_V02)           // moq-lite draft-02
      .varint(MOQT_LITE_V01)           // moq-lite draft-01
      .varint(MOQT_VERSION_DRAFT14)    // moq-transport draft-14
      .varint(2)                        // num_params
      .varint(PARAM_MAX_REQUEST_ID)    // param: MaxRequestId
      .varint(1000)                     // value
      .varint(PARAM_IMPLEMENTATION)    // param: Implementation (odd = bytes)
      .string('moqpush-custom-player') // value
      .finish();

    await this.controlWriter.writeControlMessage(MSG_CLIENT_SETUP, body);

    // SERVER_SETUP
    const serverType = await this.controlReader.varint();
    if (serverType !== MSG_SERVER_SETUP) {
      throw new Error(`Expected SERVER_SETUP (0x21), got 0x${serverType.toString(16)}`);
    }
    const serverBodySize = await this.controlReader.u16();
    const serverBody = await this.controlReader.read(serverBodySize);
    const sr = new MoqReader(serverBody);
    const serverVersion = await sr.varint();
    console.log(`[MoQT] Server version: 0x${serverVersion.toString(16)}`);
    // Read server parameters
    const numParams = await sr.varint();
    for (let i = 0; i < numParams; i++) {
      const paramId = await sr.varint();
      if (paramId % 2 === 0) {
        const val = await sr.varint();
        console.log(`[MoQT] Server param ${paramId}=${val}`);
        if (paramId === PARAM_MAX_REQUEST_ID) {
          this.serverMaxRequestId = val;
          console.log(`[MoQT] Server MaxRequestId: ${val}`);
        }
      } else {
        const len = await sr.varint();
        await sr.read(len); // bytes value
      }
    }
  }

  // --- SUBSCRIBE ---

  async _subscribe(trackName, priority) {
    const requestId = this.nextReqId;
    this.nextReqId += 2;

    const nsParts = this.namespace.split('/');
    const body = new MsgBuilder();
    body.varint(requestId);                  // request_id
    body.varint(nsParts.length);             // namespace parts count
    for (const part of nsParts) {
      body.string(part);                     // each namespace part
    }
    body.string(trackName);                  // track name
    body.u8(priority);                       // subscriber_priority
    body.u8(GROUP_ORDER_DESCENDING);         // group_order = descending
    body.bool(true);                         // forward = true
    body.varint(this.startFilter);            // filter type
    body.varint(0);                          // num_parameters = 0

    const buf = body.finish();

    // Write: varint(0x03) + u16(size) + body
    await this.controlWriter.writeControlMessage(MSG_SUBSCRIBE, buf);

    console.log(`[MoQT] SUBSCRIBE id=${requestId} track="${trackName}"`);

    // Wait for SubscribeOk
    return new Promise((resolve, reject) => {
      this.subscribeCallbacks.set(requestId, { resolve, reject, trackName });
    });
  }

  // --- Control Message Loop ---

  async _readControlLoop() {
    try {
      while (true) {
        const msgType = await this.controlReader.varint();
        const bodySize = await this.controlReader.u16();
        const bodyData = await this.controlReader.read(bodySize);
        console.log(`[MoQT] Control msg type=0x${msgType.toString(16)} size=${bodySize}`);
        const br = new MoqReader(bodyData);

        switch (msgType) {
          case MSG_SUBSCRIBE_OK:
            await this._handleSubscribeOk(br);
            break;
          case MSG_SUBSCRIBE_ERROR:
            await this._handleSubscribeError(br);
            break;
          case MSG_PUBLISH: {
            // Relay is announcing a track — respond with PUBLISH_ERROR (we're subscriber-only)
            const pubReqId = await br.varint();
            const pubNsParts = await br.varint();
            for (let i = 0; i < pubNsParts; i++) await br.string();
            const pubTrackName = await br.string();
            console.log(`[MoQT] PUBLISH (announce) id=${pubReqId} track="${pubTrackName}" — sending PUBLISH_ERROR`);
            // Respond with PUBLISH_ERROR (0x1f)
            const errBody = new MsgBuilder()
              .varint(pubReqId)    // request_id
              .varint(500)         // error_code
              .string('subscriber only') // reason
              .finish();
            await this.controlWriter.writeControlMessage(MSG_PUBLISH_ERROR, errBody);
            break;
          }
          case MSG_PUBLISH_DONE: {
            const pdReqId = await br.varint();
            const pdStatus = await br.varint();
            console.log(`[MoQT] PUBLISH_DONE id=${pdReqId} status=${pdStatus}`);
            break;
          }
          case MSG_PUBLISH_NAMESPACE: {
            // Relay announcing a namespace is available — respond with OK
            const pnReqId = await br.varint();
            const pnNsParts = await br.varint();
            const parts = [];
            for (let i = 0; i < pnNsParts; i++) parts.push(await br.string());
            console.log(`[MoQT] PUBLISH_NAMESPACE id=${pnReqId} ns="${parts.join('/')}" — sending OK`);
            // Respond with PUBLISH_NAMESPACE_OK (0x07)
            const okBody = new MsgBuilder().varint(pnReqId).finish();
            await this.controlWriter.writeControlMessage(MSG_PUBLISH_NAMESPACE_OK, okBody);
            break;
          }
          case MSG_MAX_REQUEST_ID: {
            const maxId = await br.varint();
            this.serverMaxRequestId = maxId;
            console.log(`[MoQT] MAX_REQUEST_ID updated: ${maxId}`);
            break;
          }
          case MSG_GOAWAY: {
            const uri = await br.string();
            console.warn(`[MoQT] GOAWAY: ${uri}`);
            break;
          }
          default:
            console.log(`[MoQT] Control message type 0x${msgType.toString(16)} (${bodySize}B body, ignored)`);
        }
      }
    } catch (e) {
      if (!this.wt?.closed) {
        console.error('[MoQT] Control stream error:', e);
      }
    }
  }

  async _handleSubscribeOk(reader) {
    const requestId = await reader.varint();
    const trackAlias = await reader.varint();
    // draft-14: expires + group_order + content_exists + optional largest + params
    const expires = await reader.varint();
    const groupOrder = await reader.u8();
    const contentExists = await reader.bool();
    if (contentExists) {
      await reader.varint(); // largest group
      await reader.varint(); // largest object
    }
    // Discard remaining parameters
    const numParams = await reader.varint();
    for (let i = 0; i < numParams; i++) {
      const pid = await reader.varint();
      if (pid % 2 === 0) { await reader.varint(); }
      else { const len = await reader.varint(); await reader.read(len); }
    }

    const cb = this.subscribeCallbacks.get(requestId);
    if (cb) {
      console.log(`[MoQT] SUBSCRIBE_OK id=${requestId} alias=${trackAlias} track="${cb.trackName}"`);
      this.trackAliasMap.set(trackAlias, { name: cb.trackName, requestId });
      cb.resolve(trackAlias);
      this.subscribeCallbacks.delete(requestId);
    } else {
      console.warn(`[MoQT] SUBSCRIBE_OK for unknown id=${requestId}`);
    }
  }

  async _handleSubscribeError(reader) {
    const requestId = await reader.varint();
    const errorCode = await reader.varint();
    const reason = await reader.string();

    const cb = this.subscribeCallbacks.get(requestId);
    if (cb) {
      console.error(`[MoQT] SUBSCRIBE_ERROR id=${requestId} code=${errorCode}: ${reason}`);
      cb.reject(new Error(`SUBSCRIBE_ERROR: ${reason} (code ${errorCode})`));
      this.subscribeCallbacks.delete(requestId);
    }
  }

  // --- Data Stream Loop ---

  async _readDataStreams() {
    console.log('[MoQT] Data stream reader started, waiting for unidirectional streams...');
    const reader = this.wt.incomingUnidirectionalStreams.getReader();
    let streamCount = 0;
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          console.log('[MoQT] incomingUnidirectionalStreams ended');
          break;
        }
        streamCount++;
        console.log(`[MoQT] Unidirectional stream #${streamCount} received`);
        this._handleDataStream(value).catch(e => {
          console.error('[MoQT] Data stream error:', e);
        });
      }
    } catch (e) {
      console.error('[MoQT] Data streams error:', e);
    }
    console.log(`[MoQT] Data stream reader exited after ${streamCount} streams`);
  }

  async _handleDataStream(readable) {
    const r = new MoqReader(readable);

    // Read GROUP header
    const typeId = await r.varint();

    let hasPriority, baseId;
    if (typeId >= 0x10 && typeId <= 0x1f) {
      hasPriority = true; baseId = typeId;
    } else if (typeId >= 0x30 && typeId <= 0x3f) {
      hasPriority = false; baseId = typeId - (0x30 - 0x10);
    } else {
      console.warn(`[MoQT] Unknown group type: 0x${typeId.toString(16)}`);
      r.cancel();
      return;
    }

    const hasExtensions      = (baseId & 0x01) !== 0;
    const hasSubgroupObject  = (baseId & 0x02) !== 0;
    const hasSubgroup        = (baseId & 0x04) !== 0;
    const hasEnd             = (baseId & 0x08) !== 0;

    const trackAlias = await r.varint();
    const groupId    = await r.varint();
    const subGroupId = hasSubgroup ? await r.varint() : 0;
    const priority   = hasPriority ? await r.u8() : 128;

    console.log(`[MoQT] GROUP: type=0x${typeId.toString(16)} alias=${trackAlias} group=${groupId} subgroup=${subGroupId} priority=${priority}`);

    // Look up track by alias, or fall back to matching requestId
    let track = this.trackAliasMap.get(trackAlias);
    if (!track) {
      for (const [, info] of this.trackAliasMap) {
        if (info.requestId === trackAlias) { track = info; break; }
      }
    }
    if (!track) {
      console.warn(`[MoQT] GROUP for unknown alias=${trackAlias}, known aliases:`, [...this.trackAliasMap.keys()]);
      r.cancel();
      return;
    }

    // Read frames
    const flags = { hasExtensions, hasSubgroupObject, hasSubgroup, hasEnd };
    while (true) {
      const streamDone = await r.done();
      if (streamDone) break;

      // Read frame: id_delta + [extensions] + payload_length + payload/status
      const delta = await r.varint();
      if (delta !== 0) {
        console.warn(`[MoQT] Non-zero id_delta: ${delta}`);
      }

      if (hasExtensions) {
        const extLen = await r.varint();
        if (extLen > 0) await r.read(extLen);
      }

      const payloadLen = await r.varint();

      if (payloadLen > 0) {
        const payload = await r.read(payloadLen);
        this._onFrame(track, payload, groupId);
      } else {
        const status = await r.varint();
        if (hasEnd && status === 0) {
          // Empty frame, ignore
        } else if (status === 0 || status === GROUP_END_STATUS) {
          break; // End of group
        } else {
          console.warn(`[MoQT] Unsupported object status: ${status}`);
          break;
        }
      }
    }
  }

  // --- Frame Handler ---

  _onFrame(trackInfo, payload, groupId) {
    if (!trackInfo) return;

    this.stats.framesReceived++;
    this.stats.bytesReceived += payload.byteLength;

    const name = trackInfo.name;

    // Catalog track
    if (name === 'catalog') {
      this._onCatalog(payload);
      return;
    }

    // First media frame timing
    if (!this.timing.firstMediaFrame) {
      this.timing.firstMediaFrame = performance.now();
    }

    // Determine media type from track name
    const type = trackInfo.type || (name.startsWith('video') ? 'video' : 'audio');

    // Detect init segment (moov) vs media fragment (moof)
    if (window.hasMoov(payload)) {
      console.log(`[MoQT] Init segment: ${name} (${payload.byteLength}B) group=${groupId}`);
      this.appender.setInitSegment(type, payload);
    } else if (window.hasMoof(payload)) {
      if (!this.timing.firstFragment) {
        this.timing.firstFragment = performance.now();
        this.timing.firstFragmentType = type;
      }
      this.appender.append(type, payload);
      // Trigger play immediately on first media append
      this._triggerPlay();
    } else {
      // Could be ftyp+moov or combined — try setting as init
      console.debug(`[MoQT] Unknown payload type: ${name} (${payload.byteLength}B)`);
      // Check if it has ftyp (common in init segments)
      if (payload.length > 8) {
        const boxType = String.fromCharCode(payload[4], payload[5], payload[6], payload[7]);
        if (boxType === 'ftyp') {
          this.appender.setInitSegment(type, payload);
        }
      }
    }
  }

  // --- Catalog Handler ---

  async _onCatalog(payload) {
    try {
      if (!this.timing.catalogReceived) {
        this.timing.catalogReceived = performance.now();
      }
      const text = new TextDecoder().decode(payload);
      const catalog = JSON.parse(text);
      console.log('[MoQT] Catalog received:', catalog);
      this.onCatalog(catalog);

      if (this.catalogReceived) return; // Only subscribe to tracks once
      this.catalogReceived = true;

      const tracks = catalog.tracks || [];

      // Classify tracks into video, audio, and other
      const videoTracks = [];
      const audioTracks = [];
      for (const track of tracks) {
        if (!track.name) continue;
        const selParams = track.selectionParams || {};
        const mime = selParams.mimeType || '';
        if (mime.startsWith('audio') || track.name.includes('audio')) {
          audioTracks.push(track);
        } else if (mime.startsWith('video') || track.name.includes('video')) {
          videoTracks.push(track);
        } else {
          console.log(`[MoQT] Skipping non-media track: ${track.name}`);
        }
      }

      // Pick highest resolution video track
      const selectedVideo = videoTracks.length > 0
        ? videoTracks.reduce((best, t) => {
            const res = (t.width || 0) * (t.height || 0);
            const bestRes = (best.width || 0) * (best.height || 0);
            return res > bestRes ? t : best;
          })
        : null;
      const selectedAudio = audioTracks.length > 0 ? audioTracks[0] : null;
      const selected = [selectedVideo, selectedAudio].filter(Boolean);

      console.log(`[MoQT] Selected tracks: video=${selectedVideo?.name} audio=${selectedAudio?.name}`);
      this.onStatus(`Subscribing to ${selected.length} tracks...`);

      // Extract init data from catalog before subscribing
      for (const track of selected) {
        const type = track === selectedVideo ? 'video' : 'audio';
        if (track.initData) {
          try {
            const initBytes = this._base64ToUint8Array(track.initData);
            console.log(`[MoQT] Init from catalog: ${track.name} (${initBytes.byteLength}B)`);
            this.appender.setInitSegment(type, initBytes);
          } catch (e) {
            console.warn(`[MoQT] Failed to decode initData for ${track.name}:`, e);
          }
        }
      }

      // Subscribe to video and audio in parallel — atomic writer prevents interleaved bytes
      this.timing.subscribeStart = performance.now();
      await Promise.all(selected.map(async (track) => {
        const type = track === selectedVideo ? 'video' : 'audio';
        const priority = type === 'video' ? 128 : 64;
        try {
          const alias = await this._subscribe(track.name, priority);
          const info = this.trackAliasMap.get(alias);
          if (info) info.type = type;
        } catch (e) {
          console.error(`[MoQT] Failed to subscribe to ${track.name}:`, e);
        }
      }));
      this.timing.subscribeDone = performance.now();

      this.onStatus('Playing');
    } catch (e) {
      console.error('[MoQT] Catalog parse error:', e);
    }
  }

  _base64ToUint8Array(b64) {
    const binaryString = atob(b64);
    const bytes = new Uint8Array(binaryString.length);
    for (let i = 0; i < binaryString.length; i++) {
      bytes[i] = binaryString.charCodeAt(i);
    }
    return bytes;
  }

  // --- Buffer Trimming ---

  _trimLoop() {
    setInterval(() => {
      if (this.video.currentTime > 0) {
        this.appender.trimBuffer(this.video.currentTime, 3);
      }
      // Seek to live edge if we fall too far behind.
      const edge = this._getLiveEdge();
      if (edge > 0) {
        const behind = edge - this.video.currentTime;
        if (behind > 2.0 && !this.video.paused) {
          console.log(`[MoQT] Seeking to live edge (behind ${behind.toFixed(1)}s)`);
          this.video.currentTime = edge - 0.1;
        }
      }
    }, 1000);
  }

  /** Get the live edge — use video buffer (what's actually decodable),
   *  fall back to audio if video hasn't arrived yet. */
  _getLiveEdge() {
    const videoSb = this.appender.sourceBuffers.video;
    if (videoSb && videoSb.buffered.length > 0) {
      return videoSb.buffered.end(videoSb.buffered.length - 1);
    }
    // Fallback to audio only when video has no data yet
    const audioSb = this.appender.sourceBuffers.audio;
    if (audioSb && audioSb.buffered.length > 0) {
      return audioSb.buffered.end(audioSb.buffered.length - 1);
    }
    const b = this.video.buffered;
    return b.length > 0 ? b.end(b.length - 1) : 0;
  }

  // --- Play trigger ---

  _triggerPlay() {
    if (this._playTriggered) return;
    this._playTriggered = true;

    // Seek to buffered start so Chrome transitions to HAVE_CURRENT_DATA immediately.
    // Without this, currentTime=0 but data starts at the live edge (e.g. 52s),
    // so Chrome stays at HAVE_METADATA until enough data accumulates.
    const seekToBuffered = () => {
      const b = this.video.buffered;
      if (b.length > 0 && this.video.currentTime < b.start(0)) {
        const target = b.start(0);
        console.log(`[MoQT] Seeking to buffered start: ${target.toFixed(3)}s`);
        this.video.currentTime = target;
      }
    };

    // Try immediately, and also after a short delay for the append to land
    seekToBuffered();
    setTimeout(seekToBuffered, 10);
    setTimeout(seekToBuffered, 50);

    this.video.play().catch(e => {
      console.warn('[MoQT] play() rejected:', e.message);
    });
  }

  _initialSeek() {
    if (this._initialSeekDone) return;
    this._initialSeekDone = true;
    const edge = this._getLiveEdge();
    if (edge > 0.15) {
      const target = edge - 0.05; // 50ms behind live edge
      console.log(`[MoQT] Initial seek: edge=${edge.toFixed(3)}s → ${target.toFixed(3)}s`);
      this.video.currentTime = target;
    }
  }

  // --- Timing ---

  _logTiming() {
    const t = this.timing;
    const t0 = t.connectStart || 0;
    const fmt = (label, ts) => ts ? `${label}: ${(ts - t0).toFixed(0)}ms` : null;
    const delta = (label, from, to) => (from && to) ? `${label}: +${(to - from).toFixed(0)}ms` : null;
    const lines = [
      fmt('QUIC connect',      t.connectDone),
      fmt('MSE sourceopen',    t.mseOpen),
      fmt('SETUP done',        t.setupDone),
      fmt('Catalog SUB sent',  t.catalogSubSent),
      fmt('Catalog SUB_OK',    t.catalogSubOk),
      fmt('Catalog data',      t.catalogReceived),
      fmt('Media SUBs done',   t.subscribeDone),
      fmt('First media frame', t.firstMediaFrame),
      fmt('First fragment',    t.firstFragment),
      fmt('Video appended',    t.firstVideoAppend),
      t.firstFragment ? `  (${t.firstFragmentType})` : null,
      fmt('First decoded',     t.firstFrameDecoded),
      fmt('First blit',        t.firstBlit),
      '',
      delta('  Connect→SETUP',      t.connectDone, t.setupDone),
      delta('  SETUP→Catalog data',  t.setupDone, t.catalogReceived),
      delta('  Catalog→Subscribes', t.catalogReceived, t.subscribeDone),
      delta('  Subscribe→Fragment',  t.subscribeDone, t.firstFragment),
      delta('  Fragment→Decoded',    t.firstFragment, t.firstFrameDecoded),
      delta('  Decoded→Blit',        t.firstFrameDecoded, t.firstBlit),
      `  Video appends before decode: ${this._videoAppends.length}`,
    ].filter(Boolean);
    console.log(`[MoQT] ⏱ Pipeline timing:\n  ${lines.join('\n  ')}`);
  }

  getTiming() {
    const t = this.timing;
    const t0 = t.connectStart || 0;
    const rel = (ts) => ts ? Math.round(ts - t0) : null;
    return {
      connectMs:       rel(t.connectDone),
      mseOpenMs:       rel(t.mseOpen),
      setupMs:         rel(t.setupDone),
      catalogSubMs:    rel(t.catalogSubSent),
      catalogOkMs:     rel(t.catalogSubOk),
      catalogMs:       rel(t.catalogReceived),
      subscribeMs:     rel(t.subscribeDone),
      firstMediaMs:    rel(t.firstMediaFrame),
      firstFragmentMs: rel(t.firstFragment),
      videoAppendMs:   rel(t.firstVideoAppend),
      appendsBeforeDecode: this._videoAppends.length,
      firstDecodedMs:  rel(t.firstFrameDecoded),
      firstBlitMs:     rel(t.firstBlit),
    };
  }

  // --- Stats ---

  getStats() {
    const b = this.video.buffered;
    const edge = b.length > 0 ? b.end(b.length - 1) : 0;
    const bufferHealth = edge - this.video.currentTime;
    return {
      framesReceived: this.stats.framesReceived,
      bytesReceived: this.stats.bytesReceived,
      bufferHealth: bufferHealth.toFixed(2),
      currentTime: this.video.currentTime.toFixed(2),
      uptime: Math.floor((Date.now() - this.stats.startTime) / 1000),
      width: this.video.videoWidth,
      height: this.video.videoHeight,
    };
  }

  // --- Cleanup ---

  destroy() {
    this.appender.destroy();
    if (this.wt) {
      try { this.wt.close(); } catch (e) { /* ignore */ }
    }
  }
}

// Expose to window
window.MoqtPlayer = MoqtPlayer;
