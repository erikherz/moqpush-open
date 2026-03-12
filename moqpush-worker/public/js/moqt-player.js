/**
 * moqt-player.js — Custom low-latency MoQT player.
 *
 * Connects to a Cloudflare MoQ relay via WebTransport, subscribes to
 * tracks using MoQ Transport draft-14 wire format, and feeds CMAF
 * fragments directly into MSE SourceBuffers for sub-second latency.
 *
 * Depends on fragment-appender.js (window.FragmentAppender, window.hasMoov, window.hasMoof)
 */

// ═══════════════════════════════════════════════════════════════════
// §1  QUIC Variable-Length Integer (RFC 9000 §16)
// ═══════════════════════════════════════════════════════════════════

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

// ═══════════════════════════════════════════════════════════════════
// §2  Stream Reader / Writer
// ═══════════════════════════════════════════════════════════════════

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

// ── Message Builder (serializes to buffer) ──────────────────────────

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

// ── Writer wrapper (writes to WebTransport stream) ──────────────────

class MoqWriter {
  #writer;

  constructor(writable) {
    this.#writer = writable.getWriter();
  }

  async write(data) { await this.#writer.write(data); }

  async varint(v) { await this.write(encodeVarint(v)); }

  async u16(v) {
    const b = new Uint8Array(2);
    new DataView(b.buffer).setUint16(0, v);
    await this.write(b);
  }

  /** Write a u16-size-prefixed message body */
  async writeMessage(body) {
    await this.u16(body.byteLength);
    await this.write(body);
  }

  close() { this.#writer.close().catch(() => {}); }
}

// ═══════════════════════════════════════════════════════════════════
// §3  MoQT Protocol Constants (draft-14)
// ═══════════════════════════════════════════════════════════════════

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
const FILTER_LARGEST_OBJECT  = 0x02;
const GROUP_END_STATUS       = 0x03;

// ═══════════════════════════════════════════════════════════════════
// §4  MoqtPlayer
// ═══════════════════════════════════════════════════════════════════

class MoqtPlayer {
  constructor(video, relayUrl, namespace, opts = {}) {
    this.video = video;
    this.relayUrl = relayUrl;
    this.namespace = namespace;
    this.onStatus = opts.onStatus || (() => {});
    this.onCatalog = opts.onCatalog || (() => {});

    this.appender = new window.FragmentAppender();
    this.trackAliasMap = new Map();   // trackAlias → { name, type:'video'|'audio' }
    this.subscribeCallbacks = new Map(); // requestId → { resolve, reject }
    this.nextReqId = 0;
    this.wt = null;
    this.controlWriter = null;
    this.controlReader = null;
    this.catalogReceived = false;
    this.stats = { framesReceived: 0, bytesReceived: 0, startTime: 0 };
  }

  async connect() {
    this.onStatus('Connecting WebTransport...');

    // 1. Connect WebTransport
    this.wt = new WebTransport(this.relayUrl, {
      allowPooling: false,
      congestionControl: 'low-latency',
      protocols: ['moql'],
    });
    await this.wt.ready;
    console.log('[MoQT] WebTransport connected');

    // 2. Attach MSE to video element
    this.video.src = this.appender.getObjectURL();
    this.stats.startTime = Date.now();

    // 3. SETUP exchange
    this.onStatus('MoQT SETUP...');
    const bidi = await this.wt.createBidirectionalStream();
    this.controlWriter = new MoqWriter(bidi.writable);
    this.controlReader = new MoqReader(bidi.readable);
    await this._doSetup();

    // 4. Start background loops
    this._readControlLoop();
    this._readDataStreams();

    // 5. Subscribe to catalog
    this.onStatus('Subscribing to catalog...');
    await this._subscribe('.catalog', 128);

    // 6. Buffer trimming loop
    this._trimLoop();

    console.log('[MoQT] Waiting for catalog...');
  }

  // ── SETUP Exchange ────────────────────────────────────────────────

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

    await this.controlWriter.varint(MSG_CLIENT_SETUP);
    await this.controlWriter.writeMessage(body);

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
    // Read and discard server parameters
    const numParams = await sr.varint();
    for (let i = 0; i < numParams; i++) {
      const paramId = await sr.varint();
      if (paramId % 2 === 0) {
        await sr.varint(); // varint value
      } else {
        const len = await sr.varint();
        await sr.read(len); // bytes value
      }
    }
  }

  // ── SUBSCRIBE ─────────────────────────────────────────────────────

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
    body.varint(FILTER_LARGEST_OBJECT);      // filter type = LargestObject
    body.varint(0);                          // num_parameters = 0

    const buf = body.finish();

    // Write: varint(0x03) + u16(size) + body
    await this.controlWriter.varint(MSG_SUBSCRIBE);
    await this.controlWriter.writeMessage(buf);

    console.log(`[MoQT] SUBSCRIBE id=${requestId} track="${trackName}"`);

    // Wait for SubscribeOk
    return new Promise((resolve, reject) => {
      this.subscribeCallbacks.set(requestId, { resolve, reject, trackName });
    });
  }

  // ── Control Message Loop ──────────────────────────────────────────

  async _readControlLoop() {
    try {
      while (true) {
        const msgType = await this.controlReader.varint();
        const bodySize = await this.controlReader.u16();
        const bodyData = await this.controlReader.read(bodySize);
        const br = new MoqReader(bodyData);

        switch (msgType) {
          case MSG_SUBSCRIBE_OK:
            await this._handleSubscribeOk(br);
            break;
          case MSG_SUBSCRIBE_ERROR:
            await this._handleSubscribeError(br);
            break;
          case MSG_MAX_REQUEST_ID: {
            const maxId = await br.varint();
            console.log(`[MoQT] MAX_REQUEST_ID: ${maxId}`);
            break;
          }
          case MSG_GOAWAY: {
            const uri = await br.string();
            console.warn(`[MoQT] GOAWAY: ${uri}`);
            break;
          }
          default:
            // Ignore unknown message types (PublishNamespace, etc.)
            console.debug(`[MoQT] Ignoring control message type 0x${msgType.toString(16)}`);
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

  // ── Data Stream Loop (incoming unidirectional streams) ────────────

  async _readDataStreams() {
    const reader = this.wt.incomingUnidirectionalStreams.getReader();
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        this._handleDataStream(value).catch(e => {
          console.error('[MoQT] Data stream error:', e);
        });
      }
    } catch (e) {
      console.error('[MoQT] Data streams error:', e);
    }
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

    // Look up track by alias, or fall back to matching requestId
    let track = this.trackAliasMap.get(trackAlias);
    if (!track) {
      for (const [, info] of this.trackAliasMap) {
        if (info.requestId === trackAlias) { track = info; break; }
      }
    }
    if (!track) {
      console.warn(`[MoQT] GROUP for unknown alias=${trackAlias}`);
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

  // ── Frame Handler ─────────────────────────────────────────────────

  _onFrame(trackInfo, payload, groupId) {
    if (!trackInfo) return;

    this.stats.framesReceived++;
    this.stats.bytesReceived += payload.byteLength;

    const name = trackInfo.name;

    // Catalog track
    if (name === '.catalog') {
      this._onCatalog(payload);
      return;
    }

    // Determine media type from track name
    const type = trackInfo.type || (name.startsWith('video') ? 'video' : 'audio');

    // Detect init segment (moov) vs media fragment (moof)
    if (window.hasMoov(payload)) {
      console.log(`[MoQT] Init segment: ${name} (${payload.byteLength}B) group=${groupId}`);
      this.appender.setInitSegment(type, payload);
    } else if (window.hasMoof(payload)) {
      this.appender.append(type, payload);
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

  // ── Catalog Handler ───────────────────────────────────────────────

  async _onCatalog(payload) {
    try {
      const text = new TextDecoder().decode(payload);
      const catalog = JSON.parse(text);
      console.log('[MoQT] Catalog received:', catalog);
      this.onCatalog(catalog);

      if (this.catalogReceived) return; // Only subscribe to tracks once
      this.catalogReceived = true;

      const tracks = catalog.tracks || [];
      this.onStatus(`Subscribing to ${tracks.length} tracks...`);

      for (const track of tracks) {
        const trackName = track.name;
        if (!trackName) continue;

        // Determine type
        let type = 'video';
        const selParams = track.selectionParams || {};
        if (selParams.mimeType && selParams.mimeType.startsWith('audio')) {
          type = 'audio';
        } else if (trackName.startsWith('audio') || trackName.includes('audio')) {
          type = 'audio';
        } else if (track.renderGroup !== undefined) {
          // MSF: video tracks have renderGroup
          type = 'video';
        }

        // Extract init data from catalog if available
        if (track.initData) {
          try {
            const initBytes = this._base64ToUint8Array(track.initData);
            console.log(`[MoQT] Init from catalog: ${trackName} (${initBytes.byteLength}B)`);
            this.appender.setInitSegment(type, initBytes);
          } catch (e) {
            console.warn(`[MoQT] Failed to decode initData for ${trackName}:`, e);
          }
        }

        // Subscribe to track — store type info
        const priority = type === 'video' ? 128 : 64;
        this._subscribe(trackName, priority).then(alias => {
          // Update track info with type
          const info = this.trackAliasMap.get(alias);
          if (info) info.type = type;
        }).catch(e => {
          console.error(`[MoQT] Failed to subscribe to ${trackName}:`, e);
        });
      }

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

  // ── Buffer Trimming ───────────────────────────────────────────────

  _trimLoop() {
    setInterval(() => {
      if (this.video.currentTime > 0) {
        this.appender.trimBuffer(this.video.currentTime, 3);
      }
      // Auto-seek to live edge if behind
      const b = this.video.buffered;
      if (b.length > 0) {
        const edge = b.end(b.length - 1);
        const behind = edge - this.video.currentTime;
        if (behind > 2.0 && !this.video.paused) {
          console.log(`[MoQT] Seeking to live edge (behind ${behind.toFixed(1)}s)`);
          this.video.currentTime = edge - 0.1;
        }
      }
    }, 1000);
  }

  // ── Stats ─────────────────────────────────────────────────────────

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

  // ── Cleanup ───────────────────────────────────────────────────────

  destroy() {
    this.appender.destroy();
    if (this.wt) {
      try { this.wt.close(); } catch (e) { /* ignore */ }
    }
  }
}

// Expose to window
window.MoqtPlayer = MoqtPlayer;
