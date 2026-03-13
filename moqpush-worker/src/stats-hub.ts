// StatsHub Durable Object — real-time ephemeral stats aggregation.
// Stats pushed by moqpush-app (publisher) every 1s and by players every 1s.
// Broadcasts to admin WebSocket clients. Auto-expires entries after 10s of inactivity.
// Player stats are grouped by player_type (viper/shaka) and averaged across sessions.

import { DurableObject } from "cloudflare:workers";

interface PlayerSession {
  data: Record<string, any>;
  updated_at: number;
}

interface PlayerTypeEntry {
  sessions: Map<string, PlayerSession>;
}

interface StatsEntry {
  publisher?: Record<string, any>;
  publisher_updated_at?: number;
  players: Map<string, PlayerTypeEntry>; // keyed by player_type
}

export class StatsHub extends DurableObject {
  private stats: Map<string, StatsEntry> = new Map();

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "POST" && url.pathname === "/push") {
      return this.handleStatsPush(request);
    }

    if (url.pathname === "/snapshot") {
      return Response.json({ namespaces: this.snapshot() });
    }

    if (request.headers.get("Upgrade") === "websocket") {
      return this.handleWebSocketUpgrade();
    }

    return new Response("Not found", { status: 404 });
  }

  private async handleStatsPush(request: Request): Promise<Response> {
    const body = await request.json() as Record<string, any>;
    const { namespace, role, ...data } = body;

    if (!namespace || !role) {
      return Response.json({ error: "missing namespace or role" }, { status: 400 });
    }

    let entry = this.stats.get(namespace);
    if (!entry) {
      entry = { players: new Map() };
      this.stats.set(namespace, entry);
    }

    if (role === "publisher") {
      entry.publisher = data;
      entry.publisher_updated_at = Date.now();
    } else if (role === "player") {
      const playerType = data.player_type || "unknown";
      const sessionId = data.session_id || "default";

      let typeEntry = entry.players.get(playerType);
      if (!typeEntry) {
        typeEntry = { sessions: new Map() };
        entry.players.set(playerType, typeEntry);
      }

      typeEntry.sessions.set(sessionId, {
        data,
        updated_at: Date.now(),
      });
    }

    this.broadcast();
    this.scheduleAlarm();

    return Response.json({ ok: true });
  }

  private handleWebSocketUpgrade(): Response {
    const pair = new WebSocketPair();
    this.ctx.acceptWebSocket(pair[1]);

    // Send current snapshot immediately
    pair[1].send(JSON.stringify({ namespaces: this.snapshot() }));

    return new Response(null, { status: 101, webSocket: pair[0] });
  }

  private averagePlayerSessions(typeEntry: PlayerTypeEntry): Record<string, any> {
    const sessions = Array.from(typeEntry.sessions.values()).map(s => s.data);
    if (sessions.length === 0) return {};

    const count = sessions.length;
    const result: Record<string, any> = { viewer_count: count };

    // Numeric fields to average
    const avgFields = [
      'buffer_health', 'latency', 'bitrate_mbps', 'dropped_frames',
      'estimated_bandwidth', 'load_latency', 'buffering_time', 'play_time',
      'uptime_secs', 'switch_count', 'frames_received', 'bytes_received',
    ];

    for (const field of avgFields) {
      const values = sessions.map(s => s[field]).filter(v => v != null && typeof v === 'number');
      if (values.length > 0) {
        const avg = values.reduce((a, b) => a + b, 0) / values.length;
        result[field] = parseFloat(avg.toFixed(2));
      }
    }

    // Take from first session for non-averaged fields
    const first = sessions[0];
    if (first.width) result.width = first.width;
    if (first.height) result.height = first.height;
    if (first.video_codec) result.video_codec = first.video_codec;
    if (first.audio_codec) result.audio_codec = first.audio_codec;
    if (first.relay) result.relay = first.relay;
    if (first.shaka_config) result.shaka_config = first.shaka_config;
    if (first.custom_player) result.custom_player = first.custom_player;

    // Most recent updated_at
    const maxUpdated = Math.max(...Array.from(typeEntry.sessions.values()).map(s => s.updated_at));
    result.updated_at = maxUpdated;

    return result;
  }

  private snapshot(): Record<string, any> {
    const result: Record<string, any> = {};
    for (const [ns, entry] of this.stats) {
      const out: Record<string, any> = {};
      if (entry.publisher) {
        out.publisher = { ...entry.publisher, updated_at: entry.publisher_updated_at };
      }

      // Build players object keyed by type with averaged stats
      const players: Record<string, any> = {};
      for (const [playerType, typeEntry] of entry.players) {
        if (typeEntry.sessions.size > 0) {
          players[playerType] = this.averagePlayerSessions(typeEntry);
        }
      }
      if (Object.keys(players).length > 0) {
        out.players = players;
      }

      result[ns] = out;
    }
    return result;
  }

  private broadcast() {
    const msg = JSON.stringify({ namespaces: this.snapshot() });
    for (const ws of this.ctx.getWebSockets()) {
      try { ws.send(msg); }
      catch { /* client disconnected */ }
    }
  }

  private scheduleAlarm() {
    this.ctx.storage.setAlarm(Date.now() + 5000);
  }

  async alarm() {
    const now = Date.now();
    const EXPIRY_MS = 10_000;
    let changed = false;

    for (const [ns, entry] of this.stats) {
      if (entry.publisher_updated_at && now - entry.publisher_updated_at > EXPIRY_MS) {
        delete entry.publisher;
        delete entry.publisher_updated_at;
        changed = true;
      }

      // Expire individual player sessions
      for (const [playerType, typeEntry] of entry.players) {
        for (const [sessionId, session] of typeEntry.sessions) {
          if (now - session.updated_at > EXPIRY_MS) {
            typeEntry.sessions.delete(sessionId);
            changed = true;
          }
        }
        if (typeEntry.sessions.size === 0) {
          entry.players.delete(playerType);
          changed = true;
        }
      }

      if (!entry.publisher && entry.players.size === 0) {
        this.stats.delete(ns);
        changed = true;
      }
    }

    if (changed) {
      this.broadcast();
    }

    if (this.stats.size > 0) {
      this.scheduleAlarm();
    }
  }

  // Required for hibernatable WebSocket
  webSocketMessage(_ws: WebSocket, _msg: string | ArrayBuffer) {}
  webSocketClose(_ws: WebSocket) {}
  webSocketError(_ws: WebSocket, _error: unknown) {}
}
