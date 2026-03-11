// StatsHub Durable Object — real-time ephemeral stats aggregation.
// Stats pushed by moqpush-app (publisher) every 1s and by players every 1s.
// Broadcasts to admin WebSocket clients. Auto-expires entries after 10s of inactivity.

import { DurableObject } from "cloudflare:workers";

interface StatsEntry {
  publisher?: Record<string, any>;
  publisher_updated_at?: number;
  player?: Record<string, any>;
  player_updated_at?: number;
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

    const entry: StatsEntry = this.stats.get(namespace) || {};

    if (role === "publisher") {
      entry.publisher = data;
      entry.publisher_updated_at = Date.now();
    } else if (role === "player") {
      entry.player = data;
      entry.player_updated_at = Date.now();
    }

    this.stats.set(namespace, entry);

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

  private snapshot(): Record<string, any> {
    const result: Record<string, any> = {};
    for (const [ns, entry] of this.stats) {
      const out: Record<string, any> = {};
      if (entry.publisher) {
        out.publisher = { ...entry.publisher, updated_at: entry.publisher_updated_at };
      }
      if (entry.player) {
        out.player = { ...entry.player, updated_at: entry.player_updated_at };
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
      if (entry.player_updated_at && now - entry.player_updated_at > EXPIRY_MS) {
        delete entry.player;
        delete entry.player_updated_at;
        changed = true;
      }
      if (!entry.publisher && !entry.player) {
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
