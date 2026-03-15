// PullerHub Durable Object — real-time WebSocket orchestration for puller servers.
// Each puller connects via WebSocket on startup. The worker sends pull/stop commands
// in real-time. Puller servers report warm status back over the same connection.
//
// Uses WebSocket Hibernation API: state is serialized onto each WebSocket via
// serializeAttachment so it survives DO eviction. On wake, state is reconstructed
// from getWebSockets().

import { DurableObject } from "cloudflare:workers";
import type { Env } from "./index";

interface PullerAttachment {
  node: string;
  region: string;
  activeNamespaces: string[];
  warmNamespaces: string[];
  connectedAt: number;
  lastPing: number;
}

export class PullerHub extends DurableObject<Env> {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (request.headers.get("Upgrade") === "websocket") {
      if (url.pathname === "/puller") {
        return this.handlePullerConnect(request);
      }
      if (url.pathname === "/admin") {
        return this.handleAdminConnect();
      }
    }

    if (request.method === "POST" && url.pathname === "/pull") {
      const body = await request.json() as { namespace: string; relay_url: string; nodes: string[] };
      return Response.json(this.sendPullCommand(body.namespace, body.relay_url, body.nodes));
    }

    if (request.method === "POST" && url.pathname === "/stop") {
      const body = await request.json() as { namespace: string };
      return Response.json(this.sendStopCommand(body.namespace));
    }

    if (url.pathname === "/status") {
      return Response.json(this.getStatus());
    }

    return new Response("Not found", { status: 404 });
  }

  private handlePullerConnect(request: Request): Response {
    const url = new URL(request.url);
    const node = url.searchParams.get("node") || "";
    const region = url.searchParams.get("region") || "";

    if (!node) {
      return new Response("Missing node param", { status: 400 });
    }

    // Close existing connection for same node
    const existing = this.ctx.getWebSockets(node);
    for (const ws of existing) {
      try { ws.close(1000, "replaced"); } catch {}
    }

    const pair = new WebSocketPair();
    const serverWs = pair[1];
    this.ctx.acceptWebSocket(serverWs, [node]);

    const attachment: PullerAttachment = {
      node,
      region,
      activeNamespaces: [],
      warmNamespaces: [],
      connectedAt: Date.now(),
      lastPing: Date.now(),
    };
    serverWs.serializeAttachment(attachment);

    this.broadcastAdminStatus();

    // Start ping alarm if not already running
    this.ctx.storage.setAlarm(Date.now() + 30_000);

    return new Response(null, { status: 101, webSocket: pair[0] });
  }

  private handleAdminConnect(): Response {
    const pair = new WebSocketPair();
    const serverWs = pair[1];
    this.ctx.acceptWebSocket(serverWs, ["admin"]);

    // Send current status immediately
    serverWs.send(JSON.stringify({ type: "status", ...this.getStatus() }));

    return new Response(null, { status: 101, webSocket: pair[0] });
  }

  webSocketMessage(ws: WebSocket, msg: string | ArrayBuffer) {
    if (typeof msg !== "string") return;

    const tags = this.ctx.getTags(ws);
    if (tags.includes("admin")) return;

    const node = tags[0];
    if (!node) return;

    try {
      const data = JSON.parse(msg);
      const attachment = ws.deserializeAttachment() as PullerAttachment;

      if (data.type === "warm") {
        if (!attachment.activeNamespaces.includes(data.namespace)) {
          attachment.activeNamespaces.push(data.namespace);
        }
        if (!attachment.warmNamespaces.includes(data.namespace)) {
          attachment.warmNamespaces.push(data.namespace);
        }
        attachment.lastPing = Date.now();
        ws.serializeAttachment(attachment);
        this.broadcastAdminStatus();
      }

      if (data.type === "cold") {
        attachment.activeNamespaces = attachment.activeNamespaces.filter(ns => ns !== data.namespace);
        attachment.warmNamespaces = attachment.warmNamespaces.filter(ns => ns !== data.namespace);
        attachment.lastPing = Date.now();
        ws.serializeAttachment(attachment);
        this.broadcastAdminStatus();
      }

      if (data.type === "pong") {
        attachment.lastPing = Date.now();
        ws.serializeAttachment(attachment);
      }
    } catch {}
  }

  webSocketClose(ws: WebSocket) {
    const tags = this.ctx.getTags(ws);
    if (!tags.includes("admin")) {
      this.broadcastAdminStatus();
    }
  }

  webSocketError(ws: WebSocket) {
    this.webSocketClose(ws);
  }

  private getPullerWebSockets(): Map<string, { ws: WebSocket; attachment: PullerAttachment }> {
    const result = new Map<string, { ws: WebSocket; attachment: PullerAttachment }>();
    const allWs = this.ctx.getWebSockets();
    for (const ws of allWs) {
      const tags = this.ctx.getTags(ws);
      if (tags.includes("admin")) continue;
      const node = tags[0];
      if (!node) continue;
      const attachment = ws.deserializeAttachment() as PullerAttachment;
      if (attachment) {
        result.set(node, { ws, attachment });
      }
    }
    return result;
  }

  private sendPullCommand(namespace: string, relayUrl: string, nodes: string[]): { sent: string[]; offline: string[] } {
    const sent: string[] = [];
    const offline: string[] = [];
    const pullers = this.getPullerWebSockets();

    for (const node of nodes) {
      const entry = pullers.get(node);
      if (!entry) {
        offline.push(node);
        continue;
      }
      try {
        entry.ws.send(JSON.stringify({ type: "pull", namespace, relay_url: relayUrl }));
        if (!entry.attachment.activeNamespaces.includes(namespace)) {
          entry.attachment.activeNamespaces.push(namespace);
          entry.ws.serializeAttachment(entry.attachment);
        }
        sent.push(node);
      } catch {
        offline.push(node);
      }
    }

    this.broadcastAdminStatus();
    return { sent, offline };
  }

  private sendStopCommand(namespace: string): { sent: string[] } {
    const sent: string[] = [];
    const pullers = this.getPullerWebSockets();

    for (const [node, entry] of pullers) {
      if (entry.attachment.activeNamespaces.includes(namespace)) {
        try {
          entry.ws.send(JSON.stringify({ type: "stop", namespace }));
          entry.attachment.activeNamespaces = entry.attachment.activeNamespaces.filter(ns => ns !== namespace);
          entry.attachment.warmNamespaces = entry.attachment.warmNamespaces.filter(ns => ns !== namespace);
          entry.ws.serializeAttachment(entry.attachment);
          sent.push(node);
        } catch {}
      }
    }

    this.broadcastAdminStatus();
    return { sent };
  }

  private getStatus(): { pullers: Record<string, any> } {
    const result: Record<string, any> = {};
    const pullers = this.getPullerWebSockets();
    for (const [node, entry] of pullers) {
      result[node] = {
        node: entry.attachment.node,
        region: entry.attachment.region,
        connected: true,
        connected_at: entry.attachment.connectedAt,
        last_ping: entry.attachment.lastPing,
        active_namespaces: entry.attachment.activeNamespaces,
        warm_namespaces: entry.attachment.warmNamespaces,
      };
    }
    return { pullers: result };
  }

  private broadcastAdminStatus() {
    const msg = JSON.stringify({ type: "status", ...this.getStatus() });
    const adminWs = this.ctx.getWebSockets("admin");
    for (const ws of adminWs) {
      try { ws.send(msg); } catch {}
    }
  }

  async alarm() {
    const now = Date.now();
    const pullers = this.getPullerWebSockets();
    let evicted = false;

    // Ping connected pullers and evict stale ones
    for (const [node, entry] of pullers) {
      if (now - entry.attachment.lastPing > 60_000) {
        try { entry.ws.close(1000, "timeout"); } catch {}
        evicted = true;
      } else {
        try {
          entry.ws.send(JSON.stringify({ type: "ping" }));
        } catch {
          // Send failed — socket is dead, close it
          try { entry.ws.close(1000, "send failed"); } catch {}
          evicted = true;
        }
      }
    }

    if (evicted) {
      this.broadcastAdminStatus();
    }

    // Sync heartbeats to D1 for all connected pullers
    for (const [node, entry] of pullers) {
      if (now - entry.attachment.lastPing <= 60_000) {
        try {
          await this.env.DB.prepare(
            `UPDATE pullers SET heartbeat_at = unixepoch(), active_namespaces = ? WHERE node = ?`
          ).bind(JSON.stringify(entry.attachment.activeNamespaces), node).run();
        } catch {}
      }
    }

    // Reschedule if any pullers connected
    const remaining = this.ctx.getWebSockets().length - this.ctx.getWebSockets("admin").length;
    if (remaining > 0) {
      this.ctx.storage.setAlarm(now + 30_000);
    }
  }
}
