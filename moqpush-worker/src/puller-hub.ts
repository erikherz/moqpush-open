// PullerHub Durable Object — real-time WebSocket orchestration for puller servers.
// Each puller connects via WebSocket on startup. The worker sends pull/stop commands
// in real-time. Puller servers report warm status back over the same connection.

import { DurableObject } from "cloudflare:workers";

interface PullerConnection {
  node: string;
  region: string;
  ws: WebSocket;
  activeNamespaces: Set<string>;
  warmNamespaces: Set<string>;
  connectedAt: number;
  lastPing: number;
}

export class PullerHub extends DurableObject {
  private pullers: Map<string, PullerConnection> = new Map();
  // Admin WebSocket clients watching puller status
  private adminClients: Set<WebSocket> = new Set();

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

    const pair = new WebSocketPair();
    const serverWs = pair[1];
    this.ctx.acceptWebSocket(serverWs, [node]);

    const conn: PullerConnection = {
      node,
      region,
      ws: serverWs,
      activeNamespaces: new Set(),
      warmNamespaces: new Set(),
      connectedAt: Date.now(),
      lastPing: Date.now(),
    };

    // Close existing connection for same node
    const existing = this.pullers.get(node);
    if (existing) {
      try { existing.ws.close(1000, "replaced"); } catch {}
    }

    this.pullers.set(node, conn);
    this.broadcastAdminStatus();

    return new Response(null, { status: 101, webSocket: pair[0] });
  }

  private handleAdminConnect(): Response {
    const pair = new WebSocketPair();
    const serverWs = pair[1];
    this.ctx.acceptWebSocket(serverWs, ["admin"]);
    this.adminClients.add(serverWs);

    // Send current status immediately
    serverWs.send(JSON.stringify({ type: "status", ...this.getStatus() }));

    return new Response(null, { status: 101, webSocket: pair[0] });
  }

  webSocketMessage(ws: WebSocket, msg: string | ArrayBuffer) {
    if (typeof msg !== "string") return;

    try {
      const data = JSON.parse(msg);

      if (data.type === "warm") {
        // Puller reports a namespace is warm (actively draining)
        const tags = this.ctx.getTags(ws);
        const node = tags.find(t => t !== "admin");
        if (node) {
          const conn = this.pullers.get(node);
          if (conn) {
            conn.warmNamespaces.add(data.namespace);
            conn.lastPing = Date.now();
            this.broadcastAdminStatus();
          }
        }
      }

      if (data.type === "cold") {
        // Puller reports a namespace pull ended
        const tags = this.ctx.getTags(ws);
        const node = tags.find(t => t !== "admin");
        if (node) {
          const conn = this.pullers.get(node);
          if (conn) {
            conn.warmNamespaces.delete(data.namespace);
            conn.activeNamespaces.delete(data.namespace);
            conn.lastPing = Date.now();
            this.broadcastAdminStatus();
          }
        }
      }

      if (data.type === "pong") {
        const tags = this.ctx.getTags(ws);
        const node = tags.find(t => t !== "admin");
        if (node) {
          const conn = this.pullers.get(node);
          if (conn) conn.lastPing = Date.now();
        }
      }
    } catch {}
  }

  webSocketClose(ws: WebSocket) {
    this.adminClients.delete(ws);
    const tags = this.ctx.getTags(ws);
    const node = tags.find(t => t !== "admin");
    if (node) {
      this.pullers.delete(node);
      this.broadcastAdminStatus();
    }
  }

  webSocketError(ws: WebSocket) {
    this.webSocketClose(ws);
  }

  private sendPullCommand(namespace: string, relayUrl: string, nodes: string[]): { sent: string[]; offline: string[] } {
    const sent: string[] = [];
    const offline: string[] = [];

    for (const node of nodes) {
      const conn = this.pullers.get(node);
      if (!conn) {
        offline.push(node);
        continue;
      }
      try {
        conn.ws.send(JSON.stringify({
          type: "pull",
          namespace,
          relay_url: relayUrl,
        }));
        conn.activeNamespaces.add(namespace);
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

    for (const [node, conn] of this.pullers) {
      if (conn.activeNamespaces.has(namespace)) {
        try {
          conn.ws.send(JSON.stringify({
            type: "stop",
            namespace,
          }));
          conn.activeNamespaces.delete(namespace);
          conn.warmNamespaces.delete(namespace);
          sent.push(node);
        } catch {}
      }
    }

    this.broadcastAdminStatus();
    return { sent };
  }

  private getStatus(): { pullers: Record<string, any> } {
    const result: Record<string, any> = {};
    for (const [node, conn] of this.pullers) {
      result[node] = {
        node: conn.node,
        region: conn.region,
        connected: true,
        connected_at: conn.connectedAt,
        last_ping: conn.lastPing,
        active_namespaces: [...conn.activeNamespaces],
        warm_namespaces: [...conn.warmNamespaces],
      };
    }
    return { pullers: result };
  }

  private broadcastAdminStatus() {
    const msg = JSON.stringify({ type: "status", ...this.getStatus() });
    for (const ws of this.adminClients) {
      try { ws.send(msg); } catch { this.adminClients.delete(ws); }
    }
  }

  async alarm() {
    // Ping connected pullers
    const now = Date.now();
    for (const [node, conn] of this.pullers) {
      if (now - conn.lastPing > 60_000) {
        try { conn.ws.close(1000, "timeout"); } catch {}
        this.pullers.delete(node);
      } else {
        try { conn.ws.send(JSON.stringify({ type: "ping" })); } catch {}
      }
    }
    if (this.pullers.size > 0) {
      this.ctx.storage.setAlarm(Date.now() + 30_000);
    }
  }
}
