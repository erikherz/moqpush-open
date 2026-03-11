import { nanoid } from 'nanoid';
import {
  handleOAuthLogin,
  handleOAuthCallback,
  handleAuthMe,
  handleLogout,
  getSession,
} from './auth/oauth';
import { corsHeaders } from './utils/cors';
export { StatsHub } from './stats-hub';
export { PullerHub } from './puller-hub';

export interface Env {
  KV: KVNamespace;
  DB: D1Database;
  ASSETS: Fetcher;
  STATS_HUB: DurableObjectNamespace;
  PULLER_HUB: DurableObjectNamespace;
  GOOGLE_CLIENT_ID: string;
  GOOGLE_CLIENT_SECRET: string;
  DISCORD_CLIENT_ID: string;
  DISCORD_CLIENT_SECRET: string;
  PULLER_SECRET: string;
}

function jsonResponse(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { ...corsHeaders, 'Content-Type': 'application/json' },
  });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    // CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    // --- Auth routes ---
    if (path === '/api/auth/google/login') return handleOAuthLogin('google', request, env);
    if (path === '/api/auth/google/callback') return handleOAuthCallback('google', request, env);
    if (path === '/api/auth/discord/login') return handleOAuthLogin('discord', request, env);
    if (path === '/api/auth/discord/callback') return handleOAuthCallback('discord', request, env);
    if (path === '/api/auth/me') return handleAuthMe(request, env);
    if (path === '/api/auth/logout') return handleLogout(request, env);

    // --- Push key auth (moqpush-app) ---
    if (path === '/api/push/auth' && request.method === 'POST') {
      return handlePushAuth(request, env);
    }
    if (path === '/api/push/heartbeat' && request.method === 'POST') {
      return handlePushHeartbeat(request, env);
    }
    if (path === '/api/push/announce' && request.method === 'POST') {
      return handlePushAnnounce(request, env);
    }
    if (path === '/api/push/announce' && request.method === 'DELETE') {
      return handlePushRemove(request, env);
    }
    if (path === '/api/push/directory-heartbeat' && request.method === 'POST') {
      return handleDirectoryHeartbeat(request, env);
    }

    // --- Puller WebSocket orchestration ---
    if (path === '/api/pullers/ws') {
      if (request.headers.get('Upgrade') !== 'websocket') {
        return new Response('Expected WebSocket', { status: 426 });
      }
      // Verify puller secret via query param
      const wsUrl = new URL(request.url);
      const secret = wsUrl.searchParams.get('secret');
      if (env.PULLER_SECRET && secret !== env.PULLER_SECRET) {
        return jsonResponse({ error: 'invalid secret' }, 401);
      }
      const node = wsUrl.searchParams.get('node') || '';
      const region = wsUrl.searchParams.get('region') || '';
      if (!node) return jsonResponse({ error: 'missing node' }, 400);

      // Also update D1 registration
      await env.DB.prepare(
        `INSERT INTO pullers (node, url, region, registered_at, heartbeat_at)
         VALUES (?, ?, ?, unixepoch(), unixepoch())
         ON CONFLICT(node) DO UPDATE SET
           url = excluded.url, region = excluded.region, heartbeat_at = unixepoch()`
      ).bind(node, `https://${node}.moqpush.com`, region).run();

      const stub = getPullerHub(env);
      const doUrl = new URL(`https://do/puller?node=${node}&region=${region}`);
      return stub.fetch(new Request(doUrl.toString(), {
        headers: request.headers,
      }));
    }

    // --- Puller status (for admin pages) ---
    if (path === '/api/pullers/status' && request.method === 'GET') {
      const session = await getSession(request, env);
      if (!session) return jsonResponse({ error: 'not authenticated' }, 401);
      const stub = getPullerHub(env);
      return stub.fetch(new Request('https://do/status'));
    }

    // --- Puller status WebSocket (admin live updates) ---
    if (path === '/api/pullers/status/ws') {
      if (request.headers.get('Upgrade') !== 'websocket') {
        return new Response('Expected WebSocket', { status: 426 });
      }
      const session = await getSession(request, env);
      if (!session) return jsonResponse({ error: 'not authenticated' }, 401);
      const stub = getPullerHub(env);
      return stub.fetch(new Request('https://do/admin', {
        headers: request.headers,
      }));
    }

    // --- Legacy puller registration (kept for compat) ---
    if (path === '/api/pullers/register' && request.method === 'POST') {
      return handlePullerRegister(request, env);
    }
    if (path === '/api/pullers/heartbeat' && request.method === 'POST') {
      return handlePullerHeartbeat(request, env);
    }

    // --- Broadcast lookup (player) ---
    if (path.startsWith('/api/broadcasts/') && request.method === 'GET') {
      const namespace = path.split('/api/broadcasts/')[1];
      if (namespace) return handleBroadcastLookup(namespace, env);
    }
    if (path === '/api/broadcasts' && request.method === 'GET') {
      return handleBroadcastsList(env);
    }

    // --- Admin API (session-authenticated) ---
    if (path === '/api/admin/namespaces' && request.method === 'GET') {
      return handleAdminListNamespaces(request, env);
    }
    if (path === '/api/admin/namespaces' && request.method === 'POST') {
      return handleAdminCreateNamespace(request, env);
    }
    if (path.startsWith('/api/admin/namespaces/') && path.endsWith('/regions') && request.method === 'PUT') {
      const namespace = path.split('/api/admin/namespaces/')[1].replace('/regions', '');
      if (namespace) return handleAdminUpdateRegions(namespace, request, env);
    }
    if (path.startsWith('/api/admin/namespaces/') && path.endsWith('/relay') && request.method === 'PUT') {
      const namespace = path.split('/api/admin/namespaces/')[1].replace('/relay', '');
      if (namespace) return handleAdminUpdateRelay(namespace, request, env);
    }
    if (path.startsWith('/api/admin/namespaces/') && request.method === 'DELETE') {
      const namespace = path.split('/api/admin/namespaces/')[1];
      if (namespace) return handleAdminDeleteNamespace(namespace, request, env);
    }
    if (path === '/api/admin/users' && request.method === 'GET') {
      return handleAdminListUsers(request, env);
    }
    if (path === '/api/admin/users' && request.method === 'POST') {
      return handleAdminAddUser(request, env);
    }
    if (path.startsWith('/api/admin/users/') && request.method === 'DELETE') {
      const email = decodeURIComponent(path.split('/api/admin/users/')[1] || '');
      if (email) return handleAdminRemoveUser(email, request, env);
    }
    if (path === '/api/admin/pullers' && request.method === 'GET') {
      return handleAdminListPullers(request, env);
    }
    if (path === '/api/admin/puller-secret' && request.method === 'GET') {
      const admin = await requireAdmin(request, env);
      if (admin instanceof Response) return admin;
      return jsonResponse({ secret: env.PULLER_SECRET || '' });
    }

    // --- Stats ---
    if (path === '/api/stats' && request.method === 'POST') {
      return handleStatsPush(request, env);
    }
    if (path === '/api/stats' && request.method === 'GET') {
      return handleStatsSnapshot(request, env);
    }
    if (path === '/api/stats/ws') {
      if (request.headers.get('Upgrade') !== 'websocket') {
        return new Response('Expected WebSocket', { status: 426 });
      }
      const session = await getSession(request, env);
      if (!session) return jsonResponse({ error: 'not authenticated' }, 401);
      const stub = env.STATS_HUB.get(env.STATS_HUB.idFromName('global'));
      return stub.fetch(request);
    }

    // --- Player stats (from hosted player, push to StatsHub) ---
    if (path === '/api/player-stats' && request.method === 'POST') {
      return handlePlayerStatsPush(request, env);
    }

    // --- Binary request signup ---
    if (path === '/api/request-binaries' && request.method === 'POST') {
      return handleBinaryRequest(request, env);
    }

    // --- Health ---
    if (path === '/api/health') {
      return jsonResponse({ status: 'ok' });
    }

    // --- Admin API: namespaces for current user (non-super) ---
    if (path === '/api/admin/my-namespaces' && request.method === 'GET') {
      return handleMyNamespaces(request, env);
    }

    // --- Static pages ---
    if (path === '/super-admin' || path === '/super-admin/') {
      return env.ASSETS.fetch(new Request(new URL('/super-admin.html', url.origin)));
    }
    if (path === '/admin' || path === '/admin/') {
      return env.ASSETS.fetch(new Request(new URL('/admin.html', url.origin)));
    }
    if (path === '/admin/stats' || path === '/admin/stats/') {
      return env.ASSETS.fetch(new Request(new URL('/admin-stats.html', url.origin)));
    }

    // Player: /{namespace}
    if (path.length > 1 && !path.startsWith('/api/') && !path.startsWith('/admin') && !path.includes('.')) {
      return env.ASSETS.fetch(new Request(new URL('/player.html', url.origin)));
    }

    // Fallback to static assets
    return env.ASSETS.fetch(request);
  },
};

// --- Push key auth ---

const LOCK_TTL_S = 120;
const SOFT_LOCK_MS = 30_000;

async function handlePushAuth(request: Request, env: Env): Promise<Response> {
  const body = await request.json<{ push_key: string; instance_id?: string }>();
  if (!body.push_key) {
    return jsonResponse({ error: 'missing push_key' }, 400);
  }

  const ns = await env.DB.prepare(
    'SELECT namespace, relay_url FROM namespaces WHERE push_key = ?'
  ).bind(body.push_key).first<{ namespace: string; relay_url: string }>();

  if (!ns) {
    return jsonResponse({ error: 'invalid push_key' }, 401);
  }

  // Acquire lock
  if (body.instance_id) {
    const lockKey = `lock:push:${ns.namespace}`;
    const existing = await env.KV.get(lockKey);
    if (existing) {
      const lock = JSON.parse(existing);
      if (lock.instance_id !== body.instance_id && lock.expires_at > Date.now()) {
        return jsonResponse({ error: 'namespace locked by another instance' }, 409);
      }
    }
    await env.KV.put(lockKey, JSON.stringify({
      instance_id: body.instance_id,
      expires_at: Date.now() + SOFT_LOCK_MS,
    }), { expirationTtl: LOCK_TTL_S });
  }

  return jsonResponse({
    ok: true,
    namespace: ns.namespace,
    relay_url: ns.relay_url,
  });
}

async function handlePushHeartbeat(request: Request, env: Env): Promise<Response> {
  const body = await request.json<{ push_key: string; instance_id: string }>();
  if (!body.push_key || !body.instance_id) {
    return jsonResponse({ error: 'missing push_key or instance_id' }, 400);
  }

  const ns = await env.DB.prepare(
    'SELECT namespace FROM namespaces WHERE push_key = ?'
  ).bind(body.push_key).first<{ namespace: string }>();

  if (!ns) {
    return jsonResponse({ error: 'invalid push_key' }, 401);
  }

  const lockKey = `lock:push:${ns.namespace}`;
  const existing = await env.KV.get(lockKey);
  if (existing) {
    const lock = JSON.parse(existing);
    if (lock.instance_id !== body.instance_id) {
      return jsonResponse({ error: 'lock owned by another instance' }, 409);
    }
  }

  await env.KV.put(lockKey, JSON.stringify({
    instance_id: body.instance_id,
    expires_at: Date.now() + SOFT_LOCK_MS,
  }), { expirationTtl: LOCK_TTL_S });

  return jsonResponse({ ok: true });
}

// --- Broadcast directory ---

async function handlePushAnnounce(request: Request, env: Env): Promise<Response> {
  const body = await request.json<{
    push_key: string;
    namespace: string;
    relay_url: string;
    instance_id?: string;
  }>();

  const ns = await env.DB.prepare(
    'SELECT namespace FROM namespaces WHERE push_key = ? AND namespace = ?'
  ).bind(body.push_key, body.namespace).first();

  if (!ns) {
    return jsonResponse({ error: 'invalid push_key or namespace mismatch' }, 401);
  }

  await env.DB.prepare(
    `INSERT INTO broadcast_directory (namespace, relay_url, publisher_instance, started_at, heartbeat_at)
     VALUES (?, ?, ?, unixepoch(), unixepoch())
     ON CONFLICT(namespace) DO UPDATE SET
       relay_url = excluded.relay_url,
       publisher_instance = excluded.publisher_instance,
       heartbeat_at = unixepoch()`
  ).bind(body.namespace, body.relay_url, body.instance_id || null).run();

  // Auto-trigger pulls for configured regions
  await triggerAutoPulls(body.namespace, env);

  return jsonResponse({ ok: true });
}

async function handleDirectoryHeartbeat(request: Request, env: Env): Promise<Response> {
  const body = await request.json<{ push_key: string; namespace: string }>();

  const ns = await env.DB.prepare(
    'SELECT namespace FROM namespaces WHERE push_key = ? AND namespace = ?'
  ).bind(body.push_key, body.namespace).first();

  if (!ns) {
    return jsonResponse({ error: 'invalid push_key or namespace' }, 401);
  }

  await env.DB.prepare(
    'UPDATE broadcast_directory SET heartbeat_at = unixepoch() WHERE namespace = ?'
  ).bind(body.namespace).run();

  return jsonResponse({ ok: true });
}

async function handlePushRemove(request: Request, env: Env): Promise<Response> {
  const body = await request.json<{ push_key: string; namespace: string }>();

  const ns = await env.DB.prepare(
    'SELECT namespace FROM namespaces WHERE push_key = ? AND namespace = ?'
  ).bind(body.push_key, body.namespace).first();

  if (!ns) {
    return jsonResponse({ error: 'invalid push_key or namespace' }, 401);
  }

  // Stop all pulls for this namespace
  await stopPullsForNamespace(body.namespace, env);

  await env.DB.prepare(
    'DELETE FROM broadcast_directory WHERE namespace = ?'
  ).bind(body.namespace).run();

  return jsonResponse({ ok: true });
}

// --- Auto-pull orchestration (via PullerHub DO WebSocket) ---

function getPullerHub(env: Env) {
  return env.PULLER_HUB.get(env.PULLER_HUB.idFromName('global'));
}

async function triggerAutoPulls(namespace: string, env: Env): Promise<void> {
  // Get configured regions for this namespace
  const regions = await env.DB.prepare(
    'SELECT node FROM namespace_regions WHERE namespace = ?'
  ).bind(namespace).all<{ node: string }>();

  const nodes = (regions.results || []).map(r => r.node);
  if (nodes.length === 0) return;

  // Get relay_url for this namespace
  const ns = await env.DB.prepare(
    'SELECT relay_url FROM namespaces WHERE namespace = ?'
  ).bind(namespace).first<{ relay_url: string }>();

  const relayUrl = ns?.relay_url || 'https://draft-14.cloudflare.mediaoverquic.com';

  // Send pull command via PullerHub DO
  const stub = getPullerHub(env);
  await stub.fetch(new Request('https://do/pull', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ namespace, relay_url: relayUrl, nodes }),
  }));
}

async function stopPullsForNamespace(namespace: string, env: Env): Promise<void> {
  const stub = getPullerHub(env);
  await stub.fetch(new Request('https://do/stop', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ namespace }),
  }));
}

// --- Puller registration ---

async function handlePullerRegister(request: Request, env: Env): Promise<Response> {
  const body = await request.json<{
    node: string;
    url: string;
    region: string;
    secret?: string;
  }>();

  if (!body.node || !body.url || !body.region) {
    return jsonResponse({ error: 'missing node, url, or region' }, 400);
  }

  if (env.PULLER_SECRET && body.secret !== env.PULLER_SECRET) {
    return jsonResponse({ error: 'invalid secret' }, 401);
  }

  await env.DB.prepare(
    `INSERT INTO pullers (node, url, region, registered_at, heartbeat_at)
     VALUES (?, ?, ?, unixepoch(), unixepoch())
     ON CONFLICT(node) DO UPDATE SET
       url = excluded.url, region = excluded.region, heartbeat_at = unixepoch()`
  ).bind(body.node, body.url, body.region).run();

  return jsonResponse({ ok: true, node: body.node });
}

async function handlePullerHeartbeat(request: Request, env: Env): Promise<Response> {
  const body = await request.json<{
    node: string;
    url: string;
    region: string;
    secret?: string;
    active_namespaces?: string[];
  }>();

  if (!body.node) {
    return jsonResponse({ error: 'missing node' }, 400);
  }

  if (env.PULLER_SECRET && body.secret !== env.PULLER_SECRET) {
    return jsonResponse({ error: 'invalid secret' }, 401);
  }

  await env.DB.prepare(
    `UPDATE pullers SET
       heartbeat_at = unixepoch(),
       active_namespaces = ?,
       url = COALESCE(?, url),
       region = COALESCE(?, region)
     WHERE node = ?`
  ).bind(
    JSON.stringify(body.active_namespaces || []),
    body.url || null,
    body.region || null,
    body.node,
  ).run();

  return jsonResponse({ ok: true });
}

// --- Broadcast lookup (player) ---

async function handleBroadcastLookup(namespace: string, env: Env): Promise<Response> {
  const entry = await env.DB.prepare(
    'SELECT namespace, relay_url, started_at, heartbeat_at FROM broadcast_directory WHERE namespace = ? AND heartbeat_at > unixepoch() - 60'
  ).bind(namespace).first<{
    namespace: string;
    relay_url: string;
    started_at: number;
    heartbeat_at: number;
  }>();

  if (!entry) {
    return jsonResponse({ error: 'broadcast not found or stale' }, 404);
  }

  // Get puller nodes that are actively pulling this namespace
  const pullers = await env.DB.prepare(
    `SELECT node, url, region, active_namespaces FROM pullers WHERE heartbeat_at > unixepoch() - 60`
  ).all<{ node: string; url: string; region: string; active_namespaces: string }>();

  const activePullers = (pullers.results || []).filter(p => {
    try {
      const ns = JSON.parse(p.active_namespaces || '[]');
      return ns.includes(namespace);
    } catch { return false; }
  }).map(p => ({
    node: p.node,
    url: p.url,
    region: p.region,
  }));

  return jsonResponse({
    namespace,
    relay_url: entry.relay_url,
    started_at: entry.started_at,
    pullers: activePullers,
  });
}

async function handleBroadcastsList(env: Env): Promise<Response> {
  const entries = await env.DB.prepare(
    'SELECT namespace, relay_url, started_at, heartbeat_at FROM broadcast_directory WHERE heartbeat_at > unixepoch() - 60'
  ).all();

  return jsonResponse({ broadcasts: entries.results || [] });
}

// --- Admin API ---

async function requireAdmin(request: Request, env: Env): Promise<string | Response> {
  const session = await getSession(request, env);
  if (!session) {
    return jsonResponse({ error: 'not authenticated' }, 401);
  }

  const user = await env.DB.prepare(
    'SELECT is_admin FROM users WHERE email = ? AND is_admin = 1'
  ).bind(session.email).first<{ is_admin: number }>();

  if (!user) {
    return jsonResponse({ error: 'not authorized' }, 403);
  }

  return session.email;
}

function generateKey(prefix: string): string {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let key = prefix;
  for (let i = 0; i < 32; i++) {
    key += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return key;
}

async function handleAdminListNamespaces(request: Request, env: Env): Promise<Response> {
  const admin = await requireAdmin(request, env);
  if (admin instanceof Response) return admin;

  const rows = await env.DB.prepare(
    'SELECT id, namespace, push_key, owner_email, created_at FROM namespaces ORDER BY created_at DESC'
  ).all();

  // Get region assignments for each namespace
  const namespaces = rows.results || [];
  const result = [];
  for (const ns of namespaces) {
    const regions = await env.DB.prepare(
      'SELECT node FROM namespace_regions WHERE namespace = ?'
    ).bind((ns as any).namespace).all<{ node: string }>();
    result.push({
      ...ns,
      regions: (regions.results || []).map(r => r.node),
    });
  }

  return jsonResponse({ namespaces: result });
}

async function handleAdminCreateNamespace(request: Request, env: Env): Promise<Response> {
  const admin = await requireAdmin(request, env);
  if (admin instanceof Response) return admin;

  const body = await request.json<{ namespace: string; regions?: string[] }>();
  if (!body.namespace || !/^[a-zA-Z0-9_-]+$/.test(body.namespace)) {
    return jsonResponse({ error: 'invalid namespace (alphanumeric, hyphens, underscores only)' }, 400);
  }

  const pushKey = generateKey('mpk_');

  try {
    await env.DB.prepare(
      'INSERT INTO namespaces (namespace, push_key, owner_email) VALUES (?, ?, ?)'
    ).bind(body.namespace, pushKey, admin).run();
  } catch (e: any) {
    if (e.message?.includes('UNIQUE')) {
      return jsonResponse({ error: 'namespace already exists' }, 409);
    }
    throw e;
  }

  // Set region assignments
  if (body.regions && body.regions.length > 0) {
    for (const node of body.regions) {
      await env.DB.prepare(
        'INSERT OR IGNORE INTO namespace_regions (namespace, node) VALUES (?, ?)'
      ).bind(body.namespace, node).run();
    }
  }

  return jsonResponse({
    namespace: body.namespace,
    push_key: pushKey,
    regions: body.regions || [],
  }, 201);
}

async function handleAdminUpdateRegions(namespace: string, request: Request, env: Env): Promise<Response> {
  const admin = await requireAdmin(request, env);
  if (admin instanceof Response) return admin;

  const body = await request.json<{ regions: string[] }>();

  // Clear existing regions
  await env.DB.prepare(
    'DELETE FROM namespace_regions WHERE namespace = ?'
  ).bind(namespace).run();

  // Set new regions
  for (const node of body.regions || []) {
    await env.DB.prepare(
      'INSERT OR IGNORE INTO namespace_regions (namespace, node) VALUES (?, ?)'
    ).bind(namespace, node).run();
  }

  return jsonResponse({ ok: true, namespace, regions: body.regions });
}

async function handleAdminUpdateRelay(namespace: string, request: Request, env: Env): Promise<Response> {
  const admin = await requireAdmin(request, env);
  if (admin instanceof Response) return admin;

  const body = await request.json<{ relay_url: string }>();
  const relayUrl = (body.relay_url || '').trim();
  if (!relayUrl) return jsonResponse({ error: 'relay_url required' }, 400);

  await env.DB.prepare(
    'UPDATE namespaces SET relay_url = ? WHERE namespace = ?'
  ).bind(relayUrl, namespace).run();

  return jsonResponse({ ok: true, namespace, relay_url: relayUrl });
}

async function handleAdminDeleteNamespace(namespace: string, request: Request, env: Env): Promise<Response> {
  const admin = await requireAdmin(request, env);
  if (admin instanceof Response) return admin;

  // Stop any active pulls
  await stopPullsForNamespace(namespace, env);

  await env.DB.prepare('DELETE FROM namespace_regions WHERE namespace = ?').bind(namespace).run();
  await env.DB.prepare('DELETE FROM namespaces WHERE namespace = ?').bind(namespace).run();
  await env.DB.prepare('DELETE FROM broadcast_directory WHERE namespace = ?').bind(namespace).run();

  return jsonResponse({ ok: true });
}

async function handleAdminListUsers(request: Request, env: Env): Promise<Response> {
  const admin = await requireAdmin(request, env);
  if (admin instanceof Response) return admin;

  const rows = await env.DB.prepare(
    'SELECT email, name, avatar_url, provider, is_admin, created_at, last_login FROM users WHERE is_admin = 1 ORDER BY created_at'
  ).all();

  return jsonResponse({ users: rows.results || [] });
}

async function handleAdminAddUser(request: Request, env: Env): Promise<Response> {
  const admin = await requireAdmin(request, env);
  if (admin instanceof Response) return admin;

  const body = await request.json<{ email: string }>();
  if (!body.email) {
    return jsonResponse({ error: 'missing email' }, 400);
  }

  await env.DB.prepare(
    `INSERT INTO users (email, name, provider, provider_id, is_admin)
     VALUES (?, '', 'pending', '', 1)
     ON CONFLICT(email) DO UPDATE SET is_admin = 1`
  ).bind(body.email.toLowerCase()).run();

  return jsonResponse({ ok: true, email: body.email.toLowerCase() }, 201);
}

async function handleAdminRemoveUser(email: string, request: Request, env: Env): Promise<Response> {
  const admin = await requireAdmin(request, env);
  if (admin instanceof Response) return admin;

  if (email.toLowerCase() === (admin as string).toLowerCase()) {
    return jsonResponse({ error: 'cannot remove yourself' }, 400);
  }

  await env.DB.prepare(
    'UPDATE users SET is_admin = 0 WHERE email = ?'
  ).bind(email.toLowerCase()).run();

  return jsonResponse({ ok: true });
}

async function handleAdminListPullers(request: Request, env: Env): Promise<Response> {
  const admin = await requireAdmin(request, env);
  if (admin instanceof Response) return admin;

  const rows = await env.DB.prepare(
    'SELECT node, url, region, active_namespaces, heartbeat_at FROM pullers ORDER BY node'
  ).all();

  const pullers = (rows.results || []).map((p: any) => ({
    ...p,
    active_namespaces: JSON.parse(p.active_namespaces || '[]'),
    online: p.heartbeat_at > Math.floor(Date.now() / 1000) - 60,
  }));

  return jsonResponse({ pullers });
}

// --- My namespaces (namespace owner view) ---

async function handleMyNamespaces(request: Request, env: Env): Promise<Response> {
  const session = await getSession(request, env);
  if (!session) return jsonResponse({ error: 'not authenticated' }, 401);

  const rows = await env.DB.prepare(
    'SELECT id, namespace, push_key, relay_url, owner_email, created_at FROM namespaces WHERE owner_email = ? ORDER BY created_at DESC'
  ).bind(session.email).all();

  const namespaces = rows.results || [];
  const result = [];
  for (const ns of namespaces) {
    const regions = await env.DB.prepare(
      'SELECT node FROM namespace_regions WHERE namespace = ?'
    ).bind((ns as any).namespace).all<{ node: string }>();

    const live = await env.DB.prepare(
      'SELECT namespace FROM broadcast_directory WHERE namespace = ? AND heartbeat_at > unixepoch() - 60'
    ).bind((ns as any).namespace).first();

    result.push({
      ...ns,
      regions: (regions.results || []).map(r => r.node),
      is_live: !!live,
    });
  }

  return jsonResponse({ namespaces: result });
}

// --- Stats ---

async function handleStatsPush(request: Request, env: Env): Promise<Response> {
  const body = await request.json<{
    push_key: string;
    namespace: string;
    role: string;
    [key: string]: any;
  }>();

  if (!body.push_key || !body.namespace || !body.role) {
    return jsonResponse({ error: 'missing push_key, namespace, or role' }, 400);
  }

  // Validate push key
  const ns = await env.DB.prepare(
    'SELECT namespace FROM namespaces WHERE push_key = ? AND namespace = ?'
  ).bind(body.push_key, body.namespace).first();

  if (!ns) {
    return jsonResponse({ error: 'invalid push_key or namespace' }, 401);
  }

  // Forward to StatsHub DO (strip push_key)
  const { push_key, ...statsData } = body;
  const stub = env.STATS_HUB.get(env.STATS_HUB.idFromName('global'));
  return stub.fetch(new Request('https://do/push', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(statsData),
  }));
}

async function handlePlayerStatsPush(request: Request, env: Env): Promise<Response> {
  const body = await request.json<{
    namespace: string;
    [key: string]: any;
  }>();

  if (!body.namespace) {
    return jsonResponse({ error: 'missing namespace' }, 400);
  }

  // Verify the namespace is live
  const entry = await env.DB.prepare(
    'SELECT namespace FROM broadcast_directory WHERE namespace = ? AND heartbeat_at > unixepoch() - 60'
  ).bind(body.namespace).first();

  if (!entry) {
    return jsonResponse({ error: 'namespace not live' }, 404);
  }

  // Forward to StatsHub
  const stub = env.STATS_HUB.get(env.STATS_HUB.idFromName('global'));
  return stub.fetch(new Request('https://do/push', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ...body, role: 'player' }),
  }));
}

// --- Binary request signup ---

async function handleBinaryRequest(request: Request, env: Env): Promise<Response> {
  const body = await request.json<{ name: string; email: string }>();
  if (!body.name || !body.email) {
    return jsonResponse({ error: 'missing name or email' }, 400);
  }

  try {
    await env.DB.prepare(
      'INSERT INTO binary_requests (name, email) VALUES (?, ?)'
    ).bind(body.name.trim(), body.email.trim().toLowerCase()).run();
  } catch (e: any) {
    if (e.message?.includes('UNIQUE')) {
      return jsonResponse({ ok: true, message: 'Already registered — we will be in touch!' });
    }
    throw e;
  }

  return jsonResponse({ ok: true, message: 'Request submitted! We will be in touch.' }, 201);
}

async function handleStatsSnapshot(request: Request, env: Env): Promise<Response> {
  const session = await getSession(request, env);
  if (!session) {
    return jsonResponse({ error: 'not authenticated' }, 401);
  }

  const stub = env.STATS_HUB.get(env.STATS_HUB.idFromName('global'));
  return stub.fetch(new Request('https://do/snapshot'));
}
