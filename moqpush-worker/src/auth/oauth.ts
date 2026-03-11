import { nanoid } from 'nanoid';
import type { Env } from '../index';
import { corsHeaders } from '../utils/cors';

interface OAuthConfig {
  clientId: string;
  clientSecret: string;
  authUrl: string;
  tokenUrl: string;
  userInfoUrl: string;
  scopes: string[];
}

export interface SessionData {
  email: string;
  name: string;
  avatar_url: string;
  provider: string;
}

function getOAuthConfig(provider: 'google' | 'discord', env: Env): OAuthConfig {
  if (provider === 'google') {
    return {
      clientId: env.GOOGLE_CLIENT_ID,
      clientSecret: env.GOOGLE_CLIENT_SECRET,
      authUrl: 'https://accounts.google.com/o/oauth2/v2/auth',
      tokenUrl: 'https://oauth2.googleapis.com/token',
      userInfoUrl: 'https://www.googleapis.com/oauth2/v2/userinfo',
      scopes: ['openid', 'email', 'profile'],
    };
  } else {
    return {
      clientId: env.DISCORD_CLIENT_ID,
      clientSecret: env.DISCORD_CLIENT_SECRET,
      authUrl: 'https://discord.com/api/oauth2/authorize',
      tokenUrl: 'https://discord.com/api/oauth2/token',
      userInfoUrl: 'https://discord.com/api/users/@me',
      scopes: ['identify', 'email'],
    };
  }
}

export async function handleOAuthLogin(
  provider: 'google' | 'discord',
  request: Request,
  env: Env
): Promise<Response> {
  const config = getOAuthConfig(provider, env);
  const state = nanoid();

  await env.KV.put(`oauth_state:${state}`, provider, { expirationTtl: 600 });

  const url = new URL(request.url);
  const redirectUri = `${url.origin}/api/auth/${provider}/callback`;

  const authUrl = new URL(config.authUrl);
  authUrl.searchParams.set('client_id', config.clientId);
  authUrl.searchParams.set('redirect_uri', redirectUri);
  authUrl.searchParams.set('response_type', 'code');
  authUrl.searchParams.set('scope', config.scopes.join(' '));
  authUrl.searchParams.set('state', state);

  return Response.redirect(authUrl.toString(), 302);
}

export async function handleOAuthCallback(
  provider: 'google' | 'discord',
  request: Request,
  env: Env
): Promise<Response> {
  const url = new URL(request.url);
  const code = url.searchParams.get('code');
  const state = url.searchParams.get('state');

  if (!code || !state) {
    return new Response('Missing code or state', { status: 400 });
  }

  const storedProvider = await env.KV.get(`oauth_state:${state}`);
  if (storedProvider !== provider) {
    return new Response('Invalid state', { status: 400 });
  }
  await env.KV.delete(`oauth_state:${state}`);

  const config = getOAuthConfig(provider, env);
  const redirectUri = `${url.origin}/api/auth/${provider}/callback`;

  const tokenResponse = await fetch(config.tokenUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id: config.clientId,
      client_secret: config.clientSecret,
      code,
      grant_type: 'authorization_code',
      redirect_uri: redirectUri,
    }),
  });

  if (!tokenResponse.ok) {
    const errBody = await tokenResponse.text();
    return new Response(`Token exchange failed: ${tokenResponse.status} ${errBody}`, { status: 500 });
  }

  const tokens = await tokenResponse.json<any>();

  const userResponse = await fetch(config.userInfoUrl, {
    headers: { Authorization: `Bearer ${tokens.access_token}` },
  });

  if (!userResponse.ok) {
    const errBody = await userResponse.text();
    return new Response(`User info failed: ${userResponse.status} ${errBody}`, { status: 500 });
  }

  const userInfo = await userResponse.json<any>();

  const email: string = userInfo.email;
  const name: string = userInfo.name || userInfo.username || '';
  const avatarUrl: string = userInfo.picture || (
    userInfo.avatar
      ? `https://cdn.discordapp.com/avatars/${userInfo.id}/${userInfo.avatar}.png`
      : ''
  );
  const providerId: string = String(userInfo.id || userInfo.sub || '');

  // Check admin allowlist in D1
  const adminRow = await env.DB.prepare(
    'SELECT id, is_admin FROM users WHERE email = ? AND is_admin = 1'
  ).bind(email.toLowerCase()).first<{ id: number; is_admin: number }>();

  if (!adminRow) {
    return new Response(
      `<!DOCTYPE html><html><head><title>Access Denied</title>
      <style>body{font-family:sans-serif;background:#0a0a0a;color:#fff;display:flex;align-items:center;justify-content:center;height:100vh;margin:0}
      .box{text-align:center;padding:40px;background:#1a1a1a;border-radius:12px}h1{color:#e74c3c;margin-bottom:16px}
      a{color:#667eea}</style></head>
      <body><div class="box"><h1>Access Denied</h1><p>${email || 'Unknown email'} is not authorized.</p>
      <p><a href="/">Back to home</a></p></div></body></html>`,
      { status: 403, headers: { 'Content-Type': 'text/html' } }
    );
  }

  // Upsert user in D1
  await env.DB.prepare(
    `INSERT INTO users (email, name, avatar_url, provider, provider_id, is_admin, last_login)
     VALUES (?, ?, ?, ?, ?, 1, unixepoch())
     ON CONFLICT(email) DO UPDATE SET
       name = excluded.name,
       avatar_url = excluded.avatar_url,
       provider = excluded.provider,
       provider_id = excluded.provider_id,
       last_login = unixepoch()`
  ).bind(email.toLowerCase(), name, avatarUrl, provider, providerId).run();

  // Create session
  const sessionData: SessionData = { email: email.toLowerCase(), name, avatar_url: avatarUrl, provider };
  const sessionId = nanoid();
  await env.KV.put(
    `session:${sessionId}`,
    JSON.stringify(sessionData),
    { expirationTtl: 86400 * 7 }
  );

  return new Response(null, {
    status: 302,
    headers: {
      Location: '/admin',
      'Set-Cookie': `session=${sessionId}; HttpOnly; Secure; SameSite=Lax; Max-Age=604800; Path=/`,
    },
  });
}

export async function handleAuthMe(request: Request, env: Env): Promise<Response> {
  const session = await getSession(request, env);
  if (!session) {
    return new Response(JSON.stringify({ authenticated: false }), {
      headers: { ...corsHeaders, 'Content-Type': 'application/json' },
    });
  }

  return new Response(JSON.stringify({ authenticated: true, user: session }), {
    headers: { ...corsHeaders, 'Content-Type': 'application/json' },
  });
}

export async function handleLogout(request: Request, env: Env): Promise<Response> {
  const sessionId = getSessionIdFromRequest(request);
  if (sessionId) {
    await env.KV.delete(`session:${sessionId}`);
  }

  return new Response(null, {
    status: 302,
    headers: {
      Location: '/admin',
      'Set-Cookie': 'session=; HttpOnly; Secure; SameSite=Lax; Max-Age=0; Path=/',
    },
  });
}

export async function getSession(request: Request, env: Env): Promise<SessionData | null> {
  const sessionId = getSessionIdFromRequest(request);
  if (!sessionId) return null;

  const data = await env.KV.get(`session:${sessionId}`);
  if (!data) return null;

  return JSON.parse(data) as SessionData;
}

function getSessionIdFromRequest(request: Request): string | null {
  const cookieHeader = request.headers.get('Cookie');
  if (!cookieHeader) return null;

  const cookies = cookieHeader.split(';').map(c => c.trim());
  const sessionCookie = cookies.find(c => c.startsWith('session='));
  if (!sessionCookie) return null;

  return sessionCookie.split('=')[1];
}
