# Freemium Strategy: moqpush + moqcdn

## Overview

Open source the MoQ publisher binary. Consolidate all managed hosting on moqcdn.net. Offer free tier (Cloudflare relay + Shaka player) and premium tier (moqcdn relay network + Viper player).

## Architecture

```
moqpush-app (open source)
     |
     | --push-key + --worker-url https://moqcdn.net
     v
moqcdn.net Worker (private)
     |
     +--→ Free tier:    Cloudflare relay + Shaka player
     +--→ Premium tier: moqcdn relay network + Viper player
```

## Repos

### moqpush-open (new, public)

Open source MoQ publisher. Standalone binary, no account needed.

```
moqpush-open/
├── moqpush-app/        # Rust: HTTP CMAF-IF ingest + MoQ publish
└── README.md           # standalone + moqcdn.net usage
```

**Stripped from current moqpush:**
- Remove `moqpush-worker/` — replaced by moqcdn.net
- Remove `ad-insertion.md` — ad insertion done at encoder (Ateme)
- Remove `ad_manager.rs` from moqpush-app — dead code
- No Worker dependencies, no Cloudflare-specific code

**README shows three modes:**
1. **Standalone** (no account): `moqpush-app --relay-url https://your-relay:443 --tracks 2v1a`
2. **Free managed** (moqcdn.net account): `moqpush-app --push-key mpk_XXX --worker-url https://moqcdn.net --tracks 2v1a`
3. **Premium** (same command, premium namespace): same binary, Worker returns premium relay URL + JWT

Users can audit the code, build from source, trust the binary.

### moqpush (existing, private)

Kept as archive. Development moves to moqpush-open. Create a branch `pre-open-source` to preserve current state before stripping.

### moqcdn (existing, private)

The platform. Worker + Viper player + docs.

- Consolidate all customer registration here
- Per-namespace config: relay type (cloudflare/moqcdn), player type (shaka/viper)
- Admin dashboard for namespace management
- JWT signing for premium relay auth

## moqpush.com

Convert to static landing page:
- "Open source MoQ publisher"
- Link to GitHub (moqpush-open)
- How to use standalone (BYO relay)
- How to use with moqcdn.net (free + premium)
- No more Worker, no admin, no player hosting

## moqcdn.net

Single platform for all customers:
- Register / login
- Create namespace → pick tier:
  - **Free**: Cloudflare relay + Shaka player (dropdown: verified free/open CDNs)
  - **Premium**: moqcdn relay network + Viper player
- Push key generated per namespace
- Player at `moqcdn.net/{namespace}` auto-selects Shaka or Viper based on namespace config
- Analytics, stats, relay health (premium)

## Per-Namespace Config (D1)

```json
{
  "namespace": "demo-stream",
  "push_key": "mpk_XXX",
  "relay_type": "cloudflare",
  "relay_url": "https://draft-14.cloudflare.mediaoverquic.com",
  "player": "shaka",
  "tier": "free"
}
```

```json
{
  "namespace": "live-sports",
  "push_key": "mpk_YYY",
  "relay_type": "moqcdn",
  "relay_url": "https://ord.moqcdn.net",
  "player": "viper",
  "tier": "premium",
  "relays": ["ord", "nwj", "lax", "par", "tyo", "maa"],
  "jwt": true
}
```

The `/api/push/auth` response includes relay_url and optionally a JWT. The `/api/broadcasts/:namespace` response includes player type and relay list. Same binary, same API, different experience based on tier.

## What's Open Source

- moqpush-app binary (publisher)
- Shaka player integration (standard, CDN-hosted)
- Protocol (MoQ Transport draft-14 / moq-lite-02 — standard)

## What's Private (the moat)

- Viper player (ABR, relay racing, relay-stats, latency control)
- moqcdn Worker (namespace management, JWT signing, analytics)
- Relay network (6-node global mesh, gossip routing)
- Admin dashboard
- Premium features (relay-stats overlay, relay-assisted ABR, targetLatency control)

## Archived Features

- **Ad insertion** — moved to encoder side (Ateme). Removed from moqpush-app.
- **Warm/pre-position** — unnecessary with gossip mesh (content pulls on subscribe). Tag: `archive/warm-feature`.
- **Directory relay registration** — replaced by Luke's gossip cluster. No external coordinator needed.

## Migration Steps

1. Create `moqpush-open` public repo
2. Copy moqpush-app (minus ad_manager.rs) + new README
3. Update moqcdn Worker: add `relay_type`, `player` fields to broadcasts table
4. Update moqcdn player page: load Shaka or Viper based on namespace config
5. Add relay dropdown to admin (Cloudflare free, Custom URL, moqcdn Premium)
6. Convert moqpush.com Worker to static landing page
7. Point moqpush-app README at moqcdn.net for managed hosting
8. Archive moqpush private repo (branch `pre-open-source`)
