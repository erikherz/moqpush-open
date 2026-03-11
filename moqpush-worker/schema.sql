-- moqpush D1 Database Schema

CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  avatar_url TEXT,
  provider TEXT NOT NULL,
  provider_id TEXT NOT NULL,
  is_admin INTEGER DEFAULT 0,
  created_at INTEGER DEFAULT (unixepoch()),
  last_login INTEGER DEFAULT (unixepoch())
);

-- Seed superadmin
INSERT OR IGNORE INTO users (email, name, provider, provider_id, is_admin)
VALUES ('erik@vivoh.com', 'Erik Herz', 'seed', 'seed', 1);

-- Namespaces (each has a single push key)
CREATE TABLE IF NOT EXISTS namespaces (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  namespace TEXT UNIQUE NOT NULL,
  push_key TEXT UNIQUE NOT NULL,
  owner_email TEXT NOT NULL,
  relay_url TEXT NOT NULL DEFAULT 'https://draft-14.cloudflare.mediaoverquic.com',
  created_at INTEGER DEFAULT (unixepoch()),
  FOREIGN KEY (owner_email) REFERENCES users(email)
);

CREATE INDEX IF NOT EXISTS idx_namespaces_namespace ON namespaces(namespace);
CREATE INDEX IF NOT EXISTS idx_namespaces_push_key ON namespaces(push_key);

-- Pre-position regions: which puller nodes to auto-activate per namespace
CREATE TABLE IF NOT EXISTS namespace_regions (
  namespace TEXT NOT NULL,
  node TEXT NOT NULL,
  PRIMARY KEY (namespace, node),
  FOREIGN KEY (namespace) REFERENCES namespaces(namespace) ON DELETE CASCADE
);

-- Active broadcast directory: which namespace is live
CREATE TABLE IF NOT EXISTS broadcast_directory (
  namespace TEXT PRIMARY KEY,
  relay_url TEXT NOT NULL,
  publisher_instance TEXT,
  started_at INTEGER DEFAULT (unixepoch()),
  heartbeat_at INTEGER DEFAULT (unixepoch())
);

-- Puller nodes (moqpush-puller servers)
CREATE TABLE IF NOT EXISTS pullers (
  node TEXT PRIMARY KEY,
  url TEXT NOT NULL,
  region TEXT NOT NULL,
  active_namespaces TEXT DEFAULT '[]',
  registered_at INTEGER DEFAULT (unixepoch()),
  heartbeat_at INTEGER DEFAULT (unixepoch())
);

-- Pre-seed known puller nodes
INSERT OR IGNORE INTO pullers (node, url, region)
VALUES
  ('naw', 'https://naw.moqpush.com', 'us-west'),
  ('nac', 'https://nac.moqpush.com', 'us-central'),
  ('nae', 'https://nae.moqpush.com', 'us-east'),
  ('eu1', 'https://eu1.moqpush.com', 'eu-west'),
  ('in1', 'https://in1.moqpush.com', 'ap-south');
