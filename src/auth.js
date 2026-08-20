// Roles and identity — deliberately thin.
//
// ┌────────────────────────────────────────────────────────────────────────┐
// │  THIS IS THE FILE YOU REPLACE.                                         │
// │                                                                        │
// │  `identify(db, req)` turns a request into { id, handle, role }. Right  │
// │  now it reads a bearer token or cookie against the `session` table.    │
// │  Swap the body for Discord/Twitch OAuth, Cloudflare Access headers, or │
// │  anything else — as long as it returns the same shape, nothing else in │
// │  the project changes.                                                  │
// └────────────────────────────────────────────────────────────────────────┘
//
// Roles are a total order, so every check is "at least this":
//     viewer  <  suggester  <  editor  <  admin

import { createHash, randomBytes } from 'node:crypto';
import { now, ulid } from './db.js';

export const ROLES = ['viewer', 'suggester', 'editor', 'admin'];
const RANK = Object.fromEntries(ROLES.map((r, i) => [r, i]));

export const COOKIE = 'tenma_session';
export const TTL = 30 * 24 * 3600;

export const ANON = Object.freeze({
  id: null, handle: 'anonymous', role: 'viewer', provider: 'anon',
});

export const atLeast = (person, role) => RANK[person.role] >= RANK[role];

export const capabilities = (person) => ({
  suggest: atLeast(person, 'suggester'),
  edit: atLeast(person, 'editor'),
  admin: atLeast(person, 'admin'),
});

// Sessions are stored hashed. A dumped database must not hand over live logins.
const hashToken = (t) => createHash('sha256').update(t).digest('hex');

function tokenFrom(req) {
  const auth = req.get?.('authorization') ?? req.headers?.authorization;
  if (auth && auth.toLowerCase().startsWith('bearer ')) return auth.slice(7).trim();
  const cookie = req.headers?.cookie;
  if (!cookie) return null;
  for (const part of cookie.split(';')) {
    const [k, ...v] = part.trim().split('=');
    if (k === COOKIE) return decodeURIComponent(v.join('='));
  }
  return null;
}

// ---------------------------------------------------------------------------
//  ▼ REPLACE THIS ▼
// ---------------------------------------------------------------------------

/** Who is making this request? Returns ANON when nobody is signed in. */
export function identify(db, req) {
  const token = tokenFrom(req);
  if (!token) return ANON;
  const row = db.prepare(
    `SELECT p.id, p.handle, p.role, p.provider, p.display_name, p.avatar_url, p.banned
     FROM session s JOIN person p ON p.id = s.person_id
     WHERE s.token_hash = ? AND s.expires_at > ?`).get(hashToken(token), now());
  if (!row || row.banned) return ANON;
  return row;
}

// ---------------------------------------------------------------------------
//  ▲ REPLACE THIS ▲   everything below is plumbing you can keep
// ---------------------------------------------------------------------------

/** Find or create a person for an external identity, and return their id.
 *  The first person ever to sign in becomes admin — otherwise there is no way
 *  to grant the first role without hand-editing the database. */
export function upsertPerson(db, { provider, providerUid, handle,
                                   displayName = null, avatarUrl = null,
                                   defaultRole = 'suggester' }) {
  const t = now();
  const existing = db.prepare(
    'SELECT id FROM person WHERE provider = ? AND provider_uid = ?')
    .get(provider, String(providerUid));
  if (existing) {
    db.prepare(`UPDATE person SET handle=?, display_name=?, avatar_url=?,
                last_seen_at=? WHERE id=?`)
      .run(handle, displayName, avatarUrl, t, existing.id);
    return existing.id;
  }
  const first = db.prepare('SELECT COUNT(*) c FROM person').get().c === 0;
  const id = ulid();
  db.prepare(`INSERT INTO person(id, provider, provider_uid, handle, display_name,
                avatar_url, role, created_at, last_seen_at)
              VALUES(?,?,?,?,?,?,?,?,?)`)
    .run(id, provider, String(providerUid), handle, displayName, avatarUrl,
         first ? 'admin' : defaultRole, t, t);
  return id;
}

export function issueSession(db, personId, agent = '') {
  const token = randomBytes(32).toString('base64url');
  const t = now();
  db.prepare(`INSERT INTO session(token_hash, person_id, created_at, expires_at, user_agent)
              VALUES(?,?,?,?,?)`).run(hashToken(token), personId, t, t + TTL, agent.slice(0, 200));
  db.prepare('DELETE FROM session WHERE expires_at < ?').run(t);   // opportunistic sweep
  return { token, expiresAt: t + TTL };
}

export function revokeSession(db, token) {
  if (token) db.prepare('DELETE FROM session WHERE token_hash = ?').run(hashToken(token));
}

export const sessionToken = tokenFrom;

/** Express middleware: `app.post('/x', requireRole('editor'), handler)`.
 *
 *  401 when nobody is signed in, 403 when someone is but lacks the role — the
 *  distinction is what lets a frontend choose between showing a login button
 *  and hiding an affordance entirely. */
export function requireRole(role) {
  return (req, res, next) => {
    const person = req.person ?? ANON;
    if (atLeast(person, role)) return next();
    if (!person.id) {
      return res.status(401).json({ error: `sign in required (${role} or higher)` });
    }
    return res.status(403).json({ error: `requires ${role}; you have ${person.role}` });
  };
}
