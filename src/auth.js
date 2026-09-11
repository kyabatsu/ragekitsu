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

// ---------------------------------------------------------------------------
// capabilities — what a person may DO, never what they are called
//
// `atLeast(person, 'editor')` scattered through the routes is a permission
// system whose rules are only readable by grepping for a role name, and whose
// every rule has to be edited by hand the day the roles change. The questions
// below are about ACTIONS, and the table underneath is the only place a role
// is mentioned at all. Replacing that table with rows in a database later is
// then a change to this file and to nothing else — which is the point.
//
// NOT `person_grant`. That table answers "may this person see gated content",
// which is about the CONTENT's audience; this is about what the person may do
// to the archive. They look alike and they are different questions, and
// collapsing them would mean a grant that unlocks a restricted clip also
// hands out the ability to retract tags.
//
// ── the two axes ───────────────────────────────────────────────────────────
// There is deliberately no `tag.mint`. Minting is not a separate power from
// suggesting — it is suggesting plus not having to wait, and those are two
// independent questions:
//
//     can(person, 'tag.create')    may they ask for a new tag at all?
//     can(person, 'change.apply')  do their proposals skip the review queue?
//
// A suggester has the first and not the second, so their tag lands in the
// Review panel. An editor has both, so theirs takes effect on submission —
// which is exactly what propose({ autoApply }) has always done, now asked as a
// capability rather than as a role comparison. Encoding "may create" and "may
// create immediately" as two capabilities instead would be two flags that can
// disagree, and the disagreement is unrepresentable in the UI.
// ---------------------------------------------------------------------------

export const CAPABILITIES = [
  // the vocabulary itself
  'tag.create',      // propose a tag that does not exist yet
  'tag.edit',        // rename it, refile it, write its summary, hang art on it
  'tag.retract',     // propose retiring one — tombstone, reversible
  'tag.purge',       // destroy one and everything pointing at it. Not reversible.
  // the link between a tag and a thing
  'tag.attach',      // say this stream/snippet is about that tag
  'tag.detach',      // say it is not, after all — the tag itself survives
  // music — somebody else's video, kept for preservation
  'music.submit',    // put a link forward
  'music.edit',      // correct what the probe found, or the note under it
  'music.retract',   // propose retiring one — tombstone, reversible
  'music.purge',     // destroy the row. Not reversible.
  /* Approving or turning down a SUBMISSION, which is a verdict on an object
     rather than on a field — so it is its own capability and not `change.apply`.
     Kept apart on purpose: "may review music" and "may edit the archive
     directly" are the same people today and are not the same question, and a
     role system that wants one without the other should not have to fork the
     code to get it. */
  'music.decide',
  // the queue
  'change.apply',    // my own proposals take effect without review
  'review.read',     // see other people's
  'review.decide',   // apply or reject them
  // people
  'people.manage',   // change somebody's role
];

/* Role → what it may do. The ONLY place in the archive that names a role in a
   permission decision.

   Written as deltas because that is how they are meant to be read — each role
   is the one below it plus a little more — and flattened once at module load,
   so a lookup is a Set hit rather than a walk. When roles become rows this
   function is what changes; `can()` and every call site stay as they are. */
const GRANTS = (() => {
  const viewer = [];
  /* Everything a suggester can do is a PROPOSAL, because `change.apply` is not
     on this list. That single omission is what puts their tag creations and
     retractions in the Review panel rather than into the archive. */
  const suggester = [...viewer, 'tag.create', 'tag.edit', 'tag.retract',
                     'tag.attach', 'tag.detach',
                     'music.submit', 'music.edit', 'music.retract'];
  const editor = [...suggester, 'change.apply', 'review.read', 'review.decide',
                  'music.decide'];
  /* Purge is admin-only and deliberately does NOT go through the changeset
     system: a changeset's `delete` op tombstones, and a tombstone is the
     reversible thing an editor is trusted with. Destroying rows is a different
     act with a different audience, so it is a different capability. */
  const admin = [...editor, 'tag.purge', 'music.purge', 'people.manage'];
  return {
    viewer: new Set(viewer),
    suggester: new Set(suggester),
    editor: new Set(editor),
    admin: new Set(admin),
  };
})();

/** May this person do this? The one choke point — everything else asks here.
 *
 *  An unknown capability is `false` rather than a throw: a typo'd name should
 *  hide a button, not take the page down. It is also why CAPABILITIES exists —
 *  assertCapabilities() below turns the typo into a startup failure instead,
 *  which is where you want to find it. */
export function can(person, capability) {
  return GRANTS[person?.role ?? 'viewer']?.has(capability) ?? false;
}

/** Every capability this person holds, as a flat object the client can read.
 *
 *  Sent to the browser so the UI can hide what it cannot do. It is a
 *  CONVENIENCE and never the enforcement: the same `can()` runs on the route,
 *  because a hidden button is a hint and a POST is a fact. */
export const capabilities = (person) => ({
  // Deprecated trio, still what most of the page reads. Kept verbatim rather
  // than re-derived from the grants above, so that deploying this file changes
  // no behaviour anywhere on its own. Delete them when the UI has moved.
  suggest: atLeast(person, 'suggester'),
  edit: atLeast(person, 'editor'),
  admin: atLeast(person, 'admin'),
  ...Object.fromEntries(CAPABILITIES.map((c) => [c, can(person, c)])),
});

/** Express middleware: `app.post('/x', requireCap('tag.purge'), handler)`.
 *
 *  Same 401/403 split as requireRole — 401 means "sign in and this may work",
 *  403 means "signed in and it will not" — because that difference is what
 *  lets the page choose between a login button and hiding the affordance. */
export function requireCap(capability) {
  return (req, res, next) => {
    const person = req.person ?? ANON;
    if (can(person, capability)) return next();
    if (!person.id) return res.status(401).json({ error: `sign in required (${capability})` });
    return res.status(403).json({ error: `requires ${capability}; ${person.role} does not have it` });
  };
}

/** Fail at boot on a capability name that nothing grants, or a grant naming a
 *  capability that does not exist. Both are typos, and both otherwise present
 *  as a permission that silently never applies — which reads, from the outside,
 *  exactly like the feature not being built yet. */
export function assertCapabilities() {
  const known = new Set(CAPABILITIES);
  const bad = [];
  for (const [role, set] of Object.entries(GRANTS)) {
    for (const c of set) if (!known.has(c)) bad.push(`${role} grants unknown '${c}'`);
  }
  for (const c of CAPABILITIES) {
    if (!Object.values(GRANTS).some((s) => s.has(c))) bad.push(`'${c}' is granted to nobody`);
  }
  if (bad.length) throw new Error(`capability table is inconsistent: ${bad.join('; ')}`);
}

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
