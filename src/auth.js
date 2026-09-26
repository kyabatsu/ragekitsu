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
// A ROLE IS A NAMED BAG OF CAPABILITIES, WITH NO ORDERING.
//
// It used to be a rung. This file said "roles are a total order, so every
// check is at least this: viewer < suggester < editor < admin", and every
// route asked its question that way. That was an accurate reading of the code
// and the wrong reading of the intent — those four names were placeholders for
// roles somebody would build, and `editor2: can edit tags, can upload memes`
// has no position on a ladder.
//
// So, as of 17 Sep: the four are ROWS in `role` and `role_grant`, seeded from
// the arrays below on first run and editable after. `RANK`, `atLeast` and the
// ladder assertion are gone. Nothing anywhere compares two roles; the only
// question anybody asks is `can(person, capability)`, and the answer is a set
// lookup.
//
// `viewer` keeps one special meaning and only one: it is the FLOOR.
// `person.role` defaults to it, a request nobody identified is treated as
// holding it, and it is seeded with no capabilities whatsoever — which is what
// makes "everybody, signed in or not, starts from the same place" a fact
// rather than an intention. Gates subtract from that on top; see
// `person_grant`, which is the other axis.

import { createHash, randomBytes } from 'node:crypto';
import { now, ulid } from './db.js';

/** The role every request starts from, and the only name with a meaning. */
export const FLOOR = 'viewer';

export const COOKIE = 'tenma_session';
export const TTL = 30 * 24 * 3600;

export const ANON = Object.freeze({
  id: null, handle: 'anonymous', role: 'viewer', provider: 'anon',
});

/* ── what a scoped token does, now that there is no rank ───────────────────
 *
 * `atLeast(person, role)` lived here, with a long note about why it refuses a
 * scoped token at every rung above viewer. Both are gone: the ladder, and the
 * function that walked it.
 *
 * The property that note was protecting is NOT gone, and it moved into `can()`
 * where it always belonged. A SCOPED token — one minted for a script rather
 * than held by a browser — may do the intersection of its owner's
 * capabilities and its own scope list, never the union. The old hazard was
 * that `atLeast` was a SECOND door the scope could not narrow, so an
 * upload-only token would have sailed through every `requireRole` route while
 * being correctly refused at every `requireCap` one. There is no second door
 * any more: every route and every in-handler check asks a capability, so the
 * scope narrows all of them, which is what it was always supposed to mean.
 *
 * Nine in-handler call sites read `atLeast` when this was written. Three were
 * really asking a capability and now ask it by name; four were the gate
 * bypass and ask `gate.bypass`; two were asking "is there an identified person
 * here at all", which is `!!req.person?.id` and never needed a rank —
 * `atLeast(person, 'viewer')` was true for anybody with a known role, so the
 * rank half of those two expressions never decided anything.
 */

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
// ── 2026-09-17: the warning above was right and its conclusion no longer is ─
// What it is actually against is an AMBIENT grant — a row with no payload,
// read by two subsystems that each decide for themselves what holding it
// means. That hazard is real and it is why the tables were kept apart. But the
// fix is not two tables; it is making the grant NAME WHAT IT CONFERS, which
// `person_grant.capability` now does. "Unlocks a restricted clip" and "may
// retract tags" are different values in one column rather than two readings of
// one row, so they cannot be confused — and a scoped grant reaches a decision
// only where a PREDICATE exists for its capability, never through this table.
// The missing column was the hazard. See schema.sql `person_grant`.
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
  /* Putting a clip or a picture in at all — the route the browser's upload
     window uses, and the one an API push uses.
     It exists because a SCOPED token can only reach a route that asks a
     capability: `atLeast` refuses a scoped token every rank above viewer, on
     purpose, so `requireRole('suggester')` is a door no token can open. Naming
     the action is what makes "a token that may only upload" expressible at
     all. Granted from suggester up, which is exactly who could already reach
     that route — so the browser sees no change whatsoever. */
  'snippet.upload',
  // music — somebody else's video, kept for preservation
  'music.submit',    // put a link forward
  'music.edit',      // correct what the probe found, or the note under it
  'music.retract',   // propose retiring one — tombstone, reversible
  'music.purge',     // destroy the row. Not reversible.
  /* Putting a FILE in, rather than a link to somebody else's.
     Its own capability and not part of `music.submit`, because it is not the
     same act: submitting names a public video and costs the archive a metadata
     read, where this hands the server bytes it then keeps forever. The whole
     reason the module is safe to open to a suggester is that a link is a claim
     about something already public — remove that and what is left is an upload
     endpoint, which is the one thing this archive has always refused below the
     top of the ladder.
     Stricter than the file rule elsewhere on purpose: clips and pictures stop
     at editor, this stops at admin. There are five or six of these ever — a
     concert that went private and exists nowhere else — and a door used twice
     a year should be the narrowest one in the building. */
  'music.upload',
  /* Approving or turning down a SUBMISSION, which is a verdict on an object
     rather than on a field — so it is its own capability and not `change.apply`.
     Kept apart on purpose: "may review music" and "may edit the archive
     directly" are the same people today and are not the same question, and a
     role system that wants one without the other should not have to fork the
     code to get it. */
  'music.decide',
  /* Pasting a link whose SOURCE the archive has not vouched for.
     A Discord attachment URL is a file somebody put somewhere — which server
     and which channel is the whole of what separates it from an unrestricted
     upload endpoint, so links from there are held to a list of channels
     somebody named on purpose. This is the exemption, and it belongs to the
     people who can already put a file in by hand: gating them buys no safety
     and costs them every thread and forum post, which are separate channels
     with ids nobody can enumerate in advance. */
  'link.any',
  // the queue
  'change.apply',    // my own proposals take effect without review
  'review.read',     // see other people's
  'review.decide',   // apply or reject them
  // people, and the roles they are assigned
  'people.manage',   // change somebody's role
  /* Create a role, rename one, tick and untick what it may do, delete it.
     THE POWER THAT CAN GRANT EVERY OTHER, so worth being plain about: anybody
     holding this can write themselves a role with anything in it, and
     `roles.manage` is therefore equivalent to full control of the archive by
     one extra step. There is no arrangement of capabilities that makes it less
     than that, so it is not pretended otherwise anywhere — what the routes do
     instead is refuse to let somebody hand out MORE than they themselves
     hold, which is the same rule `/api/auth/tokens` already follows, and which
     keeps the step from being invisible.
     Separate from `people.manage` because they are separate acts and the
     second is the common one: deciding what a role means is rare, putting
     somebody in one is not. */
  'roles.manage',

  /* ── the twenty that used to be ranks ─────────────────────────────────────
   *
   * Until 17 Sep, 42 of the 91 routes put up `requireRole` and there was no
   * capability to name instead. That was fine while a role was a rung; it is
   * not fine once a role is a named BAG of capabilities somebody assembles in
   * the UI — `editor2: can edit tags, can upload memes` — because a rank is
   * not something you can tick, and because `atLeast` refuses a scoped token
   * at every rung above viewer, so a rank-gated route is a route no token can
   * ever reach.
   *
   * So every one of those routes now asks for a power. The grants below give
   * each new name to exactly the roles that could already reach its route, so
   * this step changes no behaviour whatsoever — it changes what the system can
   * SAY. Verified: `assertRoutes` refuses to boot if any route's door and the
   * manifest disagree, and `rankparity.mjs` proves every new capability is
   * held by exactly the ranks that used to pass.
   *
   * Grouped by the thing a person is being trusted with, because that is how
   * a checklist reads. */

  // proposing, and the keys to do it with a script
  'change.propose',  // submit a changeset at all — per-field authority is CHANGE_CAPS
  'token.manage',    // mint and revoke your own API tokens

  /* ── what a proposal is ALLOWED to say ────────────────────────────────────
   *
   * `change.propose` is the door of `/api/changesets`; these are what a change
   * inside it may touch, asked per row by `CHANGE_CAPS` in archive.js. The
   * six below were added 17 Sep and they are the last of the ten writable
   * types to get a name — until then a proposal naming a stream, a capture, a
   * note, a chapter or a clip's title needed no capability at all, so holding
   * `change.propose` was holding all five.
   *
   * That was invisible while a role was a rung, because everyone who could
   * reach the route could do all of it. It is the first thing that breaks once
   * somebody assembles a role: "may write chapters, may not retitle a
   * broadcast" had no way to be said.
   *
   * Granted from suggester up, all six, which is exactly who could reach
   * `/api/changesets` already — so this names powers rather than moving any. */
  'stream.edit',     // a broadcast's record: title, clocks, duration, its captures
  'stream.retract',  // take one out of the index — tombstone, reversible
  'note.write',      // write, tick off and remove the notes on a stream
  'chapter.edit',    // draw, move and remove chapters and sub-chapters
  'snippet.edit',    // a clip or picture's own title, description and filing
  'snippet.retract', // take one out of the archive — tombstone, reversible

  // the transcript, which is the archive's text
  'transcript.edit', // correct a word, or rewrite the whole thing by hand
  'transcript.run',  // ask the machine for one: whisper, or OCR again

  // art and metadata that come from a FILE or from somebody else's catalogue
  'stream.poster',   // upload art for a broadcast
  'stream.rescan',   // tell the recorder to re-read a stream from its files
  'tag.art',         // upload art for a tag
  'tag.harvest',     // look a tag up in the games catalogue, and take a candidate
  'media.browse',    // list what is on the server's own disk
  'note.export',     // turn a stream's notes into a subtitle file
  'clip.cut',        // cut a range out of a master, and download it once

  // the review queue's other pile: hints a submitter typed
  'suggestion.decide',

  // the work queue
  'job.read',        // see what the Pi is doing
  'job.create',      // ask it to do something
  'job.control',     // cancel or retry somebody's job

  /* The machine room. Two boxes rather than six, because "operate the
     infrastructure" is one kind of trust: the model files, the transcription
     and OCR pause switches, the recorder's own status and its sweep. Split
     read from write, since watching the queues is not the same as changing
     what they run. */
  'ops.read',
  'ops.manage',

  /* The gate system, and the two halves are deliberately separate powers.
     Which tags restrict is a decision about the ARCHIVE; who holds a gate is a
     decision about a PERSON, and it is the sharper of the two — the whole
     point of a gate is that somebody trusted with broad write access still
     must not see what is behind it. */
  'gate.set',        // flag a tag as gating, or clear it
  'grant.manage',    // hand somebody a gate, or take it back
  /* Seeing everything regardless of gates, which used to be `atLeast(person,
     'editor')` written out at four in-handler sites and inside the
     `content.view` predicate. It is a capability now for one reason: a rung
     cannot be untucked, and this is the tick that has to come OFF the editor
     role for a gate to mean what it is supposed to mean. Seeded to the same
     two roles the rung admitted, so step C moves nobody; step E is where it
     becomes a per-person grant instead of a role's tick. */
  'gate.bypass',

  /* Destroying a snippet, completing the set beside tag.purge and
     music.purge. Not reversible, and it is guarded twice: the row has to be
     unlisted first. */
  'snippet.purge',

  /* Deleting one of two recordings of the same broadcast — the largest and
     least reversible thing in the archive, and the only capability that
     reaches a MASTER rather than a derivative.
     Its own name rather than reusing `snippet.purge`, because the two are not
     the same risk: a snippet is a clip somebody uploaded and can upload
     again, and this is a five-hour recording of a stream that happened once.
     A role may hold one without the other, and the day somebody is given the
     clip queue to run, that distinction is the whole point.
     Guarded twice like the rest: the archive refuses unless it has SEEN a
     second copy on disk, and the recorder refuses any path outside the media
     tree. */
  'capture.purge',
];

/* ── the four roles this archive starts with ───────────────────────────────
 *
 * FIRST-RUN DEFAULTS, and nothing more. These are written into `role` and
 * `role_grant` the first time an archive boots without them, and are never
 * written again — so unticking something in the role editor stays unticked
 * across a restart, which is the whole reason the rows exist.
 *
 * Three of them are lists. The fourth is not: `admin` is SOVEREIGN, which
 * means it has no list, holds every capability the build has, and cannot have
 * anything taken off it. See `role.sovereign` in schema.sql for why that is a
 * property of the data's absence rather than a rule about its content.
 *
 * The three lists still happen to NEST, because they grew out of a ladder and
 * this is a faithful snapshot of the day it was retired. That nesting is
 * history, not structure: nothing reads it, no code compares two roles, and
 * the boot check that used to insist on it is gone.
 *
 * `viewer` is the floor and holds NOTHING. That is load-bearing — a stranger
 * and a signed-in reader get the same starting point, and everything above is
 * something somebody was given. */
const SEED_ROLES = (() => {
  const viewer = [];
  /* Everything a suggester can do is a PROPOSAL, because `change.apply` is not
     on this list. That single omission is what puts their tag creations and
     retractions in the Review panel rather than into the archive. */
  const suggester = [...viewer, 'tag.create', 'tag.edit', 'tag.retract',
                     'tag.attach', 'tag.detach', 'snippet.upload',
                     'music.submit', 'music.edit', 'music.retract',
                     /* Both were `requireRole('suggester')` doors. `change.propose`
                        is the one every proposal goes through; the per-field
                        authority inside it is CHANGE_CAPS and is unchanged. */
                     'change.propose', 'token.manage',
                     /* The six powers a proposal may name. Nothing moved:
                        `change.propose` already carried all of them, because
                        CHANGE_CAPS had no row for any of their types. */
                     'stream.edit', 'stream.retract', 'note.write',
                     'chapter.edit', 'snippet.edit', 'snippet.retract'];
  const editor = [...suggester, 'change.apply', 'review.read', 'review.decide',
                  'music.decide', 'link.any',
                  /* The twelve editor-rung routes, by name. Same people as
                     before this line existed — what changed is that a role
                     which is not "editor" can now be given any subset. */
                  'transcript.edit', 'transcript.run', 'stream.poster',
                  'stream.rescan', 'tag.art', 'tag.harvest', 'media.browse',
                  'note.export', 'clip.cut', 'suggestion.decide',
                  'job.read', 'job.create'];
  return [
    { slug: 'viewer',    name: 'Viewer',    capabilities: viewer },
    { slug: 'suggester', name: 'Suggester', capabilities: suggester },
    { slug: 'editor',    name: 'Editor',    capabilities: editor },
    /* No list, on purpose. `capabilities` would be a lie here — nothing reads
       it for a sovereign role, `installRoles` writes no rows for one, and a
       list sitting in the source that the system ignores is the kind of thing
       somebody edits expecting an effect. */
    { slug: 'admin',     name: 'Admin',     sovereign: true },
  ];
})();

/** Capabilities no SEEDED non-sovereign role holds — so, in practice, the
 *  owner's own.
 *
 *  This list exists to keep a check that has already earned its place. Until
 *  today `assertCapabilities` refused to boot on a capability that appeared in
 *  no seeded role, and it caught two real mistakes: a name minted in step A
 *  without a grant, and `gate.bypass` in step C. A sovereign admin makes that
 *  check vacuous — everything is held by somebody now, always — so what it
 *  becomes is this: a capability nobody but the owner holds has to be SAID to
 *  be one.
 *
 *  Which is the same forcing function aimed at the decision that is actually
 *  left. Minting a name still makes the boot fail until somebody decides
 *  where it goes, and the two answers are "put it in a seeded role" and "add
 *  it here". What it no longer does is let the answer be silence.
 *
 *  It is a claim about the SEEDS and says nothing about the live table: a role
 *  somebody builds may hold any of these, subject to the one rule in the role
 *  editor — you cannot hand out what you do not hold. */
const ADMIN_ONLY = [
  // destroying things, which is the other half of every retract
  'tag.purge', 'music.purge', 'snippet.purge', 'capture.purge',
  // handing the server bytes it keeps forever, with no link to vouch for them
  'music.upload',
  // who somebody is, and what a role means
  'people.manage', 'roles.manage',
  // the machine room and the work queue's dangerous end
  'ops.read', 'ops.manage', 'job.control',
  // the gate system's two halves
  'gate.set', 'grant.manage',
  /* ── and the tick that defeats every gate ─────────────────────────────────
   *
   * Moved here from the editor role in step E, which is the one behaviour
   * change the whole part was built for. It was `atLeast(person, 'editor')`
   * before step C, a capability after it, and belongs to nobody-by-role now.
   *
   * The reason, in the archive owner's words: one of the gates names the
   * performer's previous life, and somebody with a good work ethic — her mod,
   * a friend — still should not be able to see it. A gate that a trusted
   * editor can see through is not a gate, and every version of this before
   * today had the editor role holding the bypass for no reason except that the
   * rung it replaced did.
   *
   * What opens a gate now is a GRANT — one person, one gate, `person_grant`
   * with `scope_gate` — which is the axis gates have always been on and the
   * only one where "trusted for this, not for that" can be said.
   *
   * The sovereign still bypasses, and must: it holds every capability there
   * is, and an owner who cannot see their own archive cannot administer it.
   * That is the one bypass left and it belongs to one role.
   *
   * It stays GRANTABLE — an owner may tick it onto a role they build, for a
   * co-owner — and the role editor marks it, because it is the only capability
   * in the vocabulary that removes a restriction rather than adding a power. */
  'gate.bypass',
];

/** Capabilities the role editor marks, and what it says about them.
 *
 *  One entry, and the mechanism exists rather than the special case being
 *  typed into the page: every hand-written list of capabilities in this
 *  project has eventually disagreed with the real one, and a warning that
 *  lives in `index.html` is a warning that survives the capability being
 *  renamed. Sent with the vocabulary by `GET /api/roles`.
 *
 *  The bar for an entry here is not "dangerous" — most of this vocabulary is
 *  dangerous, that is what it is for. It is "does not do what the checklist
 *  implies": every other tick grants a power over the archive, and this one
 *  removes a restriction on what its holder may SEE. */
export const CAP_WARNINGS = {
  'gate.bypass': 'Sees through every gate, including ones added later. '
    + 'The usual way to let somebody past one gate is to grant them that gate '
    + 'under Gate ownership — this is not that.',
};

/* ── the live registry ─────────────────────────────────────────────────────
 *
 * slug → { name, sovereign, caps }, and it is what `can()` reads. Near enough
 * the shape the hardcoded table had, which is why nothing else in the archive
 * changed when the source of it became rows: `can(person, capability)` is
 * still a lookup on a name, and every call site — including every test that
 * passes a bare `{ role: 'editor' }` — reads the same.
 *
 * It starts as the SEEDS so that code with no database in front of it has a
 * truthful answer: the suites, a one-off script, `capabilities()` called
 * before a connection exists. `loadRoles(db)` then replaces it with the rows,
 * which are authoritative from that moment. The seeds being the table's own
 * first content is what keeps those two from being two sources of truth, and
 * `rolerows.mjs` asserts a freshly seeded database matches them exactly. */
const KNOWN_CAPS = new Set(CAPABILITIES);
const asRow = (r) => ({
  name: r.name ?? r.slug,
  sovereign: !!r.sovereign,
  caps: new Set(r.sovereign ? [] : (r.capabilities ?? [])),
});
let ROLE_ROWS = new Map(SEED_ROLES.map((r) => [r.slug, asRow(r)]));

/** Every role that exists, floor first, then the rest alphabetically.
 *
 *  This replaced `ROLES`, and being a CALL is the point of it: the old export
 *  was a frozen four-element array in ladder order, and anything holding onto
 *  it would go on believing there were four of them forever. There is no
 *  order any more — the sort is for a stable listing, not a hierarchy. */
export const roles = () => {
  const out = [...ROLE_ROWS.keys()].filter((r) => r !== FLOOR).sort();
  return ROLE_ROWS.has(FLOOR) ? [FLOOR, ...out] : out;
};

/** Does this role hold everything, unconditionally? */
export const isSovereign = (slug) => !!ROLE_ROWS.get(slug)?.sovereign;

/** What one role may do, as a plain array. For the role editor and for tests.
 *
 *  A sovereign role answers with the whole vocabulary, COMPUTED — not read
 *  from rows, because it has none. So a capability added in a future version
 *  turns up here the moment its name exists, and no row can go missing. */
export const roleCaps = (slug) => {
  const r = ROLE_ROWS.get(slug);
  if (!r) return [];
  return r.sovereign ? [...CAPABILITIES].sort() : [...r.caps].sort();
};

/** A role's display name, or its slug when it has none. */
export const roleName = (slug) => ROLE_ROWS.get(slug)?.name ?? slug;

/** What this person may hand out — to a role, or in a token's scope.
 *
 *  Exactly what they hold, which is the one rule that keeps `roles.manage`
 *  from being a way to mint authority out of nothing. A sovereign holds
 *  everything, so this does not narrow them; anybody else editing a role can
 *  only ever move powers sideways. */
export const grantableBy = (person) =>
  CAPABILITIES.filter((c) => can(person, c));

/** Write the seeded roles into a database that does not have them yet.
 *
 *  Per role, and only when the ROW is absent: an archive that has had its
 *  editor role edited must not have the original list put back under it on the
 *  next restart, and re-inserting the grant rows would do exactly that. So the
 *  row's existence is the flag, and a role somebody has emptied on purpose
 *  stays empty.
 *
 *  A SOVEREIGN role gets its row and no grant rows at all — there is nothing
 *  to store, which is the whole point of it.
 *
 *  Idempotent, and safe to call on every boot. Returns
 *  `{ made, repaired }` — the slugs it created, and the slugs whose
 *  sovereignty it had to put back. */
export function installRoles(db, by = null) {
  const t = now();
  const made = [];
  /* Slugs whose sovereignty was put back — see the repair below. Reported on
     the boot banner, because it means the archive arrived in a state it should
     not have been in and somebody may want to know why. */
  const repaired = [];
  const has = db.prepare('SELECT 1 FROM role WHERE slug = ?');
  const addRole = db.prepare(
    `INSERT INTO role(slug, name, builtin, sovereign, created_at, created_by)
     VALUES(?,?,1,?,?,?)`);
  const addCap = db.prepare(
    `INSERT INTO role_grant(role_slug, capability, granted_at, granted_by)
     VALUES(?,?,?,?)`);
  for (const r of SEED_ROLES) {
    if (has.get(r.slug)) continue;
    addRole.run(r.slug, r.name, r.sovereign ? 1 : 0, t, by);
    /* `?? []` and NOT a check on `r.sovereign`, which is what this said until a
       mutation showed the branch was unreachable: `assertCapabilities` refuses
       to boot on a sovereign seed that carries a `capabilities` list, so there
       is nothing for the branch to skip. Two guards where one decides is a
       guard that can be wrong forever without anybody noticing, so the one
       that is checked keeps the job. */
    for (const c of (r.capabilities ?? [])) addCap.run(r.slug, c, t, by);
    made.push(r.slug);
  }

  /* ── and the flag, repaired if it is missing ──────────────────────────────
   *
   * An archive with NO sovereign role is the state this whole flag exists to
   * prevent: nobody holds everything, every capability minted later has to be
   * granted by hand, and one bad DELETE leaves nobody who can undo it. The
   * seeding above cannot fix it, deliberately — it only ever writes a row that
   * is ABSENT, so that an edited role keeps its edits — and the `admin` row is
   * present in exactly the case that matters: an archive from the day roles
   * became rows and sovereignty did not yet exist.
   *
   * The migration in db.js sets it for that case. This is the second, cheaper
   * answer to the same question, and it is here because the first one is a
   * one-shot: a database that misses the migration, or has the flag cleared by
   * hand, would otherwise come up permanently un-administrable.
   *
   * Narrow on purpose. It only ever acts when NOTHING is sovereign, and only
   * on a builtin row whose slug the seeds call sovereign — so a role somebody
   * built themselves and happened to call admin is never touched, and an
   * archive that already has a sovereign is left exactly as it is. */
  const sov = db.prepare('SELECT count(*) c FROM role WHERE sovereign = 1').get().c;
  if (!sov) {
    for (const r of SEED_ROLES.filter((x) => x.sovereign)) {
      const fixed = db.prepare(
        'UPDATE role SET sovereign = 1 WHERE slug = ? AND builtin = 1').run(r.slug);
      if (fixed.changes) {
        db.prepare('DELETE FROM role_grant WHERE role_slug = ?').run(r.slug);
        repaired.push(r.slug);
      }
    }
  }
  return { made, repaired };
}

/** Replace the registry from the database. Call after any role edit.
 *
 *  Returns `{ roles, sovereign, unknown }`. `unknown` is the capability rows
 *  naming something `CAPABILITIES` does not, which are SKIPPED rather than
 *  fatal — see the schema comment on `role_grant`. Such a row cannot grant
 *  anything, because `can()` is only ever asked about names the code uses, so
 *  it is inert; refusing to boot on inert data would mean a capability rename
 *  could brick a live archive.
 *
 *  `sovereign` is reported because there should be exactly one and the boot
 *  banner is where somebody would notice otherwise. Zero of them is the state
 *  the flag exists to prevent — an archive nobody can fully administer — and
 *  two is a deliberate hand-edit.
 *
 *  A database with no role rows at all leaves the seeds in place rather than
 *  emptying the registry, which is the fail-SAFE direction for the one case it
 *  covers: a connection opened by a tool that never called `installRoles`. An
 *  archive whose roles somebody deleted outright is not a case this can tell
 *  apart, and `installRoles` runs first on every boot precisely so it cannot
 *  arise. */
export function loadRoles(db) {
  const rows = db.prepare('SELECT slug, name, sovereign FROM role').all();
  if (!rows.length) {
    return { roles: roles(), sovereign: roles().filter(isSovereign), unknown: [] };
  }
  const next = new Map(rows.map((r) => [r.slug, {
    name: r.name ?? r.slug, sovereign: !!r.sovereign, caps: new Set(),
  }]));
  const unknown = [];
  const ignored = [];
  for (const g of db.prepare('SELECT role_slug, capability FROM role_grant').all()) {
    const r = next.get(g.role_slug);
    if (!r) continue;                              // orphan; the FK forbids it
    /* A sovereign role's rows are ignored rather than merged, and REPORTED —
       which is the difference between defensive code and dead code. Nothing
       depends on the skip, because `can()` and `roleCaps()` both answer from
       the vocabulary for a sovereign role, so a merged row would change no
       decision at all. What it would do is sit in the table looking like the
       truth, and mislead the next person to read it with SQL. So it is named
       on the boot banner and the row is the thing to delete.
       `installRoles` writes none of these and the migration clears any that
       predate the flag, so reaching this is a hand-edited file. */
    if (r.sovereign) { ignored.push(`${g.role_slug}:${g.capability}`); continue; }
    if (!KNOWN_CAPS.has(g.capability)) { unknown.push(`${g.role_slug}:${g.capability}`); continue; }
    r.caps.add(g.capability);
  }
  ROLE_ROWS = next;
  return { roles: roles(), sovereign: roles().filter(isSovereign), unknown, ignored };
}

/* ── the held set, and why it travels on the person ───────────────────────
 *
 * A gate grant is a row in `person_grant`, which is a database this file has
 * no connection to and should not acquire one. So the grants ride on the
 * person, resolved once per request by whoever resolved the identity — the
 * same principle `server.js` already states over `req.person = identify(...)`:
 * one answer per request, read by every guard.
 *
 * Missing means EMPTY and therefore means DENIED for anything gated, never
 * granted. That direction is the whole safety of the arrangement: a caller who
 * forgets to load grants gets content hidden from people who should see it,
 * which someone reports within the hour, rather than shown to people who
 * should not, which nobody reports at all. */
const heldBy = (person) => {
  const g = person?.grants;
  if (!g) return EMPTY_HELD;
  return g instanceof Set ? g : new Set(g);
};
const EMPTY_HELD = new Set();

/* ── object predicates: one per capability, and they do not share a rule ───
 *
 * The plan's warning, kept as code rather than as prose: `content.view` is
 * default OPEN and ALL-of, and the scoped rights coming in step seven are
 * default CLOSED and ANY-of. Different default, different quantifier. A single
 * gate-shaped function serving both is exactly how a default-closed right
 * inherits a default-open answer, so each capability brings its own predicate
 * and the table below is the only thing they have in common.
 *
 * A capability with no entry here has NO object-scoped answer, and asking for
 * one is a refusal rather than a fall-through to the bare form. That is the
 * arity trap closed: "may I edit THIS one" must never be answered by "you may
 * edit things in general".
 *
 * ── bare-less and bare-gated, which is the distinction that matters ───────
 * A name with a predicate may or may not ALSO be in CAPABILITIES, and the two
 * cases compose differently:
 *
 *   BARE-GATED  in CAPABILITIES too. The bare form is "could you ever", which
 *               is what the UI renders a tab from; the object form narrows it
 *               to one row. `can()` requires BOTH, because an object answer
 *               that skipped the role and scope check would be a scoped grant
 *               that outranked the role it hangs off. This is the shape step
 *               seven's `snippet.replace` needs, and the composition is built
 *               now so that step does not have to remember it.
 *
 *   BARE-LESS   predicate only, which is `content.view`. Asked without a row
 *               it has no meaningful answer — everyone may view content, that
 *               is what an archive is — so it is deliberately NOT in
 *               CAPABILITIES and the predicate is the whole rule.
 *
 * Keeping `content.view` out of CAPABILITIES is the one judgement call in this
 * step, and it goes against the plan's wording ("one new capability"). The
 * reason: the bare form would have to answer, and every answer available is
 * wrong. Granted to `viewer` it is `true` for everybody — a key the page ships
 * and never reads. And it would be `false` for a SCOPED TOKEN, because the
 * bare form intersects with the token's scope while viewing must not. A
 * capability whose bare answer contradicts its object answer for the same
 * caller does not belong in the list whose whole contract is that bare answers
 * mean something — and that contradiction is not hypothetical, it is the exact
 * shape of the bug step four fixed. The plan's real requirement, one name that
 * `person_grant` rows can point at, is met either way.
 *
 * A Map and not an object literal, which is not fussiness. `PREDICATE[cap]` on
 * an object finds inherited members, so `can(person, 'toString', row)` would
 * call `Object.prototype.toString` and hand "[object Object]" to a caller who
 * wrote `if (can(...))` — a truthy string is a yes. Measured, it is worse than
 * that: `can(person, 'valueOf', row)` THROWS, because the extracted method
 * gets no `this`, and a throw out of the function that decides permissions is
 * a 500 where every refusal in the archive is a 404 or a 403.
 *
 * Two defences and they overlap deliberately. The Map means the lookup finds
 * nothing to call; the `=== true` on the way out means a predicate returning
 * something truthy-but-not-boolean is a no. Either alone stops the wrong
 * answer; only the Map stops the throw. The one place in the archive that
 * decides permissions is worth two cheap defences. */
const PREDICATE = new Map([
  /* May this person see a thing carrying these gates?
   *
   * Lifted verbatim out of `server.js`'s `gateOk`, which is the rule the
   * archive has been enforcing for viewing all along — an editor sees
   * everything; an ungated thing is visible to anyone; a gated one needs every
   * one of its gates held, because a row can carry several and clearing one is
   * not clearing them.
   *
   * `gates` NOT an array means the caller could not determine them, and that
   * is a denial. It replaces `gateOk`'s fail-closed-on-a-missing-id, for the
   * same reason it had one: a route whose SELECT forgot a column should return
   * a wrong-but-safe refusal, not a 500 that tells a stranger the row exists.
   *
   * No scope intersection here, unlike the bare form, and deliberately. A
   * scoped push token can read ungated content today; routing viewing through
   * a scope list would take that away the moment the token's scope named
   * anything at all. Viewing is not one of the powers a token narrows — it is
   * the floor the archive is built on. */
  ['content.view', (person, object) => {
    /* The bypass, and as of step E it belongs to the SOVEREIGN and to nobody
       else by role.
       Its history in one line each: `atLeast(person, 'editor')` written out at
       five sites, then a capability seeded to the editor role (step C, which
       moved nobody), then off that role and into `ADMIN_ONLY` (step E, which
       is the one behaviour change the whole part was for).
       The reason is the archive's own: one of the gates names the performer's
       previous life, and somebody with a good work ethic and broad write
       access still must not see it. A gate an editor can see through is not a
       gate. What lets somebody past ONE gate is a grant — the `heldBy` line
       below — which is the axis gates have always been on.
       The owner still bypasses, and must: a sovereign role holds every
       capability, and an owner who cannot see their own archive cannot
       administer it. */
    if (can(person, 'gate.bypass')) return true;
    const gates = object?.gates;
    if (!Array.isArray(gates)) return false;
    if (!gates.length) return true;
    const held = heldBy(person);
    return gates.every((g) => held.has(g));
  }],
]);

/** Every capability that can be asked about an object, derived rather than
 *  declared — a second hand-kept list is a second thing to forget, and the
 *  table above is the only real record of which rules exist. Exported because
 *  step six's `person_grant.capability` must name something from here or from
 *  CAPABILITIES, and a grant row pointing at neither is a row that confers
 *  nothing and says otherwise. */
export const OBJECT_CAPABILITIES = [...PREDICATE.keys()];

/** Is this a capability name a `person_grant` row may point at?
 *
 *  The union of both lists, because a grant row's `capability` is checked
 *  against the vocabulary and `content.view` — the only one anybody holds
 *  today — is bare-less and therefore absent from CAPABILITIES. A grant naming
 *  neither list is a row that confers nothing while appearing on the Admin
 *  screen as though it does, which is precisely the ambient grant the column
 *  was added to abolish. So it is refused at the endpoint.
 *
 *  It deliberately does NOT answer "should this capability be grantable at
 *  all". Scoping `people.manage` to a tag is meaningless, and nothing here
 *  stops somebody writing that row. What stops it is that a scoped grant only
 *  reaches a decision where a PREDICATE exists for its capability — so a
 *  meaningless grant is inert rather than dangerous. Narrowing the list to
 *  "capabilities with a scoped meaning" belongs with step eight's op list,
 *  where there is something real to narrow it to. */
export const grantable = (capability) =>
  CAPABILITIES.includes(capability) || PREDICATE.has(capability);

/** May this person do this? The one choke point — everything else asks here.
 *
 *  Two arities, and which one you get is decided by whether a third argument
 *  was PASSED — not by whether it was truthy:
 *
 *      can(person, cap)          could you ever?      role ∧ token scope
 *      can(person, cap, object)  may you, this one?   the predicate
 *
 *  `arguments.length` rather than `object === undefined`, and it matters. A
 *  caller who means to ask about a row and hands over an undefined row has a
 *  bug; under a truthiness test that bug silently becomes the BARE question,
 *  which for a default-open right answers yes. So the count is the
 *  discriminator: ask about an object and you get an answer about that object
 *  or a refusal, never a different question's answer.
 *
 *  An unknown capability is `false` rather than a throw: a typo'd name should
 *  hide a button, not take the page down. It is also why CAPABILITIES exists —
 *  assertCapabilities() below turns the typo into a startup failure instead,
 *  which is where you want to find it. */
export function can(person, capability, object) {
  if (arguments.length >= 3) {
    const predicate = PREDICATE.get(capability);
    if (!predicate) return false;
    /* Bare-gated: if the name also has a bare form, it must pass first. The
       object form narrows and never widens, which is the same rule the token
       scope follows one level up — a scoped grant must not reach past the role
       it hangs off. Bare-less names (`content.view`) skip this, because there
       is no bare form to clear. */
    if (CAPABILITIES.includes(capability) && !can(person, capability)) return false;
    return predicate(person, object) === true;
  }
  /* The role's own answer, and the ONE place a sovereign role differs from
     every other: it does not consult its bag, it consults the vocabulary. So
     it holds a capability because the capability exists, which is what makes
     "cannot have anything taken off it" true of the data rather than of a
     rule somebody has to keep enforcing.
     `KNOWN_CAPS` and not a blanket `true`, deliberately: a name that is not a
     capability is still false for a sovereign — `can(admin, 'tag.explode')`
     is no, and `can(admin, 'content.view')` bare is no, because that one is
     predicate-only and the object form above is the way to ask it. */
  const role = ROLE_ROWS.get(person?.role ?? FLOOR);
  if (!role) return false;
  if (!(role.sovereign ? KNOWN_CAPS.has(capability) : role.caps.has(capability))) {
    return false;
  }
  /* ── and the token's own scope, which can only take things away ───────────
     THE ONE PLACE this intersection happens, which is the whole reason it is
     safe. A scoped token — one minted for a script rather than held by a
     browser — carries a capability list, and the answer is the person's caps
     AND that list. Never their union, in either direction:

       · a token cannot do anything its holder cannot, so issuing one is never
         worse than lending them your password;
       · demoting somebody narrows every token they hold in the same instant,
         with nothing to revoke and no list to go and find.

     `scope` NULL is a browser session and means "whatever this person may
     do" — which is every session row that existed before the column did, so
     nothing that was signed in loses anything. */
  const scope = person?.scope;
  if (scope === null || scope === undefined) return true;
  return scopeSet(scope).has(capability);
}

/* Parsed once per distinct string rather than per call. A bulk push is one
   token asking this question a few times per image for a few hundred images,
   and splitting the same short string every time is work for nothing. */
const SCOPE_CACHE = new Map();
function scopeSet(scope) {
  let s = SCOPE_CACHE.get(scope);
  if (!s) {
    s = new Set(String(scope).split(/[\s,]+/).filter(Boolean));
    /* Bounded, because the key is attacker-adjacent: a token's scope is a
       column somebody with people.manage wrote, but a cache with no ceiling
       keyed on stored text is the shape of a slow leak. */
    if (SCOPE_CACHE.size > 256) SCOPE_CACHE.clear();
    SCOPE_CACHE.set(scope, s);
  }
  return s;
}

/** Every capability this person holds, as a flat object the client can read.
 *
 *  Sent to the browser so the UI can hide what it cannot do. It is a
 *  CONVENIENCE and never the enforcement: the same `can()` runs on the route,
 *  because a hidden button is a hint and a POST is a fact. */
export const capabilities = (person) =>
  Object.fromEntries(CAPABILITIES.map((c) => [c, can(person, c)]));

/* `ranks()` lived here, and is gone as of step B — 17 Sep.
 *
 * It existed for one reason: 42 routes asked for a rung and no capability
 * existed to ask for instead, so the page genuinely had to ask "am I this far
 * up". What it must not do is ask that under a name that looks like a power,
 * which is why the deprecated `suggest`/`edit`/`admin` trio was pulled out of
 * `capabilities()` and given its own namespace — `ME.can` a power, `ME.at` a
 * position, and which one a line was reading visible from the line.
 *
 * Step A gave all 42 routes a capability. Step B converted the page's 47 rank
 * reads to capability reads. So the question `ranks()` answered has no asker
 * left, and the second namespace it fed is off the `/api/auth/me` payload.
 *
 * It is deleted rather than deprecated because a rung is about to stop being a
 * true thing at all: a role is becoming a named bag of capabilities with no
 * ordering, and `at.editor` has no answer once `editor2` exists. A function
 * that still exports is a function somebody imports.
 *
 * `notrio.mjs` is where both retirements are asserted — it was written for the
 * trio and now owns the namespace question end to end: no export here answers
 * per rung, no read on the page asks for one, and `/api/auth/me` ships
 * neither. `mixed.mjs` holds the per-gate half, each converted gate paired
 * against the capability its own route asks for.
 *
 * `atLeast` and `RANK` went with it in step C, and so did the ladder
 * assertion. Nothing in this archive compares two roles any more. */

/** Express middleware: `app.post('/x', requireCap('tag.purge'), handler)`.
 *
 *  Same 401/403 split as requireRole — 401 means "sign in and this may work",
 *  403 means "signed in and it will not" — because that difference is what
 *  lets the page choose between a login button and hiding the affordance. */
/* What a piece of middleware asks for, readable from outside it.
 *
 * The door a route puts up is a fact about the route, and until now it was
 * only legible by reading the line that registered it. Tagging the closure
 * means the router can be WALKED — see routes.js — so "what does this archive
 * require, per endpoint" is a question with a computed answer rather than a
 * grep and a hope. It is the same reasoning as CAPABILITIES existing at all:
 * a rule you cannot enumerate is a rule nobody can review.
 *
 * `Symbol.for` rather than a plain property, so it cannot collide with
 * anything Express puts on a handler and cannot be serialised into a response
 * by accident. */
export const REQUIRES = Symbol.for('flatfox.requires');

/** Tag a middleware with the door it is. Returns the same function. */
export const marking = (fn, kind, name) => {
  fn[REQUIRES] = { kind, name };
  return fn;
};

export function requireCap(capability) {
  return marking((req, res, next) => {
    const person = req.person ?? ANON;
    if (can(person, capability)) return next();
    if (!person.id) return res.status(401).json({ error: `sign in required (${capability})` });
    return res.status(403).json({ error: `requires ${capability}; ${person.role} does not have it` });
  }, 'cap', capability);
}

/** Fail at boot on an inconsistency in the CODE's own permission vocabulary.
 *
 *  Its subject is `CAPABILITIES`, `PREDICATE` and `SEED_ROLES` — three lists in
 *  this file — and NOT the `role` rows, which are data somebody edits at
 *  runtime and must never be able to stop the archive booting. `loadRoles`
 *  handles those, and reports rather than throws.
 *
 *  Every check here is a typo that would otherwise present as a permission
 *  which silently never applies, which reads from the outside exactly like the
 *  feature not being built yet. */
export function assertCapabilities() {
  const known = new Set(CAPABILITIES);
  const bad = [];
  if (known.size !== CAPABILITIES.length) {
    const seen = new Set(), dupe = new Set();
    for (const c of CAPABILITIES) (seen.has(c) ? dupe : seen).add(c);
    bad.push(`duplicate capability ${[...dupe].join(', ')}`);
  }
  for (const c of CAPABILITIES) {
    if (!/^[a-z]+\.[a-z]+$/.test(c)) bad.push(`capability '${c}' is not area.verb`);
  }
  /* The seeds, which are what a fresh archive gets and therefore the only
     role data this file is responsible for. A seed naming a capability that
     does not exist is a tick nobody can ever spend. */
  for (const r of SEED_ROLES) {
    for (const c of (r.capabilities ?? [])) {
      if (!known.has(c)) bad.push(`seed role ${r.slug} grants unknown '${c}'`);
    }
  }
  /* ── exactly one sovereign, and it has no list ───────────────────────────
   *
   * Zero is the state the flag exists to prevent: an archive where the owner
   * has to remember to give themselves each new power, and where one bad
   * DELETE leaves nobody who can undo it. Two is not dangerous but is nobody's
   * intent, so it is worth refusing here where it is cheap.
   *
   * And a sovereign seed carrying a `capabilities` list would be a list
   * nothing reads — `roleCaps` computes its answer and `installRoles` writes
   * no rows for it — which is exactly the kind of dead data somebody edits
   * expecting an effect. */
  const sov = SEED_ROLES.filter((r) => r.sovereign);
  if (sov.length !== 1) {
    bad.push(`${sov.length} seeded roles are sovereign; there must be exactly one`);
  }
  for (const r of sov) {
    if (r.capabilities) bad.push(`sovereign role ${r.slug} carries a list nothing reads`);
    if (r.slug === FLOOR) bad.push(`the floor cannot be sovereign`);
  }
  /* ── every capability is placed on purpose ───────────────────────────────
   *
   * This replaced "every capability is in at least one seeded role", which
   * caught two real mistakes — a name minted in step A with no grant, and
   * `gate.bypass` in step C — and which a sovereign admin makes vacuous, since
   * everything is held by somebody now, always.
   *
   * So the question becomes the one that is actually left: is this capability
   * in a seeded role, or is it the owner's alone? Both are fine and silence is
   * not. Minting a name still fails the boot until somebody decides.
   *
   * Asserted of the SEEDS and never of the live registry, for the same reason
   * as before: a role edit that leaves a capability held by no ordinary role
   * is a legitimate thing to do in the role editor, and refusing to boot over
   * it would mean the UI could brick the archive. */
  const ordinary = new Set(SEED_ROLES.filter((r) => !r.sovereign)
    .flatMap((r) => r.capabilities ?? []));
  const ownerOnly = new Set(ADMIN_ONLY);
  for (const c of ADMIN_ONLY) {
    if (!known.has(c)) bad.push(`ADMIN_ONLY names unknown '${c}'`);
  }
  if (ownerOnly.size !== ADMIN_ONLY.length) bad.push('ADMIN_ONLY has a duplicate');
  for (const c of CAPABILITIES) {
    const inRole = ordinary.has(c), declared = ownerOnly.has(c);
    if (!inRole && !declared) {
      bad.push(`'${c}' is in no seeded role and not declared owner-only — `
        + `put it in a role or add it to ADMIN_ONLY`);
    }
    /* Both is a contradiction rather than a redundancy: the list says "no
       ordinary role holds this" and a seed says one does. Whichever is wrong,
       reading either of them alone would mislead. */
    if (inRole && declared) {
      bad.push(`'${c}' is declared owner-only and also seeded to an ordinary role`);
    }
  }
  /* The floor has to exist, and it has to be empty.
   *
   * Empty is the load-bearing half. `person.role` defaults to it and a request
   * nobody identified is treated as holding it, so anything granted here is
   * granted to the entire internet. It was empty by construction while the
   * roles were arrays — `const viewer = []` — and it is asserted now that they
   * are rows a seed could grow. */
  const floor = SEED_ROLES.find((r) => r.slug === FLOOR);
  if (!floor) bad.push(`there is no '${FLOOR}' role to be the floor`);
  else if (floor.capabilities.length) {
    bad.push(`the floor grants ${floor.capabilities.join(', ')} to everybody`);
  }
  /* ── the predicates are functions, and their names are capability names ──
   *
   * Thin on purpose, because OBJECT_CAPABILITIES is derived from the table
   * rather than kept beside it — the two cannot disagree, so there is no
   * disagreement to check. What is worth refusing to boot over is a name that
   * does not look like the vocabulary, since a grant row will one day be
   * matched against these and a mis-shaped name is a grant that matches
   * nothing while appearing on the screen as though it does.
   *
   * A name appearing in BOTH lists is legal and is the bare-gated case — see
   * the comment on PREDICATE. It was briefly asserted as an error here, which
   * was wrong: it is the shape step seven's `snippet.replace` requires. */
  for (const [c, fn] of PREDICATE) {
    if (typeof fn !== 'function') bad.push(`predicate '${c}' is not a function`);
    if (!/^[a-z]+\.[a-z]+$/.test(c)) bad.push(`predicate name '${c}' is not area.verb`);
  }
  /* ── and the ladder check is GONE ────────────────────────────────────────
   *
   * It stood here and insisted that every role contained the one below it,
   * because fifty-two call sites asked "is this person at least an editor" and
   * not one of them would have failed loudly if that stopped being true.
   *
   * There are no such call sites left. `requireRole` and `atLeast` are deleted
   * and nothing compares two roles. So the check is not merely unnecessary —
   * it would be actively wrong: the first role somebody builds that is not a
   * superset of another would refuse to boot the archive, and building exactly
   * that is the point of the role editor.
   *
   * What replaced it is nothing, and that is the honest answer. A bag of
   * capabilities has no invariant to check beyond "every name in it is real",
   * which is the loop above. */
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
    /* The session's own three, alongside the person's. `scope` is what can()
       intersects with; the other two are here so a route can spend a budget
       and a refusal can name the token rather than the person — "that token is
       out of uses" is actionable where "403" is not. */
    `SELECT p.id, p.handle, p.role, p.provider, p.display_name, p.avatar_url,
            p.banned, s.scope, s.uses_left, s.label, s.token_hash
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
  /* The first PERSON, and `system` rows are not people. The recorder gets a
     person row so that "what did the Pi do" is the same query as "what did
     drifter do" — and it polls every twenty seconds, so on a fresh database it
     would almost always be the first row written. Counting it here would mean
     the first human to sign in is not made admin, and a new deployment ends up
     with nobody who can promote anybody. */
  const first = db.prepare(
    `SELECT COUNT(*) c FROM person WHERE provider <> 'system'`).get().c === 0;
  const id = ulid();
  db.prepare(`INSERT INTO person(id, provider, provider_uid, handle, display_name,
                avatar_url, role, created_at, last_seen_at)
              VALUES(?,?,?,?,?,?,?,?,?)`)
    .run(id, provider, String(providerUid), handle, displayName, avatarUrl,
         first ? 'admin' : defaultRole, t, t);
  return id;
}

/** A session, or — with `scope` — an API token that can do less than its owner.
 *
 *  The extra four are all optional and all default to the shape a browser
 *  session has always had, so the two existing call sites did not change:
 *  `ttl` seconds instead of the month a login gets, `scope` as a space-
 *  separated capability list, `uses` as a write budget, `label` for the list
 *  somebody has to revoke from, and `by` for who issued it.
 */
export function issueSession(db, personId, agent = '', opts = {}) {
  const token = randomBytes(32).toString('base64url');
  const t = now();
  const ttl = Math.max(60, Math.min(Number(opts.ttl) || TTL, TTL));
  const scope = opts.scope ? String(opts.scope).slice(0, 2000) : null;
  const uses = Number.isFinite(Number(opts.uses)) && Number(opts.uses) > 0
    ? Math.min(Math.round(Number(opts.uses)), 1_000_000) : null;
  db.prepare(`INSERT INTO session(token_hash, person_id, created_at, expires_at,
                                  user_agent, scope, label, uses_left, created_by)
              VALUES(?,?,?,?,?,?,?,?,?)`)
    .run(hashToken(token), personId, t, t + ttl, String(agent ?? '').slice(0, 200),
         scope, opts.label ? String(opts.label).slice(0, 120) : null,
         uses, opts.by ?? null);
  db.prepare('DELETE FROM session WHERE expires_at < ?').run(t);   // opportunistic sweep
  return { token, expiresAt: t + ttl, scope, uses_left: uses };
}

/** Spend one write from a token's budget.
 *
 *  Returns how many are left AFTER the spend, or -1 when there was nothing to
 *  spend. Those have to be different answers: a token with one use left spends
 *  it and legitimately has zero remaining, and an earlier version treated that
 *  zero as "refuse" — so a budget of three took two images and refused the
 *  third. The caller's test is `< 0`, never `<= 0`.
 *
 *  Conditional in SQL rather than read-then-write: a push runs as fast as the
 *  network allows and two requests in flight must not both see the last use.
 *  The UPDATE either hit a row with something left or it did not, and SQLite
 *  answers that for us.
 */
export function spendUse(db, tokenHash) {
  if (!tokenHash) return -1;
  const hit = db.prepare(
    `UPDATE session SET uses_left = uses_left - 1
      WHERE token_hash = ? AND uses_left > 0`).run(tokenHash);
  if (!hit.changes) return -1;
  return db.prepare('SELECT uses_left FROM session WHERE token_hash = ?')
    .get(tokenHash)?.uses_left ?? 0;
}

export const hashOf = hashToken;

export function revokeSession(db, token) {
  if (token) db.prepare('DELETE FROM session WHERE token_hash = ?').run(hashToken(token));
}

export const sessionToken = tokenFrom;

/* `requireRole(role)` stood here and is gone as of step C.
 *
 * Step A converted the last of its 42 route registrations to `requireCap`, so
 * by then it guarded nothing. It is deleted rather than left exported for the
 * reason `ranks()` was: a middleware that still exports is a middleware
 * somebody reaches for, and the next person to add a route would have found a
 * perfectly working way to ask a question this archive no longer has an answer
 * to. `routes.js` dropped the `role:` door class with it, so a manifest entry
 * naming one is now a boot failure rather than a value nothing can produce.
 *
 * Its 401/403 split survives in `requireCap`, which is where it mattered: 401
 * means "sign in and this may work", 403 means "signed in and it will not",
 * and that difference is what lets the page choose between showing a login
 * button and hiding an affordance. */
