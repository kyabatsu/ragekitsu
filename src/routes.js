// The door on every route, declared once and checked against the wiring.
//
// ┌────────────────────────────────────────────────────────────────────────┐
// │  This file is the overview. If you want to know who can reach what,   │
// │  read MANIFEST and nothing else.                                      │
// └────────────────────────────────────────────────────────────────────────┘
//
// The archive enforces permissions in five places — `requireCap`,
// `requireRole`, `requireIngest`, `CHANGE_CAPS` in archive.js, and a handful
// of in-handler checks — and until this file existed there was no index of
// them. "Where is `tag.attach` enforced" had five possible answers and no way
// to ask. That is what makes a permission table impossible to review: not the
// number of rules, but that no artefact lists them and so no artefact can be
// trusted.
//
// ── how it works ──────────────────────────────────────────────────────────
//
// Two sides, and the check is that they agree.
//
//   OBSERVED   walked off the live Express router. `requireCap` and
//              `requireRole` tag their closures (auth.js `REQUIRES`), so the
//              door a route actually puts up is readable from outside it.
//              This side is ground truth: it is the code that runs.
//
//   DECLARED   MANIFEST, below. Written by hand, reviewable by eye, and the
//              thing a person reads when they want to know the policy.
//
// A route in one and not the other is a boot failure. That is the whole
// point: the manifest cannot rot, because a stale entry stops the server. A
// hand-kept table that nothing checks is worse than no table, because people
// believe it.
//
// ── what the values mean ──────────────────────────────────────────────────
//
//   'cap:<name>'      requireCap — a capability from CAPABILITIES
//   'ingest'          the shared secret the recorder holds. Not a person.
//   'public'          no door. Note that the global gate (server.js OPEN_GET /
//                     OPEN_POST) still refuses anonymous callers for anything
//                     not on its allowlist, so `public` means "any signed-in
//                     viewer" and only a few are reachable without a session.
//   'in-handler:<x>'  the route is open but decides inside the handler, `x`
//                     naming what it consults. These are the ones worth
//                     converting, and listing them is how they stop being
//                     invisible.
//
// ── what this file verifies, and what it merely asserts ────────────────────
// `cap:` is CHECKED: requireCap tags its closures (auth.js REQUIRES), the
// router is walked, and a disagreement stops the boot. `public`, `ingest` and
// `in-handler:` are hand-classified CLAIMS —
// nothing in the router can see inside a handler — so they are exactly as
// reliable as the person who typed them.
//
// They have already been wrong once, in the direction that matters least and
// teaches most: `GET /media/chat/:stream_id` was recorded as `in-handler:gate`
// while the handler carried a comment saying "ungated, matching /media/video".
// The manifest claimed a check that did not exist. It is true as of 17 Sep,
// when stream gating landed — made true rather than corrected, which is luck
// and not a process. Treat an `in-handler:` value as a lead, and read the
// handler.
//
// ── the `role:` class is gone (step C, 17 Sep) ─────────────────────────────
// There were 42 of them, and this file existing is what made converting them
// tractable: the overview it prints is where "these routes ask for a rung and
// there is no capability to ask instead" became a list rather than a feeling.
// Step A converted all 42, step C deleted `requireRole` itself, and the class
// goes with it — so a manifest entry naming a role is now a BOOT FAILURE
// rather than a value nothing can produce. That is the useful direction: the
// next person to reach for a rung finds out at startup that there are none.
// See AUTH.md.

import { CAPABILITIES, REQUIRES } from './auth.js';

export const MANIFEST = {
  // ── identity ────────────────────────────────────────────────────────────
  'GET /api/auth/grants': 'in-handler:signed-in',
  'POST /api/auth/login': 'public',
  'POST /api/auth/logout': 'public',
  'GET /api/auth/me': 'public',
  'POST /api/auth/token': 'public',
  'GET /api/auth/tokens': 'cap:token.manage',
  'POST /api/auth/tokens': 'cap:token.manage',
  'DELETE /api/auth/tokens/:id': 'cap:token.manage',

  // ── people, and the roles they are put in ───────────────────────────────
  'GET /api/admin/people': 'cap:grant.manage',
  'POST /api/admin/people/:id/role': 'cap:people.manage',
  /* What a role MEANS, which is a different act from putting somebody in one
     — and the one power that can grant every other, so it is its own name. */
  'GET /api/roles': 'cap:roles.manage',
  'POST /api/roles': 'cap:roles.manage',
  'PATCH /api/roles/:slug': 'cap:roles.manage',
  'DELETE /api/roles/:slug': 'cap:roles.manage',
  'GET /api/grants': 'cap:grant.manage',
  'POST /api/people/:id/grants': 'cap:grant.manage',
  'DELETE /api/people/:id/grants/:name': 'cap:grant.manage',

  // ── streams ─────────────────────────────────────────────────────────────
  'GET /api/live': 'public',
  'GET /api/months': 'in-handler:gate',
  'GET /api/streams': 'in-handler:gate',
  'GET /api/streams/:id': 'in-handler:gate',
  'POST /api/streams/:id/audit': 'cap:stream.rescan',
  // The answer to the question that button asks, so the same capability reads
  // it. Its own route and not a field on the stream, because it carries NAS
  // filenames and platform ids the public stream route has no business in.
  'GET /api/streams/:id/audit': 'cap:stream.rescan',
  /* Public ON PURPOSE — "an archive that hides its edits is worth less".
     Deliberately unlike `/api/snippets/:id/history`, which is review.decide,
     and `/api/events`, which is too. Noted so nobody makes them agree by
     reflex: the asymmetry is a decision about what an archive owes its
     readers, not drift. */
  'GET /api/streams/:id/history': 'in-handler:gate',
  'POST /api/streams/:id/poster': 'cap:stream.poster',
  'POST /api/captures/:id/purge-video': 'cap:capture.purge',
  'POST /api/streams/:id/rescan': 'cap:stream.rescan',
  'GET /api/streams/idx/:idx': 'in-handler:gate',

  // ── the vocabulary ──────────────────────────────────────────────────────
  'GET /api/taglets': 'in-handler:editor',
  'POST /api/taglets/:id/gate': 'cap:gate.set',
  'GET /api/tags': 'in-handler:tag.purge for tombstones, editor for proposed',
  'POST /api/tags/:id/harvest': 'cap:tag.harvest',
  'POST /api/tags/:id/poster': 'cap:tag.art',
  'POST /api/tags/:id/purge': 'cap:tag.purge',
  'POST /api/tags/:id/seed': 'cap:tag.harvest',
  'GET /api/tags/:id/uses': 'in-handler:gate',

  // ── the queue ───────────────────────────────────────────────────────────
  'GET /api/changesets': 'cap:review.read',
  'POST /api/changesets': 'cap:change.propose',
  'GET /api/changesets/:id': 'cap:review.read',
  'POST /api/changesets/:id/review': 'cap:review.decide',
  // Answering what evidence cannot settle. `review.decide` on both writes:
  // settling what the archive records about an entry is the same act as
  // deciding a proposal, so it is the same door.
  'GET /api/claims': 'cap:review.read',
  'GET /api/questions': 'cap:review.read',
  'POST /api/claims': 'cap:review.decide',
  // The one door that CLOSES a question, and the only one that can: an answer
  // is a claim when it states a fact and a job on the Pi when it asks for
  // work, and something has to know which. Same capability either way —
  // queueing a repair is deciding what the archive does about an entry.
  'POST /api/questions/:id/answer': 'cap:review.decide',
  'POST /api/claims/:id/withdraw': 'cap:review.decide',
  'GET /api/events': 'cap:review.decide',
  'GET /api/suggestions': 'cap:suggestion.decide',
  'POST /api/suggestions/dismiss': 'cap:suggestion.decide',

  // ── snippets and clips ──────────────────────────────────────────────────
  'POST /api/clips': 'cap:clip.cut',
  'GET /api/clips/:id/file': 'cap:clip.cut',
  'GET /api/snippets': 'in-handler:review.read, editor',
  'GET /api/snippets/:id': 'in-handler:review.read',
  'GET /api/snippets/:id/history': 'cap:review.decide',
  'PATCH /api/snippets/:id/line/:seq': 'cap:transcript.edit',
  'POST /api/snippets/:id/purge': 'cap:snippet.purge',
  'POST /api/snippets/:id/reocr': 'cap:transcript.run',
  'POST /api/snippets/:id/retranscribe': 'cap:transcript.run',
  'PUT /api/snippets/:id/transcript': 'cap:transcript.edit',
  'POST /api/snippets/review': 'cap:review.decide',
  'POST /api/uploads': 'cap:snippet.upload',
  'DELETE /api/uploads/:id': 'cap:snippet.upload',
  'PATCH /api/uploads/:id': 'cap:snippet.upload',
  'POST /api/uploads/link': 'cap:snippet.upload',

  // ── music ───────────────────────────────────────────────────────────────
  'GET /api/music': 'in-handler:music.decide',
  'POST /api/music': 'cap:music.submit',
  'GET /api/music/:id': 'in-handler:gate',
  'POST /api/music/:id/purge': 'cap:music.purge',
  'POST /api/music/:id/retry': 'cap:music.decide',
  'POST /api/music/:id/review': 'cap:music.decide',
  'POST /api/music/upload': 'cap:music.upload',

  // ── media bytes ─────────────────────────────────────────────────────────
  'GET /media/chat-assets': 'public',
  'GET /media/chat/:stream_id': 'in-handler:gate',
  'GET /media/snippet-poster/:id': 'in-handler:gate',
  'GET /media/snippet/:id': 'in-handler:gate',
  'GET /media/snippet/:id/:name': 'in-handler:gate',
  'GET /media/song/:id': 'in-handler:gate',
  'GET /media/thumb/:rest(*)': 'public',
  /* FINDING, not intent — see AUTH.md. This reads `video_path` off the
     capture and serves it: no gate, no status, no tombstone. The comment
     beside `/media/thumb` says moving pictures are "addressed BY ID through a
     route that reads the row and checks who is asking", and this is that
     route. It reads the row. It does not check who is asking. Capture ids are
     in `/api/streams` responses, so the obscurity that was the old defence
     moved rather than went. Left as-is pending a decision: stream video may
     simply be open to any signed-in viewer, which is the archive's purpose. */
  'GET /media/video/:capture_id': 'in-handler:gate',

  // ── the recorder's half ─────────────────────────────────────────────────
  'POST /api/ingest/capture': 'ingest',
  'POST /api/ingest/jobs/:id': 'ingest',
  'POST /api/ingest/jobs/claim': 'ingest',
  'POST /api/ingest/live': 'ingest',
  'GET /api/ingest/lookup': 'ingest',
  'GET /api/ingest/next-index': 'ingest',

  // ── jobs and ops ────────────────────────────────────────────────────────
  'GET /api/jobs': 'cap:job.read',
  'POST /api/jobs': 'cap:job.create',
  'GET /api/jobs/:id': 'cap:job.read',
  'POST /api/jobs/:id/:verb': 'cap:job.control',
  'GET /api/quarantine': 'cap:ops.read',
  'GET /api/recorder': 'cap:ops.read',
  'POST /api/recorder/sweep': 'cap:ops.manage',

  // ── the ML side ─────────────────────────────────────────────────────────
  'GET /api/models': 'cap:ops.read',
  'PATCH /api/models/:task': 'cap:ops.manage',
  'POST /api/models/:task/check': 'cap:ops.manage',
  'POST /api/ocr/pause': 'cap:ops.manage',
  'GET /api/transcribe': 'cap:ops.read',
  'POST /api/transcribe/pause': 'cap:ops.manage',

  // ── the rest ────────────────────────────────────────────────────────────
  'GET /api/health': 'in-handler:signed-in',
  'GET /api/health/out-of-span': 'in-handler:gate',
  'GET /api/media/browse': 'cap:media.browse',
  'POST /api/notes/srt': 'cap:note.export',
  'GET /m/:id': 'in-handler:signed-in',
};

/* Routes that put up a door AND then narrow further inside the handler.
 *
 * `MANIFEST` holds one value per route on purpose — a single door is what can
 * be walked off the router and therefore what can be asserted. But six routes
 * consult a second capability in their body, and leaving those unlisted is how
 * they stayed invisible. This map is documentation with a well-formedness
 * check: it cannot drift into claiming a route that does not exist, and it
 * cannot name a capability that does not exist.
 *
 * It is also the conversion backlog. Each of these is a place where the real
 * rule is not expressible as one middleware, which is either a sign the route
 * does two things or a sign the capability vocabulary is missing a name. */
export const ALSO = {
  'GET /api/auth/tokens': 'people.manage — to list somebody else\'s',
  'POST /api/auth/tokens': 'people.manage — to mint on somebody else\'s behalf',
  'DELETE /api/auth/tokens/:id': 'people.manage — to revoke somebody else\'s',
  'POST /api/uploads': 'review.decide — publish straight through, or queue',
  'POST /api/uploads/link': 'link.any — a source the archive has not vouched for',
  'POST /api/music/upload': 'music.decide — publish straight through, or queue',
};

/** Every route the app actually registered, and the door each one puts up.
 *
 *  Reads Express 4's router stack. Deliberately not clever about it: if a
 *  future Express moves the stack, this throws at boot with a clear message
 *  rather than silently observing zero routes and approving everything.
 */
export function observe(app) {
  const stack = app?._router?.stack ?? app?.router?.stack;
  if (!Array.isArray(stack)) {
    throw new Error('routes.observe: cannot read the router stack — Express '
      + 'changed shape, and an unreadable stack must not read as "no routes"');
  }
  const out = new Map();
  for (const layer of stack) {
    const route = layer.route;
    if (!route?.path) continue;
    const handlers = route.stack ?? [];
    /* The FIRST tagged handler wins, which is the door: middleware runs in
       order, so the earliest refusal is the one a caller meets. The extra
       in-handler checks some routes carry are deliberately not represented
       here — they narrow further and the manifest names them separately. */
    let requires = 'public';
    for (const h of handlers) {
      const tag = h.handle?.[REQUIRES];
      if (tag) { requires = `${tag.kind}:${tag.name}`; break; }
      if (h.handle?.name === 'requireIngest') { requires = 'ingest'; break; }
    }
    for (const m of Object.keys(route.methods)) {
      if (m === '_all') continue;
      out.set(`${m.toUpperCase()} ${route.path}`, requires);
    }
  }
  return out;
}

const KNOWN_CAPS = new Set(CAPABILITIES);

/** Is this a value the manifest is allowed to hold? Null when it is, else why
 *  not — the string is appended to the entry's name in the boot failure.
 *
 *  Exported for `manifest.mjs`, which asserts the refusals directly. Every
 *  other way to test them means writing a bad manifest and catching a boot,
 *  and the one that matters most — `role:` — cannot be produced by any
 *  middleware that still exists, so there is nothing to wire it up with. */
export function wellFormed(v) {
  if (v === 'public' || v === 'ingest') return null;
  if (v.startsWith('in-handler:')) return null;
  const [kind, name] = [v.slice(0, v.indexOf(':')), v.slice(v.indexOf(':') + 1)];
  if (kind === 'cap') {
    return KNOWN_CAPS.has(name) ? null : `names no such capability '${name}'`;
  }
  /* Named rather than falling through to the generic message, because this is
     the one wrong value somebody will actually type: it was legal here until
     17 Sep and it is what forty-two of these entries used to say. */
  if (kind === 'role') {
    return 'asks for a ROLE, and roles are not doors — every route asks a '
      + 'capability now, and `requireRole` no longer exists to put one up';
  }
  return `is not one of public | ingest | cap:… | in-handler:…`;
}

/**
 * Fail at boot unless the manifest and the wiring say the same thing.
 *
 * Four ways to fail, and each of them is a real bug rather than bookkeeping:
 *
 *   a route with no entry        somebody added an endpoint and nobody
 *                                decided who may reach it — which defaults to
 *                                whatever middleware they happened to type
 *   an entry with no route       the manifest is describing a door that is
 *                                not there, so it is lying to its readers
 *   a disagreement               the two sides have drifted, which is the
 *                                whole class of bug D1–D4 came from
 *   a malformed value            a typo'd capability name, which otherwise
 *                                presents as a permission that never applies
 *
 * An `in-handler:` entry is exempt from the agreement check, because the
 * router genuinely observes `public` there — the decision is inside the
 * function. That is an admission, not a pass: the value records that somebody
 * looked, and the list of them is the conversion backlog.
 */
export function assertRoutes(app) {
  const seen = observe(app);
  const bad = [];

  for (const [key, want] of Object.entries(MANIFEST)) {
    const why = wellFormed(want);
    if (why) bad.push(`${key}: '${want}' ${why}`);
  }
  for (const [key, got] of seen) {
    if (!(key in MANIFEST)) {
      bad.push(`${key}: registered but not in the manifest (observed '${got}')`);
      continue;
    }
    const want = MANIFEST[key];
    if (want.startsWith('in-handler:')) {
      if (got !== 'public') {
        bad.push(`${key}: manifest says '${want}' but the router has '${got}'`);
      }
      continue;
    }
    if (got !== want) {
      bad.push(`${key}: manifest says '${want}', the router has '${got}'`);
    }
  }
  for (const key of Object.keys(MANIFEST)) {
    if (!seen.has(key)) bad.push(`${key}: in the manifest but no such route`);
  }
  /* ALSO is documentation, so it gets the weaker check — but it gets one, or
     it becomes a list of routes that used to exist saying things that used to
     be true. */
  for (const [key, note] of Object.entries(ALSO)) {
    if (!seen.has(key)) bad.push(`ALSO ${key}: no such route`);
    const cap = String(note).split(/[\s—]/)[0];
    if (!KNOWN_CAPS.has(cap)) {
      bad.push(`ALSO ${key}: '${cap}' is not a capability`);
    }
  }

  if (bad.length) {
    throw new Error(`route manifest does not match the wiring:\n  - `
      + bad.join('\n  - '));
  }
  return seen;
}

/** The overview, as text. `TENMA_ROUTES=1 node server.js` prints it and exits.
 *
 *  Grouped by what the door is rather than by path, because the question this
 *  answers is "what can a suggester reach", and that is unreadable off a list
 *  sorted by URL. */
export function render(app) {
  const seen = observe(app);
  const rows = [...seen].map(([key, got]) => [key, MANIFEST[key] ?? got]);
  const groups = new Map();
  for (const [key, want] of rows) {
    const k = want.startsWith('in-handler:') ? 'in-handler' : want;
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push([key, want]);
  }
  const order = (a) => {
    if (a === 'public') return 0;
    if (a === 'ingest') return 1;
    if (a === 'in-handler') return 90;
    return 10 + CAPABILITIES.indexOf(a.slice(4));
  };
  const lines = [`${rows.length} routes`, ''];
  for (const k of [...groups.keys()].sort((a, b) => order(a) - order(b))) {
    const list = groups.get(k).sort((a, b) => a[0].localeCompare(b[0]));
    lines.push(`${k}  (${list.length})`);
    for (const [key, want] of list) {
      lines.push(`    ${key}${k === 'in-handler' ? `   ${want.slice(11)}` : ''}`);
    }
    lines.push('');
  }
  return lines.join('\n');
}
