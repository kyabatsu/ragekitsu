// Connections, pragmas, migrations, ULIDs. Every SQLite call in the project
// goes through this file, so swapping node:sqlite for better-sqlite3 later is
// a one-file change rather than a grep.

import { DatabaseSync } from 'node:sqlite';
import { randomFillSync } from 'node:crypto';
import { readFileSync, mkdirSync, existsSync, statSync } from 'node:fs';
import { dirname, join, extname, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
export const SCHEMA_PATH = join(HERE, 'schema.sql');

// WAL and page_size are persistent properties of the file; the rest are
// per-connection and have to be re-set every time one is opened.
const PRAGMAS_RW = [
  'journal_mode = WAL',        // readers never block the writer
  'synchronous = NORMAL',
  'foreign_keys = ON',
  'busy_timeout = 5000',
  'temp_store = MEMORY',
  'cache_size = -32000',
  'mmap_size = 268435456',
];

const PRAGMAS_RO = [
  'query_only = ON',
  'busy_timeout = 5000',
  'temp_store = MEMORY',
  'cache_size = -32000',
  'mmap_size = 268435456',
];

/* Forward-only and idempotent: each entry runs only if its column or table is
   absent. Once real user content exists, "delete and re-import" stops being an
   option and this list is the only way the schema moves.

   ── COLLAPSED 2026-09-07, AT schema_version 4 ───────────────────────────────
   Empty, and that is the finished state rather than a gap. Sixty-eight entries
   lived here, and fifty of them ALTERed a column schema.sql already declared —
   so the schema was two documents that had to be read together, and three
   tables and eight columns had drifted into being defined ONLY here. They are
   all in schema.sql now, which the diff proves: a database built from that file
   alone is byte-identical to one built from the old file plus all sixty-eight.

   The live database was brought to this floor by a one-shot before this shipped
   — it carried the old lists, ran them, verified the result and was then thrown
   away. That is why nothing here has to catch anybody up: every database this
   code will ever open is either fresh from schema.sql or already past this
   line. A database from BEFORE the collapse cannot be migrated by this file at
   all; it needs that one-shot, which is what floorCheck() below says.

   Where the next schema change goes depends on what it is, and the difference
   is worth knowing because it is not what the first draft of this comment said:

     A NEW TABLE goes in schema.sql and NOWHERE ELSE. `CREATE TABLE IF NOT
     EXISTS` creates it on an existing database just as it does on a fresh one
     — the statement is skipped only when the table is already there — and the
     same is true of every CREATE INDEX and CREATE TRIGGER beside it. The
     `music` tables landed this way with no entry here at all.

     A NEW COLUMN ON AN EXISTING TABLE goes in BOTH: schema.sql, so a fresh
     database has it, and here, so existing ones catch up. This is the case
     that needs the list, and the only one — `IF NOT EXISTS` skips the whole
     CREATE TABLE on a database that already has the table, so a column added
     only to schema.sql silently never reaches the live database. That is
     exactly how three tables and eight columns drifted out of schema.sql in
     the first place, from the other direction.

   Either way, add it to FLOOR below only if the code cannot function without
   it — that list is what turns a database left behind by a collapse into one
   clear sentence instead of a puzzling error from inside a route. */
const MIGRATIONS = [
  /* ── 2026-09-10, memes and the gallery ────────────────────────────────────
     One table for everything this archive hosts, told apart by `kind`. See the
     column's own comment in schema.sql for why the split is by provenance.

     The rename is here and not in REBUILDS, which surprised me: ALTER TABLE
     RENAME COLUMN has been able to express this since SQLite 3.25, and it
     carries the dependent indexes across with it — `ix_snippet_live` and the
     rest come out the far side naming the new column, checked. So there is no
     12-step rebuild, no one-shot to run by hand, and no copy of fifteen hundred
     rows: the change is an ALTER that takes about a millisecond on restart.

     Keyed on `file_path` being ABSENT, which is what makes it run exactly once.
     A fresh database is built from schema.sql and already has it; a live one
     has `video_path` and gets renamed; a database that has already been through
     here is skipped. The order in this list matters for the same reason the
     entries below it read oddly: `kind` and the rest are ADD COLUMNs and could
     go in any order, but the rename must not be attempted twice.

     `capture.video_path` and `music.video_path` keep their name. Those two hold
     videos and always will; this one holds PNGs now. */
  ['snippet', 'file_path',
   `ALTER TABLE snippet RENAME COLUMN video_path TO file_path`],
  ['snippet', 'kind',
   `ALTER TABLE snippet ADD COLUMN kind TEXT NOT NULL DEFAULT 'snippet'`],
  ['snippet', 'source', `ALTER TABLE snippet ADD COLUMN source TEXT`],
];

/* Rebuilds — for the shape changes ALTER TABLE cannot express: a new PRIMARY
   KEY, a foreign key repointed at a different table. Each is keyed on the
   column that marks it DONE rather than on the one it adds, so it runs exactly
   once and is a no-op on a fresh database, which schema.sql already builds in
   the finished shape. Kept separate from MIGRATIONS because these copy data,
   and each runs inside tx() — a half-moved table is not a state anybody can
   reason about afterwards.

   Emptied in the same collapse as MIGRATIONS. Both entries that lived here have
   run everywhere: stream_tag gained its surrogate id, and `taglet` merged into
   `tag` and was dropped. */
const REBUILDS = [
];

/* ── the one vocabulary ──────────────────────────────────────────────────
 *
 * A tag is filed under one of these and a block of time is coloured by one of
 * these, and they are the same list because "what is this tag" and "what is
 * this block" are the same question asked twice.
 *
 *   media      what it belongs to — a game, a franchise, an agency, an event
 *   character  a guest, a member, a person the block is about
 *   type       what kind of stream this stretch is — collab, watchalong,
 *              karaoke, zatsudan, event
 *   elements   the scaffolding around it — intro, outro, break, waiting screen
 *   meta       what a snippet IS rather than what it is about — reviewed,
 *              duplicate, animated, audio only, restricted
 *   general    everything else about a snippet
 *
 * Closed, because it is a colour and a colour has to mean the same thing in
 * every stream in the archive. Extending it is a deliberate edit to this line.
 *
 * IT LIVES HERE, not in archive.js, and archive.js re-exports it. That reads
 * like the wrong home — this is the schema layer and that is the domain layer —
 * and it is the right one because the dependency only runs one way: archive.js
 * imports db.js, so db.js importing archive.js is a cycle, and a cycle between
 * two modules that both build lists at module scope is a TDZ ReferenceError
 * during boot rather than a warning. Both layers need the vocabulary, so it
 * lives in the upstream one. That is the only arrangement with one list and no
 * cycle.
 *
 * It earned that placement the hard way. A backfill here once had the
 * vocabulary typed out by hand in a WHERE clause, drifted a rename behind the
 * real list, and silently refiled every media, character and elements row in
 * the archive as 'unknown' on each restart. The backfill is gone; the rule it
 * bought is not — anything that has to name the whole vocabulary builds the
 * list from KINDS or ALL_KINDS, never by retyping it.
 */
export const KINDS = ['media', 'character', 'type', 'elements', 'meta', 'general'];

/* ...plus the sentinel. This is what the two columns may HOLD; KINDS is what
 * a person may choose. 'unknown' is not a category — it is where a tag sits
 * between being minted and being filed — so it is offered in a picker only to
 * something that already is one.
 *
 * `KIND_SURFACES` was here: a map of which kinds each picker was allowed to
 * offer, so that Intro and Break — which only ever label a stretch of a
 * broadcast — stayed out of a stream's tag row, where the only answer they can
 * give is "no streams". It was right about the dead end and wrong about the
 * price. It produced three derived lists that had to agree, an `?surface=`
 * parameter that had to be passed correctly at every call site, and a theater
 * screen on which `elements` could not be created at all — so the one place
 * the kind was genuinely needed was the one place it was missing.
 *
 * The vocabulary is now offered whole, everywhere, and filing something
 * uselessly is an editor's mistake to make and to undo. That also means
 * `tag.kind` and `segment.kind` are the same domain rather than two lists that
 * happen to match — which is exactly the shape that silently diverged once
 * already and cost every block in the archive its colour. */
export const ALL_KINDS = [...KINDS, 'unknown'];

/* Data backfills — one-time repairs of rows that predate a column or a rule.
   `[table, label, sql]`, run once each and recorded by LABEL; see the guard at
   the end of migrate() for why the label is the marker and what that costs.

   Emptied in the 2026-09-07 collapse. Nine of these had run everywhere, and the
   one-shot proved it the honest way rather than by trusting the marker: it RAN
   all nine against the real database and every one reported zero rows changed.
   They are idempotent by construction, so that is a measurement and not a risk.

   If you add one, it is a REPAIR and not a rule. Anything that has to stay true
   for rows written from now on belongs at the write path — a backfill that is
   really a rule fires once, records itself, and then quietly stops being
   enforced, which is indistinguishable from working. */
const BACKFILLS = [
];

/** Accept a file or a directory. Pointing at a folder is the natural reading of
 *  "where should the DB live", and SQLite answers that with a bare
 *  "unable to open database file". */
export function resolveDbPath(raw) {
  const p = resolve(raw);
  if (existsSync(p) && statSync(p).isDirectory()) return join(p, 'archive.db');
  return ['.db', '.sqlite', '.sqlite3'].includes(extname(p).toLowerCase())
    ? p : join(p, 'archive.db');
}

export function open(path, { readonly = false } = {}) {
  const p = resolveDbPath(path);
  mkdirSync(dirname(p), { recursive: true });
  const db = new DatabaseSync(p, { readOnly: readonly && existsSync(p) });
  for (const pragma of readonly && existsSync(p) ? PRAGMAS_RO : PRAGMAS_RW) {
    db.exec(`PRAGMA ${pragma}`);
  }
  return db;
}

/* Backfills that need real code rather than SQL — the same `[table, label, fn]`
   shape and the same ran-once marker, returning how many rows it touched.
   Emptied in the same collapse; the one that lived here re-derived every
   tag.slug through slugify() and has nothing left to find. */
const JS_BACKFILLS = [
];

/* ── the floor ─────────────────────────────────────────────────────────────
 *
 * What this code cannot run without, and therefore what MIGRATIONS no longer
 * carries an entry to create. NOT a schema check — schema.sql is that, and
 * re-deriving it here would be the two-documents problem again in a new place.
 * It is three spot-checks against what the 2026-09-07 collapse ASSUMED, so that
 * a database left behind by it says so in one clear line at open() instead of
 * failing an hour later with `no such column: snippet.fetch_status` from inside
 * a route.
 *
 * Cheap by construction: one sqlite_master lookup and one table_xinfo per
 * entry, once per open. `taglet` is checked from the other direction — its
 * ABSENCE is the floor, because the rebuild that merged it away is the one that
 * no longer exists to do it again.
 */
const FLOOR = [
  ['snippet', 'fetch_status'],   // the last MIGRATIONS-only column to land
  ['tag', 'gate'],               // came across in the taglet merge
  ['stream_tag', 'id'],          // the surrogate key the changeset path needs
  /* `snippet.file_path` is deliberately NOT here. This list is for what
     MIGRATIONS can no longer fix, and MIGRATIONS fixes that one — adding it
     would refuse, at open(), every database the migration above is about to
     bring forward. The test is not "does the code need this column"; it is
     "would the code be stuck without a one-shot". */
];

/** Refuse a database from before the collapse, by name and with the fix.
 *
 *  Silent on an empty file: create() is about to write the schema into it, and
 *  "there is no archive here" is isArchive()'s complaint to make, not this
 *  one's. */
function floorCheck(db) {
  const present = (t) => !!db.prepare(
    `SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`).get(t);
  if (!present('stream')) return;            // empty file; schema.sql follows

  const missing = [];
  for (const [table, column] of FLOOR) {
    if (!present(table)) { missing.push(table); continue; }
    const cols = new Set(db.prepare(`PRAGMA table_xinfo(${table})`).all().map((r) => r.name));
    if (!cols.has(column)) missing.push(`${table}.${column}`);
  }
  if (present('taglet')) missing.push('taglet (never merged into tag)');
  if (!missing.length) return;

  throw new Error(
    `This database predates the 2026-09-07 schema collapse: ${missing.join(', ')}. `
    + 'MIGRATIONS no longer carries the entries that would fix it. Stop the '
    + 'server and run the cutover one-shot against this file first — it holds '
    + 'the old migration lists and brings the database up to the current floor.');
}

/** Check the floor, then apply any migration whose column or table is missing,
 *  any rebuild, and any data backfill that has not run. Returns what it did —
 *  which, on a database already at the current schema, is an empty array. */
/** The ALTERs alone, and the reason they are their own function.
 *
 *  create() execs schema.sql BEFORE migrating, so on an existing database the
 *  CREATE TABLEs are skipped and the CREATE INDEXes are not — which is fine
 *  until an index names a column the ALTERs are about to add. Then schema.sql
 *  aborts on `no such column: kind` and the server never starts.
 *
 *  That trap is why POST_MIGRATION used to exist, and the collapse's note over
 *  it says the trap "stops being a problem once the ALTERs are gone". The ALTERs
 *  came back with memes, so rather than bringing POST_MIGRATION back — a second
 *  place indexes get declared, which is the two-documents problem again — the
 *  ORDER changed: bring the columns up to date first, then let schema.sql
 *  declare everything against a table that already has them.
 *
 *  A no-op on an empty file: every entry is skipped for want of its table, and
 *  the schema exec that follows creates the finished shape anyway. */
export function migrateColumns(db) {
  const applied = [];
  for (const [table, column, sql] of MIGRATIONS) {
    const hasTable = db.prepare(
      `SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`).get(table);
    if (column === null) {
      if (!hasTable) { db.exec(sql); applied.push(`create ${table}`); }
      continue;
    }
    if (!hasTable) continue;
    // table_xinfo, not table_info: the latter hides generated columns, so the
    // check would try to re-add one that already exists.
    const cols = new Set(db.prepare(`PRAGMA table_xinfo(${table})`).all()
      .map((r) => r.name));
    if (!cols.has(column)) { db.exec(sql); applied.push(`${table}.${column}`); }
  }
  return applied;
}

export function migrate(db) {
  floorCheck(db);
  /* Idempotent, so calling it here as well as in create() costs one
     table_xinfo per entry. Kept because migrate() is the documented entry point
     for "bring this database up to date" and a caller who reaches for it
     directly should not get a half-migrated file. */
  const applied = migrateColumns(db);

  const present = (t) => !!db.prepare(
    `SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`).get(t);

  for (const [table, column, fn] of REBUILDS) {
    if (!present(table)) continue;
    const cols = new Set(db.prepare(`PRAGMA table_xinfo(${table})`).all().map((r) => r.name));
    if (cols.has(column)) continue;
    tx(db, () => fn(db, ulid, now()));
    applied.push(`rebuild ${table}`);
  }

  /* RETIRED and RETIRED_TABLES were here — the machinery for dropping a column
     or a whole table that turned out to be a mistake, keyed on the thing still
     existing. Both lists emptied in the collapse (`tag.seg_kind`, the `upload`
     table) and the loops went with them: a removal is rare enough that bringing
     the four lines back when one is needed is cheaper than three empty
     mechanisms sitting here looking like the shape to build against.

     POST_MIGRATION was here too, creating the indexes and triggers that named
     columns this file had just ALTERed in. schema.sql declares all of them now
     and it runs first, so there is nothing left for it to add — the reason it
     existed was that create() execs schema.sql BEFORE the ALTERs, which stops
     being a problem once the ALTERs are gone.

     2026-09-10: the ALTERs came back, and with them an index over one of the
     new columns — so the problem came back too, exactly as written above. It
     was NOT solved by bringing this list back. create() now runs the ALTERs
     before the schema exec instead, which fixes the whole class rather than the
     one index, and keeps every index declared in exactly one file. */

  /* ── the ran-once guard (issues.md #13) ────────────────────────────────
   *
   * MIGRATIONS and REBUILDS ask `PRAGMA table_xinfo` before acting, so they run
   * once by construction. BACKFILLS and JS_BACKFILLS cannot — a backfill writes
   * rows, and afterwards there is no shape to inspect — so they had no guard at
   * all and ran on EVERY boot, and restarts here are frequent: every
   * `docker-compose up -d --build`.
   *
   * That is not a tidiness problem. Two of them wrote columns a human can edit:
   * one rewrote `note.tag` from `srt` to `short`, and `note.tag` is free text
   * somebody types; the other re-derived every `tag.slug` through slugify(),
   * and the applier writes `slug` on rename. So an edit applied, logged,
   * displayed correctly, and was undone at the next restart with nothing in the
   * history saying it happened. The kind columns were the same failure with the
   * volume turned up: they erased every tag kind in the archive once, and every
   * block kind on every restart for weeks.
   *
   * Both lists are empty now and this still runs, because it is the guard that
   * makes adding one safe. Deleting it along with its contents is how that
   * comes back.
   *
   * The marker is the LABEL, not the index. That is deliberate and it is the
   * useful half: change what a backfill does and you change its label, and a
   * changed label is a backfill that has never run, so it runs once more. A
   * backfill whose text you edit without touching its label will NOT re-run —
   * which is the trade, and the label is right there to change.
   *
   * The marker is written whether or not the run touched anything. A backfill
   * that legitimately matched nothing has still run; recording only the ones
   * that changed rows would leave every no-op firing forever, which is most of
   * them on most databases.
   *
   * The markers already in `meta` are the record that the nine backfills the
   * collapse removed did run. They are deliberately NOT cleared: a label that
   * comes back one day meaning something else would find itself already marked,
   * which is the same trap as reusing a migration's key.
   */
  const ranKey = 'backfills_ran';
  let ran;
  try { ran = new Set(JSON.parse(meta(db, ranKey, '[]'))); }
  catch { ran = new Set(); }
  const before = ran.size;
  const mark = (label) => { ran.add(label); };

  for (const [table, label, sql] of BACKFILLS) {
    if (ran.has(label)) continue;
    if (!present(table)) continue;
    const { changes } = db.prepare(sql.trim()).run();
    if (changes) applied.push(`${label} (${changes} rows)`);
    mark(label);
  }

  for (const [table, label, fn] of JS_BACKFILLS) {
    if (ran.has(label)) continue;
    if (!present(table)) continue;
    const nRows = tx(db, () => fn(db, slugify));
    if (nRows) applied.push(`${label} (${nRows} rows)`);
    mark(label);
  }
  /* Only when a marker was actually added, and only if there IS a meta table to
     write to. An empty database — a mistyped path, a fresh checkout — has none
     of this, and `present()` skipped every backfill above for the same reason.
     With both lists empty this writes nothing at all, which is the point: a
     boot that changed nothing should not touch the file. */
  if (ran.size !== before && present('meta')) {
    setMeta(db, ranKey, JSON.stringify([...ran].sort()));
  }
  return applied;
}

/** name -> slug. Lives here rather than in archive.js so migrate() has no
 *  import cycle; archive.js re-exports it as the public spelling. */
export function slugify(name) {
  const folded = String(name ?? '')
    .normalize('NFKD')
    // ONLY U+0300–U+036F, the Latin/Greek/Cyrillic combining diacritics. Kana
    // voicing marks are at U+3099/U+309A and must survive — decomposing ゼルダ
    // and dropping every mark yields セルタ, a different word.
    .replace(/[\u0300-\u036f]/g, '')
    .normalize('NFC')
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80);
  if (folded) return folded;
  let h = 0;
  for (const ch of String(name ?? '')) h = (h * 31 + ch.codePointAt(0)) >>> 0;
  return 'tag-' + h.toString(36);
}

/** Does this file actually hold an archive?
 *
 *  open() will happily bring a brand-new empty database into existence at a
 *  mistyped path, and then every tool downstream reports something baffling
 *  about a missing table instead of "there is nothing here". Ask first. */
export function isArchive(path) {
  const p = resolveDbPath(path);
  if (!existsSync(p)) return false;
  const db = new DatabaseSync(p, { readOnly: true });
  try {
    return !!db.prepare(
      `SELECT 1 FROM sqlite_master WHERE type='table' AND name='stream'`).get();
  } catch { return false; } finally { db.close(); }
}

/** Open, create the schema if absent, migrate. Safe on every start.
 *
 *  Three steps, in an order that is entirely load-bearing.
 *
 *  floorCheck runs BEFORE anything else. schema.sql's CREATE TABLEs are all IF
 *  NOT EXISTS, so on an existing database they are skipped and its columns are
 *  NOT added — but the CREATE INDEXes still run, and one of them names
 *  `snippet.source_url`. Against a database from before the collapse that
 *  aborts the boot with `no such column: source_url`, which is a true statement
 *  about the wrong problem. Asking first turns that into a sentence naming the
 *  cutover.
 *
 *  migrateColumns runs SECOND, and that is newer than it looks. It used to be
 *  part of migrate() below the schema exec, which was fine for as long as
 *  MIGRATIONS was empty. The moment an ALTER adds a column and schema.sql
 *  declares an index over it, the exec hits that index on a database that has
 *  not been ALTERed yet and dies. Columns first, then the declaration.
 *
 *  schema.sql is THIRD and is the whole truth: every table, every index, every
 *  trigger, against a table shape that is now current whichever kind of
 *  database this is.
 *
 *  migrate() is LAST for the rebuilds and the data repairs, which need the
 *  finished schema to run against. */
export function create(path) {
  const db = open(path);
  floorCheck(db);
  migrateColumns(db);
  db.exec(readFileSync(SCHEMA_PATH, 'utf8'));
  migrate(db);
  return db;
}

// ---------------------------------------------------------------------------
// ULID — 26 chars, Crockford base32: 48 bits of millisecond timestamp then 80
// bits of randomness.
//
// Opaque: it hashes nothing, so correcting a typo can never change an id — the
// entire reason the id exists. Sortable: lexicographic order is creation order,
// which makes it a stable tiebreaker in keyset pagination.
// ---------------------------------------------------------------------------

const ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';  // no I, L, O or U
const DECODE = new Map([...ALPHABET].map((c, i) => [c, i]));

function encodeTime(ms, len) {
  let out = '';
  for (let i = 0; i < len; i++) {
    out = ALPHABET[ms % 32] + out;
    ms = Math.floor(ms / 32);
  }
  return out;
}

function encodeRandom(len) {
  const bytes = new Uint8Array(len);
  randomFillSync(bytes);
  let out = '';
  // Low 5 bits of each byte is uniform over 0..31, so no modulo bias.
  for (const b of bytes) out += ALPHABET[b & 31];
  return out;
}

export function ulid(whenMs = Date.now()) {
  return encodeTime(Math.floor(whenMs), 10) + encodeRandom(16);
}

/** A ULID whose timestamp component is a specific moment — used when seeding so
 *  ids sort in broadcast order rather than insertion order. */
export function ulidAt(unixSeconds) {
  return ulid(Math.floor(unixSeconds * 1000));
}

export function isUlid(v) {
  return typeof v === 'string' && v.length === 26 &&
    [...v].every((c) => DECODE.has(c.toUpperCase()));
}

export function ulidTime(v) {
  let ms = 0;
  for (const c of v.slice(0, 10)) ms = ms * 32 + DECODE.get(c.toUpperCase());
  return ms;
}

// ---------------------------------------------------------------------------
// small helpers used everywhere
// ---------------------------------------------------------------------------

export const now = () => Math.floor(Date.now() / 1000);

/** SQLite has no boolean type and node:sqlite will not coerce one. */
export const bit = (v) => (v ? 1 : 0);

/** Run fn inside an immediate transaction, rolling back on any throw. */
/** Is this the database saying "somebody else is writing"? */
const isBusy = (e) => /SQLITE_BUSY|database is locked/i.test(
  String(e?.code ?? '') + ' ' + String(e?.message ?? ''));

/* How long to keep trying to START a write. `busy_timeout = 5000` already makes
 * each attempt wait five seconds inside SQLite, so this is five more tries on
 * top of that, not five seconds total.
 *
 * The retries are on BEGIN IMMEDIATE ONLY, and that is the whole design. BEGIN
 * IMMEDIATE takes the write lock up front, so under WAL with one writer this is
 * where contention actually shows — a long `probe-media.js` pass, or the Pi
 * posting ingest while somebody saves a note. Once BEGIN has succeeded, nothing
 * has run yet, so retrying it cannot repeat any work. A BUSY from INSIDE fn() is
 * deliberately NOT retried: fn may have done something already, and rerunning a
 * half-applied changeset to dodge a lock is a worse bug than the 500 it saves.
 */
const BEGIN_TRIES = 6;
const BEGIN_BACKOFF_MS = 120;

export function tx(db, fn) {
  let started = false;
  for (let i = 0; i < BEGIN_TRIES && !started; i++) {
    try { db.exec('BEGIN IMMEDIATE'); started = true; }
    catch (e) {
      if (!isBusy(e) || i === BEGIN_TRIES - 1) throw e;
      // Synchronous on purpose: every caller of this is synchronous, node:sqlite
      // is synchronous, and the request this is inside is already blocked.
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0,
                   BEGIN_BACKOFF_MS * (i + 1));
    }
  }
  try {
    const out = fn();
    db.exec('COMMIT');
    return out;
  } catch (e) {
    try { db.exec('ROLLBACK'); } catch { /* already rolled back */ }
    throw e;
  }
}

export function meta(db, key, fallback = null) {
  const r = db.prepare('SELECT value FROM meta WHERE key = ?').get(key);
  return r ? r.value : fallback;
}

export function setMeta(db, key, value) {
  db.prepare(`INSERT INTO meta(key, value) VALUES(?, ?)
              ON CONFLICT(key) DO UPDATE SET value = excluded.value`)
    .run(key, String(value));
}

/** Bumped on every write. The API turns it into an ETag, so a browser scrolling
 *  the grid gets 304s and the cache key is trivially correct. */
export function bumpGeneration(db) {
  db.prepare(`INSERT INTO meta(key, value) VALUES('generation', '1')
              ON CONFLICT(key) DO UPDATE SET
                value = CAST(CAST(value AS INTEGER) + 1 AS TEXT)`).run();
  return Number(meta(db, 'generation', '0'));
}
