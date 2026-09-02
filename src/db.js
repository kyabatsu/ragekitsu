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

// Forward-only and idempotent: each entry runs only if its column or table is
// absent. Once real user content exists, "delete and re-import" stops being an
// option and this list is the only way the schema moves.
const MIGRATIONS = [
  ['stream', 'local_date',
    `ALTER TABLE stream ADD COLUMN local_date TEXT GENERATED ALWAYS AS
       (date(started_at + tz_offset_min * 60, 'unixepoch')) VIRTUAL`],
  ['stream', 'local_month',
    `ALTER TABLE stream ADD COLUMN local_month TEXT GENERATED ALWAYS AS
       (strftime('%Y-%m', started_at + tz_offset_min * 60, 'unixepoch')) VIRTUAL`],
  ['stream', 'start_sod',
    `ALTER TABLE stream ADD COLUMN start_sod INTEGER GENERATED ALWAYS AS
       ((started_at + tz_offset_min * 60) % 86400) VIRTUAL`],
  ['upload', null,
    `CREATE TABLE IF NOT EXISTS upload (
       id TEXT PRIMARY KEY,
       person_id TEXT REFERENCES person(id) ON DELETE SET NULL,
       kind TEXT NOT NULL DEFAULT 'thumb', path TEXT NOT NULL, mime TEXT,
       bytes INTEGER, sha256 TEXT, status TEXT NOT NULL DEFAULT 'quarantined',
       created_at INTEGER NOT NULL,
       reviewed_by TEXT REFERENCES person(id), reviewed_at INTEGER)`],

  /* ---- the job queue ------------------------------------------------------
     The archive never fetches, never writes to the media tree and never
     deletes from it. It publishes INTENT here and ls-rec subscribes: the Pi
     asks for work on a tick it already runs, and the archive opens no outbound
     socket at all. See `File management` in review.md.

     The one rule the shape enforces: a job names an ID and a VERB. `url` is
     the single exception and it is inert here — the host allowlist lives in
     ls-rec's config, so a compromised archive still cannot make the recorder
     fetch from an attacker's host. Nothing in this table is a path the worker
     is told to trust; roots and filenames come from the worker's own config.

     status
       proposed  somebody asked for it; no worker will see it
       approved  an editor said yes — this is what the poll claims
       claimed   a worker holds a lease; another poll skips it until it lapses
       done      finished, result_path recorded
       failed    terminal, and `error` says why. NOT re-queued automatically:
                 a job that fails ffmpeg and returns to the queue is an
                 infinite loop with a 74-second period. An editor re-approves. */
  ['job', null,
    `CREATE TABLE IF NOT EXISTS job (
       id           TEXT PRIMARY KEY,
       kind         TEXT NOT NULL,              -- fetch | promote | purge
       status       TEXT NOT NULL DEFAULT 'proposed',
       snippet_id   TEXT REFERENCES snippet(id) ON DELETE CASCADE,
       url          TEXT,
       payload      TEXT,                       -- JSON; kind-specific, never a path
       requested_by TEXT REFERENCES person(id) ON DELETE SET NULL,
       approved_by  TEXT REFERENCES person(id) ON DELETE SET NULL,
       claimed_by   TEXT,                       -- the worker's own name for itself
       claimed_at   INTEGER,
       attempts     INTEGER NOT NULL DEFAULT 0,
       result_path  TEXT,
       error        TEXT,
       created_at   INTEGER NOT NULL,
       updated_at   INTEGER NOT NULL,
       finished_at  INTEGER)`],
  // --- clocks -------------------------------------------------------------
  // Two absolute times per capture instead of one relative one. See the block
  // comment on `capture` in schema.sql for why relative was never going to
  // hold: there is no single broadcast clock to be relative TO.
  ['capture', 'remote_start_wall',
    'ALTER TABLE capture ADD COLUMN remote_start_wall INTEGER'],
  ['capture', 'local_start_wall',
    'ALTER TABLE capture ADD COLUMN local_start_wall INTEGER'],
  ['capture', 'local_start_precision_s',
    'ALTER TABLE capture ADD COLUMN local_start_precision_s INTEGER'],

  // --- ffprobe ------------------------------------------------------------
  ['capture', 'container',   'ALTER TABLE capture ADD COLUMN container TEXT'],
  ['capture', 'video_codec', 'ALTER TABLE capture ADD COLUMN video_codec TEXT'],
  ['capture', 'audio_codec', 'ALTER TABLE capture ADD COLUMN audio_codec TEXT'],
  ['capture', 'width',       'ALTER TABLE capture ADD COLUMN width INTEGER'],
  ['capture', 'height',      'ALTER TABLE capture ADD COLUMN height INTEGER'],
  ['capture', 'fps',         'ALTER TABLE capture ADD COLUMN fps REAL'],
  ['capture', 'has_audio',   'ALTER TABLE capture ADD COLUMN has_audio INTEGER'],
  ['capture', 'probed_at',   'ALTER TABLE capture ADD COLUMN probed_at INTEGER'],

  // --- frame + precision on notes -----------------------------------------
  // ADD COLUMN with a NOT NULL DEFAULT fills every existing row, so all 915
  // vault notes become frame='unknown' by the ALTER itself. That is correct:
  // they were read off some VOD nobody wrote down.
  ['note', 'frame',
    `ALTER TABLE note ADD COLUMN frame TEXT NOT NULL DEFAULT 'unknown'`],
  ['note', 'anchor_clock',       'ALTER TABLE note ADD COLUMN anchor_clock TEXT'],
  // The timestamp expression as typed, when it holds more than one point. See
  // the block comment on `note.stamp` in schema.sql.
  ['note', 'stamp',              'ALTER TABLE note ADD COLUMN stamp TEXT'],
  ['note', 'offset_precision_s', 'ALTER TABLE note ADD COLUMN offset_precision_s INTEGER'],

  // --- segments -----------------------------------------------------------
  ['segment', null,
    `CREATE TABLE IF NOT EXISTS segment (
       id TEXT PRIMARY KEY,
       stream_id TEXT NOT NULL REFERENCES stream(id) ON DELETE CASCADE,
       frame TEXT NOT NULL DEFAULT 'stream',
       anchor_id TEXT REFERENCES capture(id) ON DELETE SET NULL,
       anchor_clock TEXT,
       start_s INTEGER NOT NULL,
       end_s INTEGER,
       kind TEXT NOT NULL DEFAULT 'unknown',
       label TEXT,
       origin TEXT NOT NULL DEFAULT 'user',
       author_id TEXT REFERENCES person(id) ON DELETE SET NULL,
       retracted_at INTEGER,
       created_at INTEGER NOT NULL,
       updated_at INTEGER NOT NULL)`],

  /* --- the second lane -----------------------------------------------------
     Defaulting to 0 is the whole migration: every chapter ever drawn is a
     lane-0 chapter, stays exactly where it is, and keeps tiling as before.
     No index. The lane is filtered in memory alongside the axis conversion
     that already has to happen per row, and a stream has tens of segments,
     not thousands — ix_segment_stream(stream_id, start_s) is still the one
     that matters. */
  ['segment', 'lane', 'ALTER TABLE segment ADD COLUMN lane INTEGER NOT NULL DEFAULT 0'],

  // --- materialised timeline ----------------------------------------------
  ['stream', 'timeline_json', 'ALTER TABLE stream ADD COLUMN timeline_json TEXT'],
  ['stream', 'timeline_at',   'ALTER TABLE stream ADD COLUMN timeline_at INTEGER'],

  // --- the merged chat ------------------------------------------------------
  // Nothing is backfilled. Every existing stream keeps chat_path NULL, which
  // recompute() reads as "never merged" and falls back to the per-capture
  // paths — so the 371 imported streams carry on exactly as before, and only
  // an entry ls-audit has actually merged switches to the new model.
  ['stream', 'chat_path',    'ALTER TABLE stream ADD COLUMN chat_path TEXT'],
  ['stream', 'chat_sources', 'ALTER TABLE stream ADD COLUMN chat_sources TEXT'],
  ['stream', 'chat_ok',      'ALTER TABLE stream ADD COLUMN chat_ok INTEGER NOT NULL DEFAULT 0'],

  // --- tag becomes an entity ----------------------------------------------
  // updated_at first, and it is not cosmetic: apply() stamps created_at AND
  // updated_at on every create, so `tag` not having one meant creating a tag
  // through a changeset threw `table tag has no column named updated_at`.
  // Tags have been read-only through the write path since the beginning.
  ['tag', 'updated_at',   'ALTER TABLE tag ADD COLUMN updated_at INTEGER'],
  ['tag', 'parent_id',    'ALTER TABLE tag ADD COLUMN parent_id TEXT REFERENCES tag(id) ON DELETE SET NULL'],
  ['tag', 'thumb_path',   'ALTER TABLE tag ADD COLUMN thumb_path TEXT'],
  ['tag', 'summary',      'ALTER TABLE tag ADD COLUMN summary TEXT'],
  ['tag', 'status',       `ALTER TABLE tag ADD COLUMN status TEXT NOT NULL DEFAULT 'confirmed'`],
  ['tag', 'origin',       `ALTER TABLE tag ADD COLUMN origin TEXT NOT NULL DEFAULT 'vault'`],
  ['tag', 'author_id',    'ALTER TABLE tag ADD COLUMN author_id TEXT REFERENCES person(id) ON DELETE SET NULL'],
  ['tag', 'retracted_at', 'ALTER TABLE tag ADD COLUMN retracted_at INTEGER'],

  ['segment', 'tag_id', 'ALTER TABLE segment ADD COLUMN tag_id TEXT REFERENCES tag(id) ON DELETE SET NULL'],

  // --- snippets -----------------------------------------------------------
  //
  // A snippet is a short standalone clip on the NAS beside the raws. It is NOT
  // a capture and not a segment: it has no broadcast, no platform, no clock to
  // convert through, and its offsets are its own file's seconds and nothing
  // else. Giving it its own table means none of the axis machinery — covers_s,
  // anchor_clock, positionToAxis — has to grow a case for something that never
  // had a second clock to be wrong about.
  //
  // source_stream_id is a link, not a dependency: most of these were cut long
  // before this archive existed and will never resolve to a stream row.
  ['snippet', null,
    `CREATE TABLE IF NOT EXISTS snippet (
       id            TEXT PRIMARY KEY,
       slug          TEXT NOT NULL UNIQUE,
       title         TEXT NOT NULL,
       summary       TEXT,
       video_path    TEXT NOT NULL,
       poster_path   TEXT,
       duration_s    REAL,
       width         INTEGER,
       height        INTEGER,
       bytes         INTEGER,
       source_stream_id TEXT REFERENCES stream(id) ON DELETE SET NULL,
       source_offset_s  INTEGER,
       -- The flat join of snippet_line.text, denormalised so FTS5 can index one
       -- column on one row. Derived, and rewritten whenever the lines are.
       transcript    TEXT,
       -- none | auto | edited. 'auto' is a machine's best guess and should be
       -- allowed to look like one; 'edited' means a human has been through it.
       transcript_status TEXT NOT NULL DEFAULT 'none',
       status        TEXT NOT NULL DEFAULT 'confirmed',
       origin        TEXT NOT NULL DEFAULT 'vault',
       author_id     TEXT REFERENCES person(id) ON DELETE SET NULL,
       retracted_at  INTEGER,
       created_at    INTEGER NOT NULL,
       updated_at    INTEGER NOT NULL)`],

  // One row per transcript line, with the timing WhisperX already produced.
  // Not tombstoned and not changeset-managed: a transcript is replaced whole by
  // its next pass, and thirty rows of "who edited line 14" is noise, not
  // history. The editable human-facing text is snippet.summary.
  ['snippet_line', null,
    `CREATE TABLE IF NOT EXISTS snippet_line (
       id         TEXT PRIMARY KEY,
       snippet_id TEXT NOT NULL REFERENCES snippet(id) ON DELETE CASCADE,
       seq        INTEGER NOT NULL,
       start_s    REAL NOT NULL,
       end_s      REAL,
       speaker    TEXT,
       text       TEXT NOT NULL,
       UNIQUE(snippet_id, seq))`],

  // --- taglets ------------------------------------------------------------
  //
  // Deliberately NOT the `tag` table. That vocabulary is a curated list of
  // things a stream was ABOUT — a game, a format — and it is shared with
  // segment.kind, where every entry has to mean a colour on a timeline.
  // `meta:funny` is not a chapter colour and `copyright:phase_connect` is not
  // something a stream is about. Two vocabularies because they answer two
  // questions; one table would have forced every taglet kind to also be a
  // legal chapter kind.
  //
  // The shape is Danbooru's, because that is the shape that works for a
  // thousand short clips: a namespace, a machine slug, a display name.
  ['taglet', null,
    `CREATE TABLE IF NOT EXISTS taglet (
       id           TEXT PRIMARY KEY,
       name         TEXT NOT NULL,
       slug         TEXT NOT NULL UNIQUE,
       kind         TEXT NOT NULL DEFAULT 'general',
       summary      TEXT,
       status       TEXT NOT NULL DEFAULT 'confirmed',
       origin       TEXT NOT NULL DEFAULT 'vault',
       author_id    TEXT REFERENCES person(id) ON DELETE SET NULL,
       retracted_at INTEGER,
       created_at   INTEGER NOT NULL,
       updated_at   INTEGER NOT NULL)`],

  ['snippet_taglet', null,
    `CREATE TABLE IF NOT EXISTS snippet_taglet (
       id         TEXT PRIMARY KEY,
       snippet_id TEXT NOT NULL REFERENCES snippet(id) ON DELETE CASCADE,
       taglet_id  TEXT NOT NULL REFERENCES taglet(id) ON DELETE CASCADE,
       created_at INTEGER NOT NULL,
       updated_at INTEGER NOT NULL,
       UNIQUE(snippet_id, taglet_id))`],

  ['snippet_fts', null,
    `CREATE VIRTUAL TABLE IF NOT EXISTS snippet_fts USING fts5(
       title, summary, transcript,
       content='snippet', content_rowid='rowid',
       tokenize = "porter unicode61 remove_diacritics 2",
       prefix = '2 3')`],

  // --- what the transcriber said about itself -----------------------------
  //
  // The ASR run's own report, kept because at a thousand clips the question is
  // not "is there a transcript" but "which forty do I need to run again, and
  // why". Without these, a clip whose transcription FAILED and a clip that is
  // genuinely silent both read as transcript_status='none' and there is no way
  // to tell them apart short of re-reading a thousand sidecars.
  //
  // Also makes a model upgrade tractable: `WHERE transcript_model='large-v3'`
  // is the list to re-run when something better arrives.
  /* When the clip landed, as distinct from when this row was made.
     created_at is the row's own age and every import stamps it with the same
     second, so ordering on it — or on the ULID, which is minted from it —
     sorts a thousand clips by the order readdir happened to return them, which
     is alphabetical. That put the newest clip at the bottom of the list.
     Taken from the file's mtime, which is the only record of when a clip was
     added that survives outside the archive. */
  ['snippet', 'added_at', 'ALTER TABLE snippet ADD COLUMN added_at INTEGER'],

  /* What the file actually CONTAINS, recorded because the extension is a claim
     and `format_name` cannot settle it either — ffprobe reports
     "matroska,webm" for a real WebM and an ordinary Matroska alike. Without
     these three the server has no way to send an honest Content-Type, and a
     clip whose codecs a browser cannot decode simply renders blank with
     nothing logged anywhere. See servedType() in archive.js. */
  ['snippet', 'container',   'ALTER TABLE snippet ADD COLUMN container TEXT'],
  ['snippet', 'video_codec', 'ALTER TABLE snippet ADD COLUMN video_codec TEXT'],
  ['snippet', 'audio_codec', 'ALTER TABLE snippet ADD COLUMN audio_codec TEXT'],
  /* A web-playable rewrap of an unplayable original, under the CACHE root —
     never the media root, which is mounted read-only and holds the masters.
     Set only when the original cannot be served as-is; NULL is the normal
     case and means "serve video_path directly". */
  ['snippet', 'play_path',   'ALTER TABLE snippet ADD COLUMN play_path TEXT'],
  /* Content hash, filled by the audit rather than the importer — hashing 70GB
     costs ten minutes and answers a question the import does not ask. It earns
     its place twice: it finds the same clip filed twice under two names in a
     collection scraped together over years, and it is what a future upload
     checks against to know it already has this file. */
  ['snippet', 'sha256',      'ALTER TABLE snippet ADD COLUMN sha256 TEXT'],

  ['snippet', 'transcript_model', 'ALTER TABLE snippet ADD COLUMN transcript_model TEXT'],
  ['snippet', 'transcript_at',    'ALTER TABLE snippet ADD COLUMN transcript_at INTEGER'],
  // The transcriber's own reason for a non-ok status. Free text from the tool,
  // shown to nobody but an admin looking at a failed batch.
  ['snippet', 'transcript_note',  'ALTER TABLE snippet ADD COLUMN transcript_note TEXT'],
];

// Rebuilds — for the shape changes ALTER TABLE cannot express. Each runs only
// when its table exists and is missing the column that marks it done, so
// re-running is free. Kept separate from MIGRATIONS because these copy data.
const REBUILDS = [
  // stream_tag needs a surrogate id. `change.target_id` holds ONE value, so a
  // composite primary key is unaddressable by a changeset — which is why
  // attaching a tag to a stream was unreachable through the only write path
  // the archive has. SQLite cannot add a PRIMARY KEY column, so: copy, drop,
  // rename, minting an id per existing pair.
  ['stream_tag', 'id', (db, ulidFn, t) => {
    db.exec(`CREATE TABLE stream_tag_new (
      id TEXT PRIMARY KEY,
      stream_id TEXT NOT NULL REFERENCES stream(id) ON DELETE CASCADE,
      tag_id    TEXT NOT NULL REFERENCES tag(id) ON DELETE CASCADE,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL,
      UNIQUE(stream_id, tag_id))`);
    const ins = db.prepare(
      'INSERT INTO stream_tag_new(id, stream_id, tag_id, created_at, updated_at) VALUES(?,?,?,?,?)');
    for (const r of db.prepare('SELECT stream_id, tag_id FROM stream_tag').all()) {
      ins.run(ulidFn(), r.stream_id, r.tag_id, t, t);
    }
    db.exec('DROP TABLE stream_tag');
    db.exec('ALTER TABLE stream_tag_new RENAME TO stream_tag');
    db.exec('CREATE INDEX IF NOT EXISTS ix_stream_tag_rev ON stream_tag(tag_id, stream_id)');
  }],
];

// Columns that turned out to be a mistake. Each runs only while its column is
// still there, so it is a no-op on every database that has already lost it —
// including a fresh one, which never grows the column in the first place.
//
// Dropping rather than leaving it orphaned: an unused column that still holds
// plausible-looking values is a trap. Someone reads `seg_kind` off a row in a
// year, believes it, and now there are two answers to what colour a tag is.
const RETIRED = [
  // tag.seg_kind was a SECOND vocabulary (idle|talk|game|read|music) running
  // parallel to tag.kind (game|format|person|other). Two lists for one
  // question. The values it held were derived from `kind` by a backfill and
  // never edited by hand, so nothing is lost by dropping it — the remap below
  // works from `kind`, which is the column a human actually set.
  ['tag', 'seg_kind', 'ALTER TABLE tag DROP COLUMN seg_kind'],
];

// Index and trigger creation, run after the ALTERs above so the columns they
// name exist. All IF NOT EXISTS, so re-running is free.
//
// Each is tagged with the table it needs. An empty database — a mistyped path,
// a fresh checkout with no import yet — has none of them, and running these
// against it threw `no such table: main.capture` from inside migrate(), which
// is a spectacularly unhelpful way to say "there is no archive here".
const POST_MIGRATION = [
  // The claim query's index: status first because it is the selective one —
  // a queue is nearly all `done`.
  ['job', `CREATE INDEX IF NOT EXISTS ix_job_claim ON job(status, kind, created_at)`],
  ['job', `CREATE INDEX IF NOT EXISTS ix_job_snippet ON job(snippet_id)
     WHERE snippet_id IS NOT NULL`],
  ['segment', `CREATE INDEX IF NOT EXISTS ix_segment_stream ON segment(stream_id, start_s)
     WHERE retracted_at IS NULL`],
  ['segment', `CREATE INDEX IF NOT EXISTS ix_segment_tag ON segment(tag_id, start_s)
     WHERE tag_id IS NOT NULL AND retracted_at IS NULL`],
  ['tag', `CREATE INDEX IF NOT EXISTS ix_tag_parent ON tag(parent_id) WHERE parent_id IS NOT NULL`],
  ['tag', `CREATE INDEX IF NOT EXISTS ix_tag_live ON tag(status, slug) WHERE retracted_at IS NULL`],
  ['capture', `CREATE TRIGGER IF NOT EXISTS capture_anchor_guard_note
     BEFORE DELETE ON capture
     WHEN EXISTS (SELECT 1 FROM note WHERE anchor_id = OLD.id AND retracted_at IS NULL)
     BEGIN SELECT RAISE(ABORT, 'capture still anchors notes — convert them first'); END`],
  ['capture', `CREATE TRIGGER IF NOT EXISTS capture_anchor_guard_segment
     BEFORE DELETE ON capture
     WHEN EXISTS (SELECT 1 FROM segment WHERE anchor_id = OLD.id AND retracted_at IS NULL)
     BEGIN SELECT RAISE(ABORT, 'capture still anchors segments — convert them first'); END`],

  // --- snippets -----------------------------------------------------------
  // The list's actual sort, so it is an index scan rather than a sort of the
  // whole table. id breaks ties, and does so deterministically — several clips
  // copied in one go share an mtime to the second.
  ['snippet', `CREATE INDEX IF NOT EXISTS ix_snippet_live ON snippet(added_at DESC, id DESC)
     WHERE retracted_at IS NULL`],
  ['snippet_line', `CREATE INDEX IF NOT EXISTS ix_snippet_line ON snippet_line(snippet_id, seq)`],
  ['taglet', `CREATE INDEX IF NOT EXISTS ix_taglet_live ON taglet(status, kind, slug)
     WHERE retracted_at IS NULL`],
  // Both directions: "what is on this snippet" renders every row, and "what is
  // tagged X" is the whole point of the filter.
  ['snippet_taglet', `CREATE INDEX IF NOT EXISTS ix_sniptag_snip ON snippet_taglet(snippet_id)`],
  ['snippet_taglet', `CREATE INDEX IF NOT EXISTS ix_sniptag_tag ON snippet_taglet(taglet_id)`],

  // External-content FTS: the triggers ARE the index. Without them the table
  // silently answers every query with nothing, which looks exactly like "no
  // results" and is the reason to keep these beside the ones for note/stream
  // rather than somewhere clever.
  ['snippet', `CREATE TRIGGER IF NOT EXISTS snippet_ai AFTER INSERT ON snippet BEGIN
     INSERT INTO snippet_fts(rowid, title, summary, transcript)
       VALUES (new.rowid, new.title, new.summary, new.transcript);
   END`],
  ['snippet', `CREATE TRIGGER IF NOT EXISTS snippet_ad AFTER DELETE ON snippet BEGIN
     INSERT INTO snippet_fts(snippet_fts, rowid, title, summary, transcript)
       VALUES('delete', old.rowid, old.title, old.summary, old.transcript);
   END`],
  ['snippet', `CREATE TRIGGER IF NOT EXISTS snippet_au AFTER UPDATE ON snippet BEGIN
     INSERT INTO snippet_fts(snippet_fts, rowid, title, summary, transcript)
       VALUES('delete', old.rowid, old.title, old.summary, old.transcript);
     INSERT INTO snippet_fts(rowid, title, summary, transcript)
       VALUES (new.rowid, new.title, new.summary, new.transcript);
   END`],
];

// Data backfills. Every one is guarded by `IS NULL`, so it fills what has never
// been filled and touches nothing a human has since decided. That is what makes
// re-running safe, and it is why none of these need a "have I run" marker.
//
// The first one is the important one: it freezes the CURRENT meaning of
// offset_s into an absolute time, once. Before it, every capture's position is
// relative to stream.started_at, so correcting a stream's start time drags
// every capture and every note with it. After it, the axis is just an axis.
const BACKFILLS = [
  ['capture', 'capture.remote_start_wall', `
    UPDATE capture SET remote_start_wall =
      (SELECT s.started_at + capture.offset_s FROM stream s WHERE s.id = capture.stream_id)
    WHERE remote_start_wall IS NULL
      AND EXISTS (SELECT 1 FROM stream s WHERE s.id = capture.stream_id)`],

  // The vault's timestamps are good to about two minutes. Saying so is the
  // difference between a pin that is honest and a pin that is a lie.
  ['note', 'note.offset_precision_s', `
    UPDATE note SET offset_precision_s = 120
    WHERE offset_precision_s IS NULL AND origin = 'vault' AND offset_s IS NOT NULL`],

  // Rows imported before added_at existed: the row's own age is the best
  // available answer, and it is what they were being ordered by anyway.
  ['snippet', 'snippet.added_at',
   'UPDATE snippet SET added_at = created_at WHERE added_at IS NULL'],

  // Every tag that already exists came from the import and is therefore
  // already part of the vocabulary — confirmed, not proposed.
  ['tag', 'tag.updated_at', `UPDATE tag SET updated_at = created_at WHERE updated_at IS NULL`],

  // --- one vocabulary: game | person | type | meta -------------------------
  // Both of these are idempotent by construction rather than by a marker: the
  // WHERE clause selects exactly the rows NOT yet in the vocabulary, and the
  // UPDATE puts every row it touches into it. Second run matches nothing.
  //
  // A mapping, not a guess. 'format' is what 'type' was called, so those two
  // are the same word. The old segment words fold in by meaning: talk, read
  // and music are all kinds of stream, and idle is scaffolding. Anything else
  // — a word nobody planned for — becomes 'unknown' rather than being filed
  // under whichever bucket looked closest, because a wrong category is a
  // wrong colour on a strip somebody will read as fact.
  ['tag', 'tag.kind → game|person|type|meta', `
    UPDATE tag SET kind = CASE kind
        WHEN 'format' THEN 'type'
        WHEN 'talk'   THEN 'type'
        WHEN 'music'  THEN 'type'
        WHEN 'read'   THEN 'type'
        WHEN 'idle'   THEN 'meta'
        ELSE 'unknown' END
    WHERE kind IS NULL OR kind NOT IN ('game', 'person', 'type', 'meta', 'unknown')`],

  // One word for one thing. The vault wrote `#srt` — a subtitle file — for what
  // is actually a short, and the editor displays `short`. Storing one word and
  // showing another is the seg_kind trap again, so the rows move. `srt` stays
  // accepted as input; it just never comes back out.
  ['note', 'note.tag srt → short', `
    UPDATE note SET tag = 'short' WHERE tag = 'srt'`],

  ['segment', 'segment.kind → game|person|type|meta', `
    UPDATE segment SET kind = CASE kind
        WHEN 'talk'  THEN 'type'
        WHEN 'music' THEN 'type'
        WHEN 'read'  THEN 'type'
        WHEN 'idle'  THEN 'meta'
        ELSE 'unknown' END
    WHERE kind IS NULL OR kind NOT IN ('game', 'person', 'type', 'meta', 'unknown')`],
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

// Backfills that need real code rather than SQL. Idempotent by construction:
// each converges after one pass and does nothing on the next.
const JS_BACKFILLS = [
  // The importer set slug = lower(name), so slugs still contain spaces, colons
  // and '#'. A '#' in a query string is a URL fragment — the browser never
  // sends it — so `?tag=clair obscur: expedition 33 #7` arrives at the server
  // as `clair obscur: expedition 33 ` and matches nothing. Eight tags in this
  // archive are unreachable for that reason alone.
  //
  // Re-derives every slug through slugify(). Where two names collapse to the
  // same slug (`CLAIR OBSCUR: EXPEDITION 33` and `CLAIR OBSCUR:EXPEDITION 33`
  // differ only by a space) the row is LEFT ALONE and reported: merging two
  // tags moves stream_tag rows around, and that is a decision, not a cleanup.
  ['tag', 'tag.slug', (db, slugify) => {
    const rows = db.prepare('SELECT id, name, slug FROM tag').all();
    const taken = new Map(rows.map((r) => [r.slug, r.id]));
    let fixed = 0;
    for (const r of rows) {
      const want = slugify(r.name);
      if (want === r.slug) continue;
      const holder = taken.get(want);
      if (holder && holder !== r.id) continue;        // collision: leave it
      db.prepare('UPDATE tag SET slug = ? WHERE id = ?').run(want, r.id);
      taken.delete(r.slug); taken.set(want, r.id);
      fixed++;
    }
    return fixed;
  }],
];

/** Apply any migration whose column or table is missing, then the indexes,
 *  triggers and data backfills. Returns what it did. */
export function migrate(db) {
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

  const present = (t) => !!db.prepare(
    `SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`).get(t);

  for (const [table, column, fn] of REBUILDS) {
    if (!present(table)) continue;
    const cols = new Set(db.prepare(`PRAGMA table_xinfo(${table})`).all().map((r) => r.name));
    if (cols.has(column)) continue;
    tx(db, () => fn(db, ulid, now()));
    applied.push(`rebuild ${table}`);
  }

  for (const [table, column, sql] of RETIRED) {
    if (!present(table)) continue;
    const cols = new Set(db.prepare(`PRAGMA table_xinfo(${table})`).all().map((r) => r.name));
    if (!cols.has(column)) continue;
    db.exec(sql);
    applied.push(`drop ${table}.${column}`);
  }

  for (const [table, sql] of POST_MIGRATION) if (present(table)) db.exec(sql);

  for (const [table, label, sql] of BACKFILLS) {
    if (!present(table)) continue;
    const { changes } = db.prepare(sql.trim()).run();
    if (changes) applied.push(`${label} (${changes} rows)`);
  }

  for (const [table, label, fn] of JS_BACKFILLS) {
    if (!present(table)) continue;
    const nRows = tx(db, () => fn(db, slugify));
    if (nRows) applied.push(`${label} (${nRows} rows)`);
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

/** Open, create the schema if absent, migrate. Safe on every start. */
export function create(path) {
  const db = open(path);
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
export function tx(db, fn) {
  db.exec('BEGIN IMMEDIATE');
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
