-- ===========================================================================
-- Tenma archive — schema.
--
-- THE COMPLETE DEFINITION. Everything the archive needs is declared here, in
-- one file, and db.js adds nothing to it. That was not true for most of this
-- project's life: the schema was whatever this file said PLUS sixty-eight
-- forward migrations, so the only way to know a database's real shape was to
-- read both and replay one of them in your head. Three tables and eight
-- columns had drifted out of this file entirely and existed only as ALTERs.
--
-- Keep it true. A new column goes HERE first, and then into MIGRATIONS in
-- db.js so that existing databases catch up — both, always, because this file
-- is what a fresh database is built from and MIGRATIONS is what an old one
-- moves through. schema_version below is bumped when the two are collapsed
-- together again; see the dated note over MIGRATIONS.
--
-- Every fact in the archive sorts into one of three kinds, and knowing which
-- kind a fact is tells you who is allowed to write it:
--
--   OBSERVATION   a machine measured it. This file is 19,914s long. This path
--                 exists. This video id is Mgv62FpvhPA. Re-derivable; if it is
--                 wrong you re-measure.       -> capture.*, *_ok, alive
--
--   DECISION      the archive's current answer. The title we show. When the
--                 broadcast "really" started.  -> stream.*, note.*
--
--   PROPOSAL      a request to change a decision, with an author and a time.
--                                              -> changeset + change
--
-- There is exactly ONE write path to a decision: an applied changeset. Direct
-- edits by staff are changesets that were auto-approved. That means no value in
-- this database exists without a row explaining who put it there and why, and
-- there is no back door that mutates state without leaving a trace.
--
-- The DB file must live on local disk. SQLite locking over SMB/NFS is broken.
-- ===========================================================================

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
) WITHOUT ROWID;

INSERT OR IGNORE INTO meta(key, value) VALUES ('schema_version', '4');
INSERT OR IGNORE INTO meta(key, value) VALUES ('generation', '0');

-- ---------------------------------------------------------------------------
-- people
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS person (
  id            TEXT PRIMARY KEY,            -- ULID
  provider      TEXT NOT NULL,               -- discord | twitch | dev | system
  provider_uid  TEXT NOT NULL,
  handle        TEXT NOT NULL,
  display_name  TEXT,
  avatar_url    TEXT,
  role          TEXT NOT NULL DEFAULT 'viewer',   -- viewer|suggester|editor|admin
  banned        INTEGER NOT NULL DEFAULT 0,
  created_at    INTEGER NOT NULL,
  last_seen_at  INTEGER,
  UNIQUE(provider, provider_uid)
);
CREATE INDEX IF NOT EXISTS ix_person_role ON person(role) WHERE role <> 'viewer';

-- Only the hash is stored: a leaked database must not hand over live logins.
CREATE TABLE IF NOT EXISTS session (
  token_hash TEXT PRIMARY KEY,
  person_id  TEXT NOT NULL REFERENCES person(id) ON DELETE CASCADE,
  created_at INTEGER NOT NULL,
  expires_at INTEGER NOT NULL,
  user_agent TEXT
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS ix_session_expiry ON session(expires_at);

-- What a person is allowed past. One row per grant held, rather than a list on
-- `person`, because the interesting questions are "who can see the restricted
-- set" and "when was this given, and by whom" — both a scan of a column here,
-- and neither of them a question a JSON blob answers.
--
-- The gate itself lives on `tag`: a tag with a gate restricts whatever carries
-- it, and a viewer needs a grant of that name to see it.
CREATE TABLE IF NOT EXISTS person_grant (
  id         TEXT PRIMARY KEY,                -- ULID
  person_id  TEXT NOT NULL REFERENCES person(id) ON DELETE CASCADE,
  name       TEXT NOT NULL,
  granted_by TEXT REFERENCES person(id) ON DELETE SET NULL,
  created_at INTEGER NOT NULL
);
-- Holding a grant twice is not a different state from holding it once, and the
-- UNIQUE is what lets the grant endpoint be idempotent rather than having to
-- read-then-write.
CREATE UNIQUE INDEX IF NOT EXISTS person_grant_one ON person_grant(person_id, name);

-- ---------------------------------------------------------------------------
-- stream — the broadcast. The archive's unit of meaning.
--
-- May have zero captures: "we know this happened and we have nothing" is a
-- legitimate, and often the most valuable, row.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stream (
  id             TEXT PRIMARY KEY,           -- ULID. Opaque, permanent.
  idx            INTEGER UNIQUE,             -- the .md number. A label: mutable,
                                             -- nullable, never a key.
  title          TEXT NOT NULL,
  summary        TEXT,

  -- Decisions, defaulted from the captures but freely overridable. Pure
  -- derivation would leave a zero-capture placeholder with no start time at
  -- all — and that is exactly the row where a human types "around 8pm".
  started_at     INTEGER NOT NULL,           -- unix seconds, UTC
  tz_offset_min  INTEGER NOT NULL DEFAULT 0, -- verbatim from the source, never
                                             -- re-derived from a zone name
  duration_s     INTEGER,

  -- States, not booleans. A flag cannot distinguish "we lost this" from "we
  -- have not looked yet", and that difference is the archive's core claim.
  --   present | truncated | lost | never | unverified | scheduled
  vod_state      TEXT NOT NULL DEFAULT 'unverified',
  chat_state     TEXT NOT NULL DEFAULT 'unverified',

  -- NULL = pick automatically (alive remote > mirror > local). Set only for the
  -- cases that actually occur: a DMCA-muted Twitch VOD, an age-gated YouTube
  -- one. Storing a required choice on every stream means maintaining hundreds
  -- of answers nobody has an opinion about.
  serve_pref     TEXT,                       -- NULL | remote | mirror | local
  thumb_path     TEXT,                       -- chosen/uploaded still

  -- The merged chat: every platform's messages in one origin-tagged file, and
  -- the only chat the site ever serves. Written by ls-audit once its pipeline
  -- has merged the raws; the raws then leave for deep storage and the archive
  -- deliberately stops tracking them.
  --
  -- chat_sources is what survives that: a canonical sorted list of the
  -- platforms that actually contributed messages ('YT', 'TW', 'YT,TW'). Once
  -- the raws are gone this is a stored claim rather than a verified fact,
  -- which is exactly why it is written from the merge result and never
  -- inferred from what happens to be on disk afterwards.
  chat_path      TEXT,                       -- merged file, under the media root
  chat_sources   TEXT,                       -- 'YT' | 'TW' | 'YT,TW'
  chat_ok        INTEGER NOT NULL DEFAULT 0, -- merged file seen on disk

  -- What the merged file says about itself, read out of its header by
  -- ls-audit and sent alongside the path. The archive never opens a chat
  -- file: they run to megabytes, and the theater's question — "is there
  -- anything here, and should I warn about it" — has to be answerable from
  -- the row.
  chat_version   INTEGER,                    -- merged schema version
  chat_messages  INTEGER,                    -- how many, after dedupe
  chat_first_ms  INTEGER,                    -- epoch ms of the first message
  chat_last_ms   INTEGER,                    -- ...and the last
  -- {"YT":"complete","TW":"unknown"} — per platform, because it is a property
  -- of how that platform was captured. complete: the source records deletions
  -- and bans. none: it cannot (a VOD download has no moderation history).
  -- unknown: a live Twitch capture from before the recorder could see
  -- CLEARCHAT at all. Sorted keys, no spaces, so it compares as a string.
  chat_moderation TEXT,
  -- WHICH FILE the five columns above describe.
  --
  -- Not redundant with chat_path: chat_path is editable — the record editor's
  -- picker repoints it in two clicks — and the moment it moves, the count and
  -- the span become five perfectly plausible numbers about a different file.
  -- Nothing in them would look wrong. So the reader compares this against
  -- chat_path and treats a mismatch as "not known", and the next ls-audit run
  -- fills it in again. Derived on write, never sent and never editable.
  chat_meta_path TEXT,

  -- Tombstone. Never DELETE a stream: the next vault import would helpfully
  -- recreate it, and someone may have been wrong.
  retracted_at   INTEGER,
  retracted_why  TEXT,
  merged_into    TEXT REFERENCES stream(id) ON DELETE SET NULL,

  origin         TEXT NOT NULL DEFAULT 'vault',  -- vault | ingest | web
  created_at     INTEGER NOT NULL,
  updated_at     INTEGER NOT NULL,

  -- The projected timeline, rebuilt by recompute() on every write that touches
  -- this stream. Not a cache for speed — at this size nothing is slow. It is so
  -- the strip, the pins and the player are projected ONCE, from one place, and
  -- cannot disagree with each other the way three call sites would.
  -- scripts/check-db.js re-derives it and shouts if it has drifted.
  timeline_json  TEXT,
  timeline_at    INTEGER,

  -- Local-calendar keys, GENERATED rather than stored by hand. The calendar
  -- asks a local-time question ("which days in July") while started_at is UTC
  -- and the offset varies per row (-5 in July, -6 in January). SQLite computes
  -- these, so they can never drift out of sync the way a denormalised column
  -- would, and VIRTUAL ones are still indexable.
  local_date     TEXT    GENERATED ALWAYS AS
                   (date(started_at + tz_offset_min * 60, 'unixepoch')) VIRTUAL,
  local_month    TEXT    GENERATED ALWAYS AS
                   (strftime('%Y-%m', started_at + tz_offset_min * 60, 'unixepoch')) VIRTUAL,
  -- Seconds since local midnight: where the ring segment starts on a 24h clock.
  start_sod      INTEGER GENERATED ALWAYS AS
                   ((started_at + tz_offset_min * 60) % 86400) VIRTUAL
);

-- Keyset pagination walks strictly backwards in time; matching the index
-- direction to the sort makes it a plain backwards range scan.
CREATE INDEX IF NOT EXISTS ix_stream_feed ON stream(started_at DESC, id DESC)
  WHERE retracted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_stream_live ON stream(vod_state)
  WHERE retracted_at IS NULL AND vod_state <> 'present';
CREATE INDEX IF NOT EXISTS ix_stream_month ON stream(local_month, local_date)
  WHERE retracted_at IS NULL;

-- ---------------------------------------------------------------------------
-- capture — one recording of a stream on one platform. Pure observation.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS capture (
  id             TEXT PRIMARY KEY,           -- ULID
  stream_id      TEXT NOT NULL REFERENCES stream(id) ON DELETE CASCADE,
  platform       TEXT NOT NULL,              -- YT | TW | ...
  -- Assigned by the platform and never changed. This — not idx — is what
  -- re-identifies a stream whose .md number was renumbered underneath it.
  remote_id      TEXT,
  url            TEXT,
  title          TEXT,                       -- the platform's own title

  -- CLOCKS. A capture carries two of them, and they are different questions:
  --
  --   remote_start_wall  wall time of the platform player's t=0
  --   local_start_wall   wall time of frame 0 of OUR file
  --
  -- They are absolute, not relative, and that is the whole point. YouTube and
  -- Twitch disagree about when the broadcast began, and the recorder disagrees
  -- with both about when it started writing; the only frame all of them share
  -- is wall-clock time. Every position in the archive is therefore a pair —
  -- (which clock, how far in) — and converting between two clocks means going
  -- through wall time. Nothing converts through stream.started_at, so
  -- correcting a stream's start slides the axis and moves nothing else.
  --
  -- local_start_wall is NULL until something measures it. The recorder is the
  -- only thing that can know it exactly; scripts/probe-media.js recovers an
  -- approximation from mtime - duration and records how much to trust it.
  remote_start_wall    INTEGER,
  local_start_wall     INTEGER,
  local_start_precision_s INTEGER,          -- +/- seconds on local_start_wall

  -- Kept in sync by recompute() as (remote_start_wall - stream.started_at):
  -- the capture's position on the archive's own axis. Derived, not authored —
  -- it is not writable through a changeset, the two wall times are.
  broadcast_started_at INTEGER,
  offset_s       INTEGER NOT NULL DEFAULT 0,

  -- ffprobe. All of it observation; re-derivable by re-probing.
  file_duration_s INTEGER,
  container      TEXT,
  video_codec    TEXT,
  audio_codec    TEXT,
  width          INTEGER,
  height         INTEGER,
  fps            REAL,
  has_audio      INTEGER,
  probed_at      INTEGER,

  video_path     TEXT,
  video_bytes    INTEGER,
  video_ok       INTEGER NOT NULL DEFAULT 0, -- from stat(), never from a claim
  chat_path      TEXT,
  chat_ok        INTEGER NOT NULL DEFAULT 0,
  thumb_path     TEXT,

  -- A reupload elsewhere (the Twitch archive channel). Third source, distinct
  -- from both the platform VOD and the local file.
  mirror_url     TEXT,
  mirror_platform TEXT,

  -- Twitch deletes VODs after 60 days. NULL = never checked. When this goes
  -- false the archive can say the only thing that really matters: this exists
  -- here and nowhere else now.
  alive          INTEGER,
  checked_at     INTEGER,
  verified_at    INTEGER,                    -- last stat() of the local files

  created_at     INTEGER NOT NULL,
  updated_at     INTEGER NOT NULL,
  UNIQUE(stream_id, platform)
);
CREATE INDEX IF NOT EXISTS ix_capture_stream ON capture(stream_id);
-- Not unique: the source data contains real duplicates (two entries sharing one
-- video id). A unique constraint would reject the archive's actual contents.
CREATE INDEX IF NOT EXISTS ix_capture_remote ON capture(platform, remote_id)
  WHERE remote_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_capture_dead ON capture(alive) WHERE alive = 0;

-- ---------------------------------------------------------------------------
-- note — markers and prose on a stream.
--
-- Hangs off the STREAM, not the capture: a zero-capture placeholder is exactly
-- the row you most want to write on. The link to a capture is the anchor, which
-- says what the offset is measured against.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS note (
  id            TEXT PRIMARY KEY,            -- ULID
  stream_id     TEXT NOT NULL REFERENCES stream(id) ON DELETE CASCADE,

  -- WHICH CLOCK the offset was measured against. Three answers, and the third
  -- is the one a boolean cannot hold:
  --
  --   capture  measured inside anchor_id, on its anchor_clock. Exact.
  --   stream   measured against the archive's own axis. Exact.
  --   unknown  imported from the vault, read off some VOD nobody recorded.
  --            NEVER converted into a player position as if it were exact.
  --
  -- Every vault note is 'unknown'. Conflating that with 'stream' is how a
  -- timestamp silently shifts, which is the one thing this schema will not do.
  frame         TEXT NOT NULL DEFAULT 'unknown',
  anchor_id     TEXT REFERENCES capture(id) ON DELETE SET NULL,
  anchor_clock  TEXT,                        -- remote | local, when frame=capture
  offset_s      INTEGER,                     -- NULL = a note with no timestamp
  -- How wrong the offset may be, in seconds. The vault's timestamps are good
  -- to about two minutes; a note dropped against a VOD is good to one. A pin
  -- drawn 1px wide is a claim of ~15s accuracy, so the renderer needs to know.
  offset_precision_s INTEGER,
  offset_approx INTEGER NOT NULL DEFAULT 0,  -- the source said "ish"

  -- The timestamp expression exactly as a human typed it, when it says more
  -- than one number:
  --
  --   (02:16:26 - 03:18:27)   a span
  --   (00:16:17; 00:16:58)    two separate moments
  --
  -- `offset_s` holds the FIRST point and that is the marker — a note is a pin,
  -- never an interval. The rest is kept verbatim rather than interpreted,
  -- because '-' and ';' mean different things and no single second column can
  -- hold both honestly. NULL for a plain single stamp, where offset_s already
  -- says everything: the editable line is rebuilt from it.
  stamp         TEXT,

  tag           TEXT,
  seq           INTEGER,                     -- the 01 in `#short 01`
  text          TEXT NOT NULL,
  done          INTEGER NOT NULL DEFAULT 0,  -- the vault's [x], read-only here
  ord           INTEGER NOT NULL DEFAULT 0,

  origin        TEXT NOT NULL DEFAULT 'vault',   -- vault | user
  author_id     TEXT REFERENCES person(id) ON DELETE SET NULL,
  raw           TEXT,                        -- verbatim source line, if any
  retracted_at  INTEGER,
  created_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_note_stream ON note(stream_id, offset_s)
  WHERE retracted_at IS NULL;
CREATE INDEX IF NOT EXISTS ix_note_tag ON note(tag) WHERE tag IS NOT NULL;

-- ---------------------------------------------------------------------------
-- segment — the editorial spine of a broadcast: what was happening, when.
--
-- The one thing on the timeline drawn as an interval. Notes are markers, even
-- the ones whose source line carried a range; a segment is the block.
--
-- Deliberately isomorphic to `note` in everything about time — same frame,
-- same anchor, same clock, same tombstone — because that arithmetic is the
-- part that breaks, and it should look identical in both places.
--
-- `kind` is a closed vocabulary because it is a COLOUR, and a colour has to
-- mean the same thing in every stream in the archive. Nothing infers it: not
-- the title, not the tags, not a keyword list. It is the SAME four words a tag
-- is filed under —
--
--   game    a specific title being played
--   person  a guest, a member, a character the block is about
--   type    what kind of stream this stretch is: collab, karaoke, zatsudan,
--           superchat reading, a creative or event block
--   meta    the scaffolding around the content: intro, outro, break, waiting
--
-- — because "what is this block" and "what is this tag" are the same question
-- asked twice, and two parallel vocabularies drift within a week. A new
-- segment is 'unknown' until a human says otherwise, and 'unknown' is NOT
-- 'meta': "nobody has labelled this" and "this is a break" are different
-- claims, and only one of them tells a viewer to skip.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS segment (
  id            TEXT PRIMARY KEY,            -- ULID
  stream_id     TEXT NOT NULL REFERENCES stream(id) ON DELETE CASCADE,

  frame         TEXT NOT NULL DEFAULT 'stream',
  anchor_id     TEXT REFERENCES capture(id) ON DELETE SET NULL,
  anchor_clock  TEXT,
  start_s       INTEGER NOT NULL,
  end_s         INTEGER,                     -- NULL = runs to the next segment

  -- Which row of the strip. 0 is what she played; 1 is what happened inside
  -- that — "No Caller ID", "Chapter VI of story". Two lanes, and the ENUM
  -- refuses a third: a strip with a row nobody fills is worse than not having
  -- it, and anything finer belongs in the lane-1 label.
  --
  -- A LANE, NOT A parent_id, and that was the decision. A parent link would
  -- buy one thing — a guaranteed answer to "inside what" — and every edge case
  -- it has comes from having to keep that answer true: a containment rule, a
  -- cascade policy when the parent is retracted, and a child that turns
  -- invalid the moment someone nudges the parent's boundary by four minutes.
  -- Lanes buy the same picture for a GROUP BY. Nothing cascades: deleting the
  -- Nikke block does not un-happen No Caller ID. And an inner block may cross
  -- an outer boundary — a sponsored read that starts inside the game and ends
  -- after she has moved on — which containment would have refused outright.
  -- "Inside what" is answered where it is needed, at projection time, by
  -- looking at which lane-0 block covers the start. See projectSegments.
  lane          INTEGER NOT NULL DEFAULT 0,

  kind          TEXT NOT NULL DEFAULT 'unknown',   -- game|person|type|meta|unknown
  -- What this block is about. NULL is fine — a waiting screen is not about
  -- anything. When set, the strip renders `label ?? tag.name`, so you can write
  -- "Mario Kart (200cc)" on one block and still link the entity.
  tag_id        TEXT REFERENCES tag(id) ON DELETE SET NULL,
  label         TEXT,                        -- overrides the tag's name

  origin        TEXT NOT NULL DEFAULT 'user',
  author_id     TEXT REFERENCES person(id) ON DELETE SET NULL,
  retracted_at  INTEGER,
  created_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_segment_stream ON segment(stream_id, start_s)
  WHERE retracted_at IS NULL;
-- "Every block of Mario Kart in the archive" — the payoff for linking the
-- entity rather than retyping its name.
CREATE INDEX IF NOT EXISTS ix_segment_tag ON segment(tag_id, start_s)
  WHERE tag_id IS NOT NULL AND retracted_at IS NULL;
-- No index on `lane`. It is filtered in memory alongside the axis conversion
-- that already has to happen per row, and a stream has tens of segments rather
-- than thousands — ix_segment_stream is still the one that matters.

-- ---------------------------------------------------------------------------
-- Anchors are load-bearing: a note measured inside a capture becomes a note
-- measured against nothing if that capture disappears, and ON DELETE SET NULL
-- would do exactly that — silently, since a NULL anchor used to MEAN
-- "broadcast-relative". These triggers make the delete fail instead. The
-- changeset applier converts dependents to frame='stream' first, in the same
-- transaction, recording each conversion as its own change row; by the time
-- the DELETE runs there is nothing left pointing here.
-- ---------------------------------------------------------------------------

CREATE TRIGGER IF NOT EXISTS capture_anchor_guard_note
BEFORE DELETE ON capture
WHEN EXISTS (SELECT 1 FROM note WHERE anchor_id = OLD.id AND retracted_at IS NULL)
BEGIN
  SELECT RAISE(ABORT, 'capture still anchors notes — convert them first');
END;

CREATE TRIGGER IF NOT EXISTS capture_anchor_guard_segment
BEFORE DELETE ON capture
WHEN EXISTS (SELECT 1 FROM segment WHERE anchor_id = OLD.id AND retracted_at IS NULL)
BEGIN
  SELECT RAISE(ABORT, 'capture still anchors segments — convert them first');
END;

-- ---------------------------------------------------------------------------
-- tags — a join table, so spellings stay canonical and filtering is an index
-- lookup. `kind` separates "Mario Kart 8" (a game) from "zatsudan" (a type of
-- stream) from "Intro" (scaffolding) without needing three tables.
--
-- Tags are entered by hand. Nothing derives one from a title: the importer did,
-- and the result was ten separate rows for one game because the title carried
-- an episode number.
-- ---------------------------------------------------------------------------

-- A tag is a THING the archive knows about — a game, a format, a recurring
-- bit — not a label stuck on a stream. It has art, it can be the name of a
-- block on the timeline, and it can be the thing a stream is about. Those are
-- the same entity seen from three places, so it is one row.
--
-- Typing a name nobody has used before mints one, and from then on it
-- autocompletes for everyone. That is the whole point, and it is also why
-- `status` exists: an autocomplete list anyone can write to fills with
-- near-duplicates otherwise. A suggester's new tag stays 'proposed' — usable on
-- their own suggestion, invisible in autocomplete — until an editor confirms it.
CREATE TABLE IF NOT EXISTS tag (
  id         TEXT PRIMARY KEY,               -- ULID
  name       TEXT NOT NULL UNIQUE,           -- canonical, display-cased
  slug       TEXT NOT NULL UNIQUE,           -- lowercased, derived from name
  -- The SAME vocabulary as segment.kind, and the same colours. A tag names a
  -- thing; a segment says that thing was happening between here and here. If
  -- the two lists could disagree, the chip and the block would be different
  -- colours for one fact.
  -- media|character|type|elements|meta|general|unknown. Which SURFACES a kind
  -- may be offered on is KIND_SURFACES in archive.js, not a column: `meta` and
  -- `general` describe a clip, `elements` only ever labels a stretch of a
  -- broadcast, and the rest are shared. A row is never hidden by that map —
  -- it governs the pickers, not the data.
  kind       TEXT NOT NULL DEFAULT 'unknown',

  -- Episode variants roll up. The vault import made ten separate rows for
  -- CLAIR OBSCUR: EXPEDITION 33 (#2 … #11), which means ?tag= returns one
  -- stream in ten and there are ten places to hang the same box art. A child
  -- inherits art and kind from its parent unless it overrides them.
  parent_id  TEXT REFERENCES tag(id) ON DELETE SET NULL,

  thumb_path TEXT,                           -- box art, under the media root
  summary    TEXT,

  -- Where the description and the art were harvested from, and whether they
  -- are still untouched. `seeded` is one bit for the row: cleared by the
  -- applier on any human edit to name, summary or thumb_path, set again by a
  -- re-seed. See the block in db.js.
  seed_url   TEXT,
  seeded     INTEGER,

  -- A tag with a gate RESTRICTS whatever carries it: a viewer must hold the
  -- grant named here to see the clip or the stream. Free text, so a new gate is
  -- another flag rather than another column, and it lives on the vocabulary
  -- rather than on the row so marking one tag `restricted` gates everything
  -- filed under it at once.
  gate       TEXT,

  -- There was a `seg_kind` here once: a second vocabulary, parallel to `kind`,
  -- for the colour a tag suggests on the timeline. It is gone. One tag, one
  -- category, one colour. `kind` is still COPIED onto a segment when a human
  -- picks the tag rather than read through at render time, so re-filing a game
  -- as a type does not silently repaint forty old streams — but what gets
  -- copied is `kind` itself, not a shadow of it.

  status     TEXT NOT NULL DEFAULT 'confirmed',   -- proposed | confirmed
  origin     TEXT NOT NULL DEFAULT 'vault',       -- vault | user
  author_id  TEXT REFERENCES person(id) ON DELETE SET NULL,
  retracted_at INTEGER,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_tag_parent ON tag(parent_id) WHERE parent_id IS NOT NULL;
-- The autocomplete's own query: confirmed tags, in slug order, tombstones out.
CREATE INDEX IF NOT EXISTS ix_tag_live ON tag(status, slug) WHERE retracted_at IS NULL;
-- Gated tags are a handful out of hundreds, so the partial index IS the list —
-- and the list is what every visibility check starts from.
CREATE INDEX IF NOT EXISTS tag_gate ON tag(gate) WHERE gate IS NOT NULL;

-- The junction carries a surrogate id for one reason: `change.target_id` points
-- at a single value, so a composite primary key cannot be addressed by a
-- changeset. Without an id here, attaching a tag to a stream is unreachable
-- through the only write path the archive has.
CREATE TABLE IF NOT EXISTS stream_tag (
  id        TEXT PRIMARY KEY,                -- ULID
  stream_id TEXT NOT NULL REFERENCES stream(id) ON DELETE CASCADE,
  tag_id    TEXT NOT NULL REFERENCES tag(id) ON DELETE CASCADE,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(stream_id, tag_id)
);
CREATE INDEX IF NOT EXISTS ix_stream_tag_rev ON stream_tag(tag_id, stream_id);

-- ---------------------------------------------------------------------------
-- changeset / change — the pull request, and the only way a decision changes.
--
-- One shape forever: "this field of this thing becomes this value".
--   update  one row: field + value
--   create  several rows sharing a client-minted target_id, applied together
--   delete  one row, no field
--
-- No JSON payloads and no second shape, so "insert this missing stream with its
-- two captures" is twelve rows in one changeset — applied in one transaction,
-- or not at all. A reviewer can never leave half a stream behind.
--
-- Applied changesets ARE the audit log. There is no separate history table to
-- keep in agreement with reality.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS changeset (
  id          TEXT PRIMARY KEY,              -- ULID
  author_id   TEXT REFERENCES person(id) ON DELETE SET NULL,
  reason      TEXT,
  status      TEXT NOT NULL DEFAULT 'open',  -- open|applied|rejected|superseded
  created_at  INTEGER NOT NULL,
  reviewed_by TEXT REFERENCES person(id) ON DELETE SET NULL,
  reviewed_at INTEGER,
  review_note TEXT
);
CREATE INDEX IF NOT EXISTS ix_changeset_queue ON changeset(status, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_changeset_author ON changeset(author_id, created_at DESC);

CREATE TABLE IF NOT EXISTS change (
  id           TEXT PRIMARY KEY,             -- ULID
  changeset_id TEXT NOT NULL REFERENCES changeset(id) ON DELETE CASCADE,
  seq          INTEGER NOT NULL,             -- apply order within the changeset
  target_type  TEXT NOT NULL,                -- stream | capture | note | tag
  target_id    TEXT NOT NULL,                -- existing id, or a client-minted
                                             -- ULID when op = create
  op           TEXT NOT NULL,                -- create | update | delete
  field        TEXT,                         -- NULL when op = delete
  value        TEXT,                         -- NULL means "clear this field"
  -- What the field held when this was written. If it no longer matches at
  -- review time, someone else got there first and the reviewer is told rather
  -- than silently discarding their work.
  base_value   TEXT,
  UNIQUE(changeset_id, seq)
);
CREATE INDEX IF NOT EXISTS ix_change_target ON change(target_type, target_id);

-- ---------------------------------------------------------------------------
-- event — what happened, in order, in the words a person would use.
--
-- Deliberately NOT more changesets. A changeset answers "what field went from
-- what to what, and can it be undone"; this answers "who did what". They
-- overlap and are not the same question — approving is both, editing a
-- transcript is only the second, and a changeset that has not been applied yet
-- is only the first.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS event (
  id           TEXT PRIMARY KEY,              -- ULID
  at           INTEGER NOT NULL,
  actor_id     TEXT REFERENCES person(id) ON DELETE SET NULL,
  -- Denormalised on purpose. A log that said "admin kyabatsu removed this" and
  -- then rendered as "removed by (deleted user)" a year later has lost the part
  -- worth keeping — and the role is the role AT THE TIME, which is a fact about
  -- the event rather than about the person, and would otherwise quietly rewrite
  -- itself every time somebody was promoted.
  actor_handle TEXT,
  actor_role   TEXT,
  verb         TEXT NOT NULL,
  target_type  TEXT NOT NULL,
  target_id    TEXT NOT NULL,
  -- JSON, because the interesting part differs per verb: a submission carries a
  -- title and its tags, a rename carries two strings, a purge carries what was
  -- destroyed. Read-only, rendered, never queried on.
  detail       TEXT,
  changeset_id TEXT REFERENCES changeset(id) ON DELETE SET NULL
);
-- The two questions the log is ever asked, and they want different orders: one
-- snippet's story, and the archive's.
CREATE INDEX IF NOT EXISTS ix_event_target ON event(target_id, at DESC);
CREATE INDEX IF NOT EXISTS ix_event_at ON event(at DESC);

-- ---------------------------------------------------------------------------
-- search
-- ---------------------------------------------------------------------------

CREATE VIRTUAL TABLE IF NOT EXISTS note_fts USING fts5(
  text, tag,
  content='note', content_rowid='rowid',
  tokenize = "porter unicode61 remove_diacritics 2",
  prefix = '2 3'
);

CREATE VIRTUAL TABLE IF NOT EXISTS stream_fts USING fts5(
  title, summary,
  content='stream', content_rowid='rowid',
  tokenize = "porter unicode61 remove_diacritics 2",
  prefix = '2 3'
);

CREATE TRIGGER IF NOT EXISTS note_ai AFTER INSERT ON note BEGIN
  INSERT INTO note_fts(rowid, text, tag) VALUES (new.rowid, new.text, new.tag);
END;
CREATE TRIGGER IF NOT EXISTS note_ad AFTER DELETE ON note BEGIN
  INSERT INTO note_fts(note_fts, rowid, text, tag) VALUES('delete', old.rowid, old.text, old.tag);
END;
CREATE TRIGGER IF NOT EXISTS note_au AFTER UPDATE ON note BEGIN
  INSERT INTO note_fts(note_fts, rowid, text, tag) VALUES('delete', old.rowid, old.text, old.tag);
  INSERT INTO note_fts(rowid, text, tag) VALUES (new.rowid, new.text, new.tag);
END;

CREATE TRIGGER IF NOT EXISTS stream_ai AFTER INSERT ON stream BEGIN
  INSERT INTO stream_fts(rowid, title, summary) VALUES (new.rowid, new.title, new.summary);
END;
CREATE TRIGGER IF NOT EXISTS stream_ad AFTER DELETE ON stream BEGIN
  INSERT INTO stream_fts(stream_fts, rowid, title, summary) VALUES('delete', old.rowid, old.title, old.summary);
END;
CREATE TRIGGER IF NOT EXISTS stream_au AFTER UPDATE ON stream BEGIN
  INSERT INTO stream_fts(stream_fts, rowid, title, summary) VALUES('delete', old.rowid, old.title, old.summary);
  INSERT INTO stream_fts(rowid, title, summary) VALUES (new.rowid, new.title, new.summary);
END;

-- ---------------------------------------------------------------------------
-- snippets
--
-- A short standalone clip on the NAS beside the raws. Deliberately not a
-- capture and not a segment: a snippet has no broadcast, no platform and no
-- second clock, so its offsets are its own file's seconds and nothing else.
-- Its own table keeps every case in the axis machinery — covers_s,
-- anchor_clock, positionToAxis — from having to grow a branch for the one
-- kind of media that never had a clock to be wrong about.
--
-- source_stream_id is a link, not a dependency. Most of these were cut long
-- before this archive existed and will never resolve to a stream row.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS snippet (
  id            TEXT PRIMARY KEY,           -- ULID
  slug          TEXT NOT NULL UNIQUE,       -- the filename stem, the importer's key
  title         TEXT NOT NULL,
  summary       TEXT,                       -- human blurb; editable, unlike the transcript
  video_path    TEXT NOT NULL,              -- relative to TENMA_MEDIA_ROOT
  poster_path   TEXT,                       -- first frame, written by the importer
  duration_s    REAL,
  width         INTEGER,
  height        INTEGER,
  bytes         INTEGER,

  source_stream_id TEXT REFERENCES stream(id) ON DELETE SET NULL,
  source_offset_s  INTEGER,

  -- When the clip landed, as distinct from when this row was made. created_at
  -- is the row's own age and a bulk import stamps every row with the same
  -- second, so ordering on it sorts by whatever order readdir returned —
  -- alphabetical — and buries the newest clip at the bottom. Taken from the
  -- file's mtime, the only record of when a clip was added that lives outside
  -- this database.
  added_at      INTEGER,

  -- What the file actually CONTAINS. The extension is a claim and ffprobe's
  -- format_name cannot settle it either -- it reports "matroska,webm" for a
  -- real WebM and for an ordinary Matroska alike, because WebM *is* Matroska
  -- with a restricted codec list. Only the codecs decide whether a browser
  -- will play it, so they are recorded rather than re-derived. servedType()
  -- in archive.js turns these three into a Content-Type.
  container     TEXT,
  video_codec   TEXT,
  audio_codec   TEXT,
  -- A web-playable rewrap of an unplayable original, relative to the CACHE
  -- root -- never the media root, which is read-only and holds the masters.
  -- NULL is the normal case and means "serve video_path directly".
  play_path     TEXT,
  -- Content hash. Written by the audit rather than by the importer: hashing the
  -- whole collection costs minutes and answers a question the import does not
  -- ask. Finds the same clip filed twice under two names, and is what a future
  -- upload checks to know it already holds this file.
  sha256        TEXT,
  -- The shape of the sound: 480 amplitude buckets, base64'd, about 640 bytes.
  -- Only ever set for a clip with no picture, where it IS the picture — a
  -- waveform the page can draw, fill as it plays, and accept a click on.
  --
  -- A column rather than a file with a route: it is smaller than the request
  -- headers needed to fetch it separately, and it rides along with the row the
  -- page already asked for, so the player has it before it needs it.
  waveform      TEXT,

  -- Where the file is RIGHT NOW, relative to the quarantine root, while it
  -- waits for a human. `video_path` is the destination it will have once an
  -- editor approves and the Pi renames it into the media tree — so the row
  -- knows both its current and its eventual home, and promote is "clear this
  -- column", not "rewrite the path everything else reads".
  --
  -- NULL for every clip the importer wrote, which is what makes it safe:
  -- `quarantine_path IS NOT NULL` is exactly the set of files living outside
  -- the served tree.
  quarantine_path TEXT,

  -- Where a clip came from, when it came from a link rather than a file. Kept
  -- even after the bytes arrive: it is the only record of what was fetched, it
  -- is what stops the same link being submitted twice, and it is what an editor
  -- looks at when a clip turns out to be somebody's reupload.
  source_url    TEXT,

  -- How the fetch is going, mirroring normalize_status and transcript_status
  -- because it answers the same shape of question about the same row.
  --   none     nothing to fetch — every uploaded and every imported clip
  --   queued   a fetch job exists and the recorder has not taken it
  --   running  the recorder holds it
  --   done     the bytes landed in quarantine; normalize takes over
  --   failed   fetch_note says why, and the row has no bytes at all
  -- A row can sit at `queued` for as long as the recorder is off, which is the
  -- whole reason this is a column and not an inference: the page has to be able
  -- to say "waiting for the recorder" rather than draw a player over a file
  -- that does not exist yet.
  fetch_status  TEXT NOT NULL DEFAULT 'none',
  fetch_note    TEXT,

  -- How far the conversion has got, mirroring transcript_status deliberately:
  -- the two answer the same shape of question about the same row, and a reader
  -- who has understood one has understood the other.
  --   none     nothing to do, or nothing has asked yet. Every imported clip.
  --   queued   a normalize job exists and no worker has taken it
  --   running  a worker holds it — the state the UI calls "converting"
  --   done     play_path and poster_path are what the worker left
  --   failed   normalize_note says why; the clip still plays if it ever could
  -- `none` and not `queued` as the default, because fifteen hundred imported
  -- rows were normalized years before this column existed and marking them all
  -- as waiting would invent a backlog.
  normalize_status TEXT NOT NULL DEFAULT 'none',
  normalize_note   TEXT,

  -- Names a submitter typed that are not in the vocabulary. A JSON array of raw
  -- strings — deliberately NOT tag rows.
  --
  -- A proposed tag in the tag table would be autocompleted, which means the
  -- second person to want "Selen Tatsuki" gets it offered to them before
  -- anybody has agreed it should exist, and a typo becomes permanent vocabulary
  -- the moment a second clip picks it up. Kept as loose text, these stay
  -- attached to the clip that suggested them and go nowhere else until an
  -- editor mints the real tag. Somebody suggesting the same name on a second
  -- upload types it again, which is the intended cost.
  --
  -- The column keeps its old name, like `snippet_taglet` does, so history rows
  -- that address it by field name can still resolve their own past.
  taglet_suggestions TEXT,

  -- The flat join of snippet_line.text. Denormalised because FTS5 indexes one
  -- column on one row, and a child table would need its own index and its own
  -- join on every search. Derived — rewritten whenever the lines are.
  transcript    TEXT,
  -- none    nobody has run one
  -- auto    a machine produced it
  -- empty   a machine ran and heard no speech — a real answer, not a gap
  -- failed  a machine ran and could not; transcript_note says why
  -- edited  a human has been through it
  transcript_status TEXT NOT NULL DEFAULT 'none',
  -- The ASR run's report on itself. Kept because at a thousand clips the
  -- question is never "is there a transcript" but "which ones need running
  -- again, and why" — and because `WHERE transcript_model = 'large-v3'` is the
  -- list to re-run when a better model lands.
  transcript_model  TEXT,
  transcript_at     INTEGER,
  transcript_note   TEXT,

  status        TEXT NOT NULL DEFAULT 'confirmed',
  origin        TEXT NOT NULL DEFAULT 'vault',
  author_id     TEXT REFERENCES person(id) ON DELETE SET NULL,
  retracted_at  INTEGER,
  created_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL
);
-- The list's actual sort, so it is an index scan rather than a sort of the
-- whole table. id breaks ties, and does so deterministically — several clips
-- copied in one go share an mtime to the second.
CREATE INDEX IF NOT EXISTS ix_snippet_live ON snippet(added_at DESC, id DESC)
  WHERE retracted_at IS NULL;
-- The same-link check runs on every submission, and it is the only query that
-- reads this column.
CREATE INDEX IF NOT EXISTS ix_snippet_source_url ON snippet(source_url)
  WHERE source_url IS NOT NULL;
-- Partial, because the answer is almost always "none of them": suggestions
-- exist on clips between upload and review, and the queue that reads this wants
-- exactly that handful out of the whole archive.
CREATE INDEX IF NOT EXISTS ix_snippet_suggestions
  ON snippet(id) WHERE taglet_suggestions IS NOT NULL;

-- One row per transcript line, carrying the timing WhisperX already produced.
-- Not tombstoned and not changeset-managed: a transcript is replaced whole by
-- its next pass, and thirty rows of "who edited line 14" is noise rather than
-- history. The human-editable prose is snippet.summary.
CREATE TABLE IF NOT EXISTS snippet_line (
  id         TEXT PRIMARY KEY,
  snippet_id TEXT NOT NULL REFERENCES snippet(id) ON DELETE CASCADE,
  seq        INTEGER NOT NULL,
  start_s    REAL NOT NULL,
  end_s      REAL,
  speaker    TEXT,                          -- nullable; for diarization later
  text       TEXT NOT NULL,
  UNIQUE(snippet_id, seq)
);
CREATE INDEX IF NOT EXISTS ix_snippet_line ON snippet_line(snippet_id, seq);

-- ---------------------------------------------------------------------------
-- the snippet half of the vocabulary
--
-- This used to argue for two tables, and the argument was: `tag` is shared with
-- segment.kind where every entry has to mean a colour on a timeline, so
-- merging would force every snippet kind to also be a legal chapter kind.
--
-- That was the right objection and the wrong conclusion. The answer is
-- KIND_SURFACES in archive.js: one vocabulary, and a map saying which surfaces
-- each kind is OFFERED on. `meta` and `general` are snippet-only and never
-- reach the strip; `elements` is timeline-only; `media` and `character` are
-- shared, which they always were in fact — `copyright` and `character` named
-- the same subjects `game` and `person` named in the other table. Two rows for
-- one person is what actually cost something: a rename had to happen twice and
-- could diverge, and it did.
--
-- The shape is still Danbooru's, because that is the shape that survives a
-- thousand short clips: a namespace, a machine slug, a display name.
--   media       what it belongs to  violet    shared
--   character   who is in it        blue      shared
--   meta        what it IS          yellow    snippets only
--   general     everything else     grey      snippets only
-- ---------------------------------------------------------------------------

-- `taglet` was here: a second vocabulary for snippets, with its own kinds
-- (character, copyright, meta, general), its own autocomplete and its own
-- proposal flow. It held the same subjects `tag` held — `copyright` and
-- `character` were `game` and `person` under other names — so renaming a
-- person meant editing two rows in two tables that could silently diverge, and
-- a slug could exist on both sides meaning one thing.
--
-- Merged into `tag`. What was genuinely snippet-only survived as two kinds
-- rather than as a table; `gate` came the other way and is why a stream can be
-- gated at all. The rebuild that moved the rows is in db.js, keyed on
-- snippet_taglet gaining `tag_id`, so it runs exactly once.

-- The snippet half of the same vocabulary. Still its own junction rather than
-- one shared with stream_tag: "this clip is about that" and "this broadcast was
-- about that" are different relationships and collapsing them would make every
-- query ask which kind of row it had. The TABLE keeps its old name so the
-- history, which addresses change rows by target_type, can still resolve its
-- own past.
CREATE TABLE IF NOT EXISTS snippet_taglet (
  id         TEXT PRIMARY KEY,
  snippet_id TEXT NOT NULL REFERENCES snippet(id) ON DELETE CASCADE,
  tag_id     TEXT NOT NULL REFERENCES tag(id) ON DELETE CASCADE,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(snippet_id, tag_id)
);
-- Both directions: "what is on this snippet" renders every row, and "what is
-- tagged X" is the whole point of the filter.
CREATE INDEX IF NOT EXISTS ix_sniptag_snip ON snippet_taglet(snippet_id);
CREATE INDEX IF NOT EXISTS ix_sniptag_tag ON snippet_taglet(tag_id);

CREATE VIRTUAL TABLE IF NOT EXISTS snippet_fts USING fts5(
  title, summary, transcript,
  content='snippet', content_rowid='rowid',
  tokenize = "porter unicode61 remove_diacritics 2",
  prefix = '2 3'
);

CREATE TRIGGER IF NOT EXISTS snippet_ai AFTER INSERT ON snippet BEGIN
  INSERT INTO snippet_fts(rowid, title, summary, transcript)
    VALUES (new.rowid, new.title, new.summary, new.transcript);
END;
CREATE TRIGGER IF NOT EXISTS snippet_ad AFTER DELETE ON snippet BEGIN
  INSERT INTO snippet_fts(snippet_fts, rowid, title, summary, transcript)
    VALUES('delete', old.rowid, old.title, old.summary, old.transcript);
END;
CREATE TRIGGER IF NOT EXISTS snippet_au AFTER UPDATE ON snippet BEGIN
  INSERT INTO snippet_fts(snippet_fts, rowid, title, summary, transcript)
    VALUES('delete', old.rowid, old.title, old.summary, old.transcript);
  INSERT INTO snippet_fts(rowid, title, summary, transcript)
    VALUES (new.rowid, new.title, new.summary, new.transcript);
END;

-- ---------------------------------------------------------------------------
-- job — the work queue, and the reason the archive opens no outbound socket.
--
-- The archive never fetches, never writes to the media tree and never deletes
-- from it. It publishes INTENT here and ls-rec subscribes: the Pi asks for work
-- on a tick it already runs. See `File management` in review.md.
--
-- The one rule the shape enforces: a job names an ID and a VERB. `url` is the
-- single exception and it is inert here — the host allowlist lives in ls-rec's
-- config, so a compromised archive still cannot make the recorder fetch from an
-- attacker's host. Nothing in this table is a path the worker is told to trust;
-- roots and filenames come from the worker's own config.
--
-- status
--   proposed  somebody asked for it; no worker will see it
--   approved  an editor said yes — this is what the poll claims
--   claimed   a worker holds a lease; another poll skips it until it lapses
--   done      finished, result_path recorded
--   failed    terminal, and `error` says why. NOT re-queued automatically: a
--             job that fails ffmpeg and returns to the queue is an infinite
--             loop with a 74-second period. An editor re-approves.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS job (
  id           TEXT PRIMARY KEY,              -- ULID
  kind         TEXT NOT NULL,                 -- fetch | promote | purge
  status       TEXT NOT NULL DEFAULT 'proposed',
  snippet_id   TEXT REFERENCES snippet(id) ON DELETE CASCADE,
  url          TEXT,
  payload      TEXT,                          -- JSON; kind-specific, never a path
  requested_by TEXT REFERENCES person(id) ON DELETE SET NULL,
  approved_by  TEXT REFERENCES person(id) ON DELETE SET NULL,
  claimed_by   TEXT,                          -- the worker's own name for itself
  claimed_at   INTEGER,
  attempts     INTEGER NOT NULL DEFAULT 0,
  result_path  TEXT,
  error        TEXT,
  created_at   INTEGER NOT NULL,
  updated_at   INTEGER NOT NULL,
  finished_at  INTEGER
);
-- The claim query's index: status first because it is the selective one — a
-- queue is nearly all `done`.
CREATE INDEX IF NOT EXISTS ix_job_claim ON job(status, kind, created_at);
CREATE INDEX IF NOT EXISTS ix_job_snippet ON job(snippet_id)
  WHERE snippet_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- music — somebody else's music video, kept because it will not always be
-- there.
--
-- Phase Connect songs, two to three minutes each, filed under the SAME `tag`
-- vocabulary as everything else: `character` for who is singing, `media` for
-- what it belongs to. There is no second list — a singer is the same row here
-- as she is on a stream, which is the whole point of having one vocabulary.
--
-- IT IS NEVER SERVED FROM THIS ARCHIVE. Viewing is always the YouTube embed,
-- and the downloaded file exists only so that the thing survives the video
-- being taken down. That inverts a snippet's relationship to its bytes: a
-- snippet with no playable file is broken, and a music row with no file is
-- merely un-preserved.
--
-- Which is why the columns a snippet needs are deliberately absent here, and
-- should stay absent: no play_path, no normalize_status, no waveform, no
-- container or codec columns, no transcript. Nothing rewraps this file,
-- nothing probes it, nothing range-requests it, and no route resolves it for
-- a browser. Adding a player later means adding all of that back, so the
-- decision to have one is a decision to make this a different kind of object.
--
-- TWO INDEPENDENT PROGRESS COLUMNS, and they answer different questions:
--   status        the editorial verdict — proposed | confirmed | rejected
--   probe_status  how far the metadata lookup got
--   fetch_status  how far the download got, which only starts after approval
-- Collapsing them is how "waiting for a human" and "waiting for the recorder"
-- become the same unreadable spinner.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS music (
  id            TEXT PRIMARY KEY,           -- ULID
  -- THE natural key, and the reason duplicate submission is answerable at all:
  -- youtu.be/X, watch?v=X and watch?v=X&t=30 are one video and one row.
  video_id      TEXT NOT NULL UNIQUE,
  url           TEXT NOT NULL,              -- canonical watch URL, as remembered

  -- What the probe found. All NULL until it lands, which is what the pending
  -- card reads to say "waiting for the recorder" rather than drawing an empty
  -- title over a video nobody has looked at yet.
  title         TEXT,
  channel       TEXT,
  channel_id    TEXT,                       -- survives a channel rename
  uploaded_at   INTEGER,                    -- the VIDEO's publish date, not the row's
  duration_s    INTEGER,

  -- Preservation, and nothing else. NULL until an approval has been fetched.
  -- Relative to the media root, under `music/`.
  video_path    TEXT,
  thumb_path    TEXT,
  bytes         INTEGER,

  --   queued   a probe job exists and the recorder has not taken it
  --   running  the recorder holds it
  --   done     the facts above are filled in
  --   failed   probe_note says why; the row still has its link and its tags
  probe_status  TEXT NOT NULL DEFAULT 'queued',
  probe_note    TEXT,
  --   none     not approved yet, so nothing has been asked for
  --   queued   approved; the recorder has not taken it
  --   running  downloading
  --   done     video_path is what the recorder left
  --   failed   fetch_note says why. The entry is still published — it is the
  --            PRESERVATION that failed, and the embed still plays.
  fetch_status  TEXT NOT NULL DEFAULT 'none',
  fetch_note    TEXT,

  status        TEXT NOT NULL DEFAULT 'proposed',  -- proposed|confirmed|rejected
  note          TEXT,                       -- the submitter's own words, if any

  origin        TEXT NOT NULL DEFAULT 'link',
  author_id     TEXT REFERENCES person(id) ON DELETE SET NULL,
  -- Tombstone. Retracting is the reversible verb every role above viewer can
  -- reach; destroying the row is a separate admin act.
  retracted_at  INTEGER,
  created_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL
);
-- The list's own sort.
CREATE INDEX IF NOT EXISTS ix_music_live ON music(status, created_at DESC)
  WHERE retracted_at IS NULL;
-- The pending quota counts a person's own unreviewed submissions, and that
-- count runs on every submit.
CREATE INDEX IF NOT EXISTS ix_music_author ON music(author_id, status)
  WHERE retracted_at IS NULL;

-- The third junction onto the one vocabulary, alongside stream_tag and
-- snippet_taglet. It carries a surrogate id for the same reason they do:
-- `change.target_id` holds ONE value, so a composite key cannot be addressed
-- by a changeset — and attaching a tag has to be a decision with an author
-- like any other.
CREATE TABLE IF NOT EXISTS music_tag (
  id         TEXT PRIMARY KEY,
  music_id   TEXT NOT NULL REFERENCES music(id) ON DELETE CASCADE,
  tag_id     TEXT NOT NULL REFERENCES tag(id) ON DELETE CASCADE,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  UNIQUE(music_id, tag_id)
);
-- Both directions: what is on this song, and what is tagged with her.
CREATE INDEX IF NOT EXISTS ix_musictag_music ON music_tag(music_id);
CREATE INDEX IF NOT EXISTS ix_musictag_tag ON music_tag(tag_id);
