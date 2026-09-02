// The whole HTTP surface, plus listen(). `node server.js`.
//
// Read endpoints are anonymous, ETag-cached and open a read-only connection.
// The write path is a single endpoint — POST /api/changesets — because a
// decision only ever changes through an applied changeset. There is no PATCH.

import express from 'express';
import { createHash } from 'node:crypto';
import { existsSync, readdirSync, statSync } from 'node:fs';
import { dirname, extname, join, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));

import {
  bumpGeneration, create, meta, now, open, resolveDbPath, tx, ulid,
} from './db.js';
import {
  ChangeError, KINDS, SEGMENT_KINDS, apply, axisToPosition, buildTimeline, clocksOf,
  deepLink, hms, projectNote, propose, recompute, reject, resolveMedia,
  servedType, sourcesFor, stale, summary, thumbFor, watchSources,
} from './archive.js';
import {
  ANON, COOKIE, ROLES, TTL, atLeast, capabilities, identify, issueSession,
  requireRole, revokeSession, sessionToken, upsertPerson,
} from './auth.js';

// ---------------------------------------------------------------------------
// config
// ---------------------------------------------------------------------------

// Read once, at import. Exported so a caller — a test, an embedder — can spread
// it and override one field, rather than having to reconstruct the whole thing
// or mutate process.env after the fact and wonder why nothing changed.
export const CONFIG = {
  db: process.env.TENMA_DB ?? 'data/archive.db',
  port: Number(process.env.PORT ?? 8000),
  host: process.env.TENMA_HOST ?? '127.0.0.1',
  // Where the archive's `raws/...` paths hang off. Unset means no filesystem
  // checks at all, and every capture reads 'unverified' rather than being
  // wrongly called lost.
  mediaRoot: process.env.TENMA_MEDIA_ROOT || null,
  /* Derived files — snippet posters today, whatever else gets generated later.
     A SEPARATE root, and writable, because TENMA_MEDIA_ROOT is mounted
     read-only on purpose: 21 TB of irreplaceable recordings should not be
     reachable for writing by a process that only ever needs to read them.
     Everything under here is regenerable from the media, so it wants no
     backup and losing it costs one import pass. */
  cacheRoot: process.env.TENMA_CACHE_ROOT || null,
  /* Received uploads, before anyone has looked at them. Separate from
     cacheRoot on purpose: /cache holds derived JPEGs that regenerate in about
     a hundred seconds, which is what makes it boring. This holds the only copy
     of something a person just handed you — different durability, different
     quota, different sweep. Unset disables uploads rather than falling back. */
  quarantineRoot: process.env.TENMA_QUARANTINE_ROOT || null,
  cors: (process.env.TENMA_CORS ?? '*').split(',').map((s) => s.trim()).filter(Boolean),
  defaultRole: process.env.TENMA_DEFAULT_ROLE ?? 'suggester',
  // Enables POST /api/auth/token, which mints a session for any handle with no
  // verification whatsoever. Local development only.
  devAuth: ['1', 'true', 'yes'].includes((process.env.TENMA_DEV_AUTH ?? '').toLowerCase()),
  // Unset disables ingest outright rather than leaving it open — an
  // unauthenticated writer on the recording path is not a sane default.
  ingestToken: process.env.TENMA_INGEST_TOKEN ?? '',
  pairWindow: Number(process.env.TENMA_PAIR_WINDOW_S ?? 600),
  pageMax: Number(process.env.TENMA_PAGE_MAX ?? 100),
};

// ---------------------------------------------------------------------------
// connections
//
// One writer, many readers. That is not a limitation imposed here — SQLite has
// exactly one writer anyway, and in WAL a reader sees a consistent snapshot
// without ever blocking it.
// ---------------------------------------------------------------------------

export function makeApp(config = CONFIG) {
  create(config.db).close();                 // schema + migrations, then let go
  const R = open(config.db, { readonly: true });
  const W = open(config.db);

  const app = express();
  app.use(express.json({ limit: '1mb' }));
  app.disable('x-powered-by');

  app.use((req, res, next) => {
    const origin = req.headers.origin;
    if (config.cors.includes('*')) res.set('Access-Control-Allow-Origin', '*');
    else if (origin && config.cors.includes(origin)) {
      res.set('Access-Control-Allow-Origin', origin);
      res.set('Access-Control-Allow-Credentials', 'true');
      res.set('Vary', 'Origin');
    }
    res.set('Access-Control-Allow-Headers', 'content-type, authorization');
    res.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    if (req.method === 'OPTIONS') return res.sendStatus(204);
    // Identity is resolved once per request and hung off req; every route and
    // guard reads the same answer.
    req.person = identify(R, req);
    next();
  });

  // -------------------------------------------------------------------------
  // caching
  // -------------------------------------------------------------------------

  const generation = () => meta(R, 'generation', '0');

  // Hashed, never interpolated: putting a raw `q=メトロイド` into a header throws
  // (headers are latin-1), and a raw CRLF would be response splitting.
  const etagFor = (...parts) =>
    `W/"g${generation()}:${createHash('blake2s256')
      .update(parts.map(String).join('\x1f')).digest('hex').slice(0, 24)}"`;

  /* `personal` is not a nicety. `public` lets a shared cache reuse the body
     for max-age REGARDLESS of the ETag, so the moment a response varies per
     person — one reader's own pending upload — `public` hands it to whoever
     asks next. The anonymous response is byte-identical to what it always was
     and stays publicly cacheable, which is where the traffic is; only signed-in
     responses go private, and they carry Vary: Cookie so an intermediary that
     ignores the directive still keys on the session. */
  function fresh(req, res, etag, { personal = false } = {}) {
    res.set('ETag', etag);
    res.set('Cache-Control', personal
      ? 'private, max-age=15, stale-while-revalidate=120'
      : 'public, max-age=15, stale-while-revalidate=120');
    if (personal) res.set('Vary', 'Cookie');
    return req.headers['if-none-match'] === etag;
  }

  // -------------------------------------------------------------------------
  // projections
  // -------------------------------------------------------------------------

  const COLS = `s.id, s.idx, s.title, s.summary, s.started_at, s.tz_offset_min,
    s.duration_s, s.vod_state, s.chat_state, s.serve_pref, s.thumb_path,
    s.retracted_at, s.retracted_why, s.merged_into, s.origin,
    s.chat_path, s.chat_sources, s.chat_ok,
    s.local_date, s.local_month, s.start_sod, s.updated_at, s.timeline_json`;

  // Captures and tags folded into the row as JSON by SQLite — one query per
  // endpoint instead of one per card.
  const CAPS = `(SELECT json_group_array(json_object(
      'id', c.id, 'platform', c.platform, 'remote_id', c.remote_id, 'url', c.url,
      'title', c.title, 'broadcast_started_at', c.broadcast_started_at,
      'offset_s', c.offset_s, 'file_duration_s', c.file_duration_s,
      'remote_start_wall', c.remote_start_wall, 'local_start_wall', c.local_start_wall,
      'local_start_precision_s', c.local_start_precision_s,
      'container', c.container, 'video_codec', c.video_codec,
      'audio_codec', c.audio_codec, 'width', c.width, 'height', c.height,
      'fps', c.fps, 'probed_at', c.probed_at,
      'video_path', c.video_path, 'video_ok', c.video_ok, 'chat_path', c.chat_path,
      'chat_ok', c.chat_ok, 'thumb_path', c.thumb_path, 'mirror_url', c.mirror_url,
      'mirror_platform', c.mirror_platform, 'alive', c.alive))
    FROM capture c WHERE c.stream_id = s.id) AS caps`;

  // A tag is an entity now, so a stream carries enough of it to render the
  // chip without a second round trip: its art, its status, and the id of the
  // junction row — which is what a changeset needs in order to detach it.
  const TAGS = `(SELECT json_group_array(json_object(
      'id', t.id, 'name', t.name, 'slug', t.slug, 'kind', t.kind,
      'thumb', t.thumb_path, 'status', t.status, 'parent_id', t.parent_id,
      'link_id', st.id))
    FROM stream_tag st JOIN tag t ON t.id = st.tag_id
    WHERE st.stream_id = s.id AND t.retracted_at IS NULL) AS tags`;

  function streamOut(row, notes) {
    const caps = JSON.parse(row.caps ?? '[]');
    const watch = watchSources(caps, row.serve_pref);
    const sources = sourcesFor(caps, row);
    // Deep links are built against whichever source leads the watch chain, so
    // a note's timestamp is clickable with no media pipeline at all.
    //
    // Resolved by capture_id, never by remote_id. ix_capture_remote is
    // deliberately non-unique — three duplicate video ids exist in the archive —
    // and the only reason matching on it has never picked the wrong row is that
    // UNIQUE(stream_id, platform) keeps the duplicates on separate streams.
    // Giving a trimmed archive-channel mirror its own capture row means
    // relaxing that constraint, and on that day remote_id stops identifying a
    // capture. Resolving by id costs nothing and does not depend on it.
    const lead = watch.find((w) => w.embeddable);
    const byId = new Map(caps.map((c) => [c.id, c]));
    const primary = (lead && byId.get(lead.capture_id)) ?? caps[0] ?? null;
    const t = thumbFor(row.thumb_path, caps);
    const timeline = row.timeline_json ? JSON.parse(row.timeline_json) : null;

    const out = {
      id: row.id,
      idx: row.idx,
      title: row.title,
      summary: row.summary,
      started_at: row.started_at,
      tz_offset_min: row.tz_offset_min,
      date: row.local_date,
      month: row.local_month,
      start_sod: row.start_sod,
      duration_s: row.duration_s,
      duration: hms(row.duration_s),
      vod_state: row.vod_state,
      chat_state: row.chat_state,
      // The raw columns as well as the `chat` block below, because the record
      // editor round-trips a field by reading and writing the same name. The
      // block is the presentation view; these two are what a changeset names.
      chat_path: row.chat_path,
      chat_sources: row.chat_sources,
      thumb: t.url,
      thumb_source: t.source,
      // The stored override, separate from the resolved url. The editor needs
      // to know whether there IS one — `thumb` is filled by the fallback chain
      // whether or not anybody chose it.
      thumb_path: row.thumb_path ?? null,
      serve_pref: row.serve_pref ?? null,
      // `axis` is what the timeline is drawn against, and it is display only —
      // nothing converts through it. Each source carries its own start_wall,
      // which is the one number the client needs to move the playhead between
      // clocks. See the clocks block in archive.js.
      axis: {
        zero_wall: row.started_at,
        domain_s: row.duration_s ?? null,
        duration_source: timeline?.duration_source ?? null,
        tiled: timeline?.tiled ?? false,
      },
      sources,
      lead: lead?.capture_id ?? null,
      watch,                                   // the older shape, unchanged
      tags: JSON.parse(row.tags ?? '[]'),
      origin: row.origin,
      captures: caps.map((c) => ({
        id: c.id, platform: c.platform, remote_id: c.remote_id, url: c.url,
        title: c.title, offset_s: c.offset_s, file_duration_s: c.file_duration_s,
        remote_start_wall: c.remote_start_wall ?? null,
        local_start_wall: c.local_start_wall ?? null,
        container: c.container ?? null, video_codec: c.video_codec ?? null,
        audio_codec: c.audio_codec ?? null,
        width: c.width ?? null, height: c.height ?? null,
        probed_at: c.probed_at ?? null,
        video_ok: !!c.video_ok, chat_ok: !!c.chat_ok,
        mirror_url: c.mirror_url, alive: c.alive,
      })),
      chat: {
        // Honest about which kind of "no chat" this is. A file that exists but
        // has never been imported is not the same claim as a file that never
        // existed, and the panel should say which.
        state: row.chat_state,
        imported: false,
        // The merged file, once ls-audit has built one. This is what a player
        // should load: every platform's messages in one origin-tagged file.
        merged: row.chat_path ?? null,
        merged_ok: row.chat_path ? !!row.chat_ok : null,
        // Which platforms are inside it. Read from the stream once merged,
        // because by then the raws are in deep storage and the captures no
        // longer carry a path to count. Before that, the captures are the
        // answer and still verifiable — `file_ok` is only meaningful there.
        sources: row.chat_path
          ? (row.chat_sources ?? '').split(',').filter(Boolean)
            .map((platform) => ({ capture_id: null, platform, file_ok: null,
                                  imported_at: null, messages: null }))
          : caps.filter((c) => c.chat_path)
            .map((c) => ({ capture_id: c.id, platform: c.platform,
                           file_ok: !!c.chat_ok, imported_at: null, messages: null })),
      },
    };
    if (row.retracted_at) {
      out.retracted = { at: row.retracted_at, why: row.retracted_why,
                        merged_into: row.merged_into };
    }
    if (notes) {
      out.notes = notes.map((n) => projectNote(n, byId, row.started_at, primary));
    }
    return out;
  }

  function notesFor(ids) {
    if (!ids.length) return new Map();
    const qs = ids.map(() => '?').join(',');
    const rows = R.prepare(
      `SELECT n.*, p.handle AS author FROM note n
       LEFT JOIN person p ON p.id = n.author_id
       WHERE n.stream_id IN (${qs}) AND n.retracted_at IS NULL
       ORDER BY n.stream_id, (n.offset_s IS NULL), n.offset_s, n.ord`).all(...ids);
    const out = new Map();
    for (const r of rows) {
      if (!out.has(r.stream_id)) out.set(r.stream_id, []);
      out.get(r.stream_id).push(r);
    }
    return out;
  }

  /** User input -> a safe FTS5 MATCH, prefix-matching the last token so search
   *  feels live as you type. */
  function ftsQuery(q) {
    const toks = (q.match(/[\p{L}\p{N}_#]+/gu) ?? [])
      .map((t) => t.replace(/#/g, '')).filter(Boolean);
    if (!toks.length) return '';
    return toks.map((t, i) => (i === toks.length - 1 ? `"${t}"*` : `"${t}"`)).join(' AND ');
  }

  // -------------------------------------------------------------------------
  // read
  // -------------------------------------------------------------------------

  app.get('/api/streams', (req, res) => {
    const { q = '', tag, month, state, before, before_id,
            after, after_id, include = '' } = req.query;
    const limit = Math.min(Math.max(Number(req.query.limit ?? 30) || 30, 1), config.pageMax);
    const wantNotes = String(include).split(',').includes('notes');

    const etag = etagFor('list', q, tag, month, state, before, before_id,
                         after, after_id, limit, wantNotes);
    if (fresh(req, res, etag)) return res.status(304).end();

    const where = ['s.retracted_at IS NULL'];
    const params = [];

    if (q) {
      const match = ftsQuery(String(q));
      if (!match) return res.json({ streams: [], next: null, count: 0,
                                    generation: Number(generation()) });
      // Union the two indexes on id first, then join. A note hit and a title
      // hit are equally valid ways to find a stream.
      // `AND n.retracted_at IS NULL` matters: the FTS triggers fire on UPDATE
      // and a tombstone IS an update, so a retracted note stays in the index
      // with its text. Without this, a note somebody deliberately withdrew
      // still made its stream findable by the words in it.
      where.push(`s.id IN (
        SELECT n.stream_id FROM note n
         WHERE n.retracted_at IS NULL
           AND n.rowid IN (SELECT rowid FROM note_fts WHERE note_fts MATCH ?)
        UNION
        SELECT s2.id FROM stream s2
         WHERE s2.rowid IN (SELECT rowid FROM stream_fts WHERE stream_fts MATCH ?))`);
      params.push(match, match);
    }
    if (tag) {
      // Matches the tag OR anything rolled up under it, so asking for
      // `clair-obscur-expedition-33` returns all ten episodes rather than the
      // one row that happens to carry the bare name.
      /* Bound twice through anonymous `?` rather than once through `?1`. Every
         other clause here uses anonymous parameters, and SQLite numbers those
         as "one past the highest index seen so far" — so a single ?1 anywhere
         in the statement collides with whatever the first anonymous ? already
         claimed. With `q` also set its two FTS parameters take indices 1 and 2,
         ?1 then resolves to the FTS match string instead of the slug, and the
         extra bound value overflows the statement: SQLITE_ERROR, column index
         out of range. Tag-alone worked, which is why this survived — the client
         never sent both until the tag library started filtering. */
      where.push(`EXISTS (
        SELECT 1 FROM stream_tag st JOIN tag t ON t.id = st.tag_id
         WHERE st.stream_id = s.id
           AND (t.slug = ? OR t.parent_id = (SELECT id FROM tag WHERE slug = ?)))`);
      const slug = String(tag).toLowerCase();
      params.push(slug, slug);
    }
    if (month) {
      if (!/^\d{4}-(0[1-9]|1[0-2])$/.test(String(month))) {
        return res.status(400).json({ error: 'month must be YYYY-MM' });
      }
      where.push('s.local_month = ?');
      params.push(String(month));
    }
    if (state) { where.push('s.vod_state = ?'); params.push(String(state)); }
    // Keyset, never OFFSET: the cursor cannot skip or duplicate a row when the
    // archive changes mid-scroll. `after` is the same discipline reversed —
    // the theater's neighbour rail scrolls towards newer streams as readily as
    // older ones, and one-directional paging cannot serve it.
    const forward = after !== undefined;
    if (before !== undefined) {
      if (before_id) { where.push('(s.started_at, s.id) < (?, ?)'); params.push(Number(before), String(before_id)); }
      else { where.push('s.started_at < ?'); params.push(Number(before)); }
    }
    if (forward) {
      if (after_id) { where.push('(s.started_at, s.id) > (?, ?)'); params.push(Number(after), String(after_id)); }
      else { where.push('s.started_at > ?'); params.push(Number(after)); }
    }

    const dir = forward ? 'ASC' : 'DESC';
    const rows = R.prepare(
      `SELECT ${COLS}, ${CAPS}, ${TAGS} FROM stream s WHERE ${where.join(' AND ')}
       ORDER BY s.started_at ${dir}, s.id ${dir} LIMIT ?`).all(...params, limit + 1);

    const more = rows.length > limit;
    // Ascending is a query-direction detail, not a result order: the caller
    // always gets newest first, whichever way it paged.
    const page = rows.slice(0, limit);
    const ordered = forward ? [...page].reverse() : page;
    const edge = page[page.length - 1];
    const notes = wantNotes ? notesFor(page.map((r) => r.id)) : null;
    res.json({
      streams: ordered.map((r) => streamOut(r, notes ? (notes.get(r.id) ?? []) : null)),
      next: more && edge
        ? (forward ? { after: edge.started_at, after_id: edge.id }
                   : { before: edge.started_at, before_id: edge.id })
        : null,
      count: page.length,
      generation: Number(generation()),
    });
  });

  /** The rail: a window of streams either side of this one, in archive order.
   *
   *  Ordered by started_at, NOT by idx. idx runs 472-698 across 221 rows with a
   *  hole in it and one NULL — it is a label, and the schema says so. Ordering
   *  the rail by it would make the rail and the prev/next arrows disagree. */
  const NB_COLS = `s.id, s.idx, s.title, s.started_at, s.local_date,
    s.duration_s, s.vod_state, s.thumb_path,
    (SELECT c.remote_id FROM capture c WHERE c.stream_id = s.id
       AND c.platform = 'YT' AND c.remote_id IS NOT NULL LIMIT 1) AS yt,
    (SELECT c.thumb_path FROM capture c WHERE c.stream_id = s.id
       AND c.thumb_path IS NOT NULL LIMIT 1) AS cap_thumb`;

  const nbOut = (r) => ({
    id: r.id, idx: r.idx, title: r.title, started_at: r.started_at,
    date: r.local_date, duration: hms(r.duration_s), vod_state: r.vod_state,
    thumb: r.thumb_path ? `/media/thumb/${r.thumb_path}`
         : r.cap_thumb ? `/media/thumb/${r.cap_thumb}`
         : r.yt ? `https://i.ytimg.com/vi/${r.yt}/hqdefault.jpg` : null,
  });

  function neighbours(row, span) {
    const older = R.prepare(
      `SELECT ${NB_COLS} FROM stream s WHERE s.retracted_at IS NULL
         AND (s.started_at, s.id) < (?, ?)
       ORDER BY s.started_at DESC, s.id DESC LIMIT ?`).all(row.started_at, row.id, span);
    const newer = R.prepare(
      `SELECT ${NB_COLS} FROM stream s WHERE s.retracted_at IS NULL
         AND (s.started_at, s.id) > (?, ?)
       ORDER BY s.started_at ASC, s.id ASC LIMIT ?`).all(row.started_at, row.id, span);
    // Both returned oldest-first. That is a data contract, not a layout one —
    // the rail renders newest-leftmost and reverses these itself.
    return { older: older.reverse().map(nbOut), newer: newer.map(nbOut) };
  }

  /** Does this changeset touch this stream?
   *
   *  Two ways, and the second is easy to miss. A change can TARGET something
   *  that belongs to the stream — the stream itself, one of its notes,
   *  captures, segments or tag links. Or, when the changeset is still open, it
   *  can be proposing to CREATE one of those, in which case the row does not
   *  exist yet and the only trace is the stream's id sitting in a `stream_id`
   *  field value. Without the second clause a queued suggestion is invisible:
   *  no badge, no history entry, nothing anywhere for the person who wrote it.
   */
  const TOUCHES_STREAM = `(
     ch.target_id = ?1
     OR ch.target_id IN (
       SELECT id FROM note       WHERE stream_id = ?1
       UNION SELECT id FROM capture    WHERE stream_id = ?1
       UNION SELECT id FROM segment    WHERE stream_id = ?1
       UNION SELECT id FROM stream_tag WHERE stream_id = ?1)
     OR (ch.field = 'stream_id' AND ch.value = ?1))`;

  function oneStream(res, whereSql, value, req = null) {
    const row = R.prepare(
      `SELECT ${COLS}, ${CAPS}, ${TAGS} FROM stream s WHERE ${whereSql}`).get(value);
    if (!row) return res.status(404).json({ error: 'no such stream' });
    const out = streamOut(row, notesFor([row.id]).get(row.id) ?? []);

    // The timeline is projected once by recompute() and read back here, so the
    // strip, the pins and the player are looking at the same numbers rather
    // than three call sites arriving at them separately. A row imported before
    // the column existed is projected on the spot rather than served empty.
    const timeline = row.timeline_json
      ? JSON.parse(row.timeline_json) : buildTimeline(R, row.id);
    out.segments = timeline.segments;
    out.coverage = timeline.coverage;
    out.counts = timeline.counts;
    out.segment_kinds = SEGMENT_KINDS;   // includes the 'unknown' sentinel
    out.kinds = KINDS;                   // the four a human may choose from

    const span = Math.min(Math.max(Number(req?.query?.rail ?? 4) || 4, 0), 12);
    if (span) out.neighbours = neighbours(row, span);

    const nb = R.prepare(
      `SELECT (SELECT id FROM stream WHERE retracted_at IS NULL
                AND (started_at, id) < (?, ?)
                ORDER BY started_at DESC, id DESC LIMIT 1) AS prev,
              (SELECT id FROM stream WHERE retracted_at IS NULL
                AND (started_at, id) > (?, ?)
                ORDER BY started_at ASC, id ASC LIMIT 1) AS next`)
      .get(row.started_at, row.id, row.started_at, row.id);
    out.prev_id = nb.prev;
    out.next_id = nb.next;
    out.open_changesets = R.prepare(
      `SELECT COUNT(DISTINCT cs.id) c FROM changeset cs
       JOIN change ch ON ch.changeset_id = cs.id
       WHERE cs.status = 'open' AND ${TOUCHES_STREAM}`).get(row.id).c;
    return res.json(out);
  }

  // Registered before /api/streams/:id so 'idx' is never read as an id.
  app.get('/api/streams/idx/:idx', (req, res) => {
    const etag = etagFor('idx', req.params.idx, req.query.rail);
    if (fresh(req, res, etag)) return res.status(304).end();
    return oneStream(res, 's.idx = ?', Number(req.params.idx), req);
  });

  app.get('/api/streams/:id/history', (req, res) => {
    // Public on purpose: an archive that hides its edits is worth less.
    const rows = R.prepare(
      `SELECT DISTINCT cs.id, cs.reason, cs.status, cs.created_at, cs.reviewed_at,
              a.handle AS author, r.handle AS reviewer
       FROM changeset cs JOIN change c ON c.changeset_id = cs.id
       LEFT JOIN person a ON a.id = cs.author_id
       LEFT JOIN person r ON r.id = cs.reviewed_by
       WHERE ${TOUCHES_STREAM.replaceAll('ch.', 'c.')}
       ORDER BY cs.created_at DESC, cs.id DESC LIMIT 200`)
      .all(req.params.id);
    res.json({
      history: rows.map((r) => ({
        ...r,
        changes: R.prepare(
          `SELECT target_type, target_id, op, field, value, base_value
           FROM change WHERE changeset_id = ? ORDER BY seq`).all(r.id),
      })),
    });
  });

  app.get('/api/streams/:id', (req, res) => {
    const etag = etagFor('s', req.params.id, req.query.rail);
    if (fresh(req, res, etag)) return res.status(304).end();
    return oneStream(res, 's.id = ?', req.params.id, req);
  });

  /** The vocabulary. `q=` makes it the autocomplete behind "type a game name":
   *  prefix-first, so typing "mario" ranks Mario Kart above Super Mario. */
  app.get('/api/tags', (req, res) => {
    const q = String(req.query.q ?? '').trim().toLowerCase();
    const limit = Math.min(Math.max(Number(req.query.limit ?? 500) || 500, 1), 500);
    // Proposed tags are hidden from the picker by default — that is the whole
    // point of the status — but an editor reviewing the queue needs to see them.
    const wantProposed = String(req.query.status ?? '') === 'all'
      && atLeast(req.person, 'editor');

    const where = ['t.retracted_at IS NULL'];
    const params = [];
    if (!wantProposed) where.push(`t.status = 'confirmed'`);
    if (q) {
      where.push('(t.slug LIKE ? OR lower(t.name) LIKE ?)');
      params.push(`%${q}%`, `%${q}%`);
    }
    const rows = R.prepare(
      `SELECT t.id, t.slug, t.name, t.kind, t.thumb_path, t.summary,
              t.status, t.parent_id, p.name AS parent_name, p.slug AS parent_slug,
              COUNT(DISTINCT st.stream_id) n,
              (SELECT COUNT(*) FROM segment g
                WHERE g.tag_id = t.id AND g.retracted_at IS NULL) blocks
       FROM tag t
       LEFT JOIN stream_tag st ON st.tag_id = t.id
       LEFT JOIN tag p ON p.id = t.parent_id
       WHERE ${where.join(' AND ')}
       GROUP BY t.id
       ORDER BY ${q ? `(CASE WHEN t.slug LIKE ? THEN 0 ELSE 1 END),` : ''} n DESC, t.name
       LIMIT ?`).all(...params, ...(q ? [`${q}%`] : []), limit);

    res.json({ tags: rows.map((t) => ({
      id: t.id, slug: t.slug, name: t.name, kind: t.kind,
      // A child inherits its parent's art rather than needing its own copy —
      // the reason parent_id exists at all.
      thumb: t.thumb_path ? `/media/thumb/${t.thumb_path}` : null,
      summary: t.summary, status: t.status,
      parent: t.parent_id ? { id: t.parent_id, name: t.parent_name, slug: t.parent_slug } : null,
      streams: t.n, blocks: t.blocks,
    })) });
  });

  app.get('/api/months', (req, res) => {
    res.json({ months: R.prepare(
      `SELECT local_month AS month, COUNT(*) streams,
              SUM(COALESCE(duration_s, 0)) seconds FROM stream
       WHERE retracted_at IS NULL GROUP BY local_month ORDER BY month DESC`).all() });
  });

  app.get('/api/health', (req, res) => {
    const c = (sql) => R.prepare(sql).get().c;
    const states = {};
    for (const r of R.prepare(
      `SELECT vod_state v, COUNT(*) c FROM stream WHERE retracted_at IS NULL
       GROUP BY 1`).all()) states[r.v] = r.c;
    res.json({
      generation: Number(generation()),
      now: now(),
      streams: c('SELECT COUNT(*) c FROM stream WHERE retracted_at IS NULL'),
      retracted: c('SELECT COUNT(*) c FROM stream WHERE retracted_at IS NOT NULL'),
      captures: c('SELECT COUNT(*) c FROM capture'),
      notes: c('SELECT COUNT(*) c FROM note WHERE retracted_at IS NULL'),
      vod_states: states,
      dead_links: c('SELECT COUNT(*) c FROM capture WHERE alive = 0'),
      unchecked_links: c(
        'SELECT COUNT(*) c FROM capture WHERE alive IS NULL AND remote_id IS NOT NULL'),
      open_changesets: c("SELECT COUNT(*) c FROM changeset WHERE status = 'open'"),
      streams_without_idx: c(
        'SELECT COUNT(*) c FROM stream WHERE idx IS NULL AND retracted_at IS NULL'),

      segments: c('SELECT COUNT(*) c FROM segment WHERE retracted_at IS NULL'),

      // A progress bar for the correction effort, not an error count. Every
      // vault note starts here; the number goes down as timestamps get pinned
      // to a real clock, and it going down is the work.
      notes_unknown_frame: c(
        `SELECT COUNT(*) c FROM note WHERE retracted_at IS NULL AND frame = 'unknown'`),

      // Nothing has measured these files yet, so duration cannot derive and
      // vod_state can never reach 'truncated'. scripts/probe-media.js.
      captures_unprobed: c('SELECT COUNT(*) c FROM capture WHERE probed_at IS NULL'),
      captures_no_local_clock: c(
        'SELECT COUNT(*) c FROM capture WHERE local_start_wall IS NULL'),
      streams_without_duration: c(
        'SELECT COUNT(*) c FROM stream WHERE duration_s IS NULL AND retracted_at IS NULL'),

      // Captures sitting outside their own stream's span — a Twitch VOD that
      // starts three hours after the YouTube one ended is not the same
      // broadcast, it is a mispairing. Only visible at all because the clocks
      // are absolute now.
      captures_out_of_span: c(
        `SELECT COUNT(*) c FROM stream s JOIN capture cp ON cp.stream_id = s.id
         WHERE s.retracted_at IS NULL AND (
           (s.duration_s IS NOT NULL AND ABS(cp.offset_s) > s.duration_s)
           OR (s.duration_s IS NULL AND ABS(cp.offset_s) > 3600))`),

      imported_at: meta(R, 'imported_at'),
      media_root: config.mediaRoot ?? null,
    });
  });

  /** The mispairings behind `captures_out_of_span`, so they can be worked
   *  through rather than merely counted. */
  app.get('/api/health/out-of-span', (req, res) => {
    res.json({ captures: R.prepare(
      `SELECT s.id AS stream_id, s.idx, s.title, s.duration_s,
              cp.id AS capture_id, cp.platform, cp.remote_id, cp.offset_s
       FROM stream s JOIN capture cp ON cp.stream_id = s.id
       WHERE s.retracted_at IS NULL AND (
         (s.duration_s IS NOT NULL AND ABS(cp.offset_s) > s.duration_s)
         OR (s.duration_s IS NULL AND ABS(cp.offset_s) > 3600))
       ORDER BY ABS(cp.offset_s) DESC`).all() });
  });

  // -------------------------------------------------------------------------
  // auth — see auth.js; identify() is the seam you replace
  // -------------------------------------------------------------------------

  app.get('/api/auth/me', (req, res) => {
    const p = req.person;
    res.json({ id: p.id, handle: p.handle, role: p.role, provider: p.provider,
               display_name: p.display_name ?? null, avatar_url: p.avatar_url ?? null,
               can: capabilities(p),
               // So the UI knows whether to offer the dev sign-in at all,
               // rather than probing a 404 to find out.
               dev_auth: !!config.devAuth });
  });

  app.post('/api/auth/token', (req, res) => {
    // Local development only: mints a session for any handle with no
    // verification at all. Gated so it cannot be reachable by accident.
    if (!config.devAuth) return res.status(404).json({ error: 'dev auth is disabled' });
    const { handle, role = 'admin' } = req.body ?? {};
    if (!handle) return res.status(400).json({ error: 'handle is required' });
    if (!ROLES.includes(role)) return res.status(400).json({ error: `role must be one of ${ROLES}` });
    const id = upsertPerson(W, { provider: 'dev', providerUid: handle, handle,
                                 displayName: handle, defaultRole: config.defaultRole });
    W.prepare('UPDATE person SET role = ? WHERE id = ?').run(role, id);
    const { token, expiresAt } = issueSession(W, id, 'dev');
    res.cookie?.(COOKIE, token, { httpOnly: true, sameSite: 'lax', maxAge: TTL * 1000 });
    res.set('Set-Cookie', `${COOKIE}=${token}; HttpOnly; SameSite=Lax; Path=/; Max-Age=${TTL}`);
    res.json({ token, id, role, expires_at: expiresAt });
  });

  app.post('/api/auth/logout', (req, res) => {
    revokeSession(W, sessionToken(req));
    res.set('Set-Cookie', `${COOKIE}=; Path=/; Max-Age=0`);
    res.json({ ok: true });
  });

  // -------------------------------------------------------------------------
  // write — one path
  // -------------------------------------------------------------------------

  const changeError = (res, e) => {
    if (e instanceof ChangeError) {
      return res.status(e.status).json(
        typeof e.detail === 'string' ? { error: e.detail } : e.detail);
    }
    throw e;
  };

  app.post('/api/changesets', requireRole('suggester'), (req, res) => {
    // An editor's own applies on submission; everyone else's queues. This is
    // the only way any decision in the archive changes.
    try {
      res.json(propose(W, {
        authorId: req.person.id,
        reason: req.body?.reason ?? null,
        changes: req.body?.changes ?? [],
        autoApply: atLeast(req.person, 'editor'),
        mediaRoot: config.mediaRoot,
      }));
    } catch (e) { return changeError(res, e); }
  });

  app.get('/api/changesets', requireRole('editor'), (req, res) => {
    const { status = 'open', target_id } = req.query;
    const sql = [`SELECT cs.*, p.handle AS author FROM changeset cs
                  LEFT JOIN person p ON p.id = cs.author_id WHERE 1=1`];
    const params = [];
    if (status !== 'all') { sql.push('AND cs.status = ?'); params.push(String(status)); }
    if (target_id) {
      sql.push(`AND EXISTS (SELECT 1 FROM change c WHERE c.changeset_id = cs.id
                            AND c.target_id = ?)`);
      params.push(String(target_id));
    }
    const limit = Math.min(Math.max(Number(req.query.limit ?? 50) || 50, 1), 200);
    // created_at is whole seconds, so it cannot order changesets made in the
    // same second. The ULID breaks the tie by creation time — that is what it
    // is for.
    sql.push('ORDER BY cs.created_at DESC, cs.id DESC LIMIT ?');
    params.push(limit);

    const rows = R.prepare(sql.join(' ')).all(...params).map((r) => {
      const out = { ...r, changes: R.prepare(
        `SELECT seq, target_type, target_id, op, field, value, base_value
         FROM change WHERE changeset_id = ? ORDER BY seq`).all(r.id) };
      if (r.status === 'open') out.conflicts = stale(R, r.id);
      return out;
    });
    res.json({ changesets: rows, count: rows.length });
  });

  app.get('/api/changesets/:id', requireRole('editor'), (req, res) => {
    try { res.json(summary(R, req.params.id)); }
    catch (e) { return changeError(res, e); }
  });

  app.post('/api/changesets/:id/review', requireRole('editor'), (req, res) => {
    const { decision, note = null, force = false } = req.body ?? {};
    if (!['approve', 'reject'].includes(decision)) {
      return res.status(400).json({ error: 'decision must be approve or reject' });
    }
    try {
      res.json(decision === 'approve'
        ? apply(W, req.params.id, { reviewerId: req.person.id, note, force: !!force,
                                    mediaRoot: config.mediaRoot })
        : reject(W, req.params.id, { reviewerId: req.person.id, note }));
    } catch (e) { return changeError(res, e); }
  });

  // -------------------------------------------------------------------------
  // machine ingest — ls-rec posts captures as they happen
  // -------------------------------------------------------------------------

  function requireIngest(req, res, next) {
    if (!config.ingestToken) {
      return res.status(503).json({ error: 'ingest is disabled; set TENMA_INGEST_TOKEN' });
    }
    const auth = req.get('authorization');
    const got = req.get('x-ingest-token')
      ?? (auth?.toLowerCase().startsWith('bearer ') ? auth.slice(7).trim() : null);
    if (got !== config.ingestToken) {
      return res.status(401).json({ error: 'bad or missing ingest token' });
    }
    next();
  }

  const nextIdx = () =>
    (W.prepare('SELECT MAX(idx) m FROM stream').get().m ?? 0) + 1;

  app.get('/api/ingest/next-index', requireIngest, (req, res) => {
    // For the recorder's offline fallback: if the API is unreachable at record
    // time it must record anyway, and this re-syncs its local counter after.
    res.json({ next_index: nextIdx() });
  });

  // Fields a human has actually set, so a re-observing machine can be told to
  // ask first. Only applied changesets count — an open proposal is an opinion,
  // not yet a decision.
  const humanFields = (type, id) => R.prepare(
    `SELECT DISTINCT c.field FROM change c
       JOIN changeset cs ON cs.id = c.changeset_id
      WHERE cs.status = 'applied' AND c.target_type = ? AND c.target_id = ?
        AND c.field IS NOT NULL`).all(type, id).map((r) => r.field);

  const LOOKUP_CAP = ['id', 'platform', 'remote_id', 'url', 'title', 'video_path',
                      'chat_path', 'thumb_path', 'mirror_url', 'file_duration_s',
                      'broadcast_started_at', 'offset_s', 'remote_start_wall',
                      'local_start_wall', 'local_start_precision_s'];
  const LOOKUP_STREAM = ['id', 'idx', 'title', 'started_at', 'tz_offset_min',
                         'duration_s', 'vod_state', 'chat_state', 'origin',
                         'retracted_at', 'chat_path', 'chat_sources'];

  const shape = (row, keys) => Object.fromEntries(keys.map((k) => [k, row?.[k] ?? null]));

  function lookupStream(streamId) {
    const s = R.prepare('SELECT * FROM stream WHERE id = ?').get(streamId);
    if (!s) return null;
    return {
      ...shape(s, LOOKUP_STREAM),
      human_fields: humanFields('stream', s.id),
      captures: R.prepare('SELECT * FROM capture WHERE stream_id = ? ORDER BY platform')
        .all(s.id).map((c) => ({
          ...shape(c, LOOKUP_CAP), human_fields: humanFields('capture', c.id),
        })),
    };
  }

  // ---------------------------------------------------------------------------
  // Read-before-write, for ls-audit.
  //
  // The recorder posts observations blind, which is right for it: it is the
  // only witness and there is nothing to compare against. ls-audit is the
  // opposite — it reconstructs from files, caches and logs long after the fact,
  // and some of what it reconstructs is *worse* than what is already stored. A
  // filename gives a start time to the minute; the recorder measured it to the
  // second. So ls-audit reads first, diffs, and asks a human about anything
  // that collides. This endpoint is the read half.
  //
  // `by` reports how the stream was found, and `conflict` is set when the two
  // identities disagree — a remote_id that resolves to one stream while the
  // vault index points at another. That is two systems having drifted apart,
  // and the answer is to say so, not to pick a winner.
  // ---------------------------------------------------------------------------
  app.get('/api/ingest/lookup', requireIngest, (req, res) => {
    const idx = req.query.idx ? Number(req.query.idx) : null;
    // Repeatable: ?id=YT:abc123&id=TW:456
    const ids = [].concat(req.query.id ?? []).map(String).filter(Boolean);

    let byRemote = null;
    const matched = [];
    for (const pair of ids) {
      const [p, ...rest] = pair.split(':');
      const remote = rest.join(':');
      if (!p || !remote) continue;
      const c = R.prepare(
        'SELECT stream_id FROM capture WHERE platform = ? AND remote_id = ?')
        .get(p.toUpperCase(), remote);
      if (c) {
        matched.push({ id: pair, stream_id: c.stream_id });
        byRemote ??= c.stream_id;
      }
    }

    // Two remote_ids from the same broadcast landing on different streams is
    // itself a drift worth reporting rather than silently using the first.
    const split = matched.filter((m) => m.stream_id !== byRemote);
    const byIdx = idx
      ? (R.prepare('SELECT id FROM stream WHERE idx = ?').get(idx)?.id ?? null)
      : null;

    let conflict = null;
    if (byRemote && byIdx && byRemote !== byIdx) {
      conflict = {
        kind: 'identity',
        detail: `remote_id resolves to stream ${byRemote}, but index ${idx} is stream ${byIdx}`,
        by_remote: lookupStream(byRemote), by_idx: lookupStream(byIdx),
      };
    } else if (split.length) {
      conflict = {
        kind: 'split',
        detail: `these captures are on different streams: ${
          matched.map((m) => `${m.id}→${m.stream_id}`).join(', ')}`,
      };
    }

    const streamId = byRemote ?? byIdx;
    res.json({
      found: Boolean(streamId),
      by: byRemote ? 'remote_id' : (byIdx ? 'idx' : null),
      conflict,
      next_index: nextIdx(),
      stream: streamId ? lookupStream(streamId) : null,
    });
  });

  // Everything a machine may name. An allowlist, checked before anything is
  // written, because the alternative — ignoring what it does not recognise — is
  // how a client bug becomes a silent no-op that nobody notices for months.
  //
  // Notes, summaries, tags and segments are absent and must stay absent. They
  // are the subjective half of the archive, they are what a person came here to
  // write, and no unattended sweep should be able to reach them even by
  // accident. Anything not on this list is a 400.
  const INGEST_TOP = new Set([
    'platform', 'remote_id', 'stream_id', 'index', 'url', 'title',
    'video_path', 'chat_path', 'thumb_path', 'mirror_url', 'duration_s',
    'started_at', 'tz_offset_min', 'broadcast_started_at', 'record_started_at',
    'local_start_precision_s', 'stream', 'clear',
  ]);
  const INGEST_STREAM = new Set([
    'title', 'started_at', 'tz_offset_min', 'chat_path', 'chat_sources',
  ]);
  // Clearing is for paths only. A file can genuinely stop existing, so "this
  // path is no longer true" is an observation. A title or a clock going blank
  // is never an observation, so those can be corrected but never emptied.
  const INGEST_CLEARABLE = new Set([
    'video_path', 'chat_path', 'thumb_path', 'mirror_url',
  ]);

  app.post('/api/ingest/capture', requireIngest, (req, res) => {
    const b = req.body ?? {};
    const platform = String(b.platform ?? '').toUpperCase();
    const remoteId = String(b.remote_id ?? '').trim();
    if (!['YT', 'TW'].includes(platform)) {
      return res.status(400).json({ error: 'platform must be YT or TW' });
    }
    if (!remoteId) {
      return res.status(400).json({ error: 'remote_id is required — it is the identity anchor' });
    }

    const unknown = Object.keys(b).filter((k) => !INGEST_TOP.has(k));
    const unknownStream = Object.keys(b.stream ?? {}).filter((k) => !INGEST_STREAM.has(k));
    if (unknown.length || unknownStream.length) {
      return res.status(400).json({
        error: 'unknown field(s) — ingest writes observations, not opinions',
        fields: [...unknown, ...unknownStream.map((k) => `stream.${k}`)],
      });
    }
    const clear = [].concat(b.clear ?? []).map(String);
    const unclearable = clear.filter((k) => !INGEST_CLEARABLE.has(k));
    if (unclearable.length) {
      return res.status(400).json({
        error: 'only media paths may be cleared', fields: unclearable,
      });
    }

    const t = now();
    // `b.stream` is only consulted for the initial INSERT, and deliberately not
    // folded into `explicitStart` below. The recorder's top-level started_at is
    // the broadcast's start *and* the capture's remote clock, because for it
    // they are one event. A stream-level correction from ls-audit is not: it
    // moves the entry's date without claiming anything about the player's t=0.
    const started = Number(b.started_at ?? b.stream?.started_at ?? t);
    const tz = Number(b.tz_offset_min ?? b.stream?.tz_offset_min ?? 0);
    const title = String(b.title ?? b.stream?.title ?? '').trim()
      || `${platform} ${remoteId}`;
    let streamId, captureId = null, created = false, pairedWith = null;

    // Upsert on (platform, remote_id): retries are free and a crashed daemon
    // can simply re-post.
    const existing = W.prepare(
      'SELECT id, stream_id FROM capture WHERE platform = ? AND remote_id = ?')
      .get(platform, remoteId);

    // An explicit target, for a caller that has already read the archive and
    // resolved the ambiguity itself. ls-audit does exactly that: it looks up,
    // shows a human the diff, and then says which stream it means. Guessing
    // here would only be able to reach a worse answer than the one it was told.
    const wantStream = b.stream_id ? String(b.stream_id) : null;
    if (wantStream && !W.prepare('SELECT 1 FROM stream WHERE id = ?').get(wantStream)) {
      return res.status(404).json({ error: `no stream ${wantStream}` });
    }
    if (wantStream && existing && existing.stream_id !== wantStream) {
      // Moving a capture between streams is a repair, not an observation. It
      // has to go through a changeset so it lands in the history with a reason.
      return res.status(409).json({
        error: 'this capture is already on a different stream',
        capture_id: existing.id, on_stream: existing.stream_id, wanted: wantStream,
      });
    }

    if (existing) {
      streamId = existing.stream_id;
      captureId = existing.id;
    } else if (wantStream) {
      streamId = wantStream;
      pairedWith = W.prepare(
        'SELECT group_concat(platform) p FROM capture WHERE stream_id = ?')
        .get(streamId).p;
    } else {
      // The other half of a broadcast already in flight? Matched against the
      // whole archive rather than one daemon's memory, so a restart between the
      // two halves no longer splits one broadcast into two.
      //
      // created_at DESC is the tiebreak, and it is not decoration: two streams
      // can sit exactly the same distance away, and until this was here SQLite
      // picked between them by whatever the query plan happened to return. The
      // freshest one is the right guess — a dual-platform broadcast pairs with
      // the half that started moments ago, not with a years-old row that
      // happens to land on the same second.
      const mate = W.prepare(
        `SELECT s.id FROM stream s WHERE s.retracted_at IS NULL
           AND ABS(s.started_at - ?) <= ?
           AND NOT EXISTS (SELECT 1 FROM capture c
                           WHERE c.stream_id = s.id AND c.platform = ?)
         ORDER BY ABS(s.started_at - ?) ASC, s.created_at DESC, s.id DESC LIMIT 1`)
        .get(started, config.pairWindow, platform, started);
      if (mate) {
        streamId = mate.id;
        pairedWith = W.prepare(
          'SELECT group_concat(platform) p FROM capture WHERE stream_id = ?')
          .get(streamId).p;
      } else {
        let idx = b.index ? Number(b.index) : nextIdx();
        if (W.prepare('SELECT 1 FROM stream WHERE idx = ?').get(idx)) idx = nextIdx();
        streamId = ulid();
        // origin='ingest': this broadcast is not in the .md and may never be.
        W.prepare(`INSERT INTO stream(id, idx, title, started_at, tz_offset_min,
                     duration_s, vod_state, chat_state, origin, created_at, updated_at)
                   VALUES(?,?,?,?,?,?, 'unverified','unverified','ingest',?,?)`)
          .run(streamId, idx, title, started, tz, b.duration_s ?? null, t, t);
        created = true;
      }
    }

    // Only fields the caller actually sent, so the completion call cannot blank
    // the title the start call set.
    const cols = {};
    const maybe = { url: b.url, title: b.title, video_path: b.video_path,
                    chat_path: b.chat_path, thumb_path: b.thumb_path,
                    mirror_url: b.mirror_url, file_duration_s: b.duration_s };
    for (const [k, v] of Object.entries(maybe)) if (v !== undefined && v !== null) cols[k] = v;
    // ...and then whatever was explicitly named as no longer true. Listed
    // second so `clear` wins over a stale value sent in the same packet.
    for (const k of clear) cols[k] = null;

    // The recorder is the ONLY thing that will ever know when it started
    // writing. Nothing can recover it later except mtime arithmetic, which is a
    // guess. If it sends the value, keep it exactly.
    //
    // Precision travels with the number, and defaults to 1 because the recorder
    // — the only caller that omits it — measures to the second. ls-audit
    // reconstructs, so it says how well: 60 off a filename stamp, 1 off a log
    // line or a chat file. A clock without its precision is a claim without a
    // confidence, and the theater draws them differently on purpose.
    if (b.record_started_at !== undefined && b.record_started_at !== null) {
      cols.local_start_wall = Number(b.record_started_at);
      cols.local_start_precision_s = Number(b.local_start_precision_s ?? 1) || 1;
    }

    // broadcast_started_at is deliberately absent from that list. The completion
    // call sends paths and a duration, usually with no start time — defaulting
    // it to "now" on an update would silently move the broadcast clock by
    // months, breaking platform pairing and every note offset with it. It is
    // written once, at creation, and changed only if explicitly sent.
    const explicitStart = b.broadcast_started_at ?? b.started_at;
    if (explicitStart !== undefined && explicitStart !== null) {
      cols.broadcast_started_at = Number(explicitStart);
      // The same number, in the column that is now the truth: the wall time of
      // the platform player's t=0. This is the one moment it can be recorded
      // first-hand instead of reconstructed.
      cols.remote_start_wall = Number(explicitStart);
    }

    if (captureId === null) {
      const insert = { ...cols };
      // Deliberately NOT defaulted to `started`.
      //
      // `started` falls back to now(), so a recorder that could not read the
      // platform's start time would have had one invented for it — and
      // `remote_start_wall` is the wall clock of the player's t=0, the number
      // every note offset and every source's covers_s is computed through.
      // Inventing it silently is the exact failure this model exists to
      // prevent. Left NULL, clocksOf() falls back to started_at + offset_s,
      // which is a stated approximation rather than a fabricated measurement.
      captureId = ulid();
      const keys = ['id', 'stream_id', 'platform', 'remote_id', ...Object.keys(insert),
                    'created_at', 'updated_at'];
      W.prepare(`INSERT INTO capture(${keys.join(',')}) ` +
                `VALUES(${keys.map(() => '?').join(',')})`)
        .run(captureId, streamId, platform, remoteId, ...Object.values(insert), t, t);
    } else if (Object.keys(cols).length) {
      W.prepare(`UPDATE capture SET ${Object.keys(cols).map((k) => `${k}=?`).join(',')},
                 updated_at=? WHERE id=?`).run(...Object.values(cols), t, captureId);
    }

    // Stream-level corrections, for a caller reconciling a whole entry rather
    // than reporting one capture. Only three fields, and no derived ones:
    // duration_s comes back out of file_duration_s in recompute(), so accepting
    // it here would just be a value waiting to be overwritten.
    //
    // started_at moving is safe to do plainly — correcting a start time changes
    // the start time and nothing else. Notes and chapters keep the offsets they
    // were written with, because a note at 02:30:00 is at 02:30:00 whether the
    // stream is recorded as starting at 09:00 or 09:05.
    const sIn = b.stream ?? {};
    const sCols = {};
    if (sIn.title !== undefined && sIn.title !== null) sCols.title = String(sIn.title);
    if (sIn.started_at !== undefined && sIn.started_at !== null) {
      sCols.started_at = Number(sIn.started_at);
    }
    if (sIn.tz_offset_min !== undefined && sIn.tz_offset_min !== null) {
      sCols.tz_offset_min = Number(sIn.tz_offset_min);
    }
    // The merged chat, and the record of which platforms are inside it. Written
    // together on purpose: the sources list is only meaningful as a description
    // of a particular merged file, and once the raws are in deep storage it is
    // the only thing that still knows the answer.
    if (sIn.chat_path !== undefined && sIn.chat_path !== null) {
      sCols.chat_path = String(sIn.chat_path);
    }
    if (sIn.chat_sources !== undefined && sIn.chat_sources !== null) {
      const list = [...new Set([].concat(sIn.chat_sources).join(',').split(',')
        .map((p) => p.trim().toUpperCase()).filter(Boolean))].sort();
      const bad = list.filter((p) => !['YT', 'TW'].includes(p));
      if (bad.length) {
        return res.status(400).json({ error: 'chat_sources must be YT and/or TW', fields: bad });
      }
      sCols.chat_sources = list.join(',') || null;
    }
    if (Object.keys(sCols).length) {
      W.prepare(`UPDATE stream SET ${Object.keys(sCols).map((k) => `${k}=?`).join(',')},
                 updated_at=? WHERE id=?`).run(...Object.values(sCols), t, streamId);
    }

    const state = recompute(W, streamId, { mediaRoot: config.mediaRoot });
    bumpGeneration(W);
    const idx = W.prepare('SELECT idx FROM stream WHERE id = ?').get(streamId).idx;
    res.json({ id: streamId, index: idx, capture_id: captureId, created,
               paired_with: pairedWith, vod_state: state.vod_state,
               chat_state: state.chat_state });
  });

  // -------------------------------------------------------------------------
  // jobs — what the archive asks the Pi to do
  //
  // The archive publishes intent; ls-rec subscribes. Nothing here opens an
  // outbound socket, parses a remote response, or writes a file. See the block
  // comment on `job` in db.js.
  // -------------------------------------------------------------------------

  const JOB_KINDS = ['fetch', 'promote', 'purge'];
  /* Five minutes. Long enough that a slow fetch is not stolen out from under
     the worker doing it, short enough that a worker killed mid-job frees its
     work before anyone notices. */
  const JOB_LEASE_S = 300;

  const jobRow = (r) => ({
    id: r.id, kind: r.kind, status: r.status,
    snippet_id: r.snippet_id, url: r.url,
    payload: r.payload ? JSON.parse(r.payload) : null,
    attempts: r.attempts, claimed_by: r.claimed_by, claimed_at: r.claimed_at,
    result_path: r.result_path, error: r.error,
    created_at: r.created_at, finished_at: r.finished_at,
  });

  /** Enqueue. Editors only — every kind here spends something that is not
   *  theirs to spend: disk, the recorder's time, or a file. */
  app.post('/api/jobs', requireRole('editor'), (req, res) => {
    const { kind, snippet_id = null, url = null, payload = null } = req.body ?? {};
    if (!JOB_KINDS.includes(kind)) {
      return res.status(400).json({ error: `kind must be one of ${JOB_KINDS}` });
    }
    if ((kind === 'promote' || kind === 'purge') && !snippet_id) {
      return res.status(400).json({ error: `${kind} needs a snippet_id` });
    }
    if (kind === 'fetch' && !url) return res.status(400).json({ error: 'fetch needs a url' });
    if (snippet_id) {
      const s = R.prepare('SELECT 1 FROM snippet WHERE id = ?').get(snippet_id);
      if (!s) return res.status(404).json({ error: 'no such snippet' });
    }
    /* Approved on creation because the creator is already an editor and the
       approval IS the editor's yes. The `proposed` state exists for the day a
       suggester can ask for one; nothing writes it yet. */
    const t = now(), id = ulid();
    W.prepare(
      `INSERT INTO job(id, kind, status, snippet_id, url, payload,
                       requested_by, approved_by, created_at, updated_at)
       VALUES(?,?,'approved',?,?,?,?,?,?,?)`)
      .run(id, kind, snippet_id, url ? String(url) : null,
           payload ? JSON.stringify(payload) : null,
           req.person?.id ?? null, req.person?.id ?? null, t, t);
    res.status(201).json({ job: jobRow(R.prepare('SELECT * FROM job WHERE id = ?').get(id)) });
  });

  /** The queue, for a human. */
  app.get('/api/jobs', requireRole('editor'), (req, res) => {
    const status = req.query.status ? String(req.query.status) : null;
    const limit = Math.min(Math.max(Number(req.query.limit ?? 50) || 50, 1), config.pageMax);
    const rows = status
      ? R.prepare('SELECT * FROM job WHERE status = ? ORDER BY created_at DESC LIMIT ?')
        .all(status, limit)
      : R.prepare('SELECT * FROM job ORDER BY created_at DESC LIMIT ?').all(limit);
    res.json({ jobs: rows.map(jobRow) });
  });

  /* POST and not GET, though it reads like a fetch: claiming TAKES A LEASE,
     which is a write, and a GET that writes is both wrong and cacheable. */
  app.post('/api/ingest/jobs/claim', requireIngest, (req, res) => {
    const worker = String(req.body?.worker ?? 'unknown').slice(0, 64);
    const want = Math.min(Math.max(Number(req.body?.limit ?? 1) || 1, 1), 10);
    const kinds = [].concat(req.body?.kinds ?? JOB_KINDS)
      .map(String).filter((k) => JOB_KINDS.includes(k));
    if (!kinds.length) {
      return res.status(400).json({ error: `kinds must be some of ${JOB_KINDS}` });
    }
    const t = now();
    try {
      /* One transaction, so two polls arriving together cannot both claim the
         same row. A lapsed lease is re-claimable — a worker that died holding
         one must not park its job forever — and `attempts` is what makes that
         visible rather than silent. */
      const rows = tx(W, () => {
        const found = W.prepare(
          `SELECT * FROM job
            WHERE kind IN (${kinds.map(() => '?').join(',')})
              AND (status = 'approved'
                   OR (status = 'claimed' AND (claimed_at IS NULL OR claimed_at < ?)))
            ORDER BY created_at LIMIT ?`).all(...kinds, t - JOB_LEASE_S, want);
        const mark = W.prepare(
          `UPDATE job SET status = 'claimed', claimed_by = ?, claimed_at = ?,
                          attempts = attempts + 1, updated_at = ?
            WHERE id = ?`);
        for (const r of found) mark.run(worker, t, t, r.id);
        return found;
      });
      if (rows.length) bumpGeneration(W);
      res.json({ jobs: rows.map((r) => jobRow({ ...r, status: 'claimed',
                                                claimed_by: worker, claimed_at: t,
                                                attempts: r.attempts + 1 })),
                 lease_s: JOB_LEASE_S });
    } catch (e) { return changeError(res, e); }
  });

  /** How a worker says what happened. `failed` is terminal on purpose. */
  app.post('/api/ingest/jobs/:id', requireIngest, (req, res) => {
    const { status, result_path = null, error = null } = req.body ?? {};
    if (!['done', 'failed'].includes(status)) {
      return res.status(400).json({ error: 'status must be done or failed' });
    }
    const j = R.prepare('SELECT * FROM job WHERE id = ?').get(req.params.id);
    if (!j) return res.status(404).json({ error: 'no such job' });
    if (j.status === 'done' || j.status === 'failed') {
      // Idempotent: a worker that reported and then lost the response should
      // be able to say it again without it being an error.
      return res.json({ job: jobRow(j), already: true });
    }
    const t = now();
    W.prepare(
      `UPDATE job SET status = ?, result_path = ?, error = ?, updated_at = ?, finished_at = ?
        WHERE id = ?`)
      .run(status, result_path ? String(result_path).slice(0, 2000) : null,
           error ? String(error).slice(0, 4000) : null, t, t, j.id);
    bumpGeneration(W);
    res.json({ job: jobRow(R.prepare('SELECT * FROM job WHERE id = ?').get(j.id)) });
  });

  // -------------------------------------------------------------------------
  // snippets
  //
  // Read-only here. Everything that writes a snippet or a taglet goes through
  // /api/changesets like every other decision in the archive; the importer
  // writes the initial thousand rows directly, the same way the vault import
  // did.
  // -------------------------------------------------------------------------

  /* Who may see a snippet, in one place because it is asked in four: the
     list, the detail route, the video route and the poster route. Four
     hand-written copies of a visibility rule is how one of them drifts, and
     the media routes are the pair that already had to be fixed once for
     serving unpublished bytes to anyone holding the id.

     Published, or you are an editor, or it is YOURS and still waiting. That
     last clause is the whole of "a submitter sees their own upload and nobody
     else does" — without it somebody uploads a clip, sees nothing happen, and
     uploads it twice more. */
  const snipVisible = (row, req) =>
    !!row && (row.status === 'confirmed'
              || atLeast(req.person, 'editor')
              || (row.status === 'proposed'
                  && !!row.author_id && row.author_id === req.person?.id));

  /* The same rule as a WHERE fragment, so the list cannot disagree with the
     routes that serve what the list linked to. A null `me` collapses it back
     to the published-only clause the archive has always had. */
  const snipVisibleSql = (me) => (me
    ? { sql: `(s.status = 'confirmed' OR (s.status = 'proposed' AND s.author_id = ?))`,
        params: [me] }
    : { sql: `s.status = 'confirmed'`, params: [] });

  const TAGLETS_OF = R.prepare(
    // st.id comes back as link_id because detaching deletes the JUNCTION, not
    // the taglet, and the client would otherwise have no way to name it.
    `SELECT st.snippet_id, st.id AS link_id, t.id, t.name, t.slug, t.kind
       FROM snippet_taglet st JOIN taglet t ON t.id = st.taglet_id
      WHERE st.snippet_id = ? AND t.retracted_at IS NULL
      ORDER BY CASE t.kind WHEN 'character' THEN 0 WHEN 'copyright' THEN 1
                           WHEN 'meta' THEN 2 ELSE 3 END, t.name`);

  const LINES_OF = R.prepare(
    `SELECT seq, start_s, end_s, speaker, text FROM snippet_line
      WHERE snippet_id = ? ORDER BY seq`);

  const snipRow = (r, { lines = false, me = null } = {}) => ({
    id: r.id,
    slug: r.slug,
    title: r.title,
    /* The client has to be able to tell a published row from one only its
       submitter can see, or the pending clip renders as though it were live. */
    status: r.status,
    mine: !!(r.author_id && r.author_id === me),
    summary: r.summary,
    duration_s: r.duration_s,
    added_at: r.added_at,
    width: r.width,
    height: r.height,
    // Paths are never sent. The client addresses media by snippet id and the
    // server resolves it, so a row can never hand out something that looks like
    // a filesystem path to try things against.
    video: `/media/snippet/${r.id}`,
    poster: r.poster_path ? `/media/snippet-poster/${r.id}` : null,
    source_stream_id: r.source_stream_id,
    source_offset_s: r.source_offset_s,
    has_transcript: !!r.transcript,
    transcript_status: r.transcript_status,
    transcript_model: r.transcript_model ?? null,
    transcript_note: r.transcript_note ?? null,
    taglets: TAGLETS_OF.all(r.id).map((t) => ({
      id: t.id, link_id: t.link_id, name: t.name, slug: t.slug, kind: t.kind })),
    ...(lines ? { lines: LINES_OF.all(r.id) } : {}),
  });

  app.get('/api/snippets', (req, res) => {
    const { q = '', taglet, kind, scope = 'all', include = '' } = req.query;
    const limit = Math.min(Math.max(Number(req.query.limit ?? 40) || 40, 1), config.pageMax);
    const before = req.query.before ?? null;
    /* The rows come with their transcripts. A page of 40 clips is a few hundred
       short lines — far less than the poster images beside them — and the
       alternative is 40 detail fetches to fill 40 panels that are all on screen
       at once. */
    const wantLines = String(include).split(',').includes('lines');

    /* The publication gate, enforced HERE and not in the page.
       A thousand clips land unreviewed; until someone says yes they are not
       part of the archive. Only an editor may ask for anything else, and the
       check is the server's — hiding the tab would leave the rows one crafted
       query string away from anyone who guessed the parameter.

       Resolved before the ETag because it is PART of the cache key: the same
       URL legitimately answers differently for an editor and for everyone
       else, and a shared ETag would let one of them be served the other's
       page. */
    const mayReview = atLeast(req.person, 'editor');
    const me = req.person?.id ?? null;
    const wantStatus = req.query.status ? String(req.query.status) : null;
    if (wantStatus && !mayReview) {
      return res.status(403).json({ error: 'only editors may filter by review status' });
    }

    /* Tag filtering is a small boolean expression now, not one slug.
         ?taglet=a&taglet=b   every one of them            (AND)
         ?taglet_any=a,b      at least one of them         (OR)
         ?taglet_not=a,b      none of them                 (NOT)
       Each accepts repeats or a comma-joined list, and `?taglet=a` alone still
       means what it always did, so old links keep working.

       Resolved here rather than below because the ETag has to carry them: two
       requests that differ only in their tag expression are two different
       pages, and a shared validator would serve one of them the other's. */
    const slugsOf = (v) => [...new Set(
      (Array.isArray(v) ? v : v == null ? [] : [v])
        .flatMap((x) => String(x).split(','))
        .map((x) => x.trim().toLowerCase())
        .filter(Boolean))].slice(0, config.tagTerms ?? 24);
    const tagAll = slugsOf(taglet);
    const tagAny = slugsOf(req.query.taglet_any);
    const tagNot = slugsOf(req.query.taglet_not);

    /* `me` and not just `mayReview`: visibility now varies per PERSON, so a
       role-keyed validator would serve one submitter's pending clip to the
       next one out of cache. The gate would be correct and bypassed. */
    const etag = etagFor('snips', q, tagAll, tagAny, tagNot, kind, scope, limit,
                         before, wantLines, req.query.transcript, wantStatus,
                         mayReview, me);
    if (fresh(req, res, etag, { personal: !!me })) return res.status(304).end();

    const where = ['s.retracted_at IS NULL'];
    const params = [];

    if (wantStatus === 'all') { /* every status; editors only, checked above */ }
    else if (wantStatus) { where.push('s.status = ?'); params.push(wantStatus); }
    else {
      const v = snipVisibleSql(me);
      where.push(v.sql);
      params.push(...v.params);
    }

    /* Two search modes, and the default is now the WIDE one.
       `scope=all`   — taglet names, title, and the transcript.
       `scope=tags`  — taglet names and the title only. Still here, because
                       "clips ABOUT donuts" and "clips where she says donut"
                       are genuinely different questions and the narrow one is
                       occasionally what you want.

       This default was 'tags', with a toggle in the panel to widen it. The
       toggle is gone: it asked the reader to know that a clip's words are
       indexed separately from its tags, which is the archive's problem and not
       theirs, and the case it existed for — a tag search drowned in
       transcripts — is served better by the taglet autocomplete, which filters
       on the junction table and cannot be diluted by prose at all.

       The default has to move with it. Leaving it at 'tags' would mean the
       panel and a bare `/api/snippets?q=` disagreed about what a search is,
       which is the kind of split that survives until someone bookmarks a URL
       and cannot work out why it finds less than the page does. */
    if (q) {
      const like = `%${String(q).trim().toLowerCase()}%`;
      const clauses = [
        `EXISTS (SELECT 1 FROM snippet_taglet st JOIN taglet t ON t.id = st.taglet_id
                  WHERE st.snippet_id = s.id AND t.retracted_at IS NULL
                    AND (lower(t.name) LIKE ? OR t.slug LIKE ?))`,
        `lower(s.title) LIKE ?`,
      ];
      params.push(like, like, like);
      if (String(scope) === 'all') {
        const match = ftsQuery(String(q));
        if (match) {
          // FTS carries the transcript because it is the only column here big
          // enough to need an index; the two LIKEs above stay literal so a
          // partial taglet name still matches mid-word, which a porter-stemmed
          // index will not do.
          clauses.push(`s.rowid IN (SELECT rowid FROM snippet_fts WHERE snippet_fts MATCH ?)`);
          params.push(match);
        }
      }
      where.push(`(${clauses.join(' OR ')})`);
    }
    /* One EXISTS per required tag, and that is the whole trick: "has a AND has
       b" is two rows in the junction table, never one row matching two slugs,
       so a single clause with an IN list would quietly mean OR. The OR group
       IS that single clause, which is why it gets one. */
    const HAS = (test) =>
      `SELECT 1 FROM snippet_taglet st JOIN taglet t ON t.id = st.taglet_id
        WHERE st.snippet_id = s.id AND t.retracted_at IS NULL AND ${test}`;
    for (const slug of tagAll) {
      where.push(`EXISTS (${HAS('t.slug = ?')})`);
      params.push(slug);
    }
    for (const slug of tagNot) {
      where.push(`NOT EXISTS (${HAS('t.slug = ?')})`);
      params.push(slug);
    }
    if (tagAny.length) {
      where.push(`EXISTS (${HAS(`t.slug IN (${tagAny.map(() => '?').join(', ')})`)})`);
      params.push(...tagAny);
    }
    if (kind) {
      where.push(`EXISTS (SELECT 1 FROM snippet_taglet st JOIN taglet t ON t.id = st.taglet_id
                           WHERE st.snippet_id = s.id AND t.kind = ? AND t.retracted_at IS NULL)`);
      params.push(String(kind));
    }
    // ?transcript=failed|none|empty|auto|edited — how you find the clips that
    // need another pass without reading a thousand sidecars off the NAS.
    if (req.query.transcript) {
      where.push('s.transcript_status = ?');
      params.push(String(req.query.transcript));
    }
    /* Newest first, by when the CLIP was added rather than when its row was.
       A bulk import stamps every row with the same created_at and mints its
       ULIDs in readdir order, so ordering on either sorts a thousand clips
       alphabetically and buries the newest one at the bottom. added_at is the
       file's own mtime; id breaks the ties, of which there are many, because
       a batch copied in one go shares an mtime to the second.

       Keyset on the same pair — a compound sort needs a compound cursor, or
       the second page starts from a position the first page never ended at
       and rows are skipped or repeated. SQLite compares row values directly,
       which is exactly the "everything strictly after this row" this wants. */
    if (before) {
      const [a, b] = String(before).split('.');
      where.push('(s.added_at, s.id) < (?, ?)');
      params.push(Number(a) || 0, b ?? '');
    }

    const rows = R.prepare(
      `SELECT * FROM snippet s WHERE ${where.join(' AND ')}
        ORDER BY s.added_at DESC, s.id DESC LIMIT ?`).all(...params, limit + 1);

    const page = rows.slice(0, limit);
    const last = page[page.length - 1];
    res.json({
      snippets: page.map((r) => snipRow(r, { lines: wantLines, me })),
      next: rows.length > limit ? `${last.added_at}.${last.id}` : null,
      count: R.prepare(`SELECT count(*) c FROM snippet s WHERE ${where.join(' AND ')}`)
        .get(...params).c,
      generation: Number(generation()),
    });
  });

  app.get('/api/snippets/:id', (req, res) => {
    const mayReview = atLeast(req.person, 'editor');
    const me = req.person?.id ?? null;
    const etag = etagFor('snip', req.params.id, mayReview, me);
    if (fresh(req, res, etag, { personal: !!me })) return res.status(304).end();
    const r = R.prepare('SELECT * FROM snippet WHERE id = ? AND retracted_at IS NULL')
      .get(req.params.id);
    // 404 rather than 403 for an unpublished one: "there is nothing here" and
    // "there is something here you may not see" are different disclosures, and
    // only the first is any of an anonymous reader's business.
    if (!snipVisible(r, req)) {
      return res.status(404).json({ error: 'no such snippet' });
    }
    res.json({ snippet: snipRow(r, { lines: true, me }) });
  });

  /* Correcting one transcript segment.
     Whisper mishears names constantly — every VTuber handle in this archive is
     a word it has never seen — so the common edit is one word in one segment,
     and it must not disturb the timings, which are the half a human cannot
     reproduce.

     Deliberately NOT a changeset, matching the note on `snippet_line` in
     schema.sql: a transcript is one artefact of one pass, and thirty rows of
     "who edited line 14" is noise rather than history. What IS recorded is
     that a human has been through it — transcript_status flips to 'edited',
     which is also what stops the next --update pass overwriting the
     correction. */
  app.patch('/api/snippets/:id/line/:seq', requireRole('editor'), (req, res) => {
    const text = String(req.body?.text ?? '');
    if (text.length > 2000) return res.status(400).json({ error: 'that is not a line' });
    const s = R.prepare('SELECT id FROM snippet WHERE id = ? AND retracted_at IS NULL')
      .get(req.params.id);
    if (!s) return res.status(404).json({ error: 'no such snippet' });
    const seq = Number(req.params.seq);
    if (!Number.isInteger(seq)) return res.status(400).json({ error: 'seq must be an integer' });

    try {
      const out = tx(W, () => {
        const hit = W.prepare(
          'UPDATE snippet_line SET text = ? WHERE snippet_id = ? AND seq = ?')
          .run(text, s.id, seq);
        /* Creates the line when it is not there yet, which is the only route
           into a clip that has NO transcript at all — whisper never ran, or ran
           and heard nothing. Editing presumes a line to edit, so without this
           those clips are permanently mute in the UI and the sole fix is
           re-importing a sidecar.
           Bounded to appending exactly one past the end: an editor writing the
           next line is ordinary, an editor inventing seq 900 on an empty clip
           is a gap the transcript has no way to represent. */
        if (!hit.changes) {
          const n = W.prepare('SELECT count(*) c FROM snippet_line WHERE snippet_id = ?')
            .get(s.id).c;
          if (seq !== n) return null;
          if (!text.trim()) return null;   // nothing to create
          W.prepare(
            `INSERT INTO snippet_line(id, snippet_id, seq, start_s, end_s, speaker, text)
             VALUES(?,?,?,?,NULL,NULL,?)`)
            .run(ulid(), s.id, seq, 0, text);
        }
        // The flat column is FTS's only view of this. Re-derived from the
        // lines rather than patched, so the index cannot drift from the text
        // the panel shows.
        const flat = W.prepare(
          'SELECT group_concat(text, \' \') g FROM (SELECT text FROM snippet_line '
          + 'WHERE snippet_id = ? ORDER BY seq)').get(s.id).g;
        W.prepare(
          `UPDATE snippet SET transcript = ?, transcript_status = 'edited', updated_at = ?
            WHERE id = ?`).run(flat, now(), s.id);
        return flat;
      });
      if (out === null) return res.status(404).json({ error: 'no such line' });
      bumpGeneration(W);
      res.json({ ok: true, seq, text });
    } catch (e) { return changeError(res, e); }
  });

  /* Replacing the WHOLE transcript.
     The per-segment PATCH above is the fast path for the common edit — one
     misheard name — and it deliberately cannot touch a timing. This is the
     other half, and it exists because retiming, splitting, joining, reordering
     and deleting are not things a per-line PATCH can express: inserting one
     line in the middle renumbers every seq after it, so N PATCHes would walk
     the transcript through N invalid intermediate states and a failure halfway
     would leave it parked in one. One request, one transaction, whole artefact.

     Not a changeset, for the same reason the segment PATCH is not — see the
     note on `snippet_line` in schema.sql. What is recorded is the same thing:
     transcript_status flips to 'edited', which is also what stops the next
     --update pass overwriting the correction.

     end_s is not in the request because the editor never shows it. It is
     carried across for any segment whose start_s came back unchanged — the
     lines nobody touched keep whisper's measured end — and dropped for the
     rest, where the panel's "until the next one starts" fallback is right and
     a stale end would light the wrong words as the clip plays. */
  app.put('/api/snippets/:id/transcript', requireRole('editor'), (req, res) => {
    const s = R.prepare('SELECT id FROM snippet WHERE id = ? AND retracted_at IS NULL')
      .get(req.params.id);
    if (!s) return res.status(404).json({ error: 'no such snippet' });

    const raw = req.body?.lines;
    if (!Array.isArray(raw)) return res.status(400).json({ error: 'lines must be an array' });
    if (raw.length > 2000) return res.status(400).json({ error: 'at most 2000 lines' });

    /* Every rejection names the line it is about. The editor on the other end
       is a textarea, and "line 7" is the only handle anyone has on it. */
    const lines = [];
    for (const [i, l] of raw.entries()) {
      const at = i + 1;
      const bad = (error) => ({ error, line: at });
      const text = String(l?.text ?? '').trim();
      if (!text) return res.status(400).json(bad(`line ${at} has no words`));
      if (text.length > 2000) return res.status(400).json(bad(`line ${at} is not a line`));
      const start = Number(l?.start_s);
      if (!Number.isFinite(start) || start < 0) {
        return res.status(400).json(bad(`line ${at} has no readable time`));
      }
      const prev = lines[lines.length - 1];
      if (prev && start < prev.start_s) {
        return res.status(400).json(bad(`line ${at} starts before the line above it`));
      }
      const speaker = l?.speaker == null ? null : String(l.speaker).trim() || null;
      if (speaker && speaker.length > 200) {
        return res.status(400).json(bad(`line ${at} has an implausible speaker`));
      }
      lines.push({ start_s: start, text, speaker });
    }

    try {
      tx(W, () => {
        const was = new Map(W.prepare(
          'SELECT start_s, end_s FROM snippet_line WHERE snippet_id = ?').all(s.id)
          .map((r) => [r.start_s, r.end_s]));
        W.prepare('DELETE FROM snippet_line WHERE snippet_id = ?').run(s.id);
        const ins = W.prepare(
          `INSERT INTO snippet_line(id, snippet_id, seq, start_s, end_s, speaker, text)
           VALUES(?,?,?,?,?,?,?)`);
        lines.forEach((l, i) => {
          let end = was.has(l.start_s) ? was.get(l.start_s) : null;
          // A carried end that now runs past the segment below it would light
          // two segments at once. The neighbour moved; the end is stale.
          const next = lines[i + 1]?.start_s;
          if (end != null && next != null && end > next) end = null;
          ins.run(ulid(), s.id, i, l.start_s, end, l.speaker, l.text);
        });
        /* Re-derived from the lines rather than patched, so the index cannot
           drift from the text the panel shows — same as the segment PATCH.
           An emptied transcript is a null column and not an empty string:
           that is what every other "there is nothing here" reads as. */
        W.prepare(
          `UPDATE snippet SET transcript = ?, transcript_status = 'edited', updated_at = ?
            WHERE id = ?`)
          .run(lines.length ? lines.map((l) => l.text).join(' ') : null, now(), s.id);
      });
      bumpGeneration(W);
      res.json({ ok: true, lines: lines.length });
    } catch (e) { return changeError(res, e); }
  });

  /* Review, in bulk, through ONE changeset.
     Publishing fifty clips is one decision taken in one sitting, and the
     history should say that rather than scrolling fifty identical entries
     past whoever reads it later. propose() with fifty change rows gives an
     editor auto-apply, provenance stamped by the applier, and a single
     reversible unit — which is also what makes Undo in the panel honest. */
  app.post('/api/snippets/review', requireRole('editor'), (req, res) => {
    const { ids, decision, note = null } = req.body ?? {};
    const list = [...new Set([].concat(ids ?? []).map(String).filter(Boolean))];
    if (!list.length) return res.status(400).json({ error: 'ids is required' });
    if (list.length > 500) return res.status(400).json({ error: 'at most 500 at a time' });
    const STATUS = { approve: 'confirmed', reject: 'rejected', reset: 'proposed' };
    if (!STATUS[decision]) {
      return res.status(400).json({ error: `decision must be one of ${Object.keys(STATUS)}` });
    }
    const want = STATUS[decision];

    // Rows that are already there contribute nothing but a change row saying
    // nothing changed, and propose() rejects a no-op update outright — so one
    // already-approved clip in a bulk selection would fail the whole batch.
    const have = R.prepare(
      `SELECT id, status FROM snippet WHERE id IN (${list.map(() => '?').join(',')})
        AND retracted_at IS NULL`).all(...list);
    const move = have.filter((r) => r.status !== want);
    if (!move.length) {
      return res.json({ changed: 0, skipped: have.length, missing: list.length - have.length });
    }
    try {
      const out = propose(W, {
        authorId: req.person.id,
        reason: note ?? `${decision} ${move.length} snippet${move.length === 1 ? '' : 's'}`,
        autoApply: true,
        mediaRoot: config.mediaRoot,
        changes: move.map((r) => ({
          target_type: 'snippet', target_id: r.id, op: 'update',
          field: 'status', value: want,
          // Client-supplied base: what we just read is what the decision was
          // taken against, so a row someone else moved in between conflicts
          // instead of being silently overwritten.
          base_value: r.status,
        })),
      });
      res.json({ changed: move.length, skipped: have.length - move.length,
                 missing: list.length - have.length, changeset: out.id ?? null });
    } catch (e) { return changeError(res, e); }
  });

  app.get('/api/taglets', (req, res) => {
    const { q = '', kind } = req.query;
    const limit = Math.min(Math.max(Number(req.query.limit ?? 500) || 500, 1), 2000);
    const etag = etagFor('taglets', q, kind, limit);
    if (fresh(req, res, etag)) return res.status(304).end();

    const where = ['t.retracted_at IS NULL'];
    const params = [];
    if (q) {
      where.push('(lower(t.name) LIKE ? OR t.slug LIKE ?)');
      const like = `%${String(q).trim().toLowerCase()}%`;
      params.push(like, like);
    }
    if (kind) { where.push('t.kind = ?'); params.push(String(kind)); }

    // Count comes back with the row because the picker is useless without it:
    // "funny (214)" and "funny (1)" are different suggestions.
    res.json({ taglets: R.prepare(
      `SELECT t.id, t.name, t.slug, t.kind, t.status, t.summary,
              (SELECT count(*) FROM snippet_taglet st JOIN snippet s ON s.id = st.snippet_id
                WHERE st.taglet_id = t.id AND s.retracted_at IS NULL) AS uses
         FROM taglet t WHERE ${where.join(' AND ')}
        ORDER BY uses DESC, t.name LIMIT ?`).all(...params, limit) });
  });

  // -------------------------------------------------------------------------
  // liveness — what the recorder is holding open right now
  // -------------------------------------------------------------------------

  /* "Live" here means ls-rec has a recording process running, NOT that the
     channel is broadcasting. The two genuinely differ: she can be live on a
     channel nobody is watching, and ls-rec can be down while she streams. The
     recorder's answer is the one this archive can stand behind — "we are
     capturing this" is the promise a badge here should make — and it is the
     only answer reachable without giving this process an outbound socket, a
     YouTube Data API key and yt-dlp, none of which it has or should.

     Deliberately NOT a table. This value is meaningless a minute after it was
     written, would be a row rewritten every sixty seconds forever, and losing
     it on restart costs nothing: the next heartbeat restores it within a tick.
     The durable half of the same event is already recorded elsewhere —
     post_start creates the stream row and its started_at, which is what
     chapters and note offsets are measured against. */
  let live = { boot: null, seq: -1, at: 0, interval_s: 60, recording: [] };

  // Three missed heartbeats. Taken from the interval the recorder reports
  // rather than a constant here, so raising ls-rec's check_interval does not
  // quietly start flapping the badge between beats.
  const STALE_MULT = 3;

  const LIVE_TOP = new Set(['boot_id', 'seq', 'sent_at', 'interval_s', 'recording']);
  const LIVE_REC = new Set(['platform', 'remote_id', 'index', 'title', 'url',
                            'broadcast_started_at', 'record_started_at', 'stalled']);

  const int = (v) => (v === null || v === undefined || v === '' || !Number.isFinite(Number(v))
    ? null : Math.trunc(Number(v)));

  app.post('/api/ingest/live', requireIngest, (req, res) => {
    const b = req.body ?? {};
    const unknown = Object.keys(b).filter((k) => !LIVE_TOP.has(k));
    if (unknown.length) {
      return res.status(400).json({ error: 'unknown field(s)', fields: unknown });
    }
    /* Required, not defaulted. An empty array is the packet that says nothing
       is running, and it is the one that makes the whole scheme self-healing —
       every beat carries the FULL set, so a lost packet can never strand a
       stuck LIVE badge and no "stopped" event is ever needed. Defaulting a
       missing key to [] would make a malformed body read as "she went offline"
       instead of failing where it can be seen. */
    if (!Array.isArray(b.recording)) {
      return res.status(400).json({
        error: 'recording must be an array — send [] to say nothing is running',
      });
    }
    const rows = [];
    for (const r of b.recording) {
      const bad = Object.keys(r ?? {}).filter((k) => !LIVE_REC.has(k));
      if (bad.length) {
        return res.status(400).json({ error: 'unknown field(s) in recording[]', fields: bad });
      }
      const platform = String(r?.platform ?? '').toUpperCase();
      if (!['YT', 'TW'].includes(platform)) {
        return res.status(400).json({ error: 'recording[].platform must be YT or TW' });
      }
      rows.push({
        platform,
        remote_id: r.remote_id ? String(r.remote_id) : null,
        index: int(r.index),
        title: r.title ? String(r.title) : null,
        url: r.url ? String(r.url) : null,
        broadcast_started_at: int(r.broadcast_started_at),
        record_started_at: int(r.record_started_at),
        stalled: !!r.stalled,
      });
    }

    /* An ordering guard and nothing more. Heartbeats go out one at a time from
       a single poll loop and are never queued for retry — a stale heartbeat is
       worse than a missing one — so genuine reordering is close to impossible.
       What this catches is the case that is plausible: two recorders pointed at
       the same archive by accident, which would otherwise flap the badge
       between their two answers forever. boot_id scopes the counter, so a
       restarted recorder beginning again at 0 is not locked out for good. */
    const seq = int(b.seq);
    const boot = b.boot_id ? String(b.boot_id) : null;
    if (seq === null) return res.status(400).json({ error: 'seq must be a number' });
    if (boot && boot === live.boot && seq <= live.seq) {
      return res.status(409).json({ error: 'stale heartbeat', have: live.seq, got: seq });
    }

    const iv = int(b.interval_s);
    live = {
      boot,
      seq,
      at: now(),
      interval_s: iv && iv > 0 ? Math.min(3600, iv) : 60,
      recording: rows,
    };
    res.json({ ok: true, recording: rows.length, stale_after_s: live.interval_s * STALE_MULT });
  });

  const capByRemote = R.prepare(
    'SELECT stream_id FROM capture WHERE platform = ? AND remote_id = ?');
  const streamByIdx = R.prepare('SELECT id FROM stream WHERE idx = ?');
  const streamById = R.prepare('SELECT id, idx, title, started_at FROM stream WHERE id = ?');

  function liveState() {
    const age = live.at ? now() - live.at : null;
    const stale = age === null || age > live.interval_s * STALE_MULT;
    /* `stale` and "not live" are different facts and the page may want to draw
       them differently — one is "she is not streaming", the other is "we have
       lost contact with the recorder and cannot say". */
    /* `now` is this server's clock at the moment of answering, and the page
       arms `@` against it rather than against Date.now(). A viewer's PC that is
       four minutes fast would otherwise file every live note four minutes late,
       with nothing on screen to suggest anything was wrong. */
    const out = {
      now: now(), checked_at: live.at || null, age_s: age, stale,
      interval_s: live.interval_s, YT: { live: false }, TW: { live: false },
    };
    if (stale) return out;
    for (const r of live.recording) {
      /* remote_id first: it is the identity anchor everywhere else in ingest,
         while index can be a number the recorder guessed while this API was
         unreachable. And the archive's OWN started_at is preferred over the
         recorder's broadcast_started_at, because it is the zero that every
         note, chapter and deep link on this stream is already measured
         against. A heartbeat is not the place to introduce a second one. */
      const sid = (r.remote_id ? capByRemote.get(r.platform, r.remote_id)?.stream_id : null)
        ?? (r.index ? streamByIdx.get(r.index)?.id : null)
        ?? null;
      const row = sid ? streamById.get(sid) : null;
      const anchored = row && row.started_at !== null && row.started_at !== undefined;
      out[r.platform] = {
        live: true,
        stalled: r.stalled,
        url: r.url ?? null,
        title: row?.title ?? r.title ?? null,
        since: r.broadcast_started_at ?? r.record_started_at ?? null,
        /* Both or neither, on purpose. A note can only be stamped against a
           stream that exists, so handing out a started_at without the id it
           belongs to is an invitation to write the number onto the wrong row.
           Absent these two, the page lights the badge and declines to offer @. */
        ...(anchored ? { stream_id: row.id, idx: row.idx, started_at: row.started_at } : {}),
      };
    }
    return out;
  }

  app.get('/api/live', (req, res) => {
    // Never cached. The entire value of this response is that it is current,
    // and fresh() would hand the page a fifteen-second-old answer.
    res.set('Cache-Control', 'no-store');
    res.json(liveState());
  });

  // -------------------------------------------------------------------------
  // admin
  // -------------------------------------------------------------------------

  app.get('/api/admin/people', requireRole('admin'), (req, res) => {
    res.json({ people: R.prepare(
      `SELECT id, provider, handle, display_name, role, banned, created_at, last_seen_at
       FROM person ORDER BY created_at DESC LIMIT 500`).all() });
  });

  app.post('/api/admin/people/:id/role', requireRole('admin'), (req, res) => {
    const { role } = req.body ?? {};
    if (!ROLES.includes(role)) return res.status(400).json({ error: `role must be one of ${ROLES}` });
    if (req.params.id === req.person.id && role !== 'admin') {
      return res.status(400).json({ error: 'refusing to demote yourself — you would ' +
        'lock yourself out of the only account that can undo it' });
    }
    if (!W.prepare('SELECT 1 FROM person WHERE id = ?').get(req.params.id)) {
      return res.status(404).json({ error: 'no such person' });
    }
    W.prepare('UPDATE person SET role = ? WHERE id = ?').run(role, req.params.id);
    res.json({ id: req.params.id, role });
  });

  // -------------------------------------------------------------------------
  // media
  //
  // Unset root means the routes 503 rather than serving from an accidental
  // default.
  //
  // This comment used to say a client never supplies a path, so there was no
  // traversal to defend against. That was true of /media/video/:capture_id and
  // false of /media/thumb/:rest(*) directly below it, which takes the path off
  // the URL. Express decodes route params after path resolution, so `../`
  // normalised away and returned 404 while `..%2F` did not — and reached
  // res.sendFile with an absolute path outside the root, which send() does not
  // check when no `root` option is given. Containment now lives inside
  // resolveMedia() so both routes and statMedia() get it; see the note there.
  // -------------------------------------------------------------------------

  const MIME = { '.mp4': 'video/mp4', '.webm': 'video/webm', '.mkv': 'video/x-matroska',
                 '.m4a': 'audio/mp4', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
                 '.png': 'image/png', '.webp': 'image/webp' };

  /* `type` overrides the extension table when the caller knows better — which
     for snippets it does, because the row records the codecs the file actually
     holds and the extension is only a claim. `root` likewise: a remuxed clip
     lives under the cache root, not the media root. */
  function sendMedia(req, res, rel, { type = null, root = null } = {}) {
    const from = root ?? config.mediaRoot;
    if (!from) {
      return res.status(503).json({ error: 'no media root — set TENMA_MEDIA_ROOT' });
    }
    const path = resolveMedia(from, rel);
    if (!path) return res.status(404).json({ error: 'not on disk' });
    // Range support is not optional for a four-hour file: without it the
    // browser cannot seek, and the theater's whole point is seeking.
    res.set('Accept-Ranges', 'bytes');
    /* The allowlist above is what makes this safe, so it must never be
       bypassed: a sniffing browser is how an uploaded file becomes stored XSS
       on your own origin. Cheap, and it stops mattering only if the allowlist
       is ever removed. */
    res.set('X-Content-Type-Options', 'nosniff');
    res.type(type ?? MIME[extname(path).toLowerCase()] ?? 'application/octet-stream');
    return res.sendFile(path, { acceptRanges: true, cacheControl: true, maxAge: '1h' });
  }

  app.get('/media/video/:capture_id', (req, res) => {
    const c = R.prepare('SELECT video_path FROM capture WHERE id = ?').get(req.params.capture_id);
    if (!c?.video_path) return res.status(404).json({ error: 'no such capture' });
    return sendMedia(req, res, c.video_path);
  });

  app.get('/media/thumb/:rest(*)', (req, res) => sendMedia(req, res, req.params.rest));

  // By id, never by path. The client is given `/media/snippet/<id>` and the row
  // holds the only path anyone gets to name — the same shape as /media/video,
  // and deliberately not the shape of /media/thumb, which takes a path off the
  // URL and needed a traversal fix to be safe.
  /* The bytes are gated exactly like the metadata, and it took a rejected clip
     to notice they were not. `/api/snippets/:id` has always 404'd anything
     unpublished, but this route checked only `retracted_at` — so a clip you
     had decided NOT to publish still streamed in full to anyone holding its
     id, poster and all.
     Not currently reachable: ids are ULIDs and appear in no ungated response.
     But "rejected" is a promise about what the archive serves, and a promise
     kept by the metadata and broken by the media is not kept. */
  app.get('/media/snippet/:id', (req, res) => {
    const s = R.prepare(`SELECT video_path, play_path, container, video_codec, audio_codec,
                                status, author_id
                           FROM snippet WHERE id = ? AND retracted_at IS NULL`).get(req.params.id);
    if (!snipVisible(s, req)) return res.status(404).json({ error: 'no such snippet' });
    if (!s?.video_path) return res.status(404).json({ error: 'no such snippet' });

    /* A remuxed copy wins when one exists: the original is a file no browser
       will decode, so serving it is a blank player and a console error rather
       than an honest failure. It lives under the cache root because the media
       root is read-only and holds the masters. */
    if (s.play_path && config.cacheRoot) {
      const t = extname(s.play_path).toLowerCase() === '.webm' ? 'video/webm' : 'video/mp4';
      return sendMedia(req, res, s.play_path, { type: t, root: config.cacheRoot });
    }

    /* Content-Type from the CODECS, not the extension. `.webm` holding H.264
       is a real file that ffmpeg will happily produce and no browser will
       play; labelling it video/webm is a lie the browser catches and the
       server never notices. servedType() returns null for exactly that case —
       and a null here is worth reporting as a 415 rather than streaming bytes
       that cannot be decoded, because "unsupported" in a network log is
       findable and a silently blank player is not. */
    const type = servedType(s.container, s.video_codec, s.audio_codec);
    if (!type && s.container) {
      return res.status(415).json({
        error: 'not web-playable',
        detail: `${s.video_codec || '?'}/${s.audio_codec || 'none'} in ${s.container}`,
        fix: 'scripts/check-media.js --remux',
      });
    }
    // No recorded codecs at all means a row imported before this existed;
    // fall back to the extension table rather than refusing to serve it.
    return sendMedia(req, res, s.video_path, { type });
  });

  /* Posters resolve against the CACHE root first, then the media root.
     Two roots because they are written by different things: the importer
     generates into the cache, while your own prep pass may already have left
     stills beside the clips under media. Both are legitimate and neither
     should have to know about the other — and resolveMedia() does the same
     containment check either way, so the fallback opens no path it would not
     already have opened. */
  app.get('/media/snippet-poster/:id', (req, res) => {
    const s = R.prepare(
      `SELECT poster_path, status, author_id FROM snippet
          WHERE id = ? AND retracted_at IS NULL`)
      .get(req.params.id);
    // A still is a frame of the thing, so it is gated with the thing.
    if (!snipVisible(s, req)) return res.status(404).json({ error: 'no poster' });
    if (!s?.poster_path) return res.status(404).json({ error: 'no poster' });
    const path = (config.cacheRoot && resolveMedia(config.cacheRoot, s.poster_path))
      || (config.mediaRoot && resolveMedia(config.mediaRoot, s.poster_path));
    if (!path) return res.status(404).json({ error: 'not on disk' });
    res.type(MIME[extname(path).toLowerCase()] ?? 'application/octet-stream');
    // A poster never changes under a given path — the importer writes a new
    // file rather than editing one — so this can be cached hard.
    return res.sendFile(path, { cacheControl: true, maxAge: '7d', immutable: true });
  });

  // ---------------------------------------------------------------------------
  // What is actually on disk, so the editor can offer it instead of asking.
  //
  // These paths point at a NAS nobody browses from the page, and typing
  // `raws/697_[ MINA THE HOLLOWER #2 ] One cheese at a tiiiime [fWk_JdowmGE] @
  // 2026-08-10_11-02.mp4` by hand is how a path acquires the typo that reads as
  // `lost`. Pick from what exists instead.
  //
  // Editor-only. A directory listing of the media root is not secret, but it is
  // nobody's business either, and it is useful only to someone who can act on it.
  // ---------------------------------------------------------------------------
  const BROWSE_KINDS = {
    video: new Set(['.mp4', '.mkv', '.webm', '.ts', '.flv', '.mov', '.m4v']),
    image: new Set(['.jpg', '.jpeg', '.png', '.webp', '.avif', '.gif']),
  };
  const BROWSE_MAX = 300;

  app.get('/api/media/browse', requireRole('editor'), (req, res) => {
    if (!config.mediaRoot) {
      return res.status(503).json({ error: 'no media root — set TENMA_MEDIA_ROOT' });
    }
    // Joined, then checked to still be inside the root — rather than scanned
    // for '..'. Resolution is the only thing that knows about symlinks, encoded
    // separators and the several other spellings of "up one".
    const rel = String(req.query.dir ?? '').replace(/^[/\\]+/, '');
    const root = resolve(config.mediaRoot);
    const dir = resolve(root, rel);
    if (dir !== root && !dir.startsWith(root + sep)) {
      return res.status(400).json({ error: 'outside the media root' });
    }

    const kind = BROWSE_KINDS[String(req.query.kind ?? '')] ?? null;
    const q = String(req.query.q ?? '').trim().toLowerCase();

    let entries;
    try { entries = readdirSync(dir, { withFileTypes: true }); }
    catch { return res.status(404).json({ error: 'no such directory' }); }

    const dirs = [];
    const names = [];
    for (const e of entries) {
      if (e.name.startsWith('.')) continue;
      if (e.isDirectory()) { dirs.push(e.name); continue; }
      if (!e.isFile()) continue;
      if (kind && !kind.has(extname(e.name).toLowerCase())) continue;
      if (q && !e.name.toLowerCase().includes(q)) continue;
      names.push(e.name);
    }
    // Newest first: the file you are looking for is nearly always the one that
    // just landed. The slice before stat() is the backstop — a directory with
    // fifty thousand raws should not stall the page on syscalls.
    const files = names.slice(0, 4000).map((name) => {
      let mtime = 0, bytes = null;
      try { const st = statSync(join(dir, name)); mtime = st.mtimeMs; bytes = st.size; }
      catch { /* vanished between readdir and stat */ }
      return { name, path: rel ? `${rel}/${name}` : name, bytes, mtime };
    });
    files.sort((a, b) => b.mtime - a.mtime);

    res.set('Cache-Control', 'no-store');
    res.json({
      dir: rel, parent: rel ? rel.split('/').slice(0, -1).join('/') : null,
      dirs: dirs.sort(), files: files.slice(0, BROWSE_MAX),
      truncated: files.length > BROWSE_MAX, total: names.length,
    });
  });

  // -------------------------------------------------------------------------
  // frontend
  //
  // Served from the same origin as the API, which is the whole reason there is
  // no CORS or cookie-domain question to answer. Registered last so a file can
  // never shadow a route.
  // -------------------------------------------------------------------------

  app.use(express.static(join(HERE, 'public'), {
    etag: true, maxAge: '5m', index: 'index.html',
  }));

  app.use((err, req, res, _next) => {
    console.error(err);
    res.status(500).json({ error: 'internal error' });
  });

  app.locals.close = () => { R.close(); W.close(); };
  return app;
}

// ---------------------------------------------------------------------------
// listen
// ---------------------------------------------------------------------------

// fileURLToPath, not new URL(...).pathname — on Windows the latter yields
// "/C:/Users/..." with a leading slash and percent-escapes intact, so the
// comparison silently fails and `node server.js` exits without starting or
// printing anything.
const isMain = process.argv[1] &&
  resolve(process.argv[1]) === resolve(fileURLToPath(import.meta.url));

if (isMain) {
  const dbPath = resolveDbPath(CONFIG.db);
  if (!existsSync(dbPath)) {
    console.error(`no database at ${dbPath} — run the import first`);
    process.exit(1);
  }
  const app = makeApp();
  app.listen(CONFIG.port, CONFIG.host, () => {
    console.log(`flatfox  http://${CONFIG.host}:${CONFIG.port}`);
    console.log(`  db          ${dbPath}`);
    console.log(`  media root  ${CONFIG.mediaRoot ?? '(unset — captures read unverified)'}`);
    console.log(`  cache root  ${CONFIG.cacheRoot ?? '(unset — posters only where the media tree has them)'}`);
    console.log(`  dev auth    ${CONFIG.devAuth ? 'ON — do not expose this' : 'off'}`);
    console.log(`  ingest      ${CONFIG.ingestToken ? 'enabled' : 'disabled (no token set)'}`);
  });
}


