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
  bumpGeneration, create, meta, now, open, resolveDbPath, ulid,
} from './db.js';
import {
  ChangeError, KINDS, SEGMENT_KINDS, apply, axisToPosition, buildTimeline, clocksOf,
  deepLink, hms, projectNote, propose, recompute, reject, resolveMedia,
  sourcesFor, stale, summary, thumbFor, watchSources,
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

  function fresh(req, res, etag) {
    res.set('ETag', etag);
    res.set('Cache-Control', 'public, max-age=15, stale-while-revalidate=120');
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
      where.push(`EXISTS (
        SELECT 1 FROM stream_tag st JOIN tag t ON t.id = st.tag_id
         WHERE st.stream_id = s.id
           AND (t.slug = ?1 OR t.parent_id = (SELECT id FROM tag WHERE slug = ?1)))`);
      params.push(String(tag).toLowerCase());
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
  // Keyed strictly by capture id. A client never supplies a path, so there is
  // no traversal to defend against — the only paths reachable are the ones
  // already in the database, resolved under TENMA_MEDIA_ROOT. Unset root means
  // the routes 503 rather than serving from an accidental default.
  // -------------------------------------------------------------------------

  const MIME = { '.mp4': 'video/mp4', '.webm': 'video/webm', '.mkv': 'video/x-matroska',
                 '.m4a': 'audio/mp4', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
                 '.png': 'image/png', '.webp': 'image/webp' };

  function sendMedia(req, res, rel) {
    if (!config.mediaRoot) {
      return res.status(503).json({ error: 'no media root — set TENMA_MEDIA_ROOT' });
    }
    const path = resolveMedia(config.mediaRoot, rel);
    if (!path) return res.status(404).json({ error: 'not on disk' });
    // Range support is not optional for a four-hour file: without it the
    // browser cannot seek, and the theater's whole point is seeking.
    res.set('Accept-Ranges', 'bytes');
    res.type(MIME[extname(path).toLowerCase()] ?? 'application/octet-stream');
    return res.sendFile(path, { acceptRanges: true, cacheControl: true, maxAge: '1h' });
  }

  app.get('/media/video/:capture_id', (req, res) => {
    const c = R.prepare('SELECT video_path FROM capture WHERE id = ?').get(req.params.capture_id);
    if (!c?.video_path) return res.status(404).json({ error: 'no such capture' });
    return sendMedia(req, res, c.video_path);
  });

  app.get('/media/thumb/:rest(*)', (req, res) => sendMedia(req, res, req.params.rest));

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
    console.log(`tenma archive  http://${CONFIG.host}:${CONFIG.port}`);
    console.log(`  db          ${dbPath}`);
    console.log(`  media root  ${CONFIG.mediaRoot ?? '(unset — captures read unverified)'}`);
    console.log(`  dev auth    ${CONFIG.devAuth ? 'ON — do not expose this' : 'off'}`);
    console.log(`  ingest      ${CONFIG.ingestToken ? 'enabled' : 'disabled (no token set)'}`);
  });
}


