// The whole HTTP surface, plus listen(). `node server.js`.
//
// Read endpoints are anonymous, ETag-cached and open a read-only connection.
// The write path is a single endpoint — POST /api/changesets — because a
// decision only ever changes through an applied changeset. There is no PATCH.

import express from 'express';
import { createHash, timingSafeEqual } from 'node:crypto';
import { execFile } from 'node:child_process';
import {
  createReadStream, createWriteStream, existsSync, mkdirSync, readFileSync, readdirSync,
  renameSync, rmSync, statSync, writeFileSync,
} from 'node:fs';
import { setPriority } from 'node:os';
import { createGunzip } from 'node:zlib';
import { basename, dirname, extname, join, resolve, sep } from 'node:path';
import { Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));

import {
  bumpGeneration, create, meta, now, open, resolveDbPath, setMeta, slugify, tx, ulid,
} from './db.js';
import {
  ALL_KINDS, ChangeError, KINDS,
  apply, axisOf, axisToPosition, buildSrt, buildTimeline, clocksOf,
  parsePhoneLine, pinned,
  KIND_DIR, MODEL_CATALOG, MODEL_TASK, MODEL_TASKS, SNIPPET_KINDS, modelInfo,
  classifyMedia, deepLink, hms, isStill, moovFirst, normalizeArgs,
  overBitrate, pcmArgs,
  peaksFromPcm, posterArgs, probeMedia, projectNote, propose, recompute, reject,
  resolveMedia, stillPosterArgs, wavePosterArgs,
  servedType, sourcesFor, stale, summary, thumbFor, watchSources,
} from './archive.js';
import {
  ANON, COOKIE, ROLES, TTL, assertCapabilities, atLeast, can, capabilities,
  identify, issueSession, requireCap, requireRole, revokeSession, sessionToken,
  upsertPerson,
} from './auth.js';

/* A capability granted to nobody, or granted under a name that does not exist,
   is a permission that silently never applies — which from the outside looks
   exactly like the feature not having been built. Fail here instead. */
assertCapabilities();

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
  /* The gate. One secret, one admin, no user management — the smallest thing
     that can stand between the internet and an archive whose real auth has not
     been built yet.
     UNSET IS NOT OPEN. With dev auth off and no password set, nothing can sign
     in at all and only the public share surface answers. That is the same
     choice quarantineRoot and ingestToken make: a control with no value
     configured refuses rather than falls back to permissive. */
  adminPass: process.env.TENMA_ADMIN_PASS ?? '',
  /* Which account that password signs you in as.
     Authorship is by person id, so this is not cosmetic: signing in as a
     brand-new row would leave every note, upload and changeset you made under
     dev auth attributed to an account you can no longer reach. Set this to the
     handle you already use and the existing row is adopted instead. */
  adminHandle: (process.env.TENMA_ADMIN_HANDLE ?? 'admin').trim() || 'admin',
  // Unset disables ingest outright rather than leaving it open — an
  // unauthenticated writer on the recording path is not a sane default.
  ingestToken: process.env.TENMA_INGEST_TOKEN ?? '',
  pairWindow: Number(process.env.TENMA_PAIR_WINDOW_S ?? 600),
  pageMax: Number(process.env.TENMA_PAGE_MAX ?? 100),
  /* The machine-learning sidecar: a second container on the docker network
     that owns Python, the model files and the loading and evicting of them.
     Unset means there is none, and every task that needs one says so rather
     than failing in a way that reads like a bug.

     A sidecar and not a bigger image, deliberately. PaddleOCR and CLIP are
     Python with an ONNX runtime under them — most of a gigabyte on top of a
     Node app that is currently one dependency — and the models want to be
     swapped and re-downloaded without rebuilding the thing that serves pages.
     It also keeps the shape of this archive intact: the container that answers
     requests gains no new write handles, and the container that holds the
     models never sees the media tree.

     Host-only. This is dialled from inside the compose network, never from a
     browser, so it belongs to the same class as TENMA_INGEST_TOKEN: a name
     the archive is allowed to talk to, and nothing a request can influence. */
  mlUrl: (process.env.TENMA_ML_URL ?? '').trim().replace(/\/+$/, '') || null,
  mlTimeoutMs: Number(process.env.TENMA_ML_TIMEOUT_MS) || 120000,
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

  /* ---- the gate ----------------------------------------------------------
   *
   * Deny by default, ahead of every route and ahead of the static mount. The
   * archive's real auth — accounts, roles, review by strangers — is a project
   * of its own; this is the thing that lets the port be opened before that
   * exists.
   *
   * ON whenever dev auth is OFF. Those two are opposites by construction, so
   * there is no configuration in which both are true and no way to expose the
   * archive by forgetting a flag: the line that has to be deleted before
   * anything is reachable is the same line that turns this on.
   *
   * WHAT IS PUBLIC is a short explicit list rather than a pattern, because the
   * list is the security decision and a pattern is a guess about the future.
   * Three things, and each one is already visibility-checked by the route
   * behind it — snipVisible() answers the anonymous question, so a proposed or
   * removed snippet 404s to a stranger exactly as it does today:
   *
   *   GET /m/<id>                       the share card and its meta tags
   *   GET /media/snippet/<id>           the video an unfurl plays
   *   GET /media/snippet-poster/<id>    its thumbnail
   *
   * Plus the doors: the login itself, /api/health (the container's own
   * healthcheck runs with no session and a gate that failed it would restart
   * the container forever), and /api/ingest/* which the recorder authenticates
   * with its own token.
   *
   * Everything else — the index, search, the lists, the streams, the theater,
   * the notes, the tools, every other /api and every other /media — is
   * refused. A stranger holding one link gets that snippet and no way to find
   * a second.
   */
  const ID_RE = '[A-Za-z0-9]{1,64}';
  const OPEN_GET = [
    new RegExp(`^/m/${ID_RE}$`),
    new RegExp(`^/media/snippet/${ID_RE}(?:/[^/]*)?$`),
    new RegExp(`^/media/snippet-poster/${ID_RE}$`),
    /^\/api\/health$/,
    /* Says whether you are signed in and nothing else when you are not. The
       login page needs that answer, and refusing it would mean the page could
       not tell a locked door from a broken server. */
    /^\/api\/auth\/me$/,
    /* The WHOLE ingest namespace, not just its POSTs. This line was missing
       for one round and the two GETs in it — next-index and lookup, both of
       them there for the recorder and nothing else — were refused by the gate
       before ever reaching requireIngest. The rule is the namespace: /ingest
       is authenticated by TENMA_INGEST_TOKEN rather than by a session, and
       splitting that by verb is a distinction the recorder does not make. */
    /^\/api\/ingest\//,
  ];
  const OPEN_POST = [
    /^\/api\/auth\/login$/,
    /^\/api\/auth\/logout$/,
    /^\/api\/ingest\//,
  ];

  const gateOn = () => !config.devAuth;
  /* `atLeast` and not a truthy check on req.person: ANON is an object, so
     `req.person` is always set and testing it would let everybody through. */
  const signedIn = (req) => atLeast(req.person, 'viewer') && !!req.person?.id;

  app.use((req, res, next) => {
    if (!gateOn() || signedIn(req)) return next();
    const path = req.path;
    const open = req.method === 'GET' || req.method === 'HEAD'
      ? OPEN_GET.some((re) => re.test(path))
      : req.method === 'POST' && OPEN_POST.some((re) => re.test(path));
    if (open) return next();
    /* The root gets the door rather than a refusal — there has to be somewhere
       to knock. It is served a self-contained login page, NOT the archive's
       own index: the app would boot, call a dozen endpoints, be refused by
       every one and render as something broken rather than as something
       locked. */
    if ((req.method === 'GET' || req.method === 'HEAD') && (path === '/' || path === '/index.html')) {
      return res.type('html').send(gatePage(req));
    }
    /* 404 and not 401 on a GET: "there is nothing here" and "there is
       something here you may not see" are different disclosures and only the
       first is a stranger's business. An API call gets 401, because something
       is going to read the status and should be told the truth. */
    if (path.startsWith('/api/')) {
      return res.status(401).json({ error: 'sign in first' });
    }
    /* And the door again for anything else a person could be looking at, so a
       mistyped or stale URL still shows the way in. Only for something asking
       for a page though — a stylesheet, a favicon or an image would otherwise
       each be answered with a whole HTML document nothing will render. */
    if (String(req.headers.accept ?? '').includes('text/html')) {
      return res.status(404).type('html').send(gatePage(req, 'Nothing here.'));
    }
    return res.sendStatus(404);
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
    s.chat_version, s.chat_messages, s.chat_first_ms, s.chat_last_ms,
    s.chat_moderation, s.chat_meta_path,
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

    const chatMeta = row.chat_path && row.chat_meta_path === row.chat_path
      ? { version: row.chat_version ?? null,
          messages: row.chat_messages ?? null,
          first_ms: row.chat_first_ms ?? null,
          last_ms: row.chat_last_ms ?? null,
          moderation: row.chat_moderation
            ? JSON.parse(row.chat_moderation) : null }
      : null;
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
        // video_path was missing here, and video_ok — a boolean — was standing
        // in for it. The record editor's "hosted video" box reads this key, so
        // it rendered empty on every capture that HAS a file, the browse panel
        // opened at `raws` instead of beside the file, and the save-time
        // comparison was always against ''. Nothing was wrong with the widget.
        video_path: c.video_path ?? null,
        // Same omission, different symptom: watchSources puts mirror_platform
        // on the mirror source, so without it here a mirror could never be
        // told which platform it is on, and the tile had no name.
        mirror_url: c.mirror_url, mirror_platform: c.mirror_platform ?? null,
        alive: c.alive,
      })),
      chat: {
        // Honest about which kind of "no chat" this is. A file that exists but
        // has never been imported is not the same claim as a file that never
        // existed, and the panel should say which.
        state: row.chat_state,
        // "There is a merged file and the archive has seen it on disk" — the
        // one question the panel needs answered before it chooses between
        // rendering chat and explaining why it cannot.
        //
        // It was a hardcoded `false`, left from a design where chat was read
        // INTO the database. Nothing is imported now: the file is served
        // whole, so the flag means what it always should have meant.
        imported: !!(row.chat_path && row.chat_ok),
        // The merged file, once ls-audit has built one. This is what a player
        // should load: every platform's messages in one origin-tagged file.
        merged: row.chat_path ?? null,
        merged_ok: row.chat_path ? !!row.chat_ok : null,
        // Where to fetch it. Given rather than built client-side, so the page
        // never has to know that chat lives under the media root at all —
        // and so the day chat moves, one string changes.
        url: row.chat_path ? `/media/chat/${row.id}` : null,
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
        // What the file says about itself — and only while it still describes
        // the file this row points at. Repointing chat_path leaves a count and
        // a span behind that are entirely plausible and about something else,
        // and nothing in the numbers would look wrong. null means "not known",
        // which the panel can say; a wrong number is not something it can
        // recover from.
        meta: chatMeta,
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
    /* A timeline materialised before this existed has no such key, and `?? []`
       is the difference between an older row rendering normally and the strip
       throwing on the first `.length`. It fills in on that stream's next
       recompute; scripts/rebuild.js does the lot. */
    out.segments_past = timeline.segments_past ?? [];
    out.coverage = timeline.coverage;
    out.counts = timeline.counts;
    out.segment_kinds = ALL_KINDS;   // includes the 'unknown' sentinel
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

  /* ---- notes -> a subtitle track ----------------------------------------
   *
   * ONE route, two modes, because they differ only in where the cues come
   * from and everything after that — the clock conversion, the ordering, the
   * cue lengths, the file — is identical. Two routes would be two places for
   * the same off-by-a-head-start bug.
   *
   *   with `paste`      the lines from a phone, parsed here
   *   without           the notes (or chapters) this stream already holds
   *
   * A preview and the download are the SAME call. The panel renders `cues`,
   * and the file it saves is the `srt` from the same response — so what you
   * looked at is what you got, rather than two renderings that can disagree.
   *
   * Editor and up: it reads notes that may not be published yet, and the
   * paste path is a parser somebody could otherwise probe for free.
   */
  /* ---- cut a clip out of a master ----------------------------------------
   *
   * `ls-rec clip <wall> <total>` by another road: the archive works out which
   * master covers the moment and what second inside it to start at, the Pi
   * does the ffmpeg, and the file lands in quarantine for one download.
   *
   * LEAD AND TOTAL. `total` INCLUDES the lead, which is counter-intuitive and
   * deliberate — it is what the CLI has always meant and there is no gain in
   * having two conventions. at=12:25:42, lead=60, total=120 cuts 12:24:42 to
   * 12:26:42. lead=0, total=120 cuts 120s forward from the mark.
   *
   * LOCAL MASTERS ONLY. The gate is `video_ok`, which is set from stat() and
   * never from a claim, so "there is a VOD to cut from" is a fact rather than
   * a hope. A platform VOD would mean a download and a second failure mode;
   * cliprip-studio already does that job.
   */
  app.post('/api/clips', requireRole('editor'), (req, res) => {
    const b = req.body ?? {};
    const row = R.prepare(
      `SELECT id, idx, title, started_at, tz_offset_min, duration_s
         FROM stream WHERE id = ? AND retracted_at IS NULL`).get(String(b.stream_id ?? ''));
    if (!row) return res.status(404).json({ error: 'no such stream' });

    /* Two sources, and the difference is who holds the clocks.

       A promoted master is a file on the recorder's mount, and the arithmetic
       happens HERE because this end owns the capture rows and therefore the
       only measured answer to "when was frame 0 of that file".

       The part being written right now is the other way round. Which file is
       open, what wall time its frame 0 is, and how close the live edge has
       crept live in the daemon's own memory and are not posted anywhere — so
       the job carries the moment in wall time and the recorder's own planner
       works out the rest. See _clip_live in ls_jobs.py and _plan_clip in
       ls_rec.py; that planner is also what `ls-rec clip` uses, which is what
       keeps a cut made from this panel identical to one typed at the Pi. */
    const wantLive = !!b.live;
    const rec = wantLive
      ? liveRecFor(row.id, b.platform ? String(b.platform).toUpperCase() : null)
      : null;
    if (wantLive && !rec) {
      return res.status(400).json({
        error: 'nothing is recording this stream right now — pick a master instead' });
    }

    let cap = null;
    if (!wantLive) {
      cap = R.prepare('SELECT * FROM capture WHERE id = ? AND stream_id = ?')
        .get(String(b.capture_id ?? ''), row.id);
      if (!cap) return res.status(400).json({ error: 'that capture is not on this stream' });
      if (!cap.video_ok || !cap.video_path) {
        return res.status(400).json({
          error: 'there is no local master for that capture — nothing to cut from' });
      }
    }

    const at = Math.round(Number(b.at_wall));
    if (!Number.isFinite(at)) return res.status(400).json({ error: 'at_wall must be a unix time' });
    const lead = Math.min(Math.max(Math.round(Number(b.lead_s) || 0), 0), 3600);
    const total = Math.round(Number(b.total_s));
    if (!Number.isFinite(total) || total < 1 || total > 7200) {
      return res.status(400).json({ error: 'total_s must be between 1 and 7200' });
    }
    if (total <= lead) {
      /* Otherwise the cut ends before the moment it is about, which is a
         request that cannot mean what it says. */
      return res.status(400).json({
        error: `total (${total}s) must be more than the lead (${lead}s) — the clip `
             + 'would end before the moment it is about' });
    }

    /* The seek, in the master's own seconds. local_start_wall is the wall time
       of frame 0 of OUR file and is the only thing that can answer this; when
       nothing has measured it, refuse rather than assume it equals the
       stream's zero — being wrong here is being wrong by minutes, silently,
       in a file somebody then cuts with. */
    let startIn = null;
    if (!wantLive) {
      const zero = clocksOf(cap, row.started_at).local;
      if (zero === null) {
        return res.status(400).json({
          error: "that master's start has never been measured, so a second inside "
               + 'it cannot be worked out. Set local_start_wall on the record first.' });
      }
      startIn = (at - lead) - zero;
      if (startIn < 0) {
        return res.status(400).json({
          error: `that starts ${Math.abs(Math.round(startIn))}s before the file does — `
               + 'use a shorter lead' });
      }
      /* Checked when the duration is known, skipped when it is not: a master
         that has never been probed is still cuttable, and the Pi will say so if
         the range runs off the end. */
      if (cap.file_duration_s && startIn >= cap.file_duration_s) {
        return res.status(400).json({
          error: `that starts past the end of the file (${hms(cap.file_duration_s)})` });
      }
    }

    /* The note, ONLY when the moment was typed. Cutting around a note that
       already exists and then writing a second one at the same second is two
       records of one fact, and the list is the thing that suffers. */
    const fromNote = String(b.note_id ?? '').trim() || null;
    let noteId = null;
    if (!fromNote) {
      const text = String(b.note_text ?? '').trim().slice(0, 2000)
        /* A placeholder rather than an empty note: the row exists to say a cut
           was made here, and a blank one says nothing at all. */
        || `clipped ${hms(total)} from here`;
      noteId = ulid();
      try {
        propose(W, {
          authorId: req.person?.id ?? null, person: req.person,
          reason: `noted a clip at ${hms(Math.max(0, at - row.started_at))}`,
          changes: [
            { target_type: 'note', target_id: noteId, op: 'create', field: 'stream_id', value: row.id },
            { target_type: 'note', target_id: noteId, op: 'create', field: 'text', value: text },
            { target_type: 'note', target_id: noteId, op: 'create', field: 'offset_s',
              value: Math.max(0, at - row.started_at) },
            { target_type: 'note', target_id: noteId, op: 'create', field: 'frame', value: 'stream' },
            { target_type: 'note', target_id: noteId, op: 'create', field: 'tag', value: 'clip' },
            /* Ticked on arrival, unlike every other #clip note: those mean
               "cut this", and by the time this row exists the cut is queued. */
            { target_type: 'note', target_id: noteId, op: 'create', field: 'done', value: 1 },
            { target_type: 'note', target_id: noteId, op: 'create', field: 'ord',
              value: (R.prepare('SELECT COALESCE(MAX(ord),0) m FROM note WHERE stream_id = ?')
                .get(row.id).m ?? 0) + 1 },
          ],
        });
      } catch (e) {
        /* The clip is the point; the note is bookkeeping. A note that will not
           write must not cost somebody their cut. */
        console.error('clip note failed:', e?.message ?? e);
        noteId = null;
      }
    }

    const id = enqueueJob('clip', { by: req.person.id, payload: {
      /* Everything the Pi needs and nothing it has to look up. On the master
         path `path` is relative to the media root exactly as it is stored, so
         the recorder joins it to its own mount rather than trusting a path
         from here; on the live path there is no path to send at all, and
         `live` is what tells the worker to go and ask the daemon. */
      ...(wantLive
        ? { live: true,
            /* The label the recorder puts in the clip's own filename. Not the
               same field as `name` below: that one is what the archive will
               serve this as, and this one is what it is called on the Pi. */
            label: String(b.note_text ?? '').trim().slice(0, 60) || null }
        : { path: cap.video_path, start_s: Math.round(startIn) }),
      duration_s: total,
      /* Both, and for different readers. `stream_idx` is what a person calls
         this broadcast and is what the queue row shows; `stream_id` is the
         key, carried so anything reading this job back can find the row —
         `idx` is a mutable label and nullable, and a queue that resolves one
         by it would be looking up a name. */
      stream_id: row.id,
      stream_idx: row.idx ?? null,
      platform: wantLive ? rec.platform : cap.platform,
      at_wall: at,
      lead_s: lead,
      /* What to call it. The Pi writes into quarantine under this name; the
         archive then serves it once and removes it. */
      name: `${slugify(`${row.idx ?? 'clip'} ${row.title ?? ''}`).slice(0, 48)}`
          + `-${hms(Math.max(0, (at - lead) - row.started_at)).replaceAll(':', '')}`
          + `-${total}s.mp4`,
    } });
    logEvent(req, `asked for a clip to be cut${wantLive ? ' from the live recording' : ''}`,
             'stream', row.id,
             { job_id: id, start_s: startIn === null ? null : Math.round(startIn),
               total_s: total, lead_s: lead, live: wantLive });
    bumpGeneration(W);
    res.status(201).json({ job_id: id, note_id: noteId, live: wantLive,
                           platform: wantLive ? rec.platform : cap.platform,
                           /* Null on a live cut and that is the honest answer:
                              the offset into the file is the recorder's to
                              work out, and it has not been asked yet. */
                           start_s: startIn === null ? null : Math.round(startIn),
                           total_s: total });
  });

  /* The one download, and then it is gone.
   *
   * Quarantine is the only place this container may write, which is why the
   * clip lands there — and why this may delete it afterwards. Removed only
   * after the response has actually finished: a send that fails halfway must
   * leave the file to be asked for again, or a dropped connection costs you a
   * re-cut.
   */
  app.get('/api/clips/:id/file', requireRole('editor'), (req, res) => {
    const j = R.prepare("SELECT * FROM job WHERE id = ? AND kind = 'clip'").get(req.params.id);
    if (!j) return res.status(404).json({ error: 'no such clip job' });
    if (j.status !== 'done') {
      return res.status(409).json({ error: `that clip is ${j.status}`, status: j.status });
    }
    if (!j.result_path) return res.status(410).json({ error: 'the recorder kept no file' });
    if (!config.quarantineRoot) return res.status(503).json({ error: 'no quarantine root is set' });
    const abs = resolveMedia(config.quarantineRoot, j.result_path);
    if (!abs || !existsSync(abs)) {
      return res.status(410).json({ error: 'that clip has already been downloaded' });
    }
    const name = (JSON.parse(j.payload ?? '{}').name) || 'clip.mp4';
    res.type('video/mp4');
    res.setHeader('content-disposition', `attachment; filename="${name.replace(/"/g, '')}"`);
    res.sendFile(abs, (err) => {
      if (err) return;                  // left in place on purpose — see above
      try { rmSync(abs, { force: true }); } catch { /* already gone */ }
      W.prepare("UPDATE job SET result_path = NULL, updated_at = ? WHERE id = ?")
        .run(now(), j.id);
      bumpGeneration(W);
    });
  });

  app.post('/api/notes/srt', requireRole('editor'), (req, res) => {
    const b = req.body ?? {};
    const row = R.prepare(
      `SELECT id, idx, title, started_at, tz_offset_min, duration_s
         FROM stream WHERE id = ? AND retracted_at IS NULL`).get(String(b.stream_id ?? ''));
    if (!row) return res.status(404).json({ error: 'no such stream' });

    const caps = R.prepare('SELECT * FROM capture WHERE stream_id = ? ORDER BY platform')
      .all(row.id);
    const capsById = new Map(caps.map((c) => [c.id, c]));

    /* `axis` means the stream's own timeline, which is what you want when the
       thing being cut is itself the archive's zero. Anything else is
       `<capture_id>:<clock>`, the same key the note editor's picker uses — so
       the two surfaces cannot drift into naming clocks differently. */
    const want = String(b.clock ?? 'axis');
    let target = null;
    if (want !== 'axis') {
      const [capId, clock] = want.split(':');
      const cap = capsById.get(capId);
      if (!cap) return res.status(400).json({ error: 'that capture is not on this stream' });
      if (clock !== 'remote' && clock !== 'local') {
        return res.status(400).json({ error: 'clock must be remote or local' });
      }
      /* An unmeasured clock is refused rather than approximated. A local file
         whose start was never measured cannot place a cue inside it, and a
         track that is silently wrong by minutes is worse than no track. */
      if (axisToPosition(cap, row.started_at, clock, 0) === null) {
        return res.status(400).json({
          error: `that capture's ${clock} clock has never been measured, so a `
               + 'position inside it cannot be worked out' });
      }
      target = { cap, clock };
    }

    const onto = (axis) => (target
      ? axisToPosition(target.cap, row.started_at, target.clock, axis) : axis);

    const prefix = String(b.prefix ?? '').slice(0, 40);
    const gap = b.gap === true;      // fixed length unless asked otherwise
    const dur = Math.min(Math.max(Number(b.dur) || 10, 1), 600);

    /* Where a cue has to land to be believable. A mis-read date is the failure
       this catches: `21.09` can only be day-first, and on a 9 September stream
       that is twelve days out — a cue at hour 288, which an NLE accepts and
       draws nowhere. Reported as out of range rather than written. */
    const span = row.duration_s ?? 86400;
    const plausible = (axis) => axis !== null && axis >= -300 && axis <= span + 600;

    const cues = [];
    const skipped = [];
    if (typeof b.paste === 'string' && b.paste.trim()) {
      const dayFirst = b.day_first === true ? true : b.day_first === false ? false : null;
      for (const raw of b.paste.split(/\r?\n/)) {
        const line = raw.trim();
        if (!line) continue;
        const hit = parsePhoneLine(line, {
          near: row.started_at, tzOffsetMin: row.tz_offset_min ?? 0, dayFirst });
        if (!hit || !hit.text) { skipped.push({ line, why: 'no timestamp in it' }); continue; }
        const axis = hit.unix - row.started_at;
        if (!plausible(axis)) {
          skipped.push({ line, why: `lands at ${hms(Math.round(axis))}, outside this stream` });
          continue;
        }
        /* The wall time, in the archive's own notation, so an imported note
           can keep it in `stamp` — the column that exists for a typed
           expression saying more than offset_s can hold. `raw` would be the
           obvious home for the line, and it is deliberately NOT writable
           through a changeset: it is the vault importer's receipt and nothing
           may edit one. `stamp` is better anyway, because `11:06:54 wall`
           re-parses through parseNoteLine and converts back to this same
           second instead of being read as a position. */
        const sod = (((hit.unix + (row.tz_offset_min ?? 0) * 60) % 86400) + 86400) % 86400;
        const two = (n) => String(n).padStart(2, '0');
        const stamp = `${two(Math.floor(sod / 3600))}:${two(Math.floor(sod % 3600 / 60))}`
                    + `:${two(sod % 60)} wall`;
        cues.push({ axis, at: onto(axis), text: prefix + hit.text, raw: line, stamp });
      }
    } else {
      const chapters = String(b.source ?? 'notes') === 'chapters';
      const rows = chapters
        ? R.prepare(`SELECT * FROM segment WHERE stream_id = ? AND retracted_at IS NULL`)
          .all(row.id)
        : R.prepare(`SELECT * FROM note WHERE stream_id = ? AND retracted_at IS NULL`)
          .all(row.id);
      for (const r of rows) {
        const axis = axisOf(r, capsById, row.started_at,
                            chapters ? 'start_s' : 'offset_s');
        /* A note with no timestamp is a thought, not a cue. Counted so the
           panel can say "9 of 14 had a time on them" rather than quietly
           producing a shorter track than the list it came from. */
        if (axis === null) {
          skipped.push({ line: r.text || r.label || '(untitled)', why: 'no timestamp on it' });
          continue;
        }
        const tag = !chapters && r.tag
          ? `[${r.tag}${r.seq === null || r.seq === undefined
                ? '' : ' ' + String(r.seq).padStart(2, '0')}] ` : '';
        const text = chapters ? (r.label || r.kind || 'unnamed') : (r.text || '');
        if (!text.trim()) { skipped.push({ line: '(no words)', why: 'nothing to show' }); continue; }
        cues.push({ axis, at: onto(axis), text: prefix + tag + text, raw: null });
      }
    }

    const usable = cues.filter((c) => c.at !== null);
    const srt = buildSrt(usable, { gap, dur });
    /* Named after the stream and the clock, because these end up in a download
       folder next to each other and `notes.srt` four times over is how you cut
       with the wrong one. */
    const slugged = slugify(`${row.idx ?? ''} ${row.title ?? 'stream'}`).slice(0, 60);
    res.json({
      stream: { id: row.id, idx: row.idx, title: row.title,
                started_at: row.started_at, duration_s: row.duration_s },
      clock: want,
      cues: usable.sort((a, b2) => a.at - b2.at)
        .map((c) => ({ at: c.at, axis: c.axis, text: c.text,
                       raw: c.raw, stamp: c.stamp ?? null })),
      skipped,
      srt,
      name: `${slugged || 'notes'}-${want === 'axis' ? 'timeline' : want.split(':')[1]}.srt`,
    });
  });

  app.get('/api/streams/:id', (req, res) => {
    const etag = etagFor('s', req.params.id, req.query.rail);
    if (fresh(req, res, etag)) return res.status(304).end();
    return oneStream(res, 's.id = ?', req.params.id, req);
  });

  /** The vocabulary. `q=` makes it the autocomplete behind "type a game name":
   *  prefix-first, so typing "mario" ranks Mario Kart above Super Mario. */
  /* What sits INSIDE a block carrying a tag.
   *
   * Written once and used by both the count and the list, because the two
   * disagreeing is the failure this file has already had several times: a tab
   * that says 2 over a list of 5 is worse than either number alone.
   *
   * `host.end_s` is NULL when a chapter runs to the next one — the schema says
   * so on the column — so the bound is the next lane-0 start, then the stream's
   * duration, then a sentinel. Same resolution projectSegments does, in SQL. */
  const insideFrom = ({ tag, withStream = false }) => `segment sub
      JOIN segment host ON host.stream_id = sub.stream_id AND host.lane = 0
           AND host.retracted_at IS NULL AND host.tag_id = ${tag}
      ${withStream ? 'JOIN stream s ON s.id = sub.stream_id' : ''}
      WHERE sub.lane = 1 AND sub.retracted_at IS NULL
        ${withStream ? 'AND s.retracted_at IS NULL' : ''}
        AND sub.start_s >= host.start_s
        AND sub.start_s < COALESCE(host.end_s,
              (SELECT MIN(n.start_s) FROM segment n
                WHERE n.stream_id = host.stream_id AND n.lane = 0
                  AND n.retracted_at IS NULL AND n.start_s > host.start_s),
              (SELECT st.duration_s FROM stream st WHERE st.id = host.stream_id),
              1000000000)`;

  app.get('/api/tags', (req, res) => {
    const q = String(req.query.q ?? '').trim().toLowerCase();
    const limit = Math.min(Math.max(Number(req.query.limit ?? 500) || 500, 1), 500);
    // Proposed tags are hidden from the picker by default — that is the whole
    // point of the status — but an editor reviewing the queue needs to see them.
    const wantProposed = String(req.query.status ?? '') === 'all'
      && atLeast(req.person, 'editor');

    /* ?retracted=1 — the tombstones, and ONLY the tombstones.
    
       Admin-only, matching the capability that can destroy one: the reason to
       look at this list is to decide what to purge, and purge is a
       `tag.purge`. An editor has no use for it, and a stranger even less.
    
       It exists because a retracted tag is not gone and cannot be: UNIQUE(slug)
       is table-wide, so a tombstone keeps owning its name forever and nothing
       else can be minted under it. Before this there was no surface anywhere
       that would admit those rows existed — which is exactly how a mint could
       report success and produce nothing visible. */
    const wantDead = String(req.query.retracted ?? '') === '1'
      && can(req.person, 'tag.purge');

    const where = [wantDead ? 't.retracted_at IS NOT NULL' : 't.retracted_at IS NULL'];
    const params = [];
    // A tombstone's status is whatever it was when it died and is not a filter
    // anybody wants applied to a list of things to destroy.
    if (!wantProposed && !wantDead) where.push(`t.status = 'confirmed'`);
    /* `?surface=` was here, and filtered the vocabulary down to the kinds
       that surface was allowed to offer. It is gone: one vocabulary, searched
       whole, everywhere. The parameter is still ACCEPTED and ignored rather
       than rejected, because a browser holding a cached copy of the old page
       will keep sending it for as long as its cache lives, and answering that
       with a 400 turns a stale tab into a tag picker that returns nothing. */
    if (q) {
      where.push('(t.slug LIKE ? OR lower(t.name) LIKE ?)');
      params.push(`%${q}%`, `%${q}%`);
    }
    const rows = R.prepare(
      /* The snippet count comes back HERE rather than from a second call when
         a row opens. It is one correlated subquery over an indexed junction,
         and the alternative is that the retract warning cannot say how much it
         is about to touch — which is the one place a vague warning costs
         something. */
      `SELECT t.id, t.slug, t.name, t.kind, t.thumb_path, t.summary,
              t.status, t.parent_id, t.seed_url, t.seeded, t.gate, t.retracted_at,
              p.name AS parent_name, p.slug AS parent_slug,
              COUNT(DISTINCT st.stream_id) n,
              (SELECT COUNT(*) FROM snippet_taglet sl JOIN snippet sn ON sn.id = sl.snippet_id
                WHERE sl.tag_id = t.id AND sn.retracted_at IS NULL) snippets,
              (SELECT COUNT(*) FROM music_tag mt JOIN music mu ON mu.id = mt.music_id
                WHERE mt.tag_id = t.id AND mu.retracted_at IS NULL) songs,
              (SELECT COUNT(*) FROM segment g
                WHERE g.tag_id = t.id AND g.retracted_at IS NULL) blocks,
              (SELECT COUNT(*) FROM ${insideFrom({ tag: 't.id' })}) inside
       FROM tag t
       LEFT JOIN stream_tag st ON st.tag_id = t.id
       LEFT JOIN tag p ON p.id = t.parent_id
       WHERE ${where.join(' AND ')}
       GROUP BY t.id
       ORDER BY ${q ? `(CASE WHEN t.slug LIKE ? THEN 0 ELSE 1 END),` : ''}
                ${wantDead ? 't.retracted_at DESC,' : ''} n DESC, t.name
       LIMIT ?`).all(...params, ...(q ? [`${q}%`] : []), limit);

    res.json({ tags: rows.map((t) => ({
      id: t.id, slug: t.slug, name: t.name, kind: t.kind,
      // A child inherits its parent's art rather than needing its own copy —
      // the reason parent_id exists at all.
      thumb: t.thumb_path ? `/media/thumb/${t.thumb_path}` : null,
      summary: t.summary, status: t.status,
      seed_url: t.seed_url ?? null,
      // 1 only while the description and art are still exactly what a harvest
      // wrote. Any human edit clears it in the applier.
      seeded: t.seeded === 1,
      gate: t.gate ?? null,
      /* Only ever set on the ?retracted=1 list, where it is the column the
         panel sorts by — what died most recently is what you are most likely
         to have meant to bring back. */
      retracted_at: t.retracted_at ?? null,
      parent: t.parent_id ? { id: t.parent_id, name: t.parent_name, slug: t.parent_slug } : null,
      streams: t.n, snippets: t.snippets, songs: t.songs, blocks: t.blocks,
      inside: t.inside,
    })) });
  });

  /* What is actually under a tag — the three lists the expanded row shows.
   *
   * Lazy, on expand, and not part of /api/tags: the counts are cheap enough to
   * carry for 222 rows and the CONTENTS are not. One tag opening at a time is
   * one query; the same rows folded into the list payload would be 222 of
   * them for a screen that shows one.
   *
   * Snippets go through the same visibility rule the snippet list uses, so a
   * gated clip does not become visible by being tagged. That is the whole
   * reason this is not a naive join.
   */
  app.get('/api/tags/:id/uses', (req, res) => {
    const t = R.prepare('SELECT id FROM tag WHERE id = ? AND retracted_at IS NULL')
      .get(req.params.id);
    if (!t) return res.status(404).json({ error: 'no such tag' });
    const lim = Math.min(Math.max(Number(req.query.limit ?? 40) || 40, 1), 200);

    const streams = R.prepare(
      `SELECT s.id, s.idx, s.title, s.local_date AS date, s.duration_s, s.timeline_json
         FROM stream s JOIN stream_tag st ON st.stream_id = s.id
        WHERE st.tag_id = ? AND s.retracted_at IS NULL
        ORDER BY s.started_at DESC LIMIT ?`).all(t.id, lim);

    const me = req.person?.id ?? null;
    const vis = snipVisibleSql(req);
    const gate = gateSql(req);
    const snippets = R.prepare(
      `SELECT s.id, s.title, s.duration_s, s.status
         FROM snippet s JOIN snippet_taglet sl ON sl.snippet_id = s.id
        WHERE sl.tag_id = ? AND s.retracted_at IS NULL AND ${vis.sql}
          ${gate.sql ? `AND ${gate.sql}` : ''}
        ORDER BY s.created_at DESC LIMIT ?`)
      .all(t.id, ...vis.params, ...gate.params, lim);

    /* The sub-chapters, not the block itself. Listing the block that carries
       the tag tells you what you already clicked on — what is worth reading is
       what happened inside it. */
    const inside = R.prepare(
      `SELECT sub.id, sub.start_s, sub.end_s, sub.label, sub.kind, sub.stream_id,
              s.idx, host.label AS under
         FROM ${insideFrom({ tag: '?', withStream: true })}
        ORDER BY s.started_at DESC, sub.start_s LIMIT ?`).all(t.id, lim);

    res.json({
      streams: streams.map((r) => ({
        id: r.id, idx: r.idx, title: r.title, date: r.date, duration_s: r.duration_s,
        /* The coloured strip, already computed. buildTimeline's projection is
           stored on the row, so drawing a stream's shape here costs a JSON
           parse rather than a rebuild. */
        strip: stripOf(r.timeline_json),
      })),
      snippets, inside,
    });
  });

  /* The projected timeline, reduced to what a 100px bar needs: a kind and a
     share of the width. Anything the projection could not place is skipped —
     a strip is a summary, and a summary that invents coverage is worse than a
     shorter one. */
  function stripOf(json) {
    if (!json) return [];
    let tl; try { tl = JSON.parse(json); } catch { return []; }
    const segs = (tl?.segments ?? []).filter(
      (x) => Number.isFinite(x.start_s) && Number.isFinite(x.end_s) && x.end_s > x.start_s
        && (x.lane ?? 0) !== 1);
    const span = segs.length ? Math.max(...segs.map((x) => x.end_s)) : 0;
    if (!span) return [];
    return segs.map((x) => ({ kind: x.kind ?? 'unknown',
      pct: Math.max(0.5, ((x.end_s - x.start_s) / span) * 100) }));
  }

  app.get('/api/months', (req, res) => {
    res.json({ months: R.prepare(
      `SELECT local_month AS month, COUNT(*) streams,
              SUM(COALESCE(duration_s, 0)) seconds FROM stream
       WHERE retracted_at IS NULL GROUP BY local_month ORDER BY month DESC`).all() });
  });

  app.get('/api/health', (req, res) => {
    /* Open to anybody, because the container's own healthcheck runs with no
       session and a gate that refused it would restart the container forever.
       But what it ANSWERS with is a different question: everything below is a
       census of the collection — how many streams, how many notes, how much is
       still uncorrected — and a stranger who can read that knows the size and
       shape of the archive without being allowed to see any of it. Docker only
       ever looks at the status code. */
    if (!atLeast(req.person, 'viewer') || !req.person?.id) {
      return res.json({ ok: true });
    }
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
               // So the page can tell "there is nothing here" from "there is
               // something here you may not see" without probing for it.
               grants: R.prepare('SELECT name FROM person_grant WHERE person_id = ?')
                 .all(p.id ?? '').map((r) => r.name),
               // So the UI knows whether to offer the dev sign-in at all,
               // rather than probing a 404 to find out.
               dev_auth: !!config.devAuth,
               /* And whether it is behind the password gate, which is what
                  makes signing OUT meaningful: with the gate on, the session
                  is the only reason any of this rendered, so the button has
                  somewhere to go and the "read-only, there is no sign-in yet"
                  note is no longer the truth. */
               gate: !config.devAuth,
               /* The channels a Discord link may come from, by the names YOU
                  gave them. Sent so the upload window can say which ones
                  rather than letting somebody paste a link and find out with
                  a 403 — the ids stay here, because a list of channel ids is
                  not something a reader needs and not something worth
                  handing out.
                  Nor are the names, to a stranger. This route is on the gate's
                  open list so the login page can tell a locked door from a
                  broken server, and that made a list of private channel names
                  readable by anybody who asked. Only somebody who can actually
                  upload needs it. */
               discord: p.id && atLeast(p, 'suggester')
                 ? [...discordChannels().values()] : [] });
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

  /* ---- the password ------------------------------------------------------
   *
   * The door in the gate at the top of this file. One secret, one account, no
   * registration, no reset, no second factor, nothing to administer — real
   * accounts replace this rather than extend it.
   *
   * Secure follows the SCHEME rather than being hardcoded on. Set
   * unconditionally, the browser drops the cookie over plain http, which is
   * how the LAN reaches this today — and the symptom is a password that is
   * accepted and a sign-in that never happens.
   */
  const isHttps = (req) =>
    String(req.headers['x-forwarded-proto'] || req.protocol || '')
      .split(',')[0].trim().toLowerCase() === 'https';

  /* One secret and an unlimited guess rate is not one secret.
   *
   * Counted per caller AND globally, because behind a tunnel there is only one
   * caller: cloudflared dials this container from the docker network, so every
   * request on earth arrives from the same address and per-IP counting
   * silently becomes a single bucket for the whole internet. The global cap is
   * the one that actually holds there, and on a site with exactly one account
   * it costs nothing — the only person it can lock out is you, for a quarter
   * of an hour, after five wrong guesses.
   */
  const LOGIN_TRIES = 5, LOGIN_WINDOW_S = 900;
  const loginMiss = new Map();          // key -> { n, until }

  const loginBlocked = (key) => {
    const t = now();
    for (const [k, v] of loginMiss) if (v.until <= t) loginMiss.delete(k);
    const mine = loginMiss.get(key), all = loginMiss.get('*');
    return !!(mine && mine.n >= LOGIN_TRIES)
        || !!(all && all.n >= LOGIN_TRIES * 4);
  };
  const loginMissed = (key) => {
    const t = now();
    for (const k of [key, '*']) {
      const v = loginMiss.get(k);
      loginMiss.set(k, { n: (v ? v.n : 0) + 1, until: t + LOGIN_WINDOW_S });
    }
  };

  /* Hashed to a fixed width before comparing, which is what makes
     timingSafeEqual usable at all: it THROWS on a length mismatch, so handing
     it the raw strings would turn a wrong-length guess into a 500 and leak the
     length of the secret through the status code. */
  const sameSecret = (a, b) => {
    if (!a || !b) return false;
    const h = (s) => createHash('sha256').update(String(s)).digest();
    return timingSafeEqual(h(a), h(b));
  };

  app.post('/api/auth/login', (req, res) => {
    /* Unset is not open. A container that has just had dev auth deleted and
       has not been given a secret yet has no way in at all, which is the
       correct state for it to be in. */
    if (!config.adminPass) {
      return res.status(503).json({ error: 'no password is set on this server' });
    }
    const key = String(req.ip || req.socket?.remoteAddress || '?');
    if (loginBlocked(key)) {
      res.set('Retry-After', String(LOGIN_WINDOW_S));
      return res.status(429).json({ error: 'too many attempts — try again later' });
    }
    if (!sameSecret(String(req.body?.password ?? ''), config.adminPass)) {
      loginMissed(key);
      // One answer for empty, wrong and too short. There is nothing here worth
      // learning and no way to find out which of the three it was.
      return res.status(401).json({ error: 'that is not the password' });
    }
    loginMiss.delete(key); loginMiss.delete('*');

    /* The account named by TENMA_ADMIN_HANDLE when it already exists — see the
       config note; adopting the row is what keeps authorship intact. An admin
       row wins over a plain one of the same name and the oldest wins after
       that, so this resolves to the same person every time. */
    let id = R.prepare(`SELECT id FROM person WHERE handle = ?
                         ORDER BY (role = 'admin') DESC, created_at ASC LIMIT 1`)
      .get(config.adminHandle)?.id;
    if (!id) {
      id = upsertPerson(W, { provider: 'pass', providerUid: config.adminHandle,
                             handle: config.adminHandle,
                             displayName: config.adminHandle,
                             defaultRole: 'admin' });
    }
    /* Forced, both of them. The role because an adopted row may have been
       anything; `banned` because identify() reads a banned person as ANON, and
       a correct password that still does not let you in is the worst failure
       this route has — there is no second account to unban you with. */
    W.prepare('UPDATE person SET role = ?, banned = 0 WHERE id = ?').run('admin', id);
    const { token, expiresAt } = issueSession(W, id, String(req.headers['user-agent'] ?? ''));
    res.set('Set-Cookie', `${COOKIE}=${token}; HttpOnly; SameSite=Lax; Path=/`
      + `; Max-Age=${TTL}` + (isHttps(req) ? '; Secure' : ''));
    res.json({ ok: true, id, handle: config.adminHandle, role: 'admin',
               expires_at: expiresAt });
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

  /* An uploaded poster that a change has just stopped pointing at.
   *
   *  Only ever called for a changeset that has APPLIED. A suggester's clear is
   *  queued, and a queued clear that had already deleted the file would be a
   *  refusal that came too late to refuse anything.
   *
   *  Two homes, two disposals. Still in quarantine means the Pi has not taken
   *  it yet and the archive can simply unlink it — quarantine is one of the two
   *  roots it holds a write handle for. Already promoted means it is in the
   *  media tree, which is mounted read-only here by design, so the only way to
   *  remove it is to write the job down and let the recorder come and take it.
   *
   *  Scoped to `posters/` and to a minted name. A thumb_path someone typed in
   *  pointing at a file that was already in the archive is not ours to purge,
   *  and this is the check that keeps a clear from deleting one.
   */
  const POSTER_REL = /^posters\/([0-9A-HJKMNP-TV-Z]{26})\.png$/;

  function posterAftercare(csId, by) {
    let rows;
    try {
      rows = R.prepare(
        `SELECT field, value, base_value FROM change
          WHERE changeset_id = ? AND target_type IN ('stream', 'tag')
            AND field = 'thumb_path'`)
        .all(csId);
    } catch { return; }
    for (const r of rows) {
      const was = r.base_value;
      if (!was || was === r.value) continue;
      const m = POSTER_REL.exec(was);
      if (!m) continue;
      // Still waiting for the Pi: drop it where it stands and there is nothing
      // for anyone to come and collect.
      const qRel = `${m[1]}.png`;
      const qAbs = config.quarantineRoot ? join(config.quarantineRoot, qRel) : null;
      if (qAbs && existsSync(qAbs)) {
        try { rmSync(qAbs, { force: true }); } catch { /* already gone */ }
        W.prepare(
          `UPDATE job SET status = 'cancelled', error = 'the poster was cleared before it moved',
                          finished_at = ?, updated_at = ?
            WHERE kind = 'promote' AND snippet_id IS NULL
              AND status IN ('approved', 'proposed') AND payload LIKE ?`)
          .run(now(), now(), `%"${qRel}"%`);
        continue;
      }
      // In the media tree. The archive cannot reach it; the recorder can.
      enqueueJob('purge', { payload: { path: was }, by });
    }
    bumpGeneration(W);
  }

  /** A row that changed collections, and the destination that has to follow.
   *
   *  `kind` is writable so that a screenshot filed as a meme can be moved to
   *  the gallery by whoever notices — which is the right affordance and also
   *  the one that can leave `file_path` naming the folder it USED to be
   *  going to.
   *
   *  Only while it is still in quarantine, and that is the whole design. Then
   *  `file_path` is a record of where the file WILL go and nothing has read it
   *  yet, so rewriting it costs nothing and the promote job — which is built
   *  from the column at accept time — files it correctly first time. Once the
   *  recorder has moved it, `file_path` names a real file on a read-only
   *  mount, so it is left exactly as it is: the master keeps living in the
   *  folder it was filed into, the row keeps resolving, and only the folder
   *  disagrees with the panel. Chasing that would mean a move job for a
   *  cosmetic mismatch, on the one tree the archive is deliberately unable to
   *  write to.
   */
  function kindAftercare(csId) {
    let rows;
    try {
      rows = R.prepare(
        `SELECT target_id, value FROM change
          WHERE changeset_id = ? AND target_type = 'snippet' AND field = 'kind'`)
        .all(csId);
    } catch { return; }
    for (const r of rows) {
      const dir = KIND_DIR[String(r.value ?? '')];
      if (!dir) continue;
      const s = R.prepare(
        'SELECT quarantine_path, file_path FROM snippet WHERE id = ?').get(r.target_id);
      // Already filed by the recorder, or never had a file to file.
      if (!s?.quarantine_path || !s.file_path) continue;
      const name = s.file_path.split('/').pop();
      const want = `${dir}/${name}`;
      if (want === s.file_path) continue;
      W.prepare('UPDATE snippet SET file_path = ?, updated_at = ? WHERE id = ?')
        .run(want, now(), r.target_id);
    }
  }

  app.post('/api/changesets', requireRole('suggester'), (req, res) => {
    /* The only way any decision in the archive changes — and therefore the
       only place per-change authorisation has to happen. `person` carries both
       questions into propose(): whether these particular changes are theirs to
       make, and whether the result waits for review. Neither is asked here,
       because a rule written at a route is a rule the next route forgets. */
    try {
      const out = propose(W, {
        authorId: req.person.id,
        person: req.person,
        reason: req.body?.reason ?? null,
        changes: req.body?.changes ?? [],
        mediaRoot: config.mediaRoot,
      });
      /* Only once it has actually landed. A suggester's changeset is queued
         rather than applied, and "sparklemuffin tagged this" is a lie until a
         reviewer says yes — the proposal is already recorded, as a changeset,
         which is the right place for a thing that has not happened yet. */
      if (out?.status === 'applied') {
        logChangeset(req, out.id);
        posterAftercare(out.id, req.person.id);
        kindAftercare(out.id);
      }
      res.json(out);
    } catch (e) { return changeError(res, e); }
  });

  app.get('/api/changesets', requireCap('review.read'), (req, res) => {
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

    /* What every id in those rows is CALLED.
     *
     * A queue that renders "create stream_tag 01K3F… with tag_id 01K9R…" is a
     * queue nobody can review — the reviewer would have to open three other
     * screens to find out what they are being asked about. Resolved here, in
     * one pass over one set of ids, rather than by the client fetching each
     * one: the rows are already in hand and the names are three indexed
     * lookups.
     *
     * A tag minted INSIDE one of these changesets has no row yet, so it will
     * not be found — that is correct and the client fills it from the change
     * rows themselves, which are the only place that name exists so far. */
    const want = { tag: new Set(), stream: new Set(), snippet: new Set() };
    for (const cs of rows) {
      for (const c of cs.changes) {
        if (want[c.target_type]) want[c.target_type].add(c.target_id);
        // A junction says which thing and which tag in two separate rows.
        if (c.field === 'tag_id') for (const v of [c.value, c.base_value]) if (v) want.tag.add(v);
        if (c.field === 'stream_id') for (const v of [c.value, c.base_value]) if (v) want.stream.add(v);
        if (c.field === 'snippet_id') for (const v of [c.value, c.base_value]) if (v) want.snippet.add(v);
      }
    }
    const names = {};
    const fill = (table, ids, sql) => {
      if (!ids.size) return;
      const list = [...ids];
      for (const r of R.prepare(
        `${sql} WHERE id IN (${list.map(() => '?').join(',')})`).all(...list)) {
        names[r.id] = r.label;
      }
    };
    fill('tag', want.tag, 'SELECT id, name AS label FROM tag');
    fill('stream', want.stream,
         `SELECT id, COALESCE('#' || idx || ' ' || title, title) AS label FROM stream`);
    fill('snippet', want.snippet, 'SELECT id, title AS label FROM snippet');

    res.json({ changesets: rows, count: rows.length, names });
  });

  app.get('/api/changesets/:id', requireCap('review.read'), (req, res) => {
    try { res.json(summary(R, req.params.id)); }
    catch (e) { return changeError(res, e); }
  });

  app.post('/api/changesets/:id/review', requireCap('review.decide'), (req, res) => {
    const { decision, note = null, force = false } = req.body ?? {};
    if (!['approve', 'reject'].includes(decision)) {
      return res.status(400).json({ error: 'decision must be approve or reject' });
    }
    try {
      if (decision !== 'approve') {
        return res.json(reject(W, req.params.id, { reviewerId: req.person.id, note }));
      }
      const out = apply(W, req.params.id, { reviewerId: req.person.id, note, force: !!force,
                                            mediaRoot: config.mediaRoot });
      /* Narrated here too. It was not, and the gap showed: a change an editor
         made themselves appeared in the log, while the same change arriving as
         somebody's accepted suggestion did not — so the log quietly recorded
         only the half of the archive's decisions that skipped review. */
      logChangeset(req, req.params.id);
      // The second apply site. A suggester's clear reaches disk HERE and
      // nowhere else, which is the whole point of doing this on apply.
      posterAftercare(req.params.id, req.person.id);
      kindAftercare(req.params.id);
      return res.json(out);
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
                         'retracted_at', 'chat_path', 'chat_sources',
                         // Read back so ls-audit's diff sees them as `same` on
                         // a re-run. A field the archive accepts but will not
                         // show is a field that collides on every sweep,
                         // forever, and asks a human about it every time.
                         'chat_version', 'chat_messages', 'chat_first_ms',
                         'chat_last_ms', 'chat_moderation'];

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
    // chat_meta_path is deliberately absent: it is derived from the path in
    // the same packet, so a caller cannot claim that a description belongs to
    // a file it does not describe.
    'chat_version', 'chat_messages', 'chat_first_ms', 'chat_last_ms',
    'chat_moderation',
  ]);
  const CHAT_META = ['chat_version', 'chat_messages', 'chat_first_ms',
                     'chat_last_ms', 'chat_moderation'];
  const MODERATION = new Set(['complete', 'none', 'unknown']);

  /** Moderation coverage as one canonical string, or null if it is not one.
   *
   *  Canonical because it is COMPARED, not merely stored. ls-audit reads the
   *  stream back before every write and skips whatever already matches, and
   *  that comparison is a string one — so {"YT":"a","TW":"b"} and
   *  {"TW":"b","YT":"a"} being two spellings of one fact would make this field
   *  collide on every sweep for the rest of the project's life. Sorted keys,
   *  no spaces, and the same shape produced on the ls-audit side.
   */
  const canonModeration = (v) => {
    let o = v;
    if (typeof o === 'string') { try { o = JSON.parse(o); } catch { return null; } }
    if (!o || typeof o !== 'object' || Array.isArray(o)) return null;
    const keys = Object.keys(o).sort();
    if (!keys.length) return null;
    for (const k of keys) {
      if (!['YT', 'TW'].includes(k) || !MODERATION.has(String(o[k]))) return null;
    }
    return JSON.stringify(Object.fromEntries(keys.map((k) => [k, String(o[k])])));
  };
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
    /* Everything the packet claims about the merged chat, validated HERE —
       before a single row is written — rather than two hundred lines below
       where it used to be. These read `b.stream` and touch no table, so being
       late was an accident of where they were typed; and being late is exactly
       what let a 400 leave a written capture row behind it. */
    const sPre = b.stream ?? {};
    let chatSources;
    if (sPre.chat_sources !== undefined && sPre.chat_sources !== null) {
      const list = [...new Set([].concat(sPre.chat_sources).join(',').split(',')
        .map((x) => x.trim().toUpperCase()).filter(Boolean))].sort();
      const badP = list.filter((x) => !['YT', 'TW'].includes(x));
      if (badP.length) {
        return res.status(400).json({ error: 'chat_sources must be YT and/or TW', fields: badP });
      }
      chatSources = list.join(',') || null;
    }

    /* The file's description of itself. Rejected rather than coerced: a
       negative message count and the string "lots" are the same bug seen from
       here, and storing either would put a number on the panel that nobody
       can trace back to anything. */
    const chatNums = {};
    const chatBad = [];
    for (const [k, min] of [['chat_version', 1], ['chat_messages', 0],
                            ['chat_first_ms', 0], ['chat_last_ms', 0]]) {
      if (sPre[k] === undefined || sPre[k] === null) continue;
      const n = Number(sPre[k]);
      if (!Number.isInteger(n) || n < min) chatBad.push(k);
      else chatNums[k] = n;
    }
    if (chatNums.chat_first_ms != null && chatNums.chat_last_ms != null
        && chatNums.chat_first_ms > chatNums.chat_last_ms) {
      chatBad.push('chat_first_ms');
    }
    let chatMod;
    if (sPre.chat_moderation !== undefined && sPre.chat_moderation !== null) {
      const mod = canonModeration(sPre.chat_moderation);
      if (mod === null) chatBad.push('chat_moderation');
      else chatMod = mod;
    }
    if (chatBad.length) {
      return res.status(400).json({
        error: 'chat metadata must be whole numbers, a first no later than a '
             + 'last, and moderation of YT/TW to complete|none|unknown',
        fields: chatBad });
    }

    const started = Number(b.started_at ?? b.stream?.started_at ?? t);
    const tz = Number(b.tz_offset_min ?? b.stream?.tz_offset_min ?? 0);
    const title = String(b.title ?? b.stream?.title ?? '').trim()
      || `${platform} ${remoteId}`;
    let streamId, captureId = null, created = false, pairedWith = null;

    /* ── one transaction, from the first read to the last write ──────────
     *
     * This handler spans ~260 lines and writes `stream`, `capture` and the
     * chat columns. It ran none of it in a transaction — `tx()` is used ten
     * times elsewhere in this file and was not used once here — so a failure
     * partway left a half-applied packet: a capture row written, a stream row
     * not, and no way to tell from the outside.
     *
     * BEGIN IMMEDIATE also makes the upsert actually an upsert. The
     * read-then-write below ("is there already a capture with this
     * remote_id", "is there a mate to pair with") is the whole identity
     * decision of the ingest path, and two packets arriving together could
     * both read "no" and both create a stream.
     *
     * Refusals inside throw and are turned back into their status codes
     * outside, so a rejected packet rolls back rather than committing whatever
     * it had managed first. */
    const refuse = (status, body) => { const e = new Error('refused'); e.refuse = { status, body }; throw e; };
    let idx, state, refused = [];
    try {
    tx(W, () => {
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
      refuse(404, { error: `no stream ${wantStream}` });
    }
    if (wantStream && existing && existing.stream_id !== wantStream) {
      // Moving a capture between streams is a repair, not an observation. It
      // has to go through a changeset so it lands in the history with a reason.
      refuse(409, {
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

    /* `pinned()` answers "did a PERSON decide this, or did a machine measure
       it" — and until now server.js never asked it. It exists in archive.js
       and is called three times, all about duration_s, none of them here.
       So an editor corrected a title by hand, the next packet from ls-audit
       put the recorder's back, and the applied change row survived: the log
       and pinned() both went on reporting the human's value while the row held
       the machine's. That is the archive contradicting its own history, which
       is the one thing the changeset design exists to prevent.
       Only these three. The chat columns below describe a file the recorder
       made and nobody edits by hand, and duration_s is refused outright a few
       lines up because recompute() derives it. */
    const observe = (field, value) => {
      if (created || !pinned(W, streamId, field)) { sCols[field] = value; return; }
      refused.push(field);
    };
    if (sIn.title !== undefined && sIn.title !== null) observe('title', String(sIn.title));
    if (sIn.started_at !== undefined && sIn.started_at !== null) {
      observe('started_at', Number(sIn.started_at));
    }
    if (sIn.tz_offset_min !== undefined && sIn.tz_offset_min !== null) {
      observe('tz_offset_min', Number(sIn.tz_offset_min));
    }
    // The merged chat, and the record of which platforms are inside it. Written
    // together on purpose: the sources list is only meaningful as a description
    // of a particular merged file, and once the raws are in deep storage it is
    // the only thing that still knows the answer.
    if (sIn.chat_path !== undefined && sIn.chat_path !== null) {
      sCols.chat_path = String(sIn.chat_path);
    }
    // Already validated at the top of the handler, before any write.
    if (chatSources !== undefined) sCols.chat_sources = chatSources;
    Object.assign(sCols, chatNums);
    if (chatMod !== undefined) sCols.chat_moderation = chatMod;
    /* Which file all of that describes. Taken from this same packet when it
       carries a path, and otherwise from the row — never from the caller,
       because a description and the claim about what it describes arriving
       separately is exactly how they come to disagree. */
    if (CHAT_META.some((k) => k in sCols)) {
      const cur = R.prepare(
        'SELECT chat_path, chat_meta_path FROM stream WHERE id = ?').get(streamId) ?? {};
      const target = sCols.chat_path ?? cur.chat_path ?? null;
      sCols.chat_meta_path = target;
      /* Re-filed against a different file, so whatever this packet did not
         mention described the OLD one and is cleared rather than inherited.
         Without this, a sweep that reports a count and no span leaves the
         previous file's span behind wearing the new file's path — which the
         projection then believes, because that is exactly what it checks.
         The description is a set: partial updates only mean anything while
         the file underneath stays the same. */
      if ((cur.chat_meta_path ?? null) !== target) {
        for (const k of CHAT_META) if (!(k in sCols)) sCols[k] = null;
      }
    }

    if (Object.keys(sCols).length) {
      W.prepare(`UPDATE stream SET ${Object.keys(sCols).map((k) => `${k}=?`).join(',')},
                 updated_at=? WHERE id=?`).run(...Object.values(sCols), t, streamId);
    }

    idx = W.prepare('SELECT idx FROM stream WHERE id = ?').get(streamId).idx;
    });
    } catch (e) {
      if (e?.refuse) return res.status(e.refuse.status).json(e.refuse.body);
      throw e;
    }

    /* AFTER the commit, and deliberately. recompute() stats every capture file
       when mediaRoot is set, and holding the write lock across filesystem IO
       would make contention worse than the problem it is solving. Its failure
       is logged rather than returned, for the same reason as in apply(): the
       packet HAS landed, and answering a landed write with a 500 is what makes
       a recorder retry into a duplicate. */
    try { state = recompute(W, streamId, { mediaRoot: config.mediaRoot }); }
    catch (e) { console.error(`ingest ${streamId}: recompute failed:`, e?.message ?? e); }
    bumpGeneration(W);
    res.json({ id: streamId, index: idx, capture_id: captureId, created,
               paired_with: pairedWith, vod_state: state?.vod_state ?? null,
               chat_state: state?.chat_state ?? null,
               /* Said out loud rather than silently dropped. A recorder that
                  keeps reporting a title somebody has corrected should be able
                  to see that it is being ignored, and why. */
               ...(refused.length ? { pinned: refused } : {}) });
  });

  // -------------------------------------------------------------------------
  // uploads — bytes arriving from a person
  //
  // Raw body, not multipart, and that is a simplification rather than a
  // shortcut. The server names the file, so there is no filename field to
  // parse. The title and taglets are typed WHILE the bytes move and arrive in
  // their own request afterwards, so there are no form fields here either.
  // What is left is one stream with nothing wrapped around it — and multipart
  // would mean adding a parser, a dependency and a class of bug in order to
  // carry fields that do not exist at this point in the flow.
  //
  // The whole client side is `fetch(url, { method: 'POST', body: file })`.
  // -------------------------------------------------------------------------

  /* Both caps are enforced on the way IN. A limit checked once the body has
     arrived has already filled the disk, which is the entire failure mode. */
  const UP_MAX_BYTES = Number(process.env.TENMA_UPLOAD_MAX_BYTES) || 200 * 1024 * 1024;
  const UP_MAX_S = Number(process.env.TENMA_UPLOAD_MAX_S) || 600;
  /* Auth answers who; this answers how much. Without it one careless
     authenticated person fills the quarantine mount in an afternoon, and
     "trusted member" plus "convenient" is exactly how that happens by
     accident rather than malice. */
  const UP_MAX_PENDING = Number(process.env.TENMA_UPLOAD_MAX_PENDING) || 20;

  /* Pictures get their own numbers, and much smaller ones. A 200 MB cap is
     sized for a ten-minute capture; the same cap on the meme panel is an
     invitation to park a film in it. The duration cap is the one that keeps a
     "meme" from being a whole video with a funny thumbnail — both picture
     collections take gifs and short clips, which is most of what a reaction
     IS, but a minute is the outside of either.

     Pending counts are separate because they measure different things: twenty
     clips is a lot of quarantine, forty memes is a few megabytes. */
  const UP_IMG_BYTES = Number(process.env.TENMA_IMAGE_MAX_BYTES) || 25 * 1024 * 1024;
  const UP_IMG_S = Number(process.env.TENMA_IMAGE_MAX_S) || 60;
  const UP_IMG_PENDING = Number(process.env.TENMA_IMAGE_MAX_PENDING) || 40;

  /* One row per collection, and the only place the differences between them
     are written down.

     `stills` and `sound` are what the table is really for, and they are
     opposites rather than a list of allowed formats: a picture in the
     Snippets pile is a row with no duration in a list built around duration,
     and a sound file in the Gallery is a picture that is not one. Neither is
     a corrupt upload — each is the right file in the wrong panel, and saying
     which panel takes it is worth more than a flat no. Moving pictures are
     not a field because all three take them.

     `one` and `noun` exist so the refusals read in the collection's own words
     rather than calling a meme a clip. */
  const UP_KIND = {
    snippet: { bytes: UP_MAX_BYTES, seconds: UP_MAX_S, pending: UP_MAX_PENDING,
               stills: false, sound: true,
               one: 'clip', noun: 'clips', elsewhere: 'Memes or Gallery' },
    meme:    { bytes: UP_IMG_BYTES, seconds: UP_IMG_S, pending: UP_IMG_PENDING,
               stills: true, sound: false,
               one: 'meme', noun: 'memes', elsewhere: 'Snippets' },
    gallery: { bytes: UP_IMG_BYTES, seconds: UP_IMG_S, pending: UP_IMG_PENDING,
               stills: true, sound: false,
               one: 'picture', noun: 'pictures', elsewhere: 'Snippets' },
  };

  /* Which of classifyMedia's answers this collection will take. The four
     video ones — conformant, remux, audio, video, encode — are a moving
     picture however they got here, and every collection takes those. */
  const upTakes = (kind, cls) => {
    const k = UP_KIND[kind];
    if (cls === 'broken') return false;
    if (cls === 'still') return k.stills;
    if (cls === 'sound' || cls === 'sound-encode') return k.sound;
    return true;
  };

  /* Our extension table, never the client's. The name is minted here, so the
     extension is always one we chose — which is what keeps sendMedia's
     allowlist load-bearing rather than advisory. Cosmetic either way: the
     Content-Type comes from servedType() reading the CODECS, because a `.webm`
     holding H.264 is a real file that no browser will play. */
  const UP_EXT = (container, vcodec, acodec) => {
    const c = String(container ?? '');
    /* No video track: it is a sound file, and naming it .mp4 makes every later
       guess about it wrong. mp4-family containers holding only audio are .m4a
       — same bytes, honest name. */
    if (!vcodec) {
      if (c.includes('mp4') || c.includes('mov') || c.includes('m4a')) return '.m4a';
      if (c.includes('mp3')) return '.mp3';
      if (c.includes('flac')) return '.flac';
      if (c.includes('wav')) return '.wav';
      if (c.includes('ogg')) return '.ogg';
      if (c.includes('matroska') || c.includes('webm')) return '.webm';
      return acodec ? '.bin' : '.bin';
    }
    if (c.includes('gif')) return '.gif';
    /* A single picture, named by its CODEC. The container cannot name it:
       ffprobe calls a jpeg `image2`, which is a family of demuxers rather than
       a format, and calls a png `png_pipe`. bmp and tiff reach here and get
       nothing on purpose — the MIME allowlist has no entry for either, so a
       name we cannot serve is worse than a refusal the route can explain. */
    if (c.endsWith('_pipe') || c === 'image2') {
      if (vcodec === 'png' || vcodec === 'apng') return '.png';
      if (vcodec === 'mjpeg') return '.jpg';
      if (vcodec === 'webp') return '.webp';
      return null;
    }
    if (c.includes('mp4') || c.includes('mov')) return '.mp4';
    if (c.includes('matroska')) {
      return ['vp8', 'vp9', 'av1'].includes(String(vcodec)) ? '.webm' : '.mkv';
    }
    if (c.includes('webm')) return '.webm';
    return '.bin';
  };

  app.post('/api/uploads', requireRole('suggester'), async (req, res) => {
    if (!config.quarantineRoot) {
      return res.status(503).json({ error: 'uploads are disabled; set TENMA_QUARANTINE_ROOT' });
    }
    const me = req.person?.id ?? null;
    if (!me) return res.status(401).json({ error: 'sign in first' });

    /* Which collection this is going into. A header rather than a query
       string because the body is the file and the URL is shared with the
       browser's own history — same reason the name arrives as x-upload-name.
       ?kind= is accepted too so the route can be driven from a shell. */
    const kind = String(req.get('x-upload-kind') || req.query.kind || 'snippet');
    if (!SNIPPET_KINDS.includes(kind)) {
      return res.status(400).json({
        error: `kind must be one of ${SNIPPET_KINDS.join(', ')}` });
    }
    const K = UP_KIND[kind];

    const pending = R.prepare(
      `SELECT count(*) c FROM snippet
        WHERE author_id = ? AND kind = ? AND status = 'proposed'
          AND retracted_at IS NULL`).get(me, kind).c;
    if (pending >= K.pending) {
      return res.status(429).json({
        error: `you already have ${pending} ${pending === 1 ? K.one : K.noun}`
          + ' waiting for review',
        pending, limit: K.pending });
    }

    const id = ulid();
    // A dotfile while it is still arriving: the importer skips dotfiles, so a
    // half-written upload can never be imported as a row for half a file.
    const partRel = `.part-${id}`;
    const partAbs = join(config.quarantineRoot, partRel);
    const scrub = () => { try { rmSync(partAbs, { force: true }); } catch { /* gone */ } };

    const hash = createHash('sha256');
    let bytes = 0, tooBig = false;
    const meter = new Transform({
      transform(chunk, _enc, cb) {
        bytes += chunk.length;
        if (bytes > K.bytes) { tooBig = true; return cb(new Error('too big')); }
        hash.update(chunk);
        cb(null, chunk);
      },
    });

    try {
      // pipeline destroys the whole chain on error, so exceeding the cap tears
      // the socket down mid-flight rather than politely reading to the end.
      await pipeline(req, meter, createWriteStream(partAbs));
    } catch {
      scrub();
      return tooBig
        ? res.status(413).json({
            error: `${K.noun} are capped at ${Math.round(K.bytes / 1048576)} MB`,
            limit_bytes: K.bytes })
        : res.status(400).json({ error: 'the upload did not finish' });
    }
    if (!bytes) { scrub(); return res.status(400).json({ error: 'that was an empty file' }); }

    /* Cheapest rejection first. ffprobe costs a syscall and throws out
       everything that is not media before a hash, a re-encode or a row. */
    const p = probeMedia(partAbs);
    /* `broken` and not merely "no video codec". ffmpeg carries demuxers for
       ANSI art and raw bitmaps and will happily describe a blob of nothing as
       `bintext, 1280x10000, duration unknown` rather than say it cannot read
       it — which the old check accepted, because a codec name was present.
       classifyMedia insists on a positive duration for anything moving, which
       every real video has and no hallucinated stream does — and on a
       recognised still FORMAT for anything that is not, which is what stops
       that same blob of nothing from arriving in the gallery instead. */
    const cls = p ? classifyMedia(partAbs, p) : 'broken';
    if (cls === 'broken') {
      scrub();
      return res.status(415).json({
        error: `that does not look like ${K.stills ? 'a picture or a video' : 'a video'}` });
    }
    /* The right file in the wrong panel. Worth its own answer rather than a
       flat 415: the person has a usable file and is one click from the place
       it goes, and nothing else in the archive can tell them which place that
       is. */
    if (!upTakes(kind, cls)) {
      scrub();
      const what = cls === 'still' ? 'a picture' : 'a sound file';
      return res.status(415).json({
        error: `that is ${what} — it belongs in ${K.elsewhere}`,
        kind, media: cls });
    }
    /* A still has no duration to cap and the cap must not be applied to one:
       ffprobe reports a jpeg as a fortieth of a second, which is under every
       limit, but a png reports nothing at all and `null > n` is false anyway.
       Guarding on the class rather than on the number says why. */
    if (cls !== 'still' && p.duration_s && p.duration_s > K.seconds) {
      scrub();
      const cap = K.seconds >= 120
        ? `${Math.round(K.seconds / 60)} minutes` : `${K.seconds} seconds`;
      return res.status(413).json({
        error: `${K.noun} are capped at ${cap}`,
        duration_s: p.duration_s, limit_s: K.seconds });
    }

    /* Exact-byte dedupe. Not the interesting duplicate — the perceptual check
       that catches a re-encode of the same moment comes later, and WARNS
       rather than refuses. This one is the double-click and the "did that
       work? let me try again", which is worth stopping before the encode. */
    const sha = hash.digest('hex');
    const twin = R.prepare(
      // author_id, because naming the twin is a disclosure and snipVisible is
      // what decides whether this person is allowed it.
      `SELECT id, title, status, author_id FROM snippet
        WHERE sha256 = ? AND retracted_at IS NULL LIMIT 1`).get(sha);
    if (twin) {
      scrub();
      /* Refuse either way, but only NAME it to somebody who could already have
         found it. Matching bytes against a gated clip used to hand back its id
         and title — to a person who gets a 404 on that clip from the list, the
         detail route, the media route and the poster. Somebody else's pending
         upload leaked the same way.

         What remains is an oracle: an uploader learns the archive holds a file
         they already possess. That is a much smaller thing than its title, and
         closing it entirely would mean accepting the duplicate — storing a
         second copy of a restricted clip and putting it in front of a reviewer
         — which is worse on every axis that matters here. */
      const mayName = snipVisible(twin, req);
      return res.status(409).json({
        error: mayName
          ? 'the archive already has this exact file'
          : 'the archive already has this exact file — an editor can tell you more',
        ...(mayName
          ? { snippet: { id: twin.id, title: twin.title, status: twin.status } }
          : {}),
      });
    }

    // Named by us. Kills traversal, extension games, unicode normalisation and
    // NAS case-collisions in one move — and because `slug` IS the filename
    // stem, minting the name mints the slug, so the rename-collision class
    // that cost a whole cleanup manifest back when these files were named by
    // hand cannot apply to anything arriving this way.
    const ext = UP_EXT(p.container, p.video_codec, p.audio_codec);
    if (!ext) {
      scrub();
      return res.status(415).json({
        error: 'the archive stores pictures as PNG, JPEG or WebP',
        video_codec: p.video_codec, container: p.container });
    }
    const rel = `${id}${ext}`;
    try { renameSync(partAbs, join(config.quarantineRoot, rel)); }
    catch (e) { scrub(); return res.status(500).json({ error: `could not store it: ${e.code}` }); }

    /* The submitted name is display text and nothing else — it never touches
       the filesystem, and it is only here so the queue can say "clip_0043.mp4"
       back to the person who sent it.

       And for a PICTURE it is not even that. A clip is a line in a list of a
       thousand and its title is how anybody finds it again, so deriving one
       from the filename is better than nothing. A meme is a tile you can see;
       its filename is `IMG_20260910_142311` or `unknown-4.png`, and turning
       that into a title produces a row labelled with gibberish that reads as
       a name somebody chose. Empty is the honest starting state, and the
       submitter can write a few words if the picture wants them. */
    const given = String(req.get('x-upload-name') ?? '').slice(0, 200);
    const title = kind === 'snippet'
      ? (given.replace(/\.[a-z0-9]{1,8}$/i, '').replace(/[_-]+/g, ' ').trim()
         || 'Untitled upload')
      : '';

    const t = now();
    W.prepare(
      `INSERT INTO snippet(id, slug, title, kind, file_path, quarantine_path, duration_s,
                           width, height, bytes, added_at, container, video_codec,
                           audio_codec, sha256, transcript_status, status, origin,
                           author_id, created_at, updated_at)
       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'none','proposed','upload',?,?,?)`)
      .run(id, id.toLowerCase(), title, kind,
           // Where it WILL live once an editor approves. The Pi resolves both
           // roots from its own config; this is a record, not an instruction.
           // Quarantine is flat and the media tree is not, which is fine — the
           // promote job carries both names and makes the parent directory.
           `${KIND_DIR[kind]}/${rel}`, rel,
           p.duration_s, p.width, p.height, bytes, t,
           p.container, p.video_codec, p.audio_codec, sha, me, t, t);

    /* A still is finished the moment it lands. There is no container to remux,
       no soundtrack to fix and no moov atom to move — the bytes that arrived
       are the bytes that get served — so the normalize job is skipped rather
       than queued to do nothing, and normalize_status stays at its 'none'
       default, which is the truth about it.

       What a still DOES need is the one thing the normalize job would have
       left behind: a poster. Made here, in-process, because there is nothing
       else coming that would make it. */
    if (cls === 'still') {
      await makePoster({ id, poster_path: null }, p, null,
                       join(config.quarantineRoot, rel), false, true);
      /* And the words on it, if there are any. Queued now rather than at
         approval — the opposite of transcription, because reading a small
         picture is a fraction of a second where a transcript is minutes, and
         having the text BEFORE the verdict is worth more than saving it: the
         reviewer can read what the meme says, and the submitter's own pending
         row is searchable straight away.

         Nothing claims this until a sidecar answers, so on a deployment with
         none it simply sits, which is the honest state. */
      enqueueJob('ocr', { snippetId: id, by: me });
    } else {
      /* Queued before the response, so the row is never briefly a clip nobody
         has asked to convert. The worker polls seconds later; for the common
         case — an MP4 that is already conformant — it will have finished
         writing a poster before the submitter has typed a title.

         `queued` and not `running`: the worker sets that when it actually
         claims, and a status that lies about what is happening is worse than
         one that is a few seconds behind. */
      W.prepare("UPDATE snippet SET normalize_status = 'queued' WHERE id = ?").run(id);
      enqueueJob('normalize', { snippetId: id, by: me });
    }
    bumpGeneration(W);

    /* The bytes arriving, which is a different moment from the submission —
       a file can be uploaded and the form abandoned, and a log that only
       recorded finished submissions would show nothing for the quarantine
       space that was spent. */
    logEvent(req, 'uploaded', 'snippet', id,
             { kind, name: given || title, bytes, duration_s: p?.duration_s ?? null });

    res.status(201).json({
      snippet: snipRow(R.prepare('SELECT * FROM snippet WHERE id = ?').get(id), { me }),
      // What still has to happen before anyone but the submitter sees it.
      // For a still: nothing. It is already showable and already has a poster.
      next: cls === 'still' ? null : 'normalize',
    });
  });

  /* ---- the submitter's own write ------------------------------------------
     The title and taglets are typed WHILE the bytes move, so they land here
     rather than with the upload. This is the one write a suggester gets on a
     snippet, and it is narrow on purpose: the clip must be theirs, and it must
     still be `proposed`. An editor's yes or no closes the window — after that
     the row belongs to the archive and changes go through a changeset like
     everything else.

     Partial by design. The panel saves a title before the tags are typed, so a
     field that is absent is left alone rather than cleared. */
  const upOwn = (req, id) => {
    const s = R.prepare('SELECT * FROM snippet WHERE id = ? AND retracted_at IS NULL').get(id);
    if (!s) return { err: [404, 'no such upload'] };
    const mine = !!s.author_id && s.author_id === req.person?.id;
    // Not yours and you are not an editor: it does not exist, rather than
    // "exists but you may not". Same disclosure rule as the media routes.
    if (!mine && !atLeast(req.person, 'editor')) return { err: [404, 'no such upload'] };
    if (s.status !== 'proposed') {
      return { err: [409, 'that has already been reviewed'] };
    }
    return { s };
  };

  app.patch('/api/uploads/:id', requireRole('suggester'), (req, res) => {
    const { s, err } = upOwn(req, req.params.id);
    if (err) return res.status(err[0]).json({ error: err[1] });

    const { title, taglets, transcript, auto_transcribe } = req.body ?? {};
    const t = now();

    if (title !== undefined) {
      const v = String(title ?? '').trim();
      /* Required on a clip and optional on a picture, which is not an
         inconsistency: a clip is a row in a list and its title is the only
         handle anyone has on it, while a picture is a tile you can see. On a
         picture the field is a short description — useful when there is
         something to say and noise when there is not. */
      if (!v && s.kind === 'snippet') {
        return res.status(400).json({ error: 'a clip needs a title' });
      }
      if (v.length > 300) return res.status(400).json({ error: 'that title is too long' });
      W.prepare('UPDATE snippet SET title = ?, updated_at = ? WHERE id = ?').run(v, t, s.id);
    }

    /* Existing taglets only. A submitter attaching a tag is describing the
       clip; minting one is editing the vocabulary, and free-text creation
       destroys a curated vocabulary faster than anything else. The panel
       already refuses these in red — this is the same rule where it counts,
       because the panel is not a security boundary. */
    if (taglets !== undefined) {
      /* Matched on the SLUG, but the display name is what gets kept for a
         suggestion — "Selen Tatsuki" is what an editor needs to read, and
         `selen-tatsuki` is what the page sent. Both are carried so neither
         has to be reconstructed. */
      const raw = [].concat(taglets ?? [])
        .map((x) => String(x ?? '').trim()).filter(Boolean).slice(0, 40);
      /* slugify, not a hand-rolled lowercase-and-hyphenate. They agree on
         `Selen Tatsuki` and disagree on everything with punctuation or an
         accent in it — so `Amelia Watson!` matched no taglet, was filed as a
         stray, and sat in the queue as a name the archive already had. The
         vocabulary is keyed by slugify(); every comparison against it has to
         be too. */
      const want = [...new Set(raw.map(slugify))];
      const found = want.length
        ? R.prepare(`SELECT id, slug FROM tag
                      WHERE slug IN (${want.map(() => '?').join(',')})
                        AND retracted_at IS NULL`).all(...want)
        : [];
      const known = new Set(found.map((r) => r.slug));

      /* Names the archive does not have. Kept as text on the clip rather than
         minted as `proposed` taglets: a proposed taglet is autocompleted, and
         then the second person to want this name is offered it before anybody
         agreed it should exist — and a typo becomes vocabulary the moment a
         second clip picks it up. */
      const strays = [...new Map(raw
        .filter((x) => !known.has(slugify(x)))
        .map((x) => [slugify(x), x.slice(0, 80)])).values()].slice(0, 12);

      tx(W, () => {
        W.prepare('DELETE FROM snippet_taglet WHERE snippet_id = ?').run(s.id);
        const ins = W.prepare(
          `INSERT INTO snippet_taglet(id, snippet_id, tag_id, created_at, updated_at)
           VALUES(?,?,?,?,?)`);
        for (const f of found) ins.run(ulid(), s.id, f.id, t, t);
        W.prepare('UPDATE snippet SET taglet_suggestions = ?, updated_at = ? WHERE id = ?')
          .run(strays.length ? JSON.stringify(strays) : null, t, s.id);
      });
    }

    /* Auto-transcribe on means whisper writes one after approval, which is
       what `none` has always meant. Off with prose means a human wrote it, so
       it is `edited` and the next --update pass will leave it alone.
       Untimed, because the field is a textarea and the words are all anyone
       has at this point: one line at t=0, which the whole-transcript editor
       can split and retime later. */
    /* Never on a still, whatever the panel sent. `auto_transcribe` means
       "whisper will write one after approval", which for a picture is simply
       untrue — whisper is never queued for a row with no audio track. What it
       DOES do is clear the transcript and its lines, and a picture's
       transcript is what OCR wrote into it, possibly seconds earlier. So the
       default state of a switch that does not apply would have quietly
       deleted the words off every meme its submitter saved.
       Checked here and not only in the panel, for the reason the taglet rule
       above gives: the panel is not a security boundary. */
    if (auto_transcribe === true && !isStill(s)) {
      tx(W, () => {
        W.prepare('DELETE FROM snippet_line WHERE snippet_id = ?').run(s.id);
        W.prepare(`UPDATE snippet SET transcript = NULL, transcript_status = 'none',
                                      updated_at = ? WHERE id = ?`).run(t, s.id);
      });
    } else if (auto_transcribe !== true && transcript !== undefined) {
      const v = String(transcript ?? '').trim();
      if (v.length > 20000) return res.status(400).json({ error: 'that transcript is too long' });
      tx(W, () => {
        W.prepare('DELETE FROM snippet_line WHERE snippet_id = ?').run(s.id);
        if (v) {
          W.prepare(
            `INSERT INTO snippet_line(id, snippet_id, seq, start_s, end_s, speaker, text)
             VALUES(?,?,0,0,NULL,NULL,?)`).run(ulid(), s.id, v);
        }
        W.prepare(
          `UPDATE snippet SET transcript = ?, transcript_status = ?, updated_at = ?
            WHERE id = ?`).run(v || null, v ? 'edited' : 'none', t, s.id);
      });
    }

    bumpGeneration(W);
    const fresh2 = R.prepare('SELECT * FROM snippet WHERE id = ?').get(s.id);

    /* The first of these is the submission — the line that says who brought
       this in and what they called it. Later ones are edits to something
       already submitted, which is a different sentence and, before an editor
       has looked, the one worth being able to notice: a title quietly changed
       after somebody glanced at it is exactly what this log is for. */
    const first = !R.prepare(
      `SELECT 1 FROM event WHERE target_id = ? AND verb = 'submitted'`).get(s.id);
    logEvent(req, first ? 'submitted' : 'edited', 'snippet', s.id, {
      title: fresh2.title,
      taglets: TAGLETS_OF.all(s.id).map((x) => x.name),
      suggested: (() => {
        try { return JSON.parse(fresh2.taglet_suggestions ?? 'null') ?? []; }
        catch { return []; }
      })(),
      auto_transcribe: fresh2.transcript_status === 'none',
      source_url: fresh2.source_url ?? null,
    });

    res.json({ snippet: snipRow(fresh2, { lines: true, me: req.person?.id ?? null }) });
  });

  /** Withdraw. The submitter's own reject, before anyone has looked at it.
   *  The bytes go with it: nothing ever pointed at them, no reviewer spent
   *  attention on them, and leaving them for a sweep that does not exist yet
   *  is how a quarantine mount fills up with things nobody wants. */
  app.delete('/api/uploads/:id', requireRole('suggester'), (req, res) => {
    const { s, err } = upOwn(req, req.params.id);
    if (err) return res.status(err[0]).json({ error: err[1] });

    if (s.quarantine_path && config.quarantineRoot) {
      const abs = resolveMedia(config.quarantineRoot, s.quarantine_path);
      if (abs) { try { rmSync(abs, { force: true }); } catch { /* already gone */ } }
    }
    /* Retracted, not merely rejected. `rejected` alone left the row live for
       every query that filters on retracted_at — including the sha256 dedupe,
       which then refused the same file forever and named a snippet the person
       could no longer see anywhere. Withdrawing is also not a review decision:
       leaving it in the rejected pile makes a clip nobody looked at count as
       one an editor turned down. The status stays as the record of how it
       ended; the tombstone is what takes it out of the archive. */
    const t = now();
    W.prepare(
      `UPDATE snippet SET status = 'rejected', quarantine_path = NULL,
                          retracted_at = ?, updated_at = ?
        WHERE id = ?`).run(t, t, s.id);
    logEvent(req, 'withdrew', 'snippet', s.id, { title: s.title });
    bumpGeneration(W);
    res.json({ ok: true, id: s.id });
  });

  // -------------------------------------------------------------------------
  // grants — who may see what
  //
  // The endpoints an admin panel needs, ahead of the panel. Deliberately not a
  // changeset: a permission is not an editorial claim about the archive that
  // somebody might later dispute, it is an access decision, and queueing it
  // for review would mean a suggester could propose granting themselves
  // something.
  // -------------------------------------------------------------------------

  /** Every gate name currently in use, so a panel can offer them rather than
   *  asking somebody to remember how they spelled it. */
  // -------------------------------------------------------------------------
  // the log — who did what, in order
  //
  // Append-only, and nothing in the archive updates or deletes a row here. It
  // is the one table whose value is entirely in not being rewritten.
  //
  // Worth being plain about what this does and does not protect against. A
  // rogue EDITOR is well contained: everything they can do goes through a
  // changeset, so it is attributed, reversible, and now narrated. A rogue
  // ADMIN is not contained by anything here — they can purge, they can lift a
  // gate, and with a shell on the NAS they can edit this table directly.
  // Tamper-evidence would mean hash-chaining and shipping entries off the box,
  // which is a different project. What holds in the meantime is that the
  // masters live on the Pi, in a tree this process cannot write to.
  // -------------------------------------------------------------------------

  const EVENT_INS = W.prepare(
    `INSERT INTO event(id, at, actor_id, actor_handle, actor_role, verb,
                       target_type, target_id, detail, changeset_id)
     VALUES(?,?,?,?,?,?,?,?,?,?)`);

  /** Write one line of the log.
   *
   *  Never throws into its caller. A log that can fail an approval is worse
   *  than a log with a hole in it: the first loses the work, the second loses
   *  the note about the work.
   */
  function logEvent(req, verb, targetType, targetId, detail = null, changesetId = null) {
    try {
      const p = req?.person ?? null;
      EVENT_INS.run(ulid(), now(), p?.id ?? null, p?.handle ?? 'anonymous',
                    p?.role ?? 'viewer', verb, targetType, String(targetId),
                    detail ? JSON.stringify(detail) : null, changesetId);
    } catch (e) { console.error('log:', e?.message ?? e); }
  }

  /** Turn an applied changeset into the lines a person would write.
   *
   *  One place, called after every propose(), so anything that goes through
   *  the changeset system is narrated without its own endpoint having to
   *  remember to say so — including whatever gets added next.
   *
   *  A changeset is a set of field changes and a log line is a sentence, and
   *  the two do not map one to one: a taglet attach is two change rows naming
   *  one link, and the sentence is "tagged X". So the rows are gathered by the
   *  thing they are about before anything is written.
   */
  function logChangeset(req, csId) {
    if (!csId) return;
    try {
      const rows = R.prepare(
        'SELECT * FROM change WHERE changeset_id = ? ORDER BY seq').all(csId);
      const name = (id) => R.prepare('SELECT name FROM tag WHERE id = ?').get(id)?.name ?? id;

      /* A junction's rows share a target_id and mean nothing apart: one says
         which snippet, the other says which taglet. Collected first, written
         once. */
      const links = new Map();
      for (const c of rows) {
        if (c.target_type !== 'snippet_taglet') continue;
        const l = links.get(c.target_id) ?? { op: c.op };
        if (c.field === 'snippet_id') l.snippet = c.value ?? c.base_value;
        if (c.field === 'tag_id') l.taglet = c.value ?? c.base_value;
        /* A delete arrives as the bare row plus the pair recorded on its way
           out, so whichever arrives second must not downgrade the verb. */
        if (c.op === 'delete') l.op = 'delete';
        links.set(c.target_id, l);
      }
      for (const l of links.values()) {
        if (!l.snippet || !l.taglet) continue;
        logEvent(req, l.op === 'delete' ? 'untagged' : 'tagged', 'snippet', l.snippet,
                 { taglet: name(l.taglet), taglet_id: l.taglet }, csId);
      }

      for (const c of rows) {
        if (c.target_type === 'snippet' && c.field === 'title') {
          logEvent(req, 'renamed', 'snippet', c.target_id,
                   { from: c.base_value, to: c.value }, csId);
        } else if (c.target_type === 'snippet' && c.field === 'status') {
          const verb = c.value === 'confirmed' ? 'approved'
            : c.value === 'rejected' ? 'rejected' : 'returned to the queue';
          logEvent(req, verb, 'snippet', c.target_id, null, csId);
        } else if (c.target_type === 'tag' && c.op === 'create' && c.field === 'name') {
          logEvent(req, 'minted', 'tag', c.target_id, { name: c.value }, csId);
        } else if (c.target_type === 'music' && c.op === 'delete') {
          logEvent(req, 'retracted', 'music', c.target_id,
                   { title: R.prepare('SELECT title FROM music WHERE id = ?')
                     .get(c.target_id)?.title ?? null }, csId);
        } else if (c.target_type === 'tag' && c.op === 'delete') {
          /* The tombstone, which the log never used to mention — so a tag
             could vanish from every picker in the archive and the only record
             was a changeset nobody opens. `name` is read now because the row
             survives a retraction; after a purge it would not. */
          logEvent(req, 'retracted', 'tag', c.target_id, { name: name(c.target_id) }, csId);
        } else if (c.target_type === 'tag' && c.op === 'update' && c.field === 'name') {
          logEvent(req, 'renamed', 'tag', c.target_id,
                   { from: c.base_value, to: c.value }, csId);
        }
      }
    } catch (e) { console.error('log changeset:', e?.message ?? e); }
  }

  /** One snippet's story, resolved into names.
   *
   *  Admin-only, alongside the archive-wide log. Editors are arguably the ones
   *  who most want it, and that is a change of one word here when it is
   *  wanted — but a surface that names who purged what is the one to be late
   *  rather than early with.
   */
  app.get('/api/snippets/:id/history', requireRole('admin'), (req, res) => {
    const rows = R.prepare(
      `SELECT * FROM event WHERE target_id = ? ORDER BY at ASC, id ASC LIMIT 500`)
      .all(req.params.id);
    res.json({ events: rows.map(eventRow) });
  });

  /** Everything, newest first.
   *
   *  The reason this exists rather than only the per-snippet view: a purge
   *  leaves a tombstone that is invisible in every list, so "who deleted that"
   *  is exactly the line a per-snippet panel can never be opened to read. The
   *  entry that matters most is the one whose subject is gone.
   */
  app.get('/api/events', requireRole('admin'), (req, res) => {
    const limit = Math.min(Math.max(Number(req.query.limit ?? 100) || 100, 1), 500);
    const before = req.query.before ?? null;
    const verb = req.query.verb ? String(req.query.verb) : null;
    const who = req.query.actor ? String(req.query.actor) : null;

    const where = ['1=1'], params = [];
    if (before) { where.push('e.id < ?'); params.push(String(before)); }
    if (verb) { where.push('e.verb = ?'); params.push(verb); }
    if (who) { where.push('e.actor_handle = ?'); params.push(who); }

    const rows = R.prepare(
      `SELECT e.* FROM event e WHERE ${where.join(' AND ')}
        ORDER BY e.at DESC, e.id DESC LIMIT ?`).all(...params, limit + 1);
    const page = rows.slice(0, limit);
    res.json({
      events: page.map(eventRow),
      next: rows.length > limit ? page[page.length - 1].id : null,
      actors: R.prepare(
        'SELECT DISTINCT actor_handle h FROM event ORDER BY h').all().map((r) => r.h),
    });
  });

  /** A transcript correction, at the grain somebody would describe it.
   *
   *  A per-word fix fires this on every word, and a person correcting eight
   *  mishearings in a row did ONE thing. So a further touch by the same person
   *  on the same snippet inside the window folds into the line already there
   *  rather than adding another — the log says "groyperkirked edited the
   *  transcript" once, which is what happened.
   *
   *  The cost is stated: two corrections half an hour apart read as one. That
   *  is the right trade for a log meant to be read, and the per-line detail it
   *  gives up was never recorded anywhere to begin with.
   */
  const TRANSCRIPT_WINDOW_S = 30 * 60;
  function logTranscript(req, snippetId, verb = 'edited the transcript') {
    try {
      const me = req.person?.id ?? null;
      const recent = R.prepare(
        `SELECT at FROM event
          WHERE target_id = ? AND verb = ? AND actor_id IS ?
          ORDER BY at DESC LIMIT 1`).get(String(snippetId), verb, me);
      if (recent && now() - recent.at < TRANSCRIPT_WINDOW_S) return;
      logEvent(req, verb, 'snippet', snippetId);
    } catch (e) { console.error('log transcript:', e?.message ?? e); }
  }

  /** A stored row as the page wants it: ids resolved, detail parsed.
   *
   *  The title comes from the snippet NOW rather than from the event, so a
   *  line about a snippet that has since been renamed still says which one it
   *  is — and the rename itself is its own line, carrying both names. A
   *  purged snippet keeps its row, so even that resolves.
   */
  const eventRow = (r) => {
    let detail = null;
    try { detail = r.detail ? JSON.parse(r.detail) : null; } catch { /* keep null */ }
    const t = r.target_type === 'snippet'
      ? R.prepare('SELECT title, status, retracted_at FROM snippet WHERE id = ?').get(r.target_id)
      /* 'taglet' as well as 'tag': the vocabularies merged, but every event
         written before that says `taglet`, and a log that cannot resolve its
         own past is not a log. Both read from the one table now. */
      : (r.target_type === 'tag' || r.target_type === 'taglet')
        ? R.prepare('SELECT name FROM tag WHERE id = ?').get(r.target_id)
        : r.target_type === 'person'
          ? R.prepare('SELECT handle FROM person WHERE id = ?').get(r.target_id)
          : null;
    return {
      id: r.id, at: r.at, verb: r.verb,
      actor: r.actor_handle, actor_role: r.actor_role, actor_id: r.actor_id,
      target_type: r.target_type, target_id: r.target_id,
      /* Null when the row it named is genuinely gone — a person deleted, a
         taglet retracted. The line still renders; it just cannot say more than
         the id it holds. */
      target: t ? (t.title ?? t.name ?? t.handle ?? null) : null,
      target_gone: r.target_type === 'snippet' ? !!t?.retracted_at : !t,
      detail, changeset_id: r.changeset_id,
    };
  };

  /** What the transcription queue is doing, for the admin panel. */
  app.get('/api/transcribe', requireRole('admin'), (req, res) => {
    const ready = whisperReady();
    const rows = R.prepare(
      `SELECT j.id, j.status, j.snippet_id, j.attempts, j.error, j.created_at,
              j.claimed_at, j.finished_at, s.title, s.duration_s, s.transcript_status
         FROM job j LEFT JOIN snippet s ON s.id = j.snippet_id
        WHERE j.kind = 'transcribe'
          AND (j.status IN ('approved', 'claimed', 'paused')
               OR j.finished_at > ?)
        ORDER BY CASE j.status WHEN 'claimed' THEN 0 WHEN 'approved' THEN 1
                               WHEN 'paused' THEN 2 ELSE 3 END,
                 j.created_at
        LIMIT 200`).all(now() - 24 * 3600);
    res.json({
      on: transcribeOn,
      ready: ready.ok,
      why: ready.why,
      model: ready.ok ? (WHISPER.model.split('/').pop() ?? null) : null,
      running: running ? { job_id: running.jobId, since: running.at } : null,
      jobs: rows,
    });
  });

  /** The big switch. */
  app.post('/api/transcribe/pause', requireRole('admin'), (req, res) => {
    const want = req.body?.on;
    transcribeOn = typeof want === 'boolean' ? want : !transcribeOn;
    logEvent(req, transcribeOn ? 'resumed transcription' : 'paused transcription',
             'setting', 'transcribe', null);
    /* Deliberately NOT persisted. It is the switch you reach for when the box
       is busy right now, and a restart is the clearest possible signal that
       "right now" is over. */
    res.json({ on: transcribeOn });
  });


  /* ---- the machines, for the panel ---------------------------------------
     One endpoint for all three slots. The alternative was a second copy of
     /api/transcribe per task, and three near-identical reports is how two of
     them come to disagree about what "ready" means. */
  app.get('/api/models', requireRole('admin'), (req, res) => {
    const t = now();
    const queue = (kind) => R.prepare(
      `SELECT count(*) n,
              sum(status IN ('approved','claimed')) waiting,
              sum(status = 'failed') failed
         FROM job WHERE kind = ? AND (status IN ('approved','claimed','paused')
                                      OR finished_at > ?)`).get(kind, t - 24 * 3600);
    const ocr = ocrReady();
    const wh = whisperReady();
    res.json({
      /* Whether there is a sidecar at all, said once. Every `absent` below
         means something different depending on this, and a panel that had to
         infer it from three state_notes would infer it three times. */
      sidecar: config.mlUrl ? { configured: true } : { configured: false },
      tasks: MODEL_TASKS.map((task) => {
        const m = modelRow(task);
        const spec = MODEL_TASK[task];
        const info = modelInfo(task, m?.slug ?? '');
        const live = task === 'ocr' ? ocr : task === 'transcribe' ? wh : null;
        return {
          task,
          label: spec.label,
          into: spec.into,
          runner: spec.runner,
          slug: m?.slug ?? null,
          name: info.name,
          note: info.note,
          bytes: m?.bytes ?? info.bytes ?? null,
          enabled: !!m?.enabled,
          state: m?.state ?? 'unknown',
          state_note: m?.state_note ?? null,
          checked_at: m?.checked_at ?? null,
          used_at: m?.used_at ?? null,
          /* `null` where the task has no runner yet, which is not the same as
             `false`. Smart search has a model named and nothing that calls it,
             and the panel should say so rather than draw it as broken. */
          ready: live ? live.ok : null,
          why: live ? live.why : 'nothing calls this yet',
          on: task === 'ocr' ? ocrOn : task === 'transcribe' ? transcribeOn : null,
          queue: task === 'search' ? null : queue(task === 'ocr' ? 'ocr' : 'transcribe'),
          /* The menu, so the panel can offer a picker rather than a text box
             that has to be spelled exactly right. */
          options: MODEL_CATALOG[task] ?? [],
        };
      }),
    });
  });

  /** Choose a model, or switch a slot off. */
  app.patch('/api/models/:task', requireRole('admin'), (req, res) => {
    const task = String(req.params.task);
    if (!MODEL_TASKS.includes(task)) return res.status(404).json({ error: 'no such task' });
    const m = modelRow(task);
    if (!m) return res.status(404).json({ error: 'no such task' });

    const patch = {};
    if (req.body?.slug !== undefined) {
      const slug = String(req.body.slug ?? '').trim();
      /* The same character class the rest of the archive mints names from.
         This string is handed to another container as a model to load, so it
         is the one field here that is worth being narrow about — a name is a
         name, not a path and not an argument. */
      if (!/^[A-Za-z0-9_.@+-]{1,120}$/.test(slug)) {
        return res.status(400).json({ error: 'a model name is letters, digits and _ . @ + -' });
      }
      if (slug !== m.slug) {
        patch.slug = slug;
        /* Everything remembered about the old model was about the old model.
           Leaving `loaded` on a slug that has just changed would have the
           panel claim something is in memory that has never been asked for. */
        patch.state = 'unknown';
        patch.state_note = null;
        patch.bytes = null;
        patch.checked_at = null;
      }
    }
    if (req.body?.enabled !== undefined) patch.enabled = req.body.enabled ? 1 : 0;
    if (!Object.keys(patch).length) return res.json({ model: m });

    modelSet(task, patch);
    logEvent(req, patch.slug ? `set ${task} to ${patch.slug}`
      : `${patch.enabled ? 'enabled' : 'disabled'} ${task}`, 'setting', `model:${task}`, null,
      patch.slug ? { was: m.slug } : null);
    bumpGeneration(W);
    res.json({ model: modelRow(task) });
  });

  /** Go and ask the sidecar. By hand, because a poll would be waking an idle
   *  container to repeat itself; the panel is opened when somebody wants to
   *  know. */
  app.post('/api/models/:task/check', requireRole('admin'), async (req, res) => {
    const task = String(req.params.task);
    if (!MODEL_TASKS.includes(task)) return res.status(404).json({ error: 'no such task' });
    res.json({ model: await modelCheck(task) });
  });

  /** The OCR switch, the same shape and the same non-persistence as the
   *  transcription one above it. */
  app.post('/api/ocr/pause', requireRole('admin'), (req, res) => {
    const want = req.body?.on;
    ocrOn = typeof want === 'boolean' ? want : !ocrOn;
    logEvent(req, ocrOn ? 'resumed reading pictures' : 'paused reading pictures',
             'setting', 'ocr', null);
    res.json({ on: ocrOn });
  });

  /** Read this one again. The picture equivalent of retranscribe, and it
   *  refuses the same thing: a transcript a human has edited is the archive's
   *  answer, and a machine must not overwrite it without being told twice. */
  app.post('/api/snippets/:id/reocr', requireRole('editor'), (req, res) => {
    const s = R.prepare(
      `SELECT id, kind, container, video_codec, width, height, transcript_status
         FROM snippet WHERE id = ? AND retracted_at IS NULL`).get(req.params.id);
    if (!s) return res.status(404).json({ error: 'no such snippet' });
    /* Stills only, and the check is isStill() rather than the kind: a meme can
       be a gif, and reading text off a moving picture means choosing which
       frames to sample and then reconciling four different answers. That is a
       feature, not a detail, and refusing it plainly beats a job that runs and
       returns whatever the first frame happened to say. */
    if (!isStill(s)) {
      return res.status(400).json({
        error: 'this one moves — OCR reads single pictures' });
    }
    if (s.transcript_status === 'edited' && !req.body?.force) {
      return res.status(409).json({
        error: 'somebody has corrected this one — pass force to overwrite it' });
    }
    const open = R.prepare(
      `SELECT id FROM job WHERE snippet_id = ? AND kind = 'ocr'
         AND status IN ('approved','claimed','paused')`).get(s.id);
    if (open) return res.json({ job_id: open.id, already: true });
    const id = enqueueJob('ocr', { snippetId: s.id, by: req.person.id });
    logEvent(req, 'asked for the text on a picture again', 'snippet', s.id);
    bumpGeneration(W);
    res.json({ job_id: id });
  });

  /** One job: pause it, put it back, stop it, or give up on it. */
  app.post('/api/jobs/:id/:verb', requireRole('admin'), (req, res) => {
    const verb = String(req.params.verb);
    if (!['pause', 'resume', 'cancel', 'retry', 'dismiss'].includes(verb)) {
      return res.status(404).json({ error: 'no such action' });
    }
    const j = R.prepare('SELECT * FROM job WHERE id = ?').get(req.params.id);
    if (!j) return res.status(404).json({ error: 'no such job' });
    const t = now();

    if (verb === 'pause') {
      /* Queued only. A running whisper cannot be paused in any way that saves
         work — it has no checkpoint, so stopping and resuming re-reads the
         file from the beginning either way. Cancel is the honest verb for a
         running one, and it is right there. */
      if (j.status !== 'approved') {
        return res.status(409).json({
          error: j.status === 'claimed'
            ? 'that one is running — cancel it instead, whisper cannot resume mid-file'
            : `that job is ${j.status}` });
      }
      W.prepare("UPDATE job SET status = 'paused', updated_at = ? WHERE id = ?").run(t, j.id);
      logEvent(req, 'paused a transcription', 'snippet', j.snippet_id ?? j.id, { job_id: j.id });
      return res.json({ ok: true, status: 'paused' });
    }

    if (verb === 'retry') {
      /* Failures only. `failed` is terminal at this end on purpose — the thing
         that decides whether to try again is a person looking at the queue,
         and this is that person saying yes. Retrying something that SUCCEEDED
         is a different act with real consequences (a fetch would download over
         a file an editor has already reviewed), so it is not offered. */
      if (j.status !== 'failed') {
        return res.status(409).json({ error: `that job is ${j.status}, not failed` });
      }
      W.prepare(`UPDATE job SET status = 'approved', claimed_by = NULL, claimed_at = NULL,
                                error = NULL, finished_at = NULL, updated_at = ?
                  WHERE id = ?`).run(t, j.id);
      /* And the snippet stops saying the fetch failed, because it is no longer
         true — the row is what the submitter sees, and leaving it as a failure
         while the job is queued again is the panel disagreeing with itself. */
      if (j.kind === 'fetch' && j.snippet_id) {
        W.prepare(`UPDATE snippet SET fetch_status = 'queued', fetch_note = NULL,
                                      updated_at = ? WHERE id = ?`).run(t, j.snippet_id);
      }
      /* Same rule, the two kinds that write a transcript. This block predates
         both of them and cleared only the fetch column, so a retried OCR job
         sat queued while its picture still read `failed` with the old reason
         under it — the panel disagreeing with itself, exactly what the note
         above is about. Guarded on `failed` so an `edited` transcript is never
         reset: that is somebody's correction, and runOcr declines it anyway. */
      if ((j.kind === 'ocr' || j.kind === 'transcribe') && j.snippet_id) {
        W.prepare(`UPDATE snippet SET transcript_status = 'none', transcript_note = NULL,
                                      updated_at = ?
                    WHERE id = ? AND transcript_status = 'failed'`).run(t, j.snippet_id);
      }
      /* And the third pair of columns that mirror a job, which this block also
         predated. A retried music job left the card reading `unread` or `not
         saved` with the old reason under it while the recorder was already
         queued to try again — the same disagreement, on the surface where it
         is least explicable, because a song's card is the only place its state
         is shown at all. */
      const mid = (j.kind === 'music_probe' || j.kind === 'music_fetch')
        ? musicJobTarget(j) : null;
      if (mid) {
        const col = j.kind === 'music_probe' ? 'probe' : 'fetch';
        W.prepare(`UPDATE music SET ${col}_status = 'queued', ${col}_note = NULL,
                                    updated_at = ?
                    WHERE id = ? AND ${col}_status = 'failed'`).run(t, mid);
      }
      logEvent(req, 'sent a job back to the recorder', 'snippet', j.snippet_id ?? j.id,
               { job_id: j.id, kind: j.kind, attempts: j.attempts });
      bumpGeneration(W);
      return res.json({ ok: true, status: 'approved' });
    }

    if (verb === 'dismiss') {
      /* Giving up, which until now there was no way to say.
       *
       * `retry` assumes the failure was circumstantial and the next attempt
       * might land. Plenty are not: a post whose host will not serve it to
       * this recorder cannot ever be fetched, and the job for it sat in the
       * panel forever with a button whose only honest label would have been
       * "fail again". A queue that accumulates work nobody will ever do is a
       * queue people stop reading, and the failure that mattered is in there
       * with the ones that never will.
       *
       * Terminal by construction: `dismissed` is not `failed`, so neither the
       * retry above nor the sweep — which re-approves everything failed —
       * picks it back up. The row stays, with its error, and the event log
       * says who gave up and when.
       *
       * Deliberately does NOT touch the snippet or music row it belongs to.
       * That row IS failed, truthfully, and its note is the reason why — "X
       * did not hand over the video for that post" is the most useful sentence
       * anybody has about it. Overwriting that with "dismissed by kyabatsu"
       * would trade the explanation for the bookkeeping. */
      if (!['failed', 'cancelled'].includes(j.status)) {
        return res.status(409).json({
          error: j.status === 'claimed' || j.status === 'approved'
            ? `that one is still ${j.status === 'claimed' ? 'running' : 'queued'}` +
              ' — cancel it first, then dismiss it'
            : `that job is ${j.status}, and only a failed one can be dismissed` });
      }
      W.prepare(`UPDATE job SET status = 'dismissed', updated_at = ?,
                                finished_at = COALESCE(finished_at, ?) WHERE id = ?`)
        .run(t, t, j.id);
      logEvent(req, 'gave up on a job', 'snippet', j.snippet_id ?? j.id,
               { job_id: j.id, kind: j.kind, attempts: j.attempts, was: j.error ?? null });
      bumpGeneration(W);
      return res.json({ ok: true, status: 'dismissed' });
    }

    if (verb === 'resume') {
      if (j.status !== 'paused') return res.status(409).json({ error: `that job is ${j.status}` });
      W.prepare("UPDATE job SET status = 'approved', updated_at = ? WHERE id = ?").run(t, j.id);
      logEvent(req, 'resumed a transcription', 'snippet', j.snippet_id ?? j.id, { job_id: j.id });
      return res.json({ ok: true, status: 'approved' });
    }

    // cancel
    if (['done', 'failed'].includes(j.status)) {
      return res.status(409).json({ error: `that job already ${j.status}` });
    }
    /* Marked BEFORE the kill, because the runner's catch fires the moment the
       child dies and has to be able to tell this from a crash — and marked for
       a claimed job whether or not there is a child to kill right now, because
       the runner checks this set before starting each of its two. */
    let killed = false;
    if (j.status === 'claimed') cancelled.add(j.id);
    if (running && running.jobId === j.id) {
      try { running.child.kill('SIGKILL'); killed = true; } catch { /* already gone */ }
      /* Said straight away rather than waiting on the child's callback, so the
         panel does not go on showing a job that is already stopped. */
      running = null;
    }
    const why = `cancelled by ${req.person?.handle ?? 'an admin'}`;
    finishJob(j.id, 'failed', null, why);
    /* What a cancellation MEANS for the snippet, which is different for every
       kind — and used to be the transcribe answer for all of them, so
       cancelling a promote would have cleared the transcript of the clip being
       promoted. A promote or a purge says nothing about the row: the file is
       where it was, and the row already describes that. */
    if (j.snippet_id && j.kind === 'transcribe') {
      /* Back to `none`, not `failed`: nothing is wrong with the snippet,
         somebody stopped the machine. */
      W.prepare(`UPDATE snippet SET transcript_status = 'none', transcript_note = NULL,
                                    updated_at = ? WHERE id = ?`).run(t, j.snippet_id);
    } else if (j.snippet_id && j.kind === 'fetch') {
      /* This one does have to show. A link whose fetch was cancelled would
         otherwise sit in the submitter's panel saying `waiting for the
         recorder` for as long as the row exists, waiting on a job that is
         never coming. */
      W.prepare(`UPDATE snippet SET fetch_status = 'failed', fetch_note = ?,
                                    updated_at = ? WHERE id = ?`).run(why, t, j.snippet_id);
    }
    logEvent(req, j.kind === 'transcribe' ? 'cancelled a transcription'
                                          : `cancelled a ${j.kind}`,
             'snippet', j.snippet_id ?? j.id,
             { job_id: j.id, kind: j.kind, was_running: killed });
    bumpGeneration(W);
    res.json({ ok: true, status: 'failed', was_running: killed });
  });

  app.get('/api/grants', requireRole('admin'), (req, res) => {
    const gates = R.prepare(
      `SELECT t.gate AS name, count(DISTINCT st.snippet_id) AS clips,
              group_concat(DISTINCT t.slug) AS taglets,
              group_concat(DISTINCT t.id) AS taglet_ids
         FROM tag t LEFT JOIN snippet_taglet st ON st.tag_id = t.id
        WHERE t.gate IS NOT NULL AND t.retracted_at IS NULL
        GROUP BY t.gate ORDER BY t.gate`).all();
    const people = R.prepare(
      `SELECT p.id, p.handle, p.role, group_concat(g.name) AS grants
         FROM person p JOIN person_grant g ON g.person_id = p.id
        GROUP BY p.id ORDER BY p.handle`).all()
      .map((r) => ({ ...r, grants: String(r.grants ?? '').split(',').filter(Boolean) }));
    res.json({
      /* The ids alongside the slugs, because lifting a gate means clearing it
         from every taglet that carries it — and a panel holding only slugs
         would have to go and look each one up again to do that. */
      gates: gates.map((g) => ({
        ...g,
        taglets: String(g.taglets ?? '').split(',').filter(Boolean),
        taglet_ids: String(g.taglet_ids ?? '').split(',').filter(Boolean),
      })),
      people,
    });
  });

  /** What the signed-in person holds. Their own, so not admin-gated — a page
   *  that has to know whether to draw a lock needs this. */
  app.get('/api/auth/grants', (req, res) => {
    res.json({ grants: grantsOf(req.person?.id ?? null) });
  });

  app.post('/api/people/:id/grants', requireRole('admin'), (req, res) => {
    const name = String(req.body?.name ?? '').trim().toLowerCase();
    if (!name || name.length > 64) return res.status(400).json({ error: 'a grant needs a name' });
    const p = R.prepare('SELECT id FROM person WHERE id = ?').get(req.params.id);
    if (!p) return res.status(404).json({ error: 'no such person' });
    // Idempotent on the UNIQUE index: granting twice is not a second grant.
    W.prepare(
      `INSERT OR IGNORE INTO person_grant(id, person_id, name, granted_by, created_at)
       VALUES(?,?,?,?,?)`).run(ulid(), p.id, name, req.person.id, now());
    logEvent(req, 'granted', 'person', p.id, { gate: name });
    /* The generation is what expires every cached list — a grant that does not
       bump it leaves the person looking at the page they had before it, for as
       long as the validator holds. */
    bumpGeneration(W);
    res.status(201).json({ grants: grantsOf(p.id) });
  });

  app.delete('/api/people/:id/grants/:name', requireRole('admin'), (req, res) => {
    W.prepare('DELETE FROM person_grant WHERE person_id = ? AND name = ?')
      .run(req.params.id, String(req.params.name).toLowerCase());
    logEvent(req, 'revoked', 'person', req.params.id,
             { gate: String(req.params.name).toLowerCase() });
    bumpGeneration(W);
    res.json({ grants: grantsOf(req.params.id) });
  });

  /** Flag a taglet as gating, or clear it. Admin only, and pointedly not an
   *  editor: flagging `funny` as a gate would hide a third of the archive from
   *  everyone, which is a bigger blast radius than any single edit an editor
   *  can make. */
  app.post('/api/taglets/:id/gate', requireRole('admin'), (req, res) => {
    const t = R.prepare(
      'SELECT id, slug FROM tag WHERE id = ? AND retracted_at IS NULL').get(req.params.id);
    if (!t) return res.status(404).json({ error: 'no such taglet' });
    const raw = req.body?.gate;
    const gate = raw == null || raw === '' ? null
      : String(raw).trim().toLowerCase().slice(0, 64) || null;
    const was = R.prepare('SELECT gate FROM tag WHERE id = ?').get(t.id)?.gate ?? null;
    W.prepare('UPDATE tag SET gate = ?, updated_at = ? WHERE id = ?').run(gate, now(), t.id);
    logEvent(req, gate ? 'gated' : 'ungated', 'taglet', t.id,
             { slug: t.slug, gate, was });
    bumpGeneration(W);
    res.json({ taglet: { id: t.id, slug: t.slug, gate } });
  });

  // -------------------------------------------------------------------------
  // links — a clip the archive does not have yet
  //
  // Nothing here opens an outbound socket. The archive records that somebody
  // asked for a URL and publishes a `fetch` job; the recorder, which already
  // has yt-dlp, a network and its own opinion about which hosts are allowed,
  // decides whether to honour it. That split is the point: a compromised
  // archive can ask the recorder to fetch from an attacker's host and the
  // recorder will say no, because the allowlist that matters is in ls-rec's
  // config and not in this database.
  //
  // The check below is therefore a COURTESY, not a control. It exists so a
  // pasted Vimeo link is refused in the same second rather than sitting in a
  // queue for an hour before the Pi declines it.
  // -------------------------------------------------------------------------

  /* `cdn.discordapp.com` and not `discord.com`, and the difference is the
     whole of whether a Discord link works.
     `discord.com/channels/g/c/m` is a MESSAGE link: it carries no file, and
     resolving one means a bot sitting in that server. `cdn.discordapp.com/...`
     is the attachment itself — what Discord's own "Copy Link" gives you — and
     the recorder already downloads those directly, no yt-dlp and no account.
     The two lists had drifted into being exact opposites: this one took the
     shape the recorder refuses and refused the shape the recorder takes. */
  const LINK_HOSTS = (process.env.TENMA_LINK_HOSTS
    || 'youtube.com,youtu.be,twitch.tv,twitter.com,x.com,'
     + 'cdn.discordapp.com,media.discordapp.net')
    .split(',').map((s) => s.trim().toLowerCase()).filter(Boolean);

  /* ── which Discord channels may be pasted from ──────────────────────────
     A `cdn.discordapp.com` link with no further check is an arbitrary file
     upload with extra steps: anyone can drop anything into a server they made
     ten seconds ago, or into a DM with themselves, and copy the link. The
     host allowlist says nothing about that — every one of those is the same
     host.

     What the URL DOES carry is the channel id: /attachments/<channel>/<id>/
     <name>. Not the guild — Discord does not put the server in the URL at all
     — so "which server is this from" is a question only the API can answer,
     and answering it means a bot in every server. The channel is enough, and
     is arguably the better unit: an allowlist of the specific channels worth
     taking pictures from, rather than a whole server including whatever gets
     posted in #off-topic.

     The signature covers the path, so the channel id cannot be swapped for an
     allowlisted one — an edited URL simply stops validating at Discord's end.
     That is what makes this worth checking rather than decorative.

         TENMA_DISCORD_CHANNELS="123456789012345678=Phase Connect #memes,
                                 987654321098765432=Tenma's place #art"

     The label is YOURS. Discord will not tell us the server's name without an
     account, and a reviewer looking at a queued picture needs to know where it
     came from — so what you call the channel is what gets stored as the row's
     source. Get the ids from Discord: Settings → Advanced → Developer Mode,
     then right-click a channel → Copy Channel ID.

     UNSET REFUSES EVERY DISCORD LINK, and that is the point rather than an
     inconvenience. An archive that took pictures from anywhere the moment
     somebody forgot a config line would be exactly the open relay this exists
     to prevent — the same reasoning as TENMA_INGEST_TOKEN, which disables
     ingest rather than leaving it open. */
  const DISCORD_HOSTS = new Set(['cdn.discordapp.com', 'media.discordapp.net']);

  /** `id = label` entries into a Map, from either source. */
  const parseChannels = (entries) => new Map(entries
    .map((raw) => {
      const line = raw.trim();
      /* `#` starts a comment ONLY at the beginning of a line. Every Discord
         channel is named `#something`, so treating it as a comment marker
         anywhere would silently eat the label off every entry in the file. */
      if (!line || line.startsWith('#')) return null;
      const at = line.indexOf('=');
      const id = (at < 0 ? line : line.slice(0, at)).trim();
      const label = at < 0 ? '' : line.slice(at + 1).trim();
      return /^\d{15,25}$/.test(id) ? [id, label || `channel ${id}`] : null;
    })
    .filter(Boolean));

  /* A FILE, because this list is long and grows one channel at a time.
     Threads are channels in their own right — a picture posted in a thread
     carries the THREAD's id, not its parent's — so covering a server properly
     means an entry per thread as well, and fifty of those in a compose
     environment variable is one unreadable line that needs a container
     restart every time somebody adds a server.

         TENMA_DISCORD_CHANNELS_FILE=/data/discord-channels.txt

         # Phase Connect
         1024339912345678901 = #memes
         1024339912345678902 = #art

     Re-read when its mtime moves, so adding a line is live. The environment
     variable still works and is the fallback when no file is named; the file
     wins when both are set, because the file is the one somebody edits.

     Unreadable is treated as empty, which refuses every Discord link. A
     source-control list that failed OPEN when its file went missing would be
     the one failure mode this whole mechanism exists to prevent. */
  const DISCORD_FILE = (process.env.TENMA_DISCORD_CHANNELS_FILE ?? '').trim() || null;
  const DISCORD_ENV = parseChannels((process.env.TENMA_DISCORD_CHANNELS ?? '').split(','));
  let chanCache = { at: null, map: new Map() };

  const discordChannels = () => {
    if (!DISCORD_FILE) return DISCORD_ENV;
    let at = null;
    try { at = statSync(DISCORD_FILE).mtimeMs; } catch { /* gone, or never there */ }
    if (at !== chanCache.at) {
      let lines = [];
      try { lines = readFileSync(DISCORD_FILE, 'utf8').split(/\r?\n/); }
      catch { /* refuse everything rather than fall back to a looser list */ }
      chanCache = { at, map: parseChannels(lines) };
    }
    return chanCache.map;
  };

  /** Which allowlisted channel this attachment is from, or why not.
   *
   *  `/attachments/` only. Discord also serves `/ephemeral-attachments/`,
   *  which comes from an interaction rather than from anything anybody posted
   *  in a channel — there is no channel to vouch for it, so it is not a
   *  source this archive has an opinion about.
   */
  const discordChannel = (u) => {
    const m = /^\/attachments\/(\d{15,25})\//.exec(u.pathname);
    if (!m) return { ok: false, why: 'that is not a Discord attachment link' };
    const channels = discordChannels();
    if (!channels.size) {
      return { ok: false,
        why: `no Discord channels are allowed yet — ${DISCORD_FILE
          ? `nothing readable in ${DISCORD_FILE}`
          : 'set TENMA_DISCORD_CHANNELS_FILE'}` };
    }
    const label = channels.get(m[1]);
    if (!label) {
      return { ok: false,
        why: 'that channel is not one this archive takes pictures from' };
    }
    return { ok: true, id: m[1], label };
  };

  /** The URL as the archive will remember it, or null if it is not one we take.
   *
   *  Canonicalised before it is stored, because the duplicate check is a string
   *  comparison and `youtu.be/X`, `www.youtube.com/watch?v=X&t=42` and the same
   *  link with a tracking parameter are all the same clip. Getting this wrong
   *  does not break anything — it just lets the same video in twice.
   */
  const linkUrl = (raw) => {
    let u;
    try { u = new URL(String(raw ?? '').trim()); } catch { return null; }
    if (u.protocol !== 'https:' && u.protocol !== 'http:') return null;
    const host = u.hostname.toLowerCase().replace(/^www\./, '');
    if (!LINK_HOSTS.some((h) => host === h || host.endsWith(`.${h}`))) return null;
    u.protocol = 'https:';
    u.hostname = host;
    u.hash = '';
    u.port = '';

    /* youtu.be/X and youtube.com/watch?v=X are the same video and people paste
       both — the share button gives one and the address bar the other. Left
       alone they are two different strings, so the duplicate check misses and
       the recorder downloads it twice. */
    if (host === 'youtu.be') {
      const id = u.pathname.replace(/^\//, '').split('/')[0];
      if (id) { u.hostname = 'youtube.com'; u.pathname = '/watch'; u.search = `?v=${id}`; }
    }

    /* ONLY what identifies the video, and ONLY on YouTube. An earlier version
       kept `t` and `list` too, which meant the same clip linked from a
       playlist, or at a timestamp, read as a different submission — measured:
       the same video pasted twice was accepted twice. Neither changes which
       video gets downloaded, so neither belongs in the identity.

       Scoping it to the host it was written for is the correction. Unscoped,
       it ran on every link — and a Discord attachment URL is SIGNED, with the
       signature in the query string. `?ex=&is=&hm=` went the way of `t` and
       `list`, so what got stored was an unsigned URL that Discord answers 404
       to. Every picture link would have failed, and the reason would have been
       three characters long and invisible in the stored row. */
    if (host === 'youtube.com') {
      const keep = new Set(['v']);
      for (const k of [...u.searchParams.keys()]) if (!keep.has(k)) u.searchParams.delete(k);
      /* An empty query renders as a trailing "?" that makes two identical URLs
         compare unequal. */
      if (![...u.searchParams.keys()].length) u.search = '';
    } else if (host === 'media.discordapp.net') {
      /* Discord's resizing proxy. The same path on `cdn.` is the original, and
         an archive that quietly kept a 300px-wide copy of a picture because
         that is the link somebody happened to right-click would be an archive
         of thumbnails. The size parameters go; the signature stays. */
      for (const k of ['width', 'height', 'format', 'quality', 'size']) {
        u.searchParams.delete(k);
      }
      u.hostname = 'cdn.discordapp.com';
    }
    return u.href;
  };

  app.post('/api/uploads/link', requireRole('suggester'), (req, res) => {
    const me = req.person?.id ?? null;
    if (!me) return res.status(401).json({ error: 'sign in first' });

    /* Which collection, exactly as the byte route takes it. A picture arrives
       by link far more often than a clip does — a meme is something you saw
       somewhere and can point at, where a clip is something you cut — so the
       one collection that could NOT be linked was the one that most wanted
       it. */
    const kind = String(req.get('x-upload-kind') || req.body?.kind || 'snippet');
    if (!SNIPPET_KINDS.includes(kind)) {
      return res.status(400).json({
        error: `kind must be one of ${SNIPPET_KINDS.join(', ')}` });
    }
    const K = UP_KIND[kind];

    const url = linkUrl(req.body?.url);
    if (!url) {
      return res.status(400).json({
        error: `links from ${LINK_HOSTS.join(', ')} only`, hosts: LINK_HOSTS });
    }

    /* Where it came from, for the one host where "a link" and "a file upload"
       are the same gesture. Everywhere else the URL names a thing that was
       published — a video on a channel, a tweet — and the platform is the
       accountable party. A Discord attachment names a file somebody put
       somewhere, and the somewhere is the whole of what makes it different
       from an unrestricted upload endpoint. */
    let from = null;
    if (DISCORD_HOSTS.has(new URL(url).hostname)) {
      const ch = discordChannel(new URL(url));
      /* `link.any` and not a role test. The exemption is held by whoever can
         already put a file in by hand — gating them buys no safety and costs
         them every thread and forum post, which are separate channels whose
         ids nobody can enumerate in advance. Asked as a capability so that
         when roles become rows, this call site does not change. */
      if (!ch.ok && !can(req.person, 'link.any')) {
        return res.status(403).json({ error: ch.why });
      }
      /* The label when there is one even for an exempt submitter — being
         allowed to skip the list is not a reason to lose the provenance when
         the channel happens to be on it. */
      from = ch.ok ? ch.label : null;
    }

    /* The same quota as bytes. A link costs the archive nothing to accept and
       costs the RECORDER a download, so if anything it wants the tighter
       limit — but one number people can hold in their head beats two.

       Counted per collection, and against the same number the byte route
       uses. A shared count would mean a full meme queue blocking a clip
       nobody has even downloaded yet — one panel holding another one closed,
       which is the thing the per-collection caps exist to avoid. */
    const pending = R.prepare(
      `SELECT count(*) c FROM snippet
        WHERE author_id = ? AND kind = ? AND status = 'proposed'
          AND retracted_at IS NULL`).get(me, kind).c;
    if (pending >= K.pending) {
      return res.status(429).json({
        error: `you already have ${pending} ${pending === 1 ? K.one : K.noun}`
          + ' waiting for review',
        pending, limit: K.pending });
    }

    /* Same link twice. Same disclosure rule as the byte-level dedupe: refuse
       either way, but only NAME the twin to somebody who could already have
       found it — otherwise submitting a URL becomes a way to ask whether a
       gated clip came from it. */
    const twin = R.prepare(
      `SELECT id, title, status, author_id FROM snippet
        WHERE source_url = ? AND retracted_at IS NULL LIMIT 1`).get(url);
    if (twin) {
      const mayName = snipVisible(twin, req);
      return res.status(409).json({
        error: mayName
          ? 'that link is already in the archive'
          : 'that link is already in the archive — an editor can tell you more',
        ...(mayName ? { snippet: { id: twin.id, title: twin.title, status: twin.status } } : {}),
      });
    }

    /* A placeholder title from the URL, so the queue reads as something rather
       than as a row of identical "Untitled". The submitter renames it in the
       same panel while the recorder works. */
    const t = now(), id = ulid();
    const guess = (() => {
      try {
        const u = new URL(url);
        const v = u.searchParams.get('v');
        return `${u.hostname.replace(/^www\./, '')}${v ? ` ${v}` : u.pathname}`.slice(0, 120);
      } catch { return 'Pending fetch'; }
    })();

    /* file_path is NOT NULL, and a link has no file for as long as the
       recorder takes. Written as the name it WILL have — the same shape an
       upload uses, where file_path is a destination from the moment the row
       exists — and rewritten with the real extension when the bytes land.
       Nothing serves from it in between: the row is `proposed` with no
       quarantine_path, so every media route resolves it to nothing. */
    W.prepare(
      `INSERT INTO snippet(id, slug, title, kind, file_path, source_url, source,
                           transcript_status, fetch_status, status, origin,
                           author_id, added_at, created_at, updated_at)
       VALUES(?,?,?,?,?,?,?,'none','queued','proposed','link',?,?,?,?)`)
      .run(id, id.toLowerCase(),
           /* A picture is not named after where it came from, for the same
              reason it is not named after its file: `cdn.discordapp.com
              shiina_stare.png` is not a description, and it would read as one
              somebody chose. A clip keeps its guess — a YouTube id is at
              least the thing itself. */
           kind === 'snippet' ? guess : '',
           kind, `${KIND_DIR[kind]}/${id}`, url,
           /* The credit line, and its first real writer. `source` was added
              for exactly this and had nothing filling it: a reviewer looking
              at a queued picture needs to know where it came from, and the
              URL is not an answer anybody reads. */
           from, me, t, t, t);
    /* The job carries the URL and nothing path-like. The recorder resolves
       where to put the file from its own config — the archive never names a
       directory to a worker. */
    const jobId = enqueueJob('fetch', { snippetId: id, url, by: me });
    logEvent(req, 'linked', 'snippet', id, { source_url: url, from });
    bumpGeneration(W);

    res.status(201).json({
      snippet: snipRow(R.prepare('SELECT * FROM snippet WHERE id = ?').get(id), { me }),
      job_id: jobId,
      next: 'fetch',
    });
  });

  // -------------------------------------------------------------------------
  // jobs — what the archive asks the Pi to do
  //
  // The archive publishes intent; ls-rec subscribes. Nothing here opens an
  // outbound socket, parses a remote response, or writes a file. See the block
  // comment on `job` in db.js.
  // -------------------------------------------------------------------------

  /* `rescan` is deliberately absent, though the Pi does it. Every kind here
     can be hand-made through POST /api/jobs with a payload of the caller's
     choosing, and a rescan payload names URLs for the recorder to go and
     probe. The one route that makes them builds the payload out of capture
     rows instead, so the URLs are always ones the archive already recorded. */
  const JOB_KINDS = ['fetch', 'promote', 'purge', 'normalize', 'transcribe',
                     'ocr', 'music_probe', 'music_fetch', 'clip'];
  /* What the RECORDER may claim. `normalize` is missing on purpose: it runs in
     this process, against the cache mount, and a Pi that claimed one would
     hold a lease on work it cannot do and cannot see the files for. Kept as a
     separate list rather than by filtering at the call site, because "which
     kinds are the Pi's" is a fact about the system, not about one request. */
  /* `transcribe` is deliberately NOT here. It runs on this server, in the
     worker slot normalize owns, and two consumers of one kind means the same
     file transcribed twice — once by each, with whichever finished last
     overwriting the other. */
  /* `rescan` is the Pi's for the same reason promote is: the masters are on a
     mount this process can only read, and the platform probe wants a network
     and a cookie jar that live on the recorder. */
  /* Both music kinds are the Pi's: each one opens an outbound socket, and
     the archive opening one is the thing this whole arrangement exists to
     avoid. `music_probe` reads facts off a page; `music_fetch` downloads a
     video that an editor has already said yes to. */
  /* `clip` is the Pi's because the masters are on a mount this process can
     only read, and cutting one means ffmpeg with a seek — which is the
     recorder's job in the same way promote is. It is also the FIRST kind whose
     product is a file the person downloads rather than one the archive
     ingests, which is why it lands in quarantine: that is the only place this
     container may write, and a clip is a working file by definition. */
  const PI_KINDS = ['fetch', 'promote', 'purge', 'rescan', 'harvest',
                    'music_probe', 'music_fetch', 'clip'];
  /* `ocr` is not the Pi's either, and for a different reason than normalize:
     the model lives in a sidecar container on THIS docker network, so a Pi
     that claimed one would be holding a lease on work it cannot reach. */
  /* A clip that is published and whose master is still in quarantine. Written
     once because two things ask it — the panel, to say how many, and the sweep,
     to do something about them — and a count that disagreed with what the
     button then queued would be worse than either. */
  const strandedSql = `SELECT id, quarantine_path, file_path FROM snippet
      WHERE status = 'confirmed' AND retracted_at IS NULL
        AND quarantine_path IS NOT NULL AND file_path IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM job j
                         WHERE j.snippet_id = snippet.id AND j.kind = 'promote'
                           AND j.status IN ('approved', 'claimed'))
      LIMIT 500`;
  /* Five minutes. Long enough that a slow fetch is not stolen out from under
     the worker doing it, short enough that a worker killed mid-job frees its
     work before anyone notices. */
  const JOB_LEASE_S = 300;

  /** Put a job on the queue. Approved on creation, because every caller here
   *  is either an editor acting or the archive acting on its own behalf —
   *  `proposed` exists for the day a suggester may ask for one. */
  const enqueueJob = (kind, { snippetId = null, url = null, payload = null, by = null } = {}) => {
    const t = now(), id = ulid();
    W.prepare(
      `INSERT INTO job(id, kind, status, snippet_id, url, payload,
                       requested_by, approved_by, created_at, updated_at)
       VALUES(?,?,'approved',?,?,?,?,?,?,?)`)
      .run(id, kind, snippetId, url, payload ? JSON.stringify(payload) : null, by, by, t, t);
    return id;
  };

  /* What a rescan found, written down.
   *
   * Per capture, and only for the captures the worker actually answered for —
   * a job that could reach one URL and not the other must not silently mark
   * the second one anything.
   */
  const PROBE_INT = ['file_duration_s', 'width', 'height'];
  const PROBE_TEXT = ['container', 'video_codec', 'audio_codec'];

  function rescanLanded(job, status, result) {
    if (status !== 'done' || !result || typeof result !== 'object') return;
    const t = now();
    const rows = Array.isArray(result.captures) ? result.captures : [];
    const seen = new Set();
    for (const r of rows) {
      const id = String(r?.id ?? '');
      if (!id) continue;
      const cap = W.prepare('SELECT id, stream_id FROM capture WHERE id = ?').get(id);
      if (!cap) continue;
      const set = [], vals = [];
      const put = (col, v) => { set.push(`${col} = ?`); vals.push(v); };

      /* Only on a definite answer. `alive` absent, null, or anything that is
         not 0/1 means the worker could not tell — a network error, a bot
         check, a timeout — and a probe that failed is not a VOD that died.
         Leaving the column alone is the whole point of it being nullable. */
      if (r.alive === 0 || r.alive === 1) { put('alive', r.alive); put('checked_at', t); }

      for (const k of PROBE_INT) {
        if (Number.isFinite(Number(r[k]))) put(k, Math.trunc(Number(r[k])));
      }
      for (const k of PROBE_TEXT) {
        if (typeof r[k] === 'string' && r[k]) put(k, r[k].slice(0, 60));
      }
      if (Number.isFinite(Number(r.fps))) put('fps', Number(r.fps));
      if (r.has_audio === 0 || r.has_audio === 1) put('has_audio', r.has_audio);
      // Stamped only when the file itself was read. It is the answer to "when
      // was this last ffprobed", and a URL probe does not answer it.
      if (r.file_duration_s !== undefined && r.file_duration_s !== null) put('probed_at', t);

      if (!set.length) continue;
      vals.push(t, id);
      W.prepare(`UPDATE capture SET ${set.join(', ')}, updated_at = ? WHERE id = ?`)
        .run(...vals);
      seen.add(cap.stream_id);
    }
    /* Everything downstream is derived: vod_state, the duration, the watch
       chain's ordering. Nothing here has to know that a dead Twitch VOD now
       sorts below the mirror — the ranks have always said so. */
    for (const sid of seen) {
      try { recompute(W, sid, { mediaRoot: config.mediaRoot, checkFiles: !!config.mediaRoot }); }
      catch (e) { console.error(`rescan recompute ${sid}:`, e?.message ?? e); }
    }
  }

  /* What a harvest found, written down.
   *
   * The worker reads a page and reports a title, a description and — if it
   * pulled one — an image already sitting in quarantine, named by result_path
   * the same way a fetch names its download. Nothing here reaches the network;
   * that is the entire reason this is a job.
   *
   * It writes DIRECTLY rather than through a changeset, for the same reason
   * ingest does: this is an observation of somebody else's page, not a
   * decision. What makes that safe is `seeded` — the row says out loud that a
   * machine wrote it and no human has been over it, and the first hand edit
   * clears the flag in the applier.
   */
  /** A harvest has two answers now, and only one of them touches the tag.
   *
   *  A SEARCH answers with candidates. Those are parked on the job's own
   *  payload and written nowhere else: nobody has decided anything yet, and a
   *  search that quietly rewrote the row would be the auto-apply this whole
   *  design exists to avoid — three different games are called Summer Camp.
   *  The page reads them back through GET /api/jobs/:id, which already exists
   *  for exactly this question: is THIS one finished yet.
   *
   *  An ART fetch answers with a file. The worker wrote it where the payload
   *  told it to, inside the media tree, so all that is left is to point the
   *  row at it.
   */
  function harvestLanded(job, status, result, resultPath) {
    let pay = null;
    try { pay = job.payload ? JSON.parse(job.payload) : null; } catch { /* not ours to fix */ }
    const id = String(pay?.tag_id ?? '');
    if (!id) return;
    const t = now();

    /* Candidates first, and recorded even on a FAILED search: "IGDB refused
       us" is an answer the page has to be able to draw, and the error is
       already on the job row beside them. */
    if (Array.isArray(result?.candidates)) {
      const https = (v) => (/^https:\/\//i.test(String(v ?? '')) ? String(v).slice(0, 600) : null);
      const trim = result.candidates.slice(0, 8).map((c) => ({
        name: String(c?.name ?? '').slice(0, 200),
        summary: String(c?.summary ?? '').slice(0, 4000),
        art: https(c?.art), url: https(c?.url),
        meta: String(c?.meta ?? '').slice(0, 200),
        exact: !!c?.exact, twins: Number(c?.twins) || 0,
      })).filter((c) => c.name);
      W.prepare('UPDATE job SET payload = ?, updated_at = ? WHERE id = ?')
        .run(JSON.stringify({ ...pay, candidates: trim }), t, job.id);
      bumpGeneration(W);
      return;
    }

    if (status !== 'done') return;
    const tag = W.prepare('SELECT id, thumb_path FROM tag WHERE id = ?').get(id);
    if (!tag) return;

    /* The art, at the name THIS JOB asked for and no other. Checked against
       the payload rather than against a shape, because the archive minted that
       name — a worker answering with some other path inside `posters/` is a
       worker writing where it was not asked to, and the row must not follow it
       there. */
    const want = String(pay?.art_to ?? '');
    if (want && resultPath === want
        && /^posters\/[0-9A-HJKMNP-TV-Z]{26}\.(jpg|png)$/.test(want)) {
      W.prepare('UPDATE tag SET thumb_path = ?, seeded = 1, updated_at = ? WHERE id = ?')
        .run(want, t, id);
      bumpGeneration(W);
    }
    if (!result || typeof result !== 'object') return;

    const set = [], vals = [];
    const put = (col, v) => { set.push(`${col} = ?`); vals.push(v); };
    if (typeof result.summary === 'string' && result.summary.trim()) {
      put('summary', result.summary.trim().slice(0, 4000));
    }
    if (!set.length) return;
    // Set LAST so the writes above do not have to care about ordering, and so
    // a harvest that found nothing leaves the flag exactly as it was.
    put('seeded', 1);
    vals.push(t, id);
    W.prepare(`UPDATE tag SET ${set.join(', ')}, updated_at = ? WHERE id = ?`).run(...vals);
    bumpGeneration(W);
  }

  /* ── what the two music jobs report ──────────────────────────────────────
   *
   * Both write DIRECTLY rather than through a changeset, for the same reason
   * `harvest` does: this is an OBSERVATION of somebody else's page, not a
   * decision the archive is taking. Nobody has to answer for what YouTube says
   * the upload date is. The editorial decisions around it — approving it,
   * retracting it, correcting a title — all still go through changesets and
   * still have an author.
   */
  const musicById = (id) => R.prepare('SELECT * FROM music WHERE id = ?').get(id);
  const musicJobTarget = (job) => {
    try { return String(JSON.parse(job.payload ?? '{}')?.music_id ?? '') || null; }
    catch { return null; }
  };

  /** Facts only: what the video is called, who uploaded it, when, how long. */
  function musicProbeLanded(job, status, result, error) {
    const id = musicJobTarget(job);
    if (!id || !musicById(id)) return;
    const t = now();
    if (status !== 'done' || !result || typeof result !== 'object') {
      /* A failed probe is not a failed submission. The row keeps its link and
         its tags and stays reviewable — an editor can watch the embed, which
         is where the title was going to come from anyway. */
      W.prepare(`UPDATE music SET probe_status = 'failed', probe_note = ?, updated_at = ?
                  WHERE id = ?`)
        .run(String(error ?? 'the recorder could not read it').slice(0, 300), t, id);
      bumpGeneration(W);
      return;
    }
    const str = (v, n) => (typeof v === 'string' && v.trim() ? v.trim().slice(0, n) : null);
    const int = (v) => (Number.isFinite(Number(v)) ? Math.trunc(Number(v)) : null);
    /* Read before the write, because the concert guess is only made ONCE: the
       first time this archive learns how long the thing is. A re-probe that
       returns the same number must not re-attach a tag somebody has since
       taken off, and "duration_s was empty and now is not" is exactly the
       moment nobody has had a chance to disagree yet. */
    const knew = musicById(id)?.duration_s ?? null;
    W.prepare(
      `UPDATE music SET title = COALESCE(?, title), channel = COALESCE(?, channel),
                        channel_id = COALESCE(?, channel_id),
                        uploaded_at = COALESCE(?, uploaded_at),
                        duration_s = COALESCE(?, duration_s),
                        probe_status = 'done', probe_note = NULL, updated_at = ?
        WHERE id = ?`)
      .run(str(result.title, 300), str(result.channel, 200), str(result.channel_id, 64),
           int(result.uploaded_at), int(result.duration_s), t, id);
    const secs = int(result.duration_s);
    if (!knew && secs !== null && secs >= CONCERT_MIN_S) markConcert(id);
    bumpGeneration(W);
  }

  /** The preservation copy. Only ever queued after an approval. */
  function musicFetchLanded(job, status, resultPath, error, result) {
    const id = musicJobTarget(job);
    if (!id || !musicById(id)) return;
    const t = now();
    if (status !== 'done') {
      /* The entry stays published. It is the PRESERVATION that failed, not the
         song — the embed still plays, and a retry is an editor's call. */
      W.prepare(`UPDATE music SET fetch_status = 'failed', fetch_note = ?, updated_at = ?
                  WHERE id = ?`)
        .run(String(error ?? 'the recorder could not fetch it').slice(0, 300), t, id);
      bumpGeneration(W);
      return;
    }
    /* A path relative to the MEDIA root, not to quarantine. Music skips the
       quarantine-then-promote dance that an upload needs, because the approval
       already happened — the bytes are only ever asked for once a human has
       said yes, so there is nothing left to hold them for.
       Still resolved rather than trusted: resolveMedia does the containment,
       so `../../etc/passwd` from a compromised worker resolves to null. */
    const rel = String(resultPath ?? '').trim();
    const abs = rel && config.mediaRoot ? resolveMedia(config.mediaRoot, rel) : null;
    if (!abs || !existsSync(abs)) {
      W.prepare(`UPDATE music SET fetch_status = 'failed', fetch_note = ?, updated_at = ?
                  WHERE id = ?`)
        .run('the recorder reported a file that is not under the media root', t, id);
      bumpGeneration(W);
      return;
    }
    const thumb = (() => {
      const p = String(result?.thumb_path ?? '').trim();
      if (!p) return null;
      return config.mediaRoot && resolveMedia(config.mediaRoot, p) ? p : null;
    })();
    W.prepare(
      `UPDATE music SET video_path = ?, thumb_path = COALESCE(?, thumb_path),
                        bytes = ?, fetch_status = 'done', fetch_note = NULL, updated_at = ?
        WHERE id = ?`)
      .run(rel, thumb, Number.isFinite(Number(result?.bytes)) ? Math.trunc(result.bytes) : null,
           t, id);
    bumpGeneration(W);
  }

  /** What a queue row is ABOUT, in words, for whichever kind it is.
   *
   *  The panel had one answer to this and it was `snippet.title`, reached
   *  through the only join the query made. So every kind that is not about a
   *  snippet — `clip`, `harvest`, `rescan`, and both music kinds — rendered as
   *  **"a snippet that is gone"**, which is the interface inventing a missing
   *  row: a clip cut out of a capture never had a snippet to lose. A queue
   *  whose rows say a thing has been destroyed when it has not is worse than
   *  one with no labels at all, because it sends you looking.
   *
   *  Built HERE and not in the page, because this is where the payload and the
   *  rows it names both are. `null` is reserved for the one case that sentence
   *  is actually true of: a job that names a snippet which is no longer there.
   */
  function jobLabel(r) {
    let p = null;
    try { p = r.payload ? JSON.parse(r.payload) : null; } catch { /* not ours to fix here */ }

    if (r.kind === 'clip') {
      /* `stream_idx` is a label and nullable, so it is offered and not relied
         on; the duration is the one thing every clip job has. */
      const head = p?.stream_idx ? `#${p.stream_idx}` : 'a clip';
      const len = Number(p?.duration_s) > 0 ? ` · ${hms(p.duration_s)}` : '';
      const from = p?.live ? ' out of the live recording' : ' out of the master';
      return `${head}${len}${from}${p?.label ? ` — ${p.label}` : ''}`;
    }
    if (r.kind === 'harvest') {
      return p?.name ? `a description for ${p.name}` : 'a description for a tag';
    }
    if (r.kind === 'rescan') {
      const n = Array.isArray(p?.captures) ? p.captures.length : 0;
      return n ? `re-read ${n} capture${n === 1 ? '' : 's'}` : 're-read a stream';
    }
    if (r.kind === 'music_probe' || r.kind === 'music_fetch') {
      const verb = r.kind === 'music_probe' ? 'read' : 'download';
      /* `channel`, not an artist column — there is not one. A song that has
         only just been linked has neither yet, because the title IS what the
         probe is on its way to find out; hence the plain fallback. */
      const m = p?.music_id
        ? R.prepare('SELECT title, channel FROM music WHERE id = ?').get(p.music_id) : null;
      const name = [m?.channel, m?.title].filter(Boolean).join(' — ');
      return name ? `${verb} ${name}` : `${verb} a song`;
    }
    /* The snippet kinds, where the old answer was the right one. A `fetch`
       carries a url and no snippet until the bytes land, which is why the
       panel draws that one from `url` and this leaves it alone. */
    if (r.kind === 'fetch' && r.url) return null;
    return r.title ?? null;
  }

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
    const id = enqueueJob(kind, {
      snippetId: snippet_id, url: url ? String(url) : null, payload,
      by: req.person?.id ?? null });
    res.status(201).json({ job: jobRow(R.prepare('SELECT * FROM job WHERE id = ?').get(id)) });
  });

  /** The queue, for a human. */
  /* One job, by id. The list route filters by status, which cannot answer
     "is THIS one finished yet" — and polling a 50-row list to find one row is
     a lot of rows to move to learn one word. */
  app.get('/api/jobs/:id', requireRole('editor'), (req, res) => {
    const j = R.prepare('SELECT * FROM job WHERE id = ?').get(req.params.id);
    if (!j) return res.status(404).json({ error: 'no such job' });
    res.json({ job: jobRow(j) });
  });

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
  /* How long a claim may be held open, and why there is a ceiling at all.
     Long enough that a person pressing a button is not waiting on a poll
     interval; short enough to answer before anything in front of this decides
     an idle socket is dead. The worker reaches this over the tailnet, where
     there is nothing in front of it, but the number should be safe if that
     ever stops being true. */
  const CLAIM_WAIT_MAX_S = 30;
  /* How often the held-open claim looks. 250ms is imperceptible next to a
     network round trip and it is ONE indexed SELECT — `ix_job_claim` is
     (status, kind, created_at) and the queue is nearly all `done`. */
  const CLAIM_POLL_MS = 250;
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

  app.post('/api/ingest/jobs/claim', requireIngest, async (req, res) => {
    const worker = String(req.body?.worker ?? 'unknown').slice(0, 64);
    const want = Math.min(Math.max(Number(req.body?.limit ?? 1) || 1, 1), 10);
    const kinds = [].concat(req.body?.kinds ?? PI_KINDS)
      .map(String).filter((k) => PI_KINDS.includes(k));
    if (!kinds.length) {
      return res.status(400).json({ error: `kinds must be some of ${PI_KINDS}` });
    }
    /* `wait` — hold this open until there is something, rather than answering
       "nothing" and making the worker sleep twenty seconds. Absent or 0 is the
       old behaviour exactly, so a worker that has not been updated is
       unaffected.

       THE WAIT HAPPENS OUTSIDE THE TRANSACTION, and that is not a detail. The
       claim below opens `BEGIN IMMEDIATE`, which takes the write lock; holding
       that for twenty-five seconds would block every write in the archive for
       twenty-five seconds — every changeset, every note, every upload. So the
       loop does a cheap unlocked read and only opens the transaction once that
       read says there is a row to take. Backwards, this is not a latency
       improvement, it is a global write lock with a timer on it. */
    const wait = Math.min(Math.max(Number(req.body?.wait ?? 0) || 0, 0), CLAIM_WAIT_MAX_S);
    /* Whether the caller is still there, from `close` on the RESPONSE.
       Not `req.destroyed`, which was the first version of this and is a trap:
       `destroyed` on an IncomingMessage is set when the request stream has
       been fully READ, not when the socket dies — and express.json() reads the
       body to completion before the handler runs. So the guard was true on
       every single request, the loop returned without answering, and every
       long poll hung until the client gave up. It looked exactly like a
       network fault. */
    let gone = false;
    res.on('close', () => { gone = true; });
    const marks = kinds.map(() => '?').join(',');
    const peek = R.prepare(
      `SELECT 1 FROM job
        WHERE kind IN (${marks})
          AND (status = 'approved'
               OR (status = 'claimed' AND (claimed_at IS NULL OR claimed_at < ?)))
        LIMIT 1`);

    /* `let`, and re-read after the wait. A claim held open for twenty-five
       seconds would otherwise stamp `claimed_at` with the time it ARRIVED and
       measure the lease from there — a lease a third short, on the one path
       where the delay is deliberate. */
    let t = now();
    if (wait) {
      const until = Date.now() + wait * 1000;
      /* Recorded on the FIRST look, before any waiting: this is what proves
         the worker is alive and proves which kinds it will never ask for, and
         a poll that ends up waiting the full twenty-five seconds must not be
         invisible for those twenty-five seconds. */
      try { tx(W, () => setMeta(W, 'worker_poll', JSON.stringify({ at: t, worker, kinds }))); }
      catch { /* a note about a poll is not worth failing the poll over */ }
      while (!peek.get(...kinds, now() - JOB_LEASE_S)) {
        if (Date.now() >= until) return res.json({ jobs: [], lease_s: JOB_LEASE_S, waited: wait });
        // The caller hanging up mid-wait is the normal end of a long poll.
        if (gone) return;
        await sleep(CLAIM_POLL_MS);
      }
      t = now();
    }
    try {
      /* One transaction, so two polls arriving together cannot both claim the
         same row. A lapsed lease is re-claimable — a worker that died holding
         one must not park its job forever — and `attempts` is what makes that
         visible rather than silent. */
      const rows = tx(W, () => {
        /* What was asked for, before finding out whether there was any of it.
           A poll that comes up empty is exactly the poll worth recording: it
           is the one that proves the worker is alive and proves which kinds it
           will never take. No bumpGeneration — see the note where the panel
           reads this back. */
        setMeta(W, 'worker_poll', JSON.stringify({ at: t, worker, kinds }));
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
        /* Older promote and purge jobs carry a snippet id and nothing else —
           they were made before the payload carried the two names. The worker
           refuses a job that does not name a file, which is right and also
           unhelpful, because the archive has both names sitting in the row.
           So they are filled in here, from that row, and WRITTEN BACK: the job
           becomes self-contained the way a new one is, rather than being
           reinterpreted differently on every claim. */
        const fill = W.prepare('UPDATE job SET payload = ?, updated_at = ? WHERE id = ?');
        for (const r of found) {
          mark.run(worker, t, t, r.id);
          if (r.payload || !r.snippet_id || !['promote', 'purge'].includes(r.kind)) continue;
          const s = W.prepare(
            'SELECT quarantine_path, file_path FROM snippet WHERE id = ?').get(r.snippet_id);
          const p = r.kind === 'promote'
            ? (s?.quarantine_path && s?.file_path
                ? { from: s.quarantine_path, to: s.file_path } : null)
            : (s?.file_path ? { path: s.file_path } : null);
          if (!p) continue;
          r.payload = JSON.stringify(p);
          fill.run(r.payload, t, r.id);
        }
        return found;
      });
      if (rows.length) bumpGeneration(W);
      res.json({ jobs: rows.map((r) => jobRow({ ...r, status: 'claimed',
                                                claimed_by: worker, claimed_at: t,
                                                attempts: r.attempts + 1 })),
                 lease_s: JOB_LEASE_S });
    } catch (e) { return changeError(res, e); }
  });

  /** What a finished job MEANS for the snippet it was about.
   *
   *  The report endpoint used to record the job and stop, which left both
   *  interesting kinds inert: a `fetch` that succeeded had bytes sitting in
   *  quarantine that no row pointed at, and a `promote` that succeeded left
   *  `quarantine_path` set on a row whose file the recorder had just moved —
   *  so the media route went on serving from a path that was now empty.
   *
   *  Kept separate from the endpoint because it is the interesting half and
   *  because it must never throw into it: a worker that did its work and got a
   *  500 for saying so will do the work again.
   */
  function jobLanded(job, status, resultPath, error, result = null) {
    if (job.kind === 'rescan') return void rescanLanded(job, status, result);
    if (job.kind === 'harvest') return void harvestLanded(job, status, result, resultPath);
    if (job.kind === 'music_probe') return void musicProbeLanded(job, status, result, error);
    if (job.kind === 'music_fetch') {
      return void musicFetchLanded(job, status, resultPath, error, result);
    }
    if (job.kind === 'clip') {
      /* What the cut turned out to BE, folded onto the job's own payload.
         There is no `result` column on `job` and this does not want one: the
         payload is already the record of what was asked for, so the answer
         belongs beside the question rather than in a second place every
         reader would have to learn to join. It is also the only way the panel
         can say a clip came back short — a live cut is trimmed at the live
         edge by the recorder, and without this the UI would hand over a
         41-second file while still claiming it asked for sixty. */
      if (status !== 'done' || !result) return;
      let pay = {};
      try { pay = JSON.parse(job.payload ?? '{}') || {}; } catch { pay = {}; }
      pay.got = result;
      W.prepare('UPDATE job SET payload = ?, updated_at = ? WHERE id = ?')
        .run(JSON.stringify(pay), now(), job.id);
      return;
    }
    if (!job.snippet_id) return;
    const t = now();

    if (job.kind === 'promote') {
      if (status !== 'done') return;
      /* The master is in the media tree now, so the row stops claiming it is
         in quarantine. `file_path` was written at accept time as the
         destination it WOULD have — clearing quarantine_path is what finally
         makes that true, and is why promote never had to rewrite a path
         everything else reads. */
      W.prepare('UPDATE snippet SET quarantine_path = NULL, updated_at = ? WHERE id = ?')
        .run(t, job.snippet_id);
      return;
    }

    if (job.kind !== 'fetch') return;

    if (status !== 'done') {
      W.prepare(
        `UPDATE snippet SET fetch_status = 'failed', fetch_note = ?, updated_at = ?
          WHERE id = ?`)
        .run(String(error ?? 'the recorder could not fetch it').slice(0, 300), t, job.snippet_id);
      return;
    }

    /* A filename, not a path. The recorder reports what it called the file and
       the archive resolves it against its OWN quarantine root — resolveMedia
       does the containment, so `../../etc/passwd` from a compromised worker
       resolves to null rather than to a file. */
    const rel = String(resultPath ?? '').trim();
    const abs = rel && config.quarantineRoot
      ? resolveMedia(config.quarantineRoot, rel) : null;
    if (!abs || !existsSync(abs)) {
      W.prepare(
        `UPDATE snippet SET fetch_status = 'failed', fetch_note = ?, updated_at = ?
          WHERE id = ?`)
        .run('the recorder reported a file that is not in quarantine', t, job.snippet_id);
      return;
    }

    /* Only the cheap facts here. Codecs, duration, the hash and the duplicate
       check all happen in the normalize worker, which is already probing this
       exact file a second later and is off the request path — the recorder
       should not be kept waiting on ffprobe for its own status report. */
    /* The destination follows the ROW's collection, not the default one. Only
       snippets can be linked today, so this reads 'snippets' every time — but
       the alternative is a literal that is silently wrong the day a meme can
       be, and the row already knows the answer. */
    const kind = R.prepare('SELECT kind FROM snippet WHERE id = ?')
      .get(job.snippet_id)?.kind ?? 'snippet';
    W.prepare(
      `UPDATE snippet SET quarantine_path = ?, file_path = ?, fetch_status = 'done',
                          fetch_note = NULL, normalize_status = 'queued', updated_at = ?
        WHERE id = ?`)
      .run(rel, `${KIND_DIR[kind] ?? 'snippets'}/${rel}`, t, job.snippet_id);
    enqueueJob('normalize', { snippetId: job.snippet_id });
  }

  /** How a worker says what happened. `failed` is terminal on purpose. */
  app.post('/api/ingest/jobs/:id', requireIngest, (req, res) => {
    /* `result` is how a job answers with FACTS rather than with a verdict.
       Only rescan sends one; everything else says done or failed and where it
       put the file. */
    const { status, result_path = null, error = null, result = null } = req.body ?? {};
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
    /* Never let this fail the report. The job IS recorded by the line above;
       a worker that finished its work and got a 500 for saying so will do the
       work again, and for a fetch that means downloading it twice. */
    try { jobLanded(j, status, result_path, error, result); }
    catch (e) { console.error(`job ${j.id} landed badly:`, e?.message ?? e); }
    bumpGeneration(W);
    res.json({ job: jobRow(R.prepare('SELECT * FROM job WHERE id = ?').get(j.id)) });
  });

  /** Ask for a fresh transcript.
   *
   *  Note what this does NOT do: delete the transcript that is there. The
   *  obvious version clears it first and asks whisper for a new one, and that
   *  is exactly wrong for this archive — every transcript here has been
   *  hand-corrected, whisper has never once spelled a VTuber's name right, and
   *  between the clearing and the arrival there is a window where the clip has
   *  no transcript at all. If the worker never runs, that window is forever.
   *
   *  So the old one stands until a new one lands and replaces it. The confirm
   *  in the page says so, because "your corrections will be overwritten" is
   *  the thing worth warning about and "it will be empty for a while" is not
   *  a thing that should be true.
   */
  app.post('/api/snippets/:id/retranscribe', requireRole('editor'), (req, res) => {
    const s = R.prepare(
      'SELECT id FROM snippet WHERE id = ? AND retracted_at IS NULL').get(req.params.id);
    if (!s) return res.status(404).json({ error: 'no such snippet' });
    // One at a time. Pressing it twice should not mean transcribing twice.
    const open = R.prepare(
      `SELECT id FROM job WHERE snippet_id = ? AND kind = 'transcribe'
        AND status IN ('approved', 'claimed')`).get(s.id);
    if (open) return res.json({ job_id: open.id, already: true });
    /* `force`, because this is somebody deciding. The automatic pass refuses
       to touch a transcript a human has corrected; pressing the button IS the
       decision to replace it, and the log records it as discarding work. */
    const id = enqueueJob('transcribe',
      { snippetId: s.id, by: req.person?.id ?? null, payload: { force: true } });
    /* Worth a line because it DISCARDS: whatever a human corrected is about to
       be overwritten by a machine's next guess, and "why did my fixes go" has
       to have an answer. */
    logEvent(req, 'queued a re-transcribe', 'snippet', s.id, { job_id: id });
    bumpGeneration(W);
    res.status(201).json({ job_id: id, already: false });
  });

  /** Delete the bytes. Admin only, and deliberately not part of rejecting.
   *
   *  Rejection is reversible — the review queue has `back to queue`, and a
   *  reset that pointed at a deleted file would be an undo that does not undo.
   *  So reject unlists and purge deletes, which is also what the original
   *  design asked for: "editor or admin can unlist, only admin can actually
   *  delete it".
   *
   *  Split by WHERE the bytes are, and that split is the whole file-safety
   *  rule restated: quarantine is the server's to write, so it deletes there
   *  directly; the media tree is mounted read-only and belongs to the Pi, so
   *  anything living there becomes a job. One of these is a syscall and the
   *  other is a request — and the second is the only kind of file operation
   *  this process is allowed to ASK for rather than do.
   */
  app.post('/api/snippets/:id/purge', requireRole('admin'), (req, res) => {
    const s = R.prepare(
      // `title` is not decoration: this row is about to become a tombstone, and
      // the log line naming what was destroyed is the only place the name
      // survives in a form anyone will read.
      `SELECT id, title, status, quarantine_path, file_path, play_path, poster_path
         FROM snippet WHERE id = ?`).get(req.params.id);
    if (!s) return res.status(404).json({ error: 'no such snippet' });
    /* A published clip has to be unlisted first. Not bureaucracy: this is the
       one irreversible action in the archive, and requiring the reversible one
       before it means a slip costs a click rather than a master. */
    if (s.status === 'confirmed') {
      return res.status(409).json({
        error: 'reject or unlist it first — a published clip cannot be purged in one step' });
    }

    const gone = [];
    if (s.quarantine_path && config.quarantineRoot) {
      const abs = resolveMedia(config.quarantineRoot, s.quarantine_path);
      if (abs) { try { rmSync(abs, { force: true }); gone.push('quarantine'); } catch { /* already */ } }
    }
    /* Derivatives are the server's too, and leaving them is how the cache
       fills with the playable copy of something that no longer exists. */
    for (const rel of [s.play_path, s.poster_path]) {
      if (!rel || !config.cacheRoot) continue;
      const abs = resolveMedia(config.cacheRoot, rel);
      if (abs) { try { rmSync(abs, { force: true }); gone.push('cache'); } catch { /* already */ } }
    }

    /* In the media tree, so not ours to delete. `quarantine_path` being null
       is what says it was promoted — an upload that never got there still has
       its bytes in quarantine and was handled above. */
    let job = null;
    if (!s.quarantine_path && s.file_path) {
      /* The one name that matters, before the row stops holding it. Two lines
         below this, `retracted_at` is set and the paths are cleared — a purge
         job that had to look the row up afterwards would find a tombstone with
         nothing left to name. */
      job = enqueueJob('purge', { snippetId: s.id, by: req.person.id,
                                  payload: { path: s.file_path } });
    }

    const t = now();
    W.prepare(
      `UPDATE snippet SET quarantine_path = NULL, play_path = NULL, poster_path = NULL,
                          waveform = NULL, retracted_at = ?, updated_at = ?
        WHERE id = ?`).run(t, t, s.id);
    /* The line this whole table exists for. The row survives as a tombstone,
       so this stays readable — but only from the archive-wide log, because a
       purged snippet is in no list to open a panel from. */
    logEvent(req, 'purged', 'snippet', s.id,
             { removed: [...new Set(gone)], master: !!job, title: s.title ?? null });
    bumpGeneration(W);
    res.json({ ok: true, removed: [...new Set(gone)], job_id: job });
  });

  /** The recorder's queue: what has been asked of the Pi, and whether it came.
   *
   *  Deliberately not /api/jobs with a filter. That one is the raw table for
   *  an editor who knows what a job is; this one answers the question an admin
   *  actually has, which is "did the thing I pressed happen", and most of its
   *  work is the two derived facts at the bottom rather than the rows.
   */
  app.get('/api/recorder', requireRole('admin'), (req, res) => {
    const t = now();
    const marks = PI_KINDS.map(() => '?').join(',');
    const rows = R.prepare(
      /* `j.payload` is here for jobLabel(), which is the only reason a row
         that is not about a snippet can say what it is about. */
      `SELECT j.id, j.kind, j.status, j.snippet_id, j.url, j.payload, j.attempts, j.error,
              j.claimed_by, j.claimed_at, j.created_at, j.updated_at, j.finished_at,
              j.result_path, s.title, s.retracted_at
         FROM job j LEFT JOIN snippet s ON s.id = j.snippet_id
        WHERE j.kind IN (${marks})
          -- Dismissed jobs are excluded outright rather than windowed like the
          -- other terminal states. Dismissing one is somebody saying "stop
          -- showing me this", so keeping it on the card for another day would
          -- answer the only thing the verb is for with "no". The row and the
          -- event log keep it.
          AND j.status <> 'dismissed'
          AND (j.status IN ('approved', 'claimed', 'paused') OR j.finished_at > ?)
        ORDER BY CASE j.status WHEN 'claimed' THEN 0 WHEN 'approved' THEN 1
                               WHEN 'paused' THEN 2 ELSE 3 END,
                 j.created_at
        LIMIT 200`).all(...PI_KINDS, t - 24 * 3600);

    /* Who last took anything, and when. There is no ping from the worker and
       there should not be one: a heartbeat would say a process is running,
       and what matters is whether it is running AND can reach this archive
       AND is pointed at the right roots — all of which a claim proves and a
       ping does not. The cost is that a quiet day says nothing, which is why
       the staleness below is measured on the QUEUE and not on this. */
    const last = R.prepare(
      `SELECT claimed_by, claimed_at FROM job
        WHERE kind IN (${marks}) AND claimed_by IS NOT NULL
        ORDER BY claimed_at DESC LIMIT 1`).get(...PI_KINDS);

    const waiting = rows.filter((r) => r.status === 'approved');

    /* WHAT THE WORKER ASKED FOR, last time it asked.
     *
     * The gap this closes: a job can sit `approved` for ever while the worker
     * is plainly alive and taking other things, and until now the panel had no
     * way to say why. `kinds` below is the ARCHIVE's list — what it is willing
     * to hand out — and the worker sends its own on every poll, filtered
     * against `ls_archive.PI_KINDS` and then against `archive_job_kinds` in
     * its config or a `--kinds` flag. So a Pi whose config predates a kind
     * asks for everything except that kind, gets handed nothing, reports
     * nothing wrong, and the queue waits for ever.
     *
     * Recorded on the claim rather than pinged: same argument as `worker`
     * below. It is one INSERT inside a transaction the claim already opens,
     * and it deliberately does NOT bump the generation — a counter that moved
     * every twenty seconds would invalidate every client's cache for ever. */
    let poll = null;
    try {
      const raw = meta(R, 'worker_poll', null);
      if (raw) poll = JSON.parse(raw);
    } catch { /* a malformed note about a poll is not worth a 500 */ }
    /* Published clips whose bytes never moved. Counted rather than listed:
       what the panel needs is a number and a button, and the rows themselves
       are the job queue's business once the button has been pressed. */
    const stranded = R.prepare(strandedSql).all().length;
    res.json({
      stranded,
      failed: rows.filter((r) => r.status === 'failed').length,
      lease_s: JOB_LEASE_S,
      kinds: PI_KINDS,
      /* What it asks for, beside what is on offer. The difference between the
         two is the answer to "why has nothing taken this". */
      poll,
      worker: last?.claimed_by
        ? { name: last.claimed_by, last_seen: last.claimed_at } : null,
      waiting: waiting.length,
      running: rows.filter((r) => r.status === 'claimed').length,
      /* The oldest thing nobody has picked up. The worker polls every twenty
         seconds, so this is the number that says the recorder is not there —
         and it is the only one that can, because a queue nobody has put
         anything into looks identical to a queue nobody is reading. */
      /* When it became AVAILABLE, not when it was created. A job somebody
         has just pressed Try again on has been waiting for a worker since
         that press — reporting the original creation would leave the card
         shouting "nothing has picked this up in 1d" about something four
         seconds old, immediately after you acted on the alarm, which is how
         an alarm stops being read. */
      waiting_since: waiting.length
        ? Math.min(...waiting.map((r) => r.updated_at ?? r.created_at)) : null,
      now: t,
      jobs: rows.map((r) => ({ ...r, payload: undefined, label: jobLabel(r) })),
    });
  });

  /** Make the queue match what the rows say.
   *
   *  Three things, all of them "this should have been asked for and was not":
   *  work that failed goes back on, clips that are published but whose bytes
   *  are still in quarantine get the promote nobody made, and a fetch that
   *  failed stops telling its submitter so while it waits again.
   *
   *  Not a migration script, though that is what prompts writing it. A queue
   *  drifting from the rows is a thing that happens more than once — a Pi off
   *  for a week, a cancelled job somebody meant to un-cancel, a failure that
   *  aged out of the panel — and each time the fix is the same reconciliation.
   */
  app.post('/api/recorder/sweep', requireRole('admin'), (req, res) => {
    const t = now();
    const marks = PI_KINDS.map(() => '?').join(',');

    /* Everything that failed, however long ago. The panel only shows a day of
       history, and a pile of failures from the week the recorder was off is
       exactly what this is for. */
    /* `= 'failed'` and not "everything terminal", which is what makes dismiss
       stick: a job somebody has given up on is not swept back in by the button
       that means "try the failures again". */
    const failed = R.prepare(
      `SELECT id, kind, snippet_id, payload FROM job
        WHERE kind IN (${marks}) AND status = 'failed'`).all(...PI_KINDS);
    for (const j of failed) {
      W.prepare(`UPDATE job SET status = 'approved', claimed_by = NULL, claimed_at = NULL,
                                error = NULL, finished_at = NULL, updated_at = ?
                  WHERE id = ?`).run(t, j.id);
      if (j.kind === 'fetch' && j.snippet_id) {
        W.prepare(`UPDATE snippet SET fetch_status = 'queued', fetch_note = NULL,
                                      updated_at = ? WHERE id = ?`).run(t, j.snippet_id);
      }
      // The same resync the single-job retry does, for the same reason: a row
      // still reading `failed` while its job is queued again is the panel
      // disagreeing with itself.
      const mid = (j.kind === 'music_probe' || j.kind === 'music_fetch')
        ? musicJobTarget(j) : null;
      if (mid) {
        const col = j.kind === 'music_probe' ? 'probe' : 'fetch';
        W.prepare(`UPDATE music SET ${col}_status = 'queued', ${col}_note = NULL,
                                    updated_at = ?
                    WHERE id = ? AND ${col}_status = 'failed'`).run(t, mid);
      }
    }

    /* Published, and its bytes are in the wrong place. Nothing else in the
       archive notices this state — the row reads perfectly normal, the clip
       plays from quarantine, and the master is one directory away from where
       every backup and every importer expects it. */
    const stranded = R.prepare(strandedSql).all();
    for (const s of stranded) {
      enqueueJob('promote', { snippetId: s.id, by: req.person.id,
                              payload: { from: s.quarantine_path, to: s.file_path } });
    }

    if (failed.length || stranded.length) {
      /* The detail belongs in the fifth argument. It was in the sixth — where
         `changesetId` is — with null in its place, so the insert refused to
         bind an object to that column and logEvent swallowed the error into
         one line on stderr. Nothing was ever written. Which means the single
         action that re-approves the ENTIRE failed queue at once was the one
         action in the archive with no record of having happened. */
      logEvent(req, 'swept the recorder queue', 'setting', 'recorder',
               { retried: failed.length, queued: stranded.length });
      bumpGeneration(W);
    }
    res.json({ retried: failed.length, queued: stranded.length });
  });

  /** What is actually sitting in quarantine, and why.
   *
   *  A purge button is half a feature without it — nobody purges a clip they
   *  have forgotten about, and the question that gets asked is "is this
   *  filling up", which is about the DIRECTORY rather than about any row.
   *  So this reads the disk and joins the rows to it, which is also the only
   *  way orphans show up: a file with no row at all, left by a crash between
   *  the rename and the insert.
   */
  app.get('/api/quarantine', requireRole('admin'), (req, res) => {
    if (!config.quarantineRoot) return res.json({ root: null, files: [] });
    let names = [];
    try { names = readdirSync(config.quarantineRoot); } catch { /* unreadable */ }
    const byPath = new Map(R.prepare(
      `SELECT id, title, status, quarantine_path, retracted_at FROM snippet
        WHERE quarantine_path IS NOT NULL`).all().map((r) => [r.quarantine_path, r]));

    let bytes = 0;
    const files = [];
    for (const name of names) {
      let size = 0;
      try { size = statSync(join(config.quarantineRoot, name)).size; } catch { continue; }
      bytes += size;
      const row = byPath.get(name) ?? null;
      files.push({
        name, bytes: size,
        // A dotfile is a transfer that never finished — the accept endpoint
        // writes `.part-<id>` and renames on success.
        state: name.startsWith('.part-') ? 'unfinished'
          : !row ? 'orphan'
            : row.retracted_at ? 'retracted' : row.status,
        snippet_id: row?.id ?? null,
        title: row?.title ?? null,
      });
    }
    files.sort((a, b) => b.bytes - a.bytes);
    res.json({ root: config.quarantineRoot, bytes, count: files.length, files });
  });

  // -------------------------------------------------------------------------
  // the normalize worker — the one thing in here that does real work
  //
  // It runs INSIDE this process, and that wants justifying, because "the web
  // server also transcodes video" is usually a mistake.
  //
  // It is not one here for two reasons. ffmpeg is a child process, so the
  // event loop is free the entire time it runs — Node is waiting on a pipe,
  // not encoding anything. And the expensive case is rare: an already-H.264
  // MP4 is a stream copy that finishes before the uploader has typed a title.
  // What is left is one child process at a time, niced to 19, holding two of
  // the four threads. That is a machine that feels slightly slower for a few
  // minutes, not a machine that stops answering.
  //
  // What it must never be is a second writer racing the first. It claims out
  // of the same `job` table the Pi polls, under the same lease, so two
  // containers pointed at one database cannot both take the same clip — and
  // every job it runs is visible in /api/jobs like any other.
  //
  // ── where the output goes ────────────────────────────────────────────────
  //
  // Into /cache, never /media. This is what lets the server do the work at all
  // while `read_only: true` stays on the media mount: `play_path` has always
  // resolved against the cache root — that is how the imported .webm clips
  // play today — so a normalized copy in the cache is a shape the archive
  // already had, not a new privilege.
  //
  // The consequence is worth stating plainly: an approved upload is fully
  // playable, with a poster, WITHOUT the Pi having done anything. The promote
  // job stops being on the critical path and becomes what it should be —
  // filing the master alongside the other 21 TB.
  // -------------------------------------------------------------------------

  /* Deliberately not `medium`. On a two-core R1600 with no hardware encoder,
     medium is roughly 3x realtime and veryfast is roughly 10x; at CRF 21 the
     difference on a clip somebody recorded off a stream is not something you
     can see, and the difference in how the site feels for the ten minutes is
     something everybody can. Slower, prettier settings belong on a PC nobody
     is browsing, not in the box that is also serving the page. */
  const NORM = {
    crf: Number(process.env.TENMA_NORMALIZE_CRF) || 21,
    preset: process.env.TENMA_NORMALIZE_PRESET || 'veryfast',
    threads: Number(process.env.TENMA_NORMALIZE_THREADS) || 2,
    maxW: Number(process.env.TENMA_NORMALIZE_MAX_W) || 1920,
    maxH: Number(process.env.TENMA_NORMALIZE_MAX_H) || 1080,
    // Half an hour. Long enough for ten minutes of 1080p on a slow box,
    // short enough that a wedged ffmpeg frees the queue the same evening.
    timeoutMs: Number(process.env.TENMA_NORMALIZE_TIMEOUT_MS) || 1_800_000,
    pollMs: Number(process.env.TENMA_NORMALIZE_POLL_MS) || 4_000,
    /* Bits per pixel per frame above which a file is worth trying to shrink.
       Raise it to re-encode less; 0 turns the whole behaviour off. */
    bpp: Number(process.env.TENMA_NORMALIZE_BPP) || 0.15,
  };

  /** One child, niced, killed on timeout. Resolves to null or an error line. */
  const ffmpeg = (args) => new Promise((done) => {
    const child = execFile('ffmpeg', args,
      { timeout: NORM.timeoutMs, maxBuffer: 1 << 22, killSignal: 'SIGKILL' },
      (err, _out, stderr) => {
        if (!err) return done(null);
        const last = String(stderr ?? '').trim().split('\n').pop();
        done(err.killed ? 'ffmpeg timed out' : (last || err.message || 'ffmpeg failed'));
      });
    /* Lowering a child's priority never needs privilege; raising one does. In
       a container without CAP_SYS_NICE this is the difference between the site
       staying responsive during an encode and not, so it is attempted and its
       failure is survivable rather than fatal. */
    try { setPriority(child.pid, 19); } catch { /* not permitted here */ }
  });

  const normSet = (id, status, note = null) => {
    W.prepare(`UPDATE snippet SET normalize_status = ?, normalize_note = ?, updated_at = ?
                WHERE id = ?`).run(status, note, now(), id);
  };

  /** Claim exactly one normalize job, or null.
   *
   *  The same transaction the Pi's claim uses, with its own much longer lease:
   *  five minutes is right for a fetch and wrong for an encode, and a lease
   *  that lapses mid-ffmpeg means two workers writing one output file.
   */
  const NORM_LEASE_S = Math.ceil(NORM.timeoutMs / 1000) + 120;
  const claimNormalize = () => {
    const t = now();
    return tx(W, () => {
      const j = W.prepare(
        `SELECT * FROM job
          WHERE kind = 'normalize'
            AND (status = 'approved'
                 OR (status = 'claimed' AND (claimed_at IS NULL OR claimed_at < ?)))
          ORDER BY created_at LIMIT 1`).get(t - NORM_LEASE_S);
      if (!j) return null;
      W.prepare(`UPDATE job SET status = 'claimed', claimed_by = 'server',
                                claimed_at = ?, attempts = attempts + 1, updated_at = ?
                  WHERE id = ?`).run(t, t, j.id);
      return j;
    });
  };

  const finishJob = (id, status, resultPath, error) => {
    const t = now();
    W.prepare(`UPDATE job SET status = ?, result_path = ?, error = ?,
                              updated_at = ?, finished_at = ? WHERE id = ?`)
      .run(status, resultPath, error ? String(error).slice(0, 4000) : null, t, t, id);
  };

  /** The still. Off the NORMALIZED copy where there is one, because that is
   *  the file people will actually watch and a poster taken from anything else
   *  can disagree with the first frame they see.
   *
   *  A sound clip gets a waveform instead. Never fatal either way: a row
   *  without a picture is worse-looking, not broken.
   */
  async function makePoster(s, p, playRel, src, audioOnly, still = false) {
    if (!config.cacheRoot) return s.poster_path ?? null;
    try {
      /* One folder for every kind's thumbnails, on purpose. /cache is derived
         data that regenerates in about a minute, so nothing in it needs to be
         findable by collection the way the masters do. */
      mkdirSync(join(config.cacheRoot, 'snippets'), { recursive: true });
      const rel = `snippets/${s.id}.jpg`;
      const from = playRel ? join(config.cacheRoot, playRel) : src;
      const abs = join(config.cacheRoot, rel);
      const err = audioOnly ? await ffmpeg(wavePosterArgs(from, abs))
        : still             ? await ffmpeg(stillPosterArgs(from, abs))
        : await ffmpeg(posterArgs(from, abs, p.duration_s));
      if (!err) {
        W.prepare('UPDATE snippet SET poster_path = ? WHERE id = ?').run(rel, s.id);
        return rel;
      }
    } catch { /* cosmetic */ }
    return s.poster_path ?? null;
  }

  /** Measure the shape of the sound and put it on the row.
   *
   *  Two steps because ffmpeg does not do arithmetic: dump raw mono PCM to a
   *  temp file, reduce it to 480 peaks, throw the PCM away. The temp file is
   *  the reason this is not streamed — 9.6 MB for a ten-minute clip, written
   *  and deleted inside the same call, against reading a pipe incrementally
   *  and getting the backpressure wrong.
   *
   *  Never fatal. A clip without a waveform falls back to the browser's own
   *  controls, which is worse-looking and completely functional.
   */
  async function makeWaveform(s, playRel, src) {
    if (!config.cacheRoot) return null;
    const from = playRel ? join(config.cacheRoot, playRel) : src;
    const tmp = join(config.cacheRoot, `.pcm-${s.id}.raw`);
    try {
      const err = await ffmpeg(pcmArgs(from, tmp));
      if (err) return null;
      const peaks = peaksFromPcm(readFileSync(tmp));
      if (!peaks) return null;
      const b64 = Buffer.from(peaks).toString('base64');
      W.prepare('UPDATE snippet SET waveform = ? WHERE id = ?').run(b64, s.id);
      return b64;
    } catch { return null; }
    finally { try { rmSync(tmp, { force: true }); } catch { /* gone */ } }
  }

  /** sha256 of a file, streamed.
   *
   *  Streamed and not readFileSync: this runs on whatever the recorder
   *  downloaded, which is bounded by nothing the archive controls, and
   *  reading a 2 GB VOD into a Buffer to hash it is how a 32 GB NAS runs out
   *  of memory serving a web page.
   */
  const sha256Of = (file) => new Promise((done, fail) => {
    const h = createHash('sha256');
    const rs = createReadStream(file);
    rs.on('error', fail);
    rs.on('data', (c) => h.update(c));
    rs.on('end', () => done(h.digest('hex')));
  });

  /** Everything the accept endpoint does to an upload, for a clip that did not
   *  come through it. Returns an error string, or null when the row is ready
   *  to normalize.
   */
  async function intakeFetched(s, p, src) {
    const t = now();

    if (p.duration_s && p.duration_s > UP_MAX_S) {
      return `too long — ${Math.round(p.duration_s / 60)} minutes, the cap is `
        + `${Math.round(UP_MAX_S / 60)}`;
    }

    /* The duplicate check the upload path runs before it stores anything. Here
       the bytes already exist, so a twin means throwing them away rather than
       refusing to accept them — and the row says so, because the person who
       submitted the link is owed an explanation better than silence. */
    const sha = await sha256Of(src);
    const twin = R.prepare(
      `SELECT id FROM snippet WHERE sha256 = ? AND id != ? AND retracted_at IS NULL LIMIT 1`)
      .get(sha, s.id);
    if (twin) return 'the archive already has this exact file';

    let bytes = null;
    try { bytes = statSync(src).size; } catch { /* probed fine a moment ago */ }
    W.prepare(
      `UPDATE snippet SET duration_s = ?, width = ?, height = ?, bytes = ?,
                          container = ?, video_codec = ?, audio_codec = ?, sha256 = ?,
                          updated_at = ?
        WHERE id = ?`)
      .run(p.duration_s, p.width, p.height, bytes, p.container, p.video_codec,
           p.audio_codec, sha, t, s.id);
    return null;
  }

  async function runNormalize(job) {
    const s = R.prepare('SELECT * FROM snippet WHERE id = ?').get(job.snippet_id);
    if (!s) { finishJob(job.id, 'failed', null, 'the snippet is gone'); return; }

    /* The source is wherever the file actually is. A pending upload is in
       quarantine; a promoted or imported clip is in the media tree. Reading
       from /media is fine — it is mounted read-only, not unreadable. */
    const src = s.quarantine_path
      ? resolveMedia(config.quarantineRoot, s.quarantine_path)
      : resolveMedia(config.mediaRoot, s.file_path);
    if (!src || !existsSync(src)) {
      normSet(s.id, 'failed', 'the file is not where the row says it is');
      finishJob(job.id, 'failed', null, `missing source for ${s.id}`);
      return;
    }
    if (!config.cacheRoot) {
      normSet(s.id, 'failed', 'no cache root configured');
      finishJob(job.id, 'failed', null, 'TENMA_CACHE_ROOT is unset');
      return;
    }

    normSet(s.id, 'running');
    bumpGeneration(W);

    const p = probeMedia(src);
    const base = classifyMedia(src, p);
    if (base === 'broken') {
      normSet(s.id, 'failed', 'ffmpeg cannot read this file');
      finishJob(job.id, 'failed', null, 'unreadable');
      return;
    }

    /* No sha256 means nothing has ever inspected this file — it arrived from
       the recorder rather than through /api/uploads, so the caps and the
       duplicate check happen now. The bytes are deleted on refusal: they are
       in quarantine, nothing points at them, and leaving them is how a link
       somebody submitted by mistake becomes a permanent 400 MB. */
    if (!s.sha256) {
      const bad = await intakeFetched(s, p, src);
      if (bad) {
        try { rmSync(src, { force: true }); } catch { /* already gone */ }
        W.prepare(
          `UPDATE snippet SET quarantine_path = NULL, fetch_status = 'failed',
                              fetch_note = ?, updated_at = ? WHERE id = ?`)
          .run(bad, now(), s.id);
        normSet(s.id, 'failed', bad);
        finishJob(job.id, 'failed', null, bad);
        bumpGeneration(W);
        return;
      }
    }

    /* A picture, which arrived here only because it came by LINK. An upload
       never queues this job for a still — the route makes its poster in
       process and stops — but a fetched file reaches quarantine with no sha
       and no facts, so it comes through here to get them, and then must not
       go one line further. Everything below encodes: `still` would fall into
       `encode` and spend two cores turning a PNG into an H.264 video of a
       PNG.
       intakeFetched() above has already written the dimensions, the codecs
       and the hash. What is left is the two things the upload route does
       after them. */
    if (base === 'still') {
      await makePoster({ id: s.id, poster_path: s.poster_path }, p, null, src, false, true);
      /* `none`, not `done`. There was never anything to normalize, and a
         column reading `done` would have somebody looking for an output that
         does not exist. */
      normSet(s.id, 'none');
      if (!R.prepare(
            `SELECT 1 FROM job WHERE snippet_id = ? AND kind = 'ocr'
               AND status IN ('approved','claimed','paused')`).get(s.id)) {
        enqueueJob('ocr', { snippetId: s.id, by: job.requested_by ?? null });
      }
      finishJob(job.id, 'done', null, null);
      bumpGeneration(W);
      return;
    }

    /* An export that is the right shape and the wrong size.
       classifyMedia would call a 190 MB Premiere export `conformant` and leave
       it alone, because every question it asks is about format and every
       answer is correct. So the picture gets re-encoded anyway — same stream
       decision as `video`, since the soundtrack is already AAC and copying it
       is free.

       Attempted, not assumed: whether the result is kept is decided by
       measuring it below. */
    const heavy = (base === 'conformant' || base === 'remux') && overBitrate(p, { bpp: NORM.bpp });
    const kind = heavy ? 'video' : base;
    const audioOnly = base === 'sound' || base === 'sound-encode';

    let playRel = s.play_path ?? null;

    /* `conformant` is the whole point of classifying. The file is already
       H.264/AAC in an MP4 with the index at the front — copying it into the
       cache would spend a gigabyte and several seconds to produce a file
       identical to the one we have. It gets a poster and nothing else. */
    if (kind !== 'conformant') {
      mkdirSync(join(config.cacheRoot, 'play'), { recursive: true });
      // An audio clip is an .m4a. Same bytes an .mp4 would hold, but every
      // player and every download names it correctly from here on.
      const ext = audioOnly ? 'm4a' : 'mp4';
      const rel = `play/${s.id}.${ext}`;
      const out = join(config.cacheRoot, rel);
      // Written as a dotfile and renamed. A crash mid-encode leaves a .part
      // nobody reads, never a half-written file that play_path points at.
      const tmp = join(config.cacheRoot, `play/.${s.id}.part.${ext}`);
      rmSync(tmp, { force: true });

      const err = await ffmpeg(normalizeArgs(src, tmp, kind, {
        ...NORM, hasAudio: !!p.audio_codec }));
      if (err) {
        rmSync(tmp, { force: true });
        normSet(s.id, 'failed', err.slice(0, 300));
        finishJob(job.id, 'failed', null, err);
        bumpGeneration(W);
        return;
      }

      /* Verify before trusting it. An ffmpeg that exits 0 having produced
         something that will not probe, or that lost half the runtime to a
         corrupt input it decoded anyway, is a real outcome — and the failure
         to catch it is a clip that plays for two seconds and stops. */
      const p2 = probeMedia(tmp);
      const drift = p2 ? Math.abs((p2.duration_s ?? 0) - (p.duration_s ?? 0)) : Infinity;
      /* "Has the stream we were converting" — which for a sound clip is the
         AUDIO one. Asking for a video codec unconditionally failed every audio
         upload with "the output will not probe", on output that was perfectly
         fine and simply had no pictures in it. */
      const missing = audioOnly ? !p2?.audio_codec : !p2?.video_codec;
      const bad = !p2 || missing ? 'the output will not probe'
        : drift > Math.max(0.5, (p.duration_s ?? 0) * 0.02)
          ? `duration moved ${drift.toFixed(1)}s`
          : !moovFirst(tmp) ? 'the moov atom is not first'
            : null;
      if (bad) {
        rmSync(tmp, { force: true });
        normSet(s.id, 'failed', bad);
        finishJob(job.id, 'failed', null, bad);
        bumpGeneration(W);
        return;
      }
      /* The whole safety of the bitrate heuristic lives here.
         `heavy` was a guess that there were bits to save; this measures
         whether there were. Noise, grain and confetti are genuinely
         incompressible, and CRF re-encoding them produces a file the same
         size or bigger — at which point we have spent CPU to make a
         SECOND copy of something we already had, and lost a generation of
         quality for it. So: keep it only if it is a real win, and
         otherwise throw it away and leave the original to be served.

         15% is the bar. A 5% saving is not worth a re-encode of anybody's
         master; the case this exists for saves 60-80%. */
      if (heavy) {
        const was = p.bytes ?? statSync(src).size;
        const now2 = statSync(tmp).size;
        if (now2 > was * 0.85) {
          rmSync(tmp, { force: true });
          const pct = Math.round((now2 / was) * 100);
          /* Not a failure. The file is fine, it simply could not be made
             smaller — so it falls back to exactly what its real
             classification wanted, which for a conformant file is nothing
             at all and for a remux is the container swap it already needed. */
          if (base === 'conformant') {
            playRel = null;
          } else {
            const e2 = await ffmpeg(normalizeArgs(src, tmp, base, {
              ...NORM, hasAudio: !!p.audio_codec }));
            if (e2) {
              normSet(s.id, 'failed', e2.slice(0, 300));
              finishJob(job.id, 'failed', null, e2);
              bumpGeneration(W);
              return;
            }
            renameSync(tmp, out);
            playRel = rel;
          }
          W.prepare(`UPDATE snippet SET play_path = ?, normalize_status = 'done',
                                        normalize_note = ?, updated_at = ?
                      WHERE id = ?`)
            .run(playRel, `already efficiently encoded (a re-encode came out ${pct}%)`,
                 now(), s.id);
          await makePoster(s, p, playRel, src, audioOnly);
          if (audioOnly) await makeWaveform(s, playRel, src);
          finishJob(job.id, 'done', playRel, null);
          bumpGeneration(W);
          return;
        }
      }

      renameSync(tmp, out);
      playRel = rel;
    }

    const posterRel = await makePoster(s, p, playRel, src, audioOnly);
    if (audioOnly) await makeWaveform(s, playRel, src);

    const t = now();
    W.prepare(
      `UPDATE snippet SET play_path = ?, poster_path = ?, normalize_status = 'done',
                          normalize_note = NULL, updated_at = ?
        WHERE id = ?`).run(playRel, posterRel, t, s.id);
    finishJob(job.id, 'done', playRel ?? posterRel, null);
    bumpGeneration(W);
  }

  /* One at a time, forever, and never two at once even if a tick is slow —
     `busy` is what stops a 12-minute encode from having four more stacked
     behind it by the time it finishes. */
  let busy = false;
  async function workerTick() {
    if (busy) return;
    busy = true;
    try {
      /* Belt and braces for the shape above: any future path that puts a job
         back instead of finishing it would otherwise be handed the same job
         again immediately, forever. Once per pass is enough for any job — the
         next tick is a second away. */
      const seen = new Set();
      for (;;) {
        /* Normalize first, always. Somebody is waiting on an encode — a
           snippet cannot be watched until it is converted — and nobody is
           waiting on a transcript, which arrives after approval by design.
           Draining normalize before touching transcribe is the whole of that
           priority; it needs no weights. */
        /* OCR sits between them, and the order is about waiting rather than
           importance: a picture is a fraction of a second and a clip is
           minutes, so an OCR job queued behind a half-hour transcription
           would wait half an hour to do something instant. */
        const job = claimNormalize() ?? claimOcr() ?? claimTranscribe();
        if (!job || seen.has(job.id)) break;
        seen.add(job.id);
        try {
          if (job.kind === 'normalize') await runNormalize(job);
          else if (job.kind === 'ocr') await runOcr(job);
          else await runTranscribe(job);
        } catch (e) { finishJob(job.id, 'failed', null, e?.message ?? String(e)); }
        /* Whatever way that went — including the early returns that never
           reach the runner's own finally — this job is over, so its mark goes
           with it rather than sitting in the set until a restart. */
        finally { cancelled.delete(job.id); }
      }
    } finally { busy = false; }
  }


  // -------------------------------------------------------------------------
  // text in pictures
  //
  // The same shape as transcription, one layer further out. Whisper is a
  // static binary this process spawns; PaddleOCR is Python with an ONNX
  // runtime under it, so it lives in a sidecar container and this asks it over
  // the docker network. Everything else is deliberately identical — the job
  // queue, the lease, the switch, and above all the COLUMN: what a picture
  // says goes into `transcript`, beside what a clip says, so one index
  // searches both and one editor corrects both.
  //
  // Queued at UPLOAD rather than at approval, which is the opposite of
  // transcription and for a measurable reason. A transcript is minutes of CPU
  // on a two-core box, so spending it on something an editor is about to
  // reject is real waste. A small picture through a 16 MB model is a fraction
  // of a second, and having the words before the verdict is worth more: the
  // reviewer can read what the meme SAYS, and the submitter's own pending row
  // is searchable straight away.
  //
  // Stills only. Reading text off a moving picture means deciding which frames
  // to sample and then what to do with four different answers, and that is a
  // feature rather than a detail — a gif with text on it can be run by hand
  // once there is something to run.
  // -------------------------------------------------------------------------

  /** The row for one task, always present — schema.sql seeds all three. */
  const modelRow = (task) =>
    R.prepare('SELECT * FROM model WHERE task = ?').get(task) ?? null;

  const modelSet = (task, patch) => {
    const cols = Object.keys(patch);
    if (!cols.length) return;
    W.prepare(`UPDATE model SET ${cols.map((c) => `${c} = ?`).join(', ')}, updated_at = ?
                WHERE task = ?`).run(...cols.map((c) => patch[c]), now(), task);
  };

  /** Whether OCR can happen at all, and why not when it cannot.
   *
   *  Three separate answers on purpose, because they need three different
   *  things done about them: no sidecar configured is a compose file, a
   *  disabled slot is a switch somebody flipped, and an unreachable sidecar is
   *  a container that is not running. "OCR is not working" would collapse all
   *  three into a shrug.
   */
  const ocrReady = () => {
    const m = modelRow('ocr');
    if (!m) return { ok: false, why: 'no ocr slot in the model table' };
    if (!m.enabled) return { ok: false, why: 'the ocr slot is switched off' };
    if (!config.mlUrl) return { ok: false, why: 'TENMA_ML_URL is not set — there is no sidecar' };
    return { ok: true, why: null, model: m };
  };

  /* Same switch as transcription, same reasoning: in memory, gating the CLAIM
     and not the enqueue, so a pause loses nothing and a restart ends it. */
  let ocrOn = true;

  /* How long to leave a sidecar alone after it failed to answer at all.
   *
   * Without this, a job put back by the `down` path in runOcr is claimable on
   * the very next tick, so an ML container that is off means a fetch attempt
   * every few seconds for as long as it stays off — the tight loop claimOcr's
   * comment above is about, just with a network call in it instead of a write.
   * In memory and not in the database, for the same reason the pause switch
   * is: it is a fact about right now, and a restart should clear it.
   */
  const ML_RETRY_S = Math.max(1, Number(process.env.TENMA_ML_RETRY_S) || 30);
  let ocrQuietUntil = 0;

  /** Claim one OCR job, or null.
   *
   *  Nothing is claimed while there is nothing to run it with. The runner
   *  could put such a job back, and that is what the transcribe path does —
   *  but a job put back is a job this same pass can claim again, which is a
   *  tight loop with database writes in it. The jobs sit as queued, the panel
   *  says why, and they go the moment a sidecar answers.
   */
  const OCR_LEASE_S = Math.ceil(config.mlTimeoutMs / 1000) + 120;
  const claimOcr = () => {
    if (!ocrOn || !ocrReady().ok) return null;
    // Backing off from a sidecar that did not answer — see ML_RETRY_S. The
    // jobs stay queued and the panel keeps saying why; only the asking pauses.
    if (now() < ocrQuietUntil) return null;
    const t = now();
    return tx(W, () => {
      const j = W.prepare(
        `SELECT * FROM job
          WHERE kind = 'ocr'
            AND (status = 'approved'
                 OR (status = 'claimed' AND (claimed_at IS NULL OR claimed_at < ?)))
          ORDER BY created_at LIMIT 1`).get(t - OCR_LEASE_S);
      if (!j) return null;
      W.prepare(`UPDATE job SET status = 'claimed', claimed_by = 'server',
                                claimed_at = ?, attempts = attempts + 1, updated_at = ?
                  WHERE id = ?`).run(t, t, j.id);
      return j;
    });
  };

  /** Ask the sidecar to read a picture.
   *
   *  The whole of the network contract, in one place, so that when the sidecar
   *  is actually built there is exactly one shape to match:
   *
   *      POST {mlUrl}/ocr        multipart: file=<the image>, model=<slug>
   *      200 { "text": "…", "model": "…", "ms": 123 }
   *
   *  Anything else is a failure with the body as the reason. The archive sends
   *  BYTES rather than a path on purpose — the sidecar has no business having
   *  the media tree mounted, and a container that cannot see the archive's
   *  files cannot be talked into reading one it was not handed.
   */
  async function askOcr(slug, abs) {
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), config.mlTimeoutMs);
    try {
      const form = new FormData();
      form.append('model', slug);
      form.append('file', new Blob([readFileSync(abs)]), basename(abs));
      const r = await fetch(`${config.mlUrl}/ocr`, {
        method: 'POST', body: form, signal: ctl.signal });
      const body = await r.text();
      if (!r.ok) return { err: `the sidecar said ${r.status}: ${body.slice(0, 300)}` };
      let d = null;
      try { d = JSON.parse(body); } catch { return { err: 'the sidecar did not answer JSON' }; }
      if (typeof d?.text !== 'string') return { err: 'the sidecar answered without any text' };
      return { text: d.text, model: d.model ?? slug };
    } catch (e) {
      /* Named separately because they mean different things to whoever reads
         the panel: a timeout is a model that is loading or a picture that is
         enormous, and a refusal is a container that is not up.
         `down` marks both as facts about the DEPLOYMENT rather than about this
         picture — see runOcr, which puts such a job back instead of failing
         it. A sidecar that answers and says something wrong is the other kind
         and carries no flag. */
      return { down: true,
        err: e?.name === 'AbortError'
          ? `the sidecar did not answer within ${Math.round(config.mlTimeoutMs / 1000)}s`
          : `could not reach the sidecar: ${e?.message ?? e}` };
    } finally { clearTimeout(timer); }
  }

  async function runOcr(job) {
    const ready = ocrReady();
    if (!ready.ok) {
      /* Back to the queue rather than failed, matching transcription. Having
         no sidecar is a fact about the deployment and not about this picture,
         and a job marked failed for it would need someone to notice and
         retry it once the container was up. */
      W.prepare(`UPDATE job SET status = 'approved', claimed_by = NULL, claimed_at = NULL,
                                error = ?, updated_at = ? WHERE id = ?`)
        .run(ready.why, now(), job.id);
      return;
    }
    const s = R.prepare('SELECT * FROM snippet WHERE id = ? AND retracted_at IS NULL')
      .get(job.snippet_id);
    if (!s) { finishJob(job.id, 'failed', null, 'that picture is gone'); return; }
    /* A human has been through it. Re-running would overwrite a correction
       with a guess, which is the one outcome this must never produce — the
       editor's version is the archive's answer. */
    if (s.transcript_status === 'edited') {
      finishJob(job.id, 'done', null, null);
      return;
    }

    const abs = (s.quarantine_path && config.quarantineRoot
                 && resolveMedia(config.quarantineRoot, s.quarantine_path))
      || (s.file_path && config.mediaRoot && resolveMedia(config.mediaRoot, s.file_path));
    if (!abs || !existsSync(abs)) {
      finishJob(job.id, 'failed', null, 'no file to read');
      return;
    }

    const t0 = now();
    W.prepare(`UPDATE snippet SET transcript_status = 'running', transcript_note = NULL,
                                  updated_at = ? WHERE id = ?`).run(t0, s.id);

    const out = await askOcr(ready.model.slug, abs);
    const t = now();
    /* A container that is down or too slow to answer gets the same treatment
       as a slot that was never configured: the job goes BACK, and the picture
       is left alone. This is the case the requeue above was written for and
       did not cover — `ocrReady()` can only see config, so a compose file with
       TENMA_ML_URL in it and no container behind it reached here and marked
       every meme uploaded during the outage `failed`, permanently, each one
       needing a re-OCR by hand. On a NAS an ML container that restarts is a
       Tuesday, so this is the common path rather than the exotic one.
       The model row still records it, because that is where somebody looks to
       find out why nothing is happening. */
    if (out.down) {
      ocrQuietUntil = t + ML_RETRY_S;
      modelSet('ocr', { state: 'error', state_note: out.err.slice(0, 300), checked_at: t });
      W.prepare(`UPDATE snippet SET transcript_status = 'none', transcript_note = NULL,
                                    updated_at = ? WHERE id = ?`).run(t, s.id);
      W.prepare(`UPDATE job SET status = 'approved', claimed_by = NULL, claimed_at = NULL,
                                error = ?, updated_at = ? WHERE id = ?`)
        .run(out.err.slice(0, 300), t, job.id);
      bumpGeneration(W);
      return;
    }
    if (out.err) {
      modelSet('ocr', { state: 'error', state_note: out.err.slice(0, 300), checked_at: t });
      W.prepare(`UPDATE snippet SET transcript_status = 'failed', transcript_note = ?,
                                    updated_at = ? WHERE id = ?`)
        .run(out.err.slice(0, 300), t, s.id);
      finishJob(job.id, 'failed', null, out.err);
      bumpGeneration(W);
      return;
    }

    /* LINES, and not just the column. `transcript` is documented as the flat
       join of snippet_line and is derived from it — writing the column alone
       would leave a picture whose words are searchable, visible nowhere, and
       uneditable, because every surface that shows or corrects a transcript
       reads the lines. One row per line the model returned, all at t=0, which
       is what a still's timing honestly is; the whole-transcript editor then
       works on a meme exactly as it does on a clip, and an editor's correction
       flips transcript_status to `edited` through the path that already
       exists rather than through a second one written for pictures.
       `empty` and not `failed` for a picture with no words on it: it is a real
       answer — most reaction faces have none — and the difference matters for
       the re-run list, where `failed` should mean something went wrong rather
       than that a blank picture was read correctly. */
    const lines = out.text.split(/\r?\n/)
      .map((l) => l.replace(/[^\S\n]+/g, ' ').trim())
      .filter(Boolean);
    const text = lines.join(' ');
    tx(W, () => {
      W.prepare('DELETE FROM snippet_line WHERE snippet_id = ?').run(s.id);
      const ins = W.prepare(
        `INSERT INTO snippet_line(id, snippet_id, seq, start_s, end_s, speaker, text)
         VALUES(?,?,?,0,NULL,NULL,?)`);
      lines.forEach((l, i) => ins.run(ulid(), s.id, i, l));
      W.prepare(`UPDATE snippet SET transcript = ?, transcript_status = ?,
                                    transcript_model = ?, transcript_at = ?,
                                    transcript_note = NULL, updated_at = ?
                  WHERE id = ?`)
        .run(text || null, text ? 'auto' : 'empty', out.model, t, t, s.id);
    });
    modelSet('ocr', { state: 'loaded', state_note: null, checked_at: t, used_at: t });
    finishJob(job.id, 'done', null, null);
    logEvent(null, 'read the text on a picture', 'snippet', s.id,
             { model: out.model, chars: text.length, seconds: t - t0 });
    bumpGeneration(W);
  }

  /** Ask the sidecar what it has, and remember the answer.
   *
   *  Called by the admin panel rather than on a timer. A poll would be asking
   *  a container that is usually idle to wake up and say the same thing it
   *  said a minute ago; the panel is opened when somebody wants to know.
   */
  async function modelCheck(task) {
    const m = modelRow(task);
    if (!m) return null;
    const spec = MODEL_TASK[task];
    const t = now();
    if (spec?.runner === 'local') {
      /* whisper is not the sidecar's. Its readiness is two paths on this
         filesystem, which is a question already answered elsewhere — asked
         here so the panel has one shape for all three rather than a special
         case it has to know about. */
      const w = whisperReady();
      modelSet(task, { state: w.ok ? 'installed' : 'absent',
                       state_note: w.why, checked_at: t });
      return modelRow(task);
    }
    if (!config.mlUrl) {
      modelSet(task, { state: 'absent',
                       state_note: 'TENMA_ML_URL is not set — there is no sidecar',
                       checked_at: t });
      return modelRow(task);
    }
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), 8000);
    try {
      /*  GET {mlUrl}/models  →  { "models": [{ slug, loaded, bytes }] }
          The second half of the contract, and the only other call the archive
          makes. Deliberately a plain GET with no arguments: the sidecar
          reporting everything it has is one round trip whatever the panel
          asks about, and it cannot be turned into a probe for anything else. */
      const r = await fetch(`${config.mlUrl}/models`, { signal: ctl.signal });
      const d = await r.json();
      const hit = (d?.models ?? []).find((x) => x?.slug === m.slug);
      modelSet(task, {
        state: !hit ? 'absent' : hit.loaded ? 'loaded' : 'installed',
        state_note: hit ? null : `the sidecar does not have ${m.slug}`,
        bytes: hit?.bytes ?? m.bytes ?? null,
        checked_at: t,
      });
    } catch (e) {
      modelSet(task, { state: 'error', checked_at: t,
        state_note: (e?.name === 'AbortError'
          ? 'the sidecar did not answer in 8s'
          : `could not reach the sidecar: ${e?.message ?? e}`).slice(0, 300) });
    } finally { clearTimeout(timer); }
    return modelRow(task);
  }

  // -------------------------------------------------------------------------
  // transcription
  //
  // whisper.cpp as a subprocess, exactly like ffmpeg: the archive already
  // shells out to one binary it did not write, and this is a second one. The
  // binary and the model are PATHS FROM THE ENVIRONMENT, not things baked into
  // the image — a 500 MB model rebuilt into a container on every deploy is
  // miserable, and this way the feature ships dark and lights up when the
  // files are put in place.
  //
  // Nothing here runs on its own thread. It takes the same single worker slot
  // normalize uses, because there are two cores.
  // -------------------------------------------------------------------------

  const WHISPER = {
    bin: String(process.env.TENMA_WHISPER_BIN ?? '').trim(),
    model: String(process.env.TENMA_WHISPER_MODEL ?? '').trim(),
    /* Two, not four. The box has two cores and is also serving pages; the
       fourth thread buys a few percent and costs the site its responsiveness
       while a ten-minute snippet grinds. */
    threads: Number(process.env.TENMA_WHISPER_THREADS) || 2,
    lang: String(process.env.TENMA_WHISPER_LANG ?? 'en').trim(),
    /* Generous, because this is measured in minutes by design. A ten-minute
       snippet at one-third of realtime is half an hour, and a timeout that
       fires at twenty minutes turns a slow success into a failure nobody can
       explain. */
    timeoutMs: Number(process.env.TENMA_WHISPER_TIMEOUT_MS) || 3 * 3600 * 1000,
  };

  /** Whether transcription can happen at all, and why not when it cannot. */
  const whisperReady = () => {
    if (!WHISPER.bin) return { ok: false, why: 'TENMA_WHISPER_BIN is not set' };
    if (!WHISPER.model) return { ok: false, why: 'TENMA_WHISPER_MODEL is not set' };
    if (!existsSync(WHISPER.bin)) return { ok: false, why: `no binary at ${WHISPER.bin}` };
    if (!existsSync(WHISPER.model)) return { ok: false, why: `no model at ${WHISPER.model}` };
    return { ok: true, why: null };
  };

  /* The big switch. In memory on purpose — it is the thing you reach for when
     the box is busy right now, and "right now" does not survive a restart. It
     gates the CLAIM rather than the enqueue: jobs still queue while it is off
     and are taken the moment it is back on, where gating the enqueue would
     make every request during the pause silently evaporate. */
  let transcribeOn = true;

  /* The child process of whatever is running, so it can be cancelled. Whisper
     cannot resume mid-file, so there is no pause here — killing it and
     requeueing loses exactly what pausing would have saved, which is nothing.
     Pause lives on QUEUED jobs, where it means something. */
  let running = null;          // { jobId, child, snippetId }
  /* Jobs an admin stopped on purpose. Killing the child makes the runner throw
     the way a crash does, and without this its catch block would overwrite the
     cancellation the endpoint just recorded — the job would read "Command
     failed: ffmpeg ..." and the snippet would look broken rather than
     untouched. */
  const cancelled = new Set();

  /** Run one binary with a timeout and a handle for cancelling it. */
  const spawnFor = (jobId, snippetId, bin, args, timeoutMs) => new Promise((done, fail) => {
    /* A transcribe is two children in a row, and between them there is a
       moment when there is nothing to kill. Without this the second one would
       start AFTER an admin stopped the job, run to the end, and write a
       transcript over a snippet the endpoint has already put back to having
       none. */
    if (cancelled.has(jobId)) return void fail(new Error('cancelled'));
    const child = execFile(bin, args, { timeout: timeoutMs, maxBuffer: 32 << 20 },
      (err, stdout, stderr) => {
        /* Only if it is still this one. A later job may have taken the slot. */
        if (running?.jobId === jobId) running = null;
        if (err) return fail(new Error(String(stderr || err.message).slice(0, 1500)));
        done(stdout);
      });
    running = { jobId, child, snippetId, at: now() };
    /* And the same race one notch tighter: the mark can land between the check
       above and this assignment, in which case the endpoint found no child and
       this is the only place left that can stop it. */
    if (cancelled.has(jobId)) { try { child.kill('SIGKILL'); } catch { /* gone */ } }
  });

  /** whisper.cpp's JSON into the rows the archive keeps.
   *
   *  `offsets` are milliseconds — the writer emits `t0 * 10` from
   *  centiseconds — and they are what the page seeks on, so they are what is
   *  read rather than the pretty `timestamps` strings beside them.
   */
  function parseWhisper(jsonText) {
    const d = JSON.parse(jsonText);
    const segs = Array.isArray(d?.transcription) ? d.transcription : [];
    const out = [];
    for (const seg of segs) {
      const text = String(seg?.text ?? '').trim();
      if (!text) continue;
      const from = Number(seg?.offsets?.from);
      const to = Number(seg?.offsets?.to);
      out.push({
        start_s: Number.isFinite(from) ? from / 1000 : 0,
        end_s: Number.isFinite(to) ? to / 1000 : null,
        text: text.slice(0, 2000),
      });
    }
    return out;
  }

  /** Claim one transcribe job, or null. Skips paused ones and obeys the switch. */
  const WHISPER_LEASE_S = Math.ceil(WHISPER.timeoutMs / 1000) + 300;
  const claimTranscribe = () => {
    if (!transcribeOn) return null;
    /* Not claimed while there is nothing to run it with. The runner handles an
       unconfigured whisper by putting the job back, which is right — but a job
       put back is a job the loop below can claim again on the same pass, and
       that is a tight loop with database writes in it that pins a core and
       stops this process answering anything at all. The jobs sit as queued,
       the panel says why, and they go the moment the files land. */
    const ready = whisperReady();
    if (!ready.ok || !config.cacheRoot) return null;
    const t = now();
    return tx(W, () => {
      const j = W.prepare(
        `SELECT * FROM job
          WHERE kind = 'transcribe'
            AND (status = 'approved'
                 OR (status = 'claimed' AND (claimed_at IS NULL OR claimed_at < ?)))
          ORDER BY created_at LIMIT 1`).get(t - WHISPER_LEASE_S);
      if (!j) return null;
      W.prepare(`UPDATE job SET status = 'claimed', claimed_by = 'server',
                                claimed_at = ?, attempts = attempts + 1, updated_at = ?
                  WHERE id = ?`).run(t, t, j.id);
      return j;
    });
  };

  async function runTranscribe(job) {
    const ready = whisperReady();
    if (!ready.ok) {
      /* Back to the queue rather than failed. Whisper being unconfigured is a
         fact about the deployment, not about this snippet, and a job that
         failed for it would need finding and re-queueing by hand the day the
         files arrive. */
      W.prepare(`UPDATE job SET status = 'approved', claimed_by = NULL, claimed_at = NULL,
                                error = ?, updated_at = ? WHERE id = ?`)
        .run(ready.why, now(), job.id);
      return;
    }

    const s = R.prepare('SELECT * FROM snippet WHERE id = ? AND retracted_at IS NULL')
      .get(job.snippet_id);
    if (!s) { finishJob(job.id, 'failed', null, 'the snippet is gone'); return; }

    let payload = {};
    try { payload = JSON.parse(job.payload ?? '{}') ?? {}; } catch { /* none */ }

    /* The one thing this must never do on its own initiative. `edited` means a
       human has been through the words; an automatic pass would silently
       replace their corrections with a machine's next guess. An editor asking
       for it explicitly is a different act, and the log already records that
       one as discarding work. */
    if (s.transcript_status === 'edited' && !payload.force) {
      finishJob(job.id, 'done', null, null);
      return;
    }

    /* What to listen to: the normalized copy when there is one, because it is
       the file that is known to decode. Falls back to the master. */
    const src = (s.play_path && config.cacheRoot && resolveMedia(config.cacheRoot, s.play_path))
      || (s.quarantine_path && config.quarantineRoot
          && resolveMedia(config.quarantineRoot, s.quarantine_path))
      || (s.file_path && config.mediaRoot && resolveMedia(config.mediaRoot, s.file_path));
    if (!src || !existsSync(src)) {
      finishJob(job.id, 'failed', null, 'no file to transcribe');
      return;
    }

    W.prepare(`UPDATE snippet SET transcript_status = 'running', transcript_note = NULL,
                                  updated_at = ? WHERE id = ?`).run(now(), s.id);

    /* The scratch wav and the JSON both land in the cache root, which is the
       only place this process may write besides quarantine. Without one there
       is nowhere to put them, and that is a deployment fact rather than a
       fault in this snippet — so it goes back to the queue like an unset
       binary does. */
    if (!config.cacheRoot) {
      W.prepare(`UPDATE job SET status = 'approved', claimed_by = NULL, claimed_at = NULL,
                                error = ?, updated_at = ? WHERE id = ?`)
        .run('no cache root, so there is nowhere to work', now(), job.id);
      return;
    }
    const stem = join(config.cacheRoot, `whisper-${job.id}`);
    const wav = `${stem}.wav`;
    try {
      /* 16 kHz mono, which is what whisper wants and the only rate it will
         take without resampling internally. The waveform path already does
         this at 8 kHz for a different reason; same one line, different rate. */
      await spawnFor(job.id, s.id, 'ffmpeg',
        ['-nostdin', '-loglevel', 'error', '-y', '-i', src,
         '-vn', '-ac', '1', '-ar', '16000', '-c:a', 'pcm_s16le', wav],
        10 * 60 * 1000);

      await spawnFor(job.id, s.id, WHISPER.bin,
        ['-m', WHISPER.model, '-f', wav, '-oj', '-of', stem,
         '-t', String(WHISPER.threads), '-l', WHISPER.lang,
         /* No progress spam, and no printing the transcript to stdout twice —
            the JSON file is the output that matters. */
         '-np', '-nt'],
        WHISPER.timeoutMs);

      /* One last look before anything is written. A cancel arriving while
         whisper was on its final segment loses the race with the kill, and the
         transcript must not come back afterwards. */
      if (cancelled.has(job.id)) return;

      const lines = parseWhisper(readFileSync(`${stem}.json`, 'utf8'));
      const t = now();
      tx(W, () => {
        W.prepare('DELETE FROM snippet_line WHERE snippet_id = ?').run(s.id);
        const ins = W.prepare(
          `INSERT INTO snippet_line(id, snippet_id, seq, start_s, end_s, speaker, text)
           VALUES(?,?,?,?,?,NULL,?)`);
        lines.forEach((l, i) => ins.run(ulid(), s.id, i, l.start_s, l.end_s, l.text));
        /* `empty` is a real answer and not a failure: a snippet can genuinely
           have no speech in it, and saying so stops it being re-queued
           forever by anything that looks for missing transcripts. */
        W.prepare(`UPDATE snippet SET transcript = ?, transcript_status = ?,
                                      transcript_model = ?, transcript_note = NULL,
                                      updated_at = ? WHERE id = ?`)
          .run(lines.length ? lines.map((l) => l.text).join(' ') : null,
               lines.length ? 'auto' : 'empty',
               WHISPER.model.split('/').pop() ?? null, t, s.id);
      });
      finishJob(job.id, 'done', null, null);
      logEvent({ person: { id: null, handle: 'the server', role: 'system' } },
               'transcribed', 'snippet', s.id,
               { lines: lines.length, model: WHISPER.model.split('/').pop() ?? null });
      bumpGeneration(W);
    } catch (e) {
      /* Somebody pressed cancel. The endpoint has already said so, on both the
         job and the snippet, and it said it better than this catch can. */
      if (cancelled.has(job.id)) { cancelled.delete(job.id); return; }
      const why = String(e?.message ?? e).slice(0, 500);
      W.prepare(`UPDATE snippet SET transcript_status = 'failed', transcript_note = ?,
                                    updated_at = ? WHERE id = ?`).run(why, now(), s.id);
      finishJob(job.id, 'failed', null, why);
      bumpGeneration(W);
    } finally {
      cancelled.delete(job.id);
      for (const f of [wav, `${stem}.json`]) {
        try { rmSync(f, { force: true }); } catch { /* already gone */ }
      }
    }
  }

  /** Sound clips that finished normalizing before waveforms were measured.
   *
   *  Bounded, and it does not race the queue: a clip that already has a
   *  normalize job waiting is skipped, so a restart in the middle of the
   *  backfill does not double it. Audio re-encodes in a couple of seconds, so
   *  the cost of being wrong here is small — but the LIMIT is there anyway,
   *  because "queue one job per row in the archive at boot" is a shape that
   *  should never be written without one.
   */
  const backfillWaveforms = () => {
    const rows = R.prepare(
      `SELECT s.id FROM snippet s
        WHERE s.retracted_at IS NULL
          AND s.video_codec IS NULL AND s.audio_codec IS NOT NULL
          AND s.waveform IS NULL
          AND s.normalize_status = 'done'
          AND NOT EXISTS (SELECT 1 FROM job j
                           WHERE j.snippet_id = s.id AND j.kind = 'normalize'
                             AND j.status IN ('approved', 'claimed'))
        LIMIT 200`).all();
    for (const r of rows) {
      W.prepare("UPDATE snippet SET normalize_status = 'queued' WHERE id = ?").run(r.id);
      enqueueJob('normalize', { snippetId: r.id });
    }
    if (rows.length) bumpGeneration(W);
    return rows.length;
  };

  app.startNormalizeWorker = () => {
    if (!config.cacheRoot) {
      console.log('  worker      off — no cache root, so there is nowhere to write');
      return null;
    }
    const back = backfillWaveforms();
    if (back) console.log(`  backfill    ${back} sound clip(s) queued for a waveform`);
    // unref, so the timer never holds the process open on its own. A test that
    // builds an app and finishes should exit, not hang for four seconds.
    const timer = setInterval(() => { workerTick().catch(() => {}); }, NORM.pollMs);
    timer.unref?.();
    console.log(`  worker      on — ${NORM.preset} crf${NORM.crf}, ${NORM.threads} threads, `
      + `ceiling ${NORM.maxW}x${NORM.maxH}`);
    workerTick().catch(() => {});
    return timer;
  };

  // -------------------------------------------------------------------------
  // snippets
  //
  // Read-only here. Everything that writes a snippet or a taglet goes through
  // /api/changesets like every other decision in the archive; the importer
  // writes the initial thousand rows directly, the same way the vault import
  // did.
  // -------------------------------------------------------------------------

  /* ── gates ────────────────────────────────────────────────────────────────
     A taglet may carry a gate; a person may hold grants. A snippet tagged with
     a gating taglet is invisible to anyone not holding a grant of that name.

     Enforced HERE and only here, in a pair that must agree: one SQL fragment
     for the list, one row-level check for the four routes that serve a single
     clip. That pairing already exists for the pending-upload rule immediately
     below, and for the same reason — the list and the media route disagreeing
     is how an id that is absent from every page still hands out bytes to
     anyone who guesses it.

     Editors and admins bypass. A reviewer cannot approve what they cannot
     watch, and uploads land in the review queue, so a gate that hid clips from
     staff would mean granting yourself every audience before you could do
     review at all. Gates are about audience, not staff trust. */
  const GRANTS_OF = R.prepare('SELECT name FROM person_grant WHERE person_id = ?');
  const grantsOf = (id) => (id ? GRANTS_OF.all(id).map((r) => r.name) : []);

  /** The WHERE fragment. Empty string when the viewer may see everything. */
  /* Parameterised over the junction, because the RULE is the subtle part and
     there is now more than one thing wearing a tag. A second copy of this for
     music would be a second place for "must clear ALL of them" to be got
     wrong, and the two would drift the first time a gate rule changed. The
     defaults are the snippet case, so every existing caller reads the same. */
  const gateSql = (req, { junction = 'snippet_taglet', fk = 'snippet_id',
                          alias = 's' } = {}) => {
    if (atLeast(req.person, 'editor')) return { sql: '', params: [] };
    const held = grantsOf(req.person?.id ?? null);
    /* NOT EXISTS and not a join: a row can carry several gating tags and must
       clear ALL of them, and a join would return it once per gate it does
       clear. Written as "has a gating tag this person cannot open". */
    const inner = held.length
      ? `AND t.gate NOT IN (${held.map(() => '?').join(',')})`
      : '';
    return {
      sql: `NOT EXISTS (SELECT 1 FROM ${junction} st JOIN tag t ON t.id = st.tag_id
                         WHERE st.${fk} = ${alias}.id AND t.retracted_at IS NULL
                           AND t.gate IS NOT NULL ${inner})`,
      params: held,
    };
  };

  const GATES_ON = R.prepare(
    `SELECT DISTINCT t.gate FROM snippet_taglet st JOIN tag t ON t.id = st.tag_id
      WHERE st.snippet_id = ? AND t.retracted_at IS NULL AND t.gate IS NOT NULL`);

  /** The same question about one row.
   *
   *  Fails CLOSED on a missing id. It is reached through snipVisible(), whose
   *  callers each run their own SELECT, and one of them did not ask for `id` —
   *  which threw inside the driver and became a 500. A 500 where every other
   *  refusal is a 404 is itself a disclosure: it says the row exists. Denying
   *  is the only safe answer to "I cannot tell", and a route that forgets the
   *  column now returns a wrong-but-safe 404 instead of a leak.
   */
  const gateOk = (id, req) => {
    if (atLeast(req.person, 'editor')) return true;
    if (typeof id !== 'string' || !id) return false;
    const need = GATES_ON.all(id).map((r) => r.gate);
    if (!need.length) return true;
    const held = new Set(grantsOf(req.person?.id ?? null));
    return need.every((g) => held.has(g));
  };

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
    !!row
    && (row.status === 'confirmed'
        /* The capability, not the role. Whoever may read the queue may read
           what is in it — asked here the same way `?status=` asks it, so the
           list and the single row cannot drift apart. */
        || can(req.person, 'review.read')
        || (row.status === 'proposed'
            && !!row.author_id && row.author_id === req.person?.id))
    /* Second, and separately: publication says whether it is part of the
       archive, the gate says who the archive is showing it to. A clip can be
       published and still not yours to see. */
    && gateOk(row.id, req);

  /* The same rule as a WHERE fragment, so the list cannot disagree with the
     routes that serve what the list linked to. A null `me` collapses it back
     to the published-only clause the archive has always had.
     Takes the REQUEST and not just an id, because the rule now has three
     branches rather than two: whoever may read the queue sees every status
     inline — the Snippets panel draws a waiting clip in yellow and a removed
     one in red rather than sending them to a separate pile — and that is the
     same question `snipVisible()` above answers for one row. */
  const snipVisibleSql = (req) => {
    if (can(req?.person, 'review.read')) return { sql: '1=1', params: [] };
    const me = req?.person?.id ?? null;
    return me
      ? { sql: `(s.status = 'confirmed' OR (s.status = 'proposed' AND s.author_id = ?))`,
          params: [me] }
      : { sql: `s.status = 'confirmed'`, params: [] };
  };

  const TAGLETS_OF = R.prepare(
    // st.id comes back as link_id because detaching deletes the JUNCTION, not
    // the taglet, and the client would otherwise have no way to name it.
    `SELECT st.snippet_id, st.id AS link_id, t.id, t.name, t.slug, t.kind, t.gate
       FROM snippet_taglet st JOIN tag t ON t.id = st.tag_id
      WHERE st.snippet_id = ? AND t.retracted_at IS NULL
      -- Who, then what it belongs to, then what it is, then the rest: the
      -- reading order of a danbooru sidebar. media sits where copyright did;
      -- naming a kind that no longer exists silently drops the whole group to
      -- ELSE and reorders every filename the download route builds.
      ORDER BY CASE t.kind WHEN 'character' THEN 0 WHEN 'media' THEN 1
                           WHEN 'meta' THEN 2 ELSE 3 END, t.name`);

  const LINES_OF = R.prepare(
    `SELECT seq, start_s, end_s, speaker, text FROM snippet_line
      WHERE snippet_id = ? ORDER BY seq`);

  /* The filename on a shareable link.
   *
   *  Not dlName(): that one names a Save-As dialog and is allowed spaces and
   *  Japanese, both of which become a screenful of percent-escapes in a URL
   *  somebody is about to paste into a chat window. A slug and a real
   *  extension is what reads as a link and what an unfurler treats as an
   *  image. The route ignores this segment entirely — it is decoration on a
   *  URL the id alone already resolves.
   */
  const shareName = (r) => {
    const ext = extname(String(r.file_path ?? '')).toLowerCase() || '.png';
    /* The emptiness is tested BEFORE slugify, not after. slugify() never
       returns '' — handed nothing it falls through to its hash branch and
       answers `tag-0`, which is a perfectly good slug and the same one every
       time. So `slugify(title) || slug` looked like a fallback and was not:
       every untitled picture in the archive would have shared one link name.
       Untitled is the ORDINARY state for a picture, so this is the common
       path rather than an edge. */
    const stem = r.title
      ? slugify(String(r.title)).slice(0, 60)
      : String(r.slug ?? 'image');
    return encodeURIComponent(stem) + ext;
  };

  const snipRow = (r, { lines = false, me = null } = {}) => (
    /* Read once and used twice — for the strip itself, and for the filter that
       decides which suggestions are still outstanding. Wrapped rather than
       rewritten into a block body so the object below keeps its indentation
       and this stays a diff about suggestions. */
    ((attached) => ({
    id: r.id,
    slug: r.slug,
    title: r.title,
    /* Which panel this belongs to. Sent on every row rather than assumed from
       the query, because the Review queue holds all three at once and a row
       there has to be able to say which collection it will go back to. */
    kind: r.kind ?? 'snippet',
    /* The credit line: who drew it, where it was found. Null on every clip in
       the archive today and sent anyway, because the gallery draws it beside
       the picture and an absent field and an empty one look different there.
       What a picture SAYS is not here — that is `transcript`, and it reaches
       the page through has_transcript and the detail route like any clip's. */
    source: r.source ?? null,
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
    /* The same bytes as `video`, under a name that ends in .png — for the
       link button, which exists so a meme can be dropped into a chat and
       show up as a picture rather than as a URL. Only on a still, because
       that is the only case where the extension of what gets served is not
       in question: a clip may be handed its original or its normalized copy
       depending on what the cache holds. */
    share: isStill(r) ? `/media/snippet/${r.id}/${shareName(r)}` : null,
    /* Whether this row is a single picture, decided HERE and sent, rather
       than left for the page to infer. The page cannot infer it: a jpeg
       probes as 0.04 seconds, so `duration_s > 0` calls it a video on the
       client for exactly the same reason it did on the server — and the
       client has no format table to correct itself with. One boolean is
       cheaper than teaching the browser what `image2` means. */
    still: isStill(r),
    poster: r.poster_path ? `/media/snippet-poster/${r.id}` : null,
    source_stream_id: r.source_stream_id,
    source_offset_s: r.source_offset_s,
    has_transcript: !!r.transcript,
    /* So a row can say "converting" rather than rendering a player over a
       file that is not finished being written. The note rides along because
       when this is `failed` the reason is the only useful thing on the row. */
    normalize_status: r.normalize_status ?? 'none',
    normalize_note: r.normalize_note ?? null,
    /* Straight from the probe at upload, not guessed from a missing width —
       plenty of imported rows have no dimensions recorded and are perfectly
       ordinary video. The page needs this to draw a waveform strip and a
       label rather than a 16:9 box with nothing in it. */
    audio_only: !r.video_codec && !!r.audio_codec,
    /* The ORIGINAL container, which normalize never rewrites — a gif that has
       been converted to H.264 for playback is still a gif on disk, and both
       the label and the download depend on knowing that. */
    is_gif: /gif/i.test(String(r.container ?? '')),
    /* A clip that came from a link, and how that is going. `fetch_status` is
       what stops the page drawing a player over a row whose bytes have not
       been downloaded yet. */
    source_url: r.source_url ?? null,
    fetch_status: r.fetch_status ?? 'none',
    fetch_note: r.fetch_note ?? null,
    /* 480 base64'd bytes, and only ever on a sound clip. The page draws its
       own scrubber from this — a still image with transport controls painted
       over it was what made an audio row read as a broken video. */
    waveform: r.waveform ?? null,
    transcript_status: r.transcript_status,
    transcript_model: r.transcript_model ?? null,
    transcript_note: r.transcript_note ?? null,
    taglets: attached.map((t) => ({
      id: t.id, link_id: t.link_id, name: t.name, slug: t.slug, kind: t.kind,
      /* Only ever non-null on a row the reader already cleared, so it marks
         who else can see this rather than advertising something withheld. */
      gate: t.gate ?? null })),
    /* Names the submitter asked for that the archive does not have. Shown to
       them and to reviewers, and to nobody else — they are not vocabulary, so
       they must not read as tags on a published clip.

       A suggestion the clip has since been GIVEN is dropped here rather than
       deleted when it is granted. Derived, so it cannot desync: an editor who
       attaches Selen Tatsuki through the picker, the queue, or a changeset
       somebody else wrote all retire the request by the same rule, and no
       code path has to remember to. The column keeps the record of what was
       asked for; this is the part that is still a question. */
    taglet_suggestions: (() => {
      let raw = [];
      try { raw = r.taglet_suggestions ? JSON.parse(r.taglet_suggestions) : []; }
      catch { return []; }
      const have = new Set(attached.map((t) => t.slug));
      return raw.filter((x) => !have.has(slugify(String(x ?? ''))));
    })(),
    ...(lines ? { lines: LINES_OF.all(r.id) } : {}),
  }))(TAGLETS_OF.all(r.id)));

  app.get('/api/snippets', (req, res) => {
    const { q = '', taglet, scope = 'all', include = '' } = req.query;
    /* `?taglet_kind=` and not `?kind=`, which it used to be. The two are
       different questions — "clips in the Memes collection" and "clips
       carrying a tag of kind character" — and one name cannot answer both.
       The collection wins the short name because it is the one the panels
       ask for on every page load and the one that matches the column. This
       filter was reachable but unused: nothing in the page, the scripts or
       the recorder ever sent it. */
    const tagletKind = req.query.taglet_kind;
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
    /* Asked as a capability, not as a role: the Queued and Removed piles are
       now reachable from the Snippets panel as well as from Review, and both
       arrive here as `?status=`. Whoever may READ the queue may read it from
       either place — that is one question, and this is where it is answered. */
    const mayReview = can(req.person, 'review.read');
    const me = req.person?.id ?? null;
    const wantStatus = req.query.status ? String(req.query.status) : null;
    if (wantStatus && !mayReview) {
      return res.status(403).json({ error: 'filtering by review status requires review.read' });
    }

    /* Which collection. `snippet` when nothing is asked for, which is what
       keeps every existing caller — the panel, the moment link, ls-archive —
       answering exactly what it did before memes existed.

       `all` is spelled out rather than being what an empty parameter means,
       because the Review queue is the one caller that wants three collections
       at once and it should have to say so. A default that meant "everything"
       would have quietly filled the Snippets panel with reaction faces on the
       day this deployed. */
    const wantKind = req.query.kind ? String(req.query.kind) : 'snippet';
    if (wantKind !== 'all' && !SNIPPET_KINDS.includes(wantKind)) {
      return res.status(400).json({
        error: `kind must be one of ${SNIPPET_KINDS.join(', ')}, or all` });
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
    /* Part of the cache key, exactly like `me` above it. Two people with the
       same id cannot exist, so `me` would in principle be enough — except for
       the anonymous case, where everyone shares one validator and the held set
       is empty. Listed explicitly so the reason is on the page rather than in
       someone's head. */
    const myGrants = atLeast(req.person, 'editor') ? ['*'] : grantsOf(me).sort();
    const etag = etagFor('snips', q, tagAll, tagAny, tagNot, wantKind, tagletKind,
                         scope, limit, myGrants,
                         before, wantLines, req.query.transcript, wantStatus,
                         mayReview, me);
    if (fresh(req, res, etag, { personal: !!me })) return res.status(304).end();

    const where = ['s.retracted_at IS NULL'];
    const params = [];

    if (wantKind !== 'all') { where.push('s.kind = ?'); params.push(wantKind); }

    if (wantStatus === 'all') { /* every status; editors only, checked above */ }
    else if (wantStatus) { where.push('s.status = ?'); params.push(wantStatus); }
    else {
      const v = snipVisibleSql(req);
      where.push(v.sql);
      params.push(...v.params);
    }
    /* Applied even when an editor asked for a review status, because it is a
       no-op for them — gateSql returns nothing for anyone who bypasses. Kept
       outside the else so nobody later adds a status filter that quietly
       skips the gate. */
    {
      const g = gateSql(req);
      if (g.sql) { where.push(g.sql); params.push(...g.params); }
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
        `EXISTS (SELECT 1 FROM snippet_taglet st JOIN tag t ON t.id = st.tag_id
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
      `SELECT 1 FROM snippet_taglet st JOIN tag t ON t.id = st.tag_id
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
    if (tagletKind) {
      where.push(`EXISTS (SELECT 1 FROM snippet_taglet st JOIN tag t ON t.id = st.tag_id
                           WHERE st.snippet_id = s.id AND t.kind = ? AND t.retracted_at IS NULL)`);
      params.push(String(tagletKind));
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
    // Same capability as the list above, so one row and a page of rows cannot
    // disagree about who is allowed to see an unpublished clip.
    const mayReview = can(req.person, 'review.read');
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
      logTranscript(req, s.id);
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
      /* Its own verb. Replacing the whole transcript — timings and all — is
         not the same act as fixing a word, and the two should not fold into
         one another's window. */
      logTranscript(req, s.id, 'rewrote the transcript');
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
  app.post('/api/snippets/review', requireCap('review.decide'), (req, res) => {
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
        /* The route has already decided, at `requireRole('editor')` above, and
           the changes it builds are its own rather than the caller's. Saying so
           is what keeps propose()'s missing-person error meaningful everywhere
           else. */
        trusted: true,
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
      /* Approving a clip that is still in quarantine asks the Pi to file the
         MASTER into the media tree. Note what this is not: it is not what
         makes the clip playable. The normalized copy in the cache already
         does that, so the site is correct the moment an editor says yes and
         stays correct if the recorder is off for a week — promote is
         archival, and it is the only step in the whole pipeline that moves a
         file, which is why it is the only step the server does not do.

         Skipped for anything already in the media tree: re-filing an imported
         clip would be a rename of something that is already where it goes. */
      let promoted = 0;
      let queued = 0;
      if (want === 'confirmed') {
        for (const r of move) {
          const q = R.prepare(
            `SELECT quarantine_path, file_path, transcript_status, audio_codec
               FROM snippet WHERE id = ?`).get(r.id);
          if (q?.quarantine_path) {
            /* Both names relative: `from` is the bare filename the accept
               endpoint minted in quarantine and `to` is the same file as the
               media tree will refer to it — which since memes and gallery
               images arrived means `memes/<name>` as often as `snippets/`.
               Quarantine stays flat because do_promote takes its source
               `bare=True`; the destination has always been allowed a
               directory and the worker makes the parent. The recorder joins
               each to its own root and refuses anything that does not stay
               inside them — the archive naming a path it does not itself
               hold a write handle for is a record, not an order.

               Taken now rather than looked up later on purpose: neither
               column moves after accept, and a job that carries its own facts
               cannot be made wrong by an edit made while it sat in the queue. */
            enqueueJob('promote', { snippetId: r.id, by: req.person.id,
                                    payload: { from: q.quarantine_path,
                                               to: q.file_path } });
            promoted++;
          }
          /* Transcription happens AFTER approval, which is the whole reason a
             slow box is acceptable: nothing is spent on a snippet somebody is
             about to reject, and nobody is waiting on the result.

             `none` is the column's word for "a machine should write one" —
             set by the auto-transcribe checkbox at upload. An `edited` or
             `auto` transcript is left alone; something with no audio track at
             all is not worth a queue slot. */
          if (q?.transcript_status === 'none' && q?.audio_codec
              && !R.prepare(`SELECT 1 FROM job WHERE snippet_id = ? AND kind = 'transcribe'
                              AND status IN ('approved','claimed','paused')`).get(r.id)) {
            enqueueJob('transcribe', { snippetId: r.id, by: req.person.id });
            queued++;
          }
        }
      }
      logChangeset(req, out?.status === 'applied' ? out.id : null);
      res.json({ changed: move.length, skipped: have.length - move.length,
                 missing: list.length - have.length, changeset: out.id ?? null,
                 promoted, transcribing: queued });
    } catch (e) { return changeError(res, e); }
  });

  // -------------------------------------------------------------------------
  // suggestions — names waiting to become vocabulary
  //
  // What is NOT here is minting. A suggestion becomes a taglet through the
  // taglet picker and the changeset system, exactly like every other taglet:
  // slug derived from the name, provenance stamped by the applier, and a
  // same-slug collision resolved to the row that already exists rather than
  // failing. A canonize endpoint would be a second implementation of all
  // three, and the second one is always the one that drifts.
  //
  // So the archive only needs to answer two questions here: what is waiting,
  // and this one never will be.
  // -------------------------------------------------------------------------

  /** Every suggestion nobody has acted on, grouped by the slug it would get.
   *
   *  Grouped, because the same name arrives from several people on several
   *  clips and canonizing it five times is how five spellings of it end up in
   *  the vocabulary. The group is the unit of work.
   */
  app.get('/api/suggestions', requireRole('editor'), (req, res) => {
    /* Checked before the work rather than after it. The queue is polled every
       time an editor comes back to the tab and the answer only changes when
       something in the archive does, so a 304 here should cost a generation
       read and nothing else. `personal`, because this is editor-only and a
       shared cache has no business holding it. */
    const etag = etagFor('sugg', generation());
    if (fresh(req, res, etag, { personal: true })) return res.status(304).end();

    /* One query rather than a per-row TAGLETS_OF: the honoured-suggestion
       filter needs each clip's attached slugs, and asking for them a clip at
       a time is how a queue over a few hundred rows becomes a few hundred
       queries. */
    const rows = R.prepare(
      `SELECT s.id, s.title, s.status, s.taglet_suggestions,
              (SELECT group_concat(t.slug, char(10))
                 FROM snippet_taglet st JOIN tag t ON t.id = st.tag_id
                WHERE st.snippet_id = s.id AND t.retracted_at IS NULL) AS have
         FROM snippet s
        WHERE s.taglet_suggestions IS NOT NULL AND s.retracted_at IS NULL`).all();

    const groups = new Map();
    for (const r of rows) {
      let names = [];
      try { names = JSON.parse(r.taglet_suggestions) ?? []; } catch { continue; }
      const have = new Set(String(r.have ?? '').split('\n').filter(Boolean));
      for (const raw of names) {
        const name = String(raw ?? '').trim();
        if (!name) continue;
        const slug = slugify(name);
        /* Already granted, by whatever route. The clip has the taglet, so the
           suggestion is answered and does not belong in a queue of work. */
        if (have.has(slug)) continue;
        let g = groups.get(slug);
        if (!g) {
          g = { slug, variants: new Map(), clips: [], total: 0 };
          groups.set(slug, g);
        }
        g.variants.set(name, (g.variants.get(name) ?? 0) + 1);
        g.total++;
        /* The LIST is capped and the COUNT is not. A name on four hundred
           clips does not become more useful for listing all four hundred, and
           a changeset is capped at 500 changes — but "5 clips" has to be true,
           so the number and the sample are counted separately. Above the cap
           the queue offers the sample and says so; the rest come back on the
           next pass, by which point the taglet exists and the group is an
           attach rather than a mint. */
        if (g.clips.length < 50) {
          g.clips.push({ id: r.id, title: r.title, status: r.status });
        }
      }
    }

    /* The whole vocabulary, once — and whole now means whole. It used to be
       narrowed to the snippet kinds, which meant a clip whose text matched a
       `type` or `elements` tag was reported as having no match at all rather
       than as matching that one. It is a few hundred short rows, every group
       has to be compared against all of it, and fifty queries that each scan
       the same small table is the slower way to write that. */
    const vocab = R.prepare(
      `SELECT id, name, slug, kind FROM tag WHERE retracted_at IS NULL`).all();
    const bySlug = new Map(vocab.map((t) => [t.slug, t]));

    /** Levenshtein, asked only "is it within max" and answered lazily.
     *
     *  The length check rejects almost every pair before any work happens, and
     *  the per-row floor bails as soon as every path through the matrix is
     *  already too far — which for max 2 is nearly always by the third row.
     */
    const within = (a, b, max) => {
      if (Math.abs(a.length - b.length) > max) return false;
      if (a.length > 40 || b.length > 40) return false;
      let prev = Array.from({ length: b.length + 1 }, (_, i) => i);
      for (let i = 1; i <= a.length; i++) {
        const cur = [i];
        let best = i;
        for (let j = 1; j <= b.length; j++) {
          cur[j] = Math.min(prev[j] + 1, cur[j - 1] + 1,
                            prev[j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1));
          if (cur[j] < best) best = cur[j];
        }
        if (best > max) return false;
        prev = cur;
      }
      return prev[b.length] <= max;
    };

    /** Vocabulary this name might have meant.
     *
     *  Two different mistakes, so two different tests, and neither one finds
     *  the other's.
     *
     *  CONTAINMENT is the partial name — `selen` when `selen-tatsuki` was
     *  meant, or somebody typing the full name onto a clip tagged with the
     *  short one. Floored at four characters, because a two-letter taglet is
     *  contained in half the vocabulary and would be offered against
     *  everything.
     *
     *  EDIT DISTANCE is the typo, which containment can never catch: `shork`
     *  and `shark` share no substring worth matching on and are one character
     *  apart. A duplicate taglet born from a single mistyped letter is the
     *  exact thing this queue exists to stop, so it is worth the pass.
     */
    const nearTo = (slug) => {
      const out = [];
      for (const t of vocab) {
        if (t.slug === slug) continue;
        const shorter = Math.min(t.slug.length, slug.length);
        const contains = shorter >= 4
          && (t.slug.includes(slug) || slug.includes(t.slug));
        if (contains || within(slug, t.slug, slug.length <= 5 ? 1 : 2)) out.push(t);
        if (out.length === 5) break;
      }
      return out;
    };

    const out = [...groups.values()]
      .map((g) => ({
        slug: g.slug,
        /* The most-suggested spelling wins the label, because it is the one
           most people meant — and on a tie the first one in, which is stable
           because Map keeps insertion order. The rest ride along so an editor
           can see that "Selen Tatsuki" and "selen tatsuki" were one request
           rather than two. */
        name: [...g.variants.entries()].sort((a, b) => b[1] - a[1])[0][0],
        variants: [...g.variants.keys()],
        count: g.total,
        clips: g.clips,
        exact: bySlug.get(g.slug) ?? null,
        near: nearTo(g.slug),
      }))
      // Most-asked-for first: the name five people wanted is the one worth
      // deciding about, and the singleton typo can wait at the bottom.
      .sort((a, b) => b.count - a.count || a.slug.localeCompare(b.slug));

    res.json({ suggestions: out, count: out.length });
  });

  /** This name is never going to be vocabulary.
   *
   *  The one case the read-time filter cannot cover. A suggestion that gets
   *  honoured disappears on its own, because the clip then has the taglet it
   *  asked for; a typo is answered by nothing ever being attached, so it needs
   *  somebody to say so.
   *
   *  Not a changeset. `taglet_suggestions` is a scratch field between upload
   *  and review, not a curated one, and "who deleted the word funy" is not
   *  history anybody will ever want.
   */
  app.post('/api/suggestions/dismiss', requireRole('editor'), (req, res) => {
    const name = String(req.body?.name ?? '').trim();
    if (!name) return res.status(400).json({ error: 'name is required' });
    const slug = slugify(name);
    const only = req.body?.snippet_id ? String(req.body.snippet_id) : null;

    const rows = only
      ? R.prepare(`SELECT id, taglet_suggestions FROM snippet
                    WHERE id = ? AND taglet_suggestions IS NOT NULL
                      AND retracted_at IS NULL`).all(only)
      : R.prepare(`SELECT id, taglet_suggestions FROM snippet
                    WHERE taglet_suggestions IS NOT NULL
                      AND retracted_at IS NULL`).all();

    const t = now();
    let touched = 0;
    /* Matched by SLUG, not by string. The same request arrives as "Selen
       Tatsuki", "selen tatsuki" and "Selen  Tatsuki"; dismissing one spelling
       and leaving the others is not dismissing anything. */
    tx(W, () => {
      const upd = W.prepare(
        'UPDATE snippet SET taglet_suggestions = ?, updated_at = ? WHERE id = ?');
      for (const r of rows) {
        let names = [];
        try { names = JSON.parse(r.taglet_suggestions) ?? []; } catch { continue; }
        const kept = names.filter((x) => slugify(String(x ?? '')) !== slug);
        if (kept.length === names.length) continue;
        upd.run(kept.length ? JSON.stringify(kept) : null, t, r.id);
        touched++;
      }
    });
    if (touched) bumpGeneration(W);
    res.json({ ok: true, dismissed: touched, slug });
  });

  app.get('/api/taglets', (req, res) => {
    const { q = '', kind } = req.query;
    const limit = Math.min(Math.max(Number(req.query.limit ?? 500) || 500, 1), 2000);
    /* Per-person, because the answer is: a gating taglet is absent for anyone
       who cannot open it, so one shared validator would hand the full
       vocabulary to whoever asked next. Same reasoning as the snippet list. */
    const mayAll = atLeast(req.person, 'editor');
    const held = mayAll ? ['*'] : grantsOf(req.person?.id ?? null).sort();
    const etag = etagFor('taglets', q, kind, limit, held);
    if (fresh(req, res, etag, { personal: !!req.person?.id })) return res.status(304).end();

    /* One vocabulary. This used to serve only the snippet half of it, on the
       grounds that `type` and `elements` describe a broadcast rather than a
       clip — true, and not worth a second list to enforce. A clip of the intro
       tagged Intro is a reasonable thing for an editor to want. */
    const where = ['t.retracted_at IS NULL'];
    const params = [];
    /* A gating taglet is invisible to anyone it gates. Leaving it in leaks the
       size of the restricted set through `uses`, and offers a filter that
       silently returns nothing — the clips behind it are already excluded by
       the snippet list's own gate. */
    if (!mayAll) {
      if (held.length) {
        where.push(`(t.gate IS NULL OR t.gate IN (${held.map(() => '?').join(',')}))`);
        params.push(...held);
      } else {
        where.push('t.gate IS NULL');
      }
    }
    if (q) {
      where.push('(lower(t.name) LIKE ? OR t.slug LIKE ?)');
      const like = `%${String(q).trim().toLowerCase()}%`;
      params.push(like, like);
    }
    if (kind) { where.push('t.kind = ?'); params.push(String(kind)); }

    // Count comes back with the row because the picker is useless without it:
    // "funny (214)" and "funny (1)" are different suggestions.
    res.json({ taglets: R.prepare(
      `SELECT t.id, t.name, t.slug, t.kind, t.status, t.summary, t.gate,
              (SELECT count(*) FROM snippet_taglet st JOIN snippet s ON s.id = st.snippet_id
                WHERE st.tag_id = t.id AND s.retracted_at IS NULL) AS uses
         FROM tag t WHERE ${where.join(' AND ')}
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
      /* Carried here so a page notices a change it did not make itself. Every
         list response already reports the generation, but a reader sitting on
         one panel makes no list requests — and this poll is the one thing the
         page does on a timer. It is how another tab's mint, or somebody else's
         approval, reaches a tab that is just sitting there. */
      generation: Number(generation()),
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

  /** The recording running for this stream right now, or null.
   *
   *  Built on liveState() rather than on `live.recording` directly, so the id
   *  resolution — remote_id first, then the vault index — stays in one place
   *  and a clip cannot be aimed at a stream the badge disagrees about.
   *
   *  With no platform named, Twitch wins. That is not a preference about
   *  platforms, it is the recorder's own rule and its reason is good: Twitch
   *  records at the live edge, so its file holds the moment you just watched,
   *  while a --live-from-start YouTube capture can be minutes behind and
   *  simply not have those frames yet. See _pick_stream in ls_rec.py.
   */
  const liveRecFor = (streamId, want = null) => {
    const st = liveState();
    if (st.stale) return null;
    for (const p of (want ? [want] : ['TW', 'YT'])) {
      const r = st[p];
      if (r?.live && r.stream_id === streamId) return { platform: p, ...r };
    }
    return null;
  };

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

  app.post('/api/admin/people/:id/role', requireCap('people.manage'), (req, res) => {
    const { role } = req.body ?? {};
    if (!ROLES.includes(role)) return res.status(400).json({ error: `role must be one of ${ROLES}` });
    if (req.params.id === req.person.id && role !== 'admin') {
      return res.status(400).json({ error: 'refusing to demote yourself — you would ' +
        'lock yourself out of the only account that can undo it' });
    }
    if (!W.prepare('SELECT 1 FROM person WHERE id = ?').get(req.params.id)) {
      return res.status(404).json({ error: 'no such person' });
    }
    const was = R.prepare('SELECT role FROM person WHERE id = ?').get(req.params.id)?.role;
    W.prepare('UPDATE person SET role = ? WHERE id = ?').run(role, req.params.id);
    logEvent(req, 'changed a role', 'person', req.params.id, { from: was, to: role });
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

  /* `.gif` joins the images for the chat emote tree, and belongs on this list
     for the same reason the others do: it is a picture format no browser
     executes. The renderer asks for the .png beside it, so this is for the day
     it wants the animation — and for the directory not being half-servable in
     the meantime, which is the kind of gap somebody debugs twice. */
  const MIME = { '.mp4': 'video/mp4', '.webm': 'video/webm', '.mkv': 'video/x-matroska',
                 '.m4a': 'audio/mp4', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
                 '.png': 'image/png', '.webp': 'image/webp', '.gif': 'image/gif' };

  /* `type` overrides the extension table when the caller knows better — which
     for snippets it does, because the row records the codecs the file actually
     holds and the extension is only a claim. `root` likewise: a remuxed clip
     lives under the cache root, not the media root. */
  /** What a downloaded clip should be called.
   *
   *      Kaneko-Lumi_Nitya-Nil_Stop jorking your peanits.mp4
   *
   *  Tags first, hyphenated and underscore-joined, then the title. The tags
   *  come first because they are what makes the name sortable and greppable in
   *  a folder of two hundred clips; the title is the part a human reads.
   *
   *  Built on the SERVER rather than in the page, so a right-click "save as"
   *  on the download link gets the same name as the button — and so the page
   *  does not have to fetch a 40 MB file into a blob just to rename it, which
   *  is the usual client-side way to do this and costs the whole file twice.
   *
   *  Truncated at a word boundary. The obvious version cuts mid-word and
   *  leaves "...jorking your peanits around l", which reads like a corrupted
   *  filename rather than a shortened one.
   */
  const dlName = (row, taglets, ext) => {
    // Everything Windows, macOS and Linux disagree about, plus control chars.
    const clean = (v) => String(v ?? '')
      .replace(/[\u0000-\u001f<>:"/\\|?*]+/g, ' ')
      .replace(/\s+/g, ' ')
      .trim();
    const tags = taglets.map((t) => clean(t.name).replace(/ /g, '-'))
      .filter(Boolean).slice(0, 6);
    /* A title when there is one, and the row's own slug when there is not —
       which on a picture is the ordinary state, because its filename was
       gibberish and was deliberately not turned into a name. The slug is the
       minted id there, so it is ugly and it is UNIQUE: a folder of thirty
       downloaded memes has thirty names rather than `picture (29).jpg`. The
       tags still lead, so what makes the name readable is what somebody
       actually chose to say about it. */
    const title = clean(row.title) || clean(row.slug) || 'clip';

    const ROOM = 120 - ext.length;

    /* The tags are trimmed FIRST, by dropping whole ones off the end. Six long
       character names can fill the budget on their own, and the alternative is
       a filename that is all tags and two words of title — or, with a blunt
       slice at the end, a tag cut in half. A dropped tag is still a legible
       name; "Kaneko-Lu" is not. */
    const TAGROOM = Math.floor(ROOM * 0.55);
    let kept = tags;
    while (kept.length > 1 && kept.join('_').length + 1 > TAGROOM) kept = kept.slice(0, -1);
    let prefix = kept.length ? `${kept.join('_')}_` : '';
    if (prefix.length > TAGROOM) prefix = '';   // one tag, longer than the budget

    let name = prefix + title;
    if (name.length > ROOM) {
      const room = ROOM - prefix.length;
      const cut = title.slice(0, room);
      // Back up to the last space, unless that would leave almost nothing.
      const sp = cut.lastIndexOf(' ');
      name = prefix + (sp > room * 0.5 ? cut.slice(0, sp) : cut).trim();
    }
    return name + ext;
  };

  /* Both forms, because neither alone is enough. `filename=` is ASCII-only and
     every browser understands it; `filename*=` carries the UTF-8 and newer
     browsers prefer it. A title with a Japanese word in it — which in this
     archive is most of them — needs the second and must not break the first. */
  const disposition = (name) => {
    const ascii = name.replace(/[^\x20-\x7e]/g, '_').replace(/"/g, "'");
    return `attachment; filename="${ascii}"; filename*=UTF-8''${encodeURIComponent(name)}`;
  };

  function sendMedia(req, res, rel, { type = null, root = null, filename = null } = {}) {
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
    if (filename) res.set('Content-Disposition', disposition(filename));
    return res.sendFile(path, { acceptRanges: true, cacheControl: true, maxAge: '1h' });
  }

  app.get('/media/video/:capture_id', (req, res) => {
    const c = R.prepare('SELECT video_path FROM capture WHERE id = ?').get(req.params.capture_id);
    if (!c?.video_path) return res.status(404).json({ error: 'no such capture' });
    return sendMedia(req, res, c.video_path);
  });

  app.get('/media/thumb/:rest(*)', (req, res) => sendMedia(req, res, req.params.rest));

  /* The merged chat, by stream id.
   *
   * `.json` is deliberately NOT in the MIME table above, so this cannot go
   * through sendMedia and should not: what makes the path-taking thumb route
   * safe is that every type it can serve is one a browser will not execute,
   * and JSON on your own origin is not that. So chat is addressed the way
   * video and snippets are — by id, with the row holding the only path anyone
   * gets to name — and never by a path off the URL.
   *
   * Ungated, matching /media/video: the VODs are open and the chat is the same
   * broadcast. When a VOD gate arrives this is where chat joins it, and
   * /media/snippet is the pattern to copy — the lesson there was that gating
   * the metadata and leaving the bytes open is not gating.
   */
  /* What pictures the chat assets tree actually holds.
   *
   * ls_assets writes an index beside each tree recording everything it has
   * fetched — the file, whether an animated twin came with it, and what came
   * back 404. Without it the page has to find out by asking: one request per
   * distinct emote per stream, most of them 404s, and a visible flicker while
   * an emote that was never fetched renders as a broken image before falling
   * back to its own name.
   *
   * Small, static, and the same answer for the whole archive, so it is read
   * whole and cached. Re-read when either file's mtime moves, which is the
   * only thing that changes it.
   */
  const CHAT_ASSET_MAX = 8 * 1024 * 1024;
  let chatAssets = null;      // { at: [mtimeMs, mtimeMs], body: string }

  const chatAssetIndex = (tree) => {
    const p = resolveMedia(config.mediaRoot, `${tree}/index.json`);
    if (!p) return [0, {}];
    try {
      const st = statSync(p);
      if (st.size > CHAT_ASSET_MAX) return [st.mtimeMs, {}];
      const got = JSON.parse(readFileSync(p, 'utf8'));
      return [st.mtimeMs, (got && got.entries) || {}];
    } catch {
      // A half-written or hand-edited index is a slower page, not a broken
      // one: everything falls back to asking for the .png.
      return [0, {}];
    }
  };

  app.get('/media/chat-assets', (req, res) => {
    if (!config.mediaRoot) {
      return res.status(503).json({ error: 'no media root — set TENMA_MEDIA_ROOT' });
    }
    const [em, emotes] = chatAssetIndex('emotes');
    const [bm, badges] = chatAssetIndex('badges');
    if (!chatAssets || chatAssets.at[0] !== em || chatAssets.at[1] !== bm) {
      chatAssets = { at: [em, bm], body: JSON.stringify({ emotes, badges }) };
    }
    res.set('X-Content-Type-Options', 'nosniff');
    res.set('Cache-Control', 'public, max-age=300');
    res.type('application/json; charset=utf-8');
    return res.send(chatAssets.body);
  });

  app.get('/media/chat/:stream_id', (req, res) => {
    const s = R.prepare(
      'SELECT chat_path FROM stream WHERE id = ? AND retracted_at IS NULL')
      .get(req.params.stream_id);
    if (!s?.chat_path) return res.status(404).json({ error: 'no chat for this stream' });
    if (!config.mediaRoot) {
      return res.status(503).json({ error: 'no media root — set TENMA_MEDIA_ROOT' });
    }
    const stored = resolveMedia(config.mediaRoot, s.chat_path);
    if (!stored) return res.status(404).json({ error: 'not on disk' });

    const ae = String(req.headers['accept-encoding'] ?? '');
    // `gzip;q=0` is the spelling for "I could, but do not send it". Nothing
    // real says it, and honouring it costs one more test.
    const wantsGz = /(^|,)\s*gzip\s*(;|,|$)/.test(ae)
                    && !/gzip\s*;\s*q=0(\.0*)?(\s|,|$)/.test(ae);

    /* Two spellings, both real. ls-audit writes the merged chat compressed and
       only compressed now — it is the most compressible file in the archive
       and nothing in this stack compresses — but every entry merged before
       that is a plain .json with a .gz written beside it. So: serve whichever
       the row points at, reach for the twin when there is one, and gunzip on
       the way out for the rare client that cannot take it. Keeping a plain
       copy on disk purely for that client would be eight times the bytes for
       something no browser has asked for in twenty years. */
    const packed = s.chat_path.endsWith('.gz');
    const twin = !packed && wantsGz
      ? resolveMedia(config.mediaRoot, s.chat_path + '.gz') : null;

    // Without this a shared cache can hand the compressed bytes to a client
    // that just said it cannot read them.
    res.set('Vary', 'Accept-Encoding');
    res.set('X-Content-Type-Options', 'nosniff');
    res.type('application/json; charset=utf-8');
    // Nothing seeks a chat log, and half a gzip stream is not JSON.
    res.set('Accept-Ranges', 'none');

    if (packed && !wantsGz) {
      // Streamed through gunzip rather than read into memory: a merged chat is
      // a megabyte and this is the path nobody takes.
      res.set('Cache-Control', 'public, max-age=3600');
      const gunzip = createGunzip();
      const src = createReadStream(stored);
      src.on('error', () => res.destroy());
      gunzip.on('error', () => res.destroy());
      return src.pipe(gunzip).pipe(res);
    }
    // Set here rather than through sendFile's `headers` option: send() writes
    // Content-Length from the file it is actually streaming, which for the
    // .gz is the encoded length, and that is the pair that has to agree.
    if (packed || twin) res.set('Content-Encoding', 'gzip');
    return res.sendFile(twin ?? stored,
                        { acceptRanges: false, cacheControl: true, maxAge: '1h' });
  });

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
  /* Two spellings of one route, and the second exists for one reason: the
     link a person pastes somewhere else.
     
         /media/snippet/01J8.../shiina-stare.png
     
     Everything after the id is ignored — the id is still the only thing that
     names a file and the gate below is still the only thing that decides —
     but a URL that ENDS in .png is what an unfurler will treat as an image,
     and it is what a browser's Save-image-as offers as the filename. The
     alternative is asking every embedder to trust a Content-Type header, and
     several of the ones people actually paste into do not.
     
     No new surface: same handler, same visibility check, same bytes. The
     trailing segment is never joined to a path. */
  /* What /media/snippet/<id> will actually answer with, decided the same way
     the route below decides it.
     Pulled out because the share card's meta tags have to name the SERVED
     container rather than the uploaded one: a VP9 upload is handed out as the
     normalized mp4, and an unfurler told `video/webm` about an mp4 plays
     nothing at all and says nothing about why. */
  const playType = (s) => {
    if (s?.play_path && config.cacheRoot) {
      const e = extname(s.play_path).toLowerCase();
      return e === '.webm' ? 'video/webm' : e === '.m4a' ? 'audio/mp4' : 'video/mp4';
    }
    return servedType(s?.container, s?.video_codec, s?.audio_codec);
  };

  const sendSnippetMedia = (req, res) => {
    /* `slug` and `kind` are not decoration: dlName() falls back to the slug
       when there is no title, which on a picture is the ordinary case, and
       without it every untitled meme downloaded under the same name. */
    const s = R.prepare(`SELECT id, slug, kind, title, file_path, play_path, quarantine_path,
                                container, video_codec, audio_codec, status, author_id,
                                normalize_status, fetch_status
                           FROM snippet WHERE id = ? AND retracted_at IS NULL`).get(req.params.id);
    if (!snipVisible(s, req)) return res.status(404).json({ error: 'no such snippet' });
    if (!s?.file_path) return res.status(404).json({ error: 'no such snippet' });

    /* ?dl=1 — the same bytes, offered as a file with a name on it.
       Handled before the play-path preference below because the answer to
       "which file" is different: what you WATCH is the normalized copy, and
       what you SAVE is usually that too, except for a GIF. A gif is converted
       to H.264 so it can be played at all, and downloading the mp4 of a gif is
       not what anybody wanting the gif meant — so for those the original is
       what comes back. */
    if (req.query.dl) {
      const isGif = /gif/i.test(String(s.container ?? ''));
      const src = isGif || !s.play_path
        ? (s.quarantine_path
            ? { rel: s.quarantine_path, root: config.quarantineRoot }
            : { rel: s.file_path, root: config.mediaRoot })
        : { rel: s.play_path, root: config.cacheRoot };
      if (!src.rel || !src.root) return res.status(404).json({ error: 'not on disk' });
      const taglets = TAGLETS_OF.all(s.id);
      const name = dlName(s, taglets, extname(src.rel).toLowerCase() || '.mp4');
      return sendMedia(req, res, src.rel, { root: src.root, filename: name });
    }

    /* A normalized copy wins over EVERYTHING, including a clip still sitting
       in quarantine. This block used to come second and that was a real bug
       the moment the worker existed: a VP9 upload would have a perfectly good
       H.264 copy in the cache and be handed the original anyway, so the one
       person who has to watch it before it goes anywhere got a 415.

       It lives under the cache root because the media root is read-only and
       holds the masters — which is also what lets the server do the
       conversion at all. */
    if (s.play_path && config.cacheRoot) {
      return sendMedia(req, res, s.play_path,
                       { type: playType(s), root: config.cacheRoot });
    }

    /* Nothing has been downloaded yet. Without this the fallthrough below
       resolves file_path to a file that does not exist and answers "not on
       disk", which is true and unhelpful — the clip is not missing, it has
       not arrived. */
    if (s.fetch_status === 'queued' || s.fetch_status === 'running') {
      return res.status(415).json({
        error: 'waiting for the recorder to fetch this one',
        fetch_status: s.fetch_status });
    }

    /* No normalized copy, so it is either conformant already or still
       converting. Either way the bytes are wherever the row says: quarantine
       while it waits for a human, the media tree once the Pi has filed it.
       The visibility check above already decided whether this person may look. */
    if (s.quarantine_path && config.quarantineRoot) {
      const qt = servedType(s.container, s.video_codec, s.audio_codec);
      if (!qt) {
        /* Honest, and specific about which of the two it is. "Come back in a
           minute" and "this will never play" are different answers and the
           uploader can act on exactly one of them. */
        return res.status(415).json({
          error: s.normalize_status === 'running' || s.normalize_status === 'queued'
            ? 'still converting — try again in a moment'
            : `not playable as uploaded: ${s.video_codec}/${s.audio_codec} in ${s.container}`,
          normalize_status: s.normalize_status ?? null,
        });
      }
      return sendMedia(req, res, s.quarantine_path, { type: qt, root: config.quarantineRoot });
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
    return sendMedia(req, res, s.file_path, { type });
  };
  app.get('/media/snippet/:id', sendSnippetMedia);
  app.get('/media/snippet/:id/:name', sendSnippetMedia);

  /* Posters resolve against the CACHE root first, then the media root.
     Two roots because they are written by different things: the importer
     generates into the cache, while your own prep pass may already have left
     stills beside the clips under media. Both are legitimate and neither
     should have to know about the other — and resolveMedia() does the same
     containment check either way, so the fallback opens no path it would not
     already have opened. */
  app.get('/media/snippet-poster/:id', (req, res) => {
    const s = R.prepare(
      // `id` is not decoration: snipVisible() asks the gate about this row by
      // id, and without it the check cannot run.
      `SELECT id, poster_path, status, author_id FROM snippet
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
    // The merged chat, so the path to it is picked the same way the video and
    // the thumbnail are. An unknown kind means NO filter, not an empty list,
    // so leaving this out would have listed the whole directory.
    // `.gz` because a merged chat is written compressed: extname of
    // `007_merged-chat.json.gz` is `.gz`, and without this the picker cannot
    // see the only file it exists to pick.
    chat: new Set(['.json', '.jsonl', '.gz']),
    // Posters, narrower than `image` on purpose: the pickers that use this one
    // choose a still that goes on a card, and the archive writes those as PNG.
    // `image` stays for anything that wants the wider set.
    png: new Set(['.png']),
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
    const all = names.slice(0, 4000).map((name) => {
      let mtime = 0, bytes = null;
      try { const st = statSync(join(dir, name)); mtime = st.mtimeMs; bytes = st.size; }
      catch { /* vanished between readdir and stat */ }
      return { name, path: rel ? `${rel}/${name}` : name, bytes, mtime };
    });
    all.sort((a, b) => b.mtime - a.mtime);

    /* Sorted BEFORE the slice, which is the whole reason paging works: an
       offset into an unsorted list is a different set of files every request,
       and a picker scrolling through it would show duplicates and gaps. */
    const offset = Math.max(Number(req.query.offset ?? 0) || 0, 0);
    // A page the caller asks for, capped by the one the server is willing to
    // build. The pickers ask for a screenful at a time so the list stays small
    // enough to render; anything else gets the old behaviour by not asking.
    const limit = Math.min(Math.max(Number(req.query.limit ?? BROWSE_MAX) || BROWSE_MAX, 1),
                           BROWSE_MAX);
    const files = all.slice(offset, offset + limit);
    const next = offset + files.length;

    res.set('Cache-Control', 'no-store');
    res.json({
      dir: rel, parent: rel ? rel.split('/').slice(0, -1).join('/') : null,
      dirs: dirs.sort(), files,
      // `next` is null at the end, so a caller pages until it stops rather than
      // arithmetic-ing its way there against a total it might disagree with.
      next: next < all.length ? next : null,
      offset,
      truncated: all.length > BROWSE_MAX, total: names.length,
    });
  });

  /* A poster, uploaded rather than picked.
   *
   *  Quarantine, then a promote job — NOT the cache. /cache says of itself
   *  that everything under it is regenerable from the media and wants no
   *  backup; a still somebody just chose is the only copy of a decision, and
   *  quarantine is the root that exists for exactly that. It also means the
   *  poster ends up in the media tree where /media/thumb already serves from,
   *  so nothing about serving has to change.
   *
   *  The Pi needs no change either: do_promote resolves `to` against its own
   *  media root and refuses anything that escapes it, and has never cared that
   *  every destination so far happened to start with `snippets/`.
   *
   *  The cost is one round trip of latency: the poster is not on disk in the
   *  served tree until ls_jobs.py has run, so the card keeps its old still
   *  until then rather than showing a hole.
   */
  /* Go and look at what this stream actually has.
   *
   * The archive cannot: the masters are on a read-only mount and the platform
   * probe needs a network and a cookie jar that live on the recorder. So it
   * writes the job down, with everything the worker needs to do it in the
   * payload — the same shape promote and purge use, and for the same reason.
   *
   * By hand only, for now. A sweep over everything is what actually wants
   * doing eventually, and it waits on rate limiting rather than on this.
   */
  app.post('/api/streams/:id/rescan', requireRole('editor'), (req, res) => {
    const s = R.prepare('SELECT id FROM stream WHERE id = ? AND retracted_at IS NULL')
      .get(req.params.id);
    if (!s) return res.status(404).json({ error: 'no such stream' });
    const caps = R.prepare(
      `SELECT id, platform, remote_id, url, video_path FROM capture WHERE stream_id = ?`)
      .all(s.id);
    if (!caps.length) {
      return res.status(400).json({ error: 'this stream has no captures to look at' });
    }
    /* Already queued? Asking twice is how a rate limit gets hit for nothing,
       and the second answer would be the same as the first. */
    const open = R.prepare(
      `SELECT id FROM job WHERE kind = 'rescan' AND status IN ('approved','claimed')
         AND payload LIKE ?`).get(`%"${s.id}"%`);
    if (open) return res.json({ job_id: open.id, already: true });

    const id = enqueueJob('rescan', {
      payload: { stream_id: s.id, captures: caps.map((c) => ({
        id: c.id, platform: c.platform, remote_id: c.remote_id,
        url: c.url, video_path: c.video_path })) },
      by: req.person.id,
    });
    bumpGeneration(W);
    logEvent(req, 're-read a stream from its files and links', 'stream', s.id);
    res.json({ job_id: id, captures: caps.length });
  });

  /* Go and read what that link says about this tag.
   *
   * The archive cannot: it opens no outbound socket, deliberately, which is
   * the whole reason the job queue exists. It also should not — a URL an
   * editor typed, fetched by this process, is a request from inside the NAS's
   * own network. The recorder already has an allowlist for exactly this.
   *
   * Destructive by design: a re-seed replaces the description AND the art, and
   * the button says so. What makes that recoverable is that the old summary is
   * in the change log if a human ever wrote one, and `seeded` says whether one
   * did.
   */
  /** Destroy a tag and every link to it. The archive's second irreversible
   *  action, and the first that can take editorial work with it.
   *
   *  TWO CALLS, ALWAYS. The first returns what would be destroyed and changes
   *  nothing; the second has to name the same counts back. That is not
   *  ceremony — retracting is the reversible verb and every surface offers it,
   *  so the only reason to reach this one is to remove a tag that should never
   *  have existed, and the difference between that and a tag with forty
   *  chapters on it is exactly the number this hands back. A confirm dialog
   *  that cannot say how much it is about to take is not a confirmation.
   *
   *  Deliberately NOT a changeset. A changeset's `delete` op tombstones — that
   *  is `tag.retract`, and it is what an editor is trusted with. This removes
   *  rows, and rows that are gone cannot be reviewed, reverted or explained
   *  later. It gets its own capability and its own log line instead.
   *
   *  `segment.tag_id` and `snippet.taglet_suggestions` are left alone on
   *  purpose: ON DELETE SET NULL takes the block's link and leaves the block,
   *  which is right — somebody drew that chapter, and the tag being wrong is
   *  not a reason for the hour of stream it marks to disappear.
   */
  // =========================================================================
  // music — somebody else's video, kept because it will not always be there
  //
  // The whole module is four routes, and that is the point of building it now:
  // submission reuses the link canonicaliser the snippet uploader already has,
  // retraction is a changeset like any other decision, and the capability
  // table decides who may do what. Only the verdict needed a route of its own.
  // =========================================================================

  /** The YouTube id out of a canonical watch URL. linkUrl() has already
   *  folded youtu.be and stripped everything but `v`, so this is a lookup and
   *  not a parse — and it is the identity the row is keyed on, because the
   *  same video reaches the archive as three different strings. */
  const videoIdOf = (url) => {
    try { return new URL(url).searchParams.get('v') || null; } catch { return null; }
  };

  /** One row, as the page reads it.
   *
   *  `embed` rather than a media path, because that is the only way this is
   *  ever watched. `thumb` prefers the preserved copy and falls back to
   *  YouTube's own — before approval there IS no preserved copy, and the
   *  viewer's browser is talking to YouTube for the embed regardless. */
  const musicRow = (r, tags = []) => ({
    id: r.id, video_id: r.video_id, url: r.url,
    title: r.title, channel: r.channel, channel_id: r.channel_id,
    uploaded_at: r.uploaded_at, duration_s: r.duration_s,
    status: r.status, note: r.note ?? null,
    probe_status: r.probe_status, probe_note: r.probe_note ?? null,
    fetch_status: r.fetch_status, fetch_note: r.fetch_note ?? null,
    // Whether the archive actually holds a copy. The point of the module.
    preserved: !!r.video_path,
    /* Two different removals, and the panel shows them in one pile — so the
       card has to be able to say which one happened to it. `rejected` is a
       verdict on whether the song belongs; this is a tombstone on the row. */
    retracted: !!r.retracted_at,
    thumb: r.thumb_path ? `/media/thumb/${r.thumb_path}`
                        : `https://i.ytimg.com/vi/${r.video_id}/hqdefault.jpg`,
    embed: `https://www.youtube-nocookie.com/embed/${r.video_id}`,
    author_id: r.author_id,
    /* Present only where the query asked for it — the list joins it in, the
       single-row reads do not. `?? null` rather than leaving it undefined so
       the field is always in the JSON and the page has one thing to test. */
    author: r.author_handle ?? null,
    created_at: r.created_at, updated_at: r.updated_at,
    tags,
  });

  const musicTagsFor = (ids) => {
    const out = new Map(ids.map((id) => [id, []]));
    if (!ids.length) return out;
    for (const r of R.prepare(
      /* `link_id` is the JUNCTION row, which is what a detach deletes — the tag
         itself survives being taken off a song. It rides along here because
         the alternative is what the snippet strip does: a second request per
         removal just to ask which row joins these two. Same column, same
         query, no round trip. */
      `SELECT mt.music_id, mt.id AS link_id, t.id, t.name, t.slug, t.kind
         FROM music_tag mt JOIN tag t ON t.id = mt.tag_id
        WHERE mt.music_id IN (${ids.map(() => '?').join(',')})
          AND t.retracted_at IS NULL
        ORDER BY t.kind, t.name`).all(...ids)) {
      out.get(r.music_id)?.push({ id: r.id, link_id: r.link_id,
                                  name: r.name, slug: r.slug, kind: r.kind });
    }
    return out;
  };

  /** Who may see a row that is not published.
   *
   *  Same rule as snippets: a proposed entry is visible to the person who
   *  submitted it and to anyone who may decide on it, and to nobody else. */
  const musicVisibleSql = (req) => {
    if (can(req.person, 'music.decide')) return { sql: '1=1', params: [] };
    const me = req.person?.id ?? null;
    return me
      ? { sql: `(m.status = 'confirmed' OR m.author_id = ?)`, params: [me] }
      : { sql: `m.status = 'confirmed'`, params: [] };
  };

  /* ---- concerts ----------------------------------------------------------
   *
   * A concert is a song that is really a set: a whole karaoke stream, an
   * anniversary live, a three-hour unarchived. It belongs on the same shelf —
   * it is still her singing, still found by the same search — but it is not
   * what somebody scrolling for a song is looking for, and mixed in among
   * three-minute covers it buries them.
   *
   * A TAG and not a column, and not a bare duration test either, which is the
   * whole design in one sentence: the length is a good guess and only a guess.
   * A fourteen-minute single with a long instrumental is not a concert and a
   * tightly-edited half-hour medley is; storing the guess as a tag means the
   * archive proposes and a person corrects, in the tag popover that already
   * exists, with no new UI and no new vocabulary. It is also why "concert"
   * typed into the music search finds them: that box already searches tags.
   *
   * `type`, because the kind vocabulary already has a word for this class of
   * thing — collab, watchalong, karaoke, zatsudan are all `type`, and they are
   * all "what this broadcast IS" rather than what it is about.
   */
  const CONCERT_MIN_S = 600;

  /* Minted on demand and only once. `vault` rather than `user`, because
     nobody proposed it — it is the archive's own word, the same as every row
     the importer wrote — and `confirmed` so it appears in the picker the
     moment it exists rather than waiting in a queue nobody opened. */
  /* Both reads go through W, and that is the whole of a bug worth writing down.
   *
   * They went through R, and R is a different connection. So inside the
   * backfill's transaction the tag this function had just INSERTED was
   * invisible to it: the first long song minted `concert`, the second one's
   * lookup found nothing — an uncommitted write is not there for a reader —
   * and it tried to mint the same slug again. UNIQUE(slug), rollback, the
   * whole pass lost, the meta flag never set, and the identical failure on
   * every boot after. Silent apart from one line in the log, and the visible
   * symptom was simply an empty Concerts shelf.
   *
   * It needed two concert-length songs to happen at all, which is why it
   * survived a test suite that had one.
   *
   * `ON CONFLICT DO NOTHING` plus a re-read is the belt to that brace: a row
   * minted by something else between the two statements now resolves rather
   * than throwing. */
  const concertTagId = () => {
    const have = W.prepare("SELECT id FROM tag WHERE slug = 'concert'").get();
    if (have) return have.id;
    const id = ulid(), t = now();
    W.prepare(`INSERT INTO tag(id, name, slug, kind, status, origin, created_at, updated_at)
               VALUES(?, 'Concert', 'concert', 'type', 'confirmed', 'vault', ?, ?)
               ON CONFLICT(slug) DO NOTHING`).run(id, t, t);
    return W.prepare("SELECT id FROM tag WHERE slug = 'concert'").get()?.id ?? id;
  };

  const isConcertTagged = W.prepare(
    `SELECT 1 FROM music_tag mt JOIN tag t ON t.id = mt.tag_id
      WHERE mt.music_id = ? AND t.slug = 'concert'`);

  /** Put the tag on, unless it is already there. Returns whether it moved. */
  const markConcert = (musicId) => {
    if (isConcertTagged.get(musicId)) return false;
    const t = now();
    W.prepare(`INSERT INTO music_tag(id, music_id, tag_id, created_at, updated_at)
               VALUES(?,?,?,?,?)`).run(ulid(), musicId, concertTagId(), t, t);
    return true;
  };

  /* Once, ever, for the rows that were here before this existed.
   *
   * Guarded by a meta flag rather than by "has no concert tag", because those
   * two are the same question only on the first run: after somebody takes the
   * tag OFF a fourteen-minute single, an unguarded pass would put it back on
   * every restart and there would be no way to win an argument with a boot
   * sequence. */
  if (!meta(R, 'music_concert_backfill')) {
    const long = R.prepare(
      `SELECT id FROM music WHERE duration_s >= ? AND retracted_at IS NULL`)
      .all(CONCERT_MIN_S);
    let n = 0;
    try {
      tx(W, () => { for (const m of long) if (markConcert(m.id)) n++; });
      setMeta(W, 'music_concert_backfill', String(now()));
      if (n) {
        bumpGeneration(W);
        console.log(`  concerts    tagged ${n} existing entr${n === 1 ? 'y' : 'ies'}`);
      }
    } catch (e) {
      /* Never worth failing a boot over. Unflagged, so the next start tries
         again — and markConcert is idempotent, so a partial pass costs
         nothing but the second attempt. */
      console.error('concert backfill:', e?.message ?? e);
    }
  }

  app.get('/api/music', (req, res) => {
    const limit = Math.min(Math.max(Number(req.query.limit ?? 200) || 200, 1), 500);
    const vis = musicVisibleSql(req);
    const gate = gateSql(req, { junction: 'music_tag', fk: 'music_id', alias: 'm' });
    const where = [vis.sql];
    const params = [...vis.params];
    if (gate.sql) { where.push(gate.sql); params.push(...gate.params); }

    /* `?status=` picks a pile. It narrows what the two clauses above allow and
       can never widen it.
     *
     * `rejected` is the odd one and it is the point of this shape: the removed
     * pile holds BOTH ways a song leaves the collection — turned down, which
     * is a verdict on whether it belongs, and retracted, which is a tombstone
     * on the row. They arrive by different routes and they mean different
     * things, but from the shelf they are one answer: it is not here any more.
     *
     * Retracted rows used to be filtered out unconditionally, everywhere, so a
     * retraction removed a song from every view with no screen that could
     * bring it back — while its own confirm dialog promised it could be. This
     * is where they go now, and where an admin can finish the job.
     */
    const want = String(req.query.status ?? '');
    if (want === 'rejected') {
      where.push(`(m.status = 'rejected' OR m.retracted_at IS NOT NULL)`);
    } else if (['proposed', 'confirmed'].includes(want)) {
      where.push('m.retracted_at IS NULL');
      where.push('m.status = ?'); params.push(want);
    } else if (can(req.person, 'music.decide')) {
      /* No pile asked for, and somebody who may decide: the whole collection,
         every status, tombstones included. The panel has no pile switcher any
         more — a waiting song is drawn in yellow and a removed one in red,
         where the songs are — so the ONE list has to be able to contain them.
         Tombstones especially: they are reversible, and a reversible thing
         nothing can show you is a thing nobody will ever reverse. */
    } else {
      /* Everyone else gets the collection: published songs, plus their own
         submission so that pasting a link visibly does something. A song
         somebody turned down is not part of the collection, and a tombstoned
         row is not either. */
      where.push('m.retracted_at IS NULL');
      where.push(`m.status <> 'rejected'`);
    }

    /* Search, and it is the snippet search's LIKE half and nothing else.
       There is no music_fts and there should not be: FTS earns its place over
       there because a transcript is thousands of words, and everything
       searchable here — a title, a channel, a tag's name — is a handful. A
       porter-stemmed index over those would be slower to maintain than the
       scan it replaced AND worse at the job, because it cannot match
       mid-word: `fuura` would stop finding "Fuura Yuri". The snippet route
       says the same thing about its own two LIKEs, for the same reason.

       Tags are searched alongside the text rather than through a separate
       control. On this collection they are how you look for anything — the
       question is nearly always "what has she sung" — and one box that
       answers it needs no explaining. */
    /* ?view=songs|concerts — the shelf's two halves, and they are exclusive by
       construction: `songs` is everything the concert tag is NOT on, so the
       two counts add up to the collection and nothing sits in both.
       Filtered HERE and not in the browser, because the list is capped: a page
       narrowed client-side would show whatever fraction of 200 rows happened
       to survive, and running out of songs would look identical to there
       being none. */
    const view = String(req.query.view ?? '');
    if (view === 'concerts' || view === 'songs') {
      where.push(`${view === 'songs' ? 'NOT ' : ''}EXISTS (
        SELECT 1 FROM music_tag mt2 JOIN tag t2 ON t2.id = mt2.tag_id
         WHERE mt2.music_id = m.id AND t2.slug = 'concert'
           AND t2.retracted_at IS NULL)`);
    }

    const q = String(req.query.q ?? '').trim().toLowerCase();
    if (q) {
      const like = `%${q}%`;
      where.push(`(lower(m.title) LIKE ? OR lower(m.channel) LIKE ?
                   OR EXISTS (SELECT 1 FROM music_tag mt JOIN tag t ON t.id = mt.tag_id
                               WHERE mt.music_id = m.id AND t.retracted_at IS NULL
                                 AND (lower(t.name) LIKE ? OR t.slug LIKE ?)))`);
      params.push(like, like, like, like);
    }
    /* Shelved by when the SONG came out, not by when somebody got round to
       pasting it. This is a collection stitched together from a dozen channels
       and the question it answers is "what is there", so the natural order is
       the catalogue's, not the queue's.

       COALESCE, and it does two jobs. A row whose probe has not landed has no
       upload date at all, and a bare `uploaded_at DESC` would sort those FIRST
       in SQLite — NULL is the smallest value, so descending puts it on top,
       and the shelf would open with a row of songs nothing is known about.
       Falling back to `created_at` puts a just-submitted entry where the person
       who submitted it will look for it, and moves it to its real place the
       moment the probe answers.

       Not indexed, deliberately: an expression index for a few hundred rows
       would cost more to maintain than the sort it saves. */
    /* The submitter's handle rides along. Only the review queue reads it — "who
       put this forward" is part of judging a submission — and it is a LEFT
       JOIN on a column already indexed, so the shelf pays nothing for it. */
    /* Three bands, then the catalogue order inside each.
     *
     * The shelf sorts by RELEASE date, which is right for a catalogue and
     * wrong for a submission: a 2019 song pasted this morning would land in
     * the middle of two hundred cards, and the panel no longer has a Queued
     * pile to catch it. So anything still waiting is pinned to the top, where
     * whoever can decide will trip over it, and anything removed sinks to the
     * bottom, where it is out of the way but still reachable — which is the
     * whole reason it is in this list at all.
     *
     * Both flags are no-ops for a reader without `music.decide`: the visibility
     * clause has already left them with published songs and their own, so the
     * first band holds their submission and the third is empty.
     *
     * No cursor to keep in step, unlike the snippet list — this endpoint pages
     * by `limit` alone, which is what makes reordering it free. */
    const rows = R.prepare(
      `SELECT m.*, p.handle AS author_handle
         FROM music m LEFT JOIN person p ON p.id = m.author_id
        WHERE ${where.join(' AND ')}
        ORDER BY (m.status = 'proposed' AND m.retracted_at IS NULL) DESC,
                 (m.status = 'rejected' OR m.retracted_at IS NOT NULL) ASC,
                 COALESCE(m.uploaded_at, m.created_at) DESC, m.id DESC LIMIT ?`)
      .all(...params, limit);
    const tags = musicTagsFor(rows.map((r) => r.id));
    res.json({ music: rows.map((r) => musicRow(r, tags.get(r.id) ?? [])),
               count: rows.length });
  });

  /** One song, by id.
   *
   *  Here so that a card can repaint itself after a tag write instead of the
   *  panel reloading the whole grid and throwing away your scroll position —
   *  the same job `/api/snippets/:id` does for a clip row, answered the same
   *  way, including the 404-not-403 for something you may not see. */
  app.get('/api/music/:id', (req, res) => {
    const vis = musicVisibleSql(req);
    const gate = gateSql(req, { junction: 'music_tag', fk: 'music_id', alias: 'm' });
    const r = R.prepare(
      `SELECT m.*, p.handle AS author_handle
         FROM music m LEFT JOIN person p ON p.id = m.author_id
        WHERE m.id = ? AND ${vis.sql}${gate.sql ? ` AND ${gate.sql}` : ''}`)
      .get(req.params.id, ...vis.params, ...gate.params);
    if (!r) return res.status(404).json({ error: 'no such entry' });
    res.json({ music: musicRow(r, musicTagsFor([r.id]).get(r.id) ?? []) });
  });

  /** Put a link forward.
   *
   *  Everything expensive here was already solved for snippet links and is
   *  reused rather than rewritten: linkUrl() canonicalises and enforces the
   *  host allowlist, and the pending quota is the same number so there is one
   *  figure to remember rather than two.
   */
  app.post('/api/music', requireCap('music.submit'), (req, res) => {
    const me = req.person?.id ?? null;
    if (!me) return res.status(401).json({ error: 'sign in first' });

    const url = linkUrl(req.body?.url);
    const videoId = url ? videoIdOf(url) : null;
    if (!url || !videoId) {
      return res.status(400).json({ error: 'a YouTube video link, please' });
    }

    /* The natural key, so the same song pasted from the share button and from
       the address bar is one row. Named openly: unlike a snippet, nothing here
       is gated at submission time, so there is nothing to disclose. */
    const twin = R.prepare('SELECT id, title, status FROM music WHERE video_id = ?')
      .get(videoId);
    if (twin) {
      return res.status(409).json({ error: 'that video is already in the archive',
                                    music: { id: twin.id, title: twin.title, status: twin.status } });
    }

    const pending = R.prepare(
      `SELECT COUNT(*) c FROM music
        WHERE author_id = ? AND status = 'proposed' AND retracted_at IS NULL`).get(me).c;
    if (pending >= UP_MAX_PENDING) {
      return res.status(429).json({
        error: `you already have ${pending} submissions waiting for review`,
        pending, limit: UP_MAX_PENDING });
    }

    /* Tags come with the submission because that is when the person knows
       them — they are pasting a song because they know who is singing it.
       Written straight in rather than as a changeset: the ENTRY is what is
       under review, and a tag on an unapproved row has decided nothing yet.
       Unknown ids are dropped rather than refused; a mistyped tag should not
       cost somebody their submission. */
    const wanted = [...new Set((req.body?.tags ?? []).map(String).filter(Boolean))].slice(0, 40);
    const known = wanted.length
      ? R.prepare(`SELECT id FROM tag WHERE retracted_at IS NULL
                    AND id IN (${wanted.map(() => '?').join(',')})`).all(...wanted).map((r) => r.id)
      : [];

    const t = now(), id = ulid();
    tx(W, () => {
      W.prepare(
        `INSERT INTO music(id, video_id, url, note, probe_status, fetch_status,
                           status, origin, author_id, created_at, updated_at)
         VALUES(?,?,?,?,'queued','none','proposed','link',?,?,?)`)
        .run(id, videoId, url, String(req.body?.note ?? '').trim().slice(0, 500) || null,
             me, t, t);
      const ins = W.prepare(
        `INSERT INTO music_tag(id, music_id, tag_id, created_at, updated_at) VALUES(?,?,?,?,?)`);
      for (const tagId of known) ins.run(ulid(), id, tagId, t, t);
    });

    /* An id and a verb. Where the file goes and what format it is are the
       recorder's own config — the archive has never told a worker either.

       This one is approved on creation even though a suggester asked for it,
       which is a departure from the rule over enqueueJob ("the creator is
       already an editor and the approval IS the editor's yes"). It is the
       right departure: a probe is one metadata read of a public page, and
       holding it for review would show the reviewer a card with no title —
       so the thing they need in order to decide would be waiting on their
       decision. The EXPENSIVE half keeps the rule: `music_fetch` spends disk
       and the recorder's time, and it is queued in the review route below,
       after a human has said yes. */
    const jobId = enqueueJob('music_probe', { url, by: me,
                                              payload: { music_id: id, video_id: videoId } });
    logEvent(req, 'linked', 'music', id, { url, video_id: videoId, tags: known.length });
    bumpGeneration(W);
    res.status(201).json({
      music: musicRow(R.prepare('SELECT * FROM music WHERE id = ?').get(id),
                      musicTagsFor([id]).get(id) ?? []),
      job_id: jobId, next: 'probe',
    });
  });

  /** The verdict.
   *
   *  Through propose() rather than a direct UPDATE, and that is deliberate:
   *  the archive's claim is that no value exists without a row saying who put
   *  it there. `trusted` because this route has already asked the capability
   *  question at the door and is writing its own change rather than relaying
   *  the caller's.
   */
  app.post('/api/music/:id/review', requireCap('music.decide'), (req, res) => {
    const STATUS = { approve: 'confirmed', reject: 'rejected', reset: 'proposed' };
    const want = STATUS[String(req.body?.decision ?? '')];
    if (!want) {
      return res.status(400).json({ error: `decision must be one of ${Object.keys(STATUS)}` });
    }
    const m = R.prepare('SELECT * FROM music WHERE id = ? AND retracted_at IS NULL')
      .get(req.params.id);
    if (!m) return res.status(404).json({ error: 'no such entry' });
    if (m.status === want) return res.json({ music: musicRow(m), changed: 0 });

    try {
      propose(W, {
        authorId: req.person.id, trusted: true, autoApply: true,
        reason: req.body?.note ?? `${req.body.decision} ${m.title ?? m.video_id}`,
        changes: [{ target_type: 'music', target_id: m.id, op: 'update',
                    field: 'status', value: want, base_value: m.status }],
      });
    } catch (e) { return changeError(res, e); }

    const t = now();
    let jobId = null;
    if (want === 'confirmed' && m.fetch_status === 'none') {
      /* The download starts HERE and nowhere else. Fetching at submission time
         would mean the archive holds the bytes of things it has turned down —
         and deciding afterwards whether to keep them is a question nobody
         should have to answer twice. */
      W.prepare(`UPDATE music SET fetch_status = 'queued', updated_at = ? WHERE id = ?`)
        .run(t, m.id);
      jobId = enqueueJob('music_fetch', { url: m.url, by: req.person.id,
                                          payload: { music_id: m.id, video_id: m.video_id } });
    }
    if (want !== 'confirmed') {
      /* Nothing is left running for something nobody wants. The same move
         posterAftercare makes when a poster is cleared before it moves.

         `!== 'confirmed'` and not `=== 'rejected'`, because Undo in the review
         panel sends `reset`: approving queued a download, and taking the
         approval back a second later has to take the download with it or the
         archive keeps fetching something no longer approved. Un-rejecting is
         the same clause reached from the other side and cancels nothing,
         because a rejected row has no job left to cancel. */
      W.prepare(
        `UPDATE job SET status = 'cancelled', error = 'the entry was turned down',
                        finished_at = ?, updated_at = ?
          WHERE kind IN ('music_probe','music_fetch') AND status IN ('proposed','approved','claimed')
            AND payload LIKE ?`).run(t, t, `%"${m.id}"%`);
      W.prepare(`UPDATE music SET fetch_status = 'none', updated_at = ? WHERE id = ?`)
        .run(t, m.id);
    }
    logEvent(req, want === 'confirmed' ? 'approved'
      : want === 'rejected' ? 'rejected' : 'returned to the queue', 'music', m.id,
      { title: m.title ?? null });
    bumpGeneration(W);
    res.json({ music: musicRow(R.prepare('SELECT * FROM music WHERE id = ?').get(m.id),
                               musicTagsFor([m.id]).get(m.id) ?? []),
               job_id: jobId, changed: 1 });
  });

  /** Ask the recorder for this one again.
   *
   *  The gap this closes: a song whose download failed had no way back. Approve
   *  is the only thing that enqueues a fetch and it short-circuits on
   *  `fetch_status !== 'none'`, so the only route was a laundering trip —
   *  unlist, back to the queue, approve — which takes the song off the public
   *  shelf and back on again to re-run a download. A failed PROBE had no route
   *  at all short of hand-posting a job.
   *
   *  Both concerts that would not pull were the same story: the recorder's
   *  post-hoc downloads went out without cookies, so the probe could read the
   *  page and the fetch could not have the file. Fixing the recorder fixed the
   *  next song; it did nothing for the two already sitting there failed.
   *
   *  One verb for both columns, and the row decides which. A card shows one
   *  "try again" because the person pressing it means "get this song" — which
   *  of the two halves is stuck is the archive's bookkeeping, not theirs.
   */
  app.post('/api/music/:id/retry', requireCap('music.decide'), (req, res) => {
    const m = R.prepare('SELECT * FROM music WHERE id = ? AND retracted_at IS NULL')
      .get(req.params.id);
    if (!m) return res.status(404).json({ error: 'no such song' });

    /* Asking twice queues two of the same job, and the second answer overwrites
       the first with itself. Same guard, same wording, as the picture re-read
       and the harvest. */
    const open = R.prepare(
      `SELECT id, kind FROM job
        WHERE kind IN ('music_probe','music_fetch') AND status IN ('approved','claimed')
          AND payload LIKE ?`).get(`%"${m.id}"%`);
    if (open) return res.json({ job_id: open.id, kind: open.kind, already: true });

    const t = now();
    /* The probe first when both are stuck, because the fetch has nothing to go
       on without it: a row with no duration walks past the length cap, and a
       row with no title is a card nobody can judge. */
    const kind = m.probe_status === 'failed' ? 'music_probe'
      : m.fetch_status === 'failed' ? 'music_fetch' : null;
    if (!kind) {
      return res.status(409).json({
        error: 'nothing about that song failed — there is nothing to try again',
        probe_status: m.probe_status, fetch_status: m.fetch_status });
    }
    /* A download is what approval means, so re-running one needs the approval
       to still stand. Without this, "try again" on a song waiting for a verdict
       would fetch it — which is the one thing the review step exists to stop. */
    if (kind === 'music_fetch' && m.status !== 'confirmed') {
      return res.status(409).json({
        error: `that song is ${m.status}, so nothing should be downloading it` });
    }

    const col = kind === 'music_probe' ? 'probe' : 'fetch';
    W.prepare(`UPDATE music SET ${col}_status = 'queued', ${col}_note = NULL,
                                updated_at = ? WHERE id = ?`).run(t, m.id);
    const jobId = enqueueJob(kind, { url: m.url, by: req.person.id,
                                     payload: { music_id: m.id, video_id: m.video_id } });
    logEvent(req, kind === 'music_probe' ? 'asked the recorder to read it again'
                                         : 'asked the recorder to save it again',
             'music', m.id, { title: m.title ?? null, job_id: jobId, was: m[`${col}_note`] ?? null });
    bumpGeneration(W);
    res.json({ job_id: jobId, kind,
               music: musicRow(R.prepare('SELECT * FROM music WHERE id = ?').get(m.id),
                               musicTagsFor([m.id]).get(m.id) ?? []) });
  });

  /** Destroy the row. Two calls, and the reversible step has to have happened
   *  first — the same shape as the tag and snippet purges, for the same
   *  reason: this is the one action nobody can undo. */
  app.post('/api/music/:id/purge', requireCap('music.purge'), (req, res) => {
    const m = R.prepare('SELECT * FROM music WHERE id = ?').get(req.params.id);
    if (!m) return res.status(404).json({ error: 'no such entry' });
    if (m.status === 'confirmed' && !m.retracted_at) {
      return res.status(409).json({
        error: 'turn it down or retract it first — a published entry cannot be purged in one step' });
    }
    const counts = {
      tags: R.prepare('SELECT COUNT(*) c FROM music_tag WHERE music_id = ?').get(m.id).c,
    };
    if (!req.body?.confirm) {
      return res.json({ preview: true, retracted: !!m.retracted_at,
                        music: { id: m.id, title: m.title, video_id: m.video_id },
                        counts, preserved: !!m.video_path, confirm: counts });
    }
    if (req.body.confirm.tags !== counts.tags) {
      return res.status(409).json({ error: 'this entry changed since you looked — nothing was destroyed',
                                    counts, you_saw: req.body.confirm });
    }
    /* The FILES are not ours to delete: they live under the media root, which
       is mounted read-only for exactly this reason. The recorder is asked.

       Both of them. Asking only for the video leaves the cover behind as an
       orphan nothing points at — the same trap the snippet purge names when it
       clears the cache derivatives, and a directory that fills with the
       artwork of songs that no longer exist is worse than either. */
    const jobs = [];
    for (const path of [m.video_path, m.thumb_path]) {
      if (path) jobs.push(enqueueJob('purge', { by: req.person.id, payload: { path } }));
    }
    const job = jobs[0] ?? null;
    tx(W, () => {
      W.prepare('DELETE FROM music_tag WHERE music_id = ?').run(m.id);
      W.prepare('DELETE FROM music WHERE id = ?').run(m.id);
    });
    // The row is gone; this line is the only place its name still exists.
    logEvent(req, 'destroyed', 'music', m.id,
             { title: m.title, video_id: m.video_id, ...counts, master: !!job });
    bumpGeneration(W);
    res.json({ ok: true, destroyed: { id: m.id, title: m.title }, counts,
               job_id: job, job_ids: jobs });
  });

  /* `songs` was missing here, and a purge preview that under-reports is worse
     than no preview: the whole point of the two-phase confirm is that nobody
     destroys more than they were shown, and a tag on twelve songs read as
     "nothing points at it". The attachments went anyway — music_tag.tag_id is
     ON DELETE CASCADE — so the number was the only thing missing, which is
     exactly the kind of quiet that this confirm exists to prevent. */
  const tagPurgeCounts = (id) => ({
    streams: R.prepare('SELECT COUNT(*) c FROM stream_tag WHERE tag_id = ?').get(id).c,
    snippets: R.prepare('SELECT COUNT(*) c FROM snippet_taglet WHERE tag_id = ?').get(id).c,
    songs: R.prepare('SELECT COUNT(*) c FROM music_tag WHERE tag_id = ?').get(id).c,
    blocks: R.prepare('SELECT COUNT(*) c FROM segment WHERE tag_id = ?').get(id).c,
    children: R.prepare('SELECT COUNT(*) c FROM tag WHERE parent_id = ?').get(id).c,
  });
  // What the caller has to hand back, and therefore what it had to have been
  // shown. Named once so the preview, the check and the client cannot drift.
  const PURGE_CONFIRM = ['streams', 'snippets', 'songs', 'blocks'];

  app.post('/api/tags/:id/purge', requireCap('tag.purge'), (req, res) => {
    const t = R.prepare('SELECT id, name, slug, retracted_at FROM tag WHERE id = ?')
      .get(req.params.id);
    if (!t) return res.status(404).json({ error: 'no such tag' });

    const counts = tagPurgeCounts(t.id);
    const total = PURGE_CONFIRM.reduce((n, k) => n + counts[k], 0);

    /* Phase one. No `confirm` in the body means the caller is asking, not
       telling — so answer and write nothing. */
    if (!req.body?.confirm) {
      return res.json({
        preview: true, tag: { id: t.id, name: t.name, slug: t.slug },
        retracted: !!t.retracted_at, counts,
        /* What the caller has to send back. Counts rather than a token: a
           token only proves the round trip happened, while these change if
           somebody tags something in between — which is the case actually
           worth catching. */
        confirm: Object.fromEntries(PURGE_CONFIRM.map((k) => [k, counts[k]])),
      });
    }

    const said = req.body.confirm;
    /* A key that is not there at all is a different thing from a key that
       disagrees, and it has a different answer. `songs` was added to this list
       after the panel shipped, so a page somebody left open yesterday sends
       three counts and would otherwise be told the tag "changed since you
       looked" — which is not true and not actionable. Nothing is destroyed
       either way; only the sentence differs. */
    const missing = PURGE_CONFIRM.filter((k) => typeof said?.[k] !== 'number');
    if (missing.length) {
      return res.status(409).json({
        error: 'this page is out of date — reload it and try again',
        missing, counts,
      });
    }
    if (PURGE_CONFIRM.some((k) => said[k] !== counts[k])) {
      return res.status(409).json({
        error: 'this tag changed since you looked — nothing was destroyed',
        counts, you_saw: said,
      });
    }

    tx(W, () => {
      W.prepare('DELETE FROM stream_tag WHERE tag_id = ?').run(t.id);
      W.prepare('DELETE FROM snippet_taglet WHERE tag_id = ?').run(t.id);
      // Explicit, like the two above and for the same reason, though the
      // cascade would do it: the count in the log should be a count of what
      // this did rather than of what SQLite did behind it.
      W.prepare('DELETE FROM music_tag WHERE tag_id = ?').run(t.id);
      /* Both are ON DELETE SET NULL, and both are done explicitly so the count
         in the log is a count of what this did rather than of what SQLite did
         afterwards. A child tag is orphaned, not destroyed — it is a tag in its
         own right that happened to hang under this one. */
      W.prepare('UPDATE segment SET tag_id = NULL, updated_at = ? WHERE tag_id = ?')
        .run(now(), t.id);
      W.prepare('UPDATE tag SET parent_id = NULL, updated_at = ? WHERE parent_id = ?')
        .run(now(), t.id);
      W.prepare('DELETE FROM tag WHERE id = ?').run(t.id);
    });

    /* The row is gone, so this line is the only place its name still exists.
       That is the whole reason `event` denormalises the actor and the detail. */
    logEvent(req, 'destroyed', 'tag', t.id,
             { name: t.name, slug: t.slug, ...counts, was_retracted: !!t.retracted_at });
    bumpGeneration(W);
    res.json({ ok: true, destroyed: { id: t.id, name: t.name }, counts, total });
  });

  /** Ask what the catalogue has under a name.
   *
   *  This used to take a URL you had gone and found, and hand it to a worker
   *  that read the lead paragraph off a wiki. Two things were wrong with that
   *  and only one of them was the source: it also never fetched the art, not
   *  once — `do_harvest` returned `result_path = None` every time, while the
   *  button said "replaces the description and the art" and the branch in
   *  harvestLanded sat waiting for a file that was never coming.
   *
   *  So: no URL. The tag HAS a name and the name is the query. One deliberate
   *  press, one job, and what comes back is CANDIDATES rather than an answer —
   *  a search returns 0, 1 or many, and three different games are called
   *  Summer Camp. Picking is /api/tags/:id/seed below, and it needs no worker.
   */
  app.post('/api/tags/:id/harvest', requireRole('editor'), (req, res) => {
    const t = R.prepare('SELECT id, name FROM tag WHERE id = ? AND retracted_at IS NULL')
      .get(req.params.id);
    if (!t) return res.status(404).json({ error: 'no such tag' });
    /* The tag's own name unless a person retyped it. That is the whole of the
       retype path: a franchise word like `Pokemon` is a bad query and no
       amount of ranking fixes it, so the answer is to let it be asked again
       differently rather than to guess harder. */
    const q = String(req.body?.q ?? t.name ?? '').trim().slice(0, 120);
    if (!q) return res.status(400).json({ error: 'nothing to look up' });

    /* Asking twice runs the same search twice and the second answer replaces
       the first with itself. `art_url` is excluded because those are a
       different errand on the same kind — see the note in seedArt. */
    const open = R.prepare(
      `SELECT id FROM job WHERE kind = 'harvest' AND status IN ('approved','claimed')
         AND payload LIKE ? AND payload NOT LIKE '%"art_url"%'`).get(`%"${t.id}"%`);
    if (open) return res.json({ job_id: open.id, already: true });

    const id = enqueueJob('harvest', {
      payload: { tag_id: t.id, name: t.name, q }, by: req.person.id });
    bumpGeneration(W);
    logEvent(req, 'looked a tag up in the catalogue', 'tag', t.id, { q }, null);
    res.json({ job_id: id, q });
  });

  /** What a person picked out of the candidates.
   *
   *  Writes DIRECTLY, like harvest always has, and for the same reason: this
   *  is an observation of somebody else's catalogue, not a decision the
   *  archive is taking. Nobody has to answer for what IGDB says a game is
   *  about. The editorial half — whether this tag should exist, what it is
   *  called, what kind it is — all still goes through changesets.
   *
   *  And it needs NO WORKER, which is the whole reason the latency of this
   *  feature is one wait and not two. The candidate already carries the words
   *  and the link; only the cover is bytes, and bytes have to go through the
   *  recorder because the recorder is the only thing that writes the media
   *  tree. Nobody watches a cover arrive.
   */
  app.post('/api/tags/:id/seed', requireRole('editor'), (req, res) => {
    const t = R.prepare('SELECT id, name FROM tag WHERE id = ? AND retracted_at IS NULL')
      .get(req.params.id);
    if (!t) return res.status(404).json({ error: 'no such tag' });

    const b = req.body ?? {};
    const url = String(b.url ?? '').trim();
    const summary = String(b.summary ?? '').trim().slice(0, 4000);
    const art = String(b.art ?? '').trim();
    const picked = String(b.name ?? '').trim().slice(0, 200);
    if (!url && !summary) {
      return res.status(400).json({ error: 'a pick needs at least a link or a description' });
    }
    for (const [what, v] of [['link', url], ['art', art]]) {
      if (v && !/^https:\/\//i.test(v)) {
        return res.status(400).json({ error: `${what} must be https` });
      }
    }

    const at = now();
    /* `seeded = 1` says out loud that a machine wrote this and no human has
       been over it; the first hand edit clears it in the applier. */
    W.prepare(`UPDATE tag SET summary = COALESCE(?, summary), seed_url = COALESCE(?, seed_url),
                              seeded = 1, updated_at = ? WHERE id = ?`)
      .run(summary || null, url || null, at, t.id);

    const job = art ? seedArt(t.id, art, req.person?.id ?? null) : null;
    bumpGeneration(W);
    logEvent(req, 'seeded a tag from the catalogue', 'tag', t.id,
             { picked: picked || null, url: url || null, art: !!art }, null);
    res.json({ ok: true, art_job: job });
  });

  /** The one thing in a pick that has to go through the recorder.
   *
   *  A `harvest` carrying `art_url` rather than a kind of its own, and that is
   *  deliberate: a new kind would have to be added to JOB_KINDS here, to
   *  PI_KINDS here, to PI_KINDS in ls_archive.py, AND to `archive_job_kinds`
   *  in the Pi's config — which is the trap that had a clip sitting WAITING
   *  for ever while the worker took everything else. A kind that already
   *  travels cannot fall into it.
   *
   *  The destination is named HERE, as a ULID, because names in the media tree
   *  are the archive's to mint — the same rule the quarantine names follow.
   *  `.jpg` and not `.png`: IGDB serves JPEG, `.jpg` is in the media MIME
   *  allowlist, and converting it on a Pi to satisfy a regex would be a
   *  transcode for nothing.
   */
  function seedArt(tagId, artUrl, by) {
    const to = `posters/${ulid()}.jpg`;
    return enqueueJob('harvest', {
      payload: { tag_id: tagId, art_url: artUrl, art_to: to }, by });
  }

  const POSTER_MAX_BYTES = Number(process.env.TENMA_POSTER_MAX_BYTES) || 8 * 1024 * 1024;
  const PNG_MAGIC = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);

  /* One handler, two subjects. A stream's poster and a tag's art are the same
     operation down to the byte: PNG in, quarantine, promote job, and the
     caller writes `thumb_path` as the destination the file WILL have. Writing
     it twice is how one of them keeps the magic-number check and the other
     grows a content-type check instead. */
  const posterUpload = (table) => async (req, res) => {
    if (!config.quarantineRoot) {
      return res.status(503).json({ error: 'uploads are disabled; set TENMA_QUARANTINE_ROOT' });
    }
    const s = R.prepare(`SELECT id FROM ${table} WHERE id = ? AND retracted_at IS NULL`)
      .get(req.params.id);
    if (!s) return res.status(404).json({ error: `no such ${table}` });

    const id = ulid();
    // A dotfile while it is arriving, for the same reason /api/uploads uses
    // one: the importer skips dotfiles, so a half-written poster can never be
    // read as a whole one.
    const partAbs = join(config.quarantineRoot, `.part-${id}`);
    const scrub = () => { try { rmSync(partAbs, { force: true }); } catch { /* gone */ } };

    let bytes = 0, tooBig = false, head = null;
    const meter = new Transform({
      transform(chunk, _enc, cb) {
        // The first bytes decide whether this is a PNG at all, and they are
        // already in hand — cheaper than reading the file back afterwards, and
        // it means a wrong file is refused without a second syscall.
        if (head === null) head = Buffer.from(chunk.subarray(0, 8));
        bytes += chunk.length;
        if (bytes > POSTER_MAX_BYTES) { tooBig = true; return cb(new Error('too big')); }
        cb(null, chunk);
      },
    });

    try {
      await pipeline(req, meter, createWriteStream(partAbs));
    } catch {
      scrub();
      return tooBig
        ? res.status(413).json({
            error: `posters are capped at ${Math.round(POSTER_MAX_BYTES / 1048576)} MB`,
            limit_bytes: POSTER_MAX_BYTES })
        : res.status(400).json({ error: 'the upload did not finish' });
    }
    if (!bytes) { scrub(); return res.status(400).json({ error: 'that was an empty file' }); }
    /* The signature, not the extension and not the content-type — both of
       those are things the sender says, and this route names the file itself.
       Same reasoning as the media allowlist on the way out: a browser that
       sniffs is how an upload becomes stored XSS on your own origin. */
    if (!head || head.length < 8 || !head.equals(PNG_MAGIC)) {
      scrub();
      return res.status(415).json({ error: 'that is not a PNG' });
    }

    const rel = `${id}.png`;
    try { renameSync(partAbs, join(config.quarantineRoot, rel)); }
    catch (e) { scrub(); return res.status(500).json({ error: `could not store it: ${e.code}` }); }

    // No snippet_id: this promote is about a stream's poster, and the claim
    // path only fills in a MISSING payload for snippet jobs — an explicit one
    // travels through untouched, and jobLanded has nothing to do afterwards
    // because thumb_path is written as the destination it will have.
    const to = `posters/${rel}`;
    const job = enqueueJob('promote', { payload: { from: rel, to }, by: req.person.id });
    bumpGeneration(W);
    res.json({ path: to, job, pending: true });
  };

  app.post('/api/streams/:id/poster', requireRole('editor'), posterUpload('stream'));
  app.post('/api/tags/:id/poster', requireRole('editor'), posterUpload('tag'));

  // -------------------------------------------------------------------------
  // frontend
  //
  // Served from the same origin as the API, which is the whole reason there is
  // no CORS or cookie-domain question to answer. Registered last so a file can
  // never shadow a route.
  // -------------------------------------------------------------------------

  // -------------------------------------------------------------------------
  // /m/:id — a moment, addressably
  //
  // Two rules hold this together, and both are about disclosure rather than
  // about rendering.
  //
  //   1. EVERY /m/<anything> answers 200 with the same page. "Never existed",
  //      "not published yet" and "gated away from you" are one response,
  //      because anything else is an oracle: a crawler that gets 404 for one
  //      id and 200 for another has learned which gated clips exist without
  //      ever being allowed to see one. The client then asks
  //      /api/snippets/:id, which is gate-aware and already answers this
  //      correctly for whoever is actually holding the cookie.
  //
  //   2. The meta tags are decided ANONYMOUSLY, whoever is asking. A crawler
  //      arrives with no cookie, so ANON is the honest question to ask on its
  //      behalf — but the rule is not "because it is a crawler",
  //      it is that <head> is the least private part of a page. It survives
  //      into browser history, tab sync, screenshots and whatever the OS
  //      shares a URL with. An editor opening a gated moment gets the clip,
  //      because the API gives it to them; they do not get its title welded
  //      into the document head where it can escape.
  // -------------------------------------------------------------------------

  const INDEX_HTML = join(HERE, 'public', 'index.html');

  /* Read once and re-read only when it changes. The file is a few hundred KB
     and this is the route people paste into chat, where several unfurlers hit
     it at once within a second or two of each other. */
  let indexCache = { mtime: -1, html: null };
  const indexHtml = () => {
    let mtime = -1;
    try { mtime = statSync(INDEX_HTML).mtimeMs; } catch { return null; }
    if (mtime !== indexCache.mtime) {
      try { indexCache = { mtime, html: readFileSync(INDEX_HTML, 'utf8') }; }
      catch { return null; }
    }
    return indexCache.html;
  };

  /* Attribute-safe. Every string that reaches this went through a title field
     somebody typed into, and an unescaped quote ends the attribute and turns
     the rest of the title into markup — in the one part of the page that is
     handed to third-party servers to render. */
  const attrEsc = (s) => String(s ?? '')
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');

  /* Absolute, because og:image is fetched by a machine with no page to
     resolve a relative path against. TENMA_PUBLIC_URL wins when it is set:
     behind a reverse proxy Host is whatever the proxy chose to pass on, and
     on a NAS that is as often `192.168.1.4:8080` as it is the name people
     actually share. Unset, the request's own view is the best guess there
     is — which is fine on a LAN and is why the variable exists for when it
     is not. */
  const publicBase = (req) => {
    const set = String(process.env.TENMA_PUBLIC_URL ?? '').trim().replace(/\/+$/, '');
    if (set) return set;
    const proto = String(req.headers['x-forwarded-proto'] || req.protocol || 'http')
      .split(',')[0].trim();
    return `${proto}://${req.headers.host ?? 'localhost'}`;
  };

  /* 0:42, 3:07, 1:02:30 — an unfurl is read at a glance and 00:00:42 is not a
     glance. hms() stays as it is; it answers a different question, about
     positions on a stream axis where the hours column is load-bearing. */
  const shortDur = (x) => {
    if (!(x > 0)) return null;
    const t = Math.round(x), h = Math.floor(t / 3600), m = Math.floor((t % 3600) / 60);
    const p = (n) => String(n).padStart(2, '0');
    return h ? `${h}:${p(m)}:${p(t % 60)}` : `${m}:${p(t % 60)}`;
  };

  const SITE = 'Flatfox';
  const SITE_BLURB = 'An archive of Tenma Maemi.';

  /** The row an anonymous reader may be told about, or null.
   *
   *  The anonymous question, deliberately — see rule 2 above. ANON is the
   *  archive's own word for nobody, and it is what every request carries
   *  before a cookie is read; asking with a bare null instead would be a
   *  different, subtly weaker question, and `atLeast()` dereferences it.
   */
  const publicSnippet = (id) => {
    const r = R.prepare(
      `SELECT * FROM snippet WHERE id = ? AND retracted_at IS NULL`).get(id);
    return snipVisible(r, { person: ANON }) ? r : null;
  };

  /** The <head> additions for one moment, or the site's own when there is
   *  nothing an anonymous reader may be told about it.
   */
  function momentMeta(id, req) {
    const base = publicBase(req);
    const generic = [
      ['og:site_name', SITE], ['og:type', 'website'], ['og:title', SITE],
      ['og:description', SITE_BLURB], ['twitter:card', 'summary'],
    ];

    const r = publicSnippet(id);
    if (!r) return generic;

    const dur = shortDur(r.duration_s);
    const tags = TAGLETS_OF.all(r.id)
      .filter((t) => !t.gate)          // a gated taglet names its own gate
      .slice(0, 6).map((t) => t.name);
    const desc = [dur, tags.join(', ')].filter(Boolean).join(' · ') || SITE_BLURB;

    const still = isStill(r);
    const out = [
      ['og:site_name', SITE],
      // A picture is not a video, and telling an unfurler otherwise is how you
      // get a play button drawn over a JPEG that will never play.
      ['og:type', still ? 'website' : 'video.other'],
      ['og:title', r.title || 'A moment'],
      ['og:description', desc],
      ['og:url', `${base}/m/${r.id}`],
    ];
    /* Only when there IS one. An og:image pointing at a 404 is worse than no
       image: several unfurlers drop the whole card rather than fall back to
       the text one. */
    if (r.poster_path) {
      out.push(['og:image', `${base}/media/snippet-poster/${r.id}`]);
      if (r.width) out.push(['og:image:width', String(r.width)]);
      if (r.height) out.push(['og:image:height', String(r.height)]);
      out.push(['twitter:card', 'summary_large_image']);
    } else if (still && r.file_path) {
      /* A picture is its own thumbnail, and a meme that never got a generated
         poster is otherwise a card with no picture in it. The trailing name is
         what makes an unfurler that goes by the URL rather than the
         Content-Type treat it as an image — see /media/snippet/:id/:name. */
      out.push(['og:image',
                `${base}/media/snippet/${r.id}/${r.id}${extname(r.file_path).toLowerCase() || '.jpg'}`]);
      if (r.width) out.push(['og:image:width', String(r.width)]);
      if (r.height) out.push(['og:image:height', String(r.height)]);
      out.push(['twitter:card', 'summary_large_image']);
    } else {
      out.push(['twitter:card', 'summary']);
    }

    /* og:video is the whole point of the public carve-out: it is what turns a
       picture card into one that plays in the message, without the reader
       leaving Discord or holding an account here.
       The type is what it will be SERVED as, not what was uploaded, and
       `secure_url` is only claimed when the base really is https — an unfurler
       handed an https url that answers on http drops the card. */
    const vt = still ? null : playType(r);
    if (r.file_path && vt && vt.startsWith('video/')) {
      const url = `${base}/media/snippet/${r.id}`;
      out.push(['og:video', url]);
      if (base.startsWith('https://')) out.push(['og:video:secure_url', url]);
      out.push(['og:video:type', vt]);
      if (r.width) out.push(['og:video:width', String(r.width)]);
      if (r.height) out.push(['og:video:height', String(r.height)]);
    }
    if (dur) out.push(['og:video:duration', String(Math.round(r.duration_s))]);
    return out;
  }

  // -------------------------------------------------------------------------
  // The two pages a stranger can see
  //
  // Both are SELF-CONTAINED: no stylesheet, no webfont, no script from
  // anywhere, nothing fetched, nothing that needs an API to answer. That is
  // not thrift, it is the requirement — these render in precisely the
  // situation where the rest of the archive is unreachable, and a page that
  // needed the archive in order to look right would look broken instead of
  // looking locked.
  //
  // Which is also why the application itself is not what a stranger gets.
  // Serving index.html to somebody with no session would boot a megabyte of
  // app, have every one of its dozen opening requests refused, and render as
  // something that is not working rather than as something they may not have.
  // -------------------------------------------------------------------------

  /* Shared so there is one palette rather than three. Deliberately a subset of
     the application's: what these pages need is the background, the ink, the
     accent and a border. */
  /* The fox, inlined.
     Read once at boot and embedded rather than linked, for two reasons: these
     pages fetch nothing, and pointing at /icon.svg would mean widening the
     gate's carve-out by a route in order to serve a logo.
     A mask and not an <img>, the same choice the topbar makes and for the same
     reason — the artwork's fills are baked dark, CSS cannot reach inside an
     external image to recolour it, and a mask uses only the alpha so
     `background` decides the colour.
     Absent, the mark falls back to the diamond these pages shipped with, which
     is what happens in the test tree where the artwork does not live. */
  const FOX = (() => {
    try {
      const svg = readFileSync(join(HERE, 'public', 'icon.svg'));
      if (!svg.length || svg.length > 48 * 1024) return null;
      return svg;
    } catch { return null; }
  })();
  const FOX_URI = FOX && `data:image/svg+xml;base64,${FOX.toString('base64')}`;
  /* The same artwork repainted for a browser tab, where there is no mask to
     hide behind and a shape baked in near-black is invisible against a dark
     one. #231f20 is what the file actually contains — the topbar's comment
     says so — and a replace that finds nothing yields no favicon rather than
     a wrong one. */
  const FOX_ICON = (() => {
    if (!FOX) return null;
    const painted = FOX.toString('utf8').replace(/#231f20/gi, '#ef9fc6');
    if (!painted.includes('#ef9fc6')) return null;
    return `data:image/svg+xml;base64,${Buffer.from(painted, 'utf8').toString('base64')}`;
  })();

  /* Shared so there is one palette rather than three. Deliberately a subset of
     the application's: what these pages need is the ground, the ink, the
     accent and a border.

     Her colours, not the app's, and the two will converge from this end. The
     ground is lighter than the application's near-black, which changes one
     thing structurally: --surface and --panel are DARKER than --bg here, so a
     field or a box reads as recessed rather than raised. That is why the input
     looks like a well and the button like a tile.

     Three tones that are not in the brief and why:
       --text-2   the warm cream, two steps lighter. #d1c1a8 on this ground is
                  4.3:1 — fine for the wordmark, which is large, and short of
                  AA for footer-sized text. So the ramp lightens for small
                  print and --text-3 is kept for what is big or decorative.
       --link     the accent as TEXT is 3.8:1 and fails; the accent as a FILL
                  with dark ink on it is 7.7:1 and passes. Different jobs,
                  different values. The button keeps her pink exactly.
       --err      warm salmon rather than red, far enough from the accent to
                  not read as another link.

     .wm exists because the gap kept landing inside the word. `Flat<i>fox</i>`
     as bare text beside an element is TWO flex items — a text node in a flex
     container becomes its own anonymous item — so the 11px meant for
     mark-to-word split the word and it read as "Flat fox".

     The wordmark is Caveat (SIL Open Font License 1.1, (c) 2014 The Caveat
     Project Authors), subset to the seven letters of the word and inlined —
     1.8 KB, so there is no request and no third party. Renaming the site means
     re-subsetting it; until then a missing glyph falls through the stack to
     whatever cursive the machine has, rather than to a blank box. */
  const PAGE_CSS = `
/* Caveat, (c) 2014 The Caveat Project Authors, SIL Open Font License 1.1
   <https://scripts.sil.org/OFL>. Subset to the letters of the wordmark.
   The notice travels with the bytes: a subset embedded in a page is still a
   redistribution, and the one place it is certain to stay attached is beside
   the data itself. */
@font-face{font-family:Caveat;font-style:normal;font-weight:700;font-display:block;
  src:url(data:font/woff2;base64,d09GMgABAAAAAAcAAA8AAAAADAwAAAapAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAGhwbIBwqBmA/U1RBVCoAZBEICo44ilIBNgIkAyALEgAEIAWDJgcgGw0JUZRuUpHgi4NM7rpFMohEthU7cm2e8FRk5fy9mkF02tzeJr0STEEnZVI95OBhp0+RIFmuWTiNIr4KhUSVbnFdKHyrDgUKIXEOjbLlS/8D+Pe/XMup69QCSyTNwtbIGpPIJnYhB0+E+r/dmLNElWxgTJKU6s3NakB8AuE7cU1dFOjsEyc+8YlUfCwqOIoiQSc4oKYs2s12ExpHAQ7Up5kbKn29FNORYSQRDEADxBkETxBFdGYyoMaCFaaZmJ4PDv0vEAPIONiJC9Ka1qymux0mNBWjaWwg+8ADciHFANLMVpgp/HwA/IyNBIPcBCxCon5p5b8rwD6ZcJbLXEmtwwa7Z4wTMQNjsAUyF4q6GQAgo4Ipsg9uB8COINiR+VQ2QIeOfItENRq1EuvW7/+/8MX/91psPgWZGabGwElg5h+0MS4coQNQBbICyB1QT2+fZIzj74N1RyY565YXdM1mVrkM5f8dRhIA0sdjF4xLoCTqDXLocgk6jhIrO07AN+ggC4uoDacArJIiSm6mmMYizKxOarHyIS5qsZmhoNVIGUVAQuMAq0kQrlZX8cvCqOI+SB+7hNpzlseYmeqyKEpbyOd5yx2V7OzLU2DVWKW+aGLFrz7zAn6MMCKIOJWXXjKvANDqSpE0VWz1ig9zyFbtbw+F685qCtlBdlD6Jip3DjohlPva0r9N6cuW631prOboRB4FQXpLEEXJJ1PaWe/wsgnSEBgQYEda2slETq1+cDNpRqtFUE6Fy1dSAGIHXFACGwcAELEWDpI27rLkOMoOsg/iqhnluSRL+Ure9YWWuMMxR95MFITZljAHBBCYbtPMVIDixL/CsOi8E0iS2SATEqveo1KQcwdNtbIxzZGM31rFrGvnwuX62lyQnbZQmOIrC0Xi4cqSa5Gjvd5JkJMhL0EyQplyUC+UGenOojVGi0zcZyK1WMJvgR0u26DoZTu4PndlobDSWHtfUbQ4coKFm6uvaiaQqKhQ83WNcktSHo3n4nHbK/KctFf8tpJ5xSbdCHmLdWYHArPMyKmNcmQFwqTC4WXu0lRqPa/V0aj/K7lN4zYvdVv/Y41yOH43f6KEQSe4sU9fXtb9Sj81eDzyTJ0RbKSrGYkET6Suw22zcbuPUaRBxdIOyeZFPtO+qlrbPqghj+0P5cQ1srEzlDp+xH6ZkW7dF7JIILDL9zeCb19QS+wIp+ezPJkIs18gqYwPcvf7pns5PIl+evQ5e9O+KAzMBm+kdJh/wj9rgV3fn/60kA7VT7rzUprR+8cudrkihjl+enuq+kXJTXUTOp3dPjhQSQPOFSQm1bsZiKovjLTYnpEn+8tp+pw7cPQGbv3y5twX+rqEsZTfIvFg3IlJiWO/Rap61+msoBBTwY3sx/3bXHp2lVYY6itPzjNJEh+Ylb1Dqq2iDo9z0CBhjq2W3QUWy9Y1TedRQ4q6begwfwZj5ISaGH2+rHG4W5FsyK4X6J8wYWkK1UwyHDWhnl9owAVbUIZPVG10PDTiMFnN4d7e7rRCFdMdN5X6tEKFq/PnpVOaRFtFZgnGgnc8bj9nl0vRd97noYam0MS5S49ScXjwvUcKQlX8qRrUPt/MyixMXuLB13y5yr7NxlkL8WV9vpkxfV4iUe4etTo/3/t9k62bp25hv5HflRlfM/vmem4IdDfLGWN084ZfzAIXrhlrsFiW7koS4Jcp8ckKalg9oGYi5SxpjPl0G98+RqkDW1qHL0zcOmbsmDf9dSdLUod9Dbx2RKrj/NOpS4dTch0domD/j1tv6qU32A2LXTTj8rIB0lFmSCqFa0z7eLpEHWPUaa8/DeyaaWLZxOfxUJioqWGt/95VHDfFVQ+Td9ovFz8K38QNBPpWd/J9+JxO79A991B4/YjtGeCgrnEnzMLbL3fWr6CHdUU3thIqreurdwYAgAEQ0H+/vlKlHfGVY9kXAFwrifgEwG3R1fD/E36PvcEqEfgIAGDgE1HmKM6KmyDAr+NSZf974xJ6cQBp4YEUW5ATzSiKNNRHEYIwAC4KoUIOOg9GonGqOrGYRBXAEsCFMjQsQQnOBpTlYQdKGXmE8pj5gfAZhR4PA2iEi3S+fQTEJPrr1ERIpJsNPz58BTTafAK1ISZQo0+I1XDabKNsYs1uep0tVo9pExHL2i4R5jKfrZuksYXxVhxCTWSvHrW81BFrE7Vi0TuFWjVoJNauWxdvdas2uu6hdxyxVvX8ePGJrg0nUCJXgvARpGdeKLJtHIUado01rV9nM3M5s3mKNNyUgQWC/m/PDw==) format("woff2")}
:root{--bg:#59525a;--panel:#453f47;--surface:#2a212b;--text:#f3f1f7;
  --text-2:#ddcfb9;--text-3:#d1c1a8;--accent:#ef9fc6;--on-accent:#2a212b;
  --link:#f8c2d8;--err:#ffc4b8;--line:rgba(209,193,168,.22)}
*{box-sizing:border-box}
body{margin:0;min-height:100vh;background:var(--bg);color:var(--text);
  font:16px/1.55 "Plus Jakarta Sans",system-ui,-apple-system,Segoe UI,sans-serif;
  -webkit-font-smoothing:antialiased;display:flex;flex-direction:column;
  align-items:center;gap:20px;padding:clamp(22px,6vh,72px) 16px}
a{color:var(--link);text-decoration:none}
a:hover{text-decoration:underline}
/* The word is ONE flex item, so the gap cannot land inside it. */
.brand{display:flex;align-items:center;gap:11px;text-decoration:none;
  color:var(--text)}
.brand:hover{text-decoration:none}
.wm{font-family:Caveat,"Segoe Script","Bradley Hand",cursive;font-weight:700;
  font-size:2.15rem;line-height:1;letter-spacing:.005em}
.wm i{font-style:normal;color:var(--text-3)}
.mark{flex:none;background:var(--accent)}
${FOX_URI
  ? `.mark{width:25px;height:28px;
      -webkit-mask:url("${FOX_URI}") no-repeat center/contain;
              mask:url("${FOX_URI}") no-repeat center/contain}`
  : '.mark{width:13px;height:13px;border-radius:3px;transform:rotate(45deg)}'}
.foot{color:var(--text-2);font-size:.78rem;text-align:center;margin:0;
  line-height:1.7}
:focus-visible{outline:2px solid var(--accent);outline-offset:2px}`;

  /* The tab icon, for both pages. A link somebody bookmarks should not be a
     blank sheet of paper, and without it every visit also spends a request on
     a /favicon.ico the gate refuses. */
  const FAVICON = FOX_ICON
    ? `<link rel="icon" href="${FOX_ICON}">`
    : '';

  /* Her channel, in the footer of both public pages. The line already named
     her; a name in the footer of a fan archive should be the way to the person
     it is about. rel=noopener because target=_blank without it hands the new
     tab a window.opener handle back into this origin. */
  const FOOT = 'An archive of <a href="https://www.youtube.com/@TenmaMaemi"'
    + ' target="_blank" rel="noopener noreferrer">Tenma Maemi</a>.'
    + '<br>Not affiliated with Phase Connect.';

  /* The door. Served to anybody without a session who asks for the root, and
     for anything else page-shaped that the gate refuses. A stranger learns the
     site's name and that there is a password, which is all there is to learn.

     Nothing in the page it returns explains itself. The inline script's
     redirect goes to the root and never back to where they came from — the
     only other page they could arrive from is the 404, which after signing in
     is still a 404, and the archive is what they were trying to reach. That
     note used to be a comment inside the <script>, where anybody who opened
     devtools read my working notes; these two pages are the ones strangers
     inspect, so the reasoning lives here and the output stays quiet. */
  function gatePage(req, msg = null) {
    return `<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow">
<title>${attrEsc(SITE)}</title>
${FAVICON}
<style>${PAGE_CSS}
body{justify-content:center;padding-bottom:14vh}
main{width:100%;max-width:330px;display:flex;flex-direction:column;gap:18px;
  align-items:center}
form{display:flex;flex-direction:column;gap:10px;width:100%}
input{width:100%;padding:12px 14px;border-radius:10px;color:var(--text);
  background:var(--surface);border:1px solid var(--line);font:inherit}
input::placeholder{color:var(--text-3)}
input:focus{border-color:var(--accent);outline:none}
button{width:100%;padding:12px 16px;border:0;border-radius:10px;cursor:pointer;
  background:var(--accent);color:var(--on-accent);font:inherit;font-weight:700}
button:hover:not([disabled]){filter:brightness(1.06)}
button[disabled]{opacity:.55;cursor:default}
.note,.err{margin:0;font-size:.85rem;text-align:center}
.note{color:var(--text-2)}
.err{color:var(--err);font-weight:600}
</style></head><body>
<main>
  <div class="brand"><span class="mark"></span><span class="wm">Flat<i>fox</i></span></div>
  ${msg ? `<p class="note">${attrEsc(msg)}</p>` : ''}
  <form id="f">
    <input id="p" type="password" name="password" placeholder="Password"
      autocomplete="current-password" aria-label="Password" autofocus>
    <button id="b" type="submit">Sign in</button>
  </form>
  <p class="err" id="e" role="alert" hidden></p>
  <p class="foot">${FOOT}</p>
</main>
<script>
(function(){
  var f=document.getElementById('f'),p=document.getElementById('p'),
      e=document.getElementById('e'),b=document.getElementById('b');
  f.addEventListener('submit',function(ev){
    ev.preventDefault();
    e.hidden=true;b.disabled=true;b.textContent='Signing in';
    fetch('/api/auth/login',{method:'POST',
      headers:{'content-type':'application/json'},
      body:JSON.stringify({password:p.value})})
      .then(function(r){
        if(r.ok){location.replace('/');return null;}
        return r.json().catch(function(){return{};}).then(function(x){
          throw new Error(x.error||('refused ('+r.status+')'));});
      })
      .catch(function(x){
        e.textContent=(x&&x.message)||'the server did not answer';
        e.hidden=false;b.disabled=false;b.textContent='Sign in';p.select();
      });
  });
})();
</script>
</body></html>`;
  }

  /* One moment, for somebody holding the link and nothing else.
   *
   * This is the human half of the Discord carve-out. The unfurler reads the
   * <head> and never renders any of this; the person who clicks the card gets
   * this page. Same three routes, same visibility check, no session, no API.
   *
   * `r` is null both when the id never existed and when it is not published,
   * and this page is IDENTICAL in the two cases — see rule 1 above. A page
   * that said "not public" would be an oracle: paste a hundred ids and learn
   * which hundred clips exist without being allowed to see one.
   */
  /* NO robots meta on this page, deliberately, and there was one for a round.
     This page exists to be read by a robot. Discord's fetcher honours
     robots.txt, and asking one unfurler not to look while asking another to
     render the tags is a distinction too fine to bet the feature on — it cost
     an evening's debugging to learn that. Keeping strangers out is the gate's
     job; search-engine reach belongs at /robots.txt, where one file governs
     the site instead of one page quietly disagreeing with the rest. */
  function cardPage(req, r, head) {
    const base = publicBase(req);
    const still = r ? isStill(r) : false;
    const vt = r && !still ? playType(r) : null;
    const dur = r ? shortDur(r.duration_s) : null;
    const tags = r
      ? TAGLETS_OF.all(r.id).filter((t) => !t.gate).slice(0, 8).map((t) => t.name)
      : [];
    const title = r ? (r.title || 'A moment') : SITE;
    /* Only when both are known, and only as a ratio — the frame then reserves
       the right shape before a byte of video has arrived, which is the
       difference between a card that settles and one that jumps. */
    const ratio = r && r.width > 0 && r.height > 0
      ? ` style="aspect-ratio:${r.width}/${r.height}"` : '';
    const poster = r?.poster_path ? `${base}/media/snippet-poster/${r.id}` : null;

    let media = '';
    if (r && still && r.file_path) {
      media = `<img${ratio} src="${attrEsc(
        `${base}/media/snippet/${r.id}/${r.id}${extname(r.file_path).toLowerCase() || '.jpg'}`)}"
        alt="${attrEsc(title)}">`;
    } else if (r && r.file_path && vt) {
      /* preload="metadata" rather than auto: this link gets pasted into a
         channel and opened by a dozen people at once, and none of them asked
         for the whole clip before pressing play. */
      media = `<video${ratio} controls playsinline preload="metadata"${
        poster ? ` poster="${attrEsc(poster)}"` : ''}>
        <source src="${attrEsc(`${base}/media/snippet/${r.id}`)}" type="${attrEsc(vt)}">
      </video>`;
    } else if (poster) {
      // The still exists and the bytes do not — better than an empty frame.
      media = `<img${ratio} src="${attrEsc(poster)}" alt="${attrEsc(title)}">`;
    }

    const line = [dur, tags.join(' · ')].filter(Boolean).join('  ·  ');
    return `<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>${attrEsc(r ? `${title} · ${SITE}` : SITE)}</title>
${FAVICON}
${head}
<style>${PAGE_CSS}
main{width:100%;max-width:760px;display:flex;flex-direction:column;gap:14px}
/* Black behind the video whatever the page's ground is: letterboxing that
   matches the page makes a 9:16 clip look like a cropping mistake, and every
   player anybody has ever seen sits on black. */
.frame{width:100%;background:#000;border:1px solid var(--line);border-radius:14px;
  overflow:hidden;display:flex;line-height:0}
video,img{width:100%;height:auto;max-height:76vh;display:block;object-fit:contain;
  background:#000}
h1{margin:0;font-size:1.18rem;line-height:1.35;font-weight:700}
.line{margin:0;color:var(--text-2);font-size:.85rem}
.bar{display:flex;gap:16px;align-items:center;flex-wrap:wrap;font-size:.85rem;
  font-weight:600}
.empty{padding:34px 18px;text-align:center;color:var(--text-2);
  background:var(--panel);border:1px solid var(--line);border-radius:14px}
/* The wordmark is the way back on this page, so it has to look like one. */
.brand:hover .wm{color:var(--link)}
.wm{transition:color .15s ease}
</style></head><body>
<a class="brand" href="/"><span class="mark"></span><span class="wm">Flat<i>fox</i></span></a>
<main>
${media ? `  <div class="frame">${media}</div>` : ''}
${r ? `  <h1>${attrEsc(title)}</h1>` : ''}
${r && line ? `  <p class="line">${attrEsc(line)}</p>` : ''}
${r ? `  <div class="bar">
    <a href="${attrEsc(`/media/snippet/${r.id}?dl=1`)}">Save the file</a>
    <a href="/">The rest of the archive</a>
  </div>` : `  <div class="empty">Nothing to show here.<br><a href="/">Flatfox</a></div>`}
  <p class="foot">${FOOT}</p>
</main>
</body></html>`;
  }

  app.get('/m/:id', (req, res) => {
    let tags = [];
    /* A malformed id, a database that does not answer, a title with something
       unusual in it — none of these should cost somebody the page. Fall back
       to the site's own card and serve it. */
    try { tags = momentMeta(String(req.params.id ?? ''), req); }
    catch (e) { console.error('meta for /m:', e?.message ?? e); tags = []; }

    const head = tags
      .map(([k, v]) => `<meta property="${attrEsc(k)}" content="${attrEsc(v)}">`)
      .join('\n');

    /* Somebody holding the link and no session gets the card, not the app.
       Signed in, the same URL opens the archive at that moment, which is what
       it has always done and what you want when it is you clicking it. */
    if (gateOn() && !signedIn(req)) {
      let row = null;
      try { row = publicSnippet(String(req.params.id ?? '')); }
      catch (e) { console.error('card for /m:', e?.message ?? e); }
      /* `private, no-cache` for the same reason as below — a shared proxy
         holding this under an id outlives the clip being gated later. No ETag
         though: this page is a few kilobytes, so asking and re-sending cost
         the same and one of them is simpler. */
      res.set('Cache-Control', 'private, no-cache');
      return res.type('html').send(cardPage(req, row, head));
    }

    const html = indexHtml();
    if (html === null) return res.status(500).json({ error: 'the page is missing' });

    /* `private`, because a shared proxy holding a page under an id is exactly
       the kind of thing that outlives a gate being added to a taglet later —
       the head would go on naming a clip that is no longer public.

       `no-cache` rather than `no-store`, which is the weaker-sounding of the
       two and the stronger choice here: store it, but ask every time. The page
       is most of a megabyte and this is the route people paste into chat, so
       no-store meant re-sending the entire application on every click of every
       shared link. The ETag below is what makes asking cheap.

       It covers the file's mtime and the injected block, which is precisely
       what can change: a rebuilt page, or a clip that has been retitled,
       gated, or published since. Anything else — who is asking, what they
       hold — the head does not depend on, which is why this needs no Vary. */
    const body = head ? html.replace('</head>', `${head}\n</head>`) : html;
    res.set('Cache-Control', 'private, no-cache');
    res.set('ETag', `W/"m${indexCache.mtime}-${
      createHash('sha1').update(head).digest('base64url').slice(0, 16)}"`);
    res.type('html');
    /* express compares this against If-None-Match for us, but only if we do
       not hand it a body first. */
    if (req.fresh) return res.status(304).end();
    res.send(body);
  });

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
  /* Create, write, delete. `existsSync` is not enough and neither is
     `accessSync(W_OK)`: on a share with Windows ACLs the POSIX bits say yes
     while an explicit Deny ACE says no, and only an actual write finds out. */
  const quarantineState = () => {
    if (!CONFIG.quarantineRoot) return '(unset — uploads are disabled)';
    const probe = join(CONFIG.quarantineRoot, `.probe-${process.pid}`);
    try {
      writeFileSync(probe, 'x');
      rmSync(probe, { force: true });
      return `${CONFIG.quarantineRoot} — writable`;
    } catch (e) {
      return `${CONFIG.quarantineRoot} — NOT WRITABLE (${e.code}); uploads will fail`;
    }
  };

  const app = makeApp();
  app.listen(CONFIG.port, CONFIG.host, () => {
    console.log(`flatfox  http://${CONFIG.host}:${CONFIG.port}`);
    console.log(`  db          ${dbPath}`);
    console.log(`  media root  ${CONFIG.mediaRoot ?? '(unset — captures read unverified)'}`);
    console.log(`  cache root  ${CONFIG.cacheRoot ?? '(unset — posters only where the media tree has them)'}`);
    console.log(`  quarantine  ${quarantineState()}`);
    app.startNormalizeWorker();
    console.log(`  dev auth    ${CONFIG.devAuth ? 'ON — do not expose this' : 'off'}`);
    /* The one line to read before opening a port. The gate and dev auth are
       opposites by construction, so there is no state where both are on and no
       way to expose the archive by forgetting a flag — but "gate on, no
       password set" is reachable and means nobody can sign in at all, which is
       worth saying out loud rather than discovering at the login box. */
    console.log(`  gate        ${CONFIG.devAuth
      ? 'off — dev auth is on'
      : CONFIG.adminPass
        ? `ON — sign in as ${CONFIG.adminHandle}`
        : 'ON, but NO PASSWORD IS SET — nobody can sign in'}`);
    console.log(`  ingest      ${CONFIG.ingestToken ? 'enabled' : 'disabled (no token set)'}`);
  });
}


