#!/usr/bin/env node
/**
 * One-shot snippet importer, and the reference for yours.
 *
 * Walks <media root>/snippets/*.mp4, reads a JSON sidecar beside each, and
 * writes snippet / snippet_line / taglet / snippet_taglet.
 *
 * The sidecar it expects — <stem>.json, next to the mp4:
 *
 *   {
 *     "title":    "I have a donut",          // optional; derived from the stem
 *     "summary":  null,                       // optional human blurb
 *     "taglets":  ["character:tenma_maemi",   // "kind:slug", or bare for general
 *                  "copyright:phase_connect",
 *                  "meta:funny"],
 *     "names":    { "tenma_maemi": "Tenma Maemi",   // slug -> display name.
 *                   "phase_connect": "Phase Connect" },  // optional; falls back
 *     "lines":    [ { "start": 0.4, "end": 2.1,      // to a title-cased slug
 *                     "speaker": null,
 *                     "text": "I have a donut" } ],
 *     "_asr":     { "model": "large-v3",              // optional; the run's own
 *                   "at": "2026-08-28T01:44:48+00:00",//  report on itself
 *                   "status": "ok", "reason": "" }
 *   }
 *
 * Two things worth copying into your own importer:
 *
 *  1. SLUGS GO THROUGH slugify(). `character:eimi_isami` is a fine thing to
 *     write in a sidecar, but the stored slug must be `eimi-isami`, because
 *     db.js's tag-style slug rules collapse underscores to hyphens. Write an
 *     underscore slug and it will look right until something re-derives it.
 *
 *  2. `snippet.transcript` is the flat join of the lines and exists only so
 *     FTS5 has one column on one row to index. Write both or search silently
 *     returns nothing while the transcript panel looks perfectly fine.
 */

import { readdirSync, readFileSync, existsSync, mkdirSync, statSync } from 'node:fs';
import { basename, extname, join } from 'node:path';
import { execFileSync } from 'node:child_process';
import { open, now, slugify, tx, ulid } from '../db.js';
import { servedType } from '../archive.js';

const MEDIA = process.env.TENMA_MEDIA_ROOT;
const DB = process.env.TENMA_DB ?? 'data/archive.db';
if (!MEDIA) { console.error('set TENMA_MEDIA_ROOT'); process.exit(1); }

/* Imports land UNREVIEWED by default. A thousand clips carrying machine
   transcripts and filename-derived titles are not a thousand things you have
   decided to publish, and the safe direction for that default is obvious: a
   clip wrongly held back is a queue item, a clip wrongly published is on the
   internet. Pass --publish to skip the queue for a batch you trust. */
const argv = process.argv.slice(2);
const STATUS = argv.includes('--publish') ? 'confirmed' : 'proposed';

/* --dry-run reads everything and writes nothing. Worth running FIRST on a big
   collection: it finds the malformed sidecars, the missing posters and the
   failed transcriptions across all of them in one pass, without putting a
   single row in the database to undo afterwards.

   --limit N stops after N imports. Because the loop skips anything already
   present by slug, running `--limit 50` repeatedly walks the collection fifty
   at a time — which is the shape of "review a batch, decide whether the rest
   is worth it". The fifty are the first fifty by filename, so the same command
   twice does not fight itself. */
const DRY = argv.includes('--dry-run') || argv.includes('-n');

/* --update revisits clips that are already in, and is the answer to a trap
   this script would otherwise set for you.

   Sidecars arrive in two passes: the prep run writes titles and taglets, and
   whisper fills in the transcript later. Import between those two and the
   normal loop — which skips anything present by slug — would never bring the
   transcripts in at all. You would be left re-importing from scratch, which
   means deleting rows a reviewer has already ruled on.

   So this refreshes the machine-written parts: transcript, its lines, its ASR
   report, the poster if one has appeared, and the taglets. It does NOT touch
   `title`, `summary` or `status` — those are where the human decisions live,
   and a second import pass has no business overwriting a title you fixed or
   re-queueing a clip you already published. */
const UPDATE = argv.includes('--update');
// The escape hatch for the above: overwrite even a corrected transcript.
const FORCE_TR = argv.includes('--force-transcript');
const LIMIT = (() => {
  const i = argv.findIndex((a) => a === '--limit');
  const raw = i >= 0 ? argv[i + 1] : argv.find((a) => a.startsWith('--limit='))?.split('=')[1];
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : Infinity;
})();

/* Posters are GENERATED here, into a root that is writable — TENMA_MEDIA_ROOT
   is mounted read-only on purpose and must stay that way. Unset it and the
   importer simply uses whatever stills already exist under the media tree.

   At ingest and not on request, deliberately: a cache miss that forks ffmpeg
   from an unauthenticated GET is a denial-of-service primitive, since one page
   of forty uncached clips is forty processes and nothing stops someone asking
   for five hundred. Here it is an offline batch on a box you are already
   logged into. Measured at 64ms a clip — a hundred seconds for fifteen
   hundred, once. */
const CACHE = process.env.TENMA_CACHE_ROOT || null;
const NO_POSTERS = argv.includes('--no-posters');

const KINDS = new Set(['character', 'copyright', 'meta', 'general']);

/* --from <subdir> is which folder under the media root to walk, and it exists
   so that "files I have dropped somewhere" and "files the site serves" can be
   two different folders without either of them needing to be writable by
   anything new.

   Note what it does NOT do: it does not move anything. `video_path` records
   where the clip was found, and /media/snippet/:id serves that path, so a clip
   imported out of `incoming/` plays exactly as well as one in `snippets/`. The
   folder is the importer's input, not the archive's idea of what is published
   — `snippet.status` is that, and it is a database fact. Moving a file into
   `snippets/` afterwards is tidiness and nothing else. */
const FROM = (() => {
  const i = argv.findIndex((a) => a === '--from');
  const raw = i >= 0 ? argv[i + 1] : argv.find((a) => a.startsWith('--from='))?.split('=')[1];
  // Containment: this is a trusted CLI, but a leading / or a .. silently
  // walking out of the media root is the sort of thing that stops being
  // trusted the day someone wires it to a job queue.
  const rel = String(raw ?? 'snippets').replace(/^\/+|\/+$/g, '');
  if (!rel || rel.split('/').includes('..')) { console.error('bad --from'); process.exit(1); }
  return rel;
})();
const dir = join(MEDIA, FROM);
const db = open(DB);
const t = now();

/* ── the vocabulary ──────────────────────────────────────────────────────
   Sidecars carry the token that came out of a filename — `airi`, `bae`,
   `bocchi`. Those are handles, not names, and minting them straight would
   leave the archive full of taglets called "Airi" and "Bae" that nobody can
   fix afterwards without touching every clip that uses them.

   --vocab points at the table that translates them. It reads the CSV the
   prep pass already produces, columns:

     token         what the sidecar says
     is_character  y = real, n = junk from filename parsing, ? = undecided
     slug          what to store; falls back to the token
     display_name  what a human should read
     kind          character | copyright | meta | general
     copyright     ATTACHED AS A SECOND TAGLET, not a column on the first
     aliases       other tokens that mean this same thing, ; or | separated

   A .json file mapping token -> { slug, name, kind, copyright } works too,
   and a bare string value is taken as the display name.

   The gate matters as much as the mapping: is_character=n rows are tokens
   like `actually`, `adds`, `ahh` — parser debris that a previous pass
   auto-marked kind=character. Without dropping them the archive gains
   ninety-three character taglets that are not characters. */
const VOCAB = (() => {
  const i = argv.findIndex((a) => a === '--vocab');
  const p = i >= 0 ? argv[i + 1] : argv.find((a) => a.startsWith('--vocab='))?.split('=')[1];
  return p || null;
})();

const vocab = new Map();      // token/alias -> { slug, name, kind, copyright }
const vocabDrop = new Set();  // tokens the table says are not taglets at all
let vocabRows = 0;

function loadVocab(path) {
  const raw = readFileSync(path, 'utf8');
  if (path.endsWith('.json')) {
    for (const [k, v] of Object.entries(JSON.parse(raw))) {
      vocab.set(k.trim().toLowerCase(),
        typeof v === 'string' ? { name: v } : v);
      vocabRows++;
    }
    return;
  }
  // Minimal CSV: quoted fields with embedded commas and doubled quotes. The
  // prep table has names like `Ninomae Ina'nis` and notes with commas in.
  const rows = [];
  let row = [], cell = '', q = false;
  for (let n = 0; n < raw.length; n++) {
    const c = raw[n];
    if (q) {
      if (c === '"' && raw[n + 1] === '"') { cell += '"'; n++; }
      else if (c === '"') q = false;
      else cell += c;
    } else if (c === '"') q = true;
    else if (c === ',') { row.push(cell); cell = ''; }
    else if (c === '\n') { row.push(cell); rows.push(row); row = []; cell = ''; }
    else if (c !== '\r') cell += c;
  }
  if (cell || row.length) { row.push(cell); rows.push(row); }

  const head = (rows.shift() ?? []).map((h) => h.trim().toLowerCase());
  const col = (r, name) => {
    const i = head.indexOf(name);
    return i < 0 ? '' : (r[i] ?? '').trim();
  };
  for (const r of rows) {
    const token = col(r, 'token').toLowerCase();
    if (!token) continue;
    const verdict = col(r, 'is_character').toLowerCase();
    const keys = [token, ...col(r, 'aliases').split(/[;|,]/).map((a) => a.trim().toLowerCase())]
      .filter(Boolean);
    if (verdict === 'n') { for (const k of keys) vocabDrop.add(k); continue; }
    // '?' is undecided — no verdict, so no opinion. Left to fall through to
    // the sidecar's own kind rather than guessed at from an unfilled row.
    if (verdict !== 'y') continue;
    const entry = {
      slug: col(r, 'slug') || token,
      name: col(r, 'display_name') || null,
      kind: col(r, 'kind') || null,
      copyright: col(r, 'copyright') || null,
    };
    for (const k of keys) vocab.set(k, entry);
    vocabRows++;
  }
}

if (VOCAB) {
  try { loadVocab(VOCAB); }
  catch (e) { console.error(`could not read --vocab ${VOCAB}: ${e.message}`); process.exit(1); }
  console.log(`vocabulary: ${vocabRows} entries, ${vocab.size} keys incl. aliases, `
    + `${vocabDrop.size} tokens marked not-a-taglet`);
}

/** ffprobe, or nulls. A missing duration is worth importing anyway — the row
 *  is still findable and playable, it just cannot say how long it is. */
/* Codecs are read here and stored, not re-derived at serve time, because the
   answer costs an ffprobe fork and the question is asked on every request.
   Both streams, not just video: a clip with Opus audio in an mp4 is a
   different playability answer from the same clip with AAC. */
function probe(file) {
  const empty = { duration_s: null, bytes: null, width: null, height: null,
                  container: null, video_codec: null, audio_codec: null };
  try {
    const out = execFileSync('ffprobe', [
      '-v', 'error',
      '-show_entries', 'format=duration,size,format_name:stream=codec_type,codec_name,width,height',
      '-of', 'json', file], { encoding: 'utf8' });
    const j = JSON.parse(out);
    const streams = j.streams ?? [];
    const v = streams.find((s) => s.codec_type === 'video') ?? {};
    const a = streams.find((s) => s.codec_type === 'audio') ?? {};
    return { duration_s: Number(j.format?.duration) || null,
             bytes: Number(j.format?.size) || null,
             width: v.width ?? null, height: v.height ?? null,
             container: j.format?.format_name ?? null,
             video_codec: v.codec_name ?? null,
             audio_codec: a.codec_name ?? null };
  } catch { return empty; }
}

const titleCase = (slug) => slug.replace(/[-_]+/g, ' ')
  .replace(/\b\w/g, (c) => c.toUpperCase());

let madePosters = 0, posterFails = 0, wantPoster = 0, keptEdits = 0;
// Clips a browser will refuse, tallied by what is actually inside them.
const unplayable = new Map();   // "h264/aac in matroska,webm" -> count

/** The still for a clip: found, or generated, or null.
 *
 *  Looked for in two places before anything is made — the cache, and the media
 *  tree, where a prep pass may already have left one. Both are stored as the
 *  same relative path, and the server resolves against both roots, so it does
 *  not matter to anyone downstream which of them produced it.
 */
function poster(stem, videoPath, durationS) {
  /* Keyed by STEM in one flat namespace, deliberately not mirroring --from:
     a clip that moves from `incoming/` to `snippets/` keeps its poster, and
     regenerating 1500 stills because a folder was renamed is a silly way to
     spend an afternoon. */
  const rel = `snippets/${stem}.jpg`;
  if (CACHE && existsSync(join(CACHE, rel))) return rel;
  // Where a prep pass writes them, next to the clips — that one does follow
  // the source folder, because that is where the prep pass put them.
  if (existsSync(join(dir, 'cache', `${stem}.jpg`))) return `${FROM}/cache/${stem}.jpg`;
  if (DRY) { wantPoster++; return null; }
  if (!CACHE || NO_POSTERS) return null;

  /* A cut clip very often opens on black or a fade, so frame zero is the worst
     available choice for a thumbnail. A second in is almost always the shot —
     but not on a clip shorter than that, hence the quarter-way floor. */
  const at = Math.min(1, Math.max(0, (durationS ?? 0) * 0.25));
  try {
    mkdirSync(join(CACHE, 'snippets'), { recursive: true });
    execFileSync('ffmpeg', [
      '-loglevel', 'error', '-y',
      ...(at > 0.05 ? ['-ss', String(at)] : []),
      '-i', videoPath, '-vframes', '1',
      // 420px wide is twice the 210px the row renders it at, so it stays sharp
      // on a 2× display and no sharper.
      '-vf', 'scale=420:-2', '-q:v', '5',
      join(CACHE, rel)], { stdio: 'pipe', timeout: 30000 });
    madePosters++;
    return rel;
  } catch (e) {
    // Never fatal. A clip with no still still plays, searches and reviews.
    posterFails++;
    return null;
  }
}

const findTaglet = db.prepare('SELECT id, kind FROM taglet WHERE slug = ?');
const addTaglet = db.prepare(
  `INSERT INTO taglet(id, name, slug, kind, status, origin, created_at, updated_at)
   VALUES(?,?,?,?,'confirmed','vault',?,?)`);   // taglets, not snippets
const findSnip = db.prepare('SELECT id, transcript_status FROM snippet WHERE slug = ?');

const dryTaglets = new Set();
const kindsBySlug = new Map();   // slug -> Set(kind), to catch a split vocabulary
const dropped = new Map();       // token -> how many clips referenced it
const unmapped = new Map();      // token -> count, for tokens the table misses

/** One spec from a sidecar -> zero, one or two taglet ids.
 *
 *  Zero when the vocabulary says the token is not a taglet at all.
 *  Two when it carries a `copyright`, which is attached alongside rather than
 *  folded in: "Chisaka Airi" and "Phase Connect" are separate facts, and a
 *  reader filtering by one should not have to know the other. */
function taglet(spec, names) {
  const [rawKind, rest] = spec.includes(':') ? spec.split(/:(.+)/) : ['general', spec];
  const token = rest.trim().toLowerCase();

  if (vocabDrop.has(token)) {
    dropped.set(token, (dropped.get(token) ?? 0) + 1);
    return [];
  }
  const v = vocab.get(token) ?? null;
  /* Only `character:` misses are worth reporting. The table adjudicates who is
     a person; `meta:funny` and a bare `donut` are the sidecar's own business
     and are not missing from anything. Reporting them would bury the handful
     of real gaps under every meta tag in the collection. */
  if (VOCAB && !v && rawKind === 'character') {
    unmapped.set(token, (unmapped.get(token) ?? 0) + 1);
  }

  const out = [];
  const one = (kindRaw, slugRaw, nameRaw) => {
    const kind = KINDS.has(kindRaw) ? kindRaw : 'general';
    const slug = slugify(slugRaw);
    /* Same slug under two kinds is a real hazard with generated sidecars:
       `reaction` and `meta:reaction` are one row, because taglet.slug is UNIQUE
       and this lookup is by slug alone — so whichever the importer meets FIRST
       silently decides the kind, and therefore the colour, for both. Recorded
       here so a dry run can say so before it matters. */
    if (!kindsBySlug.has(slug)) kindsBySlug.set(slug, new Set());
    kindsBySlug.get(slug).add(kind);

    const have = findTaglet.get(slug);
    if (have) { out.push(have.id); return; }
    // A dry run still has to resolve the spec — that is where a bad `kind:`
    // shows up — but it must not leave a vocabulary behind nobody asked for.
    if (DRY) { dryTaglets.add(`${kind}:${slug} — ${nameRaw}`); return; }
    const id = ulid();
    addTaglet.run(id, nameRaw, slug, kind, t, t);
    out.push(id);
  };

  one(v?.kind ?? rawKind,
      v?.slug ?? rest,
      v?.name ?? names?.[rest] ?? names?.[slugify(rest)] ?? titleCase(rest));

  // The copyright column: an extra taglet, named by the value itself.
  if (v?.copyright) one('copyright', v.copyright, v.copyright);
  return out;
}

let made = 0, skipped = 0, lines = 0, tagged = 0, failed = 0, refreshed = 0;

/* .mov and .m4v are in here because a collection scraped from Twitter, Discord
   and phone screen-records has them, and silently not importing a file is the
   worst of the three possible behaviours — worse than importing one that needs
   a remux, which at least shows up in the tally below. */
const files = readdirSync(dir).sort().filter((f) => /\.(mp4|m4v|mov|webm|mkv)$/i.test(f));
console.log(`${files.length} media files under ${dir}`);
if (DRY) console.log('DRY RUN — reading everything, writing nothing\n');
if (LIMIT !== Infinity) console.log(`stopping after ${LIMIT}\n`);
let noSidecar = 0, noPoster = 0;
const dryStatus = {};

/* ONE TRANSACTION PER FILE, not one for the run.
   The obvious shape — wrap the whole loop — takes a write lock for as long as
   the import runs, and this loop shells out to ffprobe on every file. At a
   thousand clips that is minutes of the live server being unable to write a
   note or apply a changeset, for no gain: each snippet is independent, so
   there is no consistency to protect across them.
   It also makes the run RESUMABLE. A crash at file 700 leaves 699 imported,
   and the slug check at the top of the loop skips them on the next attempt —
   which is the same property that makes re-running after adding sidecars free.
   And one bad sidecar costs one clip rather than the whole batch. */
for (const f of files) {
  if (made + refreshed >= LIMIT) break;
  try {
    const stem = basename(f, extname(f));
    const existing = findSnip.get(stem);
    if (existing && !UPDATE) { skipped++; continue; }

    const sidecarPath = join(dir, `${stem}.json`);
    const hasSide = existsSync(sidecarPath);
    if (!hasSide) noSidecar++;
    const side = hasSide ? JSON.parse(readFileSync(sidecarPath, 'utf8')) : {};

    // Outside the transaction below: ffprobe is the slow part and it needs no
    // lock to read a file.
    const meta = probe(join(dir, f));
    /* The file's mtime is the only surviving record of when a clip was added,
       so it is what the list sorts on. Read here rather than derived from the
       row, because every row in one import shares a created_at to the second
       and ordering on that sorts by whatever order readdir returned. */
    let addedAt = null;
    try { addedAt = Math.floor(statSync(join(dir, f)).mtimeMs / 1000); } catch { /* keep null */ }
    const posterRel = poster(stem, join(dir, f), meta.duration_s);
    if (!posterRel) noPoster++;

    /* Counted, never fatal. A clip that cannot be decoded in a browser is
       still a clip worth having in the archive with its transcript and its
       taglets — it just needs `check-media.js --remux` before anyone can watch
       it. Refusing the import would lose the metadata to fix nothing. */
    if (meta.container && !servedType(meta.container, meta.video_codec, meta.audio_codec)) {
      const k = `${meta.video_codec || '?'}/${meta.audio_codec || 'none'} in ${meta.container}`;
      unplayable.set(k, (unplayable.get(k) ?? 0) + 1);
    }

    const rows = Array.isArray(side.lines) ? side.lines : [];
    // The flat projection FTS indexes. Derived here, never authored.
    const transcript = rows.map((l) => l.text).join(' ').trim() || null;

    /* The transcriber's own report, if it left one. Four outcomes, and they are
       genuinely different questions later: nobody ran it, it ran and worked, it
       ran and heard nothing, it ran and broke. Collapsing the last two into
       "no transcript" is what makes a failed batch impossible to find. */
    const asr = side._asr ?? null;
    const asrAt = asr?.at ? Math.floor(Date.parse(asr.at) / 1000) || null : null;
    const trStatus = !asr
      ? (transcript ? 'auto' : 'none')
      : asr.status !== 'ok' ? 'failed'
      : transcript ? 'auto' : 'empty';

    if (DRY) {
      for (const spec of side.taglets ?? []) tagged += taglet(spec, side.names).length
                                                     || (vocabDrop.has(spec.split(':').pop()
                                                          .trim().toLowerCase()) ? 0 : 1);
      lines += rows.length;
      dryStatus[trStatus] = (dryStatus[trStatus] ?? 0) + 1;
      made++;
      continue;
    }

    const id = existing ? existing.id : ulid();
    tx(db, () => {
      if (existing) {
        /* A HUMAN-EDITED transcript is not the machine's to overwrite.
           Whisper mishears every VTuber name in this archive, so corrections
           are the whole reason anyone touches a transcript — and a second ASR
           pass silently reinstating "Sheena" over a fixed "Shiina" is the kind
           of loss nobody notices for months. --force-transcript is the way to
           say you mean it. */
        const keepText = existing.transcript_status === 'edited' && !FORCE_TR;
        if (keepText) keptEdits++;

        /* Machine-written columns only. title, summary and status are left
           exactly as they are — a reviewer may have fixed the title and
           published the clip already, and a transcript arriving afterwards is
           no reason to undo either. */
        /* video_path is refreshed, and it is the one column here that is not
           about transcripts. The file was just found at this path, so this is
           the truth by construction — and without it, moving a clip between
           folders on disk leaves the row pointing at nothing and the clip 404s
           with no error anywhere to explain why. Re-running the importer is
           then how you tell the archive a file moved. */
        db.prepare(
          `UPDATE snippet SET
             transcript = CASE WHEN ?1 THEN transcript ELSE ?2 END,
             transcript_status = CASE WHEN ?1 THEN transcript_status ELSE ?3 END,
             transcript_model = ?4, transcript_at = ?5, transcript_note = ?6,
             added_at = COALESCE(added_at, ?14),
             video_path = ?15,
             container = COALESCE(?16, container),
             video_codec = COALESCE(?17, video_codec),
             audio_codec = COALESCE(?18, audio_codec),
             poster_path = COALESCE(?7, poster_path),
             duration_s = COALESCE(?8, duration_s), width = COALESCE(?9, width),
             height = COALESCE(?10, height), bytes = COALESCE(?11, bytes),
             updated_at = ?12
           WHERE id = ?13`).run(
          keepText ? 1 : 0, transcript, trStatus,
          asr?.model ?? null, asrAt, (asr?.reason || null),
          posterRel, meta.duration_s, meta.width, meta.height, meta.bytes, t, id,
          addedAt ?? t, `${FROM}/${f}`,
          meta.container, meta.video_codec, meta.audio_codec);
        // Lines are replaced whole: a transcript is one artefact of one pass,
        // and merging two passes line by line would invent a version neither
        // of them produced.
        if (!keepText) db.prepare('DELETE FROM snippet_line WHERE snippet_id = ?').run(id);
        /* Taglets are UNIONED on --update, not replaced.
           They used to be deleted and rebuilt from the sidecar, which quietly
           threw away every taglet a human had attached in the panel — the
           editing feature and the importer were fighting, and the importer won
           on whoever ran last. A sidecar is one source of taglets, not the
           authority on them.
           The cost is that REMOVING a taglet from a sidecar no longer removes
           it here, which is the right way round: detaching is a decision, and
           decisions are made in the UI where they are recorded as changesets,
           not by editing a file the archive cannot see the history of. */
      } else {
        db.prepare(
          `INSERT INTO snippet(id, slug, title, summary, video_path, poster_path,
             duration_s, width, height, bytes, transcript, transcript_status,
             transcript_model, transcript_at, transcript_note,
             container, video_codec, audio_codec,
             status, origin, added_at, created_at, updated_at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'vault',?,?,?)`).run(
          id, stem, side.title ?? titleCase(stem), side.summary ?? null,
          `${FROM}/${f}`, posterRel,
          meta.duration_s, meta.width, meta.height, meta.bytes,
          transcript, trStatus,
          asr?.model ?? null, asrAt, (asr?.reason || null),
          meta.container, meta.video_codec, meta.audio_codec,
          STATUS, addedAt ?? t, t, t);
      }

      if (!(existing && existing.transcript_status === 'edited' && !FORCE_TR)) rows.forEach((l, i) => {
        db.prepare(
          `INSERT INTO snippet_line(id, snippet_id, seq, start_s, end_s, speaker, text)
           VALUES(?,?,?,?,?,?,?)`).run(
          ulid(), id, i, Number(l.start) || 0,
          l.end === undefined || l.end === null ? null : Number(l.end),
          l.speaker ?? null, String(l.text ?? ''));
        lines++;
      });

      // One spec can now resolve to two taglets (the thing and its copyright),
      // or to none at all when the vocabulary rejects the token.
      for (const spec of side.taglets ?? []) {
        for (const tid of taglet(spec, side.names)) {
          db.prepare(
            `INSERT OR IGNORE INTO snippet_taglet(id, snippet_id, taglet_id, created_at, updated_at)
             VALUES(?,?,?,?,?)`).run(ulid(), id, tid, t, t);
          tagged++;
        }
      }
    });
    if (existing) refreshed++; else made++;
    const n = made + refreshed;
    if (n % 50 === 0) console.log(`  ${n} ${UPDATE ? 'processed' : 'imported'}…`);
  } catch (e) {
    // One bad sidecar is one clip, not the batch. Named so it can be fixed and
    // the importer re-run — which costs nothing, because everything already in
    // is skipped by slug.
    failed++;
    console.error(`  SKIPPED ${f}: ${e.message}`);
  }
}

if (!DRY) {
  db.prepare(`INSERT INTO meta(key, value) VALUES('generation','1')
              ON CONFLICT(key) DO UPDATE SET value = CAST(CAST(value AS INTEGER)+1 AS TEXT)`).run();
}

console.log(`\nsnippets: ${made} ${DRY ? 'would import' : 'imported'} as ${STATUS}, `
  + `${skipped} already present`
  + (refreshed ? `, ${refreshed} refreshed` : '')
  + (failed ? `, ${failed} SKIPPED on error` : ''));
if (noSidecar) console.log(`no sidecar: ${noSidecar} — title from the filename, no tags, no transcript`);
if (unplayable.size) {
  const n = [...unplayable.values()].reduce((a, b) => a + b, 0);
  console.log(`\n${n} clip${n === 1 ? '' : 's'} no browser will play — the extension`
    + ` says one thing and the codecs say another:`);
  for (const [k, c] of [...unplayable].sort((a, b) => b[1] - a[1])) console.log(`  ${String(c).padStart(5)}  ${k}`);
  console.log('  fix: node scripts/check-media.js --remux   (rewraps the bitstream, no re-encode)');
}
if (keptEdits) {
  console.log(`kept ${keptEdits} human-edited transcript${keptEdits === 1 ? '' : 's'}`
    + ` — pass --force-transcript to overwrite them`);
}
if (madePosters) console.log(`posters generated: ${madePosters} into ${CACHE}/snippets/`);
if (posterFails) console.log(`posters FAILED: ${posterFails} — ffmpeg could not read those clips`);
if (DRY && wantPoster) {
  console.log(`posters to generate: ${wantPoster}`
    + (CACHE ? ` (~${Math.round(wantPoster * 0.064)}s of ffmpeg)` : ' — set TENMA_CACHE_ROOT to make them'));
}
if (noPoster) console.log(`no poster:  ${noPoster} — the row renders without a still`);
console.log(`transcript lines: ${lines}`);
console.log(`taglet attachments: ${tagged}`);
if (!DRY) console.log(`taglets now: ${db.prepare('SELECT count(*) c FROM taglet').get().c}`);

// The number that matters on a thousand-clip run: what needs doing again.
const pending = db.prepare(
  `SELECT count(*) c FROM snippet WHERE status = 'proposed' AND retracted_at IS NULL`).get().c;
if (pending) console.log(`awaiting review: ${pending} — open the Review tab`);

// Reported in BOTH modes: a split kind is worth knowing about whether or not
// this run is the one that writes it.
const split = [...kindsBySlug].filter(([, ks]) => ks.size > 1);
if (split.length) {
  console.log(`\n${split.length} taglet slug${split.length === 1 ? '' : 's'} used under more than one kind —`);
  console.log('the first one seen wins for all of them, so pick one in the sidecars:');
  for (const [slug, ks] of split) console.log(`  ${slug}: ${[...ks].sort().join(', ')}`);
}

if (VOCAB) {
  if (dropped.size) {
    const n = [...dropped.values()].reduce((a, b) => a + b, 0);
    console.log(`\ndropped by the vocabulary: ${dropped.size} token${dropped.size === 1 ? '' : 's'}`
      + ` across ${n} attachment${n === 1 ? '' : 's'} (is_character=n)`);
    for (const [tok, c] of [...dropped].sort((a, b) => b[1] - a[1]).slice(0, 15)) {
      console.log(`  ${tok} (${c})`);
    }
    if (dropped.size > 15) console.log(`  …and ${dropped.size - 15} more`);
  }
  if (unmapped.size) {
    console.log(`\nNOT IN THE VOCABULARY: ${unmapped.size} token${unmapped.size === 1 ? '' : 's'}`
      + ` — these keep the sidecar's own kind and a title-cased name`);
    for (const [tok, c] of [...unmapped].sort((a, b) => b[1] - a[1])) {
      console.log(`  ${tok} (${c})`);
    }
  }
}

if (DRY) {
  if (dryTaglets.size) {
    console.log(`new taglets that would be minted: ${dryTaglets.size}`);
    for (const s of [...dryTaglets].sort()) console.log(`  ${s}`);
  }
  const byDry = Object.entries(dryStatus).sort((a, b) => b[1] - a[1]);
  if (byDry.length) console.log('transcripts: ' + byDry.map(([k, v]) => `${v} ${k}`).join(', '));
  console.log('\nnothing was written. Drop --dry-run to import.');
  process.exit(failed ? 1 : 0);
}

const by = db.prepare(
  `SELECT transcript_status s, count(*) c FROM snippet GROUP BY transcript_status
    ORDER BY c DESC`).all();
console.log('transcripts: ' + by.map((r) => `${r.c} ${r.s}`).join(', '));
const bad = db.prepare(
  `SELECT slug, transcript_note FROM snippet WHERE transcript_status = 'failed'
    ORDER BY slug LIMIT 20`).all();
if (bad.length) {
  console.log(`\n${bad.length} failed — re-run these:`);
  for (const b of bad) console.log(`  ${b.slug}${b.transcript_note ? '  — ' + b.transcript_note : ''}`);
}
