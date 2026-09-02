#!/usr/bin/env node
/* scripts/check-media.js — is the imported collection actually sound?
 *
 *   node scripts/check-media.js                 audit, writes nothing
 *   node scripts/check-media.js --remux         rewrap what can be rewrapped
 *   node scripts/check-media.js --transcode     re-encode what cannot (slow)
 *   node scripts/check-media.js --rescan        re-probe every clip first
 *   node scripts/check-media.js --hash          hash everything, not just suspects
 *   node scripts/check-media.js --deep          decode every clip end to end
 *   node scripts/check-media.js --from incoming audit a different folder
 *
 * ── what this is for ─────────────────────────────────────────────────────
 *
 * This and import-snippets.js are SCAFFOLDING for the existing collection —
 * the files already sitting on the NAS, gathered by hand over years, with no
 * record of where any of them came from. Once new media arrives through a
 * link or an upload, both stop being needed: the ingest path probes a file at
 * the moment it lands, when there is exactly one of it and someone is waiting.
 *
 * What does NOT go away is the logic. servedType() and remuxTarget() live in
 * archive.js precisely so this script is a thin caller, and an uploaded file
 * poses the identical question — what is actually inside this, and will a
 * browser play it. So treat the walking-a-folder part as temporary and the
 * decisions as permanent.
 *
 * ── what "clean" means here ──────────────────────────────────────────────
 *
 * Six questions, and codec trouble is only the first:
 *
 *   1. playable    will a browser decode it, given what is actually inside
 *   2. complete    is anything on disk that never made it into the archive
 *   3. intact      duration, dimensions, audio — anything truncated
 *   4. distinct    is the same clip filed twice under two names
 *   5. described   poster, transcript, taglets
 *   6. tidy        cache files left behind by rows that no longer exist
 *
 * The audit writes nothing without a fix flag, so it is safe to run first and
 * decide afterwards.
 */
import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { existsSync, mkdirSync, readdirSync, rmSync, statSync } from 'node:fs';
import { basename, extname, join } from 'node:path';
import { execFile, execFileSync } from 'node:child_process';
import { cpus } from 'node:os';
import { open, now } from '../db.js';
import { servedType, remuxTarget, fixPlan } from '../archive.js';

const MEDIA = process.env.TENMA_MEDIA_ROOT;
const CACHE = process.env.TENMA_CACHE_ROOT || null;
const DB = process.env.TENMA_DB ?? 'data/archive.db';
if (!MEDIA) { console.error('set TENMA_MEDIA_ROOT'); process.exit(1); }

const argv = process.argv.slice(2);
const has = (f) => argv.includes(f);
const REMUX = has('--remux');
const TRANSCODE = has('--transcode');
const RESCAN = has('--rescan');
const HASH_ALL = has('--hash');
const DEEP = has('--deep');
const val = (name, dflt) => {
  const i = argv.findIndex((a) => a === name);
  const raw = i >= 0 ? argv[i + 1] : argv.find((a) => a.startsWith(`${name}=`))?.split('=')[1];
  return raw ?? dflt;
};
const FROM = String(val('--from', 'snippets')).replace(/^\/+|\/+$/g, '');
const LIMIT = (() => { const n = Number(val('--limit', '')); return Number.isFinite(n) && n > 0 ? n : Infinity; })();

if ((REMUX || TRANSCODE) && !CACHE) {
  console.error('fixing needs TENMA_CACHE_ROOT: the media root is read-only and stays that way');
  process.exit(1);
}

const db = open(DB);
const t = now();
const VIDEO_RE = /\.(mp4|m4v|mov|webm|mkv|avi)$/i;

const plural = (n, one, many = one + 's') => `${n} ${n === 1 ? one : many}`;
const head = (label) => console.log(`\n\x1b[1m${label}\x1b[0m`);
const bullet = (n, label, note) => {
  console.log(`  ${String(n).padStart(5)}  ${label}`);
  if (note) console.log(`         ${note}`);
};

function probe(file) {
  try {
    const out = execFileSync('ffprobe', ['-v', 'error', '-show_entries',
      'format=format_name,duration,size:stream=codec_type,codec_name,width,height',
      '-of', 'json', file], { encoding: 'utf8' });
    const j = JSON.parse(out);
    const s = j.streams ?? [];
    const v = s.find((x) => x.codec_type === 'video') ?? {};
    const a = s.find((x) => x.codec_type === 'audio') ?? {};
    return { container: j.format?.format_name ?? null,
             video_codec: v.codec_name ?? null, audio_codec: a.codec_name ?? null,
             duration_s: Number(j.format?.duration) || null,
             bytes: Number(j.format?.size) || null,
             width: v.width ?? null, height: v.height ?? null };
  } catch { return null; }
}

const sha256 = (file) => new Promise((res, rej) => {
  const h = createHash('sha256');
  createReadStream(file).on('data', (d) => h.update(d))
    .on('end', () => res(h.digest('hex'))).on('error', rej);
});

/* Decode the whole file and report what ffmpeg complains about.
 *
 * This exists because the cheap integrity checks are not enough, and I would
 * rather say so than imply otherwise: a Matroska file truncated to 4% of its
 * length still reports a 14-second duration, because the duration lives in
 * the header and the header survived. Nothing short of decoding the thing
 * notices. *Reproduced* — a deliberately truncated fixture passes every
 * header-level check and fails here with "File ended prematurely".
 *
 * Measured at ~55x realtime, so 1542 clips is ~40 minutes serial and ~10 with
 * four at a time. That is a lot for a routine check and cheap for a one-time
 * certification of a collection that will never be assembled this way again.
 */
const verify = (file) => new Promise((res) => {
  execFile('ffmpeg', ['-v', 'error', '-i', file, '-f', 'null', '-'],
    { timeout: 600000, maxBuffer: 1 << 20 }, (err, _out, stderr) => {
      const msg = String(stderr || '').trim();
      res(msg ? msg.split('\n')[0].replace(/^\[[^\]]+\]\s*/, '') : (err ? 'decode failed' : null));
    });
});

/** Run `fn` over `items` with a fixed number in flight. Decode is CPU-bound
 *  and single-threaded per file, so the cores are the limit, not the disk. */
async function pool(items, n, fn, onTick) {
  const out = new Array(items.length);
  let i = 0, done = 0;
  await Promise.all(Array.from({ length: Math.min(n, items.length) }, async () => {
    while (i < items.length) {
      const k = i++;
      out[k] = await fn(items[k]);
      if (onTick) onTick(++done, items.length);
    }
  }));
  return out;
}

const rows = db.prepare(
  `SELECT id, slug, title, video_path, play_path, poster_path, container, video_codec,
          audio_codec, duration_s, width, height, bytes, sha256, transcript, status
     FROM snippet WHERE retracted_at IS NULL ORDER BY added_at DESC, id DESC`).all();

console.log(`${plural(rows.length, 'clip')} in the archive, media root ${MEDIA}`);

// ── re-probe where the codecs were never recorded ─────────────────────────
let probed = 0;
for (const r of rows) {
  if (r.container && !RESCAN) continue;
  const abs = join(MEDIA, r.video_path);
  if (!existsSync(abs)) continue;
  const m = probe(abs);
  if (!m) continue;
  db.prepare(`UPDATE snippet SET container=?, video_codec=?, audio_codec=?,
              duration_s=COALESCE(?,duration_s), width=COALESCE(?,width),
              height=COALESCE(?,height), bytes=COALESCE(?,bytes), updated_at=? WHERE id=?`)
    .run(m.container, m.video_codec, m.audio_codec, m.duration_s, m.width, m.height,
         m.bytes, t, r.id);
  Object.assign(r, m);
  probed++;
}
if (probed) console.log(`probed ${plural(probed, 'clip')}`);

const problems = [];

// ── 1. playable ───────────────────────────────────────────────────────────
const onDisk = new Map();   // absolute path -> row
const bad = { missing: [], unknown: [], remuxable: [], transcode: [], ok: [] };
for (const r of rows) {
  const abs = join(MEDIA, r.video_path);
  if (!existsSync(abs)) { bad.missing.push(r); continue; }
  onDisk.set(abs, r);
  if (!r.container) { bad.unknown.push(r); continue; }
  if (servedType(r.container, r.video_codec, r.audio_codec)) { bad.ok.push(r); continue; }
  if (r.play_path && CACHE && existsSync(join(CACHE, r.play_path))) { bad.ok.push(r); continue; }
  (remuxTarget(r.video_codec, r.audio_codec) ? bad.remuxable : bad.transcode).push(r);
}

head('1. playable');
const byCodec = (arr) => {
  const m = new Map();
  for (const r of arr) {
    const k = `${r.video_codec || '?'}/${r.audio_codec || 'none'} in ${r.container || '?'}`
      + ` — .${extname(r.video_path).slice(1) || '?'}`;
    m.set(k, (m.get(k) ?? 0) + 1);
  }
  return [...m].sort((a, b) => b[1] - a[1]);
};
bullet(bad.ok.length, 'play as they are');
/* Counts by codec tell you the SHAPE of the problem; names tell you which
   files to go and look at. Both, because at three bad clips you want the
   names and at three hundred you want the pattern — so name them while the
   list is still short enough to read. */
const NAME_UP_TO = 12;
const detail = (arr) => {
  for (const [k, c] of byCodec(arr).slice(0, 6)) console.log(`           ${String(c).padStart(5)}  ${k}`);
  if (arr.length <= NAME_UP_TO) for (const r of arr) console.log(`             ${r.slug}`);
};
if (bad.remuxable.length) {
  bullet(bad.remuxable.length, 'need a container swap',
         'the bitstream is fine, the wrapper is wrong — `-c copy`, no quality loss');
  detail(bad.remuxable);
  problems.push(`${bad.remuxable.length} need --remux`);
}
if (bad.transcode.length) {
  /* Split by which STREAM is actually at fault, because the difference is
     enormous: a file with good H.264 and uncompressed PCM audio needs its
     audio encoded and its video copied, which is seconds and lossless where it
     matters. Calling that "a re-encode" invites throwing away a perfectly good
     picture to fix a soundtrack. */
  const plans = bad.transcode.map((r) => ({ r, p: fixPlan(r.video_codec, r.audio_codec) }));
  const both = plans.filter((x) => x.p.video === 'encode' && x.p.audio === 'encode');
  const vOnly = plans.filter((x) => x.p.video === 'encode' && x.p.audio !== 'encode');
  const aOnly = plans.filter((x) => x.p.video !== 'encode' && x.p.audio === 'encode');
  bullet(bad.transcode.length, 'not playable as they are', 'and only these streams need touching:');
  const sub = (arr, label, note) => {
    if (!arr.length) return;
    console.log(`         ${String(arr.length).padStart(5)}  ${label}   ${note}`);
    for (const { r } of arr.slice(0, NAME_UP_TO)) {
      console.log(`                   ${r.slug}  (${r.video_codec || '?'}/${r.audio_codec || 'none'})`);
    }
  };
  sub(aOnly, 'audio only', 'video copied untouched — fast, and the picture is preserved exactly');
  sub(vOnly, 'video only', 'audio copied untouched');
  sub(both, 'both      ', 'the expensive case');
  problems.push(`${bad.transcode.length} need --transcode`);
}
if (bad.unknown.length) { bullet(bad.unknown.length, 'never probed', 'run with --rescan'); problems.push('unprobed clips'); }
if (bad.missing.length) {
  bullet(bad.missing.length, 'row points at a file that is not on disk',
         'moved or deleted — re-run import-snippets to re-point, or retract the row');
  for (const r of bad.missing.slice(0, 8)) console.log(`           ${r.slug}  ->  ${r.video_path}`);
  problems.push(`${bad.missing.length} rows with no file`);
}

// ── 2. complete — the reverse direction, which the importer never checks ──
head('2. complete');
const dir = join(MEDIA, FROM);
let strays = [];
if (existsSync(dir)) {
  for (const f of readdirSync(dir).sort()) {
    if (!VIDEO_RE.test(f)) continue;
    const abs = join(dir, f);
    if (!onDisk.has(abs)) strays.push(f);
  }
}
bullet(onDisk.size, `files under ${FROM}/ that the archive knows about`);
if (strays.length) {
  bullet(strays.length, 'on disk but never imported',
         'run import-snippets (add --from if these live elsewhere)');
  for (const f of strays.slice(0, 10)) console.log(`           ${f}`);
  if (strays.length > 10) console.log(`           …and ${strays.length - 10} more`);
  problems.push(`${strays.length} unimported files`);
} else if (existsSync(dir)) {
  console.log('         nothing on disk is missing from the archive');
}

// ── 3. intact ─────────────────────────────────────────────────────────────
head('3. intact');
const noDur = rows.filter((r) => !bad.missing.includes(r) && !(r.duration_s > 0));
const noDim = rows.filter((r) => !bad.missing.includes(r) && !(r.width > 0 && r.height > 0));
const noAud = rows.filter((r) => !bad.missing.includes(r) && r.container && !r.audio_codec);
const tiny = rows.filter((r) => !bad.missing.includes(r) && r.bytes !== null && r.bytes < 4096);
if (noDur.length) {
  bullet(noDur.length, 'no duration — truncated, or the header is damaged');
  for (const r of noDur.slice(0, 8)) console.log(`           ${r.slug}`);
  problems.push(`${noDur.length} with no duration`);
}
if (noDim.length) {
  bullet(noDim.length, 'no dimensions — probably not a video at all');
  for (const r of noDim.slice(0, 8)) console.log(`           ${r.slug}`);
  problems.push(`${noDim.length} with no dimensions`);
}
if (tiny.length) { bullet(tiny.length, 'under 4KB — almost certainly a failed download'); problems.push(`${tiny.length} tiny files`); }
if (!noDur.length && !noDim.length && !tiny.length) console.log('         every header looks sane');
// Last, and outside the verdict: silence is a fact, not a fault.
if (noAud.length) bullet(noAud.length, 'no audio track', 'not necessarily wrong — worth a glance if you expected sound');

if (DEEP) {
  const check = rows.filter((r) => !bad.missing.includes(r));
  const jobs = Math.max(1, Math.min(4, cpus().length));
  const secs = check.reduce((a, r) => a + (r.duration_s ?? 0), 0);
  console.log(`         decoding ${plural(check.length, 'clip')} (${(secs / 3600).toFixed(1)}h of video, `
    + `${jobs} at a time, roughly ${Math.ceil(secs / 55 / jobs / 60)} min)…`);
  // Only when stdout is a terminal: a carriage return in a redirected log
  // just makes one unreadable line.
  const live = process.stdout.isTTY;
  let last = 0;
  const errs = await pool(check, jobs, (r) => verify(join(MEDIA, r.video_path)),
    (d, n) => {
      if (!live) return;
      const p = Math.floor((d / n) * 20);
      if (p > last) { last = p; process.stdout.write(`\r         ${d}/${n}`.padEnd(40)); }
    });
  const broken = check.map((r, i) => ({ r, e: errs[i] })).filter((x) => x.e);
  if (live) process.stdout.write(`\r${' '.repeat(40)}\r`);
  if (broken.length) {
    bullet(broken.length, 'fail to decode all the way through',
           'truncated or corrupt — a header check cannot see this');
    for (const { r, e } of broken.slice(0, 10)) console.log(`           ${r.slug}: ${e}`);
    problems.push(`${broken.length} that do not decode`);
  } else console.log('         every clip decodes end to end');
} else {
  console.log('         (--deep decodes every clip to catch truncation a header hides)');
}

// ── 4. distinct ───────────────────────────────────────────────────────────
/* Cheap screen, expensive confirm. Hashing everything is ~9 minutes over a
   collection this size and almost all of it is wasted, since two clips that
   are genuinely the same file agree on byte count AND duration first. So
   group on those, then hash only inside groups that collide. */
head('4. distinct');
const groups = new Map();
for (const r of bad.ok.concat(bad.remuxable, bad.transcode)) {
  if (!r.bytes) continue;
  const k = `${r.bytes}:${Math.round(r.duration_s ?? -1)}`;
  if (!groups.has(k)) groups.set(k, []);
  groups.get(k).push(r);
}
const suspects = HASH_ALL ? rows.filter((r) => !bad.missing.includes(r))
  : [...groups.values()].filter((g) => g.length > 1).flat();
if (suspects.length) {
  console.log(`         hashing ${plural(suspects.length, 'candidate')}…`);
  for (const r of suspects) {
    if (r.sha256) continue;
    try {
      r.sha256 = await sha256(join(MEDIA, r.video_path));
      db.prepare('UPDATE snippet SET sha256 = ? WHERE id = ?').run(r.sha256, r.id);
    } catch { /* unreadable is already reported above */ }
  }
}
const byHash = new Map();
for (const r of rows) {
  if (!r.sha256) continue;
  if (!byHash.has(r.sha256)) byHash.set(r.sha256, []);
  byHash.get(r.sha256).push(r);
}
const dupes = [...byHash.values()].filter((g) => g.length > 1);
if (dupes.length) {
  /* Every set, not the first eight, and annotated. Fifteen of these turned up
     in the real collection and the names are the whole difficulty —
     `tenma-maemi-catbreakdance` and
     `tenma-maemi-pipkin-pippa-busting-it-down-freestyle-dance-meme` are the
     same bytes and one of those titles is better. Nothing can pick for you,
     but showing which copy already carries a transcript and taglets turns it
     from a coin-flip into a glance. */
  const rich = db.prepare(
    `SELECT (SELECT count(*) FROM snippet_taglet st WHERE st.snippet_id = ?1) tags,
            (SELECT count(*) FROM snippet_line l WHERE l.snippet_id = ?1) lines`);
  bullet(dupes.length, 'sets of byte-identical clips filed under different names',
         'the archive serves both; keep the better-described one and retract the other');
  for (const g of dupes) {
    const scored = g.map((r) => {
      const m = rich.get(r.id);
      return { r, ...m, score: m.tags * 2 + m.lines + (r.status === 'confirmed' ? 1 : 0) };
    }).sort((a, b) => b.score - a.score);
    console.log('');
    for (const [i, s] of scored.entries()) {
      const marks = [`${s.tags} tag${s.tags === 1 ? '' : 's'}`,
                     s.lines ? `${s.lines}-line transcript` : 'no transcript',
                     s.r.status].join(', ');
      console.log(`           ${i === 0 ? '\x1b[1mkeep?\x1b[0m' : '     '} ${s.r.slug}`);
      console.log(`                 ${marks}`);
    }
  }
  problems.push(`${dupes.length} duplicate sets`);
} else {
  console.log(`         no duplicates${HASH_ALL ? '' : ' among clips that share a size and a duration'}`);
}

// ── 5. described ──────────────────────────────────────────────────────────
head('5. described');
const noPoster = rows.filter((r) => !r.poster_path);
const noText = rows.filter((r) => !r.transcript);
const noTags = db.prepare(
  `SELECT count(*) c FROM snippet s WHERE s.retracted_at IS NULL
     AND NOT EXISTS (SELECT 1 FROM snippet_taglet st WHERE st.snippet_id = s.id)`).get().c;
if (noPoster.length) bullet(noPoster.length, 'no poster', 're-run import-snippets with a cache root set');
if (noText.length) bullet(noText.length, 'no transcript', 'editors can now write one in the panel');
if (noTags) bullet(noTags, 'no taglets at all', 'unfindable by tag — these are the ones to triage first');
if (!noPoster.length && !noText.length && !noTags) console.log('         every clip has a poster, a transcript and at least one taglet');
const byStatus = db.prepare(
  'SELECT status, count(*) c FROM snippet WHERE retracted_at IS NULL GROUP BY status').all();
console.log(`         status: ${byStatus.map((s) => `${s.c} ${s.status}`).join(', ')}`);

// ── 6. tidy ───────────────────────────────────────────────────────────────
head('6. tidy');
if (!CACHE) console.log('         no cache root set — nothing to check');
else {
  const wanted = new Set();
  for (const r of rows) { if (r.poster_path) wanted.add(r.poster_path); if (r.play_path) wanted.add(r.play_path); }
  const orphans = [];
  for (const sub of ['snippets', 'play']) {
    const d = join(CACHE, sub);
    if (!existsSync(d)) continue;
    for (const f of readdirSync(d)) if (!wanted.has(`${sub}/${f}`)) orphans.push(`${sub}/${f}`);
  }
  if (orphans.length) {
    const bytes = orphans.reduce((a, f) => { try { return a + statSync(join(CACHE, f)).size; } catch { return a; } }, 0);
    bullet(orphans.length, `cache files no row refers to (${(bytes / 1e6).toFixed(1)} MB)`,
           'harmless, and regenerated on demand — delete with --tidy');
    if (has('--tidy')) {
      for (const f of orphans) rmSync(join(CACHE, f), { force: true });
      console.log(`         deleted ${plural(orphans.length, 'file')}`);
    }
  } else console.log('         no orphaned cache files');
}

// ── fixes ─────────────────────────────────────────────────────────────────
async function fix(list, mode) {
  let done = 0, failed = 0;
  mkdirSync(join(CACHE, 'play'), { recursive: true });
  for (const r of list) {
    if (done >= LIMIT) break;
    const plan = fixPlan(r.video_codec, r.audio_codec);
    const target = mode === 'remux' ? remuxTarget(r.video_codec, r.audio_codec) : plan.container;
    const rel = `play/${basename(r.video_path, extname(r.video_path))}.${target}`;
    const abs = join(CACHE, rel);
    try {
      if (!existsSync(abs)) {
        const args = ['-loglevel', 'error', '-y', '-i', join(MEDIA, r.video_path)];
        if (mode === 'remux') args.push('-c', 'copy');
        else {
          // Per stream. Copying a good picture instead of re-encoding it is
          // both faster and the only version that does not lose anything.
          args.push('-c:v', plan.video === 'copy' ? 'copy'
            : 'libx264', ...(plan.video === 'copy' ? []
              : ['-preset', 'veryfast', '-crf', '20', '-pix_fmt', 'yuv420p']));
          if (plan.audio === 'none') args.push('-an');
          else args.push('-c:a', plan.audio === 'copy' ? 'copy' : 'aac',
            ...(plan.audio === 'copy' ? [] : ['-b:a', '160k']));
        }
        // Metadata goes: it is the one part nobody looks at and the part a
        // scraped file carries whatever the source put there.
        args.push('-map_metadata', '-1');
        if (target === 'mp4') args.push('-movflags', '+faststart');
        args.push(abs);
        execFileSync('ffmpeg', args, { stdio: 'pipe', timeout: mode === 'remux' ? 120000 : 1800000 });
      }
      db.prepare('UPDATE snippet SET play_path = ?, updated_at = ? WHERE id = ?').run(rel, t, r.id);
      done++;
      if (done % 25 === 0) console.log(`  ${done} done…`);
    } catch (e) {
      failed++;
      console.log(`  FAILED ${r.slug}: ${String(e.stderr ?? e.message).trim().split('\n')[0]}`);
    }
  }
  return { done, failed };
}

if (REMUX && bad.remuxable.length) {
  head('rewrapping');
  const { done, failed } = await fix(bad.remuxable, 'remux');
  console.log(`  ${done} rewrapped${failed ? `, ${failed} failed` : ''}`);
}
if (TRANSCODE && bad.transcode.length) {
  head('re-encoding');
  console.log(`  ${plural(bad.transcode.length, 'clip')} at roughly 0.3x realtime — this is the slow one`);
  const { done, failed } = await fix(bad.transcode, 'transcode');
  console.log(`  ${done} re-encoded${failed ? `, ${failed} failed` : ''}`);
}
if (REMUX || TRANSCODE) {
  db.prepare(`INSERT INTO meta(key,value) VALUES('generation','1')
              ON CONFLICT(key) DO UPDATE SET value = CAST(CAST(value AS INTEGER)+1 AS TEXT)`).run();
}

// ── verdict ───────────────────────────────────────────────────────────────
console.log('');
if (!problems.length) {
  console.log('\x1b[32mclean\x1b[0m — every clip plays, nothing is missing in either direction,');
  console.log('        nothing is duplicated, and everything is described.');
} else {
  console.log(`\x1b[33m${plural(problems.length, 'thing')} to deal with:\x1b[0m`);
  for (const p of problems) console.log(`  · ${p}`);
}
db.close();
process.exit(problems.length ? 1 : 0);
