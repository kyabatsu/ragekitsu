#!/usr/bin/env node
/* scripts/normalize-media.js — make every clip one format, in place.
 *
 *   node scripts/normalize-media.js --root /Volumes/media/snippets
 *   node scripts/normalize-media.js --root … --apply
 *   node scripts/normalize-media.js --root … --apply --replace --jobs 6
 *
 * ── what this is, and how it differs from check-media ────────────────────
 *
 * check-media.js writes DERIVATIVES into the cache and never touches a
 * master. That is the cautious shape, and it is the right one for a server
 * whose media mount is read-only on purpose.
 *
 * This is the other thing: it REWRITES THE MASTERS. It exists because a cache
 * of rewrapped copies solves playback and does not solve sharing — a Discord
 * embed fetches the file the link points at, so the file itself has to be the
 * right format. One format on disk, and everything downstream stops caring.
 *
 * It takes NO database. That is deliberate: it runs on your PC against the
 * NAS over SMB, and SQLite over SMB is a genuine corruption risk, not a
 * theoretical one. So this moves bytes only, and the archive catches up
 * afterwards with `import-snippets.js --update`, which re-probes every file
 * and rewrites codecs, sizes and paths from what it finds. Two steps, one
 * writer each.
 *
 * ── the target ───────────────────────────────────────────────────────────
 *
 *   H.264 High profile, yuv420p, + AAC-LC, in MP4, moov atom first.
 *
 * Every part earns its place. yuv420p because 4:2:2, 4:4:4 and 10-bit H.264
 * are all legal and none of them play in Safari. AAC-LC rather than HE-AAC
 * for the same reason. moov first (`+faststart`) because otherwise the whole
 * file must arrive before the first frame shows, which breaks progressive
 * playback and makes a link preview hang. MP4 rather than WebM because a
 * Discord embed is the requirement that has only one answer.
 *
 * ── the honest cost ──────────────────────────────────────────────────────
 *
 * A clip that is ALREADY H.264/AAC is a remux: `-c copy`, no quality loss,
 * under a second. A clip that is VP9/Opus — which is what YouTube hands you —
 * cannot be remuxed into this target and must be RE-ENCODED. That is lossy
 * and it is permanent, because the output replaces the input.
 *
 * So this refuses to guess. Run it with no flags and it tells you the split
 * and what it would cost; nothing is written without --apply. Originals are
 * kept unless you say otherwise.
 */
import { execFile, execFileSync } from 'node:child_process';
import { existsSync, mkdirSync, readdirSync, renameSync, rmSync, statSync } from 'node:fs';
import { openSync, readSync, closeSync } from 'node:fs';
import { basename, extname, join } from 'node:path';
import { cpus } from 'node:os';

const argv = process.argv.slice(2);
const has = (f) => argv.includes(f);
const val = (name, dflt) => {
  const i = argv.findIndex((a) => a === name);
  return (i >= 0 ? argv[i + 1] : argv.find((a) => a.startsWith(`${name}=`))?.split('=')[1]) ?? dflt;
};

const ROOT = val('--root', process.env.TENMA_MEDIA_ROOT
  ? join(process.env.TENMA_MEDIA_ROOT, 'snippets') : null);
const APPLY = has('--apply');
const REPLACE = has('--replace');          // delete originals instead of keeping them
const CRF = String(val('--crf', '20'));
const PRESET = String(val('--preset', 'medium'));
const JOBS = Math.max(1, Number(val('--jobs', Math.min(4, Math.max(1, cpus().length - 1)))) || 1);
const LIMIT = (() => { const n = Number(val('--limit', '')); return Number.isFinite(n) && n > 0 ? n : Infinity; })();
const KEEP = join(ROOT ?? '.', String(val('--originals', '_originals')));

if (!ROOT) { console.error('--root <folder of clips>  (or set TENMA_MEDIA_ROOT)'); process.exit(1); }
if (!existsSync(ROOT)) { console.error(`no such folder: ${ROOT}`); process.exit(1); }
for (const bin of ['ffmpeg', 'ffprobe']) {
  try { execFileSync(bin, ['-version'], { stdio: 'ignore' }); }
  catch { console.error(`${bin} is not on PATH`); process.exit(1); }
}

const VIDEO_RE = /\.(mp4|m4v|mov|webm|mkv|avi|flv|ts|wmv|mpg|mpeg|ogv)$/i;
const OK_PIX = new Set(['yuv420p', 'yuvj420p']);
const OK_PROFILE = new Set(['baseline', 'constrained baseline', 'main', 'high']);
const plural = (n, one, many = one + 's') => `${n} ${n === 1 ? one : many}`;
const hms = (s) => {
  s = Math.round(s);
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60);
  return h ? `${h}h ${m}m` : m ? `${m}m ${s % 60}s` : `${s}s`;
};
const gb = (b) => `${(b / 1e9).toFixed(1)} GB`;

function probe(file) {
  try {
    const j = JSON.parse(execFileSync('ffprobe', ['-v', 'error', '-show_entries',
      'format=format_name,duration,size:stream=codec_type,codec_name,pix_fmt,profile,width,height',
      // stderr ignored: a file ffprobe cannot open writes its complaint there,
      // and it is reported properly below as `unreadable` rather than spilling
      // into the middle of the survey.
      '-of', 'json', file], { encoding: 'utf8', maxBuffer: 1 << 22, stdio: ['ignore', 'pipe', 'ignore'] }));
    const s = j.streams ?? [];
    const v = s.find((x) => x.codec_type === 'video');
    const a = s.find((x) => x.codec_type === 'audio');
    return {
      container: j.format?.format_name ?? '',
      duration: Number(j.format?.duration) || null,
      bytes: Number(j.format?.size) || null,
      v: v ? { codec: v.codec_name, pix: v.pix_fmt, profile: (v.profile || '').toLowerCase(),
               w: v.width, h: v.height } : null,
      a: a ? { codec: a.codec_name, profile: (a.profile || '').toLowerCase() } : null,
    };
  } catch { return null; }
}

/* Is the moov atom before the mdat? Walking the top-level boxes is exact and
   costs one small read; the alternative is guessing from the extension, and a
   file that needs faststart looks identical to one that has it. */
function moovFirst(file) {
  let fd;
  try {
    fd = openSync(file, 'r');
    const buf = Buffer.alloc(16);
    let off = 0;
    for (let i = 0; i < 24; i++) {
      if (readSync(fd, buf, 0, 16, off) < 8) return false;
      let size = buf.readUInt32BE(0);
      const type = buf.toString('latin1', 4, 8);
      if (type === 'moov') return true;
      if (type === 'mdat') return false;
      if (size === 1) size = Number(buf.readBigUInt64BE(8));   // 64-bit extended size
      else if (size === 0) return false;                        // runs to EOF
      if (size < 8) return false;
      off += size;
    }
    return false;
  } catch { return false; }
  finally { if (fd !== undefined) closeSync(fd); }
}

/* conformant | remux | audio | video | encode | broken
 *
 * Six outcomes rather than four, because the real collection showed that
 * "needs re-encoding" hides three very different jobs. A clip with good H.264
 * and uncompressed PCM audio needs its AUDIO encoded and its picture copied
 * byte for byte — fast, and lossless where it counts. Collapsing that into
 * `encode` would re-compress a perfectly good picture to fix a soundtrack. */
function classify(file, m) {
  if (!m || !m.v || !(m.duration > 0)) return 'broken';
  const isMp4 = /mp4|mov|m4v/.test(m.container);
  const vOk = m.v.codec === 'h264' && OK_PIX.has(m.v.pix)
    && (!m.v.profile || OK_PROFILE.has(m.v.profile));
  const aOk = !m.a || (m.a.codec === 'aac' && !/he-aac|hev2|sbr/.test(m.a.profile));
  if (vOk && aOk) {
    // Everything right AND already an .mp4 with the index at the front.
    if (isMp4 && extname(file).toLowerCase() === '.mp4' && moovFirst(file)) return 'conformant';
    return 'remux';
  }
  if (vOk && !aOk) return 'audio';    // picture copied
  if (!vOk && aOk) return 'video';    // soundtrack copied
  return 'encode';
}

// ── survey ────────────────────────────────────────────────────────────────
const files = readdirSync(ROOT).filter((f) => VIDEO_RE.test(f)).sort();
console.log(`${plural(files.length, 'file')} under ${ROOT}\n`);
if (!files.length) process.exit(0);

const items = [];
process.stdout.write('probing…');
for (const f of files) {
  const abs = join(ROOT, f);
  const m = probe(abs);
  items.push({ f, abs, m, kind: classify(abs, m) });
}
process.stdout.write('\r        \r');

const by = (k) => items.filter((x) => x.kind === k);
const conformant = by('conformant'), remux = by('remux'), broken = by('broken');
const audioOnly = by('audio'), videoOnly = by('video'), encode = by('encode');
// Everything whose picture gets re-compressed — the only bucket that loses
// anything, and therefore the only number worth being nervous about.
const lossy = [...videoOnly, ...encode];
const secs = (arr) => arr.reduce((a, x) => a + (x.m?.duration ?? 0), 0);
const size = (arr) => arr.reduce((a, x) => a + (x.m?.bytes ?? 0), 0);

const shape = (arr) => {
  const m = new Map();
  for (const x of arr) {
    const k = `${x.m?.v?.codec ?? '?'}/${x.m?.a?.codec ?? 'none'} in ${(x.m?.container ?? '?').split(',')[0]}`
      + `  (.${extname(x.f).slice(1)})`;
    m.set(k, (m.get(k) ?? 0) + 1);
  }
  return [...m].sort((a, b) => b[1] - a[1]);
};

console.log(`\x1b[1malready right\x1b[0m   ${String(conformant.length).padStart(5)}   nothing to do`);
if (remux.length) {
  console.log(`\x1b[1mrepackage\x1b[0m       ${String(remux.length).padStart(5)}   H.264/AAC already — container swap, `
    + `\x1b[32mno quality loss\x1b[0m`);
  for (const [k, c] of shape(remux).slice(0, 6)) console.log(`                ${String(c).padStart(5)}   ${k}`);
}
if (audioOnly.length) {
  console.log(`\x1b[1maudio only\x1b[0m      ${String(audioOnly.length).padStart(5)}   picture copied untouched, `
    + `\x1b[32mno visual loss\x1b[0m`);
  for (const [k, c] of shape(audioOnly).slice(0, 6)) console.log(`                ${String(c).padStart(5)}   ${k}`);
}
if (videoOnly.length) {
  console.log(`\x1b[1mvideo only\x1b[0m      ${String(videoOnly.length).padStart(5)}   \x1b[33mpicture re-encoded\x1b[0m, soundtrack copied`);
  for (const [k, c] of shape(videoOnly).slice(0, 6)) console.log(`                ${String(c).padStart(5)}   ${k}`);
}
if (encode.length) {
  console.log(`\x1b[1mboth streams\x1b[0m    ${String(encode.length).padStart(5)}   \x1b[33mlossy, and it replaces the original\x1b[0m`);
  for (const [k, c] of shape(encode).slice(0, 6)) console.log(`                ${String(c).padStart(5)}   ${k}`);
}
if (broken.length) {
  console.log(`\x1b[1munreadable\x1b[0m      ${String(broken.length).padStart(5)}   skipped — ffprobe cannot open these`);
  for (const x of broken.slice(0, 10)) console.log(`                        ${x.f}`);
}

// Collisions: x.webm -> x.mp4 would clobber an existing x.mp4.
const stems = new Map();
for (const f of files) {
  const s = basename(f, extname(f));
  if (!stems.has(s)) stems.set(s, []);
  stems.get(s).push(f);
}
const clashes = [...stems].filter(([, v]) => v.length > 1);
if (clashes.length) {
  console.log(`\n\x1b[31m${plural(clashes.length, 'name clash')}\x1b[0m — two files share a stem, so one would `
    + `overwrite the other:`);
  for (const [s, v] of clashes.slice(0, 10)) console.log(`   ${s}: ${v.join(', ')}`);
  console.log('   rename one of each pair first. Nothing will be written while these exist.');
}

// ── the cost ──────────────────────────────────────────────────────────────
/* ~3x realtime per core at preset medium on ordinary hardware. It is an
   estimate and it is stated as one — the point is to answer "is this an
   afternoon or a week" before anything is destroyed, not to be exact. */
const RATE = 3.0;
const cheap = [...remux, ...audioOnly];
const eta = secs(lossy) / RATE / JOBS + secs(cheap) * 0.03 / JOBS;
console.log(`\n${hms(secs(items))} of video, ${gb(size(items))} on disk`);
if (cheap.length) {
  console.log(`${plural(cheap.length, 'file')} copied through in seconds each`);
}
if (lossy.length) {
  console.log(`${plural(lossy.length, 'picture')} re-encoded — ${hms(secs(lossy))} of video at roughly `
    + `${RATE}x realtime on ${JOBS} job${JOBS === 1 ? '' : 's'}`);
}
console.log(`estimated total: \x1b[1m~${hms(Math.max(eta, 1))}\x1b[0m`);
const willWrite = cheap.length + lossy.length;
if (!REPLACE && willWrite) {
  console.log(`originals kept in ${basename(KEEP)}/ — about ${gb(size(cheap) + size(lossy))} more on disk`);
}

if (!APPLY) {
  console.log(`\nNothing written. Add \x1b[1m--apply\x1b[0m to do it`
    + `${REPLACE ? '' : ', or --apply --replace to discard originals'}.`);
  if (lossy.length) {
    console.log(`\n\x1b[33mBefore you do:\x1b[0m ${plural(lossy.length, 'clip')} would have its picture`);
    console.log(`re-encoded, which is lossy and permanent. If these are the only copies,`);
    console.log(`keep the originals (the default) until you have watched a few results.`);
  } else if (willWrite) {
    console.log(`\n\x1b[32mNothing here loses quality\x1b[0m — every file is a stream copy.`);
  }
  process.exit(0);
}
if (clashes.length) { console.error('\nrefusing to run with name clashes outstanding.'); process.exit(1); }

// ── apply ─────────────────────────────────────────────────────────────────
// Cheapest first, so an interrupted run has already banked the free wins.
const todo = [...remux, ...audioOnly, ...videoOnly, ...encode].slice(0, LIMIT);
if (!todo.length) { console.log('\nnothing to do.'); process.exit(0); }
if (!REPLACE) mkdirSync(KEEP, { recursive: true });

const run = (args) => new Promise((res) => {
  execFile('ffmpeg', args, { timeout: 3600000, maxBuffer: 1 << 22 },
    (err, _o, stderr) => res(err ? (String(stderr).trim().split('\n').pop() || 'ffmpeg failed') : null));
});

async function one(x) {
  const stem = basename(x.f, extname(x.f));
  const out = join(ROOT, `${stem}.mp4`);
  /* Written beside the target and renamed into place. A crash mid-encode then
     leaves a .part nobody reads, never a half-written file where a working
     clip used to be — and rename within one directory is atomic. */
  const tmp = join(ROOT, `.${stem}.part.mp4`);
  rmSync(tmp, { force: true });

  /* Per stream, always. `remux` copies both; `audio` copies the picture and
     re-encodes the soundtrack; `video` does the reverse; `encode` does both.
     Deciding stream by stream rather than file by file is what keeps a good
     picture out of the encoder when only the audio is wrong. */
  const encV = x.kind === 'video' || x.kind === 'encode';
  const encA = x.kind === 'audio' || x.kind === 'encode';
  const args = ['-loglevel', 'error', '-y', '-i', x.abs];
  if (encV) {
    args.push('-c:v', 'libx264', '-preset', PRESET, '-crf', CRF,
      '-pix_fmt', 'yuv420p', '-profile:v', 'high', '-level', '4.1',
      // Odd dimensions are legal in VP9 and rejected by H.264. Rounding down
      // to even loses at most one row or column and is the difference between
      // a clip converting and a clip failing.
      '-vf', 'scale=trunc(iw/2)*2:trunc(ih/2)*2');
  } else args.push('-c:v', 'copy');
  if (!x.m.a) args.push('-an');
  else if (encA) args.push('-c:a', 'aac', '-b:a', '160k', '-ac', '2');
  else args.push('-c:a', 'copy');
  args.push('-map_metadata', '-1', '-movflags', '+faststart', tmp);

  const err = await run(args);
  if (err) { rmSync(tmp, { force: true }); return { x, err }; }

  // Verify BEFORE destroying anything: a file that does not probe, or that
  // lost time, does not get to replace its own source.
  const m2 = probe(tmp);
  if (!m2 || !m2.v || !(m2.duration > 0)) { rmSync(tmp, { force: true }); return { x, err: 'output will not probe' }; }
  const drift = Math.abs(m2.duration - (x.m.duration ?? 0));
  if (drift > Math.max(0.5, (x.m.duration ?? 0) * 0.02)) {
    rmSync(tmp, { force: true });
    return { x, err: `duration moved ${drift.toFixed(1)}s (${x.m.duration?.toFixed(1)} -> ${m2.duration.toFixed(1)})` };
  }
  if (!moovFirst(tmp)) { rmSync(tmp, { force: true }); return { x, err: 'moov atom is not first' }; }

  // Source out of the way first, so the rename never lands on a live file.
  if (REPLACE) rmSync(x.abs, { force: true });
  else renameSync(x.abs, join(KEEP, x.f));
  renameSync(tmp, out);
  return { x, err: null, bytes: m2.bytes ?? 0, was: x.m.bytes ?? 0 };
}

console.log(`\nwriting ${plural(todo.length, 'file')}, ${JOBS} at a time…`);
const t0 = Date.now();
let done = 0, failed = 0, wrote = 0, before = 0;
const errors = [];
let i = 0;
await Promise.all(Array.from({ length: Math.min(JOBS, todo.length) }, async () => {
  while (i < todo.length) {
    const x = todo[i++];
    const r = await one(x);
    done++;
    if (r.err) { failed++; errors.push([x.f, r.err]); }
    else { wrote += r.bytes; before += r.was; }
    const el = (Date.now() - t0) / 1000;
    const left = (todo.length - done) * (el / done);
    process.stdout.write(`\r  ${done}/${todo.length}  ${hms(el)} elapsed, ~${hms(left)} left`.padEnd(58));
  }
}));
process.stdout.write(`\r${' '.repeat(58)}\r`);

console.log(`${plural(done - failed, 'file')} rewritten in ${hms((Date.now() - t0) / 1000)}`);
if (before) {
  const d = ((wrote - before) / before) * 100;
  console.log(`size ${gb(before)} -> ${gb(wrote)} (${d >= 0 ? '+' : ''}${d.toFixed(0)}%)`);
}
if (failed) {
  console.log(`\n\x1b[31m${plural(failed, 'failure')}\x1b[0m — originals untouched in every case:`);
  for (const [f, e] of errors.slice(0, 20)) console.log(`  ${f}: ${e}`);
}
if (!REPLACE) console.log(`\noriginals in ${KEEP}`);

console.log(`\nNow let the archive catch up — inside the container, where the database lives:`);
console.log(`  docker exec -it ragekitsu node scripts/import-snippets.js --update`);
console.log(`  docker exec -it ragekitsu node scripts/check-media.js`);
console.log(`\n--update re-probes every file and rewrites codecs, sizes and video_path,`);
console.log(`so the rows follow the rename from .webm to .mp4 on their own.`);
