// Measure the local files and write what ffprobe says into the database.
//
//   node scripts/probe-media.js [--db data/archive.db] [--root /path/to/media]
//                              [--limit N] [--reprobe] [--dry]
//
// Why this has to run before the theater does anything useful:
//
//   file_duration_s is NULL on every capture in the archive. That is not a gap
//   in a nice-to-have column — it is why recompute()'s duration derivation has
//   never produced a value and why vod_state can never reach 'truncated'. It is
//   also why 30 streams are marked `present` with no known length: a video you
//   can watch and a timeline that cannot be drawn.
//
// Everything written here is OBSERVATION. Re-running re-measures; nothing a
// human decided is touched, because none of these columns are decisions.
//
// The one soft value is local_start_wall. The recorder is the only thing that
// can ever know exactly when it started writing, and it was never asked. What
// is recoverable is mtime - duration, which is right to within a second when
// the file has been left alone and wrong by months when it has been copied or
// when yt-dlp stamped it with the upload time. So it is written WITH a
// precision, low, and any better value already present is left alone.

import { spawnSync } from 'node:child_process';
import { statSync } from 'node:fs';
import { create, isArchive, now, resolveDbPath } from '../db.js';
import { recompute, resolveMedia } from '../archive.js';

const argv = process.argv.slice(2);
const flag = (name, fallback = null) => {
  const i = argv.indexOf(`--${name}`);
  return i === -1 ? fallback : (argv[i + 1] ?? true);
};
const has = (name) => argv.includes(`--${name}`);

const DB = flag('db', process.env.TENMA_DB ?? 'data/archive.db');
const ROOT = flag('root', process.env.TENMA_MEDIA_ROOT ?? null);
const LIMIT = Number(flag('limit', 0)) || 0;
const REPROBE = has('reprobe');
const DRY = has('dry');

if (!ROOT) {
  console.error('no media root — pass --root or set TENMA_MEDIA_ROOT.');
  console.error('Without it there is nothing to measure and every capture would');
  console.error('read `unverified`, which is the correct answer, not a failure.');
  process.exit(1);
}

const probe = spawnSync('ffprobe', ['-version'], { encoding: 'utf8' });
if (probe.error) {
  console.error('ffprobe is not on PATH.');
  process.exit(1);
}

/** ffprobe one file. Returns null rather than throwing: an unreadable file is a
 *  fact about the archive, not a reason to abandon the other 370. */
function ffprobe(path) {
  const r = spawnSync('ffprobe', [
    '-v', 'error', '-show_format', '-show_streams', '-of', 'json', path,
  ], { encoding: 'utf8', maxBuffer: 32 * 1024 * 1024 });
  if (r.status !== 0 || !r.stdout) return null;
  try { return JSON.parse(r.stdout); } catch { return null; }
}

/** "30000/1001" -> 29.97. Returns null for the 0/0 that still images report. */
function rate(x) {
  if (!x || typeof x !== 'string') return null;
  const [n, d] = x.split('/').map(Number);
  if (!n || !d) return null;
  return Math.round((n / d) * 1000) / 1000;
}

if (!isArchive(DB)) {
  console.error(`no archive at ${resolveDbPath(DB)} — refusing to create one`);
  process.exit(1);
}

const db = create(DB);
console.log(`db    ${resolveDbPath(DB)}`);
console.log(`root  ${ROOT}`);
console.log(DRY ? 'mode  DRY RUN — nothing will be written\n' : '');

const caps = db.prepare(
  `SELECT id, stream_id, platform, video_path, local_start_wall, probed_at
   FROM capture WHERE video_path IS NOT NULL
   ${REPROBE ? '' : 'AND probed_at IS NULL'}
   ORDER BY id ${LIMIT ? 'LIMIT ' + LIMIT : ''}`).all();

console.log(`${caps.length} capture(s) to probe\n`);

const streams = new Set();
let ok = 0, missing = 0, unreadable = 0, clocked = 0;

for (const c of caps) {
  const path = resolveMedia(ROOT, c.video_path);
  if (!path) {
    missing++;
    console.log(`  miss  ${c.platform} ${c.video_path?.slice(0, 68)}`);
    continue;
  }
  const info = ffprobe(path);
  if (!info) {
    unreadable++;
    console.log(`  bad   ${c.platform} ${c.video_path?.slice(0, 68)}`);
    continue;
  }

  const v = (info.streams ?? []).find((s) => s.codec_type === 'video') ?? {};
  const a = (info.streams ?? []).find((s) => s.codec_type === 'audio') ?? null;
  const dur = Number(info.format?.duration);
  const duration = Number.isFinite(dur) ? Math.round(dur) : null;

  // Codecs are not decoration: they are what decides whether the `local` source
  // can be a plain <video src> or needs transcoding. Finding that out in the
  // browser is the expensive way.
  const cols = {
    file_duration_s: duration,
    container: info.format?.format_name ?? null,
    video_codec: v.codec_name ?? null,
    audio_codec: a?.codec_name ?? null,
    width: v.width ?? null,
    height: v.height ?? null,
    fps: rate(v.avg_frame_rate) ?? rate(v.r_frame_rate),
    has_audio: a ? 1 : 0,
    video_bytes: Number(info.format?.size) || null,
    probed_at: now(),
  };

  // The fourth clock, recovered rather than recorded. Only ever filled in when
  // it is empty — a value the recorder supplied is always better than this one.
  if (c.local_start_wall === null && duration !== null) {
    try {
      const mtime = Math.floor(statSync(path).mtimeMs / 1000);
      cols.local_start_wall = mtime - duration;
      // Low confidence on purpose. A plain `cp` rewrites mtime; yt-dlp stamps
      // the upload time unless --no-mtime was passed. Half an hour of slack
      // says "this is a seed, correct it" rather than pretending to a second.
      cols.local_start_precision_s = 1800;
      clocked++;
    } catch { /* the stat that just succeeded can still fail; skip the clock */ }
  }

  if (!DRY) {
    const keys = Object.keys(cols);
    db.prepare(`UPDATE capture SET ${keys.map((k) => `${k}=?`).join(',')},
                updated_at=? WHERE id=?`)
      .run(...keys.map((k) => cols[k]), now(), c.id);
  }
  streams.add(c.stream_id);
  ok++;
  const hms = duration === null ? '  ?  '
    : `${String(Math.floor(duration / 3600)).padStart(2, '0')}:` +
      `${String(Math.floor(duration % 3600 / 60)).padStart(2, '0')}`;
  console.log(`  ok    ${c.platform} ${hms} ${cols.video_codec ?? '?'}/` +
              `${cols.audio_codec ?? '-'} ${cols.width ?? '?'}x${cols.height ?? '?'}`);
}

// Recompute once per stream, not once per capture: duration derives from all of
// them together, and the timeline is projected from the result.
if (!DRY && streams.size) {
  console.log(`\nrecomputing ${streams.size} stream(s)…`);
  for (const sid of streams) recompute(db, sid, { mediaRoot: ROOT });
}

console.log(`\nprobed ${ok}   missing ${missing}   unreadable ${unreadable}` +
            `   local clock seeded ${clocked}`);

const left = db.prepare('SELECT COUNT(*) c FROM capture WHERE probed_at IS NULL').get().c;
const nodur = db.prepare(
  'SELECT COUNT(*) c FROM stream WHERE duration_s IS NULL AND retracted_at IS NULL').get().c;
console.log(`unprobed captures remaining ${left}   streams still without a duration ${nodur}`);
db.close();
