// The archive's logic: everything computed rather than stated, and the single
// write path.
//
// Three halves, which is one more than there used to be:
//   clocks   — the conversion every other part goes through
//   derive   — refresh a stream from its captures; project the timeline
//   changes  — propose, review and apply changesets
//
// A decision only ever changes through an applied changeset. An editor's own
// applies on submission; that is the only difference between a staff edit and a
// viewer's suggestion, and it is what keeps the history complete.

import { execFileSync } from 'node:child_process';
import { closeSync, openSync, readSync, statSync } from 'node:fs';
import { extname, resolve, sep } from 'node:path';
import { bit, bumpGeneration, isUlid, now, slugify, tx, ulid } from './db.js';
/* One import, one function. auth.js imports only db.js, so this direction is
   the safe one — archive.js → auth.js → db.js, no cycle. It is here rather
   than at the routes because a permission that lives at the route is a
   permission each new route has to remember. */
import { can } from './auth.js';

// ===========================================================================
//  clocks
//
//  There is no single broadcast clock. YouTube and Twitch disagree about when
//  the broadcast began, and the recorder disagrees with both about when it
//  started writing — four origins for one stream, five once an archive-channel
//  mirror lands. The only frame all of them share is wall-clock time.
//
//  So: every position is a pair, (which clock, how far in). Converting between
//  two clocks means going through wall time. That is the entire model, and
//  every function below is a restatement of it.
//
//    axis      seconds since stream.started_at — what the timeline draws
//    wall      absolute unix seconds — what clocks are compared in
//    position  seconds into one particular player or file
// ===========================================================================

/** A capture's two clock origins, as absolute wall times.
 *
 *  `remote_start_wall` falls back to the pre-migration reading of offset_s so
 *  a database mid-migration still answers correctly rather than answering zero. */
export function clocksOf(cap, streamStartedAt) {
  return {
    remote: cap.remote_start_wall ?? (streamStartedAt + (cap.offset_s ?? 0)),
    local: cap.local_start_wall ?? null,
  };
}

/** Position on one of a capture's clocks -> axis seconds. Null when that clock
 *  has never been measured — refused rather than approximated. */
export function positionToAxis(cap, streamStartedAt, clock, t) {
  if (t === null || t === undefined) return null;
  const zero = clocksOf(cap, streamStartedAt)[clock === 'local' ? 'local' : 'remote'];
  return zero === null ? null : (zero + t) - streamStartedAt;
}

/** Axis seconds -> position on one of a capture's clocks. The inverse. */
export function axisToPosition(cap, streamStartedAt, clock, axis) {
  if (axis === null || axis === undefined) return null;
  const zero = clocksOf(cap, streamStartedAt)[clock === 'local' ? 'local' : 'remote'];
  return zero === null ? null : (axis + streamStartedAt) - zero;
}

/** Where a note or segment sits on the axis.
 *
 *  frame='capture'  measured inside anchor_id on anchor_clock. Convert.
 *  frame='stream'   already on the axis.
 *  frame='unknown'  already on the axis *numerically*, but nobody recorded what
 *                   it was measured against. Returned, and flagged, never
 *                   quietly promoted to exact.
 *
 *  A row carrying an anchor is treated as anchored even if `frame` was never
 *  set, so a changeset that supplies anchor_id and forgets `frame` still means
 *  what its author meant. */
export function axisOf(row, capsById, streamStartedAt, field = 'offset_s') {
  const t = row[field];
  if (t === null || t === undefined) return null;
  if (row.anchor_id && row.frame !== 'stream') {
    const cap = capsById.get(row.anchor_id);
    if (!cap) return null;              // anchor gone: refuse, do not guess
    return positionToAxis(cap, streamStartedAt, row.anchor_clock, t);
  }
  return t;
}

/** Is this position exact, or is it the vault's best recollection? */
export const isExact = (row) => row.frame === 'capture' || row.frame === 'stream';

// ===========================================================================
//  derive
// ===========================================================================

// A capture shorter than this fraction of the stream is truncated, not present.
const TRUNCATE_RATIO = 0.97;

/** Vault-relative path -> a real path on this machine, or null.
 *
 *  The archive stores NFC; NTFS stores whatever yt-dlp wrote and macOS stores
 *  NFD, so an accented or Japanese title can sit on disk under a different byte
 *  sequence. Try each normalisation before concluding a capture is missing —
 *  the failure mode is a silent "lost", which is the worst way to lose this
 *  particular signal.
 *
 *  Containment is checked HERE rather than at the routes, because the routes
 *  are not all alike and the difference is invisible from the call site.
 *  `/media/video/:capture_id` looks a path up by id, so its argument is
 *  trusted; `/media/thumb/:rest(*)` takes the path straight off the URL, and
 *  express decodes route params AFTER path resolution — so `..%2F` survives
 *  where a bare `../` is normalised away, and `join()` walks straight out of
 *  the root. Resolving and comparing is the same check `/api/media/browse`
 *  already does; putting it in the one function every caller goes through
 *  means the next route to be added inherits it instead of forgetting it.
 *  statMedia() is a caller too, and its paths come from ingest, which builds
 *  them from a config-supplied prefix. */
/** What ffprobe says is inside a file, or null if it is not media at all.
 *
 *  The scripts each carry their own copy of this and each swallows the error
 *  into an empty record, which is right for a bulk pass over files that are
 *  already in the archive: one unreadable clip should not stop fifteen hundred.
 *  It is wrong at the front door. Here "ffprobe could not read it" is the
 *  answer — a cheap, early rejection before anything expensive runs — so this
 *  one returns null and the caller refuses.
 *
 *  Reproduced in review.md: the lazy polyglot, HTML prepended to a real webm,
 *  dies here with `EBML header parsing failed`. A serious polyglot is still
 *  craftable, so this is a cost imposed on an attacker rather than a proof.
 */
export function probeMedia(file) {
  try {
    const out = execFileSync('ffprobe', [
      '-v', 'error',
      '-show_entries',
      // pix_fmt and profile are not curiosities: 10-bit H.264 and HE-AAC are
      // both legal, both common out of a phone or yt-dlp, and neither plays in
      // Safari. A probe that stops at the codec name calls those conformant.
      'format=duration,size,format_name'
      + ':stream=codec_type,codec_name,width,height,pix_fmt,profile,avg_frame_rate',
      '-of', 'json', file],
      { encoding: 'utf8', timeout: 20_000, maxBuffer: 4 << 20 });
    const j = JSON.parse(out);
    const streams = j.streams ?? [];
    const v = streams.find((s) => s.codec_type === 'video') ?? {};
    const a = streams.find((s) => s.codec_type === 'audio') ?? {};
    // No format block means ffprobe read the file and found nothing it knew.
    if (!j.format) return null;
    return {
      duration_s: Number(j.format.duration) || null,
      bytes: Number(j.format.size) || null,
      width: v.width ?? null,
      height: v.height ?? null,
      container: j.format.format_name ?? null,
      video_codec: v.codec_name ?? null,
      audio_codec: a.codec_name ?? null,
      // Not columns on `snippet` — nothing stores these. They exist for
      // classifyMedia(), which is asked the question once per upload.
      pix_fmt: v.pix_fmt ?? null,
      profile: v.profile ?? null,
      audio_profile: a.profile ?? null,
      /* "30000/1001" and friends. Needed to reason about bitrate at all: bits
         per second means nothing without knowing how many pixels a second it
         is buying. A rate of 0/0 is what ffprobe says for a still or a stream
         it could not measure, and null is the honest answer there. */
      fps: (() => {
        const [n, d] = String(v.avg_frame_rate ?? '').split('/').map(Number);
        return n > 0 && d > 0 ? n / d : null;
      })(),
    };
  } catch { return null; }
}


/* ── what a file needs doing to it ─────────────────────────────────────────
 *
 * The target, and every part of it earns its place:
 *
 *   H.264 High, yuv420p, + AAC-LC, in MP4, moov atom first.
 *
 * yuv420p because 4:2:2, 4:4:4 and 10-bit H.264 are all legal and none of them
 * play in Safari. AAC-LC rather than HE-AAC for the same reason. moov first
 * because otherwise the whole file must arrive before the first frame shows,
 * which breaks progressive playback and makes a link preview hang. MP4 rather
 * than WebM because a Discord embed is a requirement with one answer.
 *
 * The rules were worked out offline against the real collection, by a bulk
 * pass that ran on a PC over SMB and took no database with it. That pass has
 * been run and its script is gone; this is the surviving copy of what it
 * learned, and the numbers below are the answer it arrived at rather than a
 * first guess. If a bulk pass is ever needed again, it starts from here.
 */
const OK_PIX = new Set(['yuv420p', 'yuvj420p']);
const OK_PROFILE = new Set(['baseline', 'constrained baseline', 'main', 'high']);

/** Is the moov atom before the mdat?
 *
 *  Walking the top-level boxes is exact and costs one 16-byte read per box;
 *  the alternative is guessing from the extension, and a file that needs
 *  faststart looks identical to one that already has it.
 */
export function moovFirst(file) {
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
      if (size === 1) size = Number(buf.readBigUInt64BE(8));   // 64-bit extended
      else if (size === 0) return false;                        // runs to EOF
      if (size < 8) return false;
      off += size;
    }
    return false;
  } catch { return false; }
  finally { if (fd !== undefined) closeSync(fd); }
}

/** conformant | remux | audio | video | encode | broken
 *
 *  Six outcomes rather than four, because the real collection showed that
 *  "needs re-encoding" hides three very different jobs. A clip with good H.264
 *  and uncompressed PCM audio needs its AUDIO encoded and its picture copied
 *  byte for byte — fast, and lossless where it counts. Collapsing that into
 *  `encode` would re-compress a perfectly good picture to fix a soundtrack.
 *
 *  Takes a probeMedia() record plus the file's own path, because two of the
 *  six answers depend on things no probe reports: the extension, and where the
 *  moov atom sits.
 */
/* What ffprobe calls a picture, measured rather than guessed:
 *
 *     .png    png_pipe   / png     no duration
 *     .jpg    image2     / mjpeg   duration 0.04  ← one frame at 25fps
 *     .webp   webp_pipe  / webp    no duration
 *     .gif    gif        / gif     duration 1.0   ← animated, and not a still
 *
 * The JPEG line is why this is a FORMAT test and not a duration test. A jpeg
 * reports a fortieth of a second, so `duration > 0` calls it a video, and the
 * classifier below then finds mjpeg where it wanted h264 and schedules an
 * H.264 encode of a photograph. GIF goes the other way and is deliberately not
 * here: an animated gif is a moving picture, and snippet rows have handled
 * those since long before memes existed.
 *
 * An animated WebP lands here and is treated as a still, which shows its first
 * frame. Rare enough to accept, and the alternative is decoding every upload
 * to count frames. */
const STILL_FORMATS = new Set(['png_pipe', 'image2', 'jpeg_pipe', 'webp_pipe',
                               'bmp_pipe', 'tiff_pipe']);
const STILL_CODECS = new Set(['png', 'apng', 'mjpeg', 'webp', 'bmp', 'tiff']);

/** Is this file a single picture? Exported because the upload route decides
 *  what to do with it and the media route decides what to call it. */
export function isStill(p) {
  return !!p && STILL_FORMATS.has(String(p.container ?? '').toLowerCase())
    && STILL_CODECS.has(String(p.video_codec ?? '').toLowerCase())
    && p.width > 0 && p.height > 0;
}

/* The three collections one table holds, and the ONLY place the list is
   written. server.js imports it for the upload route and the list filter, and
   ENUMS below uses it to refuse a fourth — so a new collection is an edit to
   this line and to nothing that has to be found afterwards. */
export const SNIPPET_KINDS = ['snippet', 'meme', 'gallery'];

/** Which directory in the media tree each collection's masters live in.
 *
 *  The MEDIA tree only. Quarantine stays flat and deliberately so: a file is
 *  there for the minutes between an upload and a verdict, the names are minted
 *  ULIDs so nothing can collide, and the promote job carries both paths
 *  explicitly — so mirroring the layout there would buy nothing and cost a
 *  worker change. ls_jobs.resolve_name() takes the promote SOURCE with
 *  `bare=True`, which is to say a quarantine name with a directory in it is
 *  refused by the recorder before any of this is consulted.
 */
export const KIND_DIR = { snippet: 'snippets', meme: 'memes', gallery: 'gallery' };

/* ── the machines that read things ────────────────────────────────────────
 *
 * Three tasks, and for each one the models worth offering. A constant and not
 * a table: the `model` row records which one is CHOSEN, and that is a fact
 * about this archive; which models exist is a fact about the world, and
 * putting the second in the database means a migration every time a new build
 * ships.
 *
 * `bytes` is what the download costs, so the panel can say so before somebody
 * on a 2-core NAS with 8 GB commits to it. Approximate on purpose — it is
 * there to distinguish "a few hundred megabytes" from "several gigabytes",
 * and the sidecar reports the real figure once the file is on disk.
 *
 * Nothing here downloads anything. This is the menu.
 */
export const MODEL_TASKS = ['transcribe', 'ocr', 'search'];

export const MODEL_TASK = {
  transcribe: {
    label: 'Transcription',
    what: 'Speech in a clip, written down.',
    /* The one task that does not go to the sidecar. whisper.cpp is a single
       static binary the archive already spawns, and moving it would be a
       rewrite of something that works to gain a network hop. */
    runner: 'local',
    into: 'the transcript, line by line, with timings',
  },
  ocr: {
    label: 'Text in pictures',
    what: 'Words ON a meme — the part a search can match.',
    runner: 'sidecar',
    /* Deliberately the same column a clip's speech lands in. A caption field
       beside it would be two homes for one fact, searched by two indexes, and
       the first to drift is the one nothing indexes. */
    into: 'the transcript, as lines with no timings',
  },
  search: {
    label: 'Smart search',
    what: 'What a picture is OF, without anybody typing it.',
    runner: 'sidecar',
    into: 'an embedding beside the row — not built yet',
  },
};

export const MODEL_CATALOG = {
  transcribe: [
    { slug: 'whisper', name: 'whisper.cpp', bytes: null,
      note: 'Whichever GGUF TENMA_WHISPER_MODEL points at. Accented English is '
          + 'the constraint here, not speed.' },
  ],
  ocr: [
    { slug: 'PP-OCRv5_mobile', name: 'PP-OCRv5 mobile', bytes: 16 << 20,
      note: 'Chinese, Japanese and English in one model, and small enough to '
          + 'load and evict without thinking about it.' },
    { slug: 'PP-OCRv5_server', name: 'PP-OCRv5 server', bytes: 90 << 20,
      note: 'More accurate on small or angled text. Several times the work per '
          + 'picture, on a box with two cores.' },
  ],
  search: [
    { slug: 'ViT-B-32__openai', name: 'CLIP ViT-B/32', bytes: 350 << 20,
      note: 'The usual first choice: fast, 512-dimension embeddings, English '
          + 'prompts.' },
    { slug: 'ViT-B-16-SigLIP-384__webli', name: 'SigLIP ViT-B/16 384', bytes: 820 << 20,
      note: 'Noticeably better at finding things, and slower per picture. '
          + 'Changing model means re-embedding everything.' },
  ],
};

/** The catalogue entry for a chosen slug, or a stand-in describing it.
 *
 *  A stand-in rather than null, because an admin is allowed to type a model
 *  this file has never heard of — the sidecar is what decides whether it
 *  exists, and a panel that refused to draw an unknown name would be the
 *  archive overruling the thing that actually knows.
 */
export function modelInfo(task, slug) {
  const hit = (MODEL_CATALOG[task] ?? []).find((m) => m.slug === slug);
  return hit ?? { slug, name: slug, bytes: null,
                  note: 'Not one this archive knows about — the sidecar decides.' };
}

export function classifyMedia(file, p) {
  if (!p) return 'broken';
  /* Before the duration test, not after — see STILL_FORMATS above for the
     jpeg that would otherwise be sent off to be re-encoded as video. A still
     needs no normalize pass at all: there is no container to remux, no
     soundtrack to fix and no moov atom to move. The bytes that arrived are the
     bytes that get served. */
  if (isStill(p)) return 'still';
  if (!(p.duration_s > 0)) return 'broken';

  /* Audio with no picture. A clip of somebody saying something is a perfectly
     good archive entry — arguably the PUREST one, since the transcript is what
     most of these are searched by — and it was being refused at the door
     because the classifier began by insisting on a video stream.

     Two outcomes, matching the video ones: already-AAC copies, everything else
     encodes. Both produce an .m4a, so what gets served is one format whatever
     arrived. A folder-walking bulk pass never sees these at all — it filters
     on video extensions — which is why this branch had to be written here and
     could not be brought over from one. */
  if (!p.video_codec) {
    if (!p.audio_codec) return 'broken';
    const aOk = p.audio_codec === 'aac'
      && !/he-aac|hev2|sbr/.test(String(p.audio_profile ?? '').toLowerCase());
    return aOk ? 'sound' : 'sound-encode';
  }
  const container = String(p.container ?? '');
  const isMp4 = /mp4|mov|m4v/.test(container);
  const vOk = p.video_codec === 'h264'
    && (!p.pix_fmt || OK_PIX.has(p.pix_fmt))
    && (!p.profile || OK_PROFILE.has(String(p.profile).toLowerCase()));
  const aOk = !p.audio_codec
    || (p.audio_codec === 'aac' && !/he-aac|hev2|sbr/.test(String(p.audio_profile ?? '').toLowerCase()));
  if (vOk && aOk) {
    if (isMp4 && extname(file).toLowerCase() === '.mp4' && moovFirst(file)) return 'conformant';
    return 'remux';
  }
  if (vOk && !aOk) return 'audio';    // picture copied byte for byte
  if (!vOk && aOk) return 'video';    // soundtrack copied
  return 'encode';
}

/** Is this file carrying far more bits than the target encode would?
 *
 *  classifyMedia only ever asks about FORMAT, and a Premiere export is a
 *  perfectly formed H.264/AAC MP4 that happens to be four times the size it
 *  needs to be. Editors export at "match source" or a fixed 40-50 Mbps by
 *  default, and nothing about the file says so — the codec is right, the
 *  pixel format is right, the moov atom is in front.
 *
 *  Bits per pixel per frame, because bitrate alone is meaningless across
 *  resolutions: 8 Mbps is generous for 720p30 and thin for 4K60. For real
 *  content x264 at CRF 21 lands around 0.03-0.10; a high-motion 1080p60 game
 *  capture at 12 Mbps is about 0.10. The 0.15 default is therefore roughly
 *  double a generous honest encode, which is a threshold only an over-fat
 *  export clears.
 *
 *  It only decides whether re-encoding is worth ATTEMPTING. Whether the
 *  attempt is kept is decided afterwards by measuring the result, which is
 *  what makes a wrong answer here cost CPU rather than quality: an incompress-
 *  ible source simply fails to shrink and the original is kept.
 */
export function overBitrate(p, { bpp = 0.15, minBytes = 8 << 20 } = {}) {
  if (!p || !p.bytes || !(p.duration_s > 0)) return false;
  if (!p.width || !p.height || !p.fps) return false;
  /* A floor in absolute bytes, and it is not a micro-optimisation — it is what
     makes the ratio safe to use at all. Bits per pixel is resolution-dependent
     in a way the threshold cannot capture: per-frame overhead is a much larger
     share of a 320x240 frame than a 1080p one, so a perfectly ordinary small
     clip sits around 0.25 while an honest 1080p encode sits at 0.05. Measured
     on the fixtures, every one of the small ones read as over-fat.

     Gating on size instead of arguing about the ratio is also the honest
     framing of what this feature is for: reclaiming real disk and real
     bandwidth. Shaving 40 KB off a 300 KB clip is not worth an encode, and the
     case this exists for is a 190 MB export. */
  if (p.bytes < minBytes) return false;
  const bits = (p.bytes * 8) / p.duration_s;
  return bits / (p.width * p.height * p.fps) > bpp;
}

/** The ffmpeg arguments for one clip, given what classifyMedia said.
 *
 *  Per STREAM, always: deciding file by file is what puts a good picture
 *  through the encoder to fix a soundtrack. `-map_metadata -1` because an
 *  uploaded file's metadata is somebody else's — camera model, GPS, the
 *  original filename, occasionally their real name — and none of it should
 *  survive into something the archive serves.
 */
export function normalizeArgs(input, output, kind, {
  crf = 21, preset = 'veryfast', threads = 2, maxW = 1920, maxH = 1080, hasAudio = true,
} = {}) {
  const args = ['-nostdin', '-loglevel', 'error', '-y', '-i', input];

  /* No picture at all. `-vn` and not merely "copy no video stream": an mp3
     with embedded cover art has a video stream of one still frame, and left
     alone ffmpeg will faithfully carry it into the m4a as a video track,
     which makes every downstream `is this audio` check wrong. */
  if (kind === 'sound' || kind === 'sound-encode') {
    args.push('-vn');
    if (kind === 'sound') args.push('-c:a', 'copy');
    else args.push('-c:a', 'aac', '-b:a', '192k');
    args.push('-map_metadata', '-1', '-movflags', '+faststart', output);
    return args;
  }

  const encV = kind === 'video' || kind === 'encode';
  const encA = kind === 'audio' || kind === 'encode';
  if (encV) {
    args.push(
      '-c:v', 'libx264', '-preset', preset, '-crf', String(crf),
      '-pix_fmt', 'yuv420p', '-profile:v', 'high', '-level', '4.1',
      '-threads', String(threads),
      /* A ceiling, not a resize. `min(iw,W)` keeps a 640x480 clip at 640x480
         rather than blowing it up to fill the box, which plain
         force_original_aspect_ratio=decrease would do. The second scale
         rounds to even numbers: odd dimensions are legal in VP9 and rejected
         outright by H.264, and losing one row is the difference between a clip
         converting and a clip failing. Written as two filters rather than
         force_divisible_by so it works on ffmpeg before 4.4. */
      '-vf', `scale='min(iw,${maxW})':'min(ih,${maxH})':force_original_aspect_ratio=decrease`
           + `,scale=trunc(iw/2)*2:trunc(ih/2)*2`);
  } else args.push('-c:v', 'copy');
  if (!hasAudio) args.push('-an');
  else if (encA) args.push('-c:a', 'aac', '-b:a', '160k', '-ac', '2');
  else args.push('-c:a', 'copy');
  args.push('-map_metadata', '-1', '-movflags', '+faststart', output);
  return args;
}

/** One frame, as a JPEG. A second in, because frame 0 of a clip cut from a
 *  stream is very often a hard cut or a black frame — the worst available
 *  choice for a thumbnail. Clamped for clips shorter than that.
 */

/** ffmpeg arguments that dump a clip as raw mono PCM.
 *
 *  8 kHz is deliberate and deliberately low: this is measured to draw about
 *  480 bars, so anything above a few hundred samples per bar is thrown away
 *  immediately. It makes ten minutes of audio 9.6 MB instead of 106, which is
 *  the difference between a temp file nobody notices and one that matters.
 */
export function pcmArgs(input, output) {
  return ['-nostdin', '-loglevel', 'error', '-y', '-i', input,
          '-vn', '-ac', '1', '-ar', '8000', '-f', 's16le', output];
}

/** Signed 16-bit mono PCM -> one 0-255 amplitude per bar.
 *
 *  The peak of each bucket, not the mean. A mean turns speech into a flat
 *  sausage, because silence between words drags every bucket down; the peak is
 *  what the eye reads as "this is where the loud bit is", and it is what every
 *  audio editor draws.
 *
 *  Normalised so the loudest bar is full height. A quiet phone recording and a
 *  hot Discord clip both want to look like a waveform rather than one looking
 *  like a flat line — absolute loudness is not what anybody is scrubbing by.
 */
export function peaksFromPcm(buf, buckets = 480) {
  const n = Math.floor(buf.length / 2);
  if (!n) return null;
  const per = Math.max(1, Math.floor(n / buckets));
  /* Int32Array and not the Uint8Array we return. A 16-bit sample peaks at
     32767, so writing it into a byte array before normalising truncates it mod
     256 — which silently turns the loudest bar into whatever 32767 & 255
     happens to be, and then scales everything against that. The output is a
     waveform of noise that looks almost plausible. */
  const raw = new Int32Array(buckets);
  let max = 1;
  for (let b = 0; b < buckets; b++) {
    const start = b * per;
    if (start >= n) break;
    const end = Math.min(start + per, n);
    let peak = 0;
    /* Stride, rather than reading every sample. At 8 kHz a ten-minute clip is
       4.8 million samples for 480 bars; sampling about 400 per bar finds the
       same peak to within a hair and costs a hundredth of the work. */
    const step = Math.max(1, Math.floor((end - start) / 400));
    for (let i = start; i < end; i += step) {
      const v = Math.abs(buf.readInt16LE(i * 2));
      if (v > peak) peak = v;
    }
    raw[b] = peak;
    if (peak > max) max = peak;
  }
  const out = new Uint8Array(buckets);
  for (let b = 0; b < buckets; b++) out[b] = Math.round((raw[b] / max) * 255);
  return out;
}

/** A waveform, for a clip that has no frames to take a still from.
 *
 *  Not decoration. Every row in the list is a rectangle with a picture in it,
 *  and an audio clip with no poster is a black hole in the grid that reads as
 *  a broken thumbnail rather than as "this one is sound". A waveform also says
 *  something true about the clip — where the loud part is.
 */
export function wavePosterArgs(input, output) {
  return ['-nostdin', '-loglevel', 'error', '-y', '-i', input,
          '-filter_complex',
          /* One band, edge to edge. An earlier version padded to add margins
             and the pad colour did not match showwavespic's own transparent
             background once flattened, so the still came out as a black box
             inside a slightly-less-black box. Full bleed has no seam to
             mismatch. */
          'showwavespic=s=960x280:colors=0xff4d94|0x8a7fff,format=yuv420p',
          '-frames:v', '1', output];
}

/** The thumbnail for a picture, which is a different problem from the still
 *  for a clip.
 *
 *  posterArgs bounds the WIDTH, because every clip it has ever been handed was
 *  wider than it was tall. A screenshot of a chat log is 600 x 3000, and
 *  bounding its width alone produces a 960 x 4800 JPEG — a thumbnail heavier
 *  than the picture it stands in for, on the one collection built to show
 *  hundreds of them at once.
 *
 *  So: fitted inside a box, not scaled to a side. `decrease` never enlarges,
 *  which matters because a reaction face is often 200 px and blowing it up to
 *  960 would cost bytes to make it look worse.
 *
 *  Deliberately NOT square, though the grid draws squares. Cropping here would
 *  throw away the part of the picture the hover is meant to reveal, and a
 *  square tile is one line of `object-fit: cover` in the panel that wants one.
 */
export function stillPosterArgs(input, output, box = 960) {
  return ['-nostdin', '-loglevel', 'error', '-y', '-i', input,
          '-frames:v', '1', '-q:v', '4',
          '-vf', `scale='min(iw,${box})':'min(ih,${box})'`
                 + ':force_original_aspect_ratio=decrease:force_divisible_by=2',
          output];
}

export function posterArgs(input, output, durationS) {
  const at = (durationS ?? 0) > 2.5 ? 2 : 0;
  return ['-nostdin', '-loglevel', 'error', '-y', '-ss', String(at), '-i', input,
          '-frames:v', '1', '-q:v', '4',
          '-vf', "scale='min(iw,960)':-2", output];
}

export function resolveMedia(root, rel) {
  if (!root || !rel) return null;
  const base = resolve(root);
  for (const form of ['NFC', 'NFD', null]) {
    const cand = resolve(base, form ? rel.normalize(form) : rel);
    if (cand !== base && !cand.startsWith(base + sep)) continue;
    try { statSync(cand); return cand; } catch { /* try the next form */ }
  }
  return null;
}

export function statMedia(root, rel) {
  const p = resolveMedia(root, rel);
  if (!p) return { ok: false, bytes: null };
  try { return { ok: true, bytes: statSync(p).size }; }
  catch { return { ok: false, bytes: null }; }
}

/** Has a human decided this field?
 *
 *  `started_at` and `duration_s` default from the captures, but recomputing
 *  would silently undo a correction. Rather than an "is overridden" column per
 *  field, ask the changeset log — it already holds that fact, so it doubles as
 *  the lock. */
export function pinned(db, streamId, field) {
  return !!db.prepare(
    `SELECT 1 FROM change c JOIN changeset cs ON cs.id = c.changeset_id
     WHERE c.target_type = 'stream' AND c.target_id = ? AND c.field = ?
       AND c.op = 'update' AND cs.status = 'applied' LIMIT 1`).get(streamId, field);
}

/** Where a capture's remote clock sits on its stream's axis. This is what
 *  `offset_s` has always meant in practice, now derived rather than authored. */
export function capAxisStart(cap, streamStartedAt) {
  return clocksOf(cap, streamStartedAt).remote - streamStartedAt;
}

/** Refresh one stream's derived state from its captures. */
export function recompute(db, streamId, { mediaRoot = null, checkFiles = true } = {}) {
  const s = db.prepare(
    `SELECT id, started_at, duration_s, chat_path, chat_sources
       FROM stream WHERE id = ?`).get(streamId);
  if (!s) throw new Error(`no stream ${streamId}`);

  const t = now();
  const root = checkFiles ? mediaRoot : null;
  const caps = db.prepare(
    'SELECT * FROM capture WHERE stream_id = ? ORDER BY platform').all(streamId);

  const rows = caps.map((c) => {
    let videoOk = !!c.video_ok, chatOk = !!c.chat_ok, bytes = c.video_bytes;
    if (root) {
      const v = statMedia(root, c.video_path);
      const ch = statMedia(root, c.chat_path);
      videoOk = v.ok; bytes = v.bytes; chatOk = ch.ok;
      db.prepare(`UPDATE capture SET video_ok=?, video_bytes=?, chat_ok=?,
                  verified_at=?, updated_at=? WHERE id=?`)
        .run(bit(videoOk), bytes ?? null, bit(chatOk), t, t, c.id);
    }
    return { ...c, video_ok: videoOk, chat_ok: chatOk };
  });

  const verifiable = !!root;
  const anyVideo = rows.some((r) => r.video_path);
  const anyVideoOk = rows.some((r) => r.video_ok);
  const anyChat = rows.some((r) => r.chat_path);
  const anyChatOk = rows.some((r) => r.chat_ok);

  const started = s.started_at;
  let duration = s.duration_s;
  let durationSource = duration === null ? null : 'stated';

  // started_at is DELIBERATELY not derived from the captures any more.
  //
  // It used to read min(broadcast_started_at), which was dead code — that
  // column is empty for every capture that has ever existed. Reviving it
  // against remote_start_wall would be worse than dead: the backfill set
  // remote_start_wall = started_at + offset_s, and twelve captures carry a
  // negative offset, so the first recompute would quietly drag the axis
  // backwards and every axis-relative note with it. The axis is a decision.
  // It moves when somebody says so, through a changeset, and §convertAxisShift
  // moves the notes with it in the open.

  // Duration still derives, and now it can: file_duration_s finally has values
  // once scripts/probe-media.js has run.
  if (rows.length && !pinned(db, streamId, 'duration_s')) {
    const spans = rows.filter((r) => r.file_duration_s)
      .map((r) => r.file_duration_s + Math.max(capAxisStart(r, started), 0));
    if (spans.length) { duration = Math.max(...spans); durationSource = 'measured'; }
  } else if (duration !== null && pinned(db, streamId, 'duration_s')) {
    durationSource = 'decided';
  }

  // States, not booleans: "we have not looked" is a different claim from "it is
  // gone", and only the filesystem can tell them apart.
  const state = (anyLink, anyOk) =>
    !anyLink ? 'never' : anyOk ? 'present' : (verifiable ? 'lost' : 'unverified');

  let vod = state(anyVideo, anyVideoOk);
  if (vod === 'present' && duration) {
    const best = Math.max(0, ...rows.filter((r) => r.video_ok)
      .map((r) => r.file_duration_s ?? 0));
    if (best && best < duration * TRUNCATE_RATIO) vod = 'truncated';
  }
  if (!rows.length && s.started_at > t + 60) vod = 'scheduled';

  // Chat has two eras and the column says which one a stream is in.
  //
  // Once ls-audit has merged an entry, the merged file IS the chat: it holds
  // every platform's messages, origin-tagged, and the raws leave for deep
  // storage where the archive deliberately stops tracking them. Deriving state
  // from the captures then would report `lost` for files that were archived on
  // purpose — a correct observation about the wrong question.
  //
  // A stream with no merged file has simply never been through that pipeline,
  // which is all 371 imported ones, so they keep the per-capture answer.
  let chatOkMerged = null;
  if (s.chat_path) {
    chatOkMerged = root ? statMedia(root, s.chat_path).ok : null;
    if (root) {
      db.prepare('UPDATE stream SET chat_ok=? WHERE id=?')
        .run(bit(chatOkMerged), streamId);
    }
  }
  const chat = s.chat_path
    ? (chatOkMerged === null ? 'unverified' : (chatOkMerged ? 'present' : 'lost'))
    : state(anyChat, anyChatOk);

  // Two absolute clocks are the truth; offset_s is a view of one of them,
  // resynced here so deepLink() and anything else reading it stay correct after
  // the axis moves. Not writable through a changeset — the wall times are.
  //
  // `remote_start_wall` is deliberately NOT written here any more. This loop
  // used to fill a NULL one with `started_at + offset_s` — the same fallback
  // clocksOf() already applies at read time — which meant a capture nobody had
  // ever measured came out of its first recompute() holding a number
  // indistinguishable from a measured one. NULL is a fact: it says the platform
  // clock was never recorded, and the read-time fallback says so every time it
  // is used instead of once, permanently, in the column.
  for (const c of caps) {
    const want = capAxisStart(c, started);
    if (c.offset_s !== want) {
      db.prepare('UPDATE capture SET offset_s=?, updated_at=? WHERE id=?').run(want, t, c.id);
    }
  }

  db.prepare(`UPDATE stream SET started_at=?, duration_s=?, vod_state=?,
              chat_state=?, updated_at=? WHERE id=?`)
    .run(started, duration ?? null, vod, chat, t, streamId);

  // Project the timeline once, here, so the strip, the pins and the player
  // cannot disagree — they all read the same row.
  const timeline = buildTimeline(db, streamId, { durationSource });
  db.prepare('UPDATE stream SET timeline_json=?, timeline_at=? WHERE id=?')
    .run(JSON.stringify(timeline), t, streamId);

  return { vod_state: vod, chat_state: chat, started_at: started,
           duration_s: duration, timeline };
}

// ===========================================================================
//  segments and the projected timeline
// ===========================================================================

/* The one vocabulary — DEFINED IN db.js, re-exported here.
 *
 * It reads backwards and it is deliberate. The two BACKFILLS in db.js have to
 * name the whole vocabulary literally, they run on every boot with no ran-once
 * guard, and a word missing from one of those lists is not "left alone" — it
 * is rewritten to 'unknown'. That has now happened twice. db.js cannot import
 * this file (this file imports db.js, and the lists are needed at module
 * scope, so the cycle is a TDZ ReferenceError during boot), so the definition
 * lives upstream of both and there is exactly one of it.
 *
 * The prose that used to be here — what each word means, why `media` was
 * `game`, why `elements` was carved out of `meta` — moved with it.
 */
//
//   media      what it belongs to — a game, a franchise, an agency, an event
//   character  a guest, a member, a person the block is about
//   type       what kind of stream this stretch is — collab, watchalong,
//              karaoke, zatsudan, event
//   elements   the scaffolding around it — intro, outro, break, waiting screen
//
// Closed, because it is a colour and a colour has to mean the same thing in
// every stream in the archive. Extending it is a deliberate edit to this line,
// not a typo in a text field.
//
// `media` was `game` and `character` was `person`. Both were renamed rather
// than kept as synonyms, because the snippet vocabulary already called the same
// two things `copyright` and `character` and the archive was carrying one idea
// under two names in two tables. `elements` was carved out of the old `meta`,
// which held two disjoint populations: Collab and Zatsudan, which are formats a
// whole stream can be, and Intro/Break/Outro, which are only ever drawn on a
// timeline and are grey because grey means skippable.
//   meta       what a snippet IS rather than what it is about — reviewed,
//              duplicate, animated, audio only, restricted
//   general    everything else about a snippet
//
// The last two are snippet-only, and that is the only thing `taglet` was ever
// for. There is one table now: `media` and `character` name the same subjects
// on both surfaces, so tagging a clip and tagging a broadcast draw from one
// vocabulary and a rename is one edit rather than two that can diverge.
export { KINDS, ALL_KINDS } from './db.js';
// ...and imported AGAIN for this module's own use. `export ... from` re-exports
// without binding the name locally, so both of these are read below — ENUMS
// names KINDS at module scope, projectSegments() reads ALL_KINDS on every
// strip — and without this line the first is a ReferenceError during import
// and the second on the first timeline the archive draws.
import { KINDS, ALL_KINDS } from './db.js';

// `TAGLET_KINDS` was here — character, copyright, meta, general — a second
// vocabulary for the snippet side. It is gone, and the merge that removed it
// was mostly a rename: `copyright` and `character` were already naming the same
// two subjects that `game` and `person` named over here, in a second table,
// with a second autocomplete and a second row to rename. What genuinely only
// belongs to a clip — `meta` and `general` — stayed, and is scoped by
// re-exported from db.js with everything else rather than living somewhere
// else. The surface gating that scoped them was retired with it.

/* ── what a browser will actually play ───────────────────────────────────
 *
 * A file extension is a claim, not a fact, and `ffprobe` will not settle it
 * either: it reports `format_name = "matroska,webm"` for a real WebM and for
 * an ordinary Matroska file alike, because WebM *is* Matroska with a
 * restricted codec list. So the only thing that decides playability is which
 * codecs are inside.
 *
 * The failure this exists to stop is specific and silent. `ffmpeg -c copy`
 * into a `.webm` name produces H.264 in a Matroska container, which is a
 * perfectly valid file that no browser will play — Chrome answers
 * DEMUXER_ERROR_NO_SUPPORTED_STREAMS, Firefox says the MIME type is not
 * supported. Nothing on the server errors, nothing is logged, and the clip is
 * simply blank. *Reproduced against Chromium and confirmed against the
 * VP9/Opus control, which plays.*
 *
 * Returns the Content-Type to serve, or null when nothing honest can be sent
 * and the file needs a remux first.
 */
const WEBM_VIDEO = new Set(['vp8', 'vp9', 'av1']);
const WEBM_AUDIO = new Set(['vorbis', 'opus']);
const MP4_VIDEO  = new Set(['h264', 'hevc', 'av1']);
const MP4_AUDIO  = new Set(['aac', 'mp3', 'opus', 'flac', 'alac']);

const STILL_MIME = { png: 'image/png', apng: 'image/apng', mjpeg: 'image/jpeg',
                     webp: 'image/webp', bmp: 'image/bmp', tiff: 'image/tiff',
                     gif: 'image/gif' };

export function servedType(container, vcodec, acodec) {
  const c = String(container || '').toLowerCase();
  const v = String(vcodec || '').toLowerCase();
  /* Pictures first, and keyed on the CODEC rather than the container: `image2`
     is the demuxer for a numbered sequence of anything, so it says nothing
     about what the frame is, while `mjpeg` says jpeg and only jpeg. Without
     this a meme fell through to `return null` and the type came from the
     extension — which works, and is the one place in the archive where a
     filename gets to decide what a file is. */
  if (STILL_MIME[v] && !acodec) return STILL_MIME[v];
  // No audio track is fine and common for a short clip; an unknown one is not.
  const aOk = (set) => !acodec || set.has(String(acodec).toLowerCase());

  if (c.includes('matroska') || c.includes('webm')) {
    // A Matroska file carrying VP9/Opus is byte-for-byte a WebM file whatever
    // it is named, so an .mkv here needs a correct header and nothing else.
    return WEBM_VIDEO.has(v) && aOk(WEBM_AUDIO) ? 'video/webm' : null;
  }
  if (c.includes('mp4') || c.includes('mov') || c.includes('m4v')) {
    /* No video track means an .m4a, and the type has to say audio — a browser
       handed `video/mp4` for an audio file plays it, but every player, embed
       and download names it wrong from then on. */
    if (!vcodec) return MP4_AUDIO.has(String(acodec ?? '').toLowerCase()) ? 'audio/mp4' : null;
    return MP4_VIDEO.has(v) && aOk(MP4_AUDIO) ? 'video/mp4' : null;
  }
  return null;
}

/* Can the bitstream survive a container swap, or does it need real encoding?
   H.264/AAC in Matroska is a 70ms `-c copy` into mp4 — measured. VP9 in an
   mp4 is legal but poorly supported, so it goes the other way. Anything else
   (MPEG-4 Part 2 from an old .avi, ProRes from an editor) needs a re-encode,
   which is minutes rather than milliseconds and is not the importer's job. */
export function remuxTarget(vcodec, acodec) {
  const v = String(vcodec || '').toLowerCase();
  const a = String(acodec || '').toLowerCase();
  if (MP4_VIDEO.has(v) && (!acodec || MP4_AUDIO.has(a))) return 'mp4';
  if (WEBM_VIDEO.has(v) && (!acodec || WEBM_AUDIO.has(a))) return 'webm';
  return null;   // needs transcoding, not remuxing
}

/* What actually has to be re-encoded, per stream.
 *
 * "Needs a re-encode" is too blunt a verdict, and the real collection proved
 * it: of three unplayable clips, two were H.264 video with UNCOMPRESSED PCM
 * audio in an mp4, and one was VP9 video with perfectly good AAC. Not one of
 * them needed both streams touched. Re-encoding the video of a file whose only
 * fault is its audio track is quality thrown away for nothing, and it is
 * hundreds of times slower than the copy it should have been.
 *
 * Returns { container, video, audio } where each stream is 'copy' or 'encode',
 * and audio may be 'none'. `copy` is exact — the bitstream is moved, not
 * re-compressed.
 */
export function fixPlan(vcodec, acodec, { target = 'mp4' } = {}) {
  const v = String(vcodec || '').toLowerCase();
  const a = acodec ? String(acodec).toLowerCase() : null;
  const [vOk, aOk] = target === 'webm'
    ? [WEBM_VIDEO, WEBM_AUDIO]
    : [MP4_VIDEO, MP4_AUDIO];
  return {
    container: target,
    video: vOk.has(v) ? 'copy' : 'encode',
    audio: !a ? 'none' : (aOk.has(a) ? 'copy' : 'encode'),
  };
}

/** Sort segments onto the axis and tile them.
 *
 *  Storage is not constrained to be contiguous — a human editing a strip will
 *  leave gaps, and making them be exhaustive is a good way to get no segments
 *  at all. The renderer, though, should never have holes. So gaps become
 *  synthetic idle blocks, an open final segment runs to the end, and the whole
 *  thing is clamped to the domain.
 *
 *  With no known duration there is no domain, so nothing is tiled and the
 *  caller is told so rather than being handed a strip scaled to a guess. */
export function projectSegments(rows, capsById, startedAt, domain) {
  const placed = rows.map((r) => ({
    id: r.id,
    lane: r.lane === 1 ? 1 : 0,
    kind: ALL_KINDS.includes(r.kind) ? r.kind : 'unknown',
    // `label ?? tag.name` — write "Mario Kart (200cc)" on one block and still
    // link the entity. The colour is NOT read through the tag: it was copied
    // onto the segment when a human picked it, so re-categorising a game never
    // silently repaints forty old streams.
    label: r.label ?? r.tag_name ?? null,
    tag: r.tag_id ? { id: r.tag_id, name: r.tag_name, slug: r.tag_slug,
                      thumb: r.tag_thumb ? `/media/thumb/${r.tag_thumb}` : null } : null,
    origin: r.origin,
    author: r.author ?? null,
    frame: r.frame,
    anchor_id: r.anchor_id ?? null,
    start_s: axisOf(r, capsById, startedAt, 'start_s'),
    end_s: axisOf(r, capsById, startedAt, 'end_s'),
    exact: isExact(r),
    /* Started and not stopped. A fact about the ROW, set here rather than in
       the tiling below, because the tiling only runs when there is a domain —
       and a stream that is still on air has none. That is exactly when this
       needs to be true. */
    open: r.end_s === null || r.end_s === undefined,
    synthetic: false,
  })).filter((x) => x.start_s !== null)
    .sort((a, b) => a.start_s - b.start_s || String(a.id).localeCompare(String(b.id)));

  /* The two lanes are projected differently and the difference is the design.
     Lane 0 is tiled below: a broadcast is continuous, so a gap in it is a
     stretch nobody has labelled yet, and saying so with the hatch is a true
     claim. Lane 1 is NOT tiled: the last forty minutes of a two-hour game are
     simply not called anything, and painting a hatch there would invent an
     obligation. Gaps in the inner lane are gaps. */
  const lane1 = placed.filter((x) => x.lane === 1);
  const lane0 = placed.filter((x) => x.lane !== 1);

  /* "Inside what", answered here rather than stored. This is the whole reason
     a lane needs no parent_id: the containing block is a fact about geometry,
     recomputed on every projection, so it cannot go stale, cannot be orphaned
     and cannot refuse a sub-chapter that crosses a boundary. Resolved at the
     inner block's START — a block that straddles two outer ones belongs, for
     naming and for colour, to the one it began in.

     It carries the colour too. A lane-1 block has no tag and usually no kind
     worth picking ("Chapter VI of story" is not a game, a person or a type),
     so it is drawn in its container's hue. That also makes the strip say
     "part of that" without a bracket or a leader line. */
  const under = (seg) => {
    const o = lane0.find((x) => x.end_s !== null
      && seg.start_s >= x.start_s && seg.start_s < x.end_s);
    return o ? { id: o.id, label: o.label, kind: o.kind } : null;
  };
  for (const s of lane1) s.under = under(s);

  if (domain === null || domain === undefined) {
    return { tiled: false, segments: placed, past: [] };
  }

  const out = [];
  /* Blocks that landed entirely outside the domain. They are NOT drawn — there
     is nowhere on a strip that ends at `domain` to draw them — but they are
     carried, because the alternative is a row that exists, cost somebody the
     work of making it, and appears nowhere at all.
     Only total disappearance counts. A block that merely reaches past the end
     is clamped, and a clamped block is visible: you can see it run to the edge
     and go looking for why. */
  const past = [];
  const push = (start, end, seg) => {
    const a = Math.max(0, Math.min(start, domain));
    const b = Math.max(a, Math.min(end, domain));
    if (b - a <= 0) {
      if (seg) past.push(seg);
      return;
    }
    // Filler is 'unknown', NOT 'meta'. Nobody said this stretch was a break —
    // they just have not labelled it yet, and those are different claims. A
    // strip with no segments at all would otherwise render as one grey bar
    // reading "none of this is worth opening", about a stream nobody has looked
    // at. When it really is a waiting screen, someone authors a meta segment.
    out.push(seg ? { ...seg, start_s: a, end_s: b }
                 : { id: null, lane: 0, kind: 'unknown', label: null, origin: 'projection',
                     author: null, frame: 'stream', anchor_id: null,
                     start_s: a, end_s: b, exact: true, synthetic: true });
  };

  let cursor = 0;
  /* Untouched from here down, and that is on purpose: lane 0 runs exactly the
     code it ran before the second lane existed, over exactly the rows it saw
     before, so nothing about an existing strip can have moved. */
  lane0.forEach((seg, i) => {
    const next = lane0[i + 1];
    const end = seg.end_s ?? (next ? next.start_s : domain);
    if (seg.start_s > cursor) push(cursor, seg.start_s, null);
    push(Math.max(seg.start_s, cursor), end, seg);
    cursor = Math.max(cursor, Math.min(end, domain));
  });
  if (cursor < domain) push(cursor, domain, null);

  /* Clamped to the domain like everything else, but never FILLED and never
     merged: the gaps between sub-chapters stay gaps, because the last forty
     minutes of a two-hour game are simply not called anything and painting a
     hatch there would invent an obligation.
     A missing end is a different question from a gap, and it now means here
     what it means in lane 0 and what the schema has always said it means:
     runs to the next one. That is what makes `>>` a boundary — you say where
     a thing starts and the next mark says where it stopped. Nothing that
     existed before this can be affected: the form has always demanded both
     ends, so every lane-1 row ever written has one. */
  for (let i = 0; i < lane1.length; i++) {
    const s = lane1[i];
    const nxt = lane1[i + 1];
    const rawEnd = s.end_s ?? (nxt ? nxt.start_s : domain);
    const a = Math.max(0, Math.min(s.start_s, domain));
    const b = Math.max(a, Math.min(rawEnd, domain));
    if (b - a > 0) out.push({ ...s, start_s: a, end_s: b });
    else past.push(s);
  }

  return { tiled: true, segments: out, past };
}

/** Segments that overlap once projected. Checked at apply time, not stored as a
 *  constraint: "shift every boundary back four minutes" is a legitimate
 *  changeset whose intermediate states overlap, so only the final state counts. */
export function segmentOverlaps(db, streamId) {
  const s = db.prepare('SELECT started_at FROM stream WHERE id = ?').get(streamId);
  if (!s) return [];
  const caps = new Map(db.prepare('SELECT * FROM capture WHERE stream_id = ?')
    .all(streamId).map((c) => [c.id, c]));
  const rows = db.prepare(
    `SELECT * FROM segment WHERE stream_id = ? AND retracted_at IS NULL`).all(streamId);
  const placed = rows
    .map((r) => ({ id: r.id, label: r.label, lane: r.lane ?? 0,
                   a: axisOf(r, caps, s.started_at, 'start_s'),
                   b: axisOf(r, caps, s.started_at, 'end_s') }))
    .filter((x) => x.a !== null && x.b !== null)
    .sort((x, y) => x.a - y.a);

  const bad = [];
  /* Per lane. Overlap is a claim about one row of the strip: two things cannot
     both be what she played at 01:14, and two things cannot both be what was
     happening inside that — but a lane-1 block sitting under a lane-0 one is
     the entire point of the second lane, and checking the two together would
     refuse every sub-chapter ever drawn. */
  for (const lane of [0, 1]) {
    const lp = placed.filter((x) => x.lane === lane);
    for (let i = 1; i < lp.length; i++) {
      if (lp[i].a < lp[i - 1].b) {
        bad.push({ first: lp[i - 1].id, second: lp[i].id,
                   first_label: lp[i - 1].label, second_label: lp[i].label,
                   overlap_s: lp[i - 1].b - lp[i].a });
      }
    }
  }
  // Backwards is backwards in either lane, so this one stays global.
  for (const p of placed) {
    if (p.b < p.a) bad.push({ first: p.id, second: p.id, first_label: p.label,
                              second_label: p.label, overlap_s: p.a - p.b,
                              reason: 'ends before it starts' });
  }
  return bad;
}

/** Where a stream's duration came from — because "4:12:00" measured off the
 *  file and "4:12:00" typed by whoever wrote the .md are different claims, and
 *  a timeline scaled to the second deserves to say which one it is standing on.
 *
 *    measured  ffprobe read it off the file
 *    decided   a human overrode it through a changeset
 *    stated    it came in with the vault import and nothing has checked it
 *    null      unknown; there is no domain and nothing can be drawn to scale */
export function durationSourceOf(db, s, caps) {
  if (s.duration_s === null || s.duration_s === undefined) return null;
  if (pinned(db, s.id, 'duration_s')) return 'decided';
  return caps.some((c) => c.file_duration_s) ? 'measured' : 'stated';
}

/** The whole drawable timeline for one stream, resolved onto the axis.
 *
 *  Stored on the stream row by recompute(). Everything here is derived; nothing
 *  here is a decision, so regenerating it can never lose anything. */
export function buildTimeline(db, streamId, { durationSource = undefined } = {}) {
  const s = db.prepare(
    'SELECT id, started_at, duration_s FROM stream WHERE id = ?').get(streamId);
  if (!s) throw new Error(`no stream ${streamId}`);
  const started = s.started_at;
  const domain = s.duration_s ?? null;

  const caps = db.prepare('SELECT * FROM capture WHERE stream_id = ?').all(streamId);
  if (durationSource === undefined) durationSource = durationSourceOf(db, s, caps);
  const capsById = new Map(caps.map((c) => [c.id, c]));

  const notes = db.prepare(
    `SELECT n.*, p.handle AS author FROM note n
     LEFT JOIN person p ON p.id = n.author_id
     WHERE n.stream_id = ? AND n.retracted_at IS NULL
     ORDER BY (n.offset_s IS NULL), n.offset_s, n.ord, n.id`).all(streamId);

  const segRows = db.prepare(
    `SELECT g.*, p.handle AS author,
            t.name AS tag_name, t.slug AS tag_slug,
            COALESCE(t.thumb_path, pt.thumb_path) AS tag_thumb
     FROM segment g
     LEFT JOIN person p ON p.id = g.author_id
     LEFT JOIN tag t    ON t.id = g.tag_id
     LEFT JOIN tag pt   ON pt.id = t.parent_id
     WHERE g.stream_id = ? AND g.retracted_at IS NULL`).all(streamId);

  const { tiled, segments, past } = projectSegments(segRows, capsById, started, domain);

  // Deep links are built against whichever source leads the chain, converted
  // through that source's own clock. `lead` is a capture id, so a duplicate
  // remote_id can no longer resolve to the wrong capture.
  const chain = sourcesFor(caps, { started_at: started, duration_s: domain,
                                   serve_pref: null });
  const lead = chain.find((w) => w.embeddable) ?? null;
  const leadCap = lead ? capsById.get(lead.capture_id) : null;

  // What each source actually covers, on the axis. A Twitch VOD that began two
  // minutes before the YouTube one starts at -120, and the toggle needs to know
  // that rather than pretending both cover the same span.
  const coverage = [];
  for (const c of caps) {
    const clk = clocksOf(c, started);
    const len = c.file_duration_s ?? domain;
    if (clk.remote !== null) {
      coverage.push({ capture_id: c.id, platform: c.platform, clock: 'remote',
                      from_s: clk.remote - started,
                      to_s: len === null ? null : (clk.remote - started) + len });
    }
    if (clk.local !== null) {
      coverage.push({ capture_id: c.id, platform: c.platform, clock: 'local',
                      from_s: clk.local - started,
                      to_s: len === null ? null : (clk.local - started) + len,
                      precision_s: c.local_start_precision_s ?? null });
    }
  }

  return {
    domain_s: domain,
    duration_source: durationSource,
    tiled,
    lead: lead?.capture_id ?? null,
    segments,
    /* Unclamped, because the whole point of them is the number that does not
       fit. A reader has to be able to see 2:00:12 against a stream the archive
       believes is 2:00:00 long and decide which of the two is wrong. */
    segments_past: past,
    notes: notes.map((n) => projectNote(n, capsById, started, leadCap)),
    coverage,
    counts: {
      notes: notes.length,
      notes_unknown_frame: notes.filter((n) => n.frame === 'unknown').length,
      segments: segRows.length,
      // Counted beside the notes' own count, and for the same reason: a number
      // in the footer is what turns "something is missing" into "two things
      // are missing and here is where to look".
      segments_past: past.length,
      captures: caps.length,
    },
  };
}

/** One note, resolved onto the axis and told how much to trust itself. */
export function projectNote(n, capsById, started, leadCap = null) {
  const axis = axisOf(n, capsById, started);
  const playerT = leadCap
    ? axisToPosition(leadCap, started, 'remote', axis) : null;
  return {
    link: leadCap && playerT !== null
      ? deepLink(leadCap.platform, leadCap.remote_id, playerT) : null,
    // A link built from an unknown-frame offset still points somewhere useful;
    // it just is not a claim about the exact second. Say which it is.
    link_exact: isExact(n),
    id: n.id, tag: n.tag, seq: n.seq, text: n.text, stamp: n.stamp ?? null,
    // The verbatim source line. Served because it is the receipt: the editor
    // shows it under a vault note so a correction can be checked against what
    // was actually written, and nothing ever writes to it.
    raw: n.raw ?? null,
    done: !!n.done, ord: n.ord, origin: n.origin, author: n.author ?? null,
    offset_s: n.offset_s, anchor_id: n.anchor_id ?? null,
    anchor_clock: n.anchor_clock ?? null,
    frame: n.frame ?? 'unknown',
    // A pin drawn one pixel wide on a four-hour bar claims about fifteen
    // seconds of accuracy. The vault's timestamps are good to two minutes.
    // Saying so is what stops the timeline presenting a guess as a fact.
    precision_s: n.offset_precision_s ?? null,
    exact: isExact(n),
    start_s: axis,
    broadcast_offset_s: axis,          // kept: the old name for the same number
    at: hms(axis),
    approx: !!n.offset_approx,
  };
}

/** Ordered playback fallback, best first.
 *
 *  YouTube leads because it embeds and seeks, Twitch because it at least
 *  deep-links, then a mirror because it is the copy that survives a takedown,
 *  then the local file. A dead remote is demoted rather than dropped — a dead
 *  link is still evidence of what existed. */
export function watchSources(caps, servePref) {
  const out = [];
  for (const c of caps) {
    const dead = c.alive === 0;
    // capture_id on every entry. Resolving back to a capture by remote_id was
    // wrong on rows that exist: ix_capture_remote is deliberately non-unique
    // because the source data contains genuine duplicates, and when one hit,
    // the wrong capture's clock was used for every link on the stream.
    if (c.platform === 'YT' && c.remote_id) {
      out.push({ kind: 'youtube', capture_id: c.id, clock: 'remote',
        platform: c.platform, embeddable: true, id: c.remote_id, url: c.url,
        embed: `https://www.youtube-nocookie.com/embed/${c.remote_id}`,
        alive: c.alive, rank: dead ? 30 : 0 });
    } else if (c.platform === 'TW' && c.remote_id) {
      out.push({ kind: 'twitch', capture_id: c.id, clock: 'remote',
        platform: c.platform, embeddable: true, id: c.remote_id, url: c.url,
        embed: `https://player.twitch.tv/?video=${c.remote_id}`,
        alive: c.alive, rank: dead ? 31 : 10 });
    }
    if (c.mirror_url) {
      // Still hanging off the parent capture, so it inherits that capture's
      // clock. That holds only while nobody trims the re-upload; the moment
      // someone does, a mirror needs to be a capture row of its own with its
      // own remote_start_wall. Flagged rather than silently assumed exact.
      out.push({ kind: 'mirror', capture_id: c.id, clock: 'remote',
        embeddable: false, platform: c.mirror_platform,
        url: c.mirror_url, alive: null, clock_inherited: true, rank: 20 });
    }
    if (c.video_ok) {
      out.push({ kind: 'local', capture_id: c.id, clock: 'local',
        platform: c.platform, embeddable: false, path: c.video_path,
        url: null, alive: true, rank: 40 });
    }
  }
  if (servePref) {
    /* A KIND, or one of the older class names. The class names came first —
       'remote' meaning "whichever platform" — and the record editor then grew a
       field that wrote 'YT' and 'TW', which this map has never contained. So
       the setting matched nothing, the chain never reordered, and the only
       symptom was that changing the default source appeared to do nothing at
       all. Accepting a bare kind fixes that and keeps every stored class name
       working, since a class name is simply not a kind. */
    const CLASSES = { remote: ['youtube', 'twitch'], mirror: ['mirror'], local: ['local'] };
    const want = CLASSES[servePref] ?? [servePref];
    for (const w of out) if (want.includes(w.kind)) w.rank -= 100;
  }
  out.sort((a, b) => a.rank - b.rank);
  return out.map(({ rank, ...rest }) => rest);
}

/** The watch chain with each entry's clock resolved — what the theater needs to
 *  switch source without losing the moment.
 *
 *  `available` is a state, not a boolean, for the same reason vod_state is:
 *  "we never checked" and "it is gone" are different claims and the toggle
 *  should render them differently. */
export function sourcesFor(caps, stream) {
  const started = stream.started_at;
  const byId = new Map(caps.map((c) => [c.id, c]));
  return watchSources(caps, stream.serve_pref).map((w) => {
    const cap = byId.get(w.capture_id);
    const clk = cap ? clocksOf(cap, started) : { remote: null, local: null };
    const zero = w.clock === 'local' ? clk.local : clk.remote;
    const len = w.clock === 'local'
      ? (cap?.file_duration_s ?? null)
      : (cap?.file_duration_s ?? stream.duration_s ?? null);
    const from = zero === null ? null : zero - started;
    return {
      ...w,
      start_wall: zero,
      duration_s: len,
      covers_s: from === null ? null : [from, len === null ? null : from + len],
      seekable: w.kind !== 'mirror',
      available:
        w.kind === 'local' ? 'local-only'
        : cap?.alive === 0 ? 'dead'
        : cap?.alive === 1 ? 'present'
        : 'unverified',
    };
  });
}

/** An uploaded still wins; otherwise a YouTube id gives one free, which is what
 *  fills the opening grid before any thumbnail pipeline exists. */
export function thumbFor(streamThumb, caps) {
  if (streamThumb) return { url: `/media/thumb/${streamThumb}`, source: 'upload' };
  const withThumb = caps.find((c) => c.thumb_path);
  if (withThumb) return { url: `/media/thumb/${withThumb.thumb_path}`, source: 'capture' };
  const yt = caps.find((c) => c.platform === 'YT' && c.remote_id);
  if (yt) return { url: `https://i.ytimg.com/vi/${yt.remote_id}/hqdefault.jpg`, source: 'youtube' };
  return { url: null, source: null };
}

export function deepLink(platform, remoteId, seconds) {
  if (!platform || !remoteId) return null;
  const t = Math.max(0, Math.round(seconds ?? 0));
  if (platform === 'YT') return `https://www.youtube.com/watch?v=${remoteId}&t=${t}s`;
  if (platform === 'TW') {
    const h = Math.floor(t / 3600), m = Math.floor((t % 3600) / 60), s = t % 60;
    return `https://www.twitch.tv/videos/${remoteId}?t=${h}h${m}m${s}s`;
  }
  return null;
}

export function hms(x) {
  if (x === null || x === undefined) return null;
  const t = Math.max(0, Math.round(x));
  const p = (n) => String(n).padStart(2, '0');
  return `${p(Math.floor(t / 3600))}:${p(Math.floor((t % 3600) / 60))}:${p(t % 60)}`;
}

// ===========================================================================
//  changesets
// ===========================================================================

// What may be written, per target. An allowlist, not a denylist: a change
// naming `id`, `created_at` or `origin` has to be impossible, not merely odd.
export const WRITABLE = {
  stream: {
    title: 'text', summary: 'text', idx: 'int', started_at: 'int',
    tz_offset_min: 'int', duration_s: 'int', serve_pref: 'text',
    thumb_path: 'text', merged_into: 'text',
    chat_path: 'text', chat_sources: 'text',
    // vod_state and chat_state are DELIBERATELY absent.
    //
    // They were writable, and the record editor offered them as enums, but
    // recompute() rewrites both on the next write to the stream — so the edit
    // saved, reverted the instant anything touched the row, and looked like
    // the field refusing to hold a value. pinned() guards started_at and
    // duration_s from exactly that; the states were never added to it.
    //
    // Adding them to pinned() was the other option and it is the wrong one. A
    // state is a claim about the filesystem, not an opinion: `present` means
    // the archive looked and the file was there. A hand-pinned `present` would
    // outlive the file it describes, which is precisely the lie the state
    // machine exists to prevent. The inputs are editable instead — correct
    // `chat_path` or `video_path` and the state follows on the next
    // recompute.
    // Writable so a delete has a way back. `op:'delete'` tombstones by setting
    // this; nothing could clear it, which made the card's delete icon a
    // one-way door and the Undo on its toast impossible.
    retracted_at: 'int',
  },
  capture: {
    stream_id: 'text', platform: 'text', remote_id: 'text', url: 'text',
    title: 'text',
    // The clocks are absolute and authored; offset_s is a view of them,
    // resynced by recompute(), and deliberately NOT writable — editing it
    // directly would desync it from the wall times it is derived from.
    remote_start_wall: 'int', local_start_wall: 'int',
    local_start_precision_s: 'int',
    /* Measured by the rescan job, and correctable by hand — which it has to
       be, because the thing that measures it can be fooled. A YouTube bot
       check and a deleted video arrive as the same failure unless something
       reads the wording, and if a probe is ever wrong about a takedown there
       has to be a way to say so. `0` is dead, `1` is there, NULL is nobody has
       looked — and `bool` rather than `int` so there is no fourth thing it can
       be set to. */
    alive: 'bool',
    file_duration_s: 'int', video_path: 'text', chat_path: 'text',
    thumb_path: 'text', mirror_url: 'text', mirror_platform: 'text',
  },
  note: {
    stream_id: 'text', frame: 'text', anchor_id: 'text', anchor_clock: 'text',
    offset_s: 'int', offset_precision_s: 'int', offset_approx: 'int',
    stamp: 'text', tag: 'text', seq: 'int', text: 'text', done: 'int', ord: 'int',
  },
  segment: {
    stream_id: 'text', frame: 'text', anchor_id: 'text', anchor_clock: 'text',
    start_s: 'int', end_s: 'int', kind: 'text', label: 'text', tag_id: 'text',
    // Writable so that moving a block between lanes is a changeset like every
    // other decision — reviewable, attributable, undoable. It was briefly
    // tempting to set it once at creation and never again; then the first
    // mis-drawn sub-chapter would have needed a delete and a redraw, losing
    // who drew it and when.
    lane: 'int',
  },
  tag: {
    name: 'text', slug: 'text', kind: 'text', parent_id: 'text',
    thumb_path: 'text', summary: 'text', status: 'text',
    /* Where a harvest reads from, and the record of where a description came
       from. Writable because you paste it.
       `seeded` is deliberately NOT here: it is a fact about whether a human
       has been over this row, and a human claiming it by hand would be the
       one thing it must never say. The harvest sets it; the loop below clears
       it. */
    seed_url: 'text',
  },
  // The junction is a write target in its own right — attaching a tag to a
  // stream is a decision like any other, and it needs a row in the log saying
  // who decided it.
  stream_tag: { stream_id: 'text', tag_id: 'text' },
  snippet: {
    title: 'text', summary: 'text', file_path: 'text', poster_path: 'text',
    /* Where a picture came from. Editable by hand for the same reason
       `summary` is, and it is the one field on a gallery image nothing can
       derive: an artist's handle is not in the archive's vocabulary and is
       not supposed to be. What a picture SAYS is `transcript`, written by the
       OCR pass and corrected through the transcript editor. */
    source: 'text',
    /* Which collection this belongs in. Writable because filing a meme as
       gallery art is a judgement somebody can get wrong and should be able to
       change — and because it going through a changeset means the move is
       attributable, which "it used to be in Memes" otherwise never is. */
    kind: 'text',
    source_stream_id: 'text', source_offset_s: 'int', status: 'text',
    // duration_s, width, height and bytes are measurements of a file. A human
    // correcting them by hand would be describing something other than what is
    // on disk, so they are set by the importer and read-only after.
    //
    // transcript is likewise derived — from snippet_line, whole, on each pass.
    // Making it writable would let an edit survive as the search index while
    // the lines under it said something else.
  },
  // `taglet` is gone: one table, so a snippet's tag is written as a `tag`.
  // It stays READABLE as a target_type in the history — change rows written
  // before the merge name it, and a log that cannot resolve its own past is
  // not a log. See the vocab lookup in `summary()`.
  snippet_taglet: { snippet_id: 'text', tag_id: 'text' },
  /* A music row is mostly OBSERVATION — the probe read the title, the channel
     and the upload date off somebody else's page — so only the parts a human
     has an opinion about are writable. `video_id` and `url` are the identity
     and are not among them: editing either would silently repoint the row at
     a different video while keeping its tags, its author and its verdict.
     Correct one by retracting it and submitting the right link. */
  music: {
    // Correctable because a probe can be wrong, or a title can be a mess of
    // brackets nobody wants to read on a card.
    title: 'text', channel: 'text', uploaded_at: 'int', note: 'text',
    // The verdict, so approving is a changeset like every other decision and
    // lands in the history with an author. See the review route.
    status: 'text',
    // Writable so a retraction has a way back, exactly as on `stream`.
    retracted_at: 'int',
  },
  music_tag: { music_id: 'text', tag_id: 'text' },
};

// Fields that must be present to create one of these from nothing.
const REQUIRED = {
  stream: ['title', 'started_at'],
  capture: ['stream_id', 'platform'],
  note: ['stream_id', 'text'],
  segment: ['stream_id', 'start_s'],
  tag: ['name'],
  stream_tag: ['stream_id', 'tag_id'],
  // No title requirement beyond this: the importer derives one from the
  // filename stem, and a clip with a bad title is recoverable where a clip with
  // no file is not.
  snippet: ['title', 'file_path'],
  snippet_taglet: ['snippet_id', 'tag_id'],
  /* Nothing creates a music row through a changeset today — POST /api/music
     does it, because a submission has to canonicalise a URL and enqueue a
     probe before there is anything to review. Listed anyway: validate() reads
     this for every create, and a target type missing from it fails with a
     TypeError instead of a refusal. */
  music: ['video_id', 'url'],
  music_tag: ['music_id', 'tag_id'],
};

// Streams, notes and segments are tombstoned; captures and tags are genuinely
// removable because nothing external points at them.
// stream_tag is genuinely removable — untagging is not a claim worth a
// tombstone, and the changeset that did it is already the record.
// A snippet is content and tombstones like a stream. snippet_taglet is a
// junction and removes cleanly, same as stream_tag.
/* `music` tombstones: retracting is the reversible verb every role above
   viewer can reach, and the row has to survive it so that an admin purge is a
   second, separate decision rather than a consequence of the first. */
const TOMBSTONED = new Set(['stream', 'note', 'segment', 'tag', 'snippet', 'music']);

// What a hard delete has to write down before it happens.
//
// A tombstoned row keeps its own columns, so the change row naming it is
// enough to reconstruct what was removed. A junction is different: it is
// DELETEd outright — a tombstoned link would still read as joined — and the
// change row for a delete carries `field: null, value: null` and the link's
// own id. Once the row is gone that id resolves to nothing, so "who took
// Selen Tatsuki off this snippet" had no answer anywhere, ever.
//
// The pair, not the whole row: created_at and updated_at are the same
// information as the changeset's own timestamp, twice.
const REMEMBER_ON_DELETE = {
  snippet_taglet: ['snippet_id', 'tag_id'],
  stream_tag: ['stream_id', 'tag_id'],
  music_tag: ['music_id', 'tag_id'],
};
const OPS = new Set(['create', 'update', 'delete']);

// Closed vocabularies, checked at validate time so a typo cannot become a
// colour nobody has a swatch for.
const ENUMS = {
  'segment.kind': ALL_KINDS,
  /* Numbers, deliberately, and not a range check. `cast('int', …)` has already
     run by the time ENUMS is consulted, so [0, 1].includes(value) does the job
     the existing machinery was built for and the refusal reads
     "segment.lane must be one of 0, 1" without a line of new error handling.
     A third lane is refused rather than clamped: the strip has two rows, and
     silently drawing a lane-2 block into lane 1 would put a claim somewhere
     nobody put it. */
  'segment.lane': [0, 1],
  'segment.frame': ['capture', 'stream', 'unknown'],
  'segment.anchor_clock': ['remote', 'local'],
  'note.frame': ['capture', 'stream', 'unknown'],
  'note.anchor_clock': ['remote', 'local'],
  /* The SAME list as segment.kind, and now literally the same constant. It used to say
     SEGMENT_KINDS, which was right for exactly as long as the two were the
     same list and wrong the moment they were not — meta and general tags
     started being refused with "tag.kind must be one of media, character,
     type, elements, unknown". There is one list again, so there is nothing
     left to diverge. */
  'tag.kind': ALL_KINDS,
  'tag.status': ['proposed', 'confirmed'],
  /* The three collections. Named for the panels rather than for the file types
     they tend to hold, because the split is by provenance: a meme is often an
     mp4 and a snippet is sometimes a gif. */
  'snippet.kind': SNIPPET_KINDS,

  /* The publication gate.
       proposed   imported, not yet looked at — invisible to the public
       confirmed  someone combed through it and said yes
       rejected   someone looked and said no. NOT retracted: the file stays,
                  the row stays, and it can be revisited. A tombstone means
                  "this should not exist"; this means "not for the front page".
     The three are distinct on purpose — a queue that cannot tell "not yet
     reviewed" from "reviewed and declined" shows you the same thousand clips
     every time you open it. */
  'snippet.status': ['proposed', 'confirmed', 'rejected'],
  /* Which source the theater opens with. Kinds, plus the three older class
     names that predate them and are still stored on some rows. Checked here
     because the failure mode is silence: an unknown value is not refused by
     watchSources, it simply boosts nothing, and the setting looks like it
     saved and then did not work. */
  'stream.serve_pref': ['youtube', 'twitch', 'mirror', 'local', 'remote'],
  /* The same three words as a snippet, and for the same reason: a queue that
     cannot tell "not looked at yet" from "looked at and declined" shows you
     the same pile every time you open it. */
  'music.status': ['proposed', 'confirmed', 'rejected'],
};

/* ── who may propose what ──────────────────────────────────────────────────
 *
 * One capability per (target_type, op), asked of every change in a changeset
 * before any of it is written.
 *
 * ABSENT MEANS NO CAPABILITY IS REQUIRED, and that is deliberate rather than
 * an oversight: only the tag half of the archive has moved onto capabilities
 * so far. Notes, segments, streams, captures and snippets are exactly as they
 * were — gated once at the route by requireRole('suggester') and then trusted
 * — so this change cannot break a write path it was not aimed at. Moving them
 * over later is adding rows here, and each row is a decision somebody has to
 * make on purpose.
 *
 * The whole map is about the VERB, never about the queue. Nothing here says
 * whether a change applies immediately; that is `change.apply`, asked once in
 * propose(). A suggester and an editor both hold `tag.retract` — the
 * difference is only that one of them waits.
 *
 * Every op of a covered type is listed, including the ones nothing sends
 * today. A gap in this table is not a refusal, it is a bypass, so the table is
 * kept total for the types it covers rather than minimal.
 */
const CHANGE_CAPS = {
  'tag:create': 'tag.create',
  'tag:update': 'tag.edit',
  'tag:delete': 'tag.retract',       // `delete` on a tag tombstones — see TOMBSTONED
  'stream_tag:create': 'tag.attach',
  'stream_tag:update': 'tag.attach', // repointing a link is attaching a different tag
  'stream_tag:delete': 'tag.detach',
  'snippet_taglet:create': 'tag.attach',
  'snippet_taglet:update': 'tag.attach',
  'snippet_taglet:delete': 'tag.detach',
  /* Music, and the shape is the tag one exactly. `music:update` covers the
     verdict as well as the title, and a suggester proposing status='confirmed'
     on their own submission is not an escalation — it is a suggestion, and it
     queues like every other, so an editor still decides. */
  'music:create': 'music.submit',
  'music:update': 'music.edit',
  'music:delete': 'music.retract',
  /* The junction is a TAG operation, not a music one: attaching Fuura Yuri to
     a song is the same act as attaching her to a stream, and someone trusted
     to do one is trusted to do the other. */
  'music_tag:create': 'tag.attach',
  'music_tag:update': 'tag.attach',
  'music_tag:delete': 'tag.detach',
};

/** The capability a change needs, or null when it needs none. */
export const capabilityFor = (targetType, op) => CHANGE_CAPS[`${targetType}:${op}`] ?? null;

/** Refuse a changeset the author may not make, before anything is written.
 *
 *  Reports EVERY capability the batch is short of rather than the first: a
 *  mint-and-attach is four change rows across two types, and being told about
 *  them one submit at a time is the kind of feedback that reads as the feature
 *  being broken.
 *
 *  A null person is a trusted internal caller — see propose(), which will not
 *  let a route reach this without saying which it is. */
function authorize(person, list) {
  if (!person) return;
  const missing = new Map();          // capability -> the act that wanted it
  for (const c of list) {
    const need = capabilityFor(c.target_type, c.op);
    if (!need || can(person, need)) continue;
    missing.set(need, `${c.op === 'delete' ? 'remove' : c.op} ${c.target_type.replace(/_/g, ' ')}`);
  }
  if (!missing.size) return;
  throw new ChangeError({
    error: `your role cannot ${[...missing.values()].join(', or ')}`,
    missing: [...missing.keys()],
  }, 403);
}

/** name -> slug. Derived, never authored, so the two cannot drift apart.
 *
 *  Keeps any unicode letter or number rather than stripping to ASCII. This
 *  archive is full of Japanese titles, and an ASCII-only slug turns every one
 *  of them into the same empty string — so 【ゼルダの伝説】 and 【ポケモン】
 *  would collide on `tag`, `tag-2`, and the vocabulary would be gibberish.
 *  Combining marks are folded so café and cafe are one tag. */
// slugify lives in db.js so migrate() can use it without an import cycle.
export { slugify } from './db.js';



/** A changeset that cannot be accepted or applied. `detail` may be an object
 *  when the caller needs the specifics (a staleness conflict, say). */
export class ChangeError extends Error {
  constructor(detail, status = 400) {
    super(typeof detail === 'string' ? detail : 'changeset rejected');
    this.detail = detail;
    this.status = status;
  }
}

function cast(kind, value) {
  if (value === null || value === undefined || value === '') return null;
  if (kind === 'int') {
    const n = Number(value);
    if (!Number.isFinite(n)) throw new ChangeError(`${value} is not a number`);
    return Math.trunc(n);
  }
  /* A three-state flag: 0, 1, or NULL for "nobody has looked". `int` would
     take a 7, and a 7 is not a fourth state — every reader compares against 0
     and 1 exactly, so it would read as unverified while sitting in the column
     looking like an answer. */
  if (kind === 'bool') {
    if (value === true || value === 1 || value === '1' || value === 'true') return 1;
    if (value === false || value === 0 || value === '0' || value === 'false') return 0;
    throw new ChangeError(`${value} is not yes or no`);
  }
  return String(value);
}

export function currentValue(db, targetType, targetId, field) {
  if (!WRITABLE[targetType]) throw new ChangeError(`unknown target type ${targetType}`);
  // Field names are allowlisted above, so interpolating one here is safe; the
  // id is still bound.
  const row = db.prepare(
    `SELECT ${field} AS v FROM ${targetType} WHERE id = ?`).get(targetId);
  return row ? row.v : null;
}

/** Normalise and check a proposed change list. Throws ChangeError.
 *
 *  `person` is who is asking, and it is checked LAST — after normalisation, so
 *  the capability question is asked of a cleaned-up `{target_type, op}` rather
 *  than of whatever arrived on the wire. Null means a trusted internal caller;
 *  propose() is what makes that choice explicit. */
export function validate(db, changes, person = null) {
  if (!Array.isArray(changes) || !changes.length) {
    throw new ChangeError('a changeset needs at least one change');
  }
  if (changes.length > 500) throw new ChangeError('too many changes (max 500)');

  const creating = new Map();   // target_id -> target_type
  const out = [];
  for (const raw of changes) {
    const tt = String(raw.target_type ?? '').trim();
    const op = String(raw.op ?? 'update').trim();
    const tid = String(raw.target_id ?? '').trim();

    if (!WRITABLE[tt]) {
      throw new ChangeError(`target_type must be one of ${Object.keys(WRITABLE).join(', ')}`);
    }
    if (!OPS.has(op)) throw new ChangeError(`op must be create, update or delete`);
    if (!tid) throw new ChangeError('target_id is required');

    if (op === 'create') {
      if (!isUlid(tid)) {
        throw new ChangeError('a create must carry a client-minted ULID as ' +
          'target_id — that is what makes the changeset idempotent');
      }
      creating.set(tid, tt);
    } else if (!creating.has(tid)) {
      if (!db.prepare(`SELECT 1 FROM ${tt} WHERE id = ?`).get(tid)) {
        throw new ChangeError(`no ${tt} with id ${tid}`);
      }
    }

    if (op === 'delete') { out.push({ target_type: tt, target_id: tid, op, field: null, value: null }); continue; }

    const field = String(raw.field ?? '').trim();
    if (!WRITABLE[tt][field]) {
      throw new ChangeError(`'${field}' is not writable on ${tt}; allowed: ` +
        Object.keys(WRITABLE[tt]).sort().join(', '));
    }
    const value = cast(WRITABLE[tt][field], raw.value);
    const allowed = ENUMS[`${tt}.${field}`];
    if (allowed && value !== null && !allowed.includes(value)) {
      throw new ChangeError(`${tt}.${field} must be one of ${allowed.join(', ')}`);
    }
    /* What the author believed the field held when they decided. Carried
       through only when supplied — see propose(), which prefers it over its
       own read and falls back when it is absent, so every existing caller
       behaves exactly as before. */
    const hasBase = Object.prototype.hasOwnProperty.call(raw, 'base_value');
    out.push({ target_type: tt, target_id: tid, op, field, value,
               ...(hasBase ? { base_value: raw.base_value === null ? null
                                                                   : String(raw.base_value) } : {}) });
  }

  for (const [tid, tt] of creating) {
    const got = new Set(out.filter((c) => c.target_id === tid && c.op === 'create' && c.field)
      .map((c) => c.field));
    const missing = REQUIRED[tt].filter((f) => !got.has(f));
    if (missing.length) throw new ChangeError(`creating a ${tt} needs ${missing.join(', ')}`);
  }
  authorize(person, out);
  return out;
}

/** Record a changeset. Applies it immediately when the author may.
 *
 *  `person` is the author, as an object with a `role` — it answers both
 *  questions this function has to ask: may they propose these changes at all
 *  (validate → authorize), and do their proposals wait (`change.apply`).
 *
 *  It is REQUIRED, and `trusted: true` is the only way to omit it. That is on
 *  purpose. A default of "no person means no check" is the kind of default
 *  that is correct in every caller that exists today and wrong in the first
 *  one somebody adds in a hurry — so the choice is made at the call site, in
 *  writing, and a route that forgets both gets an error instead of a bypass.
 *
 *  `autoApply` still wins where it is given, because a trusted caller
 *  proposing on somebody's behalf — bulk snippet review — is making the
 *  decision itself and is not asking about the author's role. */
export function propose(db, { authorId = null, person = null, trusted = false,
                              reason = null, changes, autoApply = null,
                              mediaRoot = null }) {
  if (!person && !trusted) {
    throw new ChangeError('propose() needs `person`, or `trusted: true` for an internal caller');
  }
  const list = validate(db, changes, trusted ? null : person);
  const auto = autoApply !== null ? !!autoApply : can(person, 'change.apply');
  const t = now();
  const csId = ulid();

  tx(db, () => {
    db.prepare(`INSERT INTO changeset(id, author_id, reason, status, created_at)
                VALUES(?,?,?,'open',?)`).run(csId, authorId, reason, t);
    list.forEach((c, i) => {
      let base = null;
      if (c.op === 'update') {
        const seen = currentValue(db, c.target_type, c.target_id, c.field);
        if (String(seen ?? '') === String(c.value ?? '')) {
          throw new ChangeError(`${c.target_type}.${c.field} is already that value`);
        }
        /* The author's own answer wins where they gave one.
           Reading this server-side is the natural thing to do and it is subtly
           wrong: it records what the field held at SUBMIT, not what the author
           was looking at when they decided. Someone who opens a clip, is
           distracted for ten minutes while an editor retitles it, then submits,
           gets a base_value of the editor's new title — so stale() sees no
           conflict and the suggestion silently reverts an edit its author never
           saw, with the history recording it as deliberate.
           Absent a client value the old behaviour stands, which is still the
           right fallback: a base read now beats no base at all. */
        base = Object.prototype.hasOwnProperty.call(c, 'base_value') ? c.base_value : seen;
      }
      db.prepare(`INSERT INTO change(id, changeset_id, seq, target_type, target_id,
                    op, field, value, base_value) VALUES(?,?,?,?,?,?,?,?,?)`)
        .run(ulid(), csId, i, c.target_type, c.target_id, c.op,
             c.field ?? null, c.value ?? null, base === null ? null : String(base));
    });
  });

  /* What apply() worked out, carried back out through propose().
     summary() reads the changeset's own rows and cannot know any of this: that
     a create was resolved onto a row which already existed, or that the row it
     landed on had been retracted and is now back. An editor's mint goes
     straight through here, so dropping it meant the one caller who most needs
     the distinction — the person who just pressed the button — was the only
     one who never got it. */
  let resolved = null;
  if (auto) {
    const done = apply(db, csId, { reviewerId: authorId,
                                   note: 'author may edit directly', mediaRoot });
    resolved = done?.merged ?? null;
  } else {
    // A proposal is a write. It adds rows the read API serves — the review
    // queue, the stream's history, the "2 open suggestions" badge — and
    // `generation` is what every ETag is keyed on. Without this bump the
    // detail endpoint keeps answering 304 with a body that predates the
    // suggestion, so the person who just submitted one sees no trace of it
    // anywhere. apply() bumps for the same reason; propose() simply never did.
    bumpGeneration(db);
  }
  const out = summary(db, csId);
  if (resolved?.length) out.merged = resolved;
  return out;
}

/** Changes whose field moved since the changeset was written. */
export function stale(db, csId) {
  const rows = db.prepare(
    `SELECT * FROM change WHERE changeset_id = ? AND op = 'update' ORDER BY seq`).all(csId);
  const out = [];
  for (const c of rows) {
    const nowValue = currentValue(db, c.target_type, c.target_id, c.field);
    if (String(nowValue ?? '') !== String(c.base_value ?? '')) {
      out.push({ seq: c.seq, target_type: c.target_type, target_id: c.target_id,
                 field: c.field, proposed_against: c.base_value,
                 current: nowValue, wants: c.value });
    }
  }
  return out;
}

export function apply(db, csId, { reviewerId = null, note = null, force = false,
                                  mediaRoot = null } = {}) {
  const cs = db.prepare('SELECT * FROM changeset WHERE id = ?').get(csId);
  if (!cs) throw new ChangeError(`no changeset ${csId}`, 404);
  if (cs.status !== 'open') throw new ChangeError(`changeset is already ${cs.status}`);

  const conflicts = stale(db, csId);
  if (conflicts.length && !force) {
    throw new ChangeError({
      error: 'values changed since this was proposed', conflicts,
      hint: 're-send with force=true to apply anyway',
    }, 409);
  }

  const t = now();
  const changes = db.prepare(
    'SELECT * FROM change WHERE changeset_id = ? ORDER BY seq').all(csId);
  const touched = new Set();
  /* Out here, not in the transaction, because the fold happens inside it and
     the response is assembled after it. Only ever read once tx() has
     committed — a rollback throws straight past the return below, so a
     half-resolved list can never be reported as an outcome. */
  const merged = [];                  // tag creates folded into an existing row

  tx(db, () => {
    // ---- no silent shifts -------------------------------------------------
    // Three operations change what an already-stored timestamp MEANS. Each one
    // rewrites its dependents here, in this transaction, and records every
    // rewrite as an extra change row on this same changeset — so the history
    // reads "note X moved from 1234 to 1474, because its anchor was deleted"
    // rather than the note having quietly moved four minutes while nobody
    // was looking. Doing it before the main loop means the guard triggers in
    // schema.sql see a capture nothing points at any more.
    let seq = 1 + (db.prepare(
      'SELECT COALESCE(MAX(seq), -1) m FROM change WHERE changeset_id = ?').get(csId).m);
    const record = (tt, id, field, from, to) => {
      db.prepare(`INSERT INTO change(id, changeset_id, seq, target_type, target_id,
                    op, field, value, base_value) VALUES(?,?,?,?,?,'update',?,?,?)`)
        .run(ulid(), csId, seq++, tt, id, field,
             to === null ? null : String(to), from === null ? null : String(from));
    };

    for (const c of changes) {
      if (c.target_type === 'capture' && c.op === 'delete') {
        detachAnchor(db, c.target_id, t, record);
      }
      if (c.target_type === 'capture' && c.op === 'update' && c.field === 'stream_id') {
        // Re-parenting keeps the capture's own clock but moves it under a
        // different axis, and any note anchored to it still belongs to the old
        // stream. There is no rewrite that makes that coherent, so refuse
        // rather than invent one.
        const deps = db.prepare(
          `SELECT COUNT(*) c FROM (
             SELECT id FROM note    WHERE anchor_id = ? AND retracted_at IS NULL
             UNION ALL
             SELECT id FROM segment WHERE anchor_id = ? AND retracted_at IS NULL)`)
          .get(c.target_id, c.target_id).c;
        if (deps) {
          throw new ChangeError(
            `capture ${c.target_id} anchors ${deps} note(s)/segment(s); move or ` +
            `detach them in this changeset before re-parenting it`);
        }
      }
      // started_at used to cascade: moving the axis zero rewrote every
      // unanchored note and segment offset so they held the same wall moment.
      // It does not any more, deliberately.
      //
      // Correcting a start time is a correction to ONE fact — when the
      // broadcast began — and the numbers on the notes were not measured
      // against that fact. A vault note reading 02:30:00 was read off a video,
      // and it is still 2h30m into that video after somebody discovers the
      // stream actually started five minutes earlier. Rewriting it would
      // change a number nobody has any better information about, which is the
      // opposite of a correction. Anchored rows never moved and still do not.
      //
      // The cost, stated: a chapter drawn on the strip IS axis-relative
      // (frame='stream'), so it keeps its number and therefore lands on
      // slightly different picture. Making chapters immune too means anchoring
      // them to a capture at authoring time — a different decision, not this
      // one. See `shiftAxis` in the git history if it ever needs to come back.
    }

    // A create arrives as several rows sharing one target_id; collect them into
    // a single INSERT so the row is never half-built and a reviewer can never
    // leave half a stream behind.
    const creates = new Map();          // target_id -> { type, fields }
    for (const c of changes) {
      if (c.op !== 'create') continue;
      if (!creates.has(c.target_id)) {
        creates.set(c.target_id, { type: c.target_type, fields: {} });
      }
      creates.get(c.target_id).fields[c.field] = c.value;
    }
    // Two people can both propose "Mario Kart" before either is reviewed. Both
    // mint their own ULID, and the second to be approved would hit UNIQUE(slug)
    // and roll the whole changeset back — losing an otherwise good suggestion
    // over a race. Resolve instead: point the rest of the changeset at the row
    // that already exists, and write down that it happened.
    //
    // One vocabulary now, so this runs once. It used to loop over `tag` and
    // `taglet` — two lists that collided in exactly the same way, which was
    // itself an argument for there being one of them.
    {
      const vocab = 'tag';
      for (const [tid, c] of [...creates]) {
        if (c.type !== vocab || !c.fields.name) continue;
        const slug = slugify(c.fields.name);
        /* retracted_at comes back too, and the query deliberately does NOT
           filter it out. It cannot: UNIQUE(slug) is table-wide, so a
           tombstoned row still owns its name and there is no minting around
           it — a second row with the same slug is not a thing this schema can
           hold. The bug was folding onto one WITHOUT NOTICING: the changeset
           applied, the toast said yes, and everything pointed at a row that
           every list filters out of existence. */
        const existing = db.prepare(
          `SELECT id, name, retracted_at FROM ${vocab} WHERE slug = ?`).get(slug);
        if (!existing || existing.id === tid) continue;
        creates.delete(tid);
        for (const ch of changes) {
          if (ch.value === tid && ch.target_type !== vocab) {
            db.prepare('UPDATE change SET value = ? WHERE id = ?').run(existing.id, ch.id);
            ch.value = existing.id;
          }
        }
        // `creates` was collected from `changes` before this rewrite, so it
        // still holds the id we just discarded. Rewriting only the change rows
        // leaves the pending INSERTs pointing at a row that will never exist,
        // and the whole changeset dies on a foreign key instead.
        for (const pending of creates.values()) {
          for (const [k, v] of Object.entries(pending.fields)) {
            if (v === tid) pending.fields[k] = existing.id;
          }
        }
        /* Asking for the name again IS the reversal.
           `tag.retract` is a tombstone and the capability list says so out
           loud — "reversible" — and it sits at the same rank as `tag.create`:
           both are a suggester's to propose and an editor's to approve. So
           bringing one back needs no authority that minting it did not, and
           there is nothing to escalate. Anything irreversible is `tag.purge`,
           which is an admin's and destroys the row outright — a purged tag
           leaves no slug behind and this branch never sees it.
           Recorded rather than done quietly, so the review log says a tag came
           back and does not merely imply it by the row changing shape. */
        if (existing.retracted_at) {
          record(vocab, existing.id, 'retracted_at', existing.retracted_at, null);
          /* And the name AS TYPED. slugify() is case-insensitive, so
             "Limbus Company" and "LIMBUS COMPANY" are one row and one slug —
             but the display casing is the thing somebody just took the trouble
             to type, and handing back a tombstone still wearing its old
             shouting is not what they asked for.
             Only on a restore. A LIVE row keeps its name: renaming a tag that
             forty streams already carry, as a side effect of somebody else's
             mint colliding with it, is a bigger edit than the one they made
             and not one they were shown. */
          record(vocab, existing.id, 'name', existing.name, c.fields.name);
          db.prepare(`UPDATE ${vocab} SET retracted_at = NULL, status = 'confirmed',
                                          name = ?, updated_at = ? WHERE id = ?`)
            .run(c.fields.name, t, existing.id);
        } else {
          /* from and to both the name it already has. This row exists so the
             changeset log shows the name was what collided; it used to record
             the TYPED name on both sides, which read as a rename that never
             happened. */
          record(vocab, existing.id, 'name', existing.name, existing.name);
        }
        merged.push({ wanted: tid, resolved_to: existing.id, slug,
                      ...(existing.retracted_at ? { restored: true } : {}) });
      }
    }

    // Same idea one level up: two people tagging the same thing with the same
    // thing is not a conflict, it is agreement. The pair already being there
    // means the changeset's intent is satisfied, so drop the insert rather than
    // failing the UNIQUE and rolling back everything else in it.
    for (const [jt, [a, b]] of Object.entries({
      stream_tag: ['stream_id', 'tag_id'],
      snippet_taglet: ['snippet_id', 'tag_id'],
    })) {
      for (const [tid, c] of [...creates]) {
        if (c.type !== jt) continue;
        const have = db.prepare(
          `SELECT id FROM ${jt} WHERE ${a} = ? AND ${b} = ?`)
          .get(c.fields[a], c.fields[b]);
        if (!have) continue;
        creates.delete(tid);
        merged.push({ wanted: tid, resolved_to: have.id, already: 'attached' });
      }
    }

    for (const [tid, { type: tt, fields }] of creates) {
      // Provenance is stamped by the applier, never taken from the payload —
      // otherwise anyone could submit a note attributed to someone else, or one
      // that claims to have come from the vault.
      if (tt === 'tag') {
        // slug is DERIVED. Letting a client send both is how a tag ends up
        // named one thing and matched by another.
        fields.slug = slugify(fields.name);
        fields.origin = 'user';
        fields.author_id = cs.author_id;
        /* Confirmed, because by the time this line runs somebody with the
           authority to say yes has said it.
           It used to read the AUTHOR's role and file a suggester's tag as
           'proposed', and that was wrong in the one case it was written for.
           A tag row is only ever created HERE, inside apply() — and apply() is
           only reached two ways: an editor's own changeset, which auto-applies
           because they may decide, or the review route, which requires
           `review.decide`. So a suggester's tag came into existence at the
           exact moment an editor approved it, and was then hidden from the
           autocomplete that approval existed to put it in. The editor had to
           find it again and confirm it a second time, on a screen that does
           not explain why.
           `proposed` is still a real state and still writable by hand — an
           editor parking a tag they are unsure about — it simply is not
           something an approval produces. */
        fields.status = 'confirmed';
        // No guess at a category. A row minted from an autocomplete miss has
        // none until someone gives it one, and picking the most common one for
        // them is inference from a name — the same move as reading a game off a
        // stream title. 'unknown' is shared, so an unfiled row shows up in both
        // pickers rather than hiding on the surface it was not minted from;
        // whoever files it decides which surfaces it belongs to.
        if (!fields.kind) fields.kind = 'unknown';
      } else if (tt === 'snippet') {
        fields.origin = 'user';
        fields.author_id = cs.author_id;
      } else if (tt === 'note' || tt === 'segment') {
        fields.origin = 'user';
        fields.author_id = cs.author_id;
        // A row that names an anchor is anchored. Saying so explicitly means a
        // client that supplies anchor_id and forgets `frame` still gets what it
        // meant, instead of an offset silently read as axis-relative.
        if (fields.anchor_id && !fields.frame) fields.frame = 'capture';
        if (fields.frame === 'capture' && !fields.anchor_clock) {
          fields.anchor_clock = 'remote';
        }
      } else if (tt === 'stream') {
        fields.origin = 'web';
      }
      const cols = ['id', ...Object.keys(fields), 'created_at', 'updated_at'];
      const vals = [tid, ...Object.values(fields), t, t];
      db.prepare(`INSERT INTO ${tt}(${cols.join(',')}) ` +
                 `VALUES(${cols.map(() => '?').join(',')})`).run(...vals);
      for (const s of streamsOf(db, tt, tid)) touched.add(s);
    }

    /* Editing any of these on a tag means a human has been over the row, so it
       is no longer what the harvest wrote. One bit for the whole tag — see the
       block on `seeded` in db.js. `seed_url` is absent on purpose: pasting a
       link is not editing the description, and clearing the flag there would
       make the sprout button mark its own input as hand-written. */
    const UNSEEDS = new Set(['name', 'summary', 'thumb_path']);

    for (const c of changes) {
      if (c.op === 'create') continue;
      if (c.op === 'update') {
        db.prepare(`UPDATE ${c.target_type} SET ${c.field} = ?, updated_at = ? WHERE id = ?`)
          .run(c.value, t, c.target_id);
        if (c.target_type === 'tag' && UNSEEDS.has(c.field)) {
          db.prepare(`UPDATE tag SET seeded = 0 WHERE id = ? AND seeded = 1`).run(c.target_id);
        }
        // A rename that leaves the slug behind means the row answers to its old
        // URL and matches on its old spelling forever.
        if (c.target_type === 'tag' && c.field === 'name') {
          const tbl = c.target_type;
          const slug = slugify(c.value);
          const was = db.prepare(`SELECT slug FROM ${tbl} WHERE id = ?`).get(c.target_id)?.slug;
          if (was !== slug) {
            db.prepare(`UPDATE ${tbl} SET slug = ? WHERE id = ?`).run(slug, c.target_id);
            record(tbl, c.target_id, 'slug', was, slug);
          }
        }
      } else if (c.op === 'delete') {
        if (TOMBSTONED.has(c.target_type)) {
          // Tombstone, never DELETE: a removed stream has to stay removed
          // against a future import, and someone may have been wrong.
          db.prepare(`UPDATE ${c.target_type} SET retracted_at = ?, updated_at = ? WHERE id = ?`)
            .run(t, t, c.target_id);
          if (c.target_type === 'stream') {
            db.prepare('UPDATE stream SET retracted_why = ? WHERE id = ?')
              .run(cs.reason, c.target_id);
          }
        } else {
          /* Written as ordinary update rows in the same changeset, the way
             detachAnchor already writes down the moves it makes: the pair went
             from something to nothing. It reads correctly as history and it is
             what a revert would need, if one is ever written. */
          const keep = REMEMBER_ON_DELETE[c.target_type];
          if (keep) {
            const was = db.prepare(
              `SELECT * FROM ${c.target_type} WHERE id = ?`).get(c.target_id);
            if (was) for (const f of keep) record(c.target_type, c.target_id, f, was[f], null);
          }
          for (const s of streamsOf(db, c.target_type, c.target_id)) touched.add(s);
          db.prepare(`DELETE FROM ${c.target_type} WHERE id = ?`).run(c.target_id);
          continue;
        }
      }
      for (const s of streamsOf(db, c.target_type, c.target_id)) touched.add(s);
    }

    // Invariants are checked on the FINAL state, once, still inside the
    // transaction. Per-change validation cannot do this: "shift every boundary
    // back four minutes" is a legitimate changeset whose intermediate states
    // overlap, and rejecting it row by row would reject the very edit segments
    // most need.
    for (const sid of touched) {
      if (!db.prepare('SELECT 1 FROM stream WHERE id = ?').get(sid)) continue;
      const bad = segmentOverlaps(db, sid);
      if (bad.length) {
        throw new ChangeError({
          error: 'segments would overlap once applied',
          stream_id: sid, overlaps: bad,
        }, 409);
      }
    }

    db.prepare(`UPDATE changeset SET status='applied', reviewed_by=?, reviewed_at=?,
                review_note=? WHERE id=?`).run(reviewerId, t, note, csId);
    // Deliberately NOT auto-superseding other open changesets for the same
    // field. A competing proposal is a different opinion, not a redundant one;
    // silently closing it would throw away a real suggestion. It goes stale
    // instead, and a reviewer is shown both values and decides.
  });

  /* Derived state, refreshed AFTER the commit — and its failure is a logged
     warning rather than the response.

     This runs outside the transaction on purpose: recompute() stats every
     capture file when mediaRoot is set, and holding the write lock across
     filesystem IO would make the contention it is a victim of far worse.

     But that means it can throw on a lock this request no longer holds — a
     concurrent probe-media.js is enough — and it used to throw straight out of
     apply(). The caller got a 500 for a changeset that HAD applied, retried,
     and collected a second 500 from the primary-key collision. Two errors, no
     edit lost, and no way to tell that from the outside.

     So: the edit landed, the response says so, and the stale derived columns
     are named in the log. They are all recoverable — the next write to the
     stream recomputes them, and scripts/rebuild.js does the lot. */
  const restale = [];
  for (const sid of touched) {
    if (!db.prepare('SELECT 1 FROM stream WHERE id = ?').get(sid)) continue;
    try { recompute(db, sid, { mediaRoot }); }
    catch (e) { restale.push(sid); console.error(`apply ${csId}: recompute ${sid} failed:`, e?.message ?? e); }
  }
  bumpGeneration(db);
  const out = summary(db, csId);
  /* Said out loud in the response too. A caller that cares — the record editor
     refreshing a strip — can tell "applied, and the projection is current" from
     "applied, and the projection is a rebuild behind". */
  if (restale.length) out.recompute_failed = restale;
  /* What the applier RESOLVED rather than created, which until now it worked
     out and then dropped on the floor. A caller cannot otherwise tell "your
     tag was minted" from "your tag already existed and you were quietly
     attached to it" from "a retracted tag came back" — three different
     outcomes behind one success, and the middle one is why a toast could say
     yes while nothing appeared. */
  if (merged.length) out.merged = merged;
  return out;
}

export function reject(db, csId, { reviewerId = null, note = null } = {}) {
  const cs = db.prepare('SELECT status FROM changeset WHERE id = ?').get(csId);
  if (!cs) throw new ChangeError(`no changeset ${csId}`, 404);
  if (cs.status !== 'open') throw new ChangeError(`changeset is already ${cs.status}`);
  db.prepare(`UPDATE changeset SET status='rejected', reviewed_by=?, reviewed_at=?,
              review_note=? WHERE id=?`).run(reviewerId, now(), note, csId);
  return summary(db, csId);
}

function streamsOf(db, tt, tid) {
  if (tt === 'stream') return [tid];
  if (tt === 'capture' || tt === 'note' || tt === 'segment' || tt === 'stream_tag') {
    const r = db.prepare(`SELECT stream_id FROM ${tt} WHERE id = ?`).get(tid);
    return r ? [r.stream_id] : [];
  }
  // A tag can name blocks on any number of streams, so editing one touches all
  // of them — their projected timelines carry the tag's name and art.
  if (tt === 'tag') {
    return db.prepare(
      `SELECT DISTINCT stream_id FROM segment WHERE tag_id = ? AND retracted_at IS NULL
       UNION SELECT stream_id FROM stream_tag WHERE tag_id = ?`).all(tid, tid)
      .map((r) => r.stream_id);
  }
  return [];
}

/* `authorMayEdit(db, personId)` was here — the last place in this file that
   read a role out of the database to make a decision. Its only caller was the
   new-tag `status` above, which no longer asks: reaching apply() already means
   somebody with `review.decide` said yes. Whether an author's OWN changeset
   waits is asked once now, in propose(), as `can(person, 'change.apply')`. */

// ---------------------------------------------------------------------------
//  conversions — the two rewrites that keep a timestamp meaning what it meant
// ---------------------------------------------------------------------------

/** A capture is about to be deleted. Everything anchored to it is re-expressed
 *  against the axis first, so nothing is left holding a reference to a clock
 *  that no longer exists.
 *
 *  The old FK said ON DELETE SET NULL, and a NULL anchor used to MEAN
 *  "already axis-relative" — so deleting a capture whose clock sat four minutes
 *  off the axis moved every note on it by four minutes, silently, with nothing
 *  in the history to say it had happened. */
export function detachAnchor(db, captureId, t, record) {
  const cap = db.prepare('SELECT * FROM capture WHERE id = ?').get(captureId);
  if (!cap) return;
  const s = db.prepare('SELECT started_at FROM stream WHERE id = ?').get(cap.stream_id);
  if (!s) return;
  const capsById = new Map([[cap.id, cap]]);

  for (const [table, fields] of [['note', ['offset_s']], ['segment', ['start_s', 'end_s']]]) {
    const rows = db.prepare(
      `SELECT * FROM ${table} WHERE anchor_id = ? AND retracted_at IS NULL`).all(captureId);
    for (const r of rows) {
      for (const f of fields) {
        const axis = axisOf(r, capsById, s.started_at, f);
        if (r[f] !== null && r[f] !== undefined && axis !== null && axis !== r[f]) {
          db.prepare(`UPDATE ${table} SET ${f} = ?, updated_at = ? WHERE id = ?`)
            .run(axis, t, r.id);
          record(table, r.id, f, r[f], axis);
        }
      }
      db.prepare(
        `UPDATE ${table} SET frame='stream', anchor_id=NULL, anchor_clock=NULL,
         updated_at=? WHERE id=?`).run(t, r.id);
      record(table, r.id, 'frame', r.frame, 'stream');
      record(table, r.id, 'anchor_id', r.anchor_id, null);
    }
  }
}

export function summary(db, csId) {
  const cs = db.prepare(
    `SELECT cs.*, a.handle AS author, r.handle AS reviewer FROM changeset cs
     LEFT JOIN person a ON a.id = cs.author_id
     LEFT JOIN person r ON r.id = cs.reviewed_by WHERE cs.id = ?`).get(csId);
  if (!cs) throw new ChangeError(`no changeset ${csId}`, 404);
  const out = { ...cs };
  out.changes = db.prepare(
    `SELECT seq, target_type, target_id, op, field, value, base_value
     FROM change WHERE changeset_id = ? ORDER BY seq`).all(csId);
  if (cs.status === 'open') out.conflicts = stale(db, csId);
  return out;
}

// ---------------------------------------------------------------------------
// notes -> a subtitle track
//
// A phone writes lines like
//
//     @ 09.09 11:06:54 — Tenma's Homemade Stew
//
// and an NLE wants those as cues measured from the start of the FILE it is
// cutting. Those are two different clocks with a stream between them, which is
// the conversion this archive already models — so the parsing lives here,
// beside positionToAxis and axisToPosition, rather than in a script that has
// to be told things the archive already knows.
//
// Two of those things are why this is not just the standalone script moved:
//
//   * THE YEAR. The paste has none. A script has to guess from the current
//     date; here the stream's own started_at settles it, so a January triage
//     of a December capture lands in December instead of eleven months out.
//   * THE ZONE. A wall clock means nothing without one, and the machine
//     running the conversion is not necessarily the one that wrote the notes.
//     `stream.tz_offset_min` is what the recorder observed AT THE TIME, so it
//     is right across DST and across travel, which a local-timezone guess is
//     not.
// ---------------------------------------------------------------------------

/* Accepts the shape an iPhone Action Button shortcut emits, and the obvious
   neighbours of it: an optional leading `@`, an optional year, `.` `-` or `/`
   between date parts, HH:MM or HH:MM:SS, and any of the dashes a phone
   keyboard might produce between the stamp and the words.

   Anchored at both ends and deliberately strict. A line that is nearly a
   timestamp is reported as skipped rather than guessed at — the same rule
   parseNoteLine follows, and for the same reason: a silently wrong number is
   the one failure nobody notices. */
const PHONE_RE = new RegExp(String.raw`^\s*@?\s*`
  + String.raw`(?:(?<year>\d{4})[.\-/])?`
  + String.raw`(?<a>\d{1,2})[.\-/](?<b>\d{1,2})\.?\s+`
  + String.raw`(?<h>\d{1,2}):(?<mi>\d{2})(?::(?<s>\d{2}))?`
  + String.raw`\s*[-‐-―~>|]+\s*`
  + String.raw`(?<text>.*)$`, 'u');

/** A wall clock in a fixed offset -> unix seconds.
 *
 *  Date.UTC gives the instant those numbers would name in UTC; subtracting the
 *  offset moves it to the zone they were actually written in. tzOffsetMin is
 *  minutes EAST of UTC, matching stream.tz_offset_min (-300 for UTC-5).
 */
export function wallToUnix(y, mo, d, h, mi, s, tzOffsetMin) {
  const utc = Date.UTC(y, mo - 1, d, h, mi, s);
  if (Number.isNaN(utc)) return null;
  /* Rejects 31 February rather than letting Date roll it into March. A rolled
     date is a plausible-looking number in the wrong place. */
  const back = new Date(utc);
  if (back.getUTCMonth() !== mo - 1 || back.getUTCDate() !== d) return null;
  return utc / 1000 - tzOffsetMin * 60;
}

/** One pasted line -> { unix, text } | null.
 *
 *  `near` is the stream's started_at, and it does two jobs: it picks the year
 *  the paste omitted, and it decides MM.DD against DD.MM when the numbers are
 *  ambiguous. Both are resolved by proximity, because a note is written while
 *  the stream is running — hours away at worst, never months.
 */
export function parsePhoneLine(line, { near, tzOffsetMin = 0, dayFirst = null } = {}) {
  const m = PHONE_RE.exec(String(line ?? ''));
  if (!m) return null;
  const g = m.groups;
  const text = g.text.trim();
  const h = Number(g.h), mi = Number(g.mi), s = Number(g.s ?? 0);
  if (h > 23 || mi > 59 || s > 59) return null;
  const a = Number(g.a), b = Number(g.b);

  /* Both readings, then whichever lands nearer the stream. `21.09` can only be
     day-first and needs no vote; `09.11` is genuinely ambiguous and proximity
     is the only evidence there is. An explicit dayFirst overrides both. */
  const orders = dayFirst === true ? [[b, a]] : dayFirst === false ? [[a, b]] : [[a, b], [b, a]];
  const years = g.year ? [Number(g.year)]
    : (() => { const y = new Date(near * 1000).getUTCFullYear(); return [y, y - 1, y + 1]; })();

  let best = null;
  for (const [mo, d] of orders) {
    if (mo < 1 || mo > 12 || d < 1 || d > 31) continue;
    for (const y of years) {
      const unix = wallToUnix(y, mo, d, h, mi, s, tzOffsetMin);
      if (unix === null) continue;
      const off = Math.abs(unix - near);
      if (!best || off < best.off) best = { off, unix };
    }
  }
  return best ? { unix: best.unix, text } : null;
}

/** Seconds -> `HH:MM:SS,mmm`. Negative clamps to zero: a note taken before the
 *  recording started is still about this stream, and an NLE cannot show a cue
 *  at a negative time. */
export function srtTime(sec) {
  const ms = Math.max(0, Math.round(sec * 1000));
  const h = Math.floor(ms / 3600000);
  const mi = Math.floor((ms % 3600000) / 60000);
  const s = Math.floor((ms % 60000) / 1000);
  return `${pad2(h)}:${pad2(mi)}:${pad2(s)},${String(ms % 1000).padStart(3, '0')}`;
}
const pad2 = (n) => String(n).padStart(2, '0');

/** The shortest cue anybody can read, and the floor two notes a second apart
 *  would otherwise fall below — a zero-length cue is one an NLE draws as
 *  nothing at all. */
export const CUE_MIN_S = 0.8;

/** Cues -> an .srt document.
 *
 *  Fixed length by default, `dur` seconds. Running each cue to the next one
 *  sounded better than it is: two notes an hour apart make an hour-long
 *  caption, and in an NLE the track is drawn as blocks you can see, so a gap
 *  already says "nothing marked here" without a caption stretched over it.
 *
 *  `gap: true` is the other behaviour, for when a continuous band is what you
 *  want. Either way the end is clamped to the next cue's start, so two notes
 *  ten seconds apart do not overlap into a stack an editor has to untangle.
 */
export function buildSrt(cues, { gap = false, dur = 10 } = {}) {
  const rows = cues
    .filter((c) => c.at !== null && c.at !== undefined && Number.isFinite(c.at))
    .map((c) => ({ at: Math.max(0, c.at), text: String(c.text ?? '').trim() }))
    .filter((c) => c.text)
    .sort((x, y) => x.at - y.at);

  const out = [];
  rows.forEach((c, i) => {
    const next = rows[i + 1]?.at;
    /* The last cue has no next, so it takes `dur` whatever the mode — running
       it to infinity is not a thing SRT can say, and a caption that never
       clears is worse than one that does. */
    let end = gap && next !== undefined ? next : c.at + dur;
    /* Clamped either way. A fixed 10s on notes 3s apart would overlap, and
       overlapping cues are drawn stacked — something to untangle rather than
       read. The floor below then keeps a clamped cue long enough to see. */
    if (next !== undefined && end > next) end = next;
    if (end < c.at + CUE_MIN_S) end = c.at + CUE_MIN_S;
    out.push(`${i + 1}\n${srtTime(c.at)} --> ${srtTime(end)}\n${c.text}\n`);
  });
  /* CRLF and a BOM, because the target is Premiere on Windows: without the BOM
     it reads a UTF-8 file as the system code page and every non-ASCII name in
     the track comes out as mojibake. */
  return out.length ? '﻿' + out.join('\n') : '';
}
