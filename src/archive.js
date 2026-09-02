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

import { statSync } from 'node:fs';
import { resolve, sep } from 'node:path';
import { bit, bumpGeneration, isUlid, now, slugify, tx, ulid } from './db.js';

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

// The one vocabulary. A tag is filed under one of these and a block of time is
// coloured by one of these, and they are the same list because "what is this
// tag" and "what is this block" are the same question asked twice.
//
//   game    a specific title being played
//   person  a guest, a member, a character the block is about
//   type    what kind of stream this stretch is — collab, karaoke, zatsudan,
//           superchat reading, a creative or event block
//   meta    the scaffolding around it — intro, outro, break, waiting screen
//
// Closed, because it is a colour and a colour has to mean the same thing in
// every stream in the archive. Extending it is a deliberate edit to this line,
// not a typo in a text field.
export const KINDS = ['game', 'person', 'type', 'meta'];

// ...plus the sentinel. 'unknown' is NOT 'meta': "nobody has labelled this" and
// "this is a break" are different claims, and only one of them tells a viewer
// to skip. It gets a hatch rather than a swatch for the same reason.
export const SEGMENT_KINDS = [...KINDS, 'unknown'];

// The snippet vocabulary, and a separate list on purpose — see the block
// comment on `taglet` in schema.sql. Danbooru's namespaces, because they are
// the ones that survive a thousand short clips:
//
//   character   who is in it        blue
//   copyright   what it belongs to  violet
//   meta        what it is like     yellow
//   general     everything else     grey
//
// 'general' is the default rather than a sentinel like segment's 'unknown':
// an untyped taglet is a perfectly ordinary taglet, not a gap in the record.
export const TAGLET_KINDS = ['character', 'copyright', 'meta', 'general'];

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

export function servedType(container, vcodec, acodec) {
  const c = String(container || '').toLowerCase();
  const v = String(vcodec || '').toLowerCase();
  // No audio track is fine and common for a short clip; an unknown one is not.
  const aOk = (set) => !acodec || set.has(String(acodec).toLowerCase());

  if (c.includes('matroska') || c.includes('webm')) {
    // A Matroska file carrying VP9/Opus is byte-for-byte a WebM file whatever
    // it is named, so an .mkv here needs a correct header and nothing else.
    return WEBM_VIDEO.has(v) && aOk(WEBM_AUDIO) ? 'video/webm' : null;
  }
  if (c.includes('mp4') || c.includes('mov') || c.includes('m4v')) {
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
    kind: SEGMENT_KINDS.includes(r.kind) ? r.kind : 'unknown',
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
    return { tiled: false, segments: placed };
  }

  const out = [];
  const push = (start, end, seg) => {
    const a = Math.max(0, Math.min(start, domain));
    const b = Math.max(a, Math.min(end, domain));
    if (b - a <= 0) return;
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

  /* Clamped to the domain like everything else, but never filled and never
     merged. A lane-1 block with no end runs nowhere — unlike lane 0, where a
     missing end means "until the next one", because there is no continuity to
     inherit from inside a game. */
  for (const s of lane1) {
    const a = Math.max(0, Math.min(s.start_s, domain));
    const b = Math.max(a, Math.min(s.end_s ?? s.start_s, domain));
    if (b - a > 0) out.push({ ...s, start_s: a, end_s: b });
  }

  return { tiled: true, segments: out };
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

  const { tiled, segments } = projectSegments(segRows, capsById, started, domain);

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
    notes: notes.map((n) => projectNote(n, capsById, started, leadCap)),
    coverage,
    counts: {
      notes: notes.length,
      notes_unknown_frame: notes.filter((n) => n.frame === 'unknown').length,
      segments: segRows.length,
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
  },
  // The junction is a write target in its own right — attaching a tag to a
  // stream is a decision like any other, and it needs a row in the log saying
  // who decided it.
  stream_tag: { stream_id: 'text', tag_id: 'text' },
  snippet: {
    title: 'text', summary: 'text', video_path: 'text', poster_path: 'text',
    source_stream_id: 'text', source_offset_s: 'int', status: 'text',
    // duration_s, width, height and bytes are measurements of a file. A human
    // correcting them by hand would be describing something other than what is
    // on disk, so they are set by the importer and read-only after.
    //
    // transcript is likewise derived — from snippet_line, whole, on each pass.
    // Making it writable would let an edit survive as the search index while
    // the lines under it said something else.
  },
  taglet: { name: 'text', slug: 'text', kind: 'text', summary: 'text', status: 'text' },
  snippet_taglet: { snippet_id: 'text', taglet_id: 'text' },
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
  snippet: ['title', 'video_path'],
  taglet: ['name'],
  snippet_taglet: ['snippet_id', 'taglet_id'],
};

// Streams, notes and segments are tombstoned; captures and tags are genuinely
// removable because nothing external points at them.
// stream_tag is genuinely removable — untagging is not a claim worth a
// tombstone, and the changeset that did it is already the record.
// A snippet is content and tombstones like a stream. A taglet tombstones like
// a tag; snippet_taglet is a junction and removes cleanly, same as stream_tag.
const TOMBSTONED = new Set(['stream', 'note', 'segment', 'tag', 'snippet', 'taglet']);
const OPS = new Set(['create', 'update', 'delete']);

// Closed vocabularies, checked at validate time so a typo cannot become a
// colour nobody has a swatch for.
const ENUMS = {
  'segment.kind': SEGMENT_KINDS,
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
  // Same list as segment.kind, and that is the point — see KINDS. A tag may be
  // 'unknown' too: minting one from the autocomplete miss should not force a
  // category out of somebody who was in the middle of doing something else.
  'tag.kind': SEGMENT_KINDS,
  'tag.status': ['proposed', 'confirmed'],
  'taglet.kind': TAGLET_KINDS,
  'taglet.status': ['proposed', 'confirmed'],
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
};

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

/** Normalise and check a proposed change list. Throws ChangeError. */
export function validate(db, changes) {
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
  return out;
}

/** Record a changeset. Applies it immediately when the author may. */
export function propose(db, { authorId = null, reason = null, changes, autoApply = false,
                              mediaRoot = null }) {
  const list = validate(db, changes);
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

  if (autoApply) {
    apply(db, csId, { reviewerId: authorId, note: 'author may edit directly', mediaRoot });
  } else {
    // A proposal is a write. It adds rows the read API serves — the review
    // queue, the stream's history, the "2 open suggestions" badge — and
    // `generation` is what every ETag is keyed on. Without this bump the
    // detail endpoint keeps answering 304 with a body that predates the
    // suggestion, so the person who just submitted one sees no trace of it
    // anywhere. apply() bumps for the same reason; propose() simply never did.
    bumpGeneration(db);
  }
  return summary(db, csId);
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
    const merged = [];                  // tag creates folded into an existing row
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
    // Written once over both vocabularies rather than twice: `tag` and `taglet`
    // are different lists answering different questions, but they collide in
    // exactly the same way and a second copy of this is a second place for the
    // pending-INSERT rewrite below to be forgotten.
    for (const vocab of ['tag', 'taglet']) {
      for (const [tid, c] of [...creates]) {
        if (c.type !== vocab || !c.fields.name) continue;
        const slug = slugify(c.fields.name);
        const existing = db.prepare(`SELECT id FROM ${vocab} WHERE slug = ?`).get(slug);
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
        record(vocab, existing.id, 'name', c.fields.name, c.fields.name);
        merged.push({ wanted: tid, resolved_to: existing.id, slug });
      }
    }

    // Same idea one level up: two people tagging the same thing with the same
    // thing is not a conflict, it is agreement. The pair already being there
    // means the changeset's intent is satisfied, so drop the insert rather than
    // failing the UNIQUE and rolling back everything else in it.
    for (const [jt, [a, b]] of Object.entries({
      stream_tag: ['stream_id', 'tag_id'],
      snippet_taglet: ['snippet_id', 'taglet_id'],
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
      if (tt === 'tag' || tt === 'taglet') {
        // slug is DERIVED. Letting a client send both is how a tag ends up
        // named one thing and matched by another.
        fields.slug = slugify(fields.name);
        fields.origin = 'user';
        fields.author_id = cs.author_id;
        // Anyone may mint a tag; only an editor's goes straight into everyone's
        // autocomplete. A suggester's stays 'proposed' — usable on their own
        // suggestion, invisible in the picker — until someone confirms it.
        // Same rule as auto-apply, read from the same place.
        fields.status = authorMayEdit(db, cs.author_id) ? 'confirmed' : 'proposed';
        // No guess at a category. A row minted from an autocomplete miss has
        // none until someone gives it one, and picking the most common one for
        // them is inference from a name — the same move as reading a game off a
        // stream title. The two vocabularies spell "uncategorised" differently:
        // a segment kind is a colour and 'unknown' earns a hatch, while an
        // untyped taglet is an ordinary taglet.
        if (!fields.kind) fields.kind = tt === 'taglet' ? 'general' : 'unknown';
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

    for (const c of changes) {
      if (c.op === 'create') continue;
      if (c.op === 'update') {
        db.prepare(`UPDATE ${c.target_type} SET ${c.field} = ?, updated_at = ? WHERE id = ?`)
          .run(c.value, t, c.target_id);
        // A rename that leaves the slug behind means the row answers to its old
        // URL and matches on its old spelling forever.
        if ((c.target_type === 'tag' || c.target_type === 'taglet') && c.field === 'name') {
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

  for (const sid of touched) {
    if (db.prepare('SELECT 1 FROM stream WHERE id = ?').get(sid)) {
      recompute(db, sid, { mediaRoot });
    }
  }
  bumpGeneration(db);
  return summary(db, csId);
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

/** May this person's own changesets apply on submission? Read for the same
 *  reason autoApply is: an editor's decisions are decisions, everyone else's
 *  are proposals. */
function authorMayEdit(db, personId) {
  if (!personId) return false;
  const r = db.prepare('SELECT role FROM person WHERE id = ?').get(personId);
  return ['editor', 'admin'].includes(r?.role);
}

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
