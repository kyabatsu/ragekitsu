// The theater's additions: clocks, frames, precision, segments, and the three
// places a stored timestamp can silently change meaning.
//
// These are guard-rail tests rather than coverage tests. Everything here is
// something that would fail quietly — a note four minutes off, a strip scaled
// to the wrong domain, a link into the wrong capture — rather than throwing, so
// nothing but a test is going to notice.
//
//   node --test test/clocks.test.js

import { test, before, after, describe } from 'node:test';
import assert from 'node:assert/strict';
import { copyFileSync, existsSync, rmSync, mkdirSync, mkdtempSync, utimesSync,
         writeFileSync } from 'node:fs';
import { execFileSync, spawnSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = join(HERE, '..');
const SEED = join(ROOT, 'data', 'archive.db');
const DB = join(ROOT, 'data', 'clocks-test.db');
const TOKEN = 'clock-test-token';

process.env.TENMA_DB = DB;
process.env.TENMA_DEV_AUTH = '1';
process.env.TENMA_INGEST_TOKEN = TOKEN;
process.env.TENMA_MEDIA_ROOT = '';

const { makeApp } = await import('../server.js');
const { ulid, open } = await import('../db.js');
const {
  KINDS, SEGMENT_KINDS, axisOf, axisToPosition, buildTimeline, clocksOf,
  positionToAxis, projectSegments, segmentOverlaps, watchSources, recompute,
} = await import('../archive.js');

let server, base, app;

function client(token = null) {
  const call = async (method, path, body, extra = {}) => {
    const headers = { ...extra };
    if (token) headers.authorization = `Bearer ${token}`;
    if (body !== undefined) headers['content-type'] = 'application/json';
    const res = await fetch(base + path, {
      method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
    let json = null;
    try { json = await res.json(); } catch { /* 304 */ }
    return { status: res.status, headers: res.headers, body: json };
  };
  return { get: (p, h) => call('GET', p, undefined, h),
           post: (p, b, h) => call('POST', p, b ?? {}, h) };
}
const anon = () => client();

async function login(handle, role) {
  const r = await anon().post('/api/auth/token', { handle, role });
  assert.equal(r.status, 200, JSON.stringify(r.body));
  return client(r.body.token);
}

before(async () => {
  for (const f of [DB, DB + '-wal', DB + '-shm']) if (existsSync(f)) rmSync(f);
  mkdirSync(dirname(DB), { recursive: true });
  copyFileSync(SEED, DB);
  app = makeApp();
  await new Promise((r) => { server = app.listen(0, '127.0.0.1', r); });
  base = `http://127.0.0.1:${server.address().port}`;
});

after(() => {
  server?.close();
  app?.locals?.close?.();
  for (const f of [DB, DB + '-wal', DB + '-shm']) if (existsSync(f)) rmSync(f);
});

// ===========================================================================
describe('clocks — the conversion, in isolation', () => {
// ===========================================================================

  // The worked example: a note dropped at 02:30:00 while watching the YouTube
  // embed, on a stream whose recording began 30s after YouTube's. It reads
  // 02:30:00 on YouTube forever, and 02:29:30 on the recording. Neither number
  // is stored; both are the same wall moment seen from two clocks.
  const STARTED = 1700000000;
  const yt = { id: 'CAPYT', platform: 'YT', remote_start_wall: STARTED,
               local_start_wall: STARTED + 30 };

  test('a position converts to the axis and back without drifting', () => {
    const axis = positionToAxis(yt, STARTED, 'remote', 9000);
    assert.equal(axis, 9000);
    assert.equal(axisToPosition(yt, STARTED, 'remote', axis), 9000);
  });

  test('the same moment reads differently on the recording', () => {
    const axis = positionToAxis(yt, STARTED, 'remote', 9000);   // 02:30:00 on YT
    assert.equal(axisToPosition(yt, STARTED, 'local', axis), 8970); // 02:29:30 local
  });

  test('a clock nobody has measured refuses rather than answering zero', () => {
    const tw = { id: 'CAPTW', platform: 'TW', remote_start_wall: STARTED - 120,
                 local_start_wall: null };
    assert.equal(positionToAxis(tw, STARTED, 'local', 60), null);
    // ...but its remote clock is fine, and starts before the axis zero.
    assert.equal(positionToAxis(tw, STARTED, 'remote', 0), -120);
  });

  test('clocksOf falls back to the pre-migration reading rather than to zero', () => {
    const old = { id: 'X', offset_s: 240, remote_start_wall: null, local_start_wall: null };
    assert.equal(clocksOf(old, STARTED).remote, STARTED + 240);
  });

  test('an unknown frame is carried at face value, never promoted to exact', () => {
    const caps = new Map([[yt.id, yt]]);
    const vault = { frame: 'unknown', anchor_id: null, offset_s: 6372 };
    const pinned = { frame: 'capture', anchor_id: 'CAPYT', anchor_clock: 'local',
                     offset_s: 6372 };
    assert.equal(axisOf(vault, caps, STARTED), 6372);
    assert.equal(axisOf(pinned, caps, STARTED), 6402);   // +30, the recording lag
  });

  test('an anchor that no longer exists yields null, not a guess', () => {
    const orphan = { frame: 'capture', anchor_id: 'GONE', anchor_clock: 'remote',
                     offset_s: 100 };
    assert.equal(axisOf(orphan, new Map(), STARTED), null);
  });
});

// ===========================================================================
describe('segments', () => {
// ===========================================================================

  test('the strip tiles with no holes and no overhang', () => {
    const rows = [
      { id: 'b', kind: 'game', label: 'DS2', frame: 'stream', start_s: 2280, end_s: 9660 },
      { id: 'a', kind: 'type', label: 'zatsudan', frame: 'stream', start_s: 330, end_s: 2280 },
    ];
    const { tiled, segments } = projectSegments(rows, new Map(), 0, 15120);
    assert.equal(tiled, true);
    assert.equal(segments[0].start_s, 0);
    assert.equal(segments.at(-1).end_s, 15120);
    for (let i = 1; i < segments.length; i++) {
      assert.equal(segments[i].start_s, segments[i - 1].end_s, 'a hole or an overlap');
    }
    assert.equal(segments.filter((s) => s.synthetic).length, 2, 'head and tail filler');
    // Filler is unknown, not meta: nobody said this stretch was a break.
    assert.ok(segments.filter((s) => s.synthetic).every((s) => s.kind === 'unknown'));
  });

  test('a stream nobody has segmented is unlabelled, not marked skippable', () => {
    // The whole strip is one synthetic block. If that block were 'meta' the
    // archive would be telling viewers there is nothing here worth opening, on
    // every stream nobody has got to yet — which is most of them.
    const { segments } = projectSegments([], new Map(), 0, 15120);
    assert.equal(segments.length, 1);
    assert.equal(segments[0].kind, 'unknown');
    assert.equal(segments[0].synthetic, true);
    assert.equal(segments[0].start_s, 0);
    assert.equal(segments[0].end_s, 15120);
  });

  test('a segment with no end runs to the next one', () => {
    const rows = [
      { id: 'a', kind: 'type', frame: 'stream', start_s: 0, end_s: null },
      { id: 'b', kind: 'game', frame: 'stream', start_s: 600, end_s: 1200 },
    ];
    const { segments } = projectSegments(rows, new Map(), 0, 1200);
    assert.equal(segments.find((s) => s.id === 'a').end_s, 600);
  });

  test('with no known duration nothing is tiled and the caller is told', () => {
    const rows = [{ id: 'a', kind: 'type', frame: 'stream', start_s: 0, end_s: 600 }];
    const out = projectSegments(rows, new Map(), 0, null);
    assert.equal(out.tiled, false);
    assert.equal(out.segments.length, 1);
    assert.ok(!out.segments.some((s) => s.synthetic), 'invented a strip with no domain');
  });

  test('a word outside the vocabulary is refused, not stored', async () => {
    // The whole reason kind is closed: it is a colour, and a colour with no
    // swatch renders as a transparent block that reads as "unlabelled" — a
    // typo that turns into a false claim on the strip.
    const ed = await login('kind-ed', 'editor');
    const s = (await anon().get('/api/streams/idx/690')).body;
    const SEG = ulid();
    const r = await ed.post('/api/changesets', { changes: [
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'stream_id', value: s.id },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'start_s', value: 60 },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'kind', value: 'karaoke' },
    ] });
    assert.equal(r.status, 400);
    const TID = ulid();
    const t = await ed.post('/api/changesets', { changes: [
      { target_type: 'tag', target_id: TID, op: 'create', field: 'name', value: 'Kind Probe' },
      { target_type: 'tag', target_id: TID, op: 'create', field: 'kind', value: 'format' },
    ] });
    assert.equal(t.status, 400, 'tag.kind is still free text — "format" got in');
  });

  test('unknown is not meta, and the vocabulary is the one vocabulary', () => {
    // One list, shared by tag.kind and segment.kind, plus the sentinel.
    assert.deepEqual(KINDS, ['game', 'person', 'type', 'meta']);
    assert.deepEqual(SEGMENT_KINDS, ['game', 'person', 'type', 'meta', 'unknown']);
    assert.ok(SEGMENT_KINDS.includes('unknown'));
    assert.ok(SEGMENT_KINDS.includes('meta'));
    const { segments } = projectSegments(
      [{ id: 'a', kind: 'unknown', frame: 'stream', start_s: 0, end_s: 100 }],
      new Map(), 0, 100);
    assert.equal(segments[0].kind, 'unknown');
  });

  test('a kind outside the vocabulary is refused, not rendered as a new colour', async () => {
    const ed = await login('seg-ed', 'editor');
    const s = (await anon().get('/api/streams/idx/685')).body;
    const r = await ed.post('/api/changesets', { changes: [
      { target_type: 'segment', target_id: ulid(), op: 'create', field: 'stream_id', value: s.id },
      { target_type: 'segment', target_id: ulid(), op: 'create', field: 'kind', value: 'karaoke' },
    ] });
    assert.equal(r.status, 400);
    assert.match(r.body.error, /kind must be one of/);
  });

  test('a segment lands through the changeset path and reaches the strip', async () => {
    const ed = await login('seg-ed2', 'editor');
    const s = (await anon().get('/api/streams/idx/685')).body;
    const SEG = ulid();
    const r = await ed.post('/api/changesets', {
      reason: 'first block',
      changes: [
        { target_type: 'segment', target_id: SEG, op: 'create', field: 'stream_id', value: s.id },
        { target_type: 'segment', target_id: SEG, op: 'create', field: 'start_s', value: 600 },
        { target_type: 'segment', target_id: SEG, op: 'create', field: 'end_s', value: 3600 },
        { target_type: 'segment', target_id: SEG, op: 'create', field: 'kind', value: 'game' },
        { target_type: 'segment', target_id: SEG, op: 'create', field: 'label', value: 'Super Metroid' },
      ] });
    assert.equal(r.body.status, 'applied', JSON.stringify(r.body));

    const detail = (await anon().get(`/api/streams/${s.id}`)).body;
    const mine = detail.segments.find((x) => x.id === SEG);
    assert.ok(mine, 'the segment never reached the strip');
    assert.equal(mine.kind, 'game');
    assert.equal(mine.label, 'Super Metroid');
    assert.equal(mine.origin, 'user');
    assert.equal(mine.author, 'seg-ed2');
    // and the projection filled the gap in front of it, as unlabelled rather
    // than as dead air
    assert.equal(detail.segments[0].start_s, 0);
    assert.equal(detail.segments[0].synthetic, true);
    assert.equal(detail.segments[0].kind, 'unknown');
  });

  test('overlapping segments are refused on the final state', async () => {
    const ed = await login('seg-ed3', 'editor');
    const s = (await anon().get('/api/streams/idx/686')).body;
    const mk = async (a, b) => {
      const id = ulid();
      return ed.post('/api/changesets', { changes: [
        { target_type: 'segment', target_id: id, op: 'create', field: 'stream_id', value: s.id },
        { target_type: 'segment', target_id: id, op: 'create', field: 'start_s', value: a },
        { target_type: 'segment', target_id: id, op: 'create', field: 'end_s', value: b },
        { target_type: 'segment', target_id: id, op: 'create', field: 'kind', value: 'type' },
      ] });
    };
    assert.equal((await mk(0, 1000)).body.status, 'applied');
    const clash = await mk(900, 2000);
    assert.equal(clash.status, 409);
    assert.equal(clash.body.error, 'segments would overlap once applied');
    assert.equal(clash.body.overlaps[0].overlap_s, 100);
    // and nothing was left behind by the rejected transaction
    const detail = (await anon().get(`/api/streams/${s.id}`)).body;
    assert.equal(detail.segments.filter((x) => !x.synthetic).length, 1);
  });

  test('a whole strip shifted at once is allowed, though it overlaps mid-flight', async () => {
    const ed = await login('seg-ed4', 'editor');
    const s = (await anon().get('/api/streams/idx/687')).body;
    const A = ulid(), B = ulid();
    const create = (id, a, b) => ([
      { target_type: 'segment', target_id: id, op: 'create', field: 'stream_id', value: s.id },
      { target_type: 'segment', target_id: id, op: 'create', field: 'start_s', value: a },
      { target_type: 'segment', target_id: id, op: 'create', field: 'end_s', value: b },
      { target_type: 'segment', target_id: id, op: 'create', field: 'kind', value: 'type' },
    ]);
    assert.equal((await ed.post('/api/changesets',
      { changes: [...create(A, 0, 1000), ...create(B, 1000, 2000)] })).body.status, 'applied');

    // Move both back 240s in one changeset. Applied row by row, A's new end
    // (760) never clashes — but if the check ran per change instead of on the
    // final state, a shift the other way would be rejected outright.
    const shift = await ed.post('/api/changesets', { reason: 'sync to the VOD', changes: [
      { target_type: 'segment', target_id: B, op: 'update', field: 'start_s', value: 760 },
      { target_type: 'segment', target_id: B, op: 'update', field: 'end_s', value: 1760 },
      { target_type: 'segment', target_id: A, op: 'update', field: 'start_s', value: -240 },
      { target_type: 'segment', target_id: A, op: 'update', field: 'end_s', value: 760 },
    ] });
    assert.equal(shift.body.status, 'applied', JSON.stringify(shift.body));
  });
});

// ===========================================================================
describe('no silent shifts', () => {
// ===========================================================================

  test('deleting a capture rewrites its dependents, in the open', async () => {
    const ed = await login('shift-ed', 'editor');
    // A stream of our own, so the fixture archive is not disturbed.
    const SID = ulid(), CID = ulid(), NID = ulid();
    const T = 1799000000;
    let r = await ed.post('/api/changesets', { reason: 'fixture', changes: [
      { target_type: 'stream', target_id: SID, op: 'create', field: 'title', value: 'anchor fixture' },
      { target_type: 'stream', target_id: SID, op: 'create', field: 'started_at', value: T },
      { target_type: 'stream', target_id: SID, op: 'create', field: 'duration_s', value: 7200 },
    ] });
    assert.equal(r.body.status, 'applied', JSON.stringify(r.body));

    r = await ed.post('/api/changesets', { reason: 'a capture 240s late', changes: [
      { target_type: 'capture', target_id: CID, op: 'create', field: 'stream_id', value: SID },
      { target_type: 'capture', target_id: CID, op: 'create', field: 'platform', value: 'TW' },
      { target_type: 'capture', target_id: CID, op: 'create', field: 'remote_id', value: 'ANCHORTW' },
      { target_type: 'capture', target_id: CID, op: 'create', field: 'remote_start_wall', value: T + 240 },
    ] });
    assert.equal(r.body.status, 'applied', JSON.stringify(r.body));

    r = await ed.post('/api/changesets', { reason: 'note measured in that capture', changes: [
      { target_type: 'note', target_id: NID, op: 'create', field: 'stream_id', value: SID },
      { target_type: 'note', target_id: NID, op: 'create', field: 'text', value: 'measured on twitch' },
      { target_type: 'note', target_id: NID, op: 'create', field: 'offset_s', value: 1000 },
      { target_type: 'note', target_id: NID, op: 'create', field: 'anchor_id', value: CID },
    ] });
    assert.equal(r.body.status, 'applied', JSON.stringify(r.body));

    let detail = (await anon().get(`/api/streams/${SID}`)).body;
    let note = detail.notes.find((n) => n.id === NID);
    assert.equal(note.frame, 'capture', 'anchor_id alone should imply frame=capture');
    assert.equal(note.start_s, 1240, '1000s into a capture that starts 240s in');

    // Now delete the capture out from under it.
    const del = await ed.post('/api/changesets', { reason: 'wrong capture', changes: [
      { target_type: 'capture', target_id: CID, op: 'delete' } ] });
    assert.equal(del.body.status, 'applied', JSON.stringify(del.body));

    detail = (await anon().get(`/api/streams/${SID}`)).body;
    note = detail.notes.find((n) => n.id === NID);
    assert.equal(note.start_s, 1240, 'the note moved when its anchor was deleted');
    assert.equal(note.frame, 'stream');
    assert.equal(note.offset_s, 1240, 'it should have been rewritten, not reinterpreted');

    // ...and the rewrite is in the history, not merely done.
    const rewrite = del.body.changes.find(
      (c) => c.target_id === NID && c.field === 'offset_s');
    assert.ok(rewrite, 'the conversion left no trace in the changeset');
    assert.equal(rewrite.base_value, '1000');
    assert.equal(rewrite.value, '1240');
  });

  test('correcting a start time changes the start time and nothing else', async () => {
    // This used to cascade: moving the axis zero rewrote every unanchored note
    // and segment so they held the same wall moment. It does not any more.
    //
    // Correcting a start time corrects ONE fact — when the broadcast began —
    // and the numbers on the notes were never measured against that fact. A
    // vault note reading 02:30:00 was read off a video and is still 2h30m into
    // that video after somebody discovers the stream began five minutes
    // earlier. Rewriting it would change a number nobody has any better
    // information about, which is the opposite of a correction.
    const ed = await login('shift-ed2', 'editor');
    const SID = ulid(), NID = ulid(), UID = ulid(), SEG = ulid();
    const T = 1799500000;
    await ed.post('/api/changesets', { changes: [
      { target_type: 'stream', target_id: SID, op: 'create', field: 'title', value: 'axis fixture' },
      { target_type: 'stream', target_id: SID, op: 'create', field: 'started_at', value: T },
      { target_type: 'stream', target_id: SID, op: 'create', field: 'duration_s', value: 7200 },
    ] });
    await ed.post('/api/changesets', { changes: [
      { target_type: 'note', target_id: NID, op: 'create', field: 'stream_id', value: SID },
      { target_type: 'note', target_id: NID, op: 'create', field: 'text', value: 'on the axis' },
      { target_type: 'note', target_id: NID, op: 'create', field: 'offset_s', value: 600 },
      { target_type: 'note', target_id: NID, op: 'create', field: 'frame', value: 'stream' },
      { target_type: 'note', target_id: UID, op: 'create', field: 'stream_id', value: SID },
      { target_type: 'note', target_id: UID, op: 'create', field: 'text', value: 'off a vod' },
      { target_type: 'note', target_id: UID, op: 'create', field: 'offset_s', value: 9000 },
      { target_type: 'note', target_id: UID, op: 'create', field: 'frame', value: 'unknown' },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'stream_id', value: SID },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'frame', value: 'stream' },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'start_s', value: 100 },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'end_s', value: 500 },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'kind', value: 'meta' },
    ] });

    const move = await ed.post('/api/changesets', { reason: 'started 120s earlier', changes: [
      { target_type: 'stream', target_id: SID, op: 'update', field: 'started_at', value: T - 120 },
    ] });
    assert.equal(move.body.status, 'applied', JSON.stringify(move.body));
    assert.equal(move.body.changes.length, 1,
      'correcting a start time wrote more than the start time: '
      + JSON.stringify(move.body.changes.map((c) => `${c.target_type}.${c.field}`)));

    const detail = (await anon().get(`/api/streams/${SID}`)).body;
    assert.equal(detail.started_at, T - 120);
    assert.equal(detail.notes.find((n) => n.id === NID).offset_s, 600);
    assert.equal(detail.notes.find((n) => n.id === UID).offset_s, 9000,
      'a number nobody understands was rewritten anyway');
    const seg = detail.segments.find((x) => x.id === SEG);
    assert.deepEqual([seg.start_s, seg.end_s], [100, 500]);
  });

  test('a capture that still anchors something cannot be re-parented', async () => {
    const ed = await login('shift-ed3', 'editor');
    const SID = ulid(), CID = ulid(), NID = ulid();
    const T = 1799900000;
    await ed.post('/api/changesets', { changes: [
      { target_type: 'stream', target_id: SID, op: 'create', field: 'title', value: 'reparent fixture' },
      { target_type: 'stream', target_id: SID, op: 'create', field: 'started_at', value: T },
    ] });
    await ed.post('/api/changesets', { changes: [
      { target_type: 'capture', target_id: CID, op: 'create', field: 'stream_id', value: SID },
      { target_type: 'capture', target_id: CID, op: 'create', field: 'platform', value: 'YT' },
      { target_type: 'capture', target_id: CID, op: 'create', field: 'remote_id', value: 'REPARENT1' },
    ] });
    await ed.post('/api/changesets', { changes: [
      { target_type: 'note', target_id: NID, op: 'create', field: 'stream_id', value: SID },
      { target_type: 'note', target_id: NID, op: 'create', field: 'text', value: 'held' },
      { target_type: 'note', target_id: NID, op: 'create', field: 'offset_s', value: 10 },
      { target_type: 'note', target_id: NID, op: 'create', field: 'anchor_id', value: CID },
    ] });

    const other = (await anon().get('/api/streams/idx/685')).body.id;
    const r = await ed.post('/api/changesets', { changes: [
      { target_type: 'capture', target_id: CID, op: 'update', field: 'stream_id', value: other },
    ] });
    assert.equal(r.status, 400);
    assert.match(r.body.error, /anchors 1 note/);
  });

  test('offset_s is derived and cannot be written directly', async () => {
    const ed = await login('shift-ed4', 'editor');
    const cap = (await anon().get('/api/streams/idx/685')).body.captures[0];
    const r = await ed.post('/api/changesets', { changes: [
      { target_type: 'capture', target_id: cap.id, op: 'update', field: 'offset_s', value: 99 },
    ] });
    assert.equal(r.status, 400);
    assert.match(r.body.error, /not writable/);
  });
});

// ===========================================================================
describe('precision', () => {
// ===========================================================================

  test('every vault note is unknown-framed and says how wrong it may be', async () => {
    const r = await anon().get('/api/streams/idx/685');
    const vault = r.body.notes.filter((n) => n.origin === 'vault');
    assert.ok(vault.length, 'no vault notes to check');
    assert.ok(vault.every((n) => n.frame === 'unknown'));
    assert.ok(vault.every((n) => n.precision_s === 120));
    assert.ok(vault.every((n) => n.exact === false));
    assert.ok(vault.every((n) => n.link_exact === false),
      'an unknown-frame link should not claim to be exact');
  });

  test('health counts the unknown frames, so the correction has a progress bar', async () => {
    const h = (await anon().get('/api/health')).body;
    assert.ok(h.notes_unknown_frame > 800);
    assert.ok('captures_unprobed' in h);
    assert.ok('captures_out_of_span' in h);
  });

  test('the mispaired captures are listable, not just countable', async () => {
    const r = await anon().get('/api/health/out-of-span');
    assert.ok(r.body.captures.length >= 5);
    assert.ok(Math.abs(r.body.captures[0].offset_s) > 3600);
  });
});

// ===========================================================================
describe('the theater payload', () => {
// ===========================================================================

  test('every source carries the one number needed to switch without losing the moment', async () => {
    const r = await anon().get('/api/streams/idx/685');
    assert.ok(r.body.sources.length >= 2);
    for (const s of r.body.sources) {
      assert.ok('capture_id' in s, 'a source with no capture_id cannot be converted');
      assert.ok('start_wall' in s);
      assert.ok('covers_s' in s);
      assert.ok(['present', 'dead', 'unverified', 'local-only'].includes(s.available));
    }
    assert.equal(r.body.lead, r.body.sources.find((s) => s.embeddable).capture_id);
  });

  test('the lead resolves by capture id even when two captures share a remote_id', () => {
    // Three duplicate remote_ids exist in the archive (TW/316096536551,
    // YT/XXBLiG5LI_8, YT/sg_bw6X4YV4) — the schema keeps ix_capture_remote
    // non-unique because of them. They all sit on DIFFERENT streams, though,
    // and UNIQUE(stream_id, platform) means one stream can never hold two
    // captures on one platform. So matching the lead back by remote_id has
    // never actually picked the wrong row.
    //
    // It is one constraint away from doing so. Giving an archive-channel
    // mirror its own capture row — which it needs, because a trimmed re-upload
    // has its own zero point — means relaxing that UNIQUE to include remote_id,
    // and the day that lands, remote_id stops identifying a capture. This test
    // is the tripwire: it builds the shape that change would create and checks
    // the resolution still lands on the right clock.
    const started = 1700000000;
    const caps = [
      { id: 'CAP_A', platform: 'YT', remote_id: 'DUPE1', url: null,
        remote_start_wall: started, file_duration_s: 3600, alive: null },
      { id: 'CAP_B', platform: 'YT', remote_id: 'DUPE1', url: null,
        remote_start_wall: started + 900, file_duration_s: 2700, alive: null },
    ];
    const chain = watchSources(caps, null);
    assert.equal(chain.length, 2, 'both captures should offer a source');
    assert.equal(chain[0].capture_id, 'CAP_A');
    assert.equal(chain[1].capture_id, 'CAP_B');

    const byId = new Map(caps.map((c) => [c.id, c]));
    for (const w of chain) {
      const cap = byId.get(w.capture_id);
      // The bug being guarded against: caps.find(c => c.remote_id === w.id)
      // returns CAP_A for both, so CAP_B's source would be handed CAP_A's
      // clock and every timestamp on it would be fifteen minutes out.
      const wrong = caps.find((c) => c.remote_id === w.id);
      assert.equal(cap.id, w.capture_id);
      if (w.capture_id === 'CAP_B') {
        assert.notEqual(wrong.id, cap.id, 'the remote_id match should differ here');
        assert.equal(clocksOf(cap, started).remote - clocksOf(wrong, started).remote, 900);
      }
    }
  });

  test('every lead names a capture that is actually on the stream', async () => {
    const r = await anon().get('/api/streams?limit=100');
    for (const s of r.body.streams) {
      if (!s.lead) continue;
      assert.ok(s.captures.some((c) => c.id === s.lead), `lead ${s.lead} is not a capture`);
    }
  });

  test('the constraint that makes remote_id safe today still holds', () => {
    // If this ever fails, the mirror change has landed and the resolution above
    // is the only thing keeping links pointed at the right clock.
    const db = open(DB, { readonly: true });
    const clash = db.prepare(
      `SELECT COUNT(*) c FROM (SELECT stream_id, platform FROM capture
        GROUP BY 1, 2 HAVING COUNT(*) > 1)`).get().c;
    db.close();
    assert.equal(clash, 0);
  });

  test('the axis says what it is standing on', async () => {
    const r = await anon().get('/api/streams/idx/685');
    assert.equal(r.body.axis.zero_wall, r.body.started_at);
    assert.equal(r.body.axis.domain_s, r.body.duration_s);
    assert.equal(r.body.axis.duration_source, 'stated');   // nothing has probed it
  });

  test('a stream with no duration reports no domain rather than a made-up one', async () => {
    const db = open(DB, { readonly: true });
    const row = db.prepare(
      `SELECT idx FROM stream WHERE duration_s IS NULL AND retracted_at IS NULL
         AND idx IS NOT NULL LIMIT 1`).get();
    db.close();
    const r = await anon().get(`/api/streams/idx/${row.idx}`);
    assert.equal(r.body.axis.domain_s, null);
    assert.equal(r.body.axis.duration_source, null);
    assert.equal(r.body.axis.tiled, false);
  });

  test('coverage is per source and may start before the axis zero', async () => {
    const r = await anon().get('/api/streams/idx/685');
    assert.ok(Array.isArray(r.body.coverage));
    assert.ok(r.body.coverage.every((c) => 'capture_id' in c && 'clock' in c));
  });

  test('the rail is a window either side, ordered by time and not by idx', async () => {
    const r = await anon().get('/api/streams/idx/685?rail=4');
    const { older, newer } = r.body.neighbours;
    assert.equal(older.length, 4);
    assert.equal(newer.length, 4);
    const seq = [...older, ...newer].map((s) => s.started_at);
    assert.deepEqual(seq, [...seq].sort((a, b) => a - b), 'the rail is out of order');
    assert.ok(older.every((s) => s.started_at <= r.body.started_at));
    assert.ok(newer.every((s) => s.started_at >= r.body.started_at));
    assert.ok(older.every((s) => 'thumb' in s));
  });

  test('the rail agrees with prev_id and next_id', async () => {
    const r = await anon().get('/api/streams/idx/685?rail=2');
    assert.equal(r.body.neighbours.older.at(-1).id, r.body.prev_id);
    assert.equal(r.body.neighbours.newer[0].id, r.body.next_id);
  });

  test('paging forward walks back to where it started', async () => {
    const first = await anon().get('/api/streams?limit=10');
    const page2 = await anon().get(
      `/api/streams?limit=10&before=${first.body.next.before}&before_id=${first.body.next.before_id}`);
    const oldest = page2.body.streams.at(-1);
    const back = await anon().get(
      `/api/streams?limit=10&after=${oldest.started_at}&after_id=${oldest.id}`);
    // Always newest-first, whichever way it paged.
    const t = back.body.streams.map((s) => s.started_at);
    assert.deepEqual(t, [...t].sort((a, b) => b - a));
    assert.ok(back.body.streams.some((s) => s.id === first.body.streams.at(-1).id));
    assert.ok(!back.body.streams.some((s) => s.id === oldest.id), 'the cursor row came back');
  });

  test('the panel is told which kind of "no chat" this is', async () => {
    const r = await anon().get('/api/streams/idx/685');
    assert.ok(['present', 'lost', 'never', 'unverified'].includes(r.body.chat.state));
    assert.equal(r.body.chat.imported, false);
  });

  test('the segment vocabulary ships with the payload, so it is not hardcoded twice', async () => {
    const r = await anon().get('/api/streams/idx/685');
    assert.deepEqual(r.body.segment_kinds, SEGMENT_KINDS);
  });
});

// ===========================================================================
describe('the materialised timeline', () => {
// ===========================================================================

  test('what is stored matches a fresh projection', async () => {
    const db = open(DB, { readonly: true });
    const ids = db.prepare(
      'SELECT id FROM stream WHERE timeline_json IS NOT NULL LIMIT 40').all();
    for (const { id } of ids) {
      const stored = db.prepare('SELECT timeline_json j FROM stream WHERE id=?').get(id).j;
      assert.equal(JSON.stringify(buildTimeline(db, id)), stored, `drifted: ${id}`);
    }
    db.close();
  });

  test('a write rebuilds it rather than leaving it stale', async () => {
    const ed = await login('tl-ed', 'editor');
    const s = (await anon().get('/api/streams/idx/688')).body;
    const SEG = ulid();
    const w = await ed.post('/api/changesets', { changes: [
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'stream_id', value: s.id },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'start_s', value: 60 },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'end_s', value: 120 },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'kind', value: 'meta' },
    ] });
    assert.equal(w.status, 200, JSON.stringify(w.body));
    const db = open(DB, { readonly: true });
    const stored = JSON.parse(
      db.prepare('SELECT timeline_json j FROM stream WHERE id=?').get(s.id).j);
    db.close();
    assert.ok(stored.segments.some((x) => x.id === SEG), 'the stored timeline went stale');
  });

  test('a retracted segment leaves the strip', async () => {
    const ed = await login('tl-ed2', 'editor');
    const s = (await anon().get('/api/streams/idx/689')).body;
    const SEG = ulid();
    await ed.post('/api/changesets', { changes: [
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'stream_id', value: s.id },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'start_s', value: 60 },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'end_s', value: 120 },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'kind', value: 'type' },
    ] });
    await ed.post('/api/changesets', { changes: [
      { target_type: 'segment', target_id: SEG, op: 'delete' } ] });

    const after = (await anon().get(`/api/streams/${s.id}`)).body;
    assert.ok(!after.segments.some((x) => x.id === SEG));
    // Tombstoned, not deleted — a retracted segment has to stay retracted.
    const db = open(DB, { readonly: true });
    assert.ok(db.prepare('SELECT retracted_at FROM segment WHERE id=?').get(SEG).retracted_at);
    db.close();
  });
});

// ===========================================================================
describe('search', () => {
  test('a retracted note stops making its stream findable', async () => {
    const ed = await login('fts-ed', 'editor');
    const s = (await anon().get('/api/streams/idx/690')).body;
    const NID = ulid();
    const WORD = 'zzqqxxflibbertigibbet';
    await ed.post('/api/changesets', { changes: [
      { target_type: 'note', target_id: NID, op: 'create', field: 'stream_id', value: s.id },
      { target_type: 'note', target_id: NID, op: 'create', field: 'text', value: WORD },
      { target_type: 'note', target_id: NID, op: 'create', field: 'offset_s', value: 5 },
    ] });
    assert.equal((await anon().get(`/api/streams?q=${WORD}`)).body.count, 1);

    await ed.post('/api/changesets', { reason: 'withdrawn', changes: [
      { target_type: 'note', target_id: NID, op: 'delete' } ] });
    // The FTS triggers fire on UPDATE and a tombstone IS an update, so the row
    // is still in the index with its text. The query has to exclude it.
    assert.equal((await anon().get(`/api/streams?q=${WORD}`)).body.count, 0,
      'a withdrawn note still makes its stream findable by its words');
  });
});

// ===========================================================================
describe('ffprobe', () => {
// ===========================================================================

  test('probing fills the duration, the codecs and a seeded local clock', async (t) => {
    let dir;
    try {
      execFileSync('ffprobe', ['-version'], { stdio: 'ignore' });
    } catch { return t.skip('ffprobe not on PATH'); }

    dir = mkdtempSync(join(tmpdir(), 'tenma-probe-'));
    const rel = 'raws/probe-fixture.mp4';
    mkdirSync(join(dir, 'raws'), { recursive: true });
    const file = join(dir, rel);
    // 12 seconds of colour bars and a tone: small, and real enough to probe.
    execFileSync('ffmpeg', ['-v', 'error', '-y',
      '-f', 'lavfi', '-i', 'testsrc=size=320x180:rate=10:duration=12',
      '-f', 'lavfi', '-i', 'sine=frequency=440:duration=12',
      '-c:v', 'libx264', '-preset', 'ultrafast', '-pix_fmt', 'yuv420p',
      '-c:a', 'aac', '-shortest', file]);

    const MTIME = 1799990000;                     // pretend it finished here
    utimesSync(file, MTIME, MTIME);

    const PDB = join(ROOT, 'data', 'probe-test.db');
    for (const f of [PDB, PDB + '-wal', PDB + '-shm']) if (existsSync(f)) rmSync(f);
    copyFileSync(SEED, PDB);

    // Point one real capture at the fixture file.
    const w = open(PDB);
    const cap = w.prepare(
      `SELECT c.id, c.stream_id FROM capture c WHERE c.video_path IS NOT NULL LIMIT 1`).get();
    w.prepare('UPDATE capture SET video_path=? WHERE id=?').run(rel, cap.id);
    w.prepare(`UPDATE capture SET video_path=NULL WHERE id <> ?`).run(cap.id);
    w.close();

    execFileSync(process.execPath, [join(ROOT, 'scripts', 'probe-media.js'),
      '--db', PDB, '--root', dir], { stdio: 'ignore' });

    const r = open(PDB, { readonly: true });
    const got = r.prepare(`SELECT file_duration_s, video_codec, audio_codec, width,
      height, has_audio, probed_at, local_start_wall, local_start_precision_s
      FROM capture WHERE id=?`).get(cap.id);
    const stream = r.prepare('SELECT duration_s, timeline_json FROM stream WHERE id=?')
      .get(cap.stream_id);
    r.close();

    assert.equal(got.file_duration_s, 12);
    assert.equal(got.video_codec, 'h264');
    assert.equal(got.audio_codec, 'aac');
    assert.equal(got.width, 320);
    assert.equal(got.height, 180);
    assert.equal(got.has_audio, 1);
    assert.ok(got.probed_at > 0);
    // The fourth clock, recovered from mtime - duration and labelled as a guess.
    assert.equal(got.local_start_wall, MTIME - 12);
    assert.equal(got.local_start_precision_s, 1800);
    // ...and the stream's duration finally derives from a measurement.
    assert.ok(stream.timeline_json);
    assert.equal(JSON.parse(stream.timeline_json).duration_source, 'measured');

    for (const f of [PDB, PDB + '-wal', PDB + '-shm']) if (existsSync(f)) rmSync(f);
    rmSync(dir, { recursive: true, force: true });
  });
});

// ===========================================================================
describe('an empty or missing database', () => {
// ===========================================================================

  test('migrate() survives a database with no tables in it', async () => {
    // POST_MIGRATION ran unconditionally, so the triggers threw
    // `no such table: main.capture` from inside migrate() — a spectacularly
    // unhelpful way to say "there is no archive at this path", and the first
    // thing anyone sees when they run from the wrong directory.
    const { open: openDb, migrate } = await import('../db.js');
    const p = join(ROOT, 'data', 'empty-test.db');
    for (const f of [p, p + '-wal', p + '-shm']) if (existsSync(f)) rmSync(f);
    const db = openDb(p);
    assert.doesNotThrow(() => migrate(db));
    db.close();
    for (const f of [p, p + '-wal', p + '-shm']) if (existsSync(f)) rmSync(f);
  });

  test('isArchive tells a real archive from a file that merely exists', async () => {
    const { open: openDb, isArchive } = await import('../db.js');
    const p = join(ROOT, 'data', 'empty-test2.db');
    for (const f of [p, p + '-wal', p + '-shm']) if (existsSync(f)) rmSync(f);
    assert.equal(isArchive(p), false, 'nothing there');
    openDb(p).close();                       // now the file exists, empty
    assert.equal(isArchive(p), false, 'exists, but holds no archive');
    assert.equal(isArchive(DB), true);
    for (const f of [p, p + '-wal', p + '-shm']) if (existsSync(f)) rmSync(f);
  });

  test('the old vocabulary is remapped and seg_kind is dropped, once', async () => {
    // The state on a machine that ran the previous migration: tag.seg_kind
    // exists, tag.kind still says 'format'/'other', and a segment carries one
    // of the retired words. Nothing here may be left behind — a stray 'talk'
    // is a segment that renders with no swatch, which reads as "unlabelled".
    const { open: openDb, migrate, ulid: mkUlid } = await import('../db.js');
    const p = join(ROOT, 'data', 'vocab-test.db');
    for (const f of [p, p + '-wal', p + '-shm']) if (existsSync(f)) rmSync(f);
    copyFileSync(SEED, p);
    const db = openDb(p);
    db.exec('ALTER TABLE tag ADD COLUMN seg_kind TEXT');
    // Put two rows back into the old shape by id rather than by matching
    // kind='format' — the seed on disk gets migrated in place by the scripts,
    // so a test that depends on its vintage passes on Monday and not Tuesday.
    const [a, b] = db.prepare('SELECT id FROM tag ORDER BY id LIMIT 2').all();
    const back = db.prepare('UPDATE tag SET kind = ?, seg_kind = ? WHERE id = ?');
    back.run('other', 'talk', a.id);      // "nobody said"  -> must stay unsaid
    back.run('format', 'talk', b.id);     // the old word for 'type'
    migrate(db);
    const t = Math.floor(Date.now() / 1000);
    const sid = db.prepare('SELECT id FROM stream LIMIT 1').get().id;
    for (const k of ['talk', 'idle', 'music', 'read', 'game']) {
      db.prepare(`INSERT INTO segment(id, stream_id, start_s, kind, created_at, updated_at)
                  VALUES(?,?,?,?,?,?)`).run(mkUlid(), sid, 0, k, t, t);
    }
    migrate(db);

    const cols = db.prepare('PRAGMA table_xinfo(tag)').all().map((r) => r.name);
    assert.ok(!cols.includes('seg_kind'), 'the second vocabulary is still there');
    const ok = new Set(['game', 'person', 'type', 'meta', 'unknown']);
    for (const table of ['tag', 'segment']) {
      const got = db.prepare(`SELECT DISTINCT kind k FROM ${table}`).all().map((r) => r.k);
      assert.ok(got.every((k) => ok.has(k)), `${table}: ${JSON.stringify(got)}`);
    }
    assert.equal(db.prepare(`SELECT count(*) n FROM segment WHERE kind='type'`).get().n, 3,
      'talk, music and read are all kinds of stream');
    assert.equal(db.prepare(`SELECT count(*) n FROM segment WHERE kind='meta'`).get().n, 1,
      'idle is scaffolding');
    // 'other' means nobody said. It must not become a category by migration —
    // that would be the database inventing an editorial decision.
    assert.equal(db.prepare('SELECT kind FROM tag WHERE id=?').get(a.id).kind, 'unknown');
    assert.equal(db.prepare('SELECT kind FROM tag WHERE id=?').get(b.id).kind, 'type');
    assert.deepEqual(migrate(db), [], 'the remap ran a second time');
    db.close();
    for (const f of [p, p + '-wal', p + '-shm']) if (existsSync(f)) rmSync(f);
  });

  test('the scripts refuse a path with no archive instead of inventing one', () => {
    // rebuild.js and probe-media.js call create(), which applies schema.sql —
    // so a mistyped path used to produce a brand-new empty archive and then
    // report "rebuilt 0 stream(s)" as though that were a success.
    const p = join(ROOT, 'data', 'not-here.db');
    for (const s of ['rebuild.js', 'check-db.js']) {
      const r = spawnSync(process.execPath, [join(ROOT, 'scripts', s), '--db', p],
        { encoding: 'utf8' });
      assert.equal(r.status, 1, `${s} should exit 1`);
      assert.match(r.stderr, /no archive at/, `${s} should say why`);
      assert.ok(!existsSync(p), `${s} created a database anyway`);
    }
  });
});

// ===========================================================================
describe('tags as entities', () => {
// ===========================================================================

  test('creating a tag through a changeset works at all', async () => {
    // It did not, ever: apply() stamps created_at AND updated_at on every
    // create, and `tag` had no updated_at, so every attempt threw
    // `table tag has no column named updated_at`. Tags were read-only.
    const ed = await login('tag-ed', 'editor');
    const id = ulid();
    const r = await ed.post('/api/changesets', { changes: [
      { target_type: 'tag', target_id: id, op: 'create', field: 'name', value: 'Katamari Damacy' },
      { target_type: 'tag', target_id: id, op: 'create', field: 'kind', value: 'game' }] });
    assert.equal(r.body.status, 'applied', JSON.stringify(r.body));
  });

  test('slug is derived from name, and follows a rename', async () => {
    const ed = await login('tag-ed2', 'editor');
    const id = ulid();
    await ed.post('/api/changesets', { changes: [
      { target_type: 'tag', target_id: id, op: 'create', field: 'name', value: 'Silent Hill 2' }] });
    const db = open(DB, { readonly: true });
    assert.equal(db.prepare('SELECT slug FROM tag WHERE id=?').get(id).slug, 'silent-hill-2');
    db.close();

    const r = await ed.post('/api/changesets', { changes: [
      { target_type: 'tag', target_id: id, op: 'update', field: 'name', value: 'SILENT HILL f' }] });
    const db2 = open(DB, { readonly: true });
    assert.equal(db2.prepare('SELECT slug FROM tag WHERE id=?').get(id).slug, 'silent-hill-f',
      'a rename that leaves the slug behind means the tag answers to its old url forever');
    db2.close();
    // ...and the slug move is in the log, not silent.
    assert.ok(r.body.changes.some((c) => c.field === 'slug' && c.value === 'silent-hill-f'));
  });

  test('a slug keeps kana voicing marks', async () => {
    // NFKD decomposes ゼ into セ + a combining mark; dropping every mark gives
    // セルタ, a different word, and would collide with the real セルタ.
    const { slugify } = await import('../db.js');
    assert.equal(slugify('ゼルダの伝説'), 'ゼルダの伝説');
    assert.notEqual(slugify('ゼルダ'), slugify('セルタ'));
    assert.equal(slugify('Café'), slugify('cafe'));       // Latin folding still works
    assert.equal(slugify('CLAIR OBSCUR: EXPEDITION 33 #7'), 'clair-obscur-expedition-33-7');
    assert.ok(!slugify('anything at all #7').includes('#'),
      "a '#' in a slug is a url fragment — the server never receives it");
  });

  test('mint, attach and label a block, in one changeset', async () => {
    const ed = await login('tag-ed3', 'editor');
    const s = (await anon().get('/api/streams/idx/691')).body;
    const TID = ulid(), LINK = ulid(), SEG = ulid();
    const r = await ed.post('/api/changesets', { reason: 'it is mario kart', changes: [
      { target_type: 'tag', target_id: TID, op: 'create', field: 'name', value: 'Mario Kart 8' },
      { target_type: 'tag', target_id: TID, op: 'create', field: 'kind', value: 'game' },
      { target_type: 'tag', target_id: TID, op: 'create', field: 'kind', value: 'game' },
      { target_type: 'stream_tag', target_id: LINK, op: 'create', field: 'stream_id', value: s.id },
      { target_type: 'stream_tag', target_id: LINK, op: 'create', field: 'tag_id', value: TID },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'stream_id', value: s.id },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'start_s', value: 0 },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'end_s', value: 600 },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'kind', value: 'game' },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'tag_id', value: TID } ] });
    assert.equal(r.body.status, 'applied', JSON.stringify(r.body));

    const d = (await anon().get(`/api/streams/${s.id}`)).body;
    const chip = d.tags.find((t) => t.id === TID);
    assert.ok(chip, 'the tag never reached the stream');
    assert.ok(chip.link_id, 'without the junction id there is no way to detach it');
    const seg = d.segments.find((x) => x.id === SEG);
    assert.equal(seg.tag.slug, 'mario-kart-8');
    assert.equal(seg.label, 'Mario Kart 8', 'label falls back to the tag name');
    // and it is now in everyone's autocomplete
    const ac = (await anon().get('/api/tags?q=mario kart')).body.tags;
    assert.ok(ac.some((t) => t.id === TID));
  });

  test("a suggester's new tag stays out of the picker until it is confirmed", async () => {
    const ed = await login('tag-ed4', 'editor');
    const fan = await login('tag-fan', 'suggester');
    const id = ulid();
    const p = await fan.post('/api/changesets', { changes: [
      { target_type: 'tag', target_id: id, op: 'create', field: 'name', value: 'Umineko' }] });
    assert.equal(p.body.status, 'open');
    await ed.post(`/api/changesets/${p.body.id}/review`, { decision: 'approve' });

    assert.equal((await anon().get('/api/tags?q=umineko')).body.tags.length, 0,
      'a suggester-minted tag went straight into the public vocabulary');
    const seen = (await ed.get('/api/tags?q=umineko&status=all')).body.tags;
    assert.equal(seen.length, 1);
    assert.equal(seen[0].status, 'proposed');

    await ed.post('/api/changesets', { changes: [
      { target_type: 'tag', target_id: id, op: 'update', field: 'status', value: 'confirmed' }] });
    assert.equal((await anon().get('/api/tags?q=umineko')).body.tags.length, 1);
  });

  test('two people minting the same tag resolve to one row, not a rejection', async () => {
    // Both mint their own ULID before either is reviewed. UNIQUE(slug) would
    // roll the second changeset back entirely — losing a good suggestion to a
    // race — so the applier points it at the row that already exists.
    const ed = await login('tag-ed5', 'editor');
    const fan = await login('tag-fan2', 'suggester');
    const s = (await anon().get('/api/streams/idx/692')).body;
    const mk = (tid, link) => fan.post('/api/changesets', { changes: [
      { target_type: 'tag', target_id: tid, op: 'create', field: 'name', value: 'Outer Wilds' },
      { target_type: 'stream_tag', target_id: link, op: 'create', field: 'stream_id', value: s.id },
      { target_type: 'stream_tag', target_id: link, op: 'create', field: 'tag_id', value: tid } ] });
    const a = await mk(ulid(), ulid());
    const b = await mk(ulid(), ulid());
    const ra = await ed.post(`/api/changesets/${a.body.id}/review`, { decision: 'approve' });
    const rb = await ed.post(`/api/changesets/${b.body.id}/review`, { decision: 'approve' });
    assert.equal(ra.body.status, 'applied');
    assert.equal(rb.status, 200, JSON.stringify(rb.body));
    assert.equal(rb.body.status, 'applied', 'the second lost a good suggestion to a race');

    const db = open(DB, { readonly: true });
    assert.equal(db.prepare("SELECT COUNT(*) c FROM tag WHERE slug='outer-wilds'").get().c, 1);
    assert.equal(db.prepare(
      "SELECT COUNT(*) c FROM stream_tag st JOIN tag t ON t.id=st.tag_id WHERE t.slug='outer-wilds'").get().c,
      1, 'the second attach should collapse onto the same pair');
    db.close();
  });

  test('a series rolls up, so ?tag= stops returning one episode in ten', async () => {
    const ed = await login('tag-ed6', 'editor');
    const all = (await anon().get('/api/tags?q=super metroid')).body.tags;
    const canon = all.find((t) => !/-\d+$/.test(t.slug));
    const kids = all.filter((t) => t !== canon);
    assert.ok(canon && kids.length >= 2, 'expected the fragmented SUPER METROID rows');

    const before = (await anon().get(`/api/streams?tag=${canon.slug}&limit=100`)).body.count;
    await ed.post('/api/changesets', { reason: 'roll up', changes: kids.map((k) => (
      { target_type: 'tag', target_id: k.id, op: 'update', field: 'parent_id', value: canon.id })) });
    const after = (await anon().get(`/api/streams?tag=${canon.slug}&limit=100`)).body.count;
    assert.ok(after > before, `rollup did nothing: ${before} -> ${after}`);
  });

  test('detaching a tag is a changeset like anything else', async () => {
    const ed = await login('tag-ed7', 'editor');
    const s = (await anon().get('/api/streams/idx/693')).body;
    const TID = ulid(), LINK = ulid();
    await ed.post('/api/changesets', { changes: [
      { target_type: 'tag', target_id: TID, op: 'create', field: 'name', value: 'Detach Me' },
      { target_type: 'stream_tag', target_id: LINK, op: 'create', field: 'stream_id', value: s.id },
      { target_type: 'stream_tag', target_id: LINK, op: 'create', field: 'tag_id', value: TID } ] });
    assert.ok((await anon().get(`/api/streams/${s.id}`)).body.tags.some((t) => t.id === TID));
    await ed.post('/api/changesets', { changes: [
      { target_type: 'stream_tag', target_id: LINK, op: 'delete' }] });
    assert.ok(!(await anon().get(`/api/streams/${s.id}`)).body.tags.some((t) => t.id === TID));
  });
});

// ===========================================================================
describe('notes — saying which clock a number came from', () => {
// ===========================================================================

  /** A stream whose YT capture does NOT start at the axis zero, so a note moving
   *  onto that clock has somewhere to move to. */
  function offsetStream(db) {
    return db.prepare(
      `SELECT c.stream_id, c.id cap, s.started_at,
              (c.remote_start_wall - s.started_at) off
         FROM capture c JOIN stream s ON s.id = c.stream_id
        WHERE c.remote_start_wall IS NOT NULL
          AND c.remote_start_wall <> s.started_at
          AND s.duration_s IS NOT NULL
        ORDER BY abs(c.remote_start_wall - s.started_at) DESC LIMIT 1`).get();
  }

  test('an unknown note keeps its number until someone says which clock', async () => {
    // The whole point of the editor. All 915 imported notes are frame='unknown':
    // the number is carried at face value because nobody wrote down what it was
    // measured against. Naming the clock is a DECISION, and the pin moves by
    // exactly that clock's offset — visibly, in the history, never on its own.
    const db0 = open(DB, { readonly: true });
    const pick = offsetStream(db0);
    db0.close();
    assert.ok(pick && pick.off !== 0, 'the archive is supposed to have skewed captures');

    const ed = await login('note-ed', 'editor');
    const NID = ulid();
    const w = await ed.post('/api/changesets', { changes: [
      { target_type: 'note', target_id: NID, op: 'create', field: 'stream_id', value: pick.stream_id },
      { target_type: 'note', target_id: NID, op: 'create', field: 'text', value: 'clock probe' },
      { target_type: 'note', target_id: NID, op: 'create', field: 'offset_s', value: 600 },
      { target_type: 'note', target_id: NID, op: 'create', field: 'frame', value: 'unknown' },
    ] });
    assert.equal(w.status, 200, JSON.stringify(w.body));

    const before = (await anon().get(`/api/streams/${pick.stream_id}`)).body
      .notes.find((n) => n.id === NID);
    assert.equal(before.start_s, 600, 'an unknown offset is carried at face value');
    assert.equal(before.exact, false);

    const p = await ed.post('/api/changesets', { changes: [
      { target_type: 'note', target_id: NID, op: 'update', field: 'frame', value: 'capture' },
      { target_type: 'note', target_id: NID, op: 'update', field: 'anchor_id', value: pick.cap },
      { target_type: 'note', target_id: NID, op: 'update', field: 'anchor_clock', value: 'remote' },
      { target_type: 'note', target_id: NID, op: 'update', field: 'offset_precision_s', value: 1 },
    ] });
    assert.equal(p.status, 200, JSON.stringify(p.body));

    const after = (await anon().get(`/api/streams/${pick.stream_id}`)).body
      .notes.find((n) => n.id === NID);
    assert.equal(after.offset_s, 600, 'the number a human typed must not be rewritten');
    assert.equal(after.start_s, 600 + pick.off,
      'the pin did not move onto the clock it was just told it came from');
    assert.equal(after.exact, true);
    assert.equal(after.precision_s, 1);
    // ...and it is in the log, four rows of it.
    const h = (await anon().get(`/api/streams/${pick.stream_id}/history`)).body.history;
    assert.ok(h.some((cs) => (cs.changes || []).some(
      (c) => c.target_id === NID && c.field === 'anchor_id')),
      'naming the clock moved a pin without leaving a row saying so');
  });

  test('the vault wrote #srt for what is a short, and only one word survives', async () => {
    // Storing one word and displaying another is the seg_kind trap. The rows
    // move; `srt` stays accepted as input and never comes back out.
    const db = open(DB, { readonly: true });
    const tags = db.prepare(
      'SELECT tag, count(*) n FROM note WHERE tag IS NOT NULL GROUP BY tag').all();
    db.close();
    assert.equal(tags.find((t) => t.tag === 'srt'), undefined, 'srt is still in the database');
    assert.ok((tags.find((t) => t.tag === 'short')?.n ?? 0) >= 160,
      'the 166 srt notes did not arrive as short: ' + JSON.stringify(tags));
  });

  test('a multi-point stamp is carried verbatim and only its first point pins', async () => {
    // '-' means a span and ';' means two moments. One column cannot hold both
    // honestly, so the expression is kept as typed and never interpreted.
    const ed = await login('note-ed2', 'editor');
    const s = (await anon().get('/api/streams/idx/696')).body;
    const NID = ulid();
    const r = await ed.post('/api/changesets', { changes: [
      { target_type: 'note', target_id: NID, op: 'create', field: 'stream_id', value: s.id },
      { target_type: 'note', target_id: NID, op: 'create', field: 'text', value: 'span probe' },
      { target_type: 'note', target_id: NID, op: 'create', field: 'offset_s', value: 736 },
      { target_type: 'note', target_id: NID, op: 'create', field: 'stamp', value: '00:12:16 - 00:13:27' },
    ] });
    assert.equal(r.body.status, 'applied', JSON.stringify(r.body));
    const n = (await anon().get(`/api/streams/${s.id}`)).body.notes.find((x) => x.id === NID);
    assert.equal(n.stamp, '00:12:16 - 00:13:27');
    assert.equal(n.start_s, 736, 'a note is a pin, and the pin is the first point');
  });

  test('raw is served and never written', async () => {
    // The editor shows it under a vault note so a correction can be checked
    // against what was actually written. It is not in WRITABLE, so a changeset
    // that tries to edit the receipt is refused rather than quietly ignored.
    const withRaw = (await anon().get('/api/streams/idx/697')).body
      .notes.find((n) => n.raw);
    assert.ok(withRaw, 'the vault lines are supposed to be served');
    const ed = await login('note-ed3', 'editor');
    const r = await ed.post('/api/changesets', { changes: [
      { target_type: 'note', target_id: withRaw.id, op: 'update', field: 'raw', value: 'rewritten' }] });
    assert.equal(r.status, 400);
  });

  test('a retracted note leaves the panel and the pin count', async () => {
    const ed = await login('note-ed4', 'editor');
    const s = (await anon().get('/api/streams/idx/695')).body;
    const NID = ulid();
    await ed.post('/api/changesets', { changes: [
      { target_type: 'note', target_id: NID, op: 'create', field: 'stream_id', value: s.id },
      { target_type: 'note', target_id: NID, op: 'create', field: 'text', value: 'delete me' },
      { target_type: 'note', target_id: NID, op: 'create', field: 'offset_s', value: 60 } ] });
    assert.ok((await anon().get(`/api/streams/${s.id}`)).body.notes.some((n) => n.id === NID));
    await ed.post('/api/changesets', { changes: [
      { target_type: 'note', target_id: NID, op: 'delete' }] });
    assert.ok(!(await anon().get(`/api/streams/${s.id}`)).body.notes.some((n) => n.id === NID));
    const db = open(DB, { readonly: true });
    assert.ok(db.prepare('SELECT retracted_at FROM note WHERE id=?').get(NID).retracted_at,
      'tombstoned, not deleted');
    db.close();
  });
});

// ===========================================================================
describe('chapters — what the timeline editor writes', () => {
// ===========================================================================

  // The editor emits exactly this shape. If it ever stops being accepted, the
  // failure is a silent one: the popover closes, a toast says "Refused", and
  // the block the person drew is simply not there.
  const chapter = (sid, id, a, b, kind, tagId) => [
    { target_type: 'segment', target_id: id, op: 'create', field: 'stream_id', value: sid },
    { target_type: 'segment', target_id: id, op: 'create', field: 'frame', value: 'stream' },
    { target_type: 'segment', target_id: id, op: 'create', field: 'start_s', value: a },
    { target_type: 'segment', target_id: id, op: 'create', field: 'end_s', value: b },
    { target_type: 'segment', target_id: id, op: 'create', field: 'kind', value: kind },
    ...(tagId ? [{ target_type: 'segment', target_id: id, op: 'create',
                   field: 'tag_id', value: tagId }] : []),
  ];

  test('a chapter drawn on the axis does not move when a capture clock is corrected', async () => {
    // This is the whole reason the editor writes frame='stream'. The person
    // drew on the archive's axis while some file happened to be playing;
    // anchoring the block to that file would shift it by the recording lag the
    // day somebody measures that file properly. Nobody would ever see it move.
    const ed = await login('chap-ed', 'editor');
    const s = (await anon().get('/api/streams/idx/692')).body;
    const cap = s.captures?.[0] ?? (await anon().get(`/api/streams/${s.id}`)).body.captures[0];
    const SEG = ulid();
    const w = await ed.post('/api/changesets', { changes: chapter(s.id, SEG, 300, 900, 'meta') });
    assert.equal(w.status, 200, JSON.stringify(w.body));

    const before = (await anon().get(`/api/streams/${s.id}`)).body
      .segments.find((x) => x.id === SEG);
    assert.deepEqual([before.start_s, before.end_s], [300, 900]);
    assert.equal(before.exact, true, 'a block drawn on the axis is exactly where it was drawn');

    const shift = await ed.post('/api/changesets', { changes: [
      { target_type: 'capture', target_id: cap.id, op: 'update',
        field: 'local_start_wall', value: (cap.local_start_wall ?? s.started_at) + 137 }] });
    assert.equal(shift.status, 200, JSON.stringify(shift.body));

    const after = (await anon().get(`/api/streams/${s.id}`)).body
      .segments.find((x) => x.id === SEG);
    assert.deepEqual([after.start_s, after.end_s], [300, 900],
      'the chapter followed a capture it was never anchored to');
  });

  test('a chapter and the tag it names are minted together or not at all', async () => {
    const ed = await login('chap-ed2', 'editor');
    const s = (await anon().get('/api/streams/idx/693')).body;
    const TID = ulid(), SEG = ulid();
    // The second block overlaps the first, so the whole changeset dies — and
    // the tag must die with it. A rejected suggestion that still leaves a new
    // word in everyone's autocomplete is how the vocabulary fills with debris.
    const r = await ed.post('/api/changesets', { changes: [
      { target_type: 'tag', target_id: TID, op: 'create', field: 'name', value: 'Debris Probe' },
      { target_type: 'tag', target_id: TID, op: 'create', field: 'kind', value: 'type' },
      ...chapter(s.id, SEG, 0, 600, 'type', TID),
      ...chapter(s.id, ulid(), 300, 900, 'type', TID),
    ] });
    assert.equal(r.status, 409, JSON.stringify(r.body));
    const ac = (await anon().get('/api/tags?q=debris&status=all')).body.tags;
    assert.equal(ac.length, 0, 'the tag survived a changeset that did not');
  });

  test('a chapter carries its tag onto the strip and into the tag page', async () => {
    const ed = await login('chap-ed3', 'editor');
    const s = (await anon().get('/api/streams/idx/694')).body;
    const TID = ulid(), SEG = ulid();
    const r = await ed.post('/api/changesets', { changes: [
      { target_type: 'tag', target_id: TID, op: 'create', field: 'name', value: 'Utawaku Block' },
      { target_type: 'tag', target_id: TID, op: 'create', field: 'kind', value: 'type' },
      ...chapter(s.id, SEG, 60, 240, 'type', TID),
    ] });
    assert.equal(r.body.status, 'applied', JSON.stringify(r.body));
    const seg = (await anon().get(`/api/streams/${s.id}`)).body.segments.find((x) => x.id === SEG);
    assert.equal(seg.label, 'Utawaku Block');
    assert.equal(seg.kind, 'type');
    // `blocks` is the payoff for linking the entity rather than retyping it:
    // "every stretch of this in the archive" is one index lookup.
    const tag = (await anon().get('/api/tags?q=utawaku')).body.tags.find((t) => t.id === TID);
    assert.equal(tag.blocks, 1);
  });

  test('drawing a chapter named after a tag that exists reuses it, block and all', async () => {
    // The editor mints on an autocomplete miss, and a miss is not proof of
    // absence — a proposed tag is hidden from the picker, and two people can
    // race. Resolving at apply time has to repoint the SEGMENT too, not just
    // the junction row, or the block lands on a foreign key that never exists.
    const ed = await login('chap-ed4', 'editor');
    const s = (await anon().get('/api/streams/idx/695')).body;
    const have = (await anon().get('/api/tags?q=karaoke')).body.tags[0];
    assert.ok(have, 'this archive is supposed to already know the word');
    const MINE = ulid(), SEG = ulid();
    const r = await ed.post('/api/changesets', { changes: [
      { target_type: 'tag', target_id: MINE, op: 'create', field: 'name', value: have.name },
      { target_type: 'tag', target_id: MINE, op: 'create', field: 'kind', value: 'type' },
      ...chapter(s.id, SEG, 60, 240, 'type', MINE),
    ] });
    assert.equal(r.body.status, 'applied', JSON.stringify(r.body));
    const seg = (await anon().get(`/api/streams/${s.id}`)).body.segments.find((x) => x.id === SEG);
    assert.equal(seg.tag.id, have.id, 'the block points at a duplicate, not the real tag');
    const db = open(DB, { readonly: true });
    assert.equal(db.prepare('SELECT count(*) n FROM tag WHERE id=?').get(MINE).n, 0,
      'a second row for a word the archive already had');
    db.close();
  });
});

// ===========================================================================
describe('history covers everything editable on a stream', () => {
  test('segment and tag edits appear in the stream history', async () => {
    // It only joined note and capture, so two of the four things you can change
    // about a stream were invisible in its own history.
    const ed = await login('hist-ed', 'editor');
    const s = (await anon().get('/api/streams/idx/694')).body;
    const TAG = ulid(), LINK = ulid(), SEG = ulid();
    await ed.post('/api/changesets', { reason: 'tagged it', changes: [
      { target_type: 'tag', target_id: TAG, op: 'create', field: 'name', value: 'History Probe' },
      { target_type: 'stream_tag', target_id: LINK, op: 'create', field: 'stream_id', value: s.id },
      { target_type: 'stream_tag', target_id: LINK, op: 'create', field: 'tag_id', value: TAG } ] });
    await ed.post('/api/changesets', { reason: 'blocked it out', changes: [
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'stream_id', value: s.id },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'start_s', value: 0 },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'kind', value: 'type' } ] });

    const h = (await anon().get(`/api/streams/${s.id}/history`)).body.history;
    assert.ok(h.some((e) => e.reason === 'tagged it'), 'tagging is missing from the history');
    assert.ok(h.some((e) => e.reason === 'blocked it out'), 'segmenting is missing from the history');
  });
});

// ===========================================================================
describe('a queued suggestion is visible to the person who made it', () => {
// ===========================================================================

  test('proposing bumps the generation, or every read answers 304 with stale data', async () => {
    // ETags are keyed on `generation`, and only apply() bumped it. A proposal
    // adds rows the read API serves — the badge, the history, the queue — so a
    // suggester submitted something and then saw no trace of it anywhere,
    // because the browser kept being handed the pre-suggestion body.
    const fan = await login('gen-fan', 'suggester');
    const before = (await anon().get('/api/health')).body.generation;
    const s = (await anon().get('/api/streams/idx/695')).body;
    const r = await fan.post('/api/changesets', { reason: 'a suggestion', changes: [
      { target_type: 'stream', target_id: s.id, op: 'update', field: 'summary', value: 'hello' }] });
    assert.equal(r.body.status, 'open');
    assert.ok((await anon().get('/api/health')).body.generation > before,
      'a proposal left the generation untouched, so every cached read stays stale');
  });

  test('an open suggestion shows on the stream it proposes to change', async () => {
    const fan = await login('badge-fan', 'suggester');
    const ed = await login('badge-ed', 'editor');
    // Relative, not absolute: these tests share one database and run in file
    // order, so another test's leftovers must not decide this one.
    const s = (await anon().get('/api/streams/idx/696')).body;
    const baseline = s.open_changesets;
    const tagsBefore = s.tags.length;

    // The hard case: the changeset CREATES a stream_tag, so the row it targets
    // does not exist yet and the only trace is the stream's id in a field
    // value. Counting only by target_id misses it entirely.
    const TAG = ulid(), LINK = ulid();
    await fan.post('/api/changesets', { reason: 'suggesting a tag', changes: [
      { target_type: 'tag', target_id: TAG, op: 'create', field: 'name', value: 'Badge Probe' },
      { target_type: 'stream_tag', target_id: LINK, op: 'create', field: 'stream_id', value: s.id },
      { target_type: 'stream_tag', target_id: LINK, op: 'create', field: 'tag_id', value: TAG } ] });

    const after = (await anon().get(`/api/streams/${s.id}`)).body;
    assert.equal(after.open_changesets, baseline + 1,
      'the pending suggestion is invisible on the stream it proposes to change');
    assert.equal(after.tags.length, tagsBefore, 'and it must not have been applied');

    const h = (await anon().get(`/api/streams/${s.id}/history`)).body.history;
    assert.ok(h.some((e) => e.reason === 'suggesting a tag' && e.status === 'open'),
      'a pending suggestion should be in the history as open');
  });
});

// ===========================================================================
describe('the merged chat', () => {
// ===========================================================================

  // recompute() is the only thing that decides chat_state, and it needs a real
  // media root to decide anything at all — which the app-level suite runs
  // without. So these drive it directly against files on disk.

  let db, root, streamId;

  before(() => {
    db = open(DB);
    root = mkdtempSync(join(tmpdir(), 'tenma-chat-'));
    mkdirSync(join(root, 'raws'), { recursive: true });
    const t = Math.floor(Date.now() / 1000);
    streamId = ulid();
    db.prepare(`INSERT INTO stream(id, idx, title, started_at, tz_offset_min,
                 vod_state, chat_state, origin, created_at, updated_at)
               VALUES(?,?,?,?,?, 'unverified','unverified','ingest',?,?)`)
      .run(streamId, 990001, 'chat probe', t, 0, t, t);
    db.prepare(`INSERT INTO capture(id, stream_id, platform, remote_id,
                 chat_path, created_at, updated_at)
               VALUES(?,?,?,?,?,?,?)`)
      .run(ulid(), streamId, 'YT', 'CHATPROBE1', 'raws/probe-yt.json', t, t);
  });

  after(() => { try { rmSync(root, { recursive: true, force: true }); } catch { /* */ } });

  const state = () => recompute(db, streamId, { mediaRoot: root }).chat_state;
  const setMerged = (p, sources = 'YT,TW') =>
    db.prepare('UPDATE stream SET chat_path=?, chat_sources=? WHERE id=?')
      .run(p, sources, streamId);

  test('with no merged file the per-capture raws are still the answer', () => {
    assert.equal(state(), 'lost', 'the raw is named but not on disk');
    writeFileSync(join(root, 'raws', 'probe-yt.json'), '[]');
    assert.equal(state(), 'present');
  });

  test('a merged file that exists is the answer, whatever the raws say', () => {
    // The raws have gone to deep storage and the capture no longer names one.
    db.prepare('UPDATE capture SET chat_path=NULL WHERE stream_id=?').run(streamId);
    assert.equal(state(), 'never', 'nothing named at all');

    writeFileSync(join(root, 'raws', 'probe-merged.json'), '{}');
    setMerged('raws/probe-merged.json');
    assert.equal(state(), 'present',
      'the merged file is the chat once one exists');
    assert.equal(
      db.prepare('SELECT chat_ok FROM stream WHERE id=?').get(streamId).chat_ok, 1);
  });

  test('a merged file that has gone missing reads lost, never present', () => {
    // The failure this replaces was the opposite: the archive kept saying
    // `present` because a capture still named a raw that had been moved away.
    setMerged('raws/not-there.json');
    assert.equal(state(), 'lost');
    assert.equal(
      db.prepare('SELECT chat_ok FROM stream WHERE id=?').get(streamId).chat_ok, 0);
  });

  test('without a media root a merged file is unverified, not assumed good', () => {
    setMerged('raws/probe-merged.json');
    assert.equal(recompute(db, streamId, { mediaRoot: null }).chat_state, 'unverified',
      'nothing was checked, so nothing may be claimed');
  });
});
