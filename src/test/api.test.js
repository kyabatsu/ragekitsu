// End-to-end against a real imported archive.
//
// The first four tests exist because the Python version shipped these bugs and
// I found them the hard way. They run first on purpose: a rewrite without them
// guarding it would very likely reintroduce one.
//
//   node --test test/

import { test, before, after, describe } from 'node:test';
import assert from 'node:assert/strict';
import { copyFileSync, existsSync, rmSync, mkdirSync, mkdtempSync,
         writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = join(HERE, '..');
const SEED = join(ROOT, 'data', 'archive.db');
const DB = join(ROOT, 'data', 'test.db');
const TOKEN = 'ingest-test-token';

process.env.TENMA_DB = DB;
process.env.TENMA_DEV_AUTH = '1';
process.env.TENMA_INGEST_TOKEN = TOKEN;
process.env.TENMA_MEDIA_ROOT = '';

const { makeApp, CONFIG } = await import('../server.js');
const { ulid } = await import('../db.js');

let server, base, app;

/** Tiny fetch wrapper that keeps a bearer token, so each "user" is a client. */
function client(token = null) {
  const call = async (method, path, body, extraHeaders = {}) => {
    const headers = { ...extraHeaders };
    if (token) headers.authorization = `Bearer ${token}`;
    if (body !== undefined) headers['content-type'] = 'application/json';
    const res = await fetch(base + path, {
      method, headers, body: body === undefined ? undefined : JSON.stringify(body),
    });
    let json = null;
    try { json = await res.json(); } catch { /* 304 and friends have no body */ }
    return { status: res.status, headers: res.headers, body: json };
  };
  return {
    get: (p, h) => call('GET', p, undefined, h),
    post: (p, b, h) => call('POST', p, b ?? {}, h),
    withToken: (t) => client(t),
  };
}

const anon = () => client();

async function login(handle, role) {
  const r = await anon().post('/api/auth/token', { handle, role });
  assert.equal(r.status, 200, `dev login failed: ${JSON.stringify(r.body)}`);
  return client(r.body.token);
}

before(async () => {
  for (const f of [DB, DB + '-wal', DB + '-shm']) if (existsSync(f)) rmSync(f);
  assert.ok(existsSync(SEED), `expected a seeded archive at ${SEED}`);
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
describe('regressions — these are why the tests come first', () => {
// ===========================================================================

  test('an ingest retry without started_at does not move the broadcast clock', async () => {
    // The completion call sends paths and a duration, usually with no start
    // time. Defaulting it to "now" silently shifted the stream months, which
    // broke platform pairing and every note offset hanging off it.
    const c = anon();
    const T = 1795000000;
    const first = await c.post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'CLOCKTEST1', started_at: T, tz_offset_min: -300 },
      { authorization: `Bearer ${TOKEN}` });
    assert.equal(first.status, 200);
    const id = first.body.id;

    await c.post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'CLOCKTEST1', video_path: 'raws/x.mp4', duration_s: 7200 },
      { authorization: `Bearer ${TOKEN}` });

    const after = await c.get(`/api/streams/${id}`);
    assert.equal(after.body.started_at, T, 'the completion call moved started_at');
  });

  test('a unicode search term does not blow up the ETag', async () => {
    // Headers are latin-1. Interpolating the query into the ETag threw a 500 on
    // any CJK search, and a CRLF in it would have been response splitting.
    const r = await anon().get('/api/streams?q=' + encodeURIComponent('メトロイド'));
    assert.equal(r.status, 200);
    const etag = r.headers.get('etag');
    assert.ok(/^[\x20-\x7e]*$/.test(etag), `ETag is not ASCII: ${etag}`);
  });

  test('a CRLF in a query parameter cannot reach a header', async () => {
    const r = await anon().get('/api/streams?tag=' + encodeURIComponent('\r\nX-Injected: 1'));
    assert.equal(r.status, 200);
    assert.equal(r.headers.get('x-injected'), null);
  });

  test('applying an edit does not silently close a competing proposal', async () => {
    // Auto-superseding threw away real suggestions. A different proposed value
    // is a different opinion; it should go stale and a human should arbitrate.
    const ed = await login('regress-ed', 'editor');
    const sug = await login('regress-fan', 'suggester');
    const s = (await anon().get('/api/streams?limit=1')).body.streams[0];

    const mine = await sug.post('/api/changesets', { changes: [
      { target_type: 'stream', target_id: s.id, op: 'update', field: 'summary', value: 'A' }] });
    assert.equal(mine.body.status, 'open');

    await ed.post('/api/changesets', { changes: [
      { target_type: 'stream', target_id: s.id, op: 'update', field: 'summary', value: 'B' }] });

    const still = await ed.get(`/api/changesets/${mine.body.id}`);
    assert.equal(still.body.status, 'open', 'competing proposal was auto-closed');
    assert.equal(still.body.conflicts.length, 1, 'it should be flagged stale');
  });
});

// ===========================================================================
describe('read', () => {
// ===========================================================================

  test('the grid returns cards, newest first, without notes', async () => {
    const r = await anon().get('/api/streams?limit=5');
    assert.equal(r.status, 200);
    assert.equal(r.body.streams.length, 5);
    const t = r.body.streams.map((s) => s.started_at);
    assert.deepEqual(t, [...t].sort((a, b) => b - a));
    assert.equal(r.body.streams[0].notes, undefined);
    assert.ok(r.body.next.before_id);
  });

  test('a card carries what the grid needs', async () => {
    const s = (await anon().get('/api/streams?limit=1')).body.streams[0];
    for (const k of ['id', 'idx', 'title', 'thumb', 'duration', 'vod_state',
                     'tags', 'watch', 'date', 'start_sod']) {
      assert.ok(k in s, `missing ${k}`);
    }
  });

  test('keyset pagination walks every stream exactly once', async () => {
    const total = (await anon().get('/api/health')).body.streams;
    const seen = [];
    let cursor = null;
    for (let page = 0; page < 60; page++) {
      const qs = '/api/streams?limit=25' +
        (cursor ? `&before=${cursor.before}&before_id=${cursor.before_id}` : '');
      const r = await anon().get(qs);
      seen.push(...r.body.streams.map((s) => s.id));
      cursor = r.body.next;
      if (!cursor) break;
    }
    assert.equal(seen.length, total);
    assert.equal(new Set(seen).size, total, 'a row was returned twice');
  });

  test('include=notes attaches them, and they sort by timestamp', async () => {
    const r = await anon().get('/api/streams/idx/685');
    assert.equal(r.status, 200);
    const ts = r.body.notes.map((n) => n.offset_s).filter((x) => x !== null);
    assert.deepEqual(ts, [...ts].sort((a, b) => a - b));
  });

  test('a note carries a deep link at the right second', async () => {
    const r = await anon().get('/api/streams/idx/685');
    assert.ok(r.body.notes.some((n) => (n.link ?? '').includes('t=15927s')),
      'expected the 04:25:27 note to deep-link to 15927s');
  });

  test('the watch chain leads with an embeddable source', async () => {
    const r = await anon().get('/api/streams/idx/685');
    assert.equal(r.body.watch[0].embeddable, true);
    assert.equal(r.body.watch[0].kind, 'youtube');
    assert.deepEqual(new Set(r.body.captures.map((c) => c.platform)), new Set(['YT', 'TW']));
  });

  test('a thumbnail exists with no pipeline at all', async () => {
    const r = await anon().get('/api/streams/idx/685');
    assert.equal(r.body.thumb_source, 'youtube');
    assert.match(r.body.thumb, /ytimg\.com/);
  });

  test('search finds by note text and by prefix', async () => {
    assert.ok((await anon().get('/api/streams?q=metroid')).body.count >= 1);
    assert.ok((await anon().get('/api/streams?q=tenm')).body.count >= 1);
  });

  test('hostile search input never 500s', async () => {
    for (const q of ['"', '*', 'AND', '-', '^', 'a'.repeat(500), '🍆', 'メトロイド']) {
      const r = await anon().get('/api/streams?q=' + encodeURIComponent(q));
      assert.equal(r.status, 200, `q=${q} returned ${r.status}`);
    }
  });

  test('the month filter uses local time, not UTC', async () => {
    const r = await anon().get('/api/streams?month=2026-07&limit=100');
    assert.ok(r.body.count > 0);
    assert.ok(r.body.streams.every((s) => s.month === '2026-07'));
    assert.equal((await anon().get('/api/streams?month=2026-13')).status, 400);
  });

  test('tags carry kind and counts, and filter', async () => {
    const tags = (await anon().get('/api/tags')).body.tags;
    assert.ok(tags.some((t) => t.kind === 'game'));
    // The two importer 'format' tags land on 'type' after the remap, and
    // nothing survives outside the vocabulary — 'other', 'format' and the old
    // segment words are all gone.
    assert.ok(tags.some((t) => t.kind === 'type'));
    assert.ok(tags.every((t) => ['game', 'person', 'type', 'meta', 'unknown'].includes(t.kind)),
      'a tag kept a kind outside the vocabulary: '
      + JSON.stringify([...new Set(tags.map((t) => t.kind))]));
    const filtered = await anon().get(`/api/streams?tag=${tags[0].slug}&limit=100`);
    assert.ok(filtered.body.count > 0);
  });

  test('a conditional GET returns 304', async () => {
    const first = await anon().get('/api/streams/idx/685');
    const etag = first.headers.get('etag');
    const again = await anon().get('/api/streams/idx/685', { 'if-none-match': etag });
    assert.equal(again.status, 304);
  });

  test('lookup by idx and by ulid agree', async () => {
    const byIdx = (await anon().get('/api/streams/idx/685')).body;
    const byId = (await anon().get(`/api/streams/${byIdx.id}`)).body;
    assert.equal(byId.idx, 685);
    assert.ok(byId.prev_id && byId.next_id);
  });
});

// ===========================================================================
describe('roles', () => {
// ===========================================================================

  test('anonymous cannot propose', async () => {
    const r = await anon().post('/api/changesets', { changes: [] });
    assert.equal(r.status, 401);
  });

  test('capabilities drive the UI', async () => {
    const sug = await login('cap-fan', 'suggester');
    const me = await sug.get('/api/auth/me');
    assert.deepEqual(me.body.can, { suggest: true, edit: false, admin: false });
  });

  test('a suggester cannot see the review queue or administer people', async () => {
    const sug = await login('gate-fan', 'suggester');
    assert.equal((await sug.get('/api/changesets')).status, 403);
    assert.equal((await sug.get('/api/admin/people')).status, 403);
  });

  test('an editor can review but not administer', async () => {
    const ed = await login('gate-ed', 'editor');
    assert.equal((await ed.get('/api/changesets')).status, 200);
    assert.equal((await ed.get('/api/admin/people')).status, 403);
  });

  test('logout ends the session', async () => {
    const adm = await login('gate-boss', 'admin');
    assert.equal((await adm.post('/api/auth/logout')).status, 200);
    assert.equal((await adm.get('/api/auth/me')).body.role, 'viewer');
  });
});

// ===========================================================================
describe('changesets', () => {
// ===========================================================================

  let SID, sug, ed, adm;

  before(async () => {
    sug = await login('cs-fan', 'suggester');
    ed = await login('cs-ed', 'editor');
    adm = await login('cs-boss', 'admin');
    SID = (await anon().get('/api/streams/idx/685')).body.id;
  });

  test('a suggester proposal queues and records what it was written against', async () => {
    const before = (await anon().get(`/api/streams/${SID}`)).body.title;
    const r = await sug.post('/api/changesets', {
      reason: 'cleaner title',
      changes: [{ target_type: 'stream', target_id: SID, op: 'update',
                  field: 'title', value: 'Super Metroid #5 — the finale' }] });
    assert.equal(r.body.status, 'open');
    assert.equal(r.body.changes[0].base_value, before);
    // and nothing has changed yet
    assert.equal((await anon().get(`/api/streams/${SID}`)).body.title, before);
  });

  test('nonsense proposals are refused', async () => {
    const cur = (await anon().get(`/api/streams/${SID}`)).body.title;
    const cases = [
      [{ target_type: 'stream', target_id: SID, op: 'update', field: 'title', value: cur },
       'proposing the current value'],
      [{ target_type: 'stream', target_id: SID, op: 'update', field: 'origin', value: 'web' },
       'a non-writable field'],
      [{ target_type: 'note', target_id: 'abc', op: 'create', field: 'text', value: 'x' },
       'a create without a minted ULID'],
      [{ target_type: 'note', target_id: ulid(), op: 'create', field: 'tag', value: 'clip' },
       'a create missing required fields'],
    ];
    for (const [change, why] of cases) {
      const r = await sug.post('/api/changesets', { changes: [change] });
      assert.equal(r.status, 400, `${why} should be refused`);
    }
  });

  test('approving applies it, and the history is public', async () => {
    const r = await sug.post('/api/changesets', {
      reason: 'fix the summary',
      changes: [{ target_type: 'stream', target_id: SID, op: 'update',
                  field: 'summary', value: 'she beats it' }] });
    const done = await ed.post(`/api/changesets/${r.body.id}/review`, { decision: 'approve' });
    assert.equal(done.body.status, 'applied');
    assert.equal((await anon().get(`/api/streams/${SID}`)).body.summary, 'she beats it');

    const hist = await anon().get(`/api/streams/${SID}/history`);
    const entry = hist.body.history.find((h) => h.id === r.body.id);
    assert.ok(entry, 'the applied changeset is missing from the history');
    assert.equal(entry.author, 'cs-fan');
    assert.equal(entry.reviewer, 'cs-ed');
    assert.equal(entry.status, 'applied');
  });

  test("an editor's own changeset applies on submission", async () => {
    const r = await ed.post('/api/changesets', {
      changes: [{ target_type: 'stream', target_id: SID, op: 'update',
                  field: 'serve_pref', value: 'local' }] });
    assert.equal(r.body.status, 'applied');
  });

  test('a stale changeset is refused, then forced', async () => {
    const p = await sug.post('/api/changesets', {
      changes: [{ target_type: 'stream', target_id: SID, op: 'update',
                  field: 'title', value: 'AAA' }] });
    await ed.post('/api/changesets', {
      changes: [{ target_type: 'stream', target_id: SID, op: 'update',
                  field: 'title', value: 'BBB' }] });

    const blocked = await ed.post(`/api/changesets/${p.body.id}/review`, { decision: 'approve' });
    assert.equal(blocked.status, 409);
    assert.equal(blocked.body.conflicts.length, 1);
    assert.equal(blocked.body.conflicts[0].current, 'BBB');

    const forced = await ed.post(`/api/changesets/${p.body.id}/review`,
      { decision: 'approve', force: true });
    assert.equal(forced.status, 200);
    assert.equal((await anon().get(`/api/streams/${SID}`)).body.title, 'AAA');
  });

  test('a multi-row create lands as one note, or not at all', async () => {
    const NID = ulid();
    const r = await sug.post('/api/changesets', {
      reason: 'found a timestamp',
      changes: [
        { target_type: 'note', target_id: NID, op: 'create', field: 'stream_id', value: SID },
        { target_type: 'note', target_id: NID, op: 'create', field: 'text', value: 'mother brain' },
        { target_type: 'note', target_id: NID, op: 'create', field: 'offset_s', value: 18000 },
        { target_type: 'note', target_id: NID, op: 'create', field: 'tag', value: 'clip' },
      ] });
    assert.equal(r.body.changes.length, 4);
    await ed.post(`/api/changesets/${r.body.id}/review`, { decision: 'approve' });

    const detail = (await anon().get(`/api/streams/${SID}`)).body;
    const note = detail.notes.find((n) => n.id === NID);
    assert.ok(note, 'the note did not appear in the rail');
    assert.equal(note.text, 'mother brain');
    assert.match(note.link, /t=18000s/);
    assert.equal(note.author, 'cs-fan');
  });

  test('rejecting leaves the value alone, and cannot be re-reviewed', async () => {
    const before = (await anon().get(`/api/streams/${SID}`)).body.summary;
    const p = await sug.post('/api/changesets', {
      changes: [{ target_type: 'stream', target_id: SID, op: 'update',
                  field: 'summary', value: 'nope' }] });
    const r = await ed.post(`/api/changesets/${p.body.id}/review`, { decision: 'reject' });
    assert.equal(r.body.status, 'rejected');
    assert.equal((await anon().get(`/api/streams/${SID}`)).body.summary, before);
    const again = await ed.post(`/api/changesets/${p.body.id}/review`, { decision: 'approve' });
    assert.equal(again.status, 400);
  });

  test('a delete tombstones rather than removing', async () => {
    const victim = (await anon().get('/api/streams?limit=1')).body.streams[0].id;
    await ed.post('/api/changesets', {
      reason: 'not a real stream',
      changes: [{ target_type: 'stream', target_id: victim, op: 'delete' }] });

    const grid = await anon().get('/api/streams?limit=100');
    assert.ok(!grid.body.streams.some((s) => s.id === victim), 'still in the grid');
    const row = await anon().get(`/api/streams/${victim}`);
    assert.equal(row.body.retracted.why, 'not a real stream');
  });

  test('an admin can promote but not lock themselves out', async () => {
    const people = (await adm.get('/api/admin/people')).body.people;
    const fan = people.find((p) => p.handle === 'cs-fan');
    assert.equal((await adm.post(`/api/admin/people/${fan.id}/role`,
      { role: 'editor' })).status, 200);
    const me = (await adm.get('/api/auth/me')).body;
    assert.equal((await adm.post(`/api/admin/people/${me.id}/role`,
      { role: 'viewer' })).status, 400);
  });
});

// ===========================================================================
describe('ingest', () => {
// ===========================================================================

  const H = { authorization: `Bearer ${TOKEN}` };
  const T = 1796000000;

  test('a bad or missing token is refused', async () => {
    assert.equal((await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'x' })).status, 401);
    assert.equal((await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'x' }, { authorization: 'Bearer wrong' })).status, 401);
  });

  test('validation', async () => {
    assert.equal((await anon().post('/api/ingest/capture',
      { platform: 'KICK', remote_id: 'x' }, H)).status, 400);
    assert.equal((await anon().post('/api/ingest/capture',
      { platform: 'YT' }, H)).status, 400);
  });

  /* The two packets ls-rec sends, in the shape it sends them. */
  test('the recorder fills the two clocks nothing else can ever know', async () => {
    // Every one of the archive's 371 imported captures has broadcast_started_at
    // and local_start_wall NULL. The importer never had them and nothing can
    // recover them later — the recorder is the only thing that is present at
    // both moments. This is the whole point of the ingest path.
    const START = 1796700000;      // the platform says the broadcast began here
    const REC = START + 37;        // ...and yt-dlp started writing 37s later
    const r = await anon().post('/api/ingest/capture', {
      platform: 'YT', remote_id: 'LSREC0001', index: 9001,
      title: '[ TEST ] a stream ls-rec started', url: 'https://youtu.be/LSREC0001',
      started_at: START, broadcast_started_at: START, record_started_at: REC,
      tz_offset_min: -300 }, H);
    assert.equal(r.status, 200, JSON.stringify(r.body));
    assert.equal(r.body.created, true);

    const cap = (await anon().get(`/api/streams/${r.body.id}`)).body
      .captures.find((c) => c.remote_id === 'LSREC0001');
    assert.equal(cap.remote_start_wall, START, 'the platform clock');
    assert.equal(cap.local_start_wall, REC, 'the recording clock');

    // ...and the wrapup, which must not blank anything the start call set.
    const done = await anon().post('/api/ingest/capture', {
      platform: 'YT', remote_id: 'LSREC0001',
      video_path: 'raws/9001_a stream.mp4', chat_path: 'raws/9001_a stream.json',
      duration_s: 7321 }, H);
    assert.equal(done.status, 200, JSON.stringify(done.body));
    assert.equal(done.body.created, false, 'the wrapup made a second stream');

    const after = (await anon().get(`/api/streams/${r.body.id}`)).body;
    const cap2 = after.captures.find((c) => c.remote_id === 'LSREC0001');
    assert.equal(cap2.remote_start_wall, START, 'the wrapup moved the platform clock');
    assert.equal(cap2.local_start_wall, REC, 'the wrapup moved the recording clock');
    assert.equal(cap2.file_duration_s, 7321);
    assert.equal(after.title, '[ TEST ] a stream ls-rec started',
      'the wrapup blanked the title the start call set');
  });

  test('an unknown broadcast start stays unknown rather than becoming now', async () => {
    // stream_start_epoch can be None — the probe fails, or the platform simply
    // does not say. Defaulting it to now() would write a measurement nobody
    // made into the column every note offset is computed through.
    const r = await anon().post('/api/ingest/capture', {
      platform: 'TW', remote_id: 'LSREC0002', title: 'no start time known' }, H);
    assert.equal(r.status, 200, JSON.stringify(r.body));
    const cap = (await anon().get(`/api/streams/${r.body.id}`)).body
      .captures.find((c) => c.remote_id === 'LSREC0002');
    assert.equal(cap.remote_start_wall, null,
      'a platform clock was invented for a recorder that did not report one');
    assert.equal(cap.local_start_wall, null);
  });

  test('the second half of a dual stream joins the first, not a new row', async () => {
    // The recorder pairs by its own in-memory window; the server pairs against
    // the whole archive, so a daemon restarted between the two halves still
    // produces one broadcast rather than two.
    const T2 = 1796200000;
    const a = await anon().post('/api/ingest/capture', {
      platform: 'YT', remote_id: 'DUAL0001', title: 'both at once',
      started_at: T2, broadcast_started_at: T2, record_started_at: T2 + 10 }, H);
    const b = await anon().post('/api/ingest/capture', {
      platform: 'TW', remote_id: 'DUAL0002', title: 'both at once',
      started_at: T2 + 45, broadcast_started_at: T2 + 45, record_started_at: T2 + 52 }, H);
    assert.equal(b.body.created, false, 'the twitch half made its own stream');
    assert.equal(b.body.id, a.body.id);
    assert.match(String(b.body.paired_with), /YT/);
    const caps = (await anon().get(`/api/streams/${a.body.id}`)).body.captures;
    assert.equal(caps.length, 2);
    // Each half keeps its OWN platform clock — they disagree by 45s and that
    // disagreement is the entire reason the clock model exists.
    assert.equal(caps.find((c) => c.platform === 'YT').remote_start_wall, T2);
    assert.equal(caps.find((c) => c.platform === 'TW').remote_start_wall, T2 + 45);
  });

  test('record-start creates a stream and hands back the index', async () => {
    const r = await anon().post('/api/ingest/capture', {
      platform: 'YT', remote_id: 'INGEST0001', title: 'brand new',
      url: 'https://www.youtube.com/watch?v=INGEST0001',
      started_at: T, tz_offset_min: -300 }, H);
    assert.equal(r.body.created, true);
    assert.ok(r.body.index > 689, `index ${r.body.index} should follow the archive`);

    // idempotent
    const again = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'INGEST0001' }, H);
    assert.equal(again.body.id, r.body.id);
    assert.equal(again.body.created, false);
    assert.equal(again.body.index, r.body.index);
  });

  test('two equidistant streams pair with the fresher one, every time', async () => {
    // Until the tiebreak was explicit, two streams the same distance away with
    // no capture of this platform were separated by whatever the query plan
    // returned. It stayed hidden for weeks because the seed data only produced
    // a tie by accident, and then a harmless SELECT change flipped it.
    const AT = T + 700000;
    const old = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'TIE00001', started_at: AT }, H);
    const fresh = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'TIE00002', started_at: AT }, H);
    assert.notEqual(old.body.id, fresh.body.id);
    const tw = await anon().post('/api/ingest/capture',
      { platform: 'TW', remote_id: 'TIE00003', started_at: AT }, H);
    assert.equal(tw.body.id, fresh.body.id,
      'a tie must resolve to the most recently created stream, not an arbitrary one');
  });

  test('the second platform pairs into the same stream', async () => {
    const yt = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'PAIR0001', started_at: T + 100000, tz_offset_min: -300 }, H);
    const tw = await anon().post('/api/ingest/capture',
      { platform: 'TW', remote_id: 'PAIR0002', started_at: T + 100300, tz_offset_min: -300 }, H);
    assert.equal(tw.body.id, yt.body.id);
    assert.equal(tw.body.created, false);
    assert.equal(tw.body.paired_with, 'YT');
  });

  test('a broadcast outside the pair window is separate', async () => {
    const a = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'FAR0001', started_at: T + 500000, tz_offset_min: -300 }, H);
    const b = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'FAR0002', started_at: T + 900000, tz_offset_min: -300 }, H);
    assert.notEqual(a.body.id, b.body.id);
    assert.equal(b.body.created, true);
  });

  test('the index climbs past the 999 ceiling ls-rec had', async () => {
    const r = await anon().get('/api/ingest/next-index', H);
    assert.ok(r.body.next_index > 690);
  });

  test('an ingested stream is immediately readable and marked as such', async () => {
    const r = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'READ0001', title: 'readable',
        started_at: T + 1200000, tz_offset_min: -300 }, H);
    const s = await anon().get(`/api/streams/${r.body.id}`);
    assert.equal(s.body.origin, 'ingest');
    assert.equal(s.body.idx, r.body.index);
    const byIdx = await anon().get(`/api/streams/idx/${r.body.index}`);
    assert.equal(byIdx.body.id, r.body.id);
  });
});

// ===========================================================================
describe('reconcile — ls-audit reads before it writes', () => {
// ===========================================================================

  const H = { authorization: `Bearer ${TOKEN}` };
  const T = 1797000000;

  /* ls-audit reconstructs from files long after the fact, so unlike the
     recorder it can be wrong in ways the archive already knows better about.
     Everything here exists to make that survivable. */

  test('lookup finds a stream by remote_id, not just by index', async () => {
    const made = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'LOOKUP01', title: 'findable',
        started_at: T, record_started_at: T + 30 }, H);
    const r = await anon().get('/api/ingest/lookup?id=YT:LOOKUP01', H);
    assert.equal(r.status, 200);
    assert.equal(r.body.found, true);
    assert.equal(r.body.by, 'remote_id');
    assert.equal(r.body.stream.id, made.body.id);
    const cap = r.body.stream.captures.find((c) => c.remote_id === 'LOOKUP01');
    assert.equal(cap.local_start_wall, T + 30);
    assert.equal(cap.local_start_precision_s, 1);
  });

  test('a broadcast the archive has never seen reports found:false', async () => {
    const r = await anon().get('/api/ingest/lookup?id=YT:NOTHERE9&idx=99999', H);
    assert.equal(r.body.found, false);
    assert.equal(r.body.stream, null);
    assert.ok(r.body.next_index > 0);
  });

  test('lookup refuses to pick a winner when the two identities disagree', async () => {
    // The vault says #N, but that remote_id lives on a different stream. This
    // is ls-rec's auto-index and Obsidian's index having drifted apart, and
    // guessing would silently merge two broadcasts.
    const a = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'DRIFT001', started_at: T + 100 }, H);
    const b = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'DRIFT002', started_at: T + 9000000 }, H);
    assert.notEqual(a.body.id, b.body.id);
    const r = await anon().get(
      `/api/ingest/lookup?id=YT:DRIFT001&idx=${b.body.index}`, H);
    assert.ok(r.body.conflict, 'expected a conflict');
    assert.equal(r.body.conflict.kind, 'identity');
    assert.ok(r.body.conflict.by_remote && r.body.conflict.by_idx);
  });

  test('lookup reports which fields a human has actually decided', async () => {
    const made = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'HUMAN001', title: 'machine title',
        started_at: T + 200 }, H);
    const before = await anon().get('/api/ingest/lookup?id=YT:HUMAN001', H);
    assert.deepEqual(before.body.stream.human_fields, []);

    const ed = await login('reconcile-ed', 'editor');
    const cs = await ed.post('/api/changesets', {
      reason: 'proper title',
      changes: [{ target_type: 'stream', target_id: made.body.id,
                  op: 'update', field: 'title', value: 'Hand Written' }],
    });
    assert.equal(cs.status, 200);

    const after = await anon().get('/api/ingest/lookup?id=YT:HUMAN001', H);
    assert.ok(after.body.stream.human_fields.includes('title'),
      'a human edit has to be visible to the machine about to overwrite it');
    assert.equal(after.body.stream.title, 'Hand Written');
  });

  test('an explicit stream_id skips the pairing guess entirely', async () => {
    // Two broadcasts months apart. Without stream_id the second capture would
    // have to be matched by start-time proximity; ls-audit already knows the
    // answer and says so.
    const host = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'TARGET01', started_at: T + 300 }, H);
    const joined = await anon().post('/api/ingest/capture',
      { platform: 'TW', remote_id: 'TARGET02', stream_id: host.body.id,
        started_at: T + 50000000 }, H);
    assert.equal(joined.body.id, host.body.id);
    assert.equal(joined.body.created, false);
    const r = await anon().get('/api/ingest/lookup?id=YT:TARGET01', H);
    assert.equal(r.body.stream.captures.length, 2);
  });

  test('moving a capture between streams is refused, not done quietly', async () => {
    const a = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'MOVE0001', started_at: T + 400 }, H);
    const b = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'MOVE0002', started_at: T + 60000000 }, H);
    const r = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'MOVE0001', stream_id: b.body.id }, H);
    assert.equal(r.status, 409);
    assert.equal(r.body.on_stream, a.body.id);
    // and nothing moved
    const check = await anon().get('/api/ingest/lookup?id=YT:MOVE0001', H);
    assert.equal(check.body.stream.id, a.body.id);
  });

  test('an unknown stream_id is a 404 rather than a new stream', async () => {
    const r = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'GHOST001', stream_id: 'NOSUCHSTREAM' }, H);
    assert.equal(r.status, 404);
  });

  test('a reconstructed clock records how well it was measured', async () => {
    // ls-audit read this one off a filename, which is good to the minute. The
    // recorder writes 1. Storing 60 is the difference between a measurement
    // and a guess, and the whole reason the column exists.
    const r = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'PRECIS01', started_at: T + 500,
        record_started_at: T + 530, local_start_precision_s: 60 }, H);
    assert.equal(r.status, 200);
    const got = await anon().get('/api/ingest/lookup?id=YT:PRECIS01', H);
    const cap = got.body.stream.captures[0];
    assert.equal(cap.local_start_wall, T + 530);
    assert.equal(cap.local_start_precision_s, 60);
  });

  test('a stream-level date correction moves the date and nothing else', async () => {
    const made = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'REDATE01', title: 'first',
        started_at: T + 600, broadcast_started_at: T + 600,
        record_started_at: T + 640 }, H);
    const was = (await anon().get('/api/ingest/lookup?id=YT:REDATE01', H)).body.stream;

    const moved = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'REDATE01',
        stream: { started_at: T + 300, tz_offset_min: -360 } }, H);
    assert.equal(moved.status, 200);

    const now2 = (await anon().get('/api/ingest/lookup?id=YT:REDATE01', H)).body.stream;
    assert.equal(now2.started_at, T + 300);
    assert.equal(now2.tz_offset_min, -360);
    // The capture's own clocks are untouched: a note at 02:30:00 is still at
    // 02:30:00 after the entry's date is corrected.
    assert.equal(now2.captures[0].remote_start_wall,
                 was.captures[0].remote_start_wall);
    assert.equal(now2.captures[0].local_start_wall,
                 was.captures[0].local_start_wall);
    assert.equal(now2.title, was.title, 'a date fix must not touch the title');
  });

  test('unknown fields are refused, not quietly dropped', async () => {
    // The whole point of ingest is that it cannot reach the subjective half of
    // the archive. Silently ignoring a field nobody recognises turns a client
    // bug into a no-op that survives for months; a 400 turns it into a log line
    // on the first run.
    for (const bad of [{ summary: 'a machine wrote this' },
                       { notes: ['nope'] },
                       { tags: ['zatsu'] },
                       { retracted_at: 1 }]) {
      const r = await anon().post('/api/ingest/capture',
        { platform: 'YT', remote_id: 'STRICT01', ...bad }, H);
      assert.equal(r.status, 400, `${Object.keys(bad)[0]} should be refused`);
      assert.match(String(r.body.fields), new RegExp(Object.keys(bad)[0]));
    }
    const nested = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'STRICT01', stream: { summary: 'nope' } }, H);
    assert.equal(nested.status, 400);
    assert.deepEqual(nested.body.fields, ['stream.summary']);
    // and nothing was created along the way
    const look = await anon().get('/api/ingest/lookup?id=YT:STRICT01', H);
    assert.equal(look.body.found, false);
  });

  test('only media paths may be cleared', async () => {
    await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'CLEAR001', title: 'keep me',
        started_at: T + 800, chat_path: 'raws/x.json',
        record_started_at: T + 810 }, H);
    for (const f of ['title', 'record_started_at', 'remote_id', 'local_start_wall']) {
      const r = await anon().post('/api/ingest/capture',
        { platform: 'YT', remote_id: 'CLEAR001', clear: [f] }, H);
      assert.equal(r.status, 400, `${f} must not be clearable`);
    }
    const before = (await anon().get('/api/ingest/lookup?id=YT:CLEAR001', H)).body.stream;
    assert.equal(before.captures[0].chat_path, 'raws/x.json');

    const ok = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'CLEAR001', clear: ['chat_path'] }, H);
    assert.equal(ok.status, 200);
    const after = (await anon().get('/api/ingest/lookup?id=YT:CLEAR001', H)).body.stream;
    assert.equal(after.captures[0].chat_path, null);
    // the things that are not paths survived
    assert.equal(after.title, before.title);
    assert.equal(after.captures[0].local_start_wall, T + 810);
  });

  test('the merged chat replaces the raws as the answer', async () => {
    const made = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'MERGE001', started_at: T + 900,
        chat_path: 'raws/900_yt.json' }, H);
    await anon().post('/api/ingest/capture',
      { platform: 'TW', remote_id: 'MERGE002', stream_id: made.body.id,
        chat_path: 'raws/900_tw.json' }, H);

    // Before the merge: provenance comes from the captures and is verifiable.
    let s = (await anon().get(`/api/streams/${made.body.id}`)).body;
    assert.deepEqual(s.chat.sources.map((x) => x.platform).sort(), ['TW', 'YT']);
    assert.equal(s.chat.merged, null);
    assert.ok(s.chat.sources.every((x) => x.capture_id));

    // ls-audit merges, then forgets the raws.
    const done = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'MERGE001', clear: ['chat_path'],
        stream: { chat_path: 'raws/900_merged-chat.json', chat_sources: 'YT,TW' } }, H);
    assert.equal(done.status, 200);
    await anon().post('/api/ingest/capture',
      { platform: 'TW', remote_id: 'MERGE002', clear: ['chat_path'] }, H);

    s = (await anon().get(`/api/streams/${made.body.id}`)).body;
    assert.equal(s.chat.merged, 'raws/900_merged-chat.json');
    // Provenance survives the raws leaving — as a stored claim now, which is
    // why capture_id is null: there is nothing left to re-verify it against.
    assert.deepEqual(s.chat.sources.map((x) => x.platform), ['TW', 'YT']);
    assert.ok(s.chat.sources.every((x) => x.capture_id === null));
  });

  test('chat_sources is canonicalised and validated', async () => {
    const r = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'SRC00001', started_at: T + 1000,
        stream: { chat_path: 'raws/x.json', chat_sources: 'tw, yt ,TW' } }, H);
    assert.equal(r.status, 200);
    const s = (await anon().get('/api/ingest/lookup?id=YT:SRC00001', H)).body.stream;
    assert.equal(s.chat_sources, 'TW,YT', 'sorted, de-duplicated, upper-cased');

    const bad = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'SRC00001',
        stream: { chat_path: 'raws/x.json', chat_sources: 'YT,KICK' } }, H);
    assert.equal(bad.status, 400);
  });

  test('a derived state cannot be set by hand, because it would not hold', async () => {
    // vod_state and chat_state used to be in WRITABLE and offered as enums in
    // the record editor. recompute() rewrites both on the next write, so the
    // edit saved and then silently reverted — a field that refuses to hold a
    // value while reporting success. They are refused outright now, and the
    // way to change a state is to correct the path it derives from.
    const r = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'DERIVED1', started_at: T + 1300 }, H);
    const ed = await login('derived-ed', 'editor');
    for (const field of ['vod_state', 'chat_state']) {
      const cs = await ed.post('/api/changesets', {
        reason: 'forcing a state',
        changes: [{ target_type: 'stream', target_id: r.body.id,
                    op: 'update', field, value: 'present' }],
      });
      assert.notEqual(cs.status, 200, `${field} must not be writable`);
    }
    // ...but the input it derives from is.
    const ok = await ed.post('/api/changesets', {
      reason: 'the merged chat lives here',
      changes: [{ target_type: 'stream', target_id: r.body.id,
                  op: 'update', field: 'chat_path', value: 'raws/705_merged-chat.json' }],
    });
    assert.equal(ok.status, 200, JSON.stringify(ok.body));
    const s2 = (await anon().get(`/api/streams/${r.body.id}`)).body;
    assert.equal(s2.chat_path, 'raws/705_merged-chat.json');
    assert.equal(s2.chat.merged, 'raws/705_merged-chat.json');
  });

  test('the record editor can round-trip the fields it shows', async () => {
    // It reads s[field] and writes a changeset naming the same field, so a
    // field it offers has to exist on the stream object under that exact name.
    const r = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'ROUNDTR1', started_at: T + 1400,
        stream: { chat_path: 'raws/x.json', chat_sources: 'YT' } }, H);
    const s2 = (await anon().get(`/api/streams/${r.body.id}`)).body;
    for (const f of ['tz_offset_min', 'chat_path', 'chat_sources']) {
      assert.ok(f in s2, `the editor shows ${f}, so the API must expose it`);
    }
    assert.equal(s2.chat_sources, 'YT');
  });

  test('a stream that has never been merged keeps the old chat answer', async () => {
    // All 371 imported captures are in this state and must stay working.
    const r = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'LEGACY01', started_at: T + 1100,
        chat_path: 'raws/legacy.json' }, H);
    const s = (await anon().get(`/api/streams/${r.body.id}`)).body;
    assert.equal(s.chat.merged, null);
    assert.equal(s.chat.sources.length, 1);
    assert.equal(s.chat.sources[0].platform, 'YT');
    assert.ok(s.chat.sources[0].capture_id, 'the raw is still the thing being described');
  });

  test('a stream sub-object on create lands on the new row', async () => {
    const r = await anon().post('/api/ingest/capture',
      { platform: 'YT', remote_id: 'SCREATE1',
        stream: { title: 'From The Vault', started_at: T + 700,
                  tz_offset_min: -300 } }, H);
    assert.equal(r.body.created, true);
    const s = (await anon().get('/api/ingest/lookup?id=YT:SCREATE1', H)).body.stream;
    assert.equal(s.title, 'From The Vault');
    assert.equal(s.started_at, T + 700);
    // The stream's date is not a claim about the player's t=0.
    assert.equal(s.captures[0].remote_start_wall, null);
  });
});

// ===========================================================================
describe('browsing the media root', () => {
// ===========================================================================

  // The record editor offers a file picker rather than a text box, because
  // `raws/697_[ MINA THE HOLLOWER #2 ] … @ 2026-08-10_11-02.mp4` typed by hand
  // is a path with a typo in it, and a path with a typo reads as `lost`.

  let root, ed, app2, srv2, base2;

  before(async () => {
    root = mkdtempSync(join(tmpdir(), 'tenma-browse-'));
    mkdirSync(join(root, 'raws', 'chats'), { recursive: true });
    writeFileSync(join(root, 'raws', '697_a [fWk_JdowmGE].mp4'), 'x');
    writeFileSync(join(root, 'raws', '698_b [idMOjigBk5g].mkv'), 'x');
    writeFileSync(join(root, 'raws', '697_still.jpg'), 'x');
    writeFileSync(join(root, 'raws', '697_merged-chat.json'), 'x');
    writeFileSync(join(root, 'raws', '.hidden.mp4'), 'x');
    writeFileSync(join(root, 'raws', 'chats', 'old.json'), 'x');
    // A second app with a media root. CONFIG is read at import, so setting the
    // env var here would do nothing — the override has to be passed in.
    app2 = makeApp({ ...CONFIG, mediaRoot: root });
    await new Promise((r) => { srv2 = app2.listen(0, '127.0.0.1', r); });
    base2 = `http://127.0.0.1:${srv2.address().port}`;
    const tok = await (await fetch(base2 + '/api/auth/token', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ handle: 'browse-ed', role: 'editor' }),
    })).json();
    ed = tok.token;
  });

  after(() => {
    srv2?.close(); app2?.locals?.close?.();
    try { rmSync(root, { recursive: true, force: true }); } catch { /* */ }
  });

  const browse = async (qs, token = ed) => {
    const r = await fetch(`${base2}/api/media/browse?${qs}`,
      token ? { headers: { authorization: `Bearer ${token}` } } : undefined);
    let body = null;
    try { body = await r.json(); } catch { /* */ }
    return { status: r.status, body };
  };

  test('a listing of the media root is not public', async () => {
    assert.equal((await browse('dir=raws', null)).status, 401);
    const fan = await (await fetch(base2 + '/api/auth/token', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ handle: 'browse-fan', role: 'suggester' }),
    })).json();
    assert.equal((await browse('dir=raws', fan.token)).status, 403);
  });

  test('it lists what is there, newest first, without dotfiles', async () => {
    const r = await browse('dir=raws');
    assert.equal(r.status, 200);
    assert.deepEqual(r.body.dirs, ['chats']);
    const names = r.body.files.map((f) => f.name);
    assert.ok(!names.some((n) => n.startsWith('.')), 'dotfiles are noise');
    assert.ok(names.includes('697_a [fWk_JdowmGE].mp4'));
    assert.ok(r.body.files.every((f) => f.path.startsWith('raws/')),
      'the path is what the column stores, not the bare name');
  });

  test('kind filters to what the field can hold', async () => {
    const v = (await browse('dir=raws&kind=video')).body.files.map((f) => f.name);
    assert.deepEqual(v.sort(), ['697_a [fWk_JdowmGE].mp4', '698_b [idMOjigBk5g].mkv']);
    const i = (await browse('dir=raws&kind=image')).body.files.map((f) => f.name);
    assert.deepEqual(i, ['697_still.jpg']);
    // json is neither, so the merged chat never shows up in a video picker
    assert.ok(!v.includes('697_merged-chat.json'));
  });

  test('the filter is a substring of the name', async () => {
    const r = await browse('dir=raws&kind=video&q=mina');
    assert.deepEqual(r.body.files.map((f) => f.name), []);
    const hit = await browse('dir=raws&kind=video&q=698');
    assert.deepEqual(hit.body.files.map((f) => f.name), ['698_b [idMOjigBk5g].mkv']);
  });

  test('it cannot be walked out of the media root', async () => {
    for (const dir of ['../..', 'raws/../../etc', 'raws/./../../..', '../']) {
      const r = await browse(`dir=${encodeURIComponent(dir)}`);
      assert.equal(r.status, 400, `${dir} escaped`);
    }
    // A leading slash is not an absolute path here, it is a stripped prefix.
    assert.equal((await browse('dir=%2Fetc')).status, 404);
  });

  test('subdirectories work and report their way back', async () => {
    const r = await browse('dir=raws/chats');
    assert.equal(r.status, 200);
    assert.equal(r.body.parent, 'raws');
    assert.deepEqual(r.body.files.map((f) => f.path), ['raws/chats/old.json']);
  });

  test('without a media root it says so rather than guessing', async () => {
    // The default app in this file runs with TENMA_MEDIA_ROOT unset.
    const tok = (await anon().post('/api/auth/token',
      { handle: 'browse-ed2', role: 'editor' })).body.token;
    const r = await client(tok).get('/api/media/browse?dir=raws');
    assert.equal(r.status, 503);
  });
});

// ===========================================================================
describe('health', () => {
  test('reports the shape of the archive', async () => {
    const h = (await anon().get('/api/health')).body;
    assert.ok(h.streams > 200);
    assert.equal(h.retracted, 1);
    assert.ok(h.notes > 800);
    assert.ok('vod_states' in h);
  });
});
