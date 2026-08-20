// Walk the tag write path against a throwaway copy of the archive and print
// what happens at each step.
//
//   node scripts/try-tags.js [--db data/archive.db] [--keep]
//
// Copies the database to a scratch file, boots the app in-process against the
// copy, runs the flow, then deletes the scratch. Your archive is never touched,
// so this is safe to run against the real one — which is the point: the flow is
// only convincing if it runs on real rows.

import { copyFileSync, existsSync, rmSync } from 'node:fs';
import { isArchive, resolveDbPath, ulid } from '../db.js';

const argv = process.argv.slice(2);
const flag = (n, d = null) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : argv[i + 1]; };
const SRC = resolveDbPath(flag('db', process.env.TENMA_DB ?? 'data/archive.db'));
const KEEP = argv.includes('--keep');
const SCRATCH = SRC.replace(/\.db$/, '') + '.try.db';

if (!isArchive(SRC)) {
  console.error(`no archive at ${SRC}`);
  process.exit(1);
}
for (const f of [SCRATCH, SCRATCH + '-wal', SCRATCH + '-shm']) if (existsSync(f)) rmSync(f);
copyFileSync(SRC, SCRATCH);

process.env.TENMA_DB = SCRATCH;
process.env.TENMA_DEV_AUTH = '1';        // scratch copy only, never the real one
process.env.TENMA_MEDIA_ROOT = '';
const { makeApp } = await import('../server.js');

const app = makeApp();
const srv = await new Promise((r) => { const s = app.listen(0, '127.0.0.1', () => r(s)); });
const B = `http://127.0.0.1:${srv.address().port}`;

const client = (token) => ({
  get: async (p) => (await fetch(B + p, {
    headers: token ? { authorization: `Bearer ${token}` } : {} })).json(),
  post: async (p, body) => {
    const r = await fetch(B + p, { method: 'POST',
      headers: { 'content-type': 'application/json',
                 ...(token ? { authorization: `Bearer ${token}` } : {}) },
      body: JSON.stringify(body) });
    return { status: r.status, body: await r.json().catch(() => null) };
  },
});
const anon = client();
const login = async (handle, role) =>
  client((await anon.post('/api/auth/token', { handle, role })).body.token);

const say = (s) => console.log(`\n\x1b[1m${s}\x1b[0m`);
const ok = (s) => console.log(`   \x1b[32m✓\x1b[0m ${s}`);
const info = (s) => console.log(`     ${s}`);

try {
  const editor = await login('try-editor', 'editor');
  const fan = await login('try-fan', 'suggester');
  const stream = (await anon.get('/api/streams?limit=1')).streams[0];
  console.log(`scratch copy: ${SCRATCH}`);
  console.log(`test stream : #${stream.idx}  ${stream.title.slice(0, 54)}`);

  // ---------------------------------------------------------------------
  say('1. type a name nobody has used');
  const q = (await anon.get('/api/tags?q=mario kart')).tags;
  info(`GET /api/tags?q=mario kart  ->  ${q.length} hits`);

  const TAG = ulid(), LINK = ulid(), SEG = ulid();
  const r = await editor.post('/api/changesets', {
    reason: 'it is mario kart',
    changes: [
      { target_type: 'tag', target_id: TAG, op: 'create', field: 'name', value: 'Mario Kart 8 Deluxe' },
      { target_type: 'tag', target_id: TAG, op: 'create', field: 'kind', value: 'game' },
      { target_type: 'tag', target_id: TAG, op: 'create', field: 'seg_kind', value: 'game' },
      { target_type: 'stream_tag', target_id: LINK, op: 'create', field: 'stream_id', value: stream.id },
      { target_type: 'stream_tag', target_id: LINK, op: 'create', field: 'tag_id', value: TAG },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'stream_id', value: stream.id },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'start_s', value: 0 },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'end_s', value: 900 },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'kind', value: 'game' },
      { target_type: 'segment', target_id: SEG, op: 'create', field: 'tag_id', value: TAG },
    ] });
  ok(`one changeset — mint + attach + label a block  ->  ${r.body.status}`);

  const d = await anon.get(`/api/streams/${stream.id}`);
  const chip = d.tags.find((t) => t.id === TAG);
  const seg = d.segments.find((s) => s.id === SEG);
  info(`stream.tags     ${chip.name}  [${chip.kind}, ${chip.status}]`);
  info(`segment         ${seg.kind}: "${seg.label}"  ->  tag ${seg.tag.slug}`);
  const after = (await anon.get('/api/tags?q=mario kart')).tags[0];
  ok(`now autocompletes: ${after.name} — slug derived as "${after.slug}"`);

  // ---------------------------------------------------------------------
  say('2. a suggester mints one — proposed, not live');
  const P = ulid();
  const p = await fan.post('/api/changesets', { changes: [
    { target_type: 'tag', target_id: P, op: 'create', field: 'name', value: 'Hollow Knight: Silksong' }] });
  info(`suggester submits  ->  ${p.body.status}  (queues for review)`);
  await editor.post(`/api/changesets/${p.body.id}/review`, { decision: 'approve' });
  info(`public autocomplete after approval:  ${(await anon.get('/api/tags?q=silksong')).tags.length} hits`);
  const hidden = (await editor.get('/api/tags?q=silksong&status=all')).tags[0];
  ok(`editor sees it as "${hidden.status}" — confirm it to put it in the picker`);
  await editor.post('/api/changesets', { changes: [
    { target_type: 'tag', target_id: P, op: 'update', field: 'status', value: 'confirmed' }] });
  info(`after confirming:  ${(await anon.get('/api/tags?q=silksong')).tags.length} hits`);

  // ---------------------------------------------------------------------
  say('3. two people mint the same tag before either is reviewed');
  const mk = () => { const t = ulid(), l = ulid(); return fan.post('/api/changesets', { changes: [
    { target_type: 'tag', target_id: t, op: 'create', field: 'name', value: 'Outer Wilds' },
    { target_type: 'stream_tag', target_id: l, op: 'create', field: 'stream_id', value: stream.id },
    { target_type: 'stream_tag', target_id: l, op: 'create', field: 'tag_id', value: t } ] }); };
  const a = await mk(), b = await mk();
  const ra = await editor.post(`/api/changesets/${a.body.id}/review`, { decision: 'approve' });
  const rb = await editor.post(`/api/changesets/${b.body.id}/review`, { decision: 'approve' });
  info(`first  ->  ${ra.status} ${ra.body.status}`);
  info(`second ->  ${rb.status} ${rb.body.status}   (UNIQUE(slug) would have killed this)`);
  ok(`"outer wilds" rows in the archive: ${(await editor.get('/api/tags?q=outer wilds&status=all')).tags.length}`);

  // ---------------------------------------------------------------------
  say('4. series rollup');
  const fam = (await anon.get('/api/tags?q=super metroid')).tags;
  const canon = fam.find((t) => !/-\d+$/.test(t.slug)) ?? fam[0];
  const kids = fam.filter((t) => t !== canon);
  const was = (await anon.get(`/api/streams?tag=${canon.slug}&limit=100`)).count;
  await editor.post('/api/changesets', { reason: 'roll episodes up', changes: kids.map((k) => (
    { target_type: 'tag', target_id: k.id, op: 'update', field: 'parent_id', value: canon.id })) });
  const now_ = (await anon.get(`/api/streams?tag=${canon.slug}&limit=100`)).count;
  ok(`?tag=${canon.slug}:  ${was} stream(s)  ->  ${now_} after rolling up ${kids.length} episode tags`);

  // ---------------------------------------------------------------------
  say('5. detach');
  const link = (await anon.get(`/api/streams/${stream.id}`)).tags.find((t) => t.id === TAG).link_id;
  await editor.post('/api/changesets', { changes: [
    { target_type: 'stream_tag', target_id: link, op: 'delete' }] });
  ok(`untagged — stream.tags now: ${(await anon.get(`/api/streams/${stream.id}`)).tags.map((t) => t.name).join(', ') || '(none)'}`);

  // ---------------------------------------------------------------------
  say('6. the whole thing is in the history, publicly');
  const h = await anon.get(`/api/streams/${stream.id}/history`);
  for (const e of h.history.slice(0, 4)) {
    info(`${String(e.status).padEnd(8)} ${String(e.author ?? '-').padEnd(12)} ${e.reason ?? '(no reason given)'}`);
  }
} finally {
  srv.close(); app.locals.close();
  if (!KEEP) {
    for (const f of [SCRATCH, SCRATCH + '-wal', SCRATCH + '-shm']) if (existsSync(f)) rmSync(f);
    console.log('\nscratch copy deleted. Your archive was never opened for writing.');
  } else {
    console.log(`\nkept: ${SCRATCH}`);
  }
}
