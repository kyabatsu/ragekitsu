// Point Node at an existing archive.db and report what it finds.
// Usage: node scripts/check-db.js [path]
import { create, isArchive, migrate, meta, open, resolveDbPath } from '../db.js';
import { buildTimeline, segmentOverlaps } from '../archive.js';

// Accepts a bare path, and tolerates `--db x` because the other two scripts
// take it that way and muscle memory does not read usage strings.
const argv = process.argv.slice(2);
const i = argv.indexOf('--db');
const path = (i !== -1 ? argv[i + 1] : argv.find((a) => !a.startsWith('--')))
  || 'data/archive.db';
const full = resolveDbPath(path);

// open() will happily conjure an empty database at a mistyped path, and then
// every line below reports something baffling about a missing table. This is a
// check tool: it reads, it never creates.
if (!isArchive(path)) {
  console.error(`no archive at ${full}`);
  console.error('');
  console.error('  Nothing there, or the file exists but has no `stream` table.');
  console.error('  Run this from the project root, or pass the path:');
  console.error('    node scripts/check-db.js path/to/archive.db');
  process.exit(1);
}

console.log(`opening ${full}\n`);
const db = open(path);
const applied = migrate(db);
console.log(applied.length ? `migrations applied: ${applied.join(', ')}`
                           : 'migrations: nothing to do (already current)');

const one = (sql, ...a) => db.prepare(sql).get(...a);
const n = (t) => one(`SELECT COUNT(*) c FROM ${t}`).c;

console.log(`\nschema v${meta(db, 'schema_version')}   generation ${meta(db, 'generation')}`);
for (const t of ['stream', 'capture', 'note', 'segment', 'tag', 'stream_tag',
                 'person', 'changeset', 'change', 'upload']) {
  console.log(`  ${t.padEnd(12)} ${String(n(t)).padStart(6)}`);
}

// ---------------------------------------------------------------------------
// clocks — what is known, and what is merely assumed
// ---------------------------------------------------------------------------
console.log('\nclocks');
const q1 = (sql) => one(sql).c;
console.log(`  remote clock set     ${String(q1(
  'SELECT COUNT(*) c FROM capture WHERE remote_start_wall IS NOT NULL')).padStart(6)} / ${n('capture')}`);
console.log(`  local clock set      ${String(q1(
  'SELECT COUNT(*) c FROM capture WHERE local_start_wall IS NOT NULL')).padStart(6)} / ${n('capture')}`);
console.log(`  probed by ffprobe    ${String(q1(
  'SELECT COUNT(*) c FROM capture WHERE probed_at IS NOT NULL')).padStart(6)} / ${n('capture')}`);
console.log(`  notes, frame unknown ${String(q1(
  `SELECT COUNT(*) c FROM note WHERE retracted_at IS NULL AND frame='unknown'`)).padStart(6)} / ${n('note')}`);
console.log(`  streams, no duration ${String(q1(
  'SELECT COUNT(*) c FROM stream WHERE duration_s IS NULL AND retracted_at IS NULL')).padStart(6)} / ${n('stream')}`);

// Captures parked outside their own stream's span. Only detectable at all
// because the clocks are absolute; before that this was invisible.
const oos = db.prepare(
  `SELECT s.idx, s.duration_s, cp.platform, cp.offset_s, substr(s.title,1,40) t
   FROM stream s JOIN capture cp ON cp.stream_id = s.id
   WHERE s.retracted_at IS NULL AND (
     (s.duration_s IS NOT NULL AND ABS(cp.offset_s) > s.duration_s)
     OR (s.duration_s IS NULL AND ABS(cp.offset_s) > 3600))
   ORDER BY ABS(cp.offset_s) DESC`).all();
if (oos.length) {
  console.log(`\n${oos.length} capture(s) sit outside their stream's span — likely mispaired`);
  for (const r of oos) {
    const h = (r.offset_s / 3600).toFixed(1);
    console.log(`  #${r.idx}  ${r.platform} offset ${String(r.offset_s).padStart(7)} (${h}h)` +
                `  duration ${r.duration_s ?? '?'}   ${r.t}`);
  }
}

// ---------------------------------------------------------------------------
// tags that two names want to share. The slug backfill leaves these alone
// because merging moves stream_tag rows, which is a decision, not a cleanup.
// ---------------------------------------------------------------------------
{
  const { slugify } = await import('../db.js');
  const rows = db.prepare('SELECT id, name, slug FROM tag').all();
  const byWant = new Map();
  for (const r of rows) {
    const w = slugify(r.name);
    if (!byWant.has(w)) byWant.set(w, []);
    byWant.get(w).push(r);
  }
  const clashes = [...byWant.entries()].filter(([, v]) => v.length > 1);
  const stale = rows.filter((r) => r.slug !== slugify(r.name));
  if (clashes.length) {
    console.log(`\n${clashes.length} slug collision(s) — two names, one url. Merge by hand:`);
    for (const [want, v] of clashes) {
      console.log(`  ${want}`);
      for (const r of v) console.log(`     ${r.id}  ${r.name}`);
    }
  }
  if (stale.length) {
    console.log(`\n${stale.length} tag(s) still carry a url-unsafe slug (blocked by the above)`);
  }
}

// ---------------------------------------------------------------------------
// the materialised timeline has to agree with a fresh projection, or the
// strip, the pins and the player are reading different numbers
// ---------------------------------------------------------------------------
{
  const ids = db.prepare(
    'SELECT id FROM stream WHERE retracted_at IS NULL').all().map((r) => r.id);
  let drift = 0, missing = 0, overlaps = 0;
  for (const id of ids) {
    const stored = one('SELECT timeline_json j FROM stream WHERE id = ?', id).j;
    if (!stored) { missing++; continue; }
    const fresh = JSON.stringify(buildTimeline(db, id,
      { durationSource: JSON.parse(stored).duration_source }));
    if (fresh !== stored) drift++;
    if (segmentOverlaps(db, id).length) overlaps++;
  }
  console.log(`\ntimeline projection  ${ids.length - missing - drift}/${ids.length} current` +
              `   ${drift} drifted   ${missing} never built`);
  if (overlaps) console.log(`  !! ${overlaps} stream(s) have overlapping segments`);
  if (drift) console.log('  run a recompute pass to rebuild them');
}

const s = one(`SELECT id, idx, title, local_month, start_sod, started_at
               FROM stream ORDER BY started_at DESC LIMIT 1`);
if (s) {
  const hh = String(Math.floor(s.start_sod / 3600)).padStart(2, '0');
  const mm = String(Math.floor((s.start_sod % 3600) / 60)).padStart(2, '0');
  console.log(`\nnewest stream  #${s.idx}  ${s.local_month}  ${hh}:${mm} local`);
  console.log(`  id ${s.id}`);
  console.log(`  ${s.title.slice(0, 66)}`);
}

try {
  const hits = one(`SELECT COUNT(*) c FROM note_fts WHERE note_fts MATCH '"tenm"*'`).c;
  console.log(`\nFTS5 prefix search   ${hits} hits — working`);
} catch (e) {
  console.log(`\nFTS5   UNAVAILABLE: ${e.message}`);
}

const t0 = process.hrtime.bigint();
db.prepare(`SELECT id FROM stream WHERE retracted_at IS NULL
            ORDER BY started_at DESC, id DESC LIMIT 30`).all();
console.log(`keyset page          ${(Number(process.hrtime.bigint() - t0) / 1e6).toFixed(2)} ms`);
console.log('\nready.');
