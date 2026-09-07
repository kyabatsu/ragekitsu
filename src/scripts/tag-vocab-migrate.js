// Move `tag.kind` and `segment.kind` onto the merged vocabulary.
// Usage: node scripts/tag-vocab-migrate.js [path] [--apply]
//
// Prints what it would do and changes nothing unless you pass --apply.
//
//     game   -> media       what it belongs to
//     person -> character   who it is about
//     meta   -> type        for the formats: Collab, Zatsudan, Watchalong…
//     meta   -> elements    for the scaffolding: Intro, Break, Outro
//
// A SCRIPT and deliberately not a `db.js` backfill. Backfills run on every boot
// with no ran-once guard (issues.md #13), so anything they touch is re-applied
// forever — and re-filing a tag is precisely the kind of decision a person is
// then allowed to change. This runs once, by hand, like every other one-shot in
// this directory.
//
// The `meta` split is the only part that is not mechanical. `meta` held two
// disjoint populations and the difference is not in the row: Collab and
// Zatsudan are formats a whole broadcast can be, Intro and Break and Outro only
// ever label a stretch of one. So the split is decided by NAME, listed below,
// and anything unrecognised stops the run rather than being guessed at.
import { isArchive, open, resolveDbPath } from '../db.js';

const argv = process.argv.slice(2);
const apply = argv.includes('--apply');
const i = argv.indexOf('--db');
const path = (i !== -1 ? argv[i + 1] : argv.find((a) => !a.startsWith('--')))
  || 'data/archive.db';
const full = resolveDbPath(path);

if (!isArchive(path)) {
  console.error(`no archive at ${full}`);
  console.error('  node scripts/tag-vocab-migrate.js path/to/archive.db [--apply]');
  process.exit(1);
}

/* The scaffolding, by slug. Everything else in `meta` is a format and becomes
   `type`. Listed rather than pattern-matched: there are three of them, a
   pattern would quietly capture the fourth thing somebody adds, and the whole
   point of this kind is that grey means skippable. */
const ELEMENTS = new Set(['intro', 'outro', 'break', 'waiting', 'waiting-screen',
                          'brb', 'ending', 'starting-soon']);

const SIMPLE = { game: 'media', person: 'character' };

console.log(`${apply ? 'MIGRATING' : 'DRY RUN on'} ${full}\n`);
const db = open(path);
const all = (sql, ...a) => db.prepare(sql).all(...a);

// ── work out the plan ─────────────────────────────────────────────────────
const tags = all(`SELECT id, name, slug, kind FROM tag WHERE kind IN ('game','person','meta')`);
const plan = new Map();          // tag id -> new kind
const buckets = { media: [], character: [], type: [], elements: [] };

for (const t of tags) {
  const to = SIMPLE[t.kind] ?? (ELEMENTS.has(t.slug) ? 'elements' : 'type');
  plan.set(t.id, to);
  buckets[to].push(t);
}

for (const [k, rows] of Object.entries(buckets)) {
  if (!rows.length) continue;
  console.log(`  -> ${k}  (${rows.length})`);
  // Only the two hand-decided buckets are worth reading name by name; media and
  // character are a rename of every row and listing 95 games proves nothing.
  if (k === 'type' || k === 'elements') {
    for (const r of rows) console.log(`       ${r.name}  [${r.slug}]  was ${r.kind}`);
  }
}

/* A tag left in a kind this script does not know about is a stop, not a
   warning. Half a vocabulary is worse than none: the strip would draw some
   blocks in colours the legend no longer lists.
   The four TARGET kinds are named here too, so running this twice reports
   "already done" instead of refusing to recognise its own output. */
const KNOWN = ['game', 'person', 'meta', 'type', 'unknown',
               'media', 'character', 'elements'];
const strays = all(
  `SELECT kind, COUNT(*) c FROM tag
    WHERE kind NOT IN (${KNOWN.map(() => '?').join(',')}) GROUP BY kind`, ...KNOWN);
if (strays.length) {
  console.error('\n  STOP — tags in kinds this script has no target for:');
  for (const r of strays) console.error(`    ${r.kind}  ×${r.c}`);
  console.error('  Add a target above, or re-file them first.');
  process.exit(1);
}

// `type` survives its own name, so those rows need no update — but they are
// worth counting so the totals add up on screen.
const keptType = all(`SELECT COUNT(*) c FROM tag WHERE kind = 'type'`)[0].c;
console.log(`\n  already type, untouched: ${keptType}`);

// ── segments ──────────────────────────────────────────────────────────────
/* `kind` is COPIED onto a segment when a human picks a tag, deliberately, so
   that re-filing a tag does not repaint forty old streams. Which means the
   segments have to be migrated HERE, in the same breath — after this runs,
   re-filing a tag stops moving them again, exactly as intended. Segments are
   matched through their own tag_id where they have one, and by their stored
   kind where they do not. */
const segRows = all(
  `SELECT s.id, s.kind, s.tag_id, t.slug, t.name
     FROM segment s LEFT JOIN tag t ON t.id = s.tag_id
    WHERE s.kind IN ('game','person','meta')`);
const segPlan = new Map();
const segCount = {};
for (const s of segRows) {
  const to = s.tag_id && plan.has(s.tag_id)
    ? plan.get(s.tag_id)
    : (SIMPLE[s.kind] ?? (s.slug && ELEMENTS.has(s.slug) ? 'elements' : 'type'));
  segPlan.set(s.id, to);
  segCount[to] = (segCount[to] ?? 0) + 1;
}
console.log('\n  segments');
for (const [k, c] of Object.entries(segCount)) console.log(`    -> ${k}  ${c}`);
const orphanSegs = segRows.filter((s) => !s.tag_id).length;
if (orphanSegs) {
  console.log(`    (${orphanSegs} carry no tag_id; migrated on their stored kind alone,`);
  console.log('     which sends an untagged old `meta` block to `type` rather than to');
  console.log('     `elements`. Check those on the strip afterwards.)');
}

if (!plan.size && !segPlan.size) {
  console.log('\n  Nothing on the old vocabulary. This database is already migrated.');
  process.exit(0);
}

if (!apply) {
  console.log('\nnothing written. Re-run with --apply.');
  process.exit(0);
}

// ── do it ─────────────────────────────────────────────────────────────────
const setTag = db.prepare('UPDATE tag SET kind = ?, updated_at = ? WHERE id = ?');
const setSeg = db.prepare('UPDATE segment SET kind = ?, updated_at = ? WHERE id = ?');
const t = Math.floor(Date.now() / 1000);
db.exec('BEGIN');
try {
  for (const [id, kind] of plan) setTag.run(kind, t, id);
  for (const [id, kind] of segPlan) setSeg.run(kind, t, id);
  /* The generation counter is what every ETag on this archive is built from.
     Without a bump, `/api/streams/:id` keeps serving the old colours for up to
     fifteen seconds and `stale-while-revalidate` keeps serving them for two
     minutes after that — to a person who just watched the migration run. */
  db.prepare(`UPDATE meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT)
               WHERE key = 'generation'`).run();
  db.exec('COMMIT');
} catch (e) {
  db.exec('ROLLBACK');
  console.error(`\nrolled back: ${e.message}`);
  process.exit(1);
}
console.log(`\n  ${plan.size} tag(s) and ${segPlan.size} segment(s) re-filed. Generation bumped.`);
console.log('  Restart the server so the ETag cache is not serving the old vocabulary.');
