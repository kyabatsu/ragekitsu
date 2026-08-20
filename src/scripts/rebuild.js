// Rebuild the materialised timeline on every stream.
//
//   node scripts/rebuild.js [--db data/archive.db] [--states] [--root PATH]
//
// The timeline is projected by recompute() on every write that touches a
// stream, so this is only needed once after a migration and any time
// check-db.js reports drift. Nothing here is a decision: every value it writes
// is derived from rows that already exist, so running it can only ever restore
// agreement, never lose anything.
//
// By default it rebuilds ONLY the timeline. `--states` runs the full
// recompute, which also re-derives vod_state and chat_state — and without a
// media root that turns every unverifiable capture from 'lost' into
// 'unverified', which is honest but is a change you should ask for rather than
// trip over.

import { create, isArchive, now, resolveDbPath } from '../db.js';
import { buildTimeline, recompute } from '../archive.js';

const argv = process.argv.slice(2);
const flag = (n, d = null) => { const i = argv.indexOf(`--${n}`); return i === -1 ? d : argv[i + 1]; };
const DB = flag('db', process.env.TENMA_DB ?? 'data/archive.db');
const ROOT = flag('root', process.env.TENMA_MEDIA_ROOT ?? null);
const STATES = argv.includes('--states');

// create() would apply schema.sql to a mistyped path and hand back a brand-new
// empty archive, which then rebuilds zero streams and reports success.
if (!isArchive(DB)) {
  console.error(`no archive at ${resolveDbPath(DB)} — refusing to create one`);
  process.exit(1);
}

const db = create(DB);
console.log(`db ${resolveDbPath(DB)}`);
console.log(STATES ? `mode full recompute (media root: ${ROOT ?? 'unset'})`
                   : 'mode timeline only');

const ids = db.prepare('SELECT id FROM stream').all().map((r) => r.id);
const t0 = process.hrtime.bigint();
let n = 0;

for (const id of ids) {
  if (STATES) {
    recompute(db, id, { mediaRoot: ROOT, checkFiles: !!ROOT });
  } else {
    const tl = buildTimeline(db, id);
    db.prepare('UPDATE stream SET timeline_json=?, timeline_at=? WHERE id=?')
      .run(JSON.stringify(tl), now(), id);
  }
  if (++n % 50 === 0) process.stdout.write(`  ${n}/${ids.length}\r`);
}

const ms = Number(process.hrtime.bigint() - t0) / 1e6;
console.log(`\nrebuilt ${n} stream(s) in ${ms.toFixed(0)} ms`);
db.close();
