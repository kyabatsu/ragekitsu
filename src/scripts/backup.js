// One consistent copy of the database, safe to take while the server is running.
//
//   node scripts/backup.js                      -> data/archive-backup.db
//   node scripts/backup.js /path/to/out.db
//
// VACUUM INTO and not `copy the file`: the database runs in WAL mode, so the
// most recent writes live in archive.db-wal and a copy of archive.db alone is a
// torn snapshot missing them. This folds the WAL in and writes a single file
// with no -wal or -shm beside it — nothing to forget when moving it.
//
// It is also the whole migration procedure. The database is ~1 MB and holds
// every note, chapter, tag and hand-made correction in the archive; it is the
// half that cannot be regenerated from the media.

import { DatabaseSync } from 'node:sqlite';
import { existsSync, statSync, unlinkSync } from 'node:fs';
import { resolve } from 'node:path';
import { resolveDbPath } from '../db.js';

const src = resolveDbPath(process.env.TENMA_DB ?? 'data/archive.db');
const out = resolve(process.argv[2] ?? 'data/archive-backup.db');

if (!existsSync(src)) {
  console.error(`no database at ${src}`);
  process.exit(1);
}
if (src === out) {
  console.error('refusing to vacuum a database onto itself');
  process.exit(1);
}
// VACUUM INTO refuses to overwrite, which is right for a one-shot but wrong for
// a nightly. Clearing it here keeps the failure ("could not write") about the
// thing that actually went wrong.
if (existsSync(out)) unlinkSync(out);

const db = new DatabaseSync(src, { readOnly: true });
try {
  db.exec(`VACUUM INTO '${out.replace(/'/g, "''")}'`);
} finally {
  db.close();
}

const check = new DatabaseSync(out, { readOnly: true });
const n = (t) => check.prepare(`SELECT count(*) c FROM ${t}`).get().c;
const counts = { streams: n('stream'), captures: n('capture'), notes: n('note'),
                 segments: n('segment'), tags: n('tag') };
check.close();

console.log(out);
console.log(`  ${(statSync(out).size / 1024 / 1024).toFixed(2)} MB  ` +
  Object.entries(counts).map(([k, v]) => `${v} ${k}`).join(', '));
