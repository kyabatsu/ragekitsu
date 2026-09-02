#!/usr/bin/env node
/* scripts/apply-cleanup.js — replay a tidy-snippets manifest against the DB.
 *
 *   node scripts/apply-cleanup.js --manifest /media/snippets/_cleanup-manifest.json
 *   node scripts/apply-cleanup.js --manifest … --apply
 *   node scripts/apply-cleanup.js --orphans --apply      (no manifest needed)
 *
 * ── why this exists ──────────────────────────────────────────────────────
 *
 * tidy-snippets runs on your PC and never touches the database, because it
 * reaches the archive over SMB and SQLite over SMB corrupts. So it writes down
 * what it did, and this replays it here, where the database actually lives.
 *
 * Run it AFTER `import-snippets --update`, and the order is not arbitrary.
 * --update rebuilds a row's lines from its sidecar, so running it afterwards
 * would delete the transcript this script had just merged onto a survivor and
 * rebuild it from a sidecar that has none. The merge would be undone by the
 * next routine command, silently. Ask me how I know.
 *
 *   1. tidy-snippets      (PC)         files move, manifest written
 *   2. import-snippets --update        paths, codecs, and any new files
 *   3. apply-cleanup --apply           merge, retract, and it stays merged
 *   4. check-media --deep              certify
 *
 * Three jobs:
 *
 *   renamed    move the row to the new slug, so --update matches it
 *   discarded  give the survivor anything the discarded copy had that it
 *              lacks — transcript, lines, taglets — and only then retract it
 *   orphans    retract rows whose file is no longer on disk at all
 *
 * Retract, never DELETE. This archive tombstones everything for the reason
 * `stream.retracted_at` was added: someone may have been wrong, and a deleted
 * row cannot be argued with. A retracted snippet is invisible everywhere
 * (`retracted_at IS NULL` guards every read) and can be brought back with one
 * UPDATE.
 */
import { existsSync, readFileSync } from 'node:fs';
import { basename, join } from 'node:path';
import { open, now, tx } from '../db.js';

const MEDIA = process.env.TENMA_MEDIA_ROOT;
const DB = process.env.TENMA_DB ?? 'data/archive.db';
const argv = process.argv.slice(2);
const has = (f) => argv.includes(f);
const val = (n, d) => {
  const i = argv.findIndex((a) => a === n);
  return (i >= 0 ? argv[i + 1] : argv.find((a) => a.startsWith(`${n}=`))?.split('=')[1]) ?? d;
};

const MANIFEST = val('--manifest', null);
const APPLY = has('--apply');
const ORPHANS = has('--orphans') || !!MANIFEST;
if (!MANIFEST && !has('--orphans')) {
  console.error('--manifest <path>   (or --orphans to only retract rows whose file is gone)');
  process.exit(1);
}

const db = open(DB);
const t = now();
const plural = (n, one, many = one + 's') => `${n} ${n === 1 ? one : many}`;
const head = (s) => console.log(`\n\x1b[1m${s}\x1b[0m`);

const man = MANIFEST
  ? JSON.parse(readFileSync(MANIFEST, 'utf8'))
  : { renamed: [], discarded: [], quarantined: [] };
if (MANIFEST) {
  console.log(`manifest written ${man.at} against ${man.root}`);
  if (man.applied === false) {
    console.error('\nthat manifest is from a DRY RUN — nothing was moved on disk.');
    console.error('rerun tidy-snippets with --apply first.');
    process.exit(1);
  }
}

const bySlug = db.prepare(
  'SELECT id, slug, video_path, transcript, transcript_status FROM snippet WHERE slug = ?');
let renamed = 0, merged = 0, retracted = 0, skipped = 0;

// ── renames ───────────────────────────────────────────────────────────────
head(`renames (${man.renamed.length})`);
for (const { from, to, fromFile } of man.renamed) {
  const row = bySlug.get(from);
  if (!row) { console.log(`  \x1b[2mskip\x1b[0m  ${from} — no such row`); skipped++; continue; }

  /* Only if the row is actually the file that moved.
     A collision means two files shared one stem, so the importer imported one
     and skipped the other as "already present" — and the surviving row may
     well belong to the copy that KEPT its name. Renaming that row would point
     a perfectly good clip at a filename it does not have, and the moved file
     would then import as a second row anyway. Compare video_path, not stems. */
  if (fromFile && row.video_path && basename(row.video_path) !== fromFile) {
    console.log(`  \x1b[2mkeep\x1b[0m  ${from} — its row holds ${basename(row.video_path)},`
      + ` not the file that moved`);
    console.log(`         ${fromFile} was never imported; --update will pick it up as ${to}`);
    skipped++;
    continue;
  }
  if (bySlug.get(to)) { console.log(`  \x1b[33mskip\x1b[0m  ${to} already exists — resolve by hand`); skipped++; continue; }
  console.log(`  ${from}  ->  ${to}`);
  if (APPLY) {
    tx(db, () => {
      // video_path is repaired by import-snippets --update afterwards; the
      // slug is the part only this script can know about.
      db.prepare('UPDATE snippet SET slug = ?, updated_at = ? WHERE id = ?').run(to, t, row.id);
    });
  }
  renamed++;
}
if (!man.renamed.length) console.log('  none');

// ── discards ──────────────────────────────────────────────────────────────
head(`discarded duplicates (${man.discarded.length})`);
for (const { slug, keptSlug } of man.discarded) {
  const loser = bySlug.get(slug);
  const winner = bySlug.get(keptSlug);
  if (!loser) { console.log(`  \x1b[2mskip\x1b[0m  ${slug} — no such row`); skipped++; continue; }
  if (!winner) { console.log(`  \x1b[33mskip\x1b[0m  ${slug} — kept copy "${keptSlug}" has no row`); skipped++; continue; }

  /* Metadata moves BEFORE the tombstone. A duplicate is the same clip filed
     twice, so the work done on either copy is work done on the clip — and
     retracting the one that happens to carry the transcript would throw away
     someone's afternoon for a filename preference. */
  const wLines = db.prepare('SELECT count(*) c FROM snippet_line WHERE snippet_id = ?').get(winner.id).c;
  const lLines = db.prepare('SELECT count(*) c FROM snippet_line WHERE snippet_id = ?').get(loser.id).c;
  const gained = [];
  if (APPLY) {
    tx(db, () => {
      if (!wLines && lLines) {
        db.prepare('UPDATE snippet_line SET snippet_id = ? WHERE snippet_id = ?').run(winner.id, loser.id);
        /* Marked 'edited', even though the words came out of whisper.
           That flag means "a person decided this, do not overwrite it", and
           the importer honours it. Without it the next `--update` deletes
           these lines and rebuilds them from the survivor's own sidecar, which
           has none — the merge would be undone by the next routine command,
           silently, and this is exactly how it behaved before I caught it. */
        db.prepare(`UPDATE snippet SET transcript = ?, transcript_status = 'edited', updated_at = ?
                     WHERE id = ?`).run(loser.transcript, t, winner.id);
        gained.push(`${lLines}-line transcript`);
      }
      // Taglets are a union: INSERT OR IGNORE leans on the UNIQUE(snippet_id,
      // taglet_id) constraint, so a taglet both copies carry is not doubled.
      const moved = db.prepare(
        `INSERT OR IGNORE INTO snippet_taglet(id, snippet_id, taglet_id, created_at, updated_at)
         SELECT lower(hex(randomblob(16))), ?, taglet_id, ?, ? FROM snippet_taglet WHERE snippet_id = ?`)
        .run(winner.id, t, t, loser.id).changes;
      if (moved) gained.push(plural(moved, 'taglet'));
      // A summary the survivor never got round to.
      db.prepare(`UPDATE snippet SET summary = COALESCE(summary,
                    (SELECT summary FROM snippet WHERE id = ?)), updated_at = ?
                  WHERE id = ? AND summary IS NULL`).run(loser.id, t, winner.id);
      db.prepare(`UPDATE snippet SET retracted_at = ?, updated_at = ? WHERE id = ?`)
        .run(t, t, loser.id);
    });
  } else if (!wLines && lLines) gained.push(`${lLines}-line transcript`);
  console.log(`  ${slug}  ->  retracted, kept ${keptSlug}`
    + (gained.length ? `  \x1b[32m(+${gained.join(', +')})\x1b[0m` : ''));
  if (gained.length) merged++;
  retracted++;
}
if (!man.discarded.length) console.log('  none');

// ── orphans ───────────────────────────────────────────────────────────────
let orphans = [];
if (ORPHANS) {
  head('rows whose file is gone');
  if (!MEDIA) console.log('  set TENMA_MEDIA_ROOT to check this');
  else {
    const live = db.prepare(
      'SELECT id, slug, video_path FROM snippet WHERE retracted_at IS NULL').all();
    orphans = live.filter((r) => !existsSync(join(MEDIA, r.video_path)));
    if (!orphans.length) console.log('  none — every row points at a file');
    else {
      /* A guard, not a formality. Point this at the wrong media root, or run it
         while the mount is down, and every row looks orphaned — so a result
         that sweeping is treated as a mistake rather than obeyed. */
      const share = orphans.length / Math.max(1, live.length);
      console.log(`  ${plural(orphans.length, 'row')} of ${live.length}`);
      for (const r of orphans.slice(0, 15)) console.log(`         ${r.slug}  ->  ${r.video_path}`);
      if (orphans.length > 15) console.log(`         …and ${orphans.length - 15} more`);
      if (share > 0.25) {
        console.log(`\n  \x1b[31mrefusing: that is ${(share * 100).toFixed(0)}% of the archive.\x1b[0m`);
        console.log(`  Check TENMA_MEDIA_ROOT (currently ${MEDIA}) and that the mount is up.`);
        orphans = [];
      } else if (APPLY) {
        for (const r of orphans) {
          db.prepare('UPDATE snippet SET retracted_at = ?, updated_at = ? WHERE id = ?').run(t, t, r.id);
        }
        retracted += orphans.length;
      }
    }
  }
}

if (APPLY && (renamed || retracted)) {
  db.prepare(`INSERT INTO meta(key,value) VALUES('generation','1')
              ON CONFLICT(key) DO UPDATE SET value = CAST(CAST(value AS INTEGER)+1 AS TEXT)`).run();
}

console.log('');
if (!APPLY) {
  console.log(`\x1b[1mNothing written.\x1b[0m Rerun with --apply.`);
} else {
  console.log(`${plural(renamed, 'row')} renamed, ${plural(retracted, 'row')} retracted`
    + (merged ? `, ${plural(merged, 'survivor')} gained metadata` : '')
    + (skipped ? `, ${skipped} skipped` : ''));
  console.log(`\nRetracted rows are hidden, not deleted. To bring one back:`);
  console.log(`  UPDATE snippet SET retracted_at = NULL WHERE slug = '…';`);
  console.log(`\nIf you have not run it yet: node scripts/import-snippets.js --update`);
  console.log(`(and if you run --update again later, the merge survives — the`);
  console.log(` transcript is flagged 'edited' and taglets are unioned, not replaced.)`);
}
db.close();
process.exit(0);
