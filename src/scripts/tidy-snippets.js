#!/usr/bin/env node
/* scripts/tidy-snippets.js — the one-shot cleanup, run from your PC.
 *
 *   node scripts/tidy-snippets.js --root "H:\…\snippets"
 *   node scripts/tidy-snippets.js --root "…" --apply
 *   node scripts/tidy-snippets.js --root "…" --apply --keep first   (no prompts)
 *
 * Seven passes over one folder, in an order chosen so each one cannot undo the
 * last:
 *
 *   1. inventory      what is actually in here — video, stills, audio, junk
 *   2. quarantine     move anything that is not a video out of the way
 *   3. sidecars       every clip has one, every sidecar has a clip, all parse
 *   4. duplicates     byte-identical files, and you choose which name survives
 *   5. collisions     two files, one stem — rename, because one would clobber
 *   6. normalise      hands off to normalize-media.js for the format work
 *   7. manifest       write down everything that moved
 *
 * Duplicates BEFORE collisions on purpose: two files sharing a stem are very
 * often the same clip twice, and deciding that as a duplicate keeps the good
 * name. Renaming first would leave you with `x.mp4` and `x-2.webm`, both kept,
 * and the duplicate pass would then ask you a question you had already
 * answered by hand.
 *
 * ── the manifest, and why it is not optional ─────────────────────────────
 *
 * This script never touches the database — it runs over SMB, and SQLite over
 * SMB corrupts. But every delete and every rename here IS a database change
 * waiting to happen:
 *
 *   deleted   the row survives, video_path points at nothing, the clip 404s
 *   renamed   worse. `slug` is the filename stem AND the importer's key, so
 *             --update cannot see a rename. It sees one row whose file
 *             vanished and one new file, and you get two rows for one clip.
 *
 * So every change is written to _cleanup-manifest.json, and
 * `scripts/apply-cleanup.js` replays it inside the container: renames follow
 * the row, a discarded duplicate hands its transcript and taglets to the copy
 * you kept, and only then is it retracted. Run it AFTER `--update`, not
 * before — --update rebuilds lines from sidecars and would undo the merge.
 */
import { createHash } from 'node:crypto';
import { createReadStream } from 'node:fs';
import {
  existsSync, mkdirSync, readdirSync, readFileSync, renameSync, statSync, writeFileSync,
} from 'node:fs';
import { basename, extname, join } from 'node:path';
import { execFileSync, spawnSync } from 'node:child_process';
import { createInterface } from 'node:readline/promises';
import { fileURLToPath } from 'node:url';
import { dirname } from 'node:path';

const HERE = dirname(fileURLToPath(import.meta.url));
const argv = process.argv.slice(2);
const has = (f) => argv.includes(f);
const val = (name, dflt) => {
  const i = argv.findIndex((a) => a === name);
  return (i >= 0 ? argv[i + 1] : argv.find((a) => a.startsWith(`${name}=`))?.split('=')[1]) ?? dflt;
};

const TAKES_VALUE = new Set(['--root', '--keep', '--jobs', '--preset', '--crf']);
const BOOLEAN = new Set(['--apply', '--no-normalise', '--no-normalize', '--help', '-h']);
{
  const bad = [];
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (!a.startsWith('-')) continue;
    const name = a.split('=')[0];
    if (BOOLEAN.has(name)) continue;
    if (TAKES_VALUE.has(name)) { if (!a.includes('=')) i++; continue; }
    bad.push(a);
  }
  if (bad.length) {
    console.error(`unknown ${bad.length === 1 ? 'flag' : 'flags'}: ${bad.join(', ')}`);
    console.error(`known: ${[...TAKES_VALUE, ...BOOLEAN].filter((f) => f !== '-h').sort().join(' ')}`);
    process.exit(2);
  }
}

const ROOT = val('--root', null);
const APPLY = has('--apply');
const KEEP = val('--keep', null);              // first | largest | longest | shortest
const NORMALISE = !has('--no-normalise') && !has('--no-normalize');
if (!ROOT) { console.error('--root <folder of clips>'); process.exit(1); }
if (!existsSync(ROOT)) { console.error(`no such folder: ${ROOT}`); process.exit(1); }

const VIDEO = /\.(mp4|m4v|mov|webm|mkv|avi|flv|ts|wmv|mpg|mpeg|ogv)$/i;
const IMAGE = /\.(png|jpg|jpeg|gif|webp|avif|bmp|tiff?|heic)$/i;
const AUDIO = /\.(wav|mp3|m4a|flac|ogg|opus|aac|aiff?)$/i;
const NOTVIDEO = '_notvideo';
const ORIGINALS = '_originals';
const SKIP_DIRS = new Set([NOTVIDEO, ORIGINALS, '_duplicates', 'cache']);

const plural = (n, one, many = one + 's') => `${n} ${n === 1 ? one : many}`;
const mb = (b) => `${(b / 1e6).toFixed(1)} MB`;
const head = (n, s) => console.log(`\n\x1b[1m${n}. ${s}\x1b[0m`);
const sha256 = (f) => new Promise((res, rej) => {
  const h = createHash('sha256');
  createReadStream(f).on('data', (d) => h.update(d)).on('end', () => res(h.digest('hex'))).on('error', rej);
});
const probe = (f) => {
  try {
    const j = JSON.parse(execFileSync('ffprobe', ['-v', 'error', '-show_entries',
      'format=duration,size:stream=codec_type,codec_name,width,height', '-of', 'json', f],
      { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'], maxBuffer: 1 << 22 }));
    const v = (j.streams ?? []).find((s) => s.codec_type === 'video');
    return { duration: Number(j.format?.duration) || null, bytes: Number(j.format?.size) || null,
             w: v?.width ?? null, h: v?.height ?? null, codec: v?.codec_name ?? null };
  } catch { return null; }
};

/* Everything that happened, in the shape apply-cleanup.js expects. Written
   even on a dry run, so you can read what WOULD change before it does. */
const manifest = { root: ROOT, at: new Date().toISOString(), applied: APPLY,
                   renamed: [], discarded: [], quarantined: [] };
const moves = [];      // [from, to] queued, executed only under --apply
const queue = (from, to) => { moves.push([from, to]); };
const flush = () => {
  if (!APPLY) return;
  for (const [from, to] of moves.splice(0)) {
    mkdirSync(dirname(to), { recursive: true });
    renameSync(from, to);
  }
};

// ── 1. inventory ──────────────────────────────────────────────────────────
head(1, 'inventory');
const all = readdirSync(ROOT, { withFileTypes: true })
  .filter((d) => d.isFile()).map((d) => d.name);
const videos = all.filter((f) => VIDEO.test(f));
const sidecars = all.filter((f) => /\.json$/i.test(f));
const images = all.filter((f) => IMAGE.test(f));
const audios = all.filter((f) => AUDIO.test(f));
const other = all.filter((f) => !VIDEO.test(f) && !IMAGE.test(f) && !AUDIO.test(f) && !/\.json$/i.test(f));
console.log(`  ${String(videos.length).padStart(5)}  video`);
console.log(`  ${String(sidecars.length).padStart(5)}  sidecar`);
for (const [n, label] of [[images.length, 'image'], [audios.length, 'audio'], [other.length, 'other']]) {
  if (n) console.log(`  ${String(n).padStart(5)}  ${label}  \x1b[33m<- not a clip\x1b[0m`);
}
if (!videos.length) { console.log('\nnothing to do.'); process.exit(0); }

// ── 2. quarantine ─────────────────────────────────────────────────────────
head(2, 'quarantine');
const strays = [...images, ...audios, ...other];
if (!strays.length) console.log('  nothing here but clips and sidecars');
else {
  console.log(`  ${plural(strays.length, 'file')} moved to ${NOTVIDEO}/ — nothing is deleted`);
  for (const f of strays.slice(0, 12)) console.log(`         ${f}`);
  if (strays.length > 12) console.log(`         …and ${strays.length - 12} more`);
  for (const f of strays) {
    queue(join(ROOT, f), join(ROOT, NOTVIDEO, f));
    manifest.quarantined.push(f);
  }
}

// ── 3. sidecars ───────────────────────────────────────────────────────────
head(3, 'sidecars');
const stemsOf = (list) => new Set(list.map((f) => basename(f, extname(f))));
const vStems = stemsOf(videos), sStems = stemsOf(sidecars);
const missing = [...vStems].filter((s) => !sStems.has(s)).sort();
const orphaned = [...sStems].filter((s) => !vStems.has(s)).sort();
const broken = [];
for (const f of sidecars) {
  try { JSON.parse(readFileSync(join(ROOT, f), 'utf8')); }
  catch (e) { broken.push([f, e.message.split('\n')[0]]); }
}
if (missing.length) {
  console.log(`  ${plural(missing.length, 'clip')} with no sidecar`);
  console.log(`         these still import — title from the filename, no tags, no transcript`);
  for (const s of missing.slice(0, 15)) console.log(`         ${s}`);
  if (missing.length > 15) console.log(`         …and ${missing.length - 15} more`);
}
if (orphaned.length) {
  console.log(`  ${plural(orphaned.length, 'sidecar')} with no clip`);
  for (const s of orphaned.slice(0, 10)) console.log(`         ${s}.json`);
}
if (broken.length) {
  console.log(`  \x1b[31m${plural(broken.length, 'sidecar')} that will not parse\x1b[0m — the importer skips these`);
  for (const [f, e] of broken) console.log(`         ${f}: ${e}`);
}
if (!missing.length && !orphaned.length && !broken.length) console.log('  every clip has one, every one parses');

// ── 4. duplicates ─────────────────────────────────────────────────────────
head(4, 'duplicates');
/* Cheap screen first: identical files agree on byte count before anything
   else, so only collide-on-size candidates are ever hashed. */
const bySize = new Map();
for (const f of videos) {
  let s; try { s = statSync(join(ROOT, f)).size; } catch { continue; }
  if (!bySize.has(s)) bySize.set(s, []);
  bySize.get(s).push(f);
}
const candidates = [...bySize.values()].filter((g) => g.length > 1).flat();
const hashes = new Map();
if (candidates.length) {
  process.stdout.write(`  hashing ${plural(candidates.length, 'candidate')}…`);
  for (const f of candidates) {
    try { hashes.set(f, await sha256(join(ROOT, f))); } catch { /* unreadable */ }
  }
  process.stdout.write('\r'.padEnd(50) + '\r');
}
const byHash = new Map();
for (const [f, h] of hashes) {
  if (!byHash.has(h)) byHash.set(h, []);
  byHash.get(h).push(f);
}
const dupeSets = [...byHash.values()].filter((g) => g.length > 1);
const discarded = new Set();

const pick = (group) => {
  // Non-interactive rules, for a scripted run or a very long list.
  if (KEEP === 'first') return group[0];
  if (KEEP === 'largest') return group.slice().sort((a, b) =>
    statSync(join(ROOT, b)).size - statSync(join(ROOT, a)).size)[0];
  if (KEEP === 'longest') return group.slice().sort((a, b) => b.length - a.length)[0];
  if (KEEP === 'shortest') return group.slice().sort((a, b) => a.length - b.length)[0];
  return null;
};

if (!dupeSets.length) console.log('  no byte-identical clips');
else {
  console.log(`  ${plural(dupeSets.length, 'set')} of byte-identical clips\n`);
  const rl = (!KEEP && APPLY && process.stdin.isTTY)
    ? createInterface({ input: process.stdin, output: process.stdout }) : null;
  let auto = KEEP;
  for (const [n, group] of dupeSets.entries()) {
    const info = group.map((f) => {
      const m = probe(join(ROOT, f));
      const side = existsSync(join(ROOT, `${basename(f, extname(f))}.json`));
      return { f, m, side };
    });
    console.log(`  set ${n + 1} of ${dupeSets.length}`);
    for (const [i, x] of info.entries()) {
      console.log(`    ${i + 1}) ${x.f}`);
      console.log(`       ${x.m ? `${x.m.w}x${x.m.h}, ${x.m.duration?.toFixed(1)}s, ${mb(x.m.bytes)}` : 'unreadable'}`
        + `, ${x.side ? 'has a sidecar' : '\x1b[33mno sidecar\x1b[0m'}`);
    }
    let keep = auto ? pick(group) : null;
    if (!keep && rl) {
      const a = (await rl.question(`    keep which? [1-${group.length}, s=skip, a=all-first] `)).trim().toLowerCase();
      if (a === 's') { console.log(''); continue; }
      if (a === 'a') { auto = 'first'; keep = group[0]; }
      else {
        const k = Number(a);
        if (!(k >= 1 && k <= group.length)) { console.log('    not a choice — skipped\n'); continue; }
        keep = group[k - 1];
      }
    }
    if (!keep) { console.log(`    \x1b[2m(dry run — rerun with --apply to choose)\x1b[0m\n`); continue; }
    for (const f of group) {
      if (f === keep) continue;
      discarded.add(f);
      queue(join(ROOT, f), join(ROOT, '_duplicates', f));
      const side = `${basename(f, extname(f))}.json`;
      if (existsSync(join(ROOT, side))) queue(join(ROOT, side), join(ROOT, '_duplicates', side));
      manifest.discarded.push({ slug: basename(f, extname(f)), keptSlug: basename(keep, extname(keep)) });
    }
    console.log(`    keeping ${keep}\n`);
  }
  rl?.close();
}

// ── 5. collisions ─────────────────────────────────────────────────────────
head(5, 'name collisions');
/* Only what survived the duplicate pass. Two files with one stem and DIFFERENT
   content are both worth keeping, but the normalise step would turn both into
   `<stem>.mp4` and one would silently eat the other. */
const live = videos.filter((f) => !discarded.has(f));
const byStem = new Map();
for (const f of live) {
  const s = basename(f, extname(f));
  if (!byStem.has(s)) byStem.set(s, []);
  byStem.get(s).push(f);
}
const collided = [...byStem].filter(([, v]) => v.length > 1);
if (!collided.length) console.log('  every remaining clip has a unique name');
else {
  console.log(`  ${plural(collided.length, 'stem')} used by more than one file`);
  for (const [stem, group] of collided) {
    // Keep the first alphabetically under the original name; suffix the rest.
    const sorted = group.slice().sort();
    for (const [i, f] of sorted.entries()) {
      if (i === 0) { console.log(`    ${f}  \x1b[2m(keeps the name)\x1b[0m`); continue; }
      let suffix = i + 1, to;
      do { to = `${stem}-${suffix++}${extname(f)}`; } while (existsSync(join(ROOT, to)));
      console.log(`    ${f}  ->  ${to}`);
      queue(join(ROOT, f), join(ROOT, to));
      /* The FILE names matter here, not just the stems. Both files share a
         stem, so at most one of them is the one the database actually holds —
         the importer keys on stem and skipped the other as "already present".
         Recording only `clash -> clash-2` would tell apply-cleanup to rename a
         row that belongs to the file which KEPT its name. It checks
         video_path against fromFile to tell them apart. */
      const side = `${stem}.json`;
      // A sidecar can only belong to one of them; the renamed copy gets a copy
      // of it so its metadata is not silently lost on import.
      if (existsSync(join(ROOT, side)) && i > 0) {
        const sideTo = `${basename(to, extname(to))}.json`;
        if (APPLY && !existsSync(join(ROOT, sideTo))) {
          writeFileSync(join(ROOT, sideTo), readFileSync(join(ROOT, side)));
        }
      }
      manifest.renamed.push({ from: stem, to: basename(to, extname(to)),
                              fromFile: f, toFile: to });
    }
  }
}

// ── act ───────────────────────────────────────────────────────────────────
if (!APPLY) {
  console.log(`\n\x1b[1mNothing written.\x1b[0m Rerun with \x1b[1m--apply\x1b[0m to move files`
    + ` and choose between duplicates.`);
  process.exit(0);
}
flush();

// ── 6. normalise ──────────────────────────────────────────────────────────
if (NORMALISE) {
  head(6, 'format');
  const args = [join(HERE, 'normalize-media.js'), '--root', ROOT, '--apply'];
  for (const f of ['--jobs', '--preset', '--crf']) {
    const v = val(f, null); if (v) args.push(f, String(v));
  }
  const r = spawnSync(process.execPath, args, { stdio: 'inherit' });
  if (r.status !== 0) {
    console.error('\nnormalize-media exited non-zero — the manifest below is still correct');
  }
  /* Conversions rename `.webm` to `.mp4`, which is a stem-preserving change,
     so the slug is unaffected and the row follows on --update. Nothing for the
     manifest to record. */
}

// ── 7. manifest ───────────────────────────────────────────────────────────
head(7, 'manifest');
const out = join(ROOT, '_cleanup-manifest.json');
writeFileSync(out, JSON.stringify(manifest, null, 2));
console.log(`  ${out}`);
console.log(`  ${plural(manifest.renamed.length, 'rename')}, ${plural(manifest.discarded.length, 'discard')},`
  + ` ${plural(manifest.quarantined.length, 'file')} quarantined`);

console.log(`\n\x1b[1mNow reconcile the database\x1b[0m — inside the container, where it lives:`);
console.log(`  docker exec -it ragekitsu node scripts/import-snippets.js --update`);
console.log(`  docker exec -it ragekitsu node scripts/apply-cleanup.js --apply \\`);
console.log(`      --manifest /media/snippets/_cleanup-manifest.json`);
console.log(`  docker exec -it ragekitsu node scripts/check-media.js --deep`);
console.log(`\nThat order matters. --update rebuilds a clip's lines from its sidecar, so`);
console.log(`running it AFTER apply-cleanup would delete the transcript just merged onto`);
console.log(`a survivor. apply-cleanup goes last and its work stays done.`);
