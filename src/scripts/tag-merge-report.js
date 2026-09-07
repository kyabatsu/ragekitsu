// What merging `taglet` into `tag` would actually do to THIS database.
// Usage: node scripts/tag-merge-report.js [path]
//
// Reads. Never writes, never migrates — it is the thing you run BEFORE the
// one-shot script, because the merge has exactly one class of decision a script
// cannot make for you, and this is what surfaces it.
//
// The decided shape:
//
//     media      shared        (game + copyright, purple)
//     character  shared        (was person, blue)
//     type       VOD only      (absorbs the old VOD `meta`)
//     meta       snippet only  (the VOD one is gone, so no collision)
//     general    snippet only
//
// Scope comes from `kind` rather than a column, so a slug that exists on BOTH
// sides has to land under a shared kind or one surface silently loses its tag —
// the junction rows survive, the tag just stops rendering there. Those are the
// rows this prints under "needs a decision"; everything else is mechanical.
import { isArchive, meta, open, resolveDbPath } from '../db.js';

const argv = process.argv.slice(2);
const i = argv.indexOf('--db');
const path = (i !== -1 ? argv[i + 1] : argv.find((a) => !a.startsWith('--')))
  || 'data/archive.db';
const full = resolveDbPath(path);

if (!isArchive(path)) {
  console.error(`no archive at ${full}`);
  console.error('  Run this from the project root, or pass the path:');
  console.error('    node scripts/tag-merge-report.js path/to/archive.db');
  process.exit(1);
}
console.log(`reading ${full}   (read-only: this script writes nothing)\n`);
const db = open(path);

const all = (sql, ...a) => db.prepare(sql).all(...a);
const one = (sql, ...a) => db.prepare(sql).get(...a);

/* Where each existing kind lands, and what scope that gives it. `unknown` is
   deliberately shared: it is the sentinel a tag sits in before anyone has filed
   it, and scoping it to one surface would hide half of the unfiled ones from
   whoever is doing the filing. */
const TAG_MAP = { game: 'media', person: 'character', type: 'type',
                  meta: 'type', unknown: 'unknown' };
const TAGLET_MAP = { copyright: 'media', character: 'character',
                     meta: 'meta', general: 'general' };
const SCOPE = { media: 'shared', character: 'shared', unknown: 'shared',
                type: 'vod', meta: 'snippet', general: 'snippet' };

const pad = (s, n) => String(s ?? '').padEnd(n);
const rpad = (s, n) => String(s ?? '').padStart(n);
const head = (s) => console.log(`\n${s}\n${'─'.repeat(s.length)}`);

// ── what is there now ─────────────────────────────────────────────────────
head('today');
const tagKinds = all(
  `SELECT kind, COUNT(*) c FROM tag WHERE retracted_at IS NULL GROUP BY kind ORDER BY c DESC`);
const tlKinds = all(
  `SELECT kind, COUNT(*) c FROM taglet WHERE retracted_at IS NULL GROUP BY kind ORDER BY c DESC`);
console.log('  tag');
for (const r of tagKinds) {
  console.log(`    ${pad(r.kind, 12)}${rpad(r.c, 5)}  ->  ${pad(TAG_MAP[r.kind] ?? '??', 10)}`
    + `${SCOPE[TAG_MAP[r.kind]] ?? '?'}`);
}
console.log('  taglet');
for (const r of tlKinds) {
  console.log(`    ${pad(r.kind, 12)}${rpad(r.c, 5)}  ->  ${pad(TAGLET_MAP[r.kind] ?? '??', 10)}`
    + `${SCOPE[TAGLET_MAP[r.kind]] ?? '?'}`);
}
const unmapped = [...tagKinds.filter((r) => !TAG_MAP[r.kind]).map((r) => `tag.${r.kind}`),
                  ...tlKinds.filter((r) => !TAGLET_MAP[r.kind]).map((r) => `taglet.${r.kind}`)];
if (unmapped.length) {
  console.log(`\n  !! kinds with nowhere to go: ${unmapped.join(', ')}`);
  console.log('     Every one of these needs a target before anything runs.');
}

// ── the decision the script cannot make ───────────────────────────────────
head('slugs on both sides');
const both = all(
  `SELECT t.slug, t.id AS tag_id, t.kind AS tag_kind, t.name AS tag_name,
          l.id AS taglet_id, l.kind AS taglet_kind, l.name AS taglet_name,
          (SELECT COUNT(*) FROM stream_tag st WHERE st.tag_id = t.id) AS streams,
          (SELECT COUNT(*) FROM snippet_taglet sl WHERE sl.taglet_id = l.id) AS snippets
     FROM tag t JOIN taglet l ON l.slug = t.slug
    WHERE t.retracted_at IS NULL AND l.retracted_at IS NULL
    ORDER BY t.slug`);

if (!both.length) {
  console.log('  none — every slug lives on one side only. The merge is mechanical.');
} else {
  const free = [], decide = [];
  for (const r of both) {
    const a = TAG_MAP[r.tag_kind], b = TAGLET_MAP[r.taglet_kind];
    (a === b && SCOPE[a] === 'shared' ? free : decide).push({ ...r, a, b });
  }
  console.log(`  ${both.length} slug(s) exist in both tables.\n`);

  if (free.length) {
    console.log(`  ${free.length} resolve themselves — both sides land on the same shared kind,`);
    console.log('  so the two rows become one and both junctions follow:\n');
    console.log(`    ${pad('slug', 26)}${pad('tag', 12)}${pad('taglet', 12)}`
      + `${pad('->', 11)}${rpad('str', 5)}${rpad('snip', 6)}`);
    for (const r of free) {
      console.log(`    ${pad(r.slug, 26)}${pad(r.tag_kind, 12)}${pad(r.taglet_kind, 12)}`
        + `${pad(r.a, 11)}${rpad(r.streams, 5)}${rpad(r.snippets, 6)}`);
    }
  }

  if (decide.length) {
    console.log(`\n  ${decide.length} NEED A DECISION. The two sides land on kinds with`);
    console.log('  different scopes, so whichever you pick, the other surface loses this');
    console.log('  tag — its junction rows stay in the table and stop rendering.\n');
    console.log(`    ${pad('slug', 26)}${pad('tag', 20)}${pad('taglet', 20)}`
      + `${rpad('str', 5)}${rpad('snip', 6)}`);
    for (const r of decide) {
      console.log(`    ${pad(r.slug, 26)}`
        + `${pad(`${r.tag_kind} -> ${r.a} (${SCOPE[r.a]})`, 20)}`
        + `${pad(`${r.taglet_kind} -> ${r.b} (${SCOPE[r.b]})`, 20)}`
        + `${rpad(r.streams, 5)}${rpad(r.snippets, 6)}`);
    }
    console.log('\n    For each: file it under a SHARED kind to keep both, or accept the');
    console.log('    loss on one side. A third option is to rename one of them first.');
  }

  const cased = both.filter((r) => r.tag_name !== r.taglet_name);
  if (cased.length) {
    console.log(`\n  ${cased.length} of these disagree about the display name:`);
    for (const r of cased) console.log(`    ${pad(r.slug, 26)}"${r.tag_name}"  vs  "${r.taglet_name}"`);
    console.log('    The merged row keeps one. Pick before, not during.');
  }
}

// ── same person, two rows, two slugs ──────────────────────────────────────
/* A slug match is the easy case. The interesting one is the same NAME under
   different slugs — which is what the initial batch import left behind, where a
   snippet tagged `shiina_sometitle` minted a taglet `shiina` while the VOD side
   already held `amanogawa-shiina`. Those are not a constraint to work around:
   they are one subject with two rows, and the merge is the moment to collapse
   them. Where both sides land on the same shared kind that is automatic; the
   only thing to say out loud is which slug survives. */
head('same name, two slugs');
const named = all(
  `SELECT t.id AS tag_id, t.name AS tag_name, t.slug AS tag_slug, t.kind AS tag_kind,
          l.id AS taglet_id, l.name AS taglet_name, l.slug AS taglet_slug, l.kind AS taglet_kind,
          (SELECT COUNT(*) FROM stream_tag st WHERE st.tag_id = t.id) AS streams,
          (SELECT COUNT(*) FROM snippet_taglet sl WHERE sl.taglet_id = l.id) AS snippets
     FROM tag t JOIN taglet l ON LOWER(TRIM(l.name)) = LOWER(TRIM(t.name))
    WHERE t.retracted_at IS NULL AND l.retracted_at IS NULL AND l.slug <> t.slug
    ORDER BY t.name`);
if (!named.length) {
  console.log('  none.');
} else {
  const auto = [], manual = [];
  for (const r of named) {
    const a = TAG_MAP[r.tag_kind], b = TAGLET_MAP[r.taglet_kind];
    (a === b && SCOPE[a] === 'shared' ? auto : manual).push({ ...r, a, b });
  }
  if (auto.length) {
    console.log(`  ${auto.length} collapse into one row. Both sides are already the same`);
    console.log('  shared kind, so the surviving row keeps the tag slug and the taglet\'s');
    console.log('  snippet links are repointed at it:\n');
    console.log(`    ${pad('name', 22)}${pad('keeps', 20)}${pad('drops', 12)}`
      + `${rpad('str', 5)}${rpad('snip', 6)}`);
    for (const r of auto) {
      console.log(`    ${pad(r.tag_name, 22)}${pad(r.tag_slug, 20)}${pad(r.taglet_slug, 12)}`
        + `${rpad(r.streams, 5)}${rpad(r.snippets, 6)}`);
    }
    console.log('\n    Nothing is lost: the dropped slug was a shorthand, and after this');
    console.log('    there is one row to rename when the name changes instead of two.');
  }
  if (manual.length) {
    console.log(`\n  ${manual.length} share a name but land on different scopes — same`);
    console.log('  decision as a slug collision:\n');
    for (const r of manual) {
      console.log(`    ${pad(r.tag_name, 22)}${pad(`${r.tag_kind} -> ${r.a} (${SCOPE[r.a]})`, 22)}`
        + `${r.taglet_kind} -> ${r.b} (${SCOPE[r.b]})`);
    }
  }
}

// ── the constraint that can fail mid-migration ────────────────────────────
head('name uniqueness');
console.log('  `tag.name` is UNIQUE and `taglet.name` is not, so the merged table');
console.log('  either enforces it or drops the constraint. Left over after the');
console.log('  collapses above:\n');
const dupNames = all(
  `SELECT LOWER(TRIM(name)) k, COUNT(*) c, group_concat(name, ' | ') names
     FROM taglet WHERE retracted_at IS NULL
    GROUP BY k HAVING c > 1 ORDER BY c DESC`);
if (!dupNames.length) {
  console.log('    nothing — no two taglets share a name. Keep the constraint.');
} else {
  for (const r of dupNames) console.log(`    ${r.c} taglets named the same: ${r.names}`);
  console.log('\n    Each is a rename or a merge before the one-shot runs.');
}

// ── what is actually in each bucket ───────────────────────────────────────
/* Printed because two decisions cannot be made from counts: what the VOD-only
   kinds should be CALLED once `meta` folds into `type`, and what colour each
   kind wants. Both are answered by reading forty names. */
head('what is in each bucket');
/* A tag is used in TWO ways and only one of them is a junction row. `stream_tag`
   says "this broadcast was about that"; `segment.tag_id` says "that was
   happening between here and here". Counting only the first reported Intro,
   Break and Outro as unused when they are the most-drawn blocks on the whole
   timeline — which is exactly backwards, and the kind of number somebody
   deletes a row on the strength of. Both are counted, and shown apart, because
   a tag used only on segments is a different animal from one used only on
   streams and the difference decides which kind it belongs in. */
for (const table of ['tag', 'taglet']) {
  const kinds = all(`SELECT DISTINCT kind FROM ${table} WHERE retracted_at IS NULL ORDER BY kind`);
  for (const k of kinds) {
    const rows = table === 'tag'
      ? all(`SELECT x.name,
                    (SELECT COUNT(*) FROM stream_tag j WHERE j.tag_id = x.id) AS uses,
                    (SELECT COUNT(*) FROM segment g
                      WHERE g.tag_id = x.id AND g.retracted_at IS NULL) AS segs
               FROM tag x WHERE x.kind = ? AND x.retracted_at IS NULL
              ORDER BY (uses + segs) DESC, x.name LIMIT 40`, k.kind)
      : all(`SELECT x.name,
                    (SELECT COUNT(*) FROM snippet_taglet j WHERE j.taglet_id = x.id) AS uses,
                    0 AS segs
               FROM taglet x WHERE x.kind = ? AND x.retracted_at IS NULL
              ORDER BY uses DESC, x.name LIMIT 40`, k.kind);
    const total = one(
      `SELECT COUNT(*) c FROM ${table} WHERE kind = ? AND retracted_at IS NULL`, k.kind).c;
    const segOnly = rows.filter((r) => r.segs && !r.uses).length;
    console.log(`\n  ${table}.${k.kind}  (${total})`
      + (segOnly ? `   — ${segOnly} of these live ONLY on the timeline` : ''));
    console.log('    ' + rows.map((r) => {
      const bits = [];
      if (r.uses) bits.push(`×${r.uses}`);
      if (r.segs) bits.push(`▮${r.segs}`);
      return `${r.name}${bits.length ? ` ${bits.join(' ')}` : ' ·unused'}`;
    }).join(', ').replace(/(.{86})\s/g, '$1\n    '));
    if (total > rows.length) console.log(`    … and ${total - rows.length} more`);
  }
}
console.log('\n  ×n = streams or snippets carrying it.  ▮n = blocks drawn with it.');

// ── the rollup parent_id was built for ────────────────────────────────────
/* Not part of the merge, but it is visible in the same data and the merge is
   when somebody is looking. `schema.sql` names this case verbatim: the vault
   import made ten separate rows for one game. */
head('near-duplicate names');
const fam = all(
  `SELECT a.name AS a, b.name AS b,
          (SELECT COUNT(*) FROM stream_tag j WHERE j.tag_id = a.id) AS ua,
          (SELECT COUNT(*) FROM stream_tag j WHERE j.tag_id = b.id) AS ub
     FROM tag a JOIN tag b
       ON b.id <> a.id AND LENGTH(b.name) > LENGTH(a.name)
      AND (REPLACE(LOWER(b.name), ' ', '') LIKE REPLACE(LOWER(a.name), ' ', '') || '%')
    WHERE a.retracted_at IS NULL AND b.retracted_at IS NULL AND a.kind = b.kind
    ORDER BY a.name, b.name`);
if (!fam.length) {
  console.log('  none.');
} else {
  let last = null;
  for (const r of fam) {
    if (r.a !== last) { console.log(`\n    ${r.a} ×${r.ua}`); last = r.a; }
    console.log(`      └ ${r.b} ×${r.ub}`);
  }
  console.log('\n    `parent_id` exists for exactly this and nothing uses it yet. A child');
  console.log('    inherits art and kind from its parent, so filing these turns ?tag=');
  console.log('    returning one stream in ten back into returning ten.');
}

// ── what else moves ───────────────────────────────────────────────────────
head('the rest, which is mechanical');
const segKinds = all(
  `SELECT kind, COUNT(*) c FROM segment WHERE retracted_at IS NULL GROUP BY kind ORDER BY c DESC`);
console.log('  segment.kind shares tag.kind\'s vocabulary on purpose, so it migrates too:');
for (const r of segKinds) {
  console.log(`    ${pad(r.kind, 12)}${rpad(r.c, 6)}  ->  ${TAG_MAP[r.kind] ?? '?? UNMAPPED'}`);
}
const gates = all(
  `SELECT kind, gate, COUNT(*) c FROM taglet
    WHERE gate IS NOT NULL AND retracted_at IS NULL GROUP BY kind, gate`);
console.log('\n  gate carriers (the column that comes with the merge, and gives #3 to streams):');
if (!gates.length) console.log('    none yet');
for (const r of gates) {
  console.log(`    ${pad(r.gate, 20)}${pad(r.kind, 12)}${rpad(r.c, 4)} taglet(s)`
    + `  -> ${TAGLET_MAP[r.kind]} (${SCOPE[TAGLET_MAP[r.kind]]})`);
}
console.log('\n    A gate rides on the tag, not on the kind, so a VOD gate is simply a');
console.log('    `type` that carries one — members-only, dmca-muted — and a snippet gate');
console.log('    stays a `meta`. Same mechanism, different word, both work.');

const orphanTl = one(
  `SELECT COUNT(*) c FROM taglet l WHERE retracted_at IS NULL
     AND NOT EXISTS (SELECT 1 FROM snippet_taglet s WHERE s.taglet_id = l.id)`).c;
const orphanTag = one(
  `SELECT COUNT(*) c FROM tag t WHERE retracted_at IS NULL
     AND NOT EXISTS (SELECT 1 FROM stream_tag s WHERE s.tag_id = t.id)`).c;
console.log(`\n  unused rows (nothing links to them; cheapest thing to drop rather than move)`);
console.log(`    tag    ${rpad(orphanTag, 5)}`);
console.log(`    taglet ${rpad(orphanTl, 5)}`);

const rich = one(
  `SELECT COUNT(*) p FROM tag WHERE parent_id IS NOT NULL AND retracted_at IS NULL`).p;
const art = one(
  `SELECT COUNT(*) c FROM tag WHERE thumb_path IS NOT NULL AND retracted_at IS NULL`).c;
console.log(`\n  what taglets gain by moving into tag: ${rich} row(s) already use parent_id,`);
console.log(`  ${art} carry box art. Both columns come along; the snippet UI just does not`);
console.log('  have to offer them.');

console.log(`\nschema v${meta(db, 'schema_version')}   nothing was written.`);
