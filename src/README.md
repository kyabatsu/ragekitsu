# Tenma archive — backend

```
server.js    express app + all routes + listen()
archive.js   clocks + derive + changesets — the logic
db.js        connections, pragmas, migrations, ULID
auth.js      roles + the identify() seam — YOURS
schema.sql   twelve tables
test/api.test.js     67 tests — the original surface, ingest, reconcile, browsing
test/clocks.test.js  68 tests — clocks, frames, segments, chapters, notes,
                                no-silent-shift, the merged chat
test/ui.test.js      20 tests — the bug classes only a rendered pixel shows,
                                plus grouping, the note field, and inline editing.
                                Needs playwright; skips itself without it.
scripts/check-db.js    point Node at a db and report
scripts/probe-media.js ffprobe pass — durations, codecs, the local clock
scripts/rebuild.js     rebuild every stream's projected timeline
```

One dependency: `express`. SQLite is `node:sqlite`, built into Node.

## Run

```
npm install
node scripts/check-db.js data/archive.db     # migrates, then reports
node scripts/rebuild.js                      # build the timelines once
node scripts/probe-media.js --root /path/to/media
node server.js
npm test          # 155 tests; the browser ones skip if playwright is absent
npm run test:ui   # just those, once `npm i -D playwright`
```

Environment:

| | |
|---|---|
| `TENMA_DB` | path to `archive.db` (a folder works too) |
| `TENMA_MEDIA_ROOT` | where `raws/...` hangs off. Unset = no file checks, captures read `unverified` rather than being wrongly called lost |
| `TENMA_INGEST_TOKEN` | shared secret for ls-rec. Unset disables ingest entirely |
| `TENMA_DEV_AUTH=1` | enables `POST /api/auth/token`. Never set this anywhere real |
| `PORT`, `TENMA_HOST`, `TENMA_CORS`, `TENMA_PAIR_WINDOW_S` | |

## The one rule

**A decision only changes through an applied changeset.** There is no PATCH. A
staff edit and a viewer's suggestion hit the same endpoint; the only difference
is that an editor's applies on submission instead of queueing. Every value has a
row saying who set it, when, from what, and why.

Machine ingest is the one exception, deliberately: those are *observations* from
a recorder, not decisions. It writes capture-level facts only, and stream-level
derivation skips any field a human has already decided.

## Auth — the part you replace

`auth.js` has one function marked `▼ REPLACE THIS ▼`:

```js
export function identify(db, req) { ... }   // -> { id, handle, role } or ANON
```

Right now it checks a bearer token or cookie against the `session` table. Swap
the body for Discord/Twitch OAuth, Cloudflare Access headers, whatever — return
the same shape and nothing else in the project changes. `requireRole('editor')`
is Express middleware and stays as is.

Roles are a total order: `viewer < suggester < editor < admin`. The first person
to sign in becomes admin, because otherwise there is no way to grant the first
role without hand-editing the database.

## The theater

Three layers, and Esc walks down one at a time: index → details sheet → theater.
`#/w/:id` opens it directly; `#/w/:id@1234` opens it at a moment.

Everything on the timeline arrives already converted onto the axis. The browser
does exactly one conversion, because it has to — the live playhead, sixty times
a minute:

```
axis     = playerPosition + source.covers_s[0]
position = axis - source.covers_s[0]        // covers_s[0] IS start_wall - zero_wall
```

That is why switching source keeps your place: the moment is held on the axis and
re-projected into whichever clock you switched to. A source whose clock has never
been measured is not mounted at all, rather than mounted at a guess.

Three players behind one interface — `time()`, `seek()`, `destroy()`:

| | |
|---|---|
| YouTube | IFrame API, hosted on `youtube-nocookie.com` |
| Twitch | embed v1, `parent` = the serving hostname. **A bare IP will not work** — reach the page as `localhost`. The button says so. |
| local | `<video>` against `/media/video/:capture_id`, ranged, gated on `TENMA_MEDIA_ROOT` |

What the theater refuses to draw, and says instead:

- **no duration** → no strip at all. There is no domain to scale to, and drawing
  one against a guessed length is the thing the clock model exists to prevent.
- **a note past the end of the stream** → no pin. `thPct()` clamps, so it would
  otherwise sit on the right edge claiming to be the finale. It stays in the
  panel with a red `!` and the footer counts them.
- **nothing playable** → a tombstone that says which kind of nothing, from
  `vod_state`. `never` gets *"We know this broadcast happened. That is the whole
  record."*
- **an unlabelled stretch** → hatching, not grey. Grey is a claim.

## Editing from the browser

Three pieces, in the order they had to be built:

**Where it is:** far right of the header bar, same row as the logo. With
`TENMA_DEV_AUTH=1` you get `DEV AUTH · VIEWER · [sign in]`; without it, just
`VIEWER · READ-ONLY` and no button, because there is no other way to sign in
yet. Note `express.static` sets `max-age=300`, so after restarting the server a
browser can serve you the previous `index.html` for five minutes — hard-reload
if the topbar looks unchanged.

1. **Identity.** There was no way to establish a session from the page at all —
   zero calls to `/api/auth/token`, no `Authorization` header anywhere. The
   topbar now has a sign-in control, gated on `TENMA_DEV_AUTH` and badged in red,
   because that endpoint mints a session for any handle with no verification
   whatsoever. Replacing auth means replacing `identify()` in `auth.js`; this
   control just stops existing.
2. **One write primitive.** `submit(changes, reason)` posts a changeset and says
   which of four things happened: **applied**, **queued for review**, *someone
   got there first* (409, with both values), or refused. An editor's change
   lands; everyone else's queues — and saying so every time is what stops a
   suggester thinking their edit vanished.
3. **The tag editor**, built on those two.

Tag chips sit under the meta pills on the sheet and in the theater header.
`+` opens an autocomplete over `/api/tags?q=`. **Creating is always the last row
and always a deliberate click** — left implicit, typing "mario kart" next to an
existing "Mario Kart 8 Deluxe" quietly mints a second one, which is how a
vocabulary rots. Clicking a chip opens an editor with two clearly separated
scopes: *remove from this stream* (one junction row) and *the tag itself — every
stream using it* (rename, kind, confirm, retract; editors only).

## The record editor

Everything about a stream and its captures lives in one popover. Reached three
ways: the wrench on a card, `+` for a new stream, and — new — **a wrench in the
theatre's source bar**. That last one used to be a double-click on the bar, and
a row of source buttons gives no hint that it is double-clickable. An affordance
you have to be told about is not an affordance. It renders only with edit
rights; a viewer sees the sources and nothing else.

Three of the rows are not text boxes, because three of these values are not text:

| | |
|---|---|
| **thumbnail**, **hosted video** | a picker over `GET /api/media/browse`. Editor-only, traversal-checked by resolving and comparing against the root rather than scanning for `..`. Newest first, since the file you want is nearly always the one that just landed. Writes the media-root-relative path — `raws/697_….mp4` — which is exactly what the column holds. |
| **default source** | pills, with `auto` as a real pill rather than an empty box. NULL means "the usual order", which is a choice, and an empty text field does not say that. |
| **tz offset** | entered in hours, stored in minutes. The column stays minutes because half-hour zones exist — India is +5:30, Nepal +5:45 — so hours as storage would make them unrepresentable. Fractions are accepted for that reason, and refused if they do not land on a whole minute. |

`remote_id` fills in from the url and stays editable. It is the key ls-rec
upserts on, so a url typo that silently repointed it would orphan the capture
and make the next recorder packet create a duplicate. The rule: it is replaced
only while it still matches what the previous url produced, so a hand-typed id
survives every later url edit.

What the capture rows no longer offer:

- **chat path** — chat is a property of the stream now. The merged path in the
  section above is where it is set.
- **thumb path** — `thumbFor()` already falls back capture → YouTube
  `hqdefault`, and the thumbnail worth choosing is the stream's, because that is
  the one on the card.
- **vod / chat state** — derived. See below.

The three clocks stay. They are measurements rather than anything downstream of
a url, and this popover is the only place a wrong one can be corrected by hand.

### Why the states are not editable

`vod_state` and `chat_state` were in `WRITABLE` and rendered as enums, and
editing one appeared to work and then reverted, because `recompute()` rewrites
both on the next write to the stream. `pinned()` guards `started_at` and
`duration_s` from exactly that; the states were never added to it.

Adding them was the other option and it is the wrong one. A state is a claim
about the filesystem — `present` means the archive looked and the file was
there. A hand-pinned `present` outlives the file it describes, which is the lie
the state machine exists to prevent. So the inputs are editable and the outputs
are not: correct `chat_path` or `video_path`, and the state follows.

## Notes

A note is one line of text, and four columns fall out of it:

```
#short 01 Tenma eats a cake (00:12:16 - 00:13:27)
 └tag┘ └id┘ └──── text ────┘ └────── stamp ──────┘
```

Only the **first** point of a stamp is the marker — a note is a pin, never an
interval. `-` means a span and `;` means two moments, so the expression is kept
verbatim in `note.stamp` rather than reduced to a second number that would have
to lie about one of them. `offset_s` holds the marker; a plain single stamp
needs no `stamp` at all, because the line is rebuilt from `offset_s`.

The grammar is **strict and refuses rather than guesses**. A trailing paren group
is a timestamp only if it matches exactly. `#srt 06 Tenma doesn't care about your
consent (sent to me)` — a real note here — stays text. So does a half-typed
number, and so does the leading bare stamp the vault used on about half its
lines. Nearly-a-timestamp is how one silently becomes the wrong number.

**The editor never reads `raw`.** The importer already parsed those lines; the
editable line is composed from the stored columns and parsed back into them, and
`raw` stays the untouched receipt — shown greyed under a vault note so a
correction can be checked against what was actually written. Under that rule 913
of the archive's 915 notes round-trip byte-identical. The two that don't are
`#clip 01 …`, where the importer only took a seq when a colon followed it.

`#srt` is accepted and never comes back out: the 166 rows are renamed to `short`
by a migration. Storing one word and displaying another is the `seg_kind` trap.

### Which clock

This is the part the whole clock model was built for and had never been used.
Every imported note is `frame='unknown'` — the number is carried at face value
because nobody wrote down what it was measured against.

A small dot in front of the parsed timestamp opens the list: **youtube**,
**twitch**, **local YT/TW**, **the timeline**, or **not recorded**. Clocks that
have never been measured are offered greyed with the reason, not silently
accepted and then refused at draw time. Picking one sets
`frame='capture'`, `anchor_id`, `anchor_clock` and precision 1 — and the pin
moves by exactly that capture's offset, visibly, with a row in the history.

**A typed timestamp is exact.** Pasting `02:30:00` out of the YouTube player is
an exact position *on YouTube's clock*, which is why naming the clock is the
whole control and not a detail. When the two differ the form shows both:
`00:06:25 on youtube → 00:08:25 on the timeline`.

Type `!playhead` anywhere in a note and it expands to the current position the
moment the word completes, replacing any stamp already on the line. `+ note`
opens the form with the playhead already in it and the caret at the start, so
writing a note while watching is press, type, Enter.

### Adding one

A field, not a button. Adding a note is the most common thing anyone does here,
and putting it behind a click that then opens a form is two gestures for one
line of text. Type the line, press Enter.

It sits at the **end** of the list on the index card and at the **top** of the
theater panel, and the difference is the job: on the card you are reviewing a
finished stream, in the theater you are writing while something plays, and a
field that walks down the page as the list grows is a field you have to chase.

The theater's field knows the playhead, so `!playhead` expands there and a
timestamped note is anchored to whatever is mounted. The card's does not: a note
written there lands `frame='unknown'` -- a number with nobody's word on which
clock it came from -- and its row shows a hollow dot saying so. The theater is
where you name it. Inventing a clock from a card with no player on it would be a
guess with a pin attached.

### The order they show in

Grouped by what the note is **for**, not by when it happened, with a small header
per group:

```
asset - lore - meme - short - clip - anything unplanned - untagged
```

A note list is a work queue before it is a transcript, and the work batches by
kind: you cut every clip in one sitting, not one clip per hour of stream.
Chronological survives *inside* each group, because the server orders by offset
and the sort is stable.

Words nobody planned for -- `highlight`, `extra`, `deprecated`, `project`,
`personal`, nine notes between them -- sort after the five and before untagged,
alphabetically. Burying them with the untagged notes would hide the fact that
somebody invented a word.

The trade-off, stated: in the theater this list no longer reads down in time next
to a timeline that does. The group headers make it obviously grouped rather than
mis-sorted, and the playhead highlight still finds the right row wherever the
sort puts it -- but if scanning the panel chronologically while watching turns
out to matter more, `NOTE_ORDER` is one constant to change.

Rows carry the note's **id**, not its index. Grouping broke the correspondence
between a row's position and its position in `notes`, and an index that used to
be right is a worse bug than one that never was.

### The box

Only some notes are tasks. `lore` and `asset` are records of a fact and arrive
already ticked; `short` and `clip` are work and start empty. An untagged note is
an observation and gets **no box at all** — three of the archive's 433 untagged
notes have ever been ticked, which is noise, and those three keep their box
rather than having a stored fact hidden from them.

Done is dimmed, never struck through: a line through a sentence is hard to read
and says "wrong" when it means "handled". The box is a `<button>`, not a
checkbox input — it writes through a changeset like every other decision, and a
native box that flips the instant you click it would be claiming a write that
may still be queued for review.

## The details sheet

The left column is one object: `[Watch here] [🔗] [✈]` sitting on top of the
thumbnail and sharing its width, so the row grows with the preview when a player
is mounted rather than stranding three buttons above a 560px video. The link
button flips to **Copied!** for a beat — by a class, never by rewriting its
innerHTML, because swapping the node inside a click handler detaches the very
element the event came from and the page's outside-click check then reads it as
"not in the sheet" and closes the sheet under you. The plane opens the same
`#/s/:id` in a new tab.

Where the older/newer buttons used to be there is now the theater's strip,
read-only. It answers "what is in this stream" before you commit to opening it —
the question the sheet exists to answer and the one thing it could not say.
Deliberately inert: chapters get edited on the page that has a playhead to check
them against.

`←` and `→` replace the two buttons. **Left is newer, right is older**, matching
the grid (newest top-left) and the theater's rail, which now runs newest-leftmost
too. The direction is printed above the strip rather than left to be discovered.
Anything with a text cursor in it keeps its own arrow keys, and an open popover
takes them first.

Two pills became one — a date and a clock time are one fact, and splitting them
made the eye do a join. `vod: present` became `vod ✔`, with **three** marks and
not two: `✔` on record, `✗` recorded once and gone, `—` never captured, `?`
nothing has checked. Collapsing the last three into one `✗` would make the pill
disagree with the theater, which gives `never` its own tombstone copy on the
strength of exactly that difference. The word is in the tooltip.

The summary lives here too, inline. All 221 are empty, so this is not an edit
control with an occasional empty state — it is an empty state with an occasional
edit, and it says so and is the click target. Explicit save, one changeset: a
textarea that saved as you typed would put a row in the history per keystroke
burst, and the history is what this archive is made of.

The page reserves room for the sheet by measuring it (`fitSheet()` writes
`--sheet-h`), not by a constant. The sheet has changed height three times now and
each time the hardcoded number went stale silently, parking the bottom row of
cards underneath it.

## Endpoints

**Read** — anonymous, ETag-cached.

| | |
|---|---|
| `GET /api/streams` | `q=` `tag=` `month=` `state=` `limit=` `include=notes`, keyset both ways: `before=`/`before_id=` and `after=`/`after_id=` |
| `GET /api/streams/:id` | sources, segments, notes, coverage, neighbours, prev/next. `rail=N` sizes the neighbour window (default 4) |
| `GET /api/streams/idx/:n` | by the `.md` number |
| `GET /api/streams/:id/history` | who changed what. Public on purpose. |
| `GET /api/tags` · `/api/months` · `/api/health` | |
| `GET /api/health/out-of-span` | captures parked outside their stream's span — likely mispairings |

The detail response carries `axis` (`zero_wall`, `domain_s`, `duration_source`,
`tiled`), `sources[]` (each with `capture_id`, `start_wall`, `covers_s`,
`available`), `lead` as a capture id, the tiled `segments[]`, per-source
`coverage[]`, and `counts`. `watch[]` is still there, unchanged, for anything
already reading it.

**Auth** — `GET /api/auth/me` · `POST /api/auth/token` (dev only) · `POST /api/auth/logout`

**Write** — `POST /api/changesets` · `GET /api/changesets` · `GET /api/changesets/:id` · `POST /api/changesets/:id/review`

**Machine** — `POST /api/ingest/capture` · `GET /api/ingest/next-index`

**Admin** — `GET /api/admin/people` · `POST /api/admin/people/:id/role`

## A changeset

```json
POST /api/changesets
{ "reason": "cleaner title",
  "changes": [
    { "target_type": "stream", "target_id": "01KYQ…",
      "op": "update", "field": "title", "value": "Super Metroid #5" }
  ]}
```

`op` is `create` | `update` | `delete`. A create is several rows sharing a
**client-minted ULID**, applied in one transaction — "insert this missing stream
with its two captures" can never leave half a stream behind, and submitting
twice applies once. A delete tombstones streams and notes rather than removing
them, because a retracted stream has to stay dead.

Each update records `base_value`. If the field moved before review, approving
returns **409 with both values**; `force: true` overrides. Competing proposals
are deliberately not auto-closed — a different suggested value is a different
opinion, and a human arbitrates.

Provenance on created rows is stamped by the applier, never read from the
payload, so nobody can submit a note attributed to someone else.

## The recorder

`POST /api/ingest/capture` is the one write that is not a changeset, and the
reason is worth stating plainly: everything else in this system is somebody's
*decision* about the archive, and a decision deserves review. What the recorder
sends is an *observation* — a machine writing down what it saw at the only
moment it could be seen. There is nothing to arbitrate.

`ls-rec` calls it twice per broadcast, from `ls_archive.py`:

| when | sends |
|---|---|
| **start** — recording begins | `platform`, `remote_id`, `title`, `url`, `record_started_at`, `broadcast_started_at`, `started_at`, `tz_offset_min`, optional `index` |
| **completion** — the wrapup finishes | `platform`, `remote_id`, `video_path`, `chat_path`, `duration_s` |

Auth is `Authorization: Bearer <TENMA_INGEST_TOKEN>` (or `X-Ingest-Token`). The
token is a shared secret; **unset disables ingest entirely** and the endpoint
answers 503 rather than accepting anonymous writes.

Four properties this contract depends on:

**It upserts on `(platform, remote_id)`.** Start and completion are the same
call twice. A daemon that crashes between them, retries, or replays its whole
outbox cannot create a duplicate, and there is no ordering requirement.

**Only fields that were actually sent are written.** The completion call carries
no title, so it cannot blank the one the start call set. `undefined` and `null`
both mean "no news", never "clear this".

**`broadcast_started_at` is written once and never defaulted.** The completion
call sends paths and a duration with no start time; defaulting that to `now()`
on an update moved broadcasts by months, broke platform pairing, and dragged
every note offset with it. That is the first regression test in the file.

**A start nobody measured stays NULL.** If the recorder could not read the
platform's start time it omits the field rather than substituting its own
`record_started_at` — the two differ by however long the probe took to notice.
NULL means *nobody measured this*, and `clocksOf()` falls back to
`started_at + offset_s`, which is a stated approximation. A number in that
column is a measurement. All 371 imported captures hold NULL because the
importer never had one, and this endpoint is the only thing that will ever fill
it in.

Pairing is matched against the whole archive, not one daemon's memory, so a
restart between the YouTube and Twitch halves still joins them into one stream
if their starts fall inside `pairWindow`. A stream the recorder creates is
stamped `origin='ingest'` — it is not in the `.md` and may never be.

The recorder never lets any of this break a recording: six-second timeout, every
call wrapped, and anything undeliverable goes to a small on-disk outbox that
flushes on the next poll tick. Only 5xx queues — a 4xx is the archive saying the
packet is *wrong*, and retrying an identical bad packet forever is worse than
dropping it with a loud log line. The completion packet fires from a `finally`,
so it also covers no parts, merge failed, upload failed, and shutdown: a
broadcast that happened and produced no file is the case the archive most wants
to hear about.

`GET /api/ingest/next-index` exists for the offline case — if the API is
unreachable at record time the recorder still records, then re-syncs its counter.

### ls-audit, which reads before it writes

`ls-rec` posts blind, and that is right for it: it is the only witness, and the
numbers it holds exist nowhere else. `ls-audit` is the opposite — it
reconstructs an entry long after the fact from files, caches and log lines, and
some of what it reconstructs is *worse* than what is already stored. A filename
gives a start time to the minute where the recorder measured it to the second.
A machine that overwrites on principle would degrade the archive every time it
ran.

So the audit path is read, diff, ask:

| | |
|---|---|
| `GET /api/ingest/lookup` | `?id=YT:abc&id=TW:123&idx=515` — what is stored, which fields a human has decided, and whether the two identities disagree |
| `POST /api/ingest/capture` | the same endpoint, now also accepting `stream_id`, `local_start_precision_s`, and a `stream: {}` sub-object |

Four of the five outcomes need no human:

- **new** — the archive holds nothing, so write it
- **same** — they agree, so skip
- **refine** — more precise, and inside the old value's own margin of error
- **noise** — less precise, and consistent with what is stored: say nothing

Only a real disagreement is a **collision**, and those go in front of a person
as before → after. A batch caller rejects them instead of guessing, so the worst
a sweep can do is fill in blanks.

`local_start_precision_s` is what makes *refine* and *noise* decidable. It is
the difference between a measurement and a guess, and it is why the recorder
sends 1 and ls-audit sends 60 when it read a clock off a filename.

Three things this endpoint will not do, because each is a repair rather than an
observation, and repairs belong in a changeset where they get a reason and an
author:

- **Move a capture between streams.** `stream_id` pointing somewhere else
  returns **409** with both stream ids.
- **Delete anything.** A vault that says a platform had no stream, against an
  archive that holds a capture for it, is reported and skipped.
- **Resolve an identity conflict.** When a `remote_id` resolves to one stream
  while the vault index points at another, `lookup` returns both and
  `conflict.kind = 'identity'`, and the client sends nothing. That is ls-rec's
  auto-assigned index and Obsidian's index having drifted apart — two systems
  disagreeing about what is the same broadcast, which a human should look at.

`human_fields` on the lookup response lists every field an applied changeset has
ever set. A machine about to overwrite one is told so, and the prompt says
`← edited by hand`.

## Clocks

There is no single broadcast clock. YouTube and Twitch disagree about when the
broadcast began, and the recorder disagrees with both about when it started
writing — four origins for one stream, five once an archive-channel mirror
lands. **The only frame all of them share is wall-clock time**, so every
position in the archive is a pair — *(which clock, how far in)* — and converting
between two clocks means going through wall time.

```
capture.remote_start_wall   wall time of the platform player's t=0
capture.local_start_wall    wall time of frame 0 of our file
stream.started_at           the axis zero — display only, nothing converts through it
```

A note dropped at 02:30:00 while watching the YouTube embed, on a stream whose
recording began 30 s later, reads 02:30:00 on YouTube forever and 02:29:30 on
the recording. Neither number is stored; both are the same wall moment.

`capture.offset_s` still exists and still means what it always did — the
capture's position on the axis — but it is **derived** now, resynced by
`recompute()`, and not writable through a changeset. The two wall times are.

### Frames

`note.frame` and `segment.frame` say which clock an offset was measured against,
and the third value is the one a boolean cannot hold:

| | |
|---|---|
| `capture` | measured inside `anchor_id`, on `anchor_clock`. Exact. |
| `stream` | measured against the axis. Exact. |
| `unknown` | imported from the vault, read off some VOD nobody wrote down. |

All 915 vault notes are `unknown`, with `offset_precision_s = 120` — the source
timestamps are good to about two minutes, and a pin drawn one pixel wide on a
four-hour bar is a claim of about fifteen seconds. An unknown-frame offset is
never converted into a player position as if it were exact; its link comes back
with `link_exact: false`. `/api/health` counts them, so the correction effort has
a number that goes down.

## No silent shifts

Three operations change what an already-stored timestamp *means*. Each rewrites
its dependents inside the same transaction and records every rewrite as an extra
`change` row on the same changeset, so the history reads *"note X moved from
1234 to 1474, because its anchor was deleted"*.

| | |
|---|---|
| deleting a capture | dependents converted to `frame='stream'`, offsets rewritten |
| moving `stream.started_at` | axis-relative rows slide with the axis; anchored rows don't |
| re-parenting a capture | **refused** while anything still anchors to it |

The old `note.anchor_id … ON DELETE SET NULL` was a silent shift by
construction, because a NULL anchor used to *mean* "already broadcast-relative".
Triggers in `schema.sql` now make that delete fail outright; by the time it runs,
the applier has already converted everything pointing at it.

## Segments

The editorial spine: what was happening, when. The only thing on the timeline
drawn as an interval — notes are markers, including the ones whose source line
carried a range.

`kind` is a closed vocabulary because it is a colour, and a colour has to mean
the same thing in every stream. It is the **same list `tag.kind` uses** —
"what is this tag" and "what is this block" are one question asked twice, and
two parallel lists drift within a week:

| kind | | swatch |
|---|---|---|
| `game` | a specific title being played | `#e0834f` |
| `person` | a guest, a member, a character it is about | `#e0c463` |
| `type` | what kind of stream this stretch is — collab, karaoke, zatsudan, superchat reading, creative, event | `#b797bd` |
| `meta` | the scaffolding — intro, outro, break, waiting screen | `#6f6a7e` |
| `unknown` | nobody has said | 45° hatch, no swatch |

**Nothing infers it** — not the title, not the tags. A new segment is `unknown`
until a human says otherwise, and `unknown` is not `meta`: grey means "nothing
here worth opening", and saying that about content is worse than saying nothing.
A tag minted from an autocomplete miss is `unknown` too, for the same reason —
filing it as `game` because most tags are games is inference wearing a default's
clothes. Tags are entered by hand, never derived from a title.

There was a `tag.seg_kind` alongside `tag.kind` — a second vocabulary for the
colour a tag suggests. It is dropped, by a migration, not merely unused: an
orphan column full of plausible values is a trap. `kind` is still *copied onto*
the segment when a human picks a tag rather than read through at render time, so
re-filing a game later does not repaint forty old strips.

Storage is not constrained to be contiguous, because a human editing a strip will
leave gaps. The API projects a tiled one — holes become synthetic `unknown`
blocks, an open final segment runs to the end — and `apply()` rejects overlaps on
the **final** state, so "shift every boundary back four minutes" still works even
though its intermediate states overlap.

### Drawing one

The theatre's strip is editable: `+ chapter` → click the in point → click the out
point → a form with the tag autocomplete, both timestamps and the four kinds.

- **The button proposes first.** Pressing it opens the form already filled with
  the block you were probably about to draw — from wherever the last chapter
  ended, or 0, up to the playhead — so the common case is zero clicks of
  drawing. It is only a proposal: clicking a different in point throws it away
  and starts a manual draw, and typing over the fields replaces it. Once you
  have touched the form, a stray click on the strip no longer discards it. The
  button stays quiet when there is nothing obvious to propose — playhead at
  zero, or parked inside a chapter that already exists.
- Drawing is an **explicit mode**, because the same click on the same pixel
  already means "seek". One listener decides which; two listeners racing over it
  is how a timeline starts jumping.
- Clicks **snap** to the boundaries of existing blocks, to 0 and the duration,
  and to the playhead — 9px worth of seconds, held `Alt` to turn off. At 2624s
  across ~1100px a pixel is 2.4 seconds, which is why the fields are typable
  (`h:mm:ss`, `m:ss`, or plain seconds) and why snapping exists at all.
- The form refuses before the server does: out before in, out of the domain, or
  overlapping a block that is already there. The server still checks — that is
  the authority — but a 409 round-trip is a worse way to learn it.
- A chapter is written `frame='stream'`. It was drawn on the archive's axis and
  that is what it is a claim about; anchoring it to whichever file happened to be
  playing would move it the day that file's clock is measured properly. That is
  a silent shift from a click that never named a file.
- Minting a tag from the form puts the tag and the block in **one changeset**, so
  a rejected suggestion leaves no orphan word in everyone's autocomplete.
- Clicking an existing block in draw mode opens it — same form, plus Remove.
  Blocks anchored to a capture show their times read-only and say why.

## Editing the record

Every editable thing on the sheet and in the theater is one mechanism: an
element carrying `data-edit`, a rounded outline that appears on hover *only if
you may write*, and a double-click that turns it into an input in place. Enter
or blur saves, Escape cancels, and a value the parser refuses leaves the field
open with a toast rather than swallowing it.

One field table (`INLINE`) drives all of it. There are fourteen of these across
two views, and fourteen bespoke editors would be fourteen places for the same
validation bug. Double-click and not single, because most of these elements are
already something you click.

| | |
|---|---|
| sheet | `#idx` · title · started · length · thumbnail · tags · summary · notes |
| theater | `#idx` · title · started · length · default source · tags · notes |
| both | double-clicking the tags row opens the picker it already has |

The theater has no thumbnail on screen to double-click; it is editable on the
sheet and in the record editor. The `#0697` is `stream.idx`, the number from the
`.md` files — the ULID is not editable and never will be, since being opaque is
the reason correcting a typo can never change an id.

### Correcting a start time changes the start time

It used to cascade. Moving the axis zero rewrote every unanchored note and
segment offset so they held the same wall moment, and each rewrite was logged.

That was wrong. A start time is **one fact** — when the broadcast began — and the
numbers on the notes were never measured against it. A vault note reading
`02:30:00` was read off a video, and it is still 2h30m into that video after
somebody discovers the stream began five minutes earlier. Rewriting it changes a
number nobody has better information about, which is the opposite of a
correction. So `started_at` now writes exactly one change row, and there is a
test that counts them.

The cost, stated: a chapter drawn on the strip *is* axis-relative
(`frame='stream'`), so it keeps its number and therefore lands on slightly
different picture. Making chapters immune too means anchoring them to a capture
at authoring time — a different decision, not this one.

### The record editor and the two card tools

A card carries two controls in its corner, revealed on hover and only for an
editor, because a grid of 221 cards should not also be a grid of 442 buttons.

The **wrench** opens the whole record: the same fields as inline, plus the ones
nothing displays (`tz_offset_min`, `vod_state`, `chat_state`) and the captures —
platform, remote id, url, both clocks, the file paths, the mirror. Captures have
no other home anywhere in the app, which is the wrench's real job. In the theater
the same panel opens by double-clicking the source bar. Shared fields go through
the shared parser, so a duration typed in either place is validated once.

The **trash** deletes, behind a confirm that names the stream — and it is
undoable. `retracted_at` was made writable for exactly this: `op:'delete'`
tombstones by setting it, nothing could clear it, and a one-way door in the
corner of a grid you click constantly is not a button worth having. The toast
carries an Undo for twelve seconds.

`+` beside the Streams heading builds one from nothing. It refuses without a
title and without a start time, because the start time is the axis zero and a
stream without one has no timeline to draw.

## Two bugs this codebase keeps making

Both are invisible: nothing throws, nothing logs, no response is wrong. They
show up only as a rendered pixel, which is why `test/ui.test.js` exists.

**`hidden` loses to any author `display`.** The attribute is not special-cased —
it is one rule in the user-agent stylesheet, `[hidden] { display: none }`, and an
author rule at the same specificity beats it. So `.th-ghost { display: flex }`
silently makes `el.hidden = true` a no-op. Six per-element opt-back-in rules were
already in the stylesheet before the theater existed; four more turned up while
building it. There is now one `[hidden] { display: none !important }` at the end
of the sheet, and a test that sets `hidden` on **every element on the page** and
asserts each one actually goes away. Caveat: that rule also breaks
`hidden="until-found"`, which relies on `content-visibility` rather than
`display`. Nothing here uses it.

**`e.target` can be an orphan by the time you ask about it.** A delegated
listener on `document` runs in the bubble phase — after every handler between the
click and the document. Three controls replace their own element inside their own
click handler (`closePop()`, the copy button, the summary editor), which detaches
the node the event came from; `closest('#sheet')` then walks a chain that no
longer reaches the sheet, and the page closes the thing you were editing. The
answer is now recorded during **capture**, before anything can detach:

```js
let clickedIn = null;
document.addEventListener('click', (e) => {
  clickedIn = { keep: !!e.target.closest('#sheet, …'), day: … };
}, true);                                   // capture: first in the dispatch
document.addEventListener('click', () => { /* reads the booleans */ });
```

A fourth instance turned up with inline editing, one phase earlier than the
others: pressing the mouse on another element blurs the input, which commits,
which collapses a three-row textarea to one line. The sheet is anchored to the
bottom of the window, so it shrinks *upward* — and by the time mouseup lands, the
point you pressed is above its top edge, the click target is `<body>`, and the
page closes the panel you were editing. Recording at `click` capture was still
too late. It is recorded at **`pointerdown`** capture now, which is before
anything can move; a click with no preceding pointerdown (the keyboard) falls
back to the live target.

All the fixes are mutation-tested: reverting any of them makes the matching test
fail by name — *"starting a summary closed the sheet"*, *"committing an inline
edit does not close the sheet under you"*, *"correcting a start time inline
writes one change and moves nothing"*.

## Things worth knowing

- **`stream.timeline_json` is projected once**, by `recompute()`, on every write
  that touches the stream. Not a cache for speed — at 221 streams nothing is
  slow. It is so the strip, the pins and the player read the same numbers
  instead of three call sites arriving at them separately. `check-db.js`
  re-derives every one and reports drift; `rebuild.js` fixes it.
- **`started_at` is no longer derived from the captures.** It used to read
  `min(broadcast_started_at)`, which was dead code — that column is empty for
  every capture that has ever existed. Pointing it at `remote_start_wall`
  instead would be worse than dead: twelve captures carry a negative offset, so
  the first recompute would drag the axis backwards and take every axis-relative
  note with it. The axis moves when somebody says so, through a changeset.
- `local_date` / `local_month` / `start_sod` are **generated columns**. The
  calendar asks a local-time question while `started_at` is UTC and the offset
  varies per row. SQLite computes them, so they cannot drift, and they are
  indexed.
- Migrations in `db.js` are forward-only and idempotent. Once real user content
  exists, "delete and re-import" stops being an option and that list is the only
  way the schema moves.
- The service has **no parser and no path to the `.md`**. Import is a separate
  archived Python script, so no file edit can reach live data.
- `ORDER BY created_at DESC, id DESC` everywhere: `created_at` is whole seconds
  and cannot order two changesets made in the same second. The ULID breaks the
  tie by creation time — that is what it is for.
