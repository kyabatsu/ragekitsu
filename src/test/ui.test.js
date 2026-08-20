// Browser regressions.
//
// Two whole classes of bug in this project only ever appear as a rendered
// pixel. Nothing throws, nothing logs, no API response is wrong — an element
// that should be gone is still painted, or a panel closes when you click inside
// it. Between them they have cost about ten bugs, and every one was caught by a
// human looking at a screenshot. That is not a durable way to catch a
// regression, so: these.
//
//   node --test test/ui.test.js         (needs playwright)
//   npm run test:ui
//
// Playwright is deliberately NOT a dependency. This project has one, and one is
// the point. The suite skips itself when it is absent, so `npm test` still runs
// on a checkout with nothing installed but express.

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
const DB = join(ROOT, 'data', 'ui-test.db');

let chromium = null;
try { ({ chromium } = await import('playwright')); } catch { /* not installed */ }

// The container this was developed in ships a Chromium outside the default
// cache. Falling through to undefined lets Playwright find its own.
const EXE = [
  '/opt/pw-browsers/chromium-1194/chrome-linux/chrome',
  '/opt/pw-browsers/chromium-1194/chrome-linux64/chrome',
].find(existsSync);

const why = !chromium ? 'playwright is not installed — npm i -D playwright'
  : !existsSync(SEED) ? `no archive at ${SEED}` : false;

describe('browser regressions', { skip: why }, () => {
  let server, browser, page, base, PICK, MEDIA;
  const errs = [];

  before(async () => {
    process.env.TENMA_DB = DB;
    process.env.TENMA_DEV_AUTH = '1';
    // A real media root, so the file picker has something to list. The states
    // it produces are irrelevant to these tests; the listing is the point.
    MEDIA = mkdtempSync(join(tmpdir(), 'tenma-ui-media-'));
    mkdirSync(join(MEDIA, 'raws'), { recursive: true });
    writeFileSync(join(MEDIA, 'raws', '901_picker probe [AAAAAAAAAAA].mp4'), 'x');
    writeFileSync(join(MEDIA, 'raws', '902_other probe [BBBBBBBBBBB].mkv'), 'x');
    writeFileSync(join(MEDIA, 'raws', '901_probe still.jpg'), 'x');
    process.env.TENMA_MEDIA_ROOT = MEDIA;
    for (const f of [DB, DB + '-wal', DB + '-shm']) if (existsSync(f)) rmSync(f);
    mkdirSync(dirname(DB), { recursive: true });
    copyFileSync(SEED, DB);

    const { makeApp } = await import('../server.js');
    const app = makeApp();
    await new Promise((r) => { server = app.listen(0, '127.0.0.1', r); });
    base = `http://127.0.0.1:${server.address().port}`;
    server.app = app;

    browser = await chromium.launch({ executablePath: EXE, args: ['--no-sandbox'] });
    page = await browser.newPage({ viewport: { width: 1500, height: 940 } });
    // Anything the machine could not reach. On a box with no outbound network
    // the platform SDKs never arrive and thumbnails 404, and the page reports
    // both correctly — that is the environment failing, not the code. Narrow on
    // purpose: a real exception in page code still lands in `errs`.
    const OFFLINE = /Failed to load resource|ERR_[A-Z_]+|could not load https?:|iframe API did not load/i;
    page.on('pageerror', (e) => { if (!OFFLINE.test(e.message)) errs.push('pageerror: ' + e.message); });
    page.on('console', (m) => {
      if (m.type() === 'error' && !OFFLINE.test(m.text())) errs.push(m.text());
    });

    await page.goto(base + '/', { waitUntil: 'domcontentloaded' });
    await page.evaluate(async () => {
      await fetch('/api/auth/token', {
        method: 'POST', headers: { 'content-type': 'application/json' },
        credentials: 'same-origin',
        body: JSON.stringify({ handle: 'ui-test', role: 'editor' }) });
    });
    await page.goto(base + '/', { waitUntil: 'domcontentloaded' });
    await page.waitForSelector('.vod.item', { timeout: 15000 });
    await page.waitForTimeout(400);

    // A stream with both a duration and at least one note — the theater tests
    // need a strip to draw on and a row to open. Picked from the data rather
    // than assumed of whichever card happens to be first.
    PICK = await page.evaluate(async () => {
      const months = (await (await fetch('/api/months')).json()).months;
      for (const m of [...months].reverse()) {
        const j = await (await fetch(
          `/api/streams?month=${m.month}&include=notes&limit=100`)).json();
        const hit = j.streams.find((s) => s.duration_s > 600 && (s.notes || []).length);
        if (hit) return hit.id;
      }
      return null;
    });
    assert.ok(PICK, 'no stream in this archive has both a duration and a note');
  });

  after(async () => {
    await browser?.close();
    server?.close();
    try { if (MEDIA) rmSync(MEDIA, { recursive: true, force: true }); } catch { /* */ }
    server?.app?.locals?.close?.();
    for (const f of [DB, DB + '-wal', DB + '-shm']) if (existsSync(f)) rmSync(f);
  });

  /** Every element currently in the document, asked one at a time: if I set
   *  `hidden` on you, do you actually go away? */
  const sweepHidden = () => page.evaluate(() => {
    const bad = [];
    for (const el of document.querySelectorAll('body *')) {
      // `hidden` is an HTMLElement property. SVG children do not have one, and
      // assigning to them sets a JS field nobody reads.
      if (!(el instanceof HTMLElement)) continue;
      const was = el.hidden;
      el.hidden = true;
      if (getComputedStyle(el).display !== 'none') {
        bad.push(el.tagName.toLowerCase()
          + (typeof el.className === 'string' && el.className ? '.' + el.className.split(/\s+/).join('.') : '')
          + (el.id ? '#' + el.id : ''));
      }
      el.hidden = was;
    }
    return [...new Set(bad)];
  });

  /* A goto whose URL differs from the current one ONLY by its fragment is a
     same-document navigation: no reload, so boot() never runs and the deep link
     is never read. The counter forces a real load. */
  let nav = 0;
  const deepLink = (hash) => page.goto(`${base}/?t=${++nav}${hash}`,
    { waitUntil: 'domcontentloaded' });

  const sheetOpen = () => page.evaluate(() => {
    const s = document.getElementById('sheet');
    return !s.hidden && s.classList.contains('open');
  });

  // =========================================================================
  test('the hidden attribute actually hides, on every element on the page', async () => {
    // `hidden` is not special-cased by the browser. It is one rule in the
    // user-agent stylesheet — [hidden] { display: none } — and an author rule
    // at the same specificity beats it, silently. Every element that carries a
    // `display` for layout reasons is therefore one line away from ignoring the
    // attribute entirely, and the failure is invisible: no error, just a thing
    // that will not go away.
    //
    // Six per-element opt-back-in rules were already in the stylesheet when the
    // theater was built. Four more turned up while building it. This sweeps the
    // whole rendered document instead of naming the next one.
    const stages = [];
    stages.push(['the index', await sweepHidden()]);

    await deepLink(`#/s/${PICK}`);
    await page.waitForSelector('#sheet-actions [data-act="theater"]', { timeout: 15000 });
    await page.waitForTimeout(600);
    stages.push(['the details sheet', await sweepHidden()]);

    await page.locator('#sheet [data-tagadd]').click();
    await page.waitForTimeout(700);
    stages.push(['the tag picker', await sweepHidden()]);
    await page.keyboard.press('Escape');
    await page.waitForTimeout(300);

    await page.locator('#sheet-actions [data-act="theater"]').click();
    await page.waitForTimeout(2200);
    stages.push(['the theater', await sweepHidden()]);

    for (const [where, bad] of stages) {
      assert.deepEqual(bad, [], `${where}: these ignore the hidden attribute`);
    }
  });

  // =========================================================================
  test('a chapter stops being a ghost the moment it becomes a chapter', async () => {
    // #th-ghost is the orange preview you drag out. It carries display:flex, so
    // hiding it did nothing, and after Accept it stayed painted on top of the
    // real block — brighter, with the duration still floating in the middle.
    // It reads as a rendering fault in the strip.
    await page.evaluate(() => thSeek(0));
    await page.waitForTimeout(300);
    const box = await page.locator('#th-tlbody').boundingBox();
    const y = box.y + box.height - 22;
    await page.locator('#th-draw').click();
    await page.waitForTimeout(250);
    await page.mouse.click(box.x + box.width * 0.2, y);
    await page.waitForTimeout(200);
    await page.mouse.click(box.x + box.width * 0.45, y);
    await page.waitForTimeout(400);

    assert.ok(await page.locator('#th-ghost').isVisible(), 'nothing was drawn to accept');
    await page.locator('.pop3 [data-chkind="meta"]').click();
    await page.locator('.pop3 [data-ch="ok"]').click();
    await page.waitForFunction(
      () => (thS?.segments || []).some((s) => !s.synthetic), null, { timeout: 12000 });
    await page.waitForTimeout(500);

    assert.equal(await page.locator('#th-ghost').isVisible(), false,
      'the drag preview is still painted over the block it became');
  });

  // =========================================================================
  test('the note editor opens with its clock list closed', async () => {
    // .nte-list carries display:flex for the column layout, so the `hidden` it
    // is built with did nothing and the dropdown was open before anyone had
    // clicked the dot — six options shoving the Save button off the bottom.
    await page.locator('#th-notes .nt').first().dblclick();
    await page.waitForTimeout(500);
    assert.ok(await page.locator('.pop4').isVisible(), 'the note editor did not open');
    assert.equal(await page.locator('.pop4 .nte-list').isVisible(), false,
      'the clock list is open before anyone asked for it');

    await page.locator('.pop4 .nte-clock .nte-dot').click();
    await page.waitForTimeout(250);
    assert.ok(await page.locator('.pop4 .nte-list').isVisible(),
      'and it still has to open when the dot is clicked');
    await page.keyboard.press('Escape');
    await page.waitForTimeout(300);
  });

  // =========================================================================
  test('clicking inside the sheet never closes the sheet', async () => {
    // The outside-click closer asks `e.target.closest('#sheet')` in the bubble
    // phase — after every handler between the target and the document. Three
    // controls replace their own element inside their own click handler, which
    // detaches the node the event came from; closest() then walks a chain that
    // no longer reaches #sheet, and the page concludes the click was outside
    // and closes the thing being edited.
    //
    // The answer is now recorded during CAPTURE, before anything can detach.
    // These are the three that actually happened.
    await page.keyboard.press('Escape');       // leave the theater
    await page.waitForTimeout(700);
    await deepLink(`#/s/${PICK}`);
    await page.waitForSelector('#sheet-actions [data-act="copy"]', { timeout: 15000 });
    await page.waitForTimeout(600);
    assert.ok(await sheetOpen(), 'the sheet did not open');

    await page.locator('#sheet-actions [data-act="copy"]').click();
    await page.waitForTimeout(400);
    assert.ok(await sheetOpen(), 'copying a link closed the sheet');
    await page.waitForTimeout(1200);

    await page.locator('#sheet .tagchip').first().click();
    await page.waitForTimeout(500);
    assert.ok(await sheetOpen(), 'opening the tag editor closed the sheet');
    await page.keyboard.press('Escape');
    await page.waitForTimeout(300);

    await page.locator('#sheet .sum-t').first().dblclick();
    await page.waitForTimeout(400);
    assert.ok(await sheetOpen(), 'starting a summary closed the sheet');
    assert.ok(await page.locator('#sheet .sum-t textarea.inline-in').isVisible(),
      'the summary editor never appeared');
    await page.keyboard.press('Escape');
    await page.waitForTimeout(300);
  });

  // =========================================================================
  test('notes group by what they are for, in the order the work happens', async () => {
    // A note list is a work queue before it is a transcript, and the work
    // batches by kind. Chronological has to survive INSIDE each group, which it
    // does because the server orders by offset and this sort is stable.
    const order = await page.evaluate(() => {
      const mk = (tag, off) => ({ id: `${tag}${off}`, tag, offset_s: off });
      // Deliberately shuffled, and with two words nobody planned for.
      return sortNotes([
        mk('clip', 10), mk(null, 20), mk('lore', 30), mk('asset', 40),
        mk('short', 50), mk('meme', 60), mk('highlight', 70), mk('clip', 80),
        mk(null, 90), mk('personal', 100),
      ]).map((n) => `${n.tag ?? 'raw'}:${n.offset_s}`);
    });
    assert.deepEqual(order, [
      'asset:40', 'lore:30', 'meme:60', 'short:50',
      'clip:10', 'clip:80',              // stable: the incoming order is offset order
      'highlight:70', 'personal:100',    // unplanned words, after the planned ones
      'raw:20', 'raw:90',                // untagged last
    ]);
  });

  test('the add-a-note field is last on the card and first in the theater', async () => {
    // Two different jobs. On the card you are reviewing a finished stream, so
    // the field belongs after what is already there. In the theater you are
    // writing while something plays, and a field that walks down the page as
    // the list grows is a field you have to chase.
    await deepLink(`#/s/${PICK}`);
    await page.waitForSelector('.cal-notes .nb-list', { timeout: 15000 });
    await page.waitForTimeout(500);
    assert.equal(
      await page.evaluate(() => document.querySelector('.cal-notes .nb-list').lastElementChild.className),
      'ob-new', 'the card\'s field is not at the end of the list');

    await page.locator('#sheet-actions [data-act="theater"]').click();
    await page.waitForTimeout(2200);
    assert.equal(
      await page.evaluate(() => document.getElementById('th-notes').firstElementChild.className),
      'nt-new', "the theater's field is not above the first note");
  });

  test('typing a note and pressing Enter writes it', async () => {
    const before = await page.evaluate(() => (thS.notes || []).length);
    await page.locator('#th-notes [data-notenew]').fill('#lore 02 tenma is a linguist (00:20:34)');
    await page.keyboard.press('Enter');
    await page.waitForFunction(
      (n) => (thS?.notes || []).length > n, before, { timeout: 12000 });
    const made = await page.evaluate(() =>
      (thS.notes || []).find((n) => n.text === 'tenma is a linguist'));
    assert.ok(made, 'the note never arrived');
    assert.equal(made.tag, 'lore');
    assert.equal(made.seq, 2);
    assert.equal(made.done, true, 'lore is a record, not a task — it arrives ticked');
    assert.equal(await page.locator('#th-notes [data-notenew]').inputValue(), '',
      'the field kept what it just saved');
  });

  test('a row still seeks its own note after the grouping reorders it', async () => {
    // Rows used to carry their index into thS.notes. Grouping by tag broke that
    // correspondence, so they carry the id — and clicking one has to land on
    // the note it names, not on whatever now sits at that position.
    await page.evaluate(() => thSeek(0));
    await page.waitForTimeout(400);
    const want = await page.evaluate(() => {
      const n = (thS.notes || []).find((x) => x.start_s > 100);
      return n && { id: n.id, at: Math.round(n.start_s) };
    });
    assert.ok(want, 'this stream has no note far enough in to test with');
    await page.locator(`.nt[data-thnote="${want.id}"]`).click();
    await page.waitForTimeout(500);
    assert.equal(await page.evaluate(() => Math.round(thAxis)), want.at);
  });

  // =========================================================================
  test('correcting a start time inline writes one change and moves nothing', async () => {
    // The rule, enforced from the surface a human actually uses: a start time
    // is one fact, and the numbers on the notes were never measured against
    // it. This used to cascade and rewrite every unanchored offset.
    await deepLink(`#/s/${PICK}`);
    await page.waitForSelector('#sheet [data-edit="when"]', { timeout: 15000 });
    await page.waitForTimeout(600);
    // The STORED numbers, not the derived axis positions. An anchored note's
    // axis position moves when the zero does — that is the clock model working.
    // What must not change is the number somebody typed.
    const raw = () => page.evaluate(async () => {
      const j = await (await fetch(`/api/streams/${current.id}`, { cache: 'no-cache' })).json();
      return { started: j.started_at, offsets: (j.notes || []).map((n) => n.offset_s) };
    });
    const before = await raw();

    await page.locator('#sheet [data-edit="when"]').dblclick();
    await page.waitForTimeout(300);
    const was = await page.locator('#sheet .inline-in').inputValue();
    await page.locator('#sheet .inline-in').fill(
      was.replace(/(\d\d):(\d\d)$/, (m, h, mi) => `${h}:${String((+mi + 5) % 60).padStart(2, '0')}`));
    await page.keyboard.press('Enter');
    await page.waitForFunction((b) => current && current.started_at !== b, before.started,
      { timeout: 12000 });
    await page.waitForTimeout(400);

    const after = await raw();
    assert.notEqual(after.started, before.started, 'the start time did not move');
    assert.deepEqual(after.offsets, before.offsets, 'the notes moved with the start time');
    const fields = await page.evaluate(async () => {
      const h = (await (await fetch(`/api/streams/${current.id}/history`,
        { cache: 'no-cache' })).json()).history;
      return (h[0]?.changes || []).map((c) => `${c.target_type}.${c.field}`);
    });
    assert.deepEqual(fields, ['stream.started_at'],
      'correcting a start time wrote more than the start time');
  });

  test('committing an inline edit does not close the sheet under you', async () => {
    // Pressing the mouse on another element blurs the input, which commits,
    // which collapses a three-row textarea to one line. The sheet is anchored
    // to the bottom of the window, so it shrinks UPWARD — and by the time
    // mouseup lands, the point you pressed is above its top edge and the page
    // concludes you clicked outside. The outside-click answer is recorded at
    // pointerdown for exactly this.
    assert.ok(await sheetOpen());
    await page.locator('#sheet .sum-t').first().dblclick();
    await page.waitForTimeout(350);
    await page.locator('#sheet .sum-t textarea.inline-in').fill('A summary, typed and blurred.');
    await page.locator('#sheet [data-edit="title"]').click();      // blur commits
    await page.waitForTimeout(2500);
    assert.ok(await sheetOpen(), 'committing a summary closed the sheet');
    assert.equal(await page.evaluate(async () =>
      (await (await fetch(`/api/streams/${current.id}`, { cache: 'no-cache' })).json()).summary),
      'A summary, typed and blurred.');
  });

  test('a bad value is refused and the field stays open', async () => {
    await page.locator('#sheet [data-edit="idx"]').dblclick();
    await page.waitForTimeout(300);
    await page.locator('#sheet .inline-in').fill('not a number');
    await page.keyboard.press('Enter');
    await page.waitForTimeout(600);
    assert.equal(await page.locator('#sheet .inline-in').count(), 1,
      'the field closed on a value it refused to save');
    await page.keyboard.press('Escape');
    await page.waitForTimeout(300);
  });

  test('deleting a stream is confirmed, and can be undone', async () => {
    // `retracted_at` was made writable for exactly this. Without it the corner
    // icon is a one-way door on a grid you click constantly.
    // Whatever is in the grid — PICK is chosen for having notes and a duration
    // and may well be in a month the calendar is not showing.
    const victim = await page.locator('.vod.item').first().getAttribute('data-id');
    assert.ok(victim, 'no cards in the grid to delete');
    const card = page.locator(`.vod.item[data-id="${victim}"]`);
    await card.hover();
    await card.locator('[data-del]').click();
    await page.waitForTimeout(500);
    assert.match(await page.locator('.pop2:not([hidden])').innerText(), /Tombstoned, not erased/);

    await page.locator('.pop2 [data-del-act="go"]').click();
    await page.waitForFunction((id) => !document.querySelector(`.vod.item[data-id="${id}"]`),
      victim, { timeout: 12000 });

    await page.locator('.toast .toast-act').last().click();
    await page.waitForFunction((id) => !!document.querySelector(`.vod.item[data-id="${id}"]`),
      victim, { timeout: 12000 });
  });

  // =========================================================================
  //  the record editor's widgets
  // =========================================================================

  // deepLink, not goto — a fragment-only navigation does not reload, so the
  // second theatre test would open against the first one's page.
  const openTheatre = async () => {
    await deepLink(`#/w/${PICK}`);
    await page.waitForSelector('#th-srcbar button', { timeout: 15000 });
    await page.waitForTimeout(500);
  };
  const openTheatreRecord = async () => {
    await openTheatre();
    await page.locator('#th-srcbar [data-thwrench]').click();
    await page.waitForSelector('.pop5:not([hidden])', { timeout: 8000 });
  };

  test('the theatre offers a wrench instead of an undiscoverable double-click', async () => {
    await openTheatre();
    const wrench = page.locator('#th-srcbar [data-thwrench]');
    assert.equal(await wrench.count(), 1, 'an editor should see exactly one wrench');
    await wrench.click();
    await page.waitForSelector('.pop5:not([hidden])', { timeout: 8000 });
    // Switching source must not blow the wrench away — the bar only repaints
    // aria-pressed when the source count is unchanged, and the wrench is not a
    // source, so it has to survive that path.
    await page.locator('.pop5 [data-r-act="cancel"]').click();
    const srcs = page.locator('#th-srcbar [data-thsrc]:not([disabled])');
    if (await srcs.count() > 1) {
      await srcs.nth(1).click();
      await page.waitForTimeout(300);
      assert.equal(await page.locator('#th-srcbar [data-thwrench]').count(), 1,
        'the wrench vanished when the source changed');
    }
  });

  test('the tz offset is entered in hours and stored in minutes', async () => {
    await openTheatreRecord();
    const box = page.locator('.pop5 [data-r="tz_offset_min"]');
    const shown = await box.inputValue();
    const stored = await page.evaluate((id) => fetch(`/api/streams/${id}`)
      .then((r) => r.json()).then((j) => j.tz_offset_min), PICK);
    assert.equal(Number(shown), stored / 60,
      `showed ${shown} for a stored ${stored} minutes`);
    assert.equal(await box.getAttribute('data-unit'), 'hours');
    await page.locator('.pop5 [data-r-act="cancel"]').click();
  });

  test('the default source is pills, with auto a real choice', async () => {
    await openTheatreRecord();
    const pills = page.locator('.pop5 [data-pills="serve"] button');
    assert.equal(await pills.count(), 3, 'auto, YouTube, Twitch');
    assert.equal(await pills.first().innerText(), 'auto');
    // Exactly one pressed, and it agrees with the hidden input the save reads.
    const pressed = page.locator('.pop5 [data-pills="serve"] button[aria-pressed="true"]');
    assert.equal(await pressed.count(), 1);
    await pills.nth(1).click();
    assert.equal(await page.locator('.pop5 [data-r="serve"]').inputValue(), 'YT');
    assert.equal(await page.locator(
      '.pop5 [data-pills="serve"] button[aria-pressed="true"]').count(), 1,
      'pressing one must release the others');
    await page.locator('.pop5 [data-r-act="cancel"]').click();
  });

  test('the file picker lists what is on disk and writes the stored path', async () => {
    await openTheatreRecord();
    const cap = page.locator('.pop5 .rec-cap').first();
    if (!await cap.count()) return;             // nothing to pick for
    await cap.locator('[data-browse]').first().click();
    await page.waitForSelector('.pop5 .rec-browse .rec-browse-list button', { timeout: 8000 });

    const names = await page.locator('.pop5 .rec-browse-list button').allInnerTexts();
    assert.ok(names.some((n) => n.includes('901_picker probe')), names.join(' | '));
    // A video field must not offer a jpg.
    assert.ok(!names.some((n) => n.includes('.jpg')), 'image in a video picker');

    await page.locator('.pop5 .rec-browse-list button', { hasText: '901_picker probe' })
      .first().click();
    const field = cap.locator('[data-r$=":video_path"]');
    assert.equal(await field.inputValue(),
      'raws/901_picker probe [AAAAAAAAAAA].mp4',
      'the picker writes the media-root-relative path, which is what the column holds');
    assert.equal(await page.locator('.pop5 .rec-browse').count(), 0, 'panel should close');
    await page.locator('.pop5 [data-r-act="cancel"]').click();
  });

  test('the capture rows drop the fields that are not decisions', async () => {
    await openTheatreRecord();
    const cap = page.locator('.pop5 .rec-cap').first();
    if (!await cap.count()) return;
    assert.equal(await cap.locator('[data-r$=":chat_path"]').count(), 0,
      'chat is a property of the stream now');
    assert.equal(await cap.locator('[data-r$=":thumb_path"]').count(), 0,
      'the capture thumbnail is derived, not chosen');
    for (const f of ['platform', 'url', 'remote_id', 'video_path', 'mirror_url']) {
      assert.equal(await cap.locator(`[data-r$=":${f}"]`).count(), 1, `${f} is missing`);
    }
    await page.locator('.pop5 [data-r-act="cancel"]').click();
  });

  test('the remote id fills in from the url without eating a typed one', async () => {
    await openTheatreRecord();
    const cap = page.locator('.pop5 .rec-cap').first();
    if (!await cap.count()) return;
    const url = cap.locator('[data-r$=":url"]');
    const rid = cap.locator('[data-r$=":remote_id"]');

    await rid.fill('');
    await url.fill('https://youtu.be/dQw4w9WgXcQ');
    assert.equal(await rid.inputValue(), 'dQw4w9WgXcQ');

    // A hand-typed id is not overwritten by the next url edit.
    await rid.fill('MINEMINEMIN');
    await url.fill('https://www.youtube.com/watch?v=fWk_JdowmGE');
    assert.equal(await rid.inputValue(), 'MINEMINEMIN',
      'a deliberate id must survive the url changing');
    await page.locator('.pop5 [data-r-act="cancel"]').click();
  });

  test('a signed-out viewer is shown nothing they cannot do', async () => {
    // Starts from the index rather than inheriting whatever the previous test
    // left open — this one is about the grid, and clicking the topbar from
    // inside the theatre hits the overlay instead.
    await deepLink('');
    await page.waitForSelector('.vod.item', { timeout: 15000 });
    await page.locator('[data-who="out"]').click();
    await page.waitForTimeout(2200);
    assert.equal(await page.locator('[data-edit]:visible').count(), 0);
    assert.equal(await page.locator('.card-tool').count(), 0);
    assert.equal(await page.locator('#cal-new').isVisible(), false);
  });

  // =========================================================================
  test('nothing threw along the way', () => {
    assert.deepEqual(errs, []);
  });
});
