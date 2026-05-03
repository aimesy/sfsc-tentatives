// Service worker: GitHub commits + background bulk scraping + hotkey.

// In Chrome MV3 we run as a service worker and load holidays.js via importScripts.
// In Firefox MV3 we run as an event page; manifest's background.scripts loads
// holidays.js for us, and importScripts isn't defined here.
if (typeof importScripts === 'function') {
  importScripts('./holidays.js'); // exposes COURT_HOLIDAYS, localISO, weekdaysBetween, nextBusinessDay
}

// ── Message router ────────────────────────────────────────────────────────────

chrome.runtime.onMessage.addListener((msg, _sender, respond) => {
  const handlers = {
    commit:          () => commitToGitHub(msg.payload),
    'check-updates': checkForUpdates,
    'download-update': downloadUpdate,
    'start-bulk':    () => startBulk(msg.payload),
    'stop-bulk':     () => stopBulk(msg.payload),
    'resume-bulk':   () => resumeBulk(msg.payload),
    'bulk-status':   () => readJob(msg.payload?.tabId),
    'bulk-status-all': () => readAllJobs(),
  };
  const handler = handlers[msg.action];
  if (!handler) return false;
  Promise.resolve().then(handler).then(respond).catch(err => respond({ error: err.message }));
  return true;
});

// Drop a tab's job state when the tab itself goes away — otherwise we'd
// accumulate stale per-tab entries in storage forever.
chrome.tabs.onRemoved.addListener(async (tabId) => {
  await chrome.alarms.clear(bulkAlarmName(tabId));
  const all = await readAllJobs();
  if (all[tabId]) {
    delete all[tabId];
    await chrome.storage.local.set({ _bulkJobs: all });
  }
});

// ── Update checking ───────────────────────────────────────────────────────────

async function checkForUpdates() {
  const res = await fetch(
    'https://api.github.com/repos/aimesy/sfsc-tentatives/contents/sfsc-extension.zip',
    { headers: { 'X-GitHub-Api-Version': '2022-11-28' } }
  );
  if (!res.ok) return { hasUpdate: false };
  const { sha: latestSha } = await res.json();
  const { _lastExtensionSha: lastSha } = await chrome.storage.local.get(['_lastExtensionSha']);
  if (latestSha === lastSha) return { hasUpdate: false };
  await chrome.storage.local.set({ _lastExtensionSha: latestSha });
  return { hasUpdate: true, latestSha };
}

async function downloadUpdate() {
  try {
    const downloadId = await new Promise((resolve, reject) => {
      chrome.downloads.download({
        url: 'https://raw.githubusercontent.com/aimesy/sfsc-tentatives/master/sfsc-extension.zip',
        filename: 'sfsc-extension.zip',
        saveAs: false,
      }, id => id !== undefined ? resolve(id) : reject(new Error('Download failed')));
    });
    return { success: true, downloadId };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

// ── Duplicate detection (cached for 60s per dept) ─────────────────────────────

const _dirCache = new Map();

async function getDeptDir(token, owner, repo, branch, department) {
  const cached = _dirCache.get(department);
  if (cached && Date.now() - cached.time < 60_000) return cached.files;
  const res = await fetch(
    `https://api.github.com/repos/${owner}/${repo}/contents/raw/dept${department}?ref=${branch}`,
    { headers: { Authorization: `Bearer ${token}`, 'X-GitHub-Api-Version': '2022-11-28' } }
  );
  const json = res.ok ? await res.json().catch(() => []) : [];
  const files = Array.isArray(json) ? json : [];
  _dirCache.set(department, { files, time: Date.now() });
  return files;
}

// ── Local commit log (chrome.storage.local._localCommitted) ───────────────────
// Records every successful commit immediately so the next "Scan Unscanned
// Pages" sees freshly-scraped dates even when coverage/dept<N>.json hasn't
// caught up yet. coverage.json only refreshes after the ingest workflow runs
// (throttled to ~once/minute). Without this log, stopping a bulk scan and
// restarting it inside that window would replay the same dates from the top.
//
// Schema: { [department]: [{ d: 'YYYY-MM-DD', t: epoch_ms }, ...] }
// Entries older than LOCAL_LOG_TTL_MS are pruned on every write — by then
// the workflow has long since baked the date into coverage.json.

const LOCAL_LOG_TTL_MS = 24 * 60 * 60 * 1000;

async function trackLocalCommit(department, date) {
  const { _localCommitted = {} } = await chrome.storage.local.get('_localCommitted');
  const now = Date.now();
  const list = (_localCommitted[department] || [])
    .filter(e => e && e.d && e.d !== date && now - (e.t || 0) < LOCAL_LOG_TTL_MS);
  list.push({ d: date, t: now });
  _localCommitted[department] = list;
  await chrome.storage.local.set({ _localCommitted });
}

async function readLocalCommitted(department) {
  const { _localCommitted = {} } = await chrome.storage.local.get('_localCommitted');
  const now = Date.now();
  return (_localCommitted[department] || [])
    .filter(e => e && e.d && now - (e.t || 0) < LOCAL_LOG_TTL_MS)
    .map(e => e.d);
}

// ── Commit ────────────────────────────────────────────────────────────────────

function parseCourtDateISO(raw) {
  if (!raw) return null;
  const m = raw.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (m) return `${m[3]}-${m[1].padStart(2, '0')}-${m[2].padStart(2, '0')}`;
  if (/^\d{4}-\d{2}-\d{2}/.test(raw)) return raw.slice(0, 10);
  return null;
}

function courtDate(rulings) {
  return parseCourtDateISO(rulings?.[0]?.['Court Date'] ?? '');
}

async function commitToGitHub({ token, owner, repo, branch, data }) {
  const { department, scraped_at } = data;
  let { rulings } = data;
  const date = data._date || courtDate(rulings);
  if (!date) throw new Error('No court date — pass _date in payload when rulings is empty.');

  // Stale-page guard. SFTC keeps the previous search's rulings in the DOM
  // when the new search returns zero records, so a scrape of date X can
  // surface rulings whose Court Date is some earlier date Y. Without this
  // check we'd write `<X>-<time>.json` containing rulings for Y — yielding
  // exactly the "uploaded 25 rulings to 2020-06-10 but it had 0 rulings"
  // symptom. When detected, treat the scrape as zero rulings and commit an
  // empty marker so the gap still closes.
  let staleCount = 0;
  if (rulings?.length) {
    const wrongDate = rulings.find(r => {
      const iso = parseCourtDateISO(r['Court Date'] || '');
      return iso && iso !== date;
    });
    if (wrongDate) {
      staleCount = rulings.length;
      rulings = [];
      data = { ...data, rulings: [], _stale_dropped: staleCount,
               _stale_court_date: parseCourtDateISO(wrongDate['Court Date']) };
    }
  }

  const time = new Date(scraped_at).toISOString().slice(11, 19).replace(/:/g, '');
  const path = `raw/dept${department}/${date}-${time}.json`;

  const files = await getDeptDir(token, owner, repo, branch, department);
  if (files.some(f => f.name.startsWith(`${date}-`))) return { duplicate: true, path };

  const content = btoa(unescape(encodeURIComponent(JSON.stringify(data, null, 2))));
  const message = staleCount
    ? `Mark ${date} (Dept ${department}) — 0 rulings (page returned stale ${data._stale_court_date} data)`
    : `Add ${rulings.length} rulings for ${date} (Dept ${department})`;
  const res = await fetch(`https://api.github.com/repos/${owner}/${repo}/contents/${path}`, {
    method: 'PUT',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
      'X-GitHub-Api-Version': '2022-11-28',
    },
    body: JSON.stringify({ message, content, branch }),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.message || `GitHub API error ${res.status}`);
  }

  // Update the in-memory caches so the next duplicate check and the next
  // hotkey advance both see this date as covered without a round-trip.
  files.push({ name: `${date}-${time}.json` });
  for (const [key, entry] of _covCache) {
    if (key.startsWith(`${department}|`) && !entry.dates.includes(date)) {
      entry.dates.push(date);
    }
  }

  // Persist the commit so a popup reopen, service-worker restart, or a fresh
  // "Scan Unscanned Pages" started before the ingest workflow runs still sees
  // this date as covered. Best-effort — failure here only loses a 24h cache.
  trackLocalCommit(department, date).catch(() => {});

  const json = await res.json();
  return { ok: true, path, sha: json.content?.sha, stale: !!staleCount, staleCount };
}

// ── Bulk scraping state machine ───────────────────────────────────────────────
// State lives in chrome.storage.local._bulkJobs as a tabId-keyed map so each
// SFTC tab can run an independent scrape. State survives popup close and
// service-worker restarts. Each tab gets its own alarm name (sfsc-bulk-next-<tabId>)
// so wakeups route to the right job.
//
// Per-tab schema: {
//   runId,                       // monotonic ID; used to discard stale callbacks
//   running, done, fatalError,
//   dates[], index, currentDate,
//   tabId, settings, waitMs,
//   committed, skipped, errors,
//   waitingForTab,               // true while a navigation is in flight
//   pausedForSession,            // true when SFTC returned the "session expired"
//                                // page; the tab has been auto-reloaded so the
//                                // user can solve the Cloudflare CAPTCHA.
//                                // Resume picks up at the same date that
//                                // triggered the expiry (index NOT advanced).
// }

const BULK_ALARM_PREFIX = 'sfsc-bulk-next:';
function bulkAlarmName(tabId) { return `${BULK_ALARM_PREFIX}${tabId}`; }
function tabIdFromAlarm(name) {
  if (!name?.startsWith(BULK_ALARM_PREFIX)) return null;
  const n = parseInt(name.slice(BULK_ALARM_PREFIX.length), 10);
  return Number.isFinite(n) ? n : null;
}

chrome.alarms.onAlarm.addListener(alarm => {
  const tabId = tabIdFromAlarm(alarm.name);
  if (tabId != null) bulkStep(tabId);
});

// When an SFTC tab finishes loading after a form-submit navigation,
// resume scraping immediately rather than waiting for the next alarm.
// Each tab is matched to its own job state.
chrome.tabs.onUpdated.addListener(async (tabId, info) => {
  if (info.status !== 'complete') return;
  const job = await readJob(tabId);
  if (!job?.running || !job.waitingForTab) return;
  bulkScrapeAfterLoad(job);
});

async function readAllJobs() {
  const { _bulkJobs = {} } = await chrome.storage.local.get('_bulkJobs');
  return _bulkJobs && typeof _bulkJobs === 'object' ? _bulkJobs : {};
}

async function readJob(tabId) {
  if (tabId == null) return null;
  const all = await readAllJobs();
  return all[tabId] || null;
}

async function writeJob(tabId, job) {
  const all = await readAllJobs();
  all[tabId] = job;
  await chrome.storage.local.set({ _bulkJobs: all });
}

// Apply `mutator` to the named tab's job, but ONLY if `runId` still matches —
// otherwise the in-flight callback was superseded by Stop or a new Start, and
// the update is silently discarded. Returns the new state, or null if discarded.
async function applyJobUpdate(tabId, runId, mutator) {
  const current = await readJob(tabId);
  if (!current?.running || current.runId !== runId) return null;
  const next = mutator({ ...current });
  await writeJob(tabId, next);
  return next;
}

async function startBulk({ dates, tabId, settings, waitMs }) {
  if (tabId == null) return { error: 'No tabId on start-bulk request.' };
  await chrome.alarms.clear(bulkAlarmName(tabId));
  const job = {
    runId: Date.now(),
    running: true, done: false, fatalError: null,
    dates, index: 0, currentDate: null,
    tabId, settings, waitMs: waitMs || 5000,
    committed: 0, skipped: 0, errors: 0,
    waitingForTab: false,
    pausedForSession: false,
  };
  await writeJob(tabId, job);
  bulkStep(tabId);
  return { ok: true };
}

async function stopBulk({ tabId } = {}) {
  if (tabId == null) return { error: 'No tabId on stop-bulk request.' };
  await chrome.alarms.clear(bulkAlarmName(tabId));
  const current = await readJob(tabId);
  if (current) await writeJob(tabId, { ...current, running: false, pausedForSession: false });
  return { ok: true };
}

// Resume after an auto-pause (session expiry, then CAPTCHA solved). Picks
// up at the same date that triggered the expiry — the index is intentionally
// NOT advanced when sessionExpired is detected. Bumps runId so any
// in-flight callbacks from the prior batch are discarded.
async function resumeBulk({ tabId }) {
  if (tabId == null) return { error: 'No tabId on resume-bulk request.' };
  const current = await readJob(tabId);
  if (!current || current.running) return { error: 'No paused job to resume.' };
  if (!current.dates || current.index >= current.dates.length) {
    return { error: 'Nothing left to scrape.' };
  }
  await chrome.alarms.clear(bulkAlarmName(tabId));
  const next = {
    ...current,
    runId: Date.now(),
    running: true, done: false, fatalError: null,
    tabId,
    waitingForTab: false,
    pausedForSession: false,
  };
  await writeJob(tabId, next);
  bulkStep(tabId);
  return { ok: true };
}

async function bulkStep(tabId) {
  const job = await readJob(tabId);
  if (!job?.running) return;

  if (job.index >= job.dates.length) {
    await writeJob(tabId, { ...job, running: false, done: true });
    return;
  }

  const date = job.dates[job.index];
  await writeJob(tabId, { ...job, currentDate: date });

  if (!await injectContentScript(job)) return;

  // fill-and-scrape may navigate (form submit reloads the page).
  // If it does, the message channel disconnects (lastError is set);
  // tabs.onUpdated → bulkScrapeAfterLoad will resume once the page loads.
  const result = await new Promise(resolve => {
    chrome.tabs.sendMessage(job.tabId, { action: 'fill-and-scrape', date, waitMs: job.waitMs }, r =>
      resolve(chrome.runtime.lastError ? { navigated: true } : r)
    );
  });

  if (result?.navigated) {
    await applyJobUpdate(tabId, job.runId, j => ({ ...j, currentDate: date, waitingForTab: true }));
    return;
  }

  await bulkHandleResult(job, date, result);
}

async function bulkScrapeAfterLoad(job) {
  if (!await injectContentScript(job)) return;
  await new Promise(r => setTimeout(r, 200));

  // One retry if content script isn't quite ready yet
  let data = await sendScrape(job.tabId);
  if (!data || data.error) {
    await new Promise(r => setTimeout(r, 400));
    data = await sendScrape(job.tabId);
  }
  await bulkHandleResult(job, job.currentDate, data);
}

async function sendScrape(tabId) {
  return new Promise(resolve =>
    chrome.tabs.sendMessage(tabId, { action: 'scrape' }, r =>
      resolve(chrome.runtime.lastError ? null : r)
    )
  );
}

async function injectContentScript(job) {
  try {
    await chrome.scripting.executeScript({ target: { tabId: job.tabId }, files: ['content.js'] });
    return true;
  } catch {
    await applyJobUpdate(job.tabId, job.runId, j => ({
      ...j, running: false, waitingForTab: false,
      fatalError: 'SFTC tab was closed. Reopen it and Resume.',
    }));
    return false;
  }
}

async function bulkHandleResult(job, date, data) {
  // Commit first — we can't unwind a network call, and duplicate detection
  // makes it idempotent on Resume. Then atomically advance the job state
  // (or discard the update if Stop landed during the commit).
  let outcome = 'errors';
  if (data?.sessionExpired) {
    outcome = 'session';
  } else if (data && !data.error && !data.pending) {
    try {
      const { token, repo, branch } = job.settings;
      const [owner, repoName] = repo.split('/');
      const res = await commitToGitHub({
        token, owner, repo: repoName, branch,
        data: { ...data, _date: date },
      });
      outcome = res.duplicate ? 'skipped' : 'committed';
    } catch {
      outcome = 'errors';
    }
  }

  const next = await applyJobUpdate(job.tabId, job.runId, j => {
    const u = { ...j, waitingForTab: false };
    if (outcome === 'session') {
      // Auto-pause: stop the run, leave `index` unchanged so Resume
      // re-tries the same date, and reload the SFTC tab so the user
      // hits the Cloudflare CAPTCHA. The page reload happens out-of-band
      // below so storage state is settled before the tab nav fires.
      u.running = false;
      u.pausedForSession = true;
      return u;
    }
    u[outcome]++;
    u.index++;
    if (u.index >= u.dates.length) {
      u.running = false; u.done = true;
    }
    return u;
  });

  if (outcome === 'session') {
    // Fire-and-forget: tell the content script to reload the page. We don't
    // await it — the reload tears down the message channel, and the popup's
    // status listener already shows the paused-for-session prompt.
    chrome.tabs.sendMessage(job.tabId, { action: 'restart-session' }).catch(() => {});
    return;
  }

  if (next?.running) chrome.alarms.create(bulkAlarmName(job.tabId), { when: Date.now() + 300 });
}

// ── Hotkey: commit current page and advance to next business day ──────────────
// Default Alt+Shift+S; user customizes at chrome://extensions/shortcuts (Chrome)
// or about:addons → ⚙ → Manage Extension Shortcuts (Firefox).

chrome.commands.onCommand.addListener(async cmd => {
  if (cmd === 'commit-and-next') await commitAndAdvance();
});

async function commitAndAdvance() {
  const tab = await findSftcTab();
  if (!tab) {
    // Nothing to surface this on — service worker can't toast without a tab.
    return;
  }
  const toast = (message, type) =>
    chrome.tabs.sendMessage(tab.id, { action: 'show-toast', message, type })
      .catch(() => {});

  const settings = await chrome.storage.local.get(['token', 'repo', 'branch']);
  if (!settings.token || !settings.repo?.includes('/')) {
    toast('SFSC: open the popup → Settings to configure your GitHub PAT', 'error');
    return;
  }

  try {
    await chrome.scripting.executeScript({ target: { tabId: tab.id }, files: ['content.js'] });
  } catch {
    toast('SFSC: cannot run on this tab — navigate to the rulings page', 'error');
    return;
  }

  const data = await sendMessage(tab.id, { action: 'scrape' });
  if (!data || data.error) {
    toast(`SFSC: ${data?.error || 'could not read page — run a search first'}`, 'error');
    return;
  }
  if (data.sessionExpired) {
    toast('SFSC: session expired — log in to the SFTC page again', 'error');
    return;
  }

  // Use the page's date input as the source of truth for the current date —
  // it works even when rulings is empty (so the user can step through holidays /
  // empty calendars without getting stuck).
  const dateRes = await sendMessage(tab.id, { action: 'get-date' });
  const currentDate = dateRes?.date || (data.rulings[0]?.['Court Date']
    ? parseCourtDateISO(data.rulings[0]['Court Date']) : null);
  if (!currentDate) {
    toast('SFSC: no date on this page — run a search first', 'error');
    return;
  }

  const [owner, repo] = settings.repo.split('/');
  const branch = settings.branch || 'master';
  let commitMsg;
  try {
    const res = await commitToGitHub({
      token: settings.token, owner, repo, branch,
      data: { ...data, _date: currentDate },
    });
    commitMsg = res.duplicate
      ? `Already committed ${currentDate} — skipped`
      : res.stale
      ? `Page held stale rulings — marked ${currentDate} as 0 rulings`
      : `Committed ${data.rulings.length} ruling${data.rulings.length === 1 ? '' : 's'} for ${currentDate}`;
  } catch (err) {
    toast(`SFSC commit failed: ${err.message}`, 'error');
    return;
  }

  const today = localISO(new Date());
  const next  = await nextUnscannedBusinessDay({
    after: currentDate, until: today,
    token: settings.token, owner, repo, branch,
    department: data.department || '302',
  });
  if (!next) {
    toast(`${commitMsg}. No more business days — you're caught up.`, 'success');
    return;
  }

  toast(`${commitMsg}. Loading ${next}…`, 'success');
  // Fire and forget — the page navigation will finish and the user reviews,
  // then presses the hotkey again. waitMs is short because we don't actually
  // wait for the result here.
  sendMessage(tab.id, { action: 'fill-and-scrape', date: next, waitMs: 100 });
}

// Walk forward from `after` until we find a weekday that isn't a court holiday
// AND isn't already covered. We union three sources so a freshly-committed
// raw JSON closes its gap immediately, even before the ingest workflow has
// rebuilt coverage.json:
//   • coverage/dept<N>.json — parquet court_dates ∪ raw filenames (lags by
//     workflow runtime; covers historical Excel-imports with no raw files)
//   • raw/dept<N>/ listing  — live within seconds of any commit
//   • _localCommitted log   — instant, survives popup reopen and SW restart
async function nextUnscannedBusinessDay({ after, until, token, owner, repo, branch, department }) {
  const covered = new Set();
  const [covRes, dirRes, localRes] = await Promise.allSettled([
    getCoverage(token, owner, repo, branch, department),
    getDeptDir(token, owner, repo, branch, department),
    readLocalCommitted(department),
  ]);
  if (covRes.status === 'fulfilled') covRes.value.forEach(d => covered.add(d));
  if (dirRes.status === 'fulfilled') {
    for (const f of dirRes.value) {
      const iso = f.name?.slice(0, 10);
      if (iso && /^\d{4}-\d{2}-\d{2}$/.test(iso)) covered.add(iso);
    }
  }
  if (localRes.status === 'fulfilled') localRes.value.forEach(d => covered.add(d));

  let d = nextBusinessDay(after);
  while (d <= until) {
    if (!covered.has(d)) return d;
    d = nextBusinessDay(d);
  }
  return null;
}

const _covCache = new Map();
async function getCoverage(token, owner, repo, branch, department) {
  const key = `${department}|${branch}`;
  const cached = _covCache.get(key);
  if (cached && Date.now() - cached.time < 60_000) return cached.dates;
  const res = await fetch(
    `https://api.github.com/repos/${owner}/${repo}/contents/coverage/dept${department}.json?ref=${branch}`,
    { headers: { Authorization: `Bearer ${token}`, 'X-GitHub-Api-Version': '2022-11-28' } }
  );
  if (!res.ok) throw new Error(`coverage fetch failed: ${res.status}`);
  const meta = await res.json();
  const json = JSON.parse(atob((meta.content || '').replace(/\n/g, '')));
  const dates = Array.isArray(json.covered) ? json.covered : [];
  _covCache.set(key, { dates, time: Date.now() });
  return dates;
}

async function findSftcTab() {
  const [active] = await chrome.tabs.query({ active: true, currentWindow: true });
  if (active?.url?.includes('webapps.sftc.org/tr/')) return active;
  const [any] = await chrome.tabs.query({ url: 'https://webapps.sftc.org/tr/*' });
  return any || null;
}

function sendMessage(tabId, msg) {
  return new Promise(resolve =>
    chrome.tabs.sendMessage(tabId, msg, r =>
      resolve(chrome.runtime.lastError ? null : r)
    )
  );
}

