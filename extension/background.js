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
    'stop-bulk':     stopBulk,
    'bulk-status':   () => chrome.storage.local.get(['_bulkJob']).then(r => r._bulkJob || null),
  };
  const handler = handlers[msg.action];
  if (!handler) return false;
  Promise.resolve().then(handler).then(respond).catch(err => respond({ error: err.message }));
  return true;
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

  const json = await res.json();
  return { ok: true, path, sha: json.content?.sha, stale: !!staleCount, staleCount };
}

// ── Bulk scraping state machine ───────────────────────────────────────────────
// State lives in chrome.storage.local._bulkJob so it survives popup close and
// service-worker restarts.
//
// Schema: {
//   runId,                       // monotonic ID; used to discard stale callbacks
//   running, done, fatalError,
//   dates[], index, currentDate,
//   tabId, settings, waitMs,
//   committed, skipped, errors,
//   waitingForTab,               // true while a navigation is in flight
// }

const BULK_ALARM = 'sfsc-bulk-next';

chrome.alarms.onAlarm.addListener(alarm => {
  if (alarm.name === BULK_ALARM) bulkStep();
});

// When the SFTC tab finishes loading after a form-submit navigation,
// resume scraping immediately rather than waiting for the next alarm.
chrome.tabs.onUpdated.addListener(async (tabId, info) => {
  if (info.status !== 'complete') return;
  const job = await readJob();
  if (!job?.running || !job.waitingForTab || job.tabId !== tabId) return;
  bulkScrapeAfterLoad(job);
});

async function readJob() {
  const { _bulkJob } = await chrome.storage.local.get('_bulkJob');
  return _bulkJob;
}

// Apply `mutator` to the latest job state, but ONLY if `runId` still matches —
// otherwise the in-flight callback was superseded by Stop or a new Start, and
// the update is silently discarded. Returns the new state, or null if discarded.
async function applyJobUpdate(runId, mutator) {
  const current = await readJob();
  if (!current?.running || current.runId !== runId) return null;
  const next = mutator({ ...current });
  await chrome.storage.local.set({ _bulkJob: next });
  return next;
}

async function startBulk({ dates, tabId, settings, waitMs }) {
  await chrome.alarms.clear(BULK_ALARM);
  const job = {
    runId: Date.now(),
    running: true, done: false, fatalError: null,
    dates, index: 0, currentDate: null,
    tabId, settings, waitMs: waitMs || 5000,
    committed: 0, skipped: 0, errors: 0,
    waitingForTab: false,
  };
  await chrome.storage.local.set({ _bulkJob: job });
  bulkStep();
  return { ok: true };
}

async function stopBulk() {
  await chrome.alarms.clear(BULK_ALARM);
  const current = await readJob();
  if (current) await chrome.storage.local.set({ _bulkJob: { ...current, running: false } });
  return { ok: true };
}

async function bulkStep() {
  const job = await readJob();
  if (!job?.running) return;

  if (job.index >= job.dates.length) {
    await chrome.storage.local.set({ _bulkJob: { ...job, running: false, done: true } });
    return;
  }

  const date = job.dates[job.index];
  await chrome.storage.local.set({ _bulkJob: { ...job, currentDate: date } });

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
    await applyJobUpdate(job.runId, j => ({ ...j, currentDate: date, waitingForTab: true }));
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
    await applyJobUpdate(job.runId, j => ({
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

  const next = await applyJobUpdate(job.runId, j => {
    const u = { ...j, waitingForTab: false };
    if (outcome === 'session') {
      u.running = false;
      u.fatalError = 'Session has expired. Please log in again and Resume.';
      return u;
    }
    u[outcome]++;
    u.index++;
    if (u.index >= u.dates.length) { u.running = false; u.done = true; }
    return u;
  });

  if (next?.running) chrome.alarms.create(BULK_ALARM, { when: Date.now() + 300 });
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
// AND isn't already covered (parquet rulings or raw scrape file). Coverage
// merges parquet court_dates with raw filenames, which keeps the hotkey from
// jumping back into 2017-2024 (the Excel-imported range with no raw files).
async function nextUnscannedBusinessDay({ after, until, token, owner, repo, branch, department }) {
  let covered = new Set();
  try {
    const dates = await getCoverage(token, owner, repo, branch, department);
    covered = new Set(dates);
  } catch {
    // If coverage fails, fall back to raw listing.
    try {
      const files = await getDeptDir(token, owner, repo, branch, department);
      covered = new Set(files.map(f => f.name?.slice(0, 10)).filter(Boolean));
    } catch { /* fall through to plain next-business-day */ }
  }
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

