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
    'inflight-dates':  () => listInFlightDates(msg.payload?.department),
  };
  const handler = handlers[msg.action];
  if (!handler) return false;
  Promise.resolve().then(handler).then(respond).catch(err => respond({ error: err.message }));
  return true;
});

// Drop a tab's job state when the tab itself goes away — otherwise we'd
// accumulate stale per-tab entries in storage forever. Also release any
// in-flight date claims this tab held so sibling tabs aren't permanently
// locked out of those dates.
chrome.tabs.onRemoved.addListener(async (tabId) => {
  await chrome.alarms.clear(bulkAlarmName(tabId));
  const all = await readAllJobs();
  if (all[tabId]) {
    delete all[tabId];
    await chrome.storage.local.set({ _bulkJobs: all });
  }
  await releaseAllForTab(tabId);
});

// ── Cross-tab in-flight date claims ───────────────────────────────────────────
// Two SFTC tabs running bulk scrapes against the same department would
// otherwise both decide the same date is "unscanned" (the duplicate guard
// only sees commits that have already landed on GitHub) and pay the SFTC
// round-trip twice — or worse, both buffer the same date and produce two
// raw files for it. We keep a per-department `_inFlightClaims` map of
// date → { tabId, claimedAt } so each date is owned by at most one tab
// at a time.
//
// Schema: { [dept]: { [date]: { tabId, claimedAt } } }
//
// Stale entries (older than INFLIGHT_TTL_MS) are treated as released — a
// tab that crashed mid-scrape shouldn't strand its dates forever. The TTL
// is generous because a single bulk run with frequent CAPTCHAs can keep
// a date claimed for many minutes.

const INFLIGHT_TTL_MS = 30 * 60 * 1000;

// Single-promise mutex serialises all claim get-modify-set sequences in
// the service worker. Without it, two parallel bulkStep calls could both
// read the same "unclaimed" map and both commit a claim, breaking the
// invariant.
let _claimMutex = Promise.resolve();
function withClaimLock(fn) {
  const next = _claimMutex.then(fn, fn);
  _claimMutex = next.catch(() => {});
  return next;
}

function _now() { return Date.now(); }

async function _readClaims() {
  const { _inFlightClaims = {} } = await chrome.storage.local.get('_inFlightClaims');
  return (_inFlightClaims && typeof _inFlightClaims === 'object') ? _inFlightClaims : {};
}

async function _writeClaims(claims) {
  await chrome.storage.local.set({ _inFlightClaims: claims });
}

function _isStale(entry) {
  return !entry || !entry.claimedAt || (_now() - entry.claimedAt) > INFLIGHT_TTL_MS;
}

// Try to claim a date for `tabId` in `department`. Returns true on success,
// false if some other tab currently owns it.
async function claimDate(department, date, tabId) {
  if (!department || !date || tabId == null) return true;
  return withClaimLock(async () => {
    const claims = await _readClaims();
    const deptMap = claims[department] || {};
    const existing = deptMap[date];
    if (existing && existing.tabId !== tabId && !_isStale(existing)) return false;
    deptMap[date] = { tabId, claimedAt: _now() };
    claims[department] = deptMap;
    await _writeClaims(claims);
    return true;
  });
}

async function releaseDate(department, date, tabId) {
  if (!department || !date || tabId == null) return;
  return withClaimLock(async () => {
    const claims = await _readClaims();
    const deptMap = claims[department];
    if (!deptMap) return;
    const entry = deptMap[date];
    if (!entry || entry.tabId !== tabId) return;
    delete deptMap[date];
    if (Object.keys(deptMap).length === 0) delete claims[department];
    await _writeClaims(claims);
  });
}

async function releaseAllForTab(tabId) {
  if (tabId == null) return;
  return withClaimLock(async () => {
    const claims = await _readClaims();
    let changed = false;
    for (const dept of Object.keys(claims)) {
      const map = claims[dept];
      for (const date of Object.keys(map)) {
        if (map[date].tabId === tabId || _isStale(map[date])) {
          delete map[date];
          changed = true;
        }
      }
      if (Object.keys(map).length === 0) delete claims[dept];
    }
    if (changed) await _writeClaims(claims);
  });
}

// Returns the set of dates currently claimed by tabs OTHER than `excludeTabId`
// in `department`. Used by startBulk to pre-filter dates so two tabs never
// race to scrape the same date.
async function getClaimsByOtherTabs(department, excludeTabId) {
  if (!department) return new Set();
  const claims = await _readClaims();
  const deptMap = claims[department] || {};
  const out = new Set();
  for (const date of Object.keys(deptMap)) {
    const entry = deptMap[date];
    if (entry.tabId !== excludeTabId && !_isStale(entry)) out.add(date);
  }
  return out;
}

// Surface the current claim map (post-staleness-filter) so the popup can
// show which dates a sibling tab is working on — useful when the user is
// curious why "Skip already scanned" produced a smaller list than expected.
async function listInFlightDates(department) {
  const claims = await _readClaims();
  if (department) {
    const deptMap = claims[department] || {};
    const out = {};
    for (const [date, entry] of Object.entries(deptMap)) {
      if (!_isStale(entry)) out[date] = entry;
    }
    return out;
  }
  const out = {};
  for (const [dept, map] of Object.entries(claims)) {
    out[dept] = {};
    for (const [date, entry] of Object.entries(map)) {
      if (!_isStale(entry)) out[dept][date] = entry;
    }
  }
  return out;
}

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

// Apply the stale-page guard, build the JSON path, and check for an existing
// duplicate file in a department. Returns { duplicate, path, content, message,
// staleCount } where `content` is the b64 payload to PUT and `message` is the
// suggested commit message — or { duplicate: true, path } when the date is
// already covered. Used by both the single-file commit (commitToGitHub) and
// the batched tree commit (commitBatchToGitHub).
async function preparePayload({ token, owner, repo, branch, data }) {
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
  let staleDate = null;
  if (rulings?.length) {
    const wrongDate = rulings.find(r => {
      const iso = parseCourtDateISO(r['Court Date'] || '');
      return iso && iso !== date;
    });
    if (wrongDate) {
      staleCount = rulings.length;
      staleDate = parseCourtDateISO(wrongDate['Court Date']);
      rulings = [];
      data = { ...data, rulings: [], _stale_dropped: staleCount,
               _stale_court_date: staleDate };
    }
  }

  const time = new Date(scraped_at).toISOString().slice(11, 19).replace(/:/g, '');
  const path = `raw/dept${department}/${date}-${time}.json`;

  const files = await getDeptDir(token, owner, repo, branch, department);
  if (files.some(f => f.name.startsWith(`${date}-`))) return { duplicate: true, path, date, department };

  const content = btoa(unescape(encodeURIComponent(JSON.stringify(data, null, 2))));
  const message = staleCount
    ? `Mark ${date} (Dept ${department}) — 0 rulings (page returned stale ${staleDate} data)`
    : `Add ${(rulings || []).length} rulings for ${date} (Dept ${department})`;
  return {
    duplicate: false, path, date, department, content, message,
    staleCount, staleDate, rulings: rulings || [],
  };
}

function recordCommittedPath({ department, date, path }) {
  // Prime the dept-dir cache and the coverage cache so the next duplicate
  // check + the next hotkey advance both see this date as covered without
  // a round-trip.
  const cached = _dirCache.get(department);
  const fileName = path.split('/').pop();
  if (cached && fileName) cached.files.push({ name: fileName });
  for (const [key, entry] of _covCache) {
    if (key.startsWith(`${department}|`) && !entry.dates.includes(date)) {
      entry.dates.push(date);
    }
  }
  trackLocalCommit(department, date).catch(() => {});
}

async function commitToGitHub({ token, owner, repo, branch, data }) {
  const prep = await preparePayload({ token, owner, repo, branch, data });
  if (prep.duplicate) return { duplicate: true, path: prep.path };

  const res = await fetch(`https://api.github.com/repos/${owner}/${repo}/contents/${prep.path}`, {
    method: 'PUT',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
      'X-GitHub-Api-Version': '2022-11-28',
    },
    body: JSON.stringify({ message: prep.message, content: prep.content, branch }),
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.message || `GitHub API error ${res.status}`);
  }

  recordCommittedPath({ department: prep.department, date: prep.date, path: prep.path });

  const json = await res.json();
  return {
    ok: true, path: prep.path, sha: json.content?.sha,
    stale: !!prep.staleCount, staleCount: prep.staleCount,
  };
}

// ── Batched tree commit (used by bulk runs) ───────────────────────────────────
// One git commit covering every buffered scrape — much cheaper than N pushes
// and yields a single ingest-workflow run instead of N (most of which would
// have been throttle no-ops anyway). Falls back to per-file PUTs if the
// caller passes a single item, since that path's response shape is what the
// existing single-page Send button expects.
async function commitBatchToGitHub({ token, owner, repo, branch, items }) {
  // 1. Prepare each payload (stale-guard, duplicate check, content/path).
  //    A duplicate-only batch is a clean no-op.
  const prepared = [];
  let duplicates = 0;
  let staleCount = 0;
  for (const it of items) {
    const prep = await preparePayload({ token, owner, repo, branch, data: it });
    if (prep.duplicate) { duplicates++; continue; }
    prepared.push(prep);
    if (prep.staleCount) staleCount++;
  }
  if (!prepared.length) return { committed: 0, duplicates, staleCount, sha: null };

  const headers = {
    Authorization: `Bearer ${token}`,
    'Content-Type': 'application/json',
    'X-GitHub-Api-Version': '2022-11-28',
  };

  // Upload each blob exactly once — the b64 content doesn't change between
  // attempts, so we can reuse blob SHAs even if we have to rebuild the tree
  // on top of a newer head SHA.
  const treeEntries = [];
  for (const p of prepared) {
    const blobRes = await fetch(`https://api.github.com/repos/${owner}/${repo}/git/blobs`, {
      method: 'POST', headers,
      body: JSON.stringify({ content: p.content, encoding: 'base64' }),
    });
    if (!blobRes.ok) {
      const body = await blobRes.json().catch(() => ({}));
      throw new Error(body.message || `blob create failed for ${p.path}: ${blobRes.status}`);
    }
    treeEntries.push({
      path: p.path, mode: '100644', type: 'blob',
      sha: (await blobRes.json()).sha,
    });
  }

  // Commit message summarises the batch — first 5 paths spelled out, rest
  // collapsed. This is what shows up in the GitHub history, so make it
  // self-explanatory.
  const totalRulings = prepared.reduce((n, p) => n + (p.rulings?.length || 0), 0);
  const datesArr = prepared.map(p => p.date);
  const minDate = datesArr.reduce((a, b) => a < b ? a : b);
  const maxDate = datesArr.reduce((a, b) => a > b ? a : b);
  const depts = [...new Set(prepared.map(p => p.department))].sort();
  const subject = `Bulk-add ${prepared.length} day(s), ${totalRulings} ruling(s) — ${minDate}..${maxDate} (Dept ${depts.join(', ')})`;
  const bodyText = prepared.map(p => `  • ${p.message}`).join('\n');
  const commitMsg = `${subject}\n\n${bodyText}`;

  // The ref-update step fails with 422 whenever another commit landed on
  // `branch` between our head fetch and our ref PATCH. With multiple SFTC
  // tabs (or the GitHub Actions ingest bot) committing concurrently this
  // is routine — without retry the entire 25-item batch was silently
  // converted to "errors" and the user had to re-scrape the whole window.
  // Refetch head, rebuild the tree on top of the new base, and try again.
  const MAX_REF_ATTEMPTS = 5;
  let newCommitSha = null;
  let lastError = null;
  for (let attempt = 1; attempt <= MAX_REF_ATTEMPTS; attempt++) {
    // 2. Resolve the head commit + base tree of `branch`.
    const refRes = await fetch(
      `https://api.github.com/repos/${owner}/${repo}/git/ref/heads/${encodeURIComponent(branch)}`,
      { headers });
    if (!refRes.ok) {
      const body = await refRes.json().catch(() => ({}));
      throw new Error(body.message || `git ref fetch failed: ${refRes.status}`);
    }
    const headSha = (await refRes.json()).object.sha;
    const headCommitRes = await fetch(
      `https://api.github.com/repos/${owner}/${repo}/git/commits/${headSha}`,
      { headers });
    if (!headCommitRes.ok) {
      const body = await headCommitRes.json().catch(() => ({}));
      throw new Error(body.message || `git commit fetch failed: ${headCommitRes.status}`);
    }
    const baseTreeSha = (await headCommitRes.json()).tree.sha;

    // 3. Build a tree on top of the current head, then a commit pointing at it.
    const treeRes = await fetch(`https://api.github.com/repos/${owner}/${repo}/git/trees`, {
      method: 'POST', headers,
      body: JSON.stringify({ base_tree: baseTreeSha, tree: treeEntries }),
    });
    if (!treeRes.ok) {
      const body = await treeRes.json().catch(() => ({}));
      throw new Error(body.message || `git tree create failed: ${treeRes.status}`);
    }
    const treeSha = (await treeRes.json()).sha;

    const commitRes = await fetch(`https://api.github.com/repos/${owner}/${repo}/git/commits`, {
      method: 'POST', headers,
      body: JSON.stringify({ message: commitMsg, tree: treeSha, parents: [headSha] }),
    });
    if (!commitRes.ok) {
      const body = await commitRes.json().catch(() => ({}));
      throw new Error(body.message || `git commit create failed: ${commitRes.status}`);
    }
    const candidateSha = (await commitRes.json()).sha;

    // 4. Fast-forward the branch ref. 422 means a parallel commit landed
    //    after our head fetch — retry from step 2 against the new head.
    const updateRes = await fetch(
      `https://api.github.com/repos/${owner}/${repo}/git/refs/heads/${encodeURIComponent(branch)}`,
      {
        method: 'PATCH', headers,
        body: JSON.stringify({ sha: candidateSha, force: false }),
      });
    if (updateRes.ok) {
      newCommitSha = candidateSha;
      break;
    }
    const errBody = await updateRes.json().catch(() => ({}));
    lastError = errBody.message || `git ref update failed: ${updateRes.status}`;
    // Only retry on 422 (ref conflict) — other errors are surfaced.
    if (updateRes.status !== 422 || attempt === MAX_REF_ATTEMPTS) {
      throw new Error(lastError);
    }
    // Brief backoff with jitter so two retrying tabs don't lock-step.
    await new Promise(r => setTimeout(r, 200 * attempt + Math.random() * 200));
  }

  // Prime caches so the next duplicate check sees these dates as covered.
  for (const p of prepared) {
    recordCommittedPath({ department: p.department, date: p.date, path: p.path });
  }

  return {
    committed: prepared.length, duplicates, staleCount,
    sha: newCommitSha,
  };
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
//   pendingBuffer,               // [{ date, data }] — scraped pages awaiting
//                                // a tree-API batch commit. Flushed every
//                                // BULK_FLUSH_EVERY successful scrapes and
//                                // on every stop transition (done, paused,
//                                // user-stop, fatalError) so we don't lose
//                                // work if the run is interrupted.
//   waitingForTab,               // true while a navigation is in flight
//   pausedForSession,            // true when SFTC returned the "session
//                                // expired" page OR a Cloudflare CAPTCHA.
//                                // The tab is auto-reloaded so the user
//                                // can solve the challenge; Resume picks up
//                                // at the same date (index NOT advanced).
//   pauseReason,                 // 'session' | 'captcha' — drives the popup's
//                                // status text. Kept distinct so the user
//                                // knows what they're being asked to solve.
// }

// How many completed scrapes to accumulate before flushing a tree commit.
// Smaller = fewer dates lost on a crash; larger = fewer commits / workflow
// runs. 25 is a comfortable balance for typical bulk scans (~150 dates).
const BULK_FLUSH_EVERY = 25;

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

// Same as applyJobUpdate but doesn't require running===true; used by stop /
// pause / done flushes that mutate the job AFTER it has been marked stopped.
async function applyJobUpdateAny(tabId, mutator) {
  const current = await readJob(tabId);
  if (!current) return null;
  const next = mutator({ ...current });
  await writeJob(tabId, next);
  return next;
}

async function startBulk({ dates, tabId, settings, waitMs, department }) {
  if (tabId == null) return { error: 'No tabId on start-bulk request.' };
  await chrome.alarms.clear(bulkAlarmName(tabId));

  // We DON'T pre-filter the date list against in-flight claims from other
  // tabs here, even though that would save SFTC round-trips. The pre-filter
  // was producing a confusing UX: a second tab launched right after the
  // first would arrive with 0 dates and immediately report "Done: 0
  // committed" — looking like "all dates already scanned" when in fact
  // the sibling tab was still working through them. The per-step claim
  // in bulkStep already rejects duplicates correctly: a sibling-claimed
  // date is counted as "skipped" and the run advances in milliseconds,
  // so the second tab quickly reaches dates the first tab hasn't touched.
  const job = {
    runId: Date.now(),
    running: true, done: false, fatalError: null,
    dates, index: 0, currentDate: null,
    tabId, settings, waitMs: waitMs || 5000,
    department: department || null,
    committed: 0, skipped: 0, errors: 0,
    pendingBuffer: [],
    waitingForTab: false,
    pausedForSession: false,
    pauseReason: null,
  };
  await writeJob(tabId, job);
  bulkStep(tabId);
  return { ok: true };
}

async function stopBulk({ tabId } = {}) {
  if (tabId == null) return { error: 'No tabId on stop-bulk request.' };
  await chrome.alarms.clear(bulkAlarmName(tabId));
  const current = await readJob(tabId);
  if (current) {
    await writeJob(tabId, { ...current, running: false, pausedForSession: false, pauseReason: null });
    // Flush whatever's been scraped before the user pulled the brake — same
    // contract as completion / pause: every stop produces a commit. The
    // flush itself releases the in-flight claims for the buffered dates.
    // Anything scraped-but-not-yet-buffered is dropped here too.
    flushBulkBuffer(tabId, 'stop').catch(() => {});
    releaseAllForTab(tabId).catch(() => {});
  }
  return { ok: true };
}

// Resume after an auto-pause (session expiry or CAPTCHA, then user solved
// the challenge). Picks up at the same date that triggered the pause —
// the index is intentionally NOT advanced. Bumps runId so any in-flight
// callbacks from the prior batch are discarded.
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
    pauseReason: null,
  };
  await writeJob(tabId, next);
  bulkStep(tabId);
  return { ok: true };
}

// Hand the buffered scrapes to the tree-API batch commit. Always clears the
// buffer (and decrements counts on rollback failure) so the popup's running
// totals stay aligned with what's actually on GitHub.
//
// `reason` is one of: 'threshold' | 'done' | 'pause' | 'stop' | 'fatal'.
// Threshold flushes happen mid-run; the rest happen exactly once per stop
// transition.
//
// If the batch tree-commit fails (the 25-at-a-time path), we DON'T just
// throw the work away — we fall back to per-file PUTs so the data we
// already paid the SFTC round-trip for actually lands on GitHub. The
// per-file path is slower but reliable: each PUT is its own atomic API
// call with its own ref-update, so a failure on one file doesn't sink
// the others.
async function flushBulkBuffer(tabId, reason) {
  const job = await readJob(tabId);
  if (!job || !job.pendingBuffer?.length) return;
  const items = job.pendingBuffer;
  const dept = job.department;
  // Clear the buffer up front so a re-entrant flush (e.g. stop racing with
  // a threshold flush) doesn't double-commit.
  await applyJobUpdateAny(tabId, j => ({ ...j, pendingBuffer: [] }));

  const { token, repo, branch } = job.settings;
  const [owner, repoName] = repo.split('/');

  let batchOk = false;
  let batchErr = null;
  try {
    await commitBatchToGitHub({
      token, owner, repo: repoName, branch,
      items: items.map(({ date, data }) => ({ ...data, _date: date })),
    });
    batchOk = true;
  } catch (err) {
    batchErr = err;
  }

  if (!batchOk) {
    // Batch commit failed. Fall back to per-file PUTs so we don't lose
    // 25 scrapes' worth of SFTC round-trips. Each PUT is independent,
    // so a single bad payload (corrupt JSON, weird filename collision)
    // doesn't take down the rest.
    let salvagedOk = 0;
    let salvagedErr = 0;
    let lastErr = batchErr?.message || 'unknown batch error';
    for (const { date, data } of items) {
      try {
        const r = await commitToGitHub({
          token, owner, repo: repoName, branch,
          data: { ...data, _date: date },
        });
        if (r?.ok || r?.duplicate) salvagedOk++;
        else salvagedErr++;
      } catch (e) {
        salvagedErr++;
        lastErr = e.message;
      }
    }
    await applyJobUpdateAny(tabId, j => {
      const u = { ...j };
      // Re-baseline the optimistic increment from bulkHandleResult, then
      // add what actually landed on GitHub.
      u.committed = Math.max(0, (u.committed || 0) - items.length) + salvagedOk;
      u.errors = (u.errors || 0) + salvagedErr;
      if (salvagedErr > 0) {
        u.lastFlushError = `Batch ${reason} failed; salvaged ${salvagedOk}/${items.length} via per-file PUT, ${salvagedErr} lost: ${lastErr}`;
      } else if (salvagedOk > 0) {
        u.lastFlushError = `Batch ${reason} failed; recovered all ${salvagedOk} via per-file PUT.`;
      } else {
        u.lastFlushError = `Batch ${reason} failed: ${lastErr}`;
      }
      return u;
    });
  }

  // Release the in-flight claim on every date in this flush, regardless of
  // outcome. Either the date is now on GitHub (claim no longer meaningful,
  // duplicate guard takes over) or it failed (sibling tabs / re-runs are
  // welcome to retry it).
  if (dept) {
    for (const { date } of items) {
      releaseDate(dept, date, tabId).catch(() => {});
    }
  }
}

async function bulkStep(tabId) {
  const job = await readJob(tabId);
  if (!job?.running) return;

  if (job.index >= job.dates.length) {
    await writeJob(tabId, { ...job, running: false, done: true });
    await flushBulkBuffer(tabId, 'done');
    releaseAllForTab(tabId).catch(() => {});
    return;
  }

  const date = job.dates[job.index];
  await writeJob(tabId, { ...job, currentDate: date });

  // Claim the date for this tab. If a sibling tab is already on it (started
  // a tick earlier, or holds it from a previous run that never released),
  // skip and advance — counting it as a skip rather than burning a SFTC
  // round-trip and producing a duplicate raw file.
  if (job.department) {
    const got = await claimDate(job.department, date, job.tabId);
    if (!got) {
      const next = await applyJobUpdate(tabId, job.runId, j => {
        const u = { ...j };
        u.skipped++;
        u.index++;
        if (u.index >= u.dates.length) { u.running = false; u.done = true; }
        return u;
      });
      if (next?.done) {
        await flushBulkBuffer(tabId, 'done');
        releaseAllForTab(tabId).catch(() => {});
        return;
      }
      // Schedule the next step immediately — no SFTC round-trip happened.
      if (next?.running) chrome.alarms.create(bulkAlarmName(tabId), { when: Date.now() + 50 });
      return;
    }
  }

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
    // Flush whatever made it into the buffer before the tab vanished. Best
    // effort — we lose the in-progress run but at least keep the dates that
    // already succeeded.
    flushBulkBuffer(job.tabId, 'fatal').catch(() => {});
    releaseAllForTab(job.tabId).catch(() => {});
    return false;
  }
}

async function bulkHandleResult(job, date, data) {
  // Three terminal states:
  //   • session / captcha → pause and reload tab; user solves and resumes.
  //   • error / pending   → count an error, advance.
  //   • good scrape       → buffer for tree-commit.
  let outcome = 'errors';
  let pauseReason = null;
  if (data?.captchaChallenge) {
    outcome = 'session';
    pauseReason = 'captcha';
  } else if (data?.sessionExpired) {
    outcome = 'session';
    pauseReason = 'session';
  } else if (data && !data.error && !data.pending) {
    // Buffer the scrape; commit happens at flush time. We optimistically
    // count it as "committed" — if the tree commit fails, flushBulkBuffer
    // falls back to per-file PUTs and reconciles the count to whatever
    // actually landed on GitHub. That keeps the running display useful
    // (fast feedback per date) without blocking on the network.
    outcome = 'committed';
  }

  // If we just learned the department from a successful scrape and the job
  // didn't have one yet, persist it on the job so subsequent bulkSteps can
  // claim against the right dept map.
  const learnedDept = (outcome === 'committed' && data?.department) ? String(data.department) : null;

  const next = await applyJobUpdate(job.tabId, job.runId, j => {
    const u = { ...j, waitingForTab: false };
    if (learnedDept && !u.department) u.department = learnedDept;
    if (outcome === 'session') {
      // Auto-pause: stop the run, leave `index` unchanged so Resume
      // re-tries the same date, and reload the SFTC tab so the user
      // hits the Cloudflare CAPTCHA. The page reload happens out-of-band
      // below so storage state is settled before the tab nav fires.
      u.running = false;
      u.pausedForSession = true;
      u.pauseReason = pauseReason;
      return u;
    }
    if (outcome === 'committed') {
      u.pendingBuffer = [...(u.pendingBuffer || []), { date, data }];
    }
    u[outcome]++;
    u.index++;
    if (u.index >= u.dates.length) {
      u.running = false; u.done = true;
    }
    return u;
  });

  // Release the in-flight claim for non-buffered outcomes. Committed dates
  // stay claimed until flushBulkBuffer commits them (or rolls them back).
  // Session pauses release the claim too — the user may take a while to
  // solve the CAPTCHA, and we don't want to lock other tabs out of that
  // date in the meantime; the resumeBulk path will re-claim on the next
  // bulkStep against the same date.
  const dept = next?.department || job.department || data?.department || null;
  if (dept && outcome !== 'committed') {
    releaseDate(dept, date, job.tabId).catch(() => {});
  }

  if (outcome === 'session') {
    // Flush the buffer before the user touches the tab — keeps everything
    // we've collected safe even if the user closes the popup or the tab
    // before solving the CAPTCHA.
    await flushBulkBuffer(job.tabId, 'pause');
    // Fire-and-forget: tell the content script to reload the page. We don't
    // await it — the reload tears down the message channel, and the popup's
    // status listener already shows the paused-for-session prompt.
    chrome.tabs.sendMessage(job.tabId, { action: 'restart-session' }).catch(() => {});
    return;
  }

  // Threshold flush: every BULK_FLUSH_EVERY *successful* scrapes, push a
  // tree commit so we never have more than that many at risk.
  if (next && (next.committed || 0) > 0
      && (next.committed % BULK_FLUSH_EVERY) === 0
      && (next.pendingBuffer?.length || 0) > 0) {
    await flushBulkBuffer(job.tabId, 'threshold');
  }

  if (next?.done) {
    await flushBulkBuffer(job.tabId, 'done');
    releaseAllForTab(job.tabId).catch(() => {});
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
  if (data.captchaChallenge) {
    toast('SFSC: Cloudflare CAPTCHA — solve it in the SFTC tab and try again', 'error');
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

