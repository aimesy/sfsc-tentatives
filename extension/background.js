// Service worker: GitHub commits + background bulk scraping.

// ── Message router ────────────────────────────────────────────────────────────

chrome.runtime.onMessage.addListener((msg, _sender, respond) => {
  if (msg.action === 'commit') {
    commitToGitHub(msg.payload).then(respond).catch(err => respond({ error: err.message }));
    return true;
  }
  if (msg.action === 'check-updates') {
    checkForUpdates().then(respond).catch(err => respond({ error: err.message }));
    return true;
  }
  if (msg.action === 'download-update') {
    downloadUpdate().then(respond).catch(err => respond({ error: err.message }));
    return true;
  }
  if (msg.action === 'start-bulk') {
    startBulk(msg.payload).then(respond).catch(err => respond({ error: err.message }));
    return true;
  }
  if (msg.action === 'stop-bulk') {
    stopBulk().then(respond);
    return true;
  }
  if (msg.action === 'bulk-status') {
    chrome.storage.local.get(['_bulkJob'], r => respond(r._bulkJob || null));
    return true;
  }
});

// ── Update checking ───────────────────────────────────────────────────────────

async function checkForUpdates() {
  const owner = 'aimesy', repo = 'sfsc-tentatives';
  const res = await fetch(
    `https://api.github.com/repos/${owner}/${repo}/commits?per_page=1`,
    { headers: { 'X-GitHub-Api-Version': '2022-11-28' } }
  );
  if (!res.ok) return { hasUpdate: false };
  const [commit] = await res.json();
  const latestSha = commit.sha;
  const stored = await new Promise(r => chrome.storage.local.get(['_lastCommitSha'], r));
  const lastSha = stored._lastCommitSha;
  if (latestSha !== lastSha) {
    await new Promise(r => chrome.storage.local.set({ _lastCommitSha: latestSha }, r));
    return { hasUpdate: true, latestSha };
  }
  return { hasUpdate: false };
}

async function downloadUpdate() {
  try {
    const url = 'https://raw.githubusercontent.com/aimesy/sfsc-tentatives/master/sfsc-extension.zip';
    const downloadId = await new Promise((resolve, reject) => {
      chrome.downloads.download({ url, filename: 'sfsc-extension.zip', saveAs: false }, id => {
        if (id !== undefined) resolve(id);
        else reject(new Error('Download failed'));
      });
    });
    return { success: true, downloadId };
  } catch (err) {
    return { success: false, error: err.message };
  }
}

// ── Duplicate detection ───────────────────────────────────────────────────────

const _dirCache = new Map(); // dept -> { files, time }

async function getDeptDir(token, owner, repo, branch, department) {
  const cached = _dirCache.get(department);
  if (cached && Date.now() - cached.time < 60_000) return cached.files;
  const res = await fetch(
    `https://api.github.com/repos/${owner}/${repo}/contents/raw/dept${department}?ref=${branch}`,
    { headers: { Authorization: `Bearer ${token}`, 'X-GitHub-Api-Version': '2022-11-28' } }
  );
  const files = res.ok ? (await res.json().catch(() => [])) : [];
  _dirCache.set(department, { files: Array.isArray(files) ? files : [], time: Date.now() });
  return _dirCache.get(department).files;
}

async function isDuplicate(token, owner, repo, branch, date, department) {
  const files = await getDeptDir(token, owner, repo, branch, department);
  return files.some(f => f.name.startsWith(`${date}-`));
}

// ── Commit ────────────────────────────────────────────────────────────────────

function courtDate(rulings) {
  const raw = rulings?.[0]?.['Court Date'] ?? '';
  const m = raw.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
  if (m) return `${m[3]}-${m[1].padStart(2, '0')}-${m[2].padStart(2, '0')}`;
  if (/^\d{4}-\d{2}-\d{2}/.test(raw)) return raw.slice(0, 10);
  return new Date().toISOString().slice(0, 10);
}

async function commitToGitHub({ token, owner, repo, branch, data }) {
  const { department, scraped_at, rulings } = data;
  const date = courtDate(rulings);
  const time = new Date(scraped_at).toISOString().slice(11, 19).replace(/:/g, '');
  const path = `raw/dept${department}/${date}-${time}.json`;

  if (await isDuplicate(token, owner, repo, branch, date, department)) {
    return { duplicate: true, path };
  }

  const content = btoa(unescape(encodeURIComponent(JSON.stringify(data, null, 2))));
  const res = await fetch(`https://api.github.com/repos/${owner}/${repo}/contents/${path}`, {
    method: 'PUT',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
      'X-GitHub-Api-Version': '2022-11-28',
    },
    body: JSON.stringify({
      message: `Add ${rulings.length} rulings for ${date} (Dept ${department})`,
      content,
      branch,
    }),
  });

  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.message || `GitHub API error ${res.status}`);
  }

  // Append to cache instead of invalidating — avoids a re-fetch on next duplicate check
  const cached = _dirCache.get(department);
  if (cached) cached.files.push({ name: `${date}-${time}.json` });

  const json = await res.json();
  return { ok: true, path, sha: json.content?.sha };
}

// ── Background bulk scraping ──────────────────────────────────────────────────
// State is kept in chrome.storage.local so it survives popup close.
// Schema: { running, done, dates[], index, tabId, settings, waitMs,
//           committed, skipped, errors, currentDate, waitingForTab, fatalError }

const BULK_ALARM = 'sfsc-bulk-next';

chrome.alarms.onAlarm.addListener(alarm => {
  if (alarm.name === BULK_ALARM) bulkStep();
});

// When the SFTC tab finishes loading (after a form-submit navigation),
// resume scraping without waiting for an alarm.
chrome.tabs.onUpdated.addListener(async (tabId, info) => {
  if (info.status !== 'complete') return;
  const { _bulkJob: job } = await chrome.storage.local.get('_bulkJob');
  if (!job?.running || !job.waitingForTab || job.tabId !== tabId) return;
  bulkScrapeAfterLoad(job);
});

async function startBulk({ dates, tabId, settings, waitMs }) {
  chrome.alarms.clear(BULK_ALARM);
  const job = {
    running: true, done: false,
    dates, index: 0, tabId, settings, waitMs: waitMs || 1000,
    committed: 0, skipped: 0, errors: 0,
    currentDate: null, waitingForTab: false, fatalError: null,
  };
  await chrome.storage.local.set({ _bulkJob: job });
  bulkStep();
  return { ok: true };
}

async function stopBulk() {
  chrome.alarms.clear(BULK_ALARM);
  const { _bulkJob } = await chrome.storage.local.get('_bulkJob');
  if (_bulkJob) {
    await chrome.storage.local.set({ _bulkJob: { ..._bulkJob, running: false } });
  }
  return { ok: true };
}

async function bulkStep() {
  const { _bulkJob: job } = await chrome.storage.local.get('_bulkJob');
  if (!job?.running) return;

  if (job.index >= job.dates.length) {
    await chrome.storage.local.set({ _bulkJob: { ...job, running: false, done: true } });
    return;
  }

  const date = job.dates[job.index];
  await chrome.storage.local.set({ _bulkJob: { ...job, currentDate: date } });

  // Inject content script — if tab is gone, abort
  try {
    await chrome.scripting.executeScript({ target: { tabId: job.tabId }, files: ['content.js'] });
  } catch {
    await chrome.storage.local.set({
      _bulkJob: { ...job, running: false, fatalError: 'SFTC tab was closed. Reopen it and Resume.' }
    });
    return;
  }

  // Send fill-and-scrape; if lastError the page is navigating
  const result = await new Promise(resolve => {
    chrome.tabs.sendMessage(job.tabId, { action: 'fill-and-scrape', date, waitMs: job.waitMs }, r =>
      resolve(chrome.runtime.lastError ? { navigated: true } : r)
    );
  });

  if (result?.navigated) {
    // onUpdated will fire when the page finishes loading
    await chrome.storage.local.set({ _bulkJob: { ...job, currentDate: date, waitingForTab: true } });
    return;
  }

  await bulkHandleResult({ ...job, currentDate: date }, result);
}

async function bulkScrapeAfterLoad(job) {
  // Inject content script and scrape the freshly-loaded page
  try {
    await chrome.scripting.executeScript({ target: { tabId: job.tabId }, files: ['content.js'] });
  } catch {
    await chrome.storage.local.set({
      _bulkJob: { ...job, running: false, waitingForTab: false,
                  fatalError: 'SFTC tab was closed. Reopen it and Resume.' }
    });
    return;
  }

  await new Promise(r => setTimeout(r, 200));

  let data = await new Promise(resolve =>
    chrome.tabs.sendMessage(job.tabId, { action: 'scrape' }, r =>
      resolve(chrome.runtime.lastError ? null : r)
    )
  );

  // One retry if content script wasn't ready yet
  if (!data || data.error) {
    await new Promise(r => setTimeout(r, 400));
    data = await new Promise(resolve =>
      chrome.tabs.sendMessage(job.tabId, { action: 'scrape' }, r =>
        resolve(chrome.runtime.lastError ? null : r)
      )
    );
  }

  await bulkHandleResult({ ...job, waitingForTab: false }, data);
}

async function bulkHandleResult(job, data) {
  const update = { ...job, waitingForTab: false };

  if (data?.sessionExpired) {
    update.running = false;
    update.fatalError = 'Session has expired. Please log in again and Resume.';
    await chrome.storage.local.set({ _bulkJob: update });
    return;
  }

  if (!data || data.error) {
    update.errors++;
  } else if (!data.rulings?.length) {
    update.skipped++;
  } else {
    try {
      const { token, repo, branch } = job.settings;
      const [owner, repoName] = repo.split('/');
      const res = await commitToGitHub({ token, owner, repo: repoName, branch, data });
      if (res.error)          update.errors++;
      else if (res.duplicate) update.skipped++;
      else                    update.committed++;
    } catch {
      update.errors++;
    }
  }

  update.index++;

  if (update.index >= update.dates.length) {
    update.running = false;
    update.done    = true;
    await chrome.storage.local.set({ _bulkJob: update });
    return;
  }

  await chrome.storage.local.set({ _bulkJob: update });
  chrome.alarms.create(BULK_ALARM, { when: Date.now() + 300 });
}
