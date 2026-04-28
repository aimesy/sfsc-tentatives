// popup.js — runs in the extension popup.

const $ = id => document.getElementById(id);
let scrapedData = null;
let bulkRunning = false;

// ── Helpers ───────────────────────────────────────────────────────────────────

function setStatus(msg, type = '') {
  const el = $('status');
  el.className = 'status' + (type ? ' ' + type : '');
  el.innerHTML = type === 'loading'
    ? `<div class="spinner"></div><span>${msg}</span>`
    : `<span>${msg}</span>`;
}

function loadSettings() {
  return new Promise(resolve =>
    chrome.storage.local.get(['token', 'repo', 'branch'], resolve)
  );
}

function validateSettings(s) {
  if (!s.token) return 'GitHub token not set — open Settings below.';
  if (!s.repo || !s.repo.includes('/')) return 'Repository not set — open Settings below.';
  return null;
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// Accepts YYYY-MM-DD, MM/DD/YYYY, M/D/YYYY, MM-DD-YYYY → returns YYYY-MM-DD or null
function parseDate(str) {
  if (!str) return null;
  str = str.trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(str)) return str;
  const slash = str.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (slash) return `${slash[3]}-${slash[1].padStart(2,'0')}-${slash[2].padStart(2,'0')}`;
  const dash = str.match(/^(\d{1,2})-(\d{1,2})-(\d{4})$/);
  if (dash)  return `${dash[3]}-${dash[1].padStart(2,'0')}-${dash[2].padStart(2,'0')}`;
  return null;
}

// Returns ISO date strings (YYYY-MM-DD) for every weekday in [from, to]
function weekdaysBetween(from, to) {
  const dates = [];
  const d   = new Date(from + 'T12:00:00');
  const end = new Date(to   + 'T12:00:00');
  while (d <= end) {
    const day = d.getDay();
    if (day !== 0 && day !== 6) dates.push(d.toISOString().slice(0, 10));
    d.setDate(d.getDate() + 1);
  }
  return dates;
}


// ── GitHub date helpers ───────────────────────────────────────────────────────

function nextWeekday(isoDate) {
  const d = new Date(isoDate + 'T12:00:00');
  d.setDate(d.getDate() + 1);
  while (d.getDay() === 0 || d.getDay() === 6) d.setDate(d.getDate() + 1);
  return d.toISOString().slice(0, 10);
}

async function fetchScannedDates(s) {
  const [owner, repo] = s.repo.split('/');
  // Determine department from current page if possible, default to 302
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  let dept = '302';
  if (tab?.url?.includes('webapps.sftc.org')) {
    try {
      const results = await new Promise(resolve =>
        chrome.tabs.sendMessage(tab.id, { action: 'scrape' }, r =>
          resolve(chrome.runtime.lastError ? null : r)
        )
      );
      if (results?.department) dept = results.department;
    } catch (_) {}
  }

  const res = await fetch(
    `https://api.github.com/repos/${owner}/${repo}/contents/raw/dept${dept}?ref=${s.branch || 'master'}`,
    { headers: { Authorization: `Bearer ${s.token}`, 'X-GitHub-Api-Version': '2022-11-28' } }
  );
  if (!res.ok) return null;
  const files = await res.json();
  if (!Array.isArray(files)) return null;
  return files
    .map(f => f.name.match(/^(\d{4}-\d{2}-\d{2})/)?.[1])
    .filter(Boolean)
    .sort();
}

async function jumpToFirstGap() {
  const s = await loadSettings();
  const err = validateSettings(s);
  if (err) { setStatus(err, 'error'); return; }
  setStatus('Fetching scanned dates…', 'loading');
  const dates = await fetchScannedDates(s);
  if (!dates?.length) { setStatus('No scanned dates found.', 'warn'); return; }
  const scanned = new Set(dates);
  const min = dates[0], max = dates[dates.length - 1];
  // Find first weekday in range that's missing
  const d = new Date(min + 'T12:00:00');
  const end = new Date(max + 'T12:00:00');
  while (d <= end) {
    const iso = d.toISOString().slice(0, 10);
    if ((d.getDay() !== 0 && d.getDay() !== 6) && !scanned.has(iso)) {
      $('bulk-from').value = iso; $('bulk-from-picker').value = iso;
      $('bulk-to').value   = max; $('bulk-to-picker').value   = max;
      setStatus(`First gap: ${iso}`, 'success');
      return;
    }
    d.setDate(d.getDate() + 1);
  }
  setStatus('No gaps found in scanned range.', 'success');
}

async function jumpToResume() {
  const s = await loadSettings();
  const err = validateSettings(s);
  if (err) { setStatus(err, 'error'); return; }
  setStatus('Fetching last scanned date…', 'loading');
  const dates = await fetchScannedDates(s);
  if (!dates?.length) { setStatus('No scanned dates found.', 'warn'); return; }
  const last = dates[dates.length - 1];
  const from = nextWeekday(last);
  $('bulk-from').value = from; $('bulk-from-picker').value = from;
  $('bulk-to').value   = '';   $('bulk-to-picker').value   = '';
  setStatus(`Resuming from ${from} (To = today)`, 'success');
}

// ── Auto-scan: fetch gaps and start bulk immediately ──────────────────────────

async function autoScanUnscanned() {
  const s = await loadSettings();
  const err = validateSettings(s);
  if (err) { setStatus(err, 'error'); return; }

  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  if (!tab?.url?.includes('webapps.sftc.org/tr/')) {
    setStatus('Navigate to the SFSC page first.', 'warn');
    return;
  }

  $('auto-scan-btn').disabled = true;
  setStatus('Finding unscanned dates…', 'loading');

  const scannedDates = await fetchScannedDates(s);
  const today = new Date().toISOString().slice(0, 10);

  let allWeekdays;
  if (scannedDates?.length) {
    allWeekdays = weekdaysBetween(scannedDates[0], today);
  } else {
    const oneYearAgo = new Date();
    oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);
    allWeekdays = weekdaysBetween(oneYearAgo.toISOString().slice(0, 10), today);
  }

  const scanned = new Set(scannedDates || []);
  const unscanned = allWeekdays.filter(d => !scanned.has(d));

  if (!unscanned.length) {
    setStatus('All dates up to today are already scanned!', 'success');
    $('auto-scan-btn').disabled = false;
    return;
  }

  const waitMs = parseInt($('bulk-wait').value) || 1_000;
  const settings = { token: s.token, repo: s.repo, branch: s.branch || 'master' };

  $('bulk-btn').disabled = true;
  $('bulk-stop').style.display = 'block';
  $('send-btn').disabled = true;
  setStatus(`Starting scan of ${unscanned.length} unscanned dates…`, 'loading');

  chrome.runtime.sendMessage(
    { action: 'start-bulk', payload: { dates: unscanned, tabId: tab.id, settings, waitMs } },
    res => {
      if (res?.error) {
        setStatus('Error starting auto-scan: ' + res.error, 'error');
        $('bulk-btn').disabled = false;
        $('bulk-stop').style.display = 'none';
        $('auto-scan-btn').disabled = false;
      }
    }
  );
}

// ── Date inputs (text + hidden calendar picker) ───────────────────────────────

function wireDateInput(textId, pickerId, calBtnId) {
  const text   = $(textId);
  const picker = $(pickerId);
  const calBtn = $(calBtnId);

  function applyIso(iso) {
    text.value   = iso;
    picker.value = iso;
    text.classList.remove('invalid');
  }

  text.addEventListener('blur', () => {
    const iso = parseDate(text.value);
    if (iso)              applyIso(iso);
    else if (text.value)  text.classList.add('invalid');
  });

  text.addEventListener('paste', e => {
    const raw = (e.clipboardData || window.clipboardData).getData('text');
    const iso = parseDate(raw);
    if (iso) { e.preventDefault(); applyIso(iso); }
  });

  // Calendar button opens the native date picker
  calBtn.addEventListener('click', () => {
    if (picker.showPicker) picker.showPicker();
    else picker.click();
  });

  picker.addEventListener('change', () => {
    if (picker.value) applyIso(picker.value);
  });
}

wireDateInput('bulk-from', 'bulk-from-picker', 'cal-from');
wireDateInput('bulk-to',   'bulk-to-picker',   'cal-to');

// ── Settings: auto-save on blur ───────────────────────────────────────────────

function flashSaved(labelId) {
  const el = $(labelId);
  if (!el) return;
  el.textContent = '✓ saved';
  setTimeout(() => { el.textContent = ''; }, 2000);
}

function saveField(field) {
  const update = {};
  update[field] = $(field).value.trim() || (field === 'branch' ? 'master' : '');
  chrome.storage.local.set(update, () => flashSaved(field + '-saved'));
  if (field === 'token') {
    // Token saved — try re-initialising
    setTimeout(init, 300);
  }
}

$('token').addEventListener('blur',  () => saveField('token'));
$('repo').addEventListener('blur',   () => saveField('repo'));
$('branch').addEventListener('blur', () => saveField('branch'));

$('settings-btn').addEventListener('click', async () => {
  const panel = $('settings');
  const open  = panel.style.display === 'block';
  panel.style.display = open ? 'none' : 'block';
  if (!open) {
    const s = await loadSettings();
    $('token').value  = s.token  || '';
    $('repo').value   = s.repo   || 'aimesy/sfsc-tentatives';
    $('branch').value = s.branch || 'master';
  }
});

// ── Auto-update check ─────────────────────────────────────────────────────────

function checkAndDownloadUpdate() {
  chrome.runtime.sendMessage({ action: 'check-updates' }, result => {
    if (!result?.hasUpdate) return;
    $('update-banner').style.display = 'flex';
  });
}

$('update-download').addEventListener('click', () => {
  $('update-download').disabled = true;
  $('update-banner').querySelector('span').textContent = 'Downloading…';
  chrome.runtime.sendMessage({ action: 'download-update' }, dlResult => {
    $('update-download').style.display = 'none';
    $('update-banner').querySelector('span').textContent = dlResult?.success
      ? 'Downloaded — unzip sfsc-extension.zip and reload in chrome://extensions'
      : 'Download failed — get sfsc-extension.zip from GitHub';
  });
});

$('update-dismiss').addEventListener('click', () => {
  $('update-banner').style.display = 'none';
});

// ── On open: scrape current tab ───────────────────────────────────────────────

async function init() {
  setStatus('Checking page…', 'loading');

  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });

  if (!tab.url?.includes('webapps.sftc.org/tr/')) {
    setStatus('Navigate to the SFSC Tentative Rulings page first.', 'warn');
    return;
  }

  $('bulk-btn').disabled = false;
  $('auto-scan-btn').disabled = false;

  try {
    await chrome.scripting.executeScript({ target: { tabId: tab.id }, files: ['content.js'] });
  } catch (_) {}

  // Pre-fill date range from current page date input
  chrome.tabs.sendMessage(tab.id, { action: 'get-date' }, r => {
    if (!chrome.runtime.lastError && r?.date) {
      if (!$('bulk-from').value) { $('bulk-from').value = r.date; $('bulk-from-picker').value = r.date; }
      if (!$('bulk-to').value)   { $('bulk-to').value   = r.date; $('bulk-to-picker').value   = r.date; }
    }
  });

  chrome.tabs.sendMessage(tab.id, { action: 'scrape' }, result => {
    if (chrome.runtime.lastError || !result) {
      setStatus('Could not read page. Refresh and try again.', 'error');
      return;
    }
    if (result.sessionExpired) {
      setStatus('Session has expired. Please log in to the SFSC page again.', 'error');
      return;
    }
    if (result.error) { setStatus(result.error, 'warn'); return; }

    scrapedData = result;
    const n    = result.rulings.length;
    const dept = result.department;

    // Pre-fill date range from scraped ruling court date
    if (!$('bulk-from').value && result.rulings[0]?.['Court Date']) {
      const iso = parseDate(result.rulings[0]['Court Date']);
      if (iso) {
        if (!$('bulk-from').value) { $('bulk-from').value = iso; $('bulk-from-picker').value = iso; }
        if (!$('bulk-to').value)   { $('bulk-to').value   = iso; $('bulk-to-picker').value   = iso; }
      }
    }

    if (n === 0) { setStatus('No rulings found on this page.', 'warn'); return; }

    const warn = result.reported_total && result.reported_total !== n
      ? ` (page reports ${result.reported_total} — scroll to load all)`
      : '';
    setStatus(`Found ${n} ruling${n !== 1 ? 's' : ''} — Dept ${dept}.${warn}`, warn ? 'warn' : '');
    $('send-btn').disabled = false;
  });
}

// ── Send to GitHub (single page) ──────────────────────────────────────────────

$('send-btn').addEventListener('click', async () => {
  if (!scrapedData) return;
  $('send-btn').disabled = true;
  setStatus('Committing to GitHub…', 'loading');

  const s = await loadSettings();
  const err = validateSettings(s);
  if (err) { setStatus(err, 'error'); $('send-btn').disabled = false; return; }

  const [owner, repo] = s.repo.split('/');
  chrome.runtime.sendMessage(
    { action: 'commit', payload: { token: s.token, owner, repo, branch: s.branch || 'master', data: scrapedData } },
    res => {
      if (res?.error) {
        setStatus('Error: ' + res.error, 'error');
        $('send-btn').disabled = false;
      } else if (res?.duplicate) {
        setStatus(`Already submitted for this date — skipped. (${res.path})`, 'warn');
        $('send-btn').disabled = false;
      } else {
        setStatus(`Committed ${scrapedData.rulings.length} rulings → ${res.path}`, 'success');
      }
    }
  );
});

// ── Bulk status display ───────────────────────────────────────────────────────

function updateBulkStatus(job) {
  if (!job) return;
  if (job.fatalError) {
    setStatus(job.fatalError, 'error');
    $('bulk-btn').disabled = false;
    $('auto-scan-btn').disabled = false;
    $('bulk-stop').style.display = 'none';
    return;
  }
  if (job.done) {
    setStatus(
      `Done: ${job.committed} committed, ${job.skipped} skipped, ${job.errors} errors`,
      job.errors > 0 ? 'warn' : 'success'
    );
    $('bulk-btn').disabled = false;
    $('auto-scan-btn').disabled = false;
    $('bulk-stop').style.display = 'none';
    return;
  }
  if (job.running) {
    const pct = job.dates?.length ? Math.round(job.index / job.dates.length * 100) : 0;
    setStatus(
      `[${job.index}/${job.dates?.length}] ${job.currentDate || '…'} — ` +
      `${job.committed} saved, ${job.skipped} skipped, ${job.errors} err — ${pct}% ` +
      `(close popup freely)`,
      'loading'
    );
    $('bulk-btn').disabled = true;
    $('bulk-stop').style.display = 'block';
  }
}

// Keep popup in sync with background job even while open
chrome.storage.onChanged.addListener((changes, area) => {
  if (area === 'local' && changes._bulkJob) {
    updateBulkStatus(changes._bulkJob.newValue);
  }
});

// ── Bulk Scrape ───────────────────────────────────────────────────────────────

$('bulk-btn').addEventListener('click', async () => {
  const from = parseDate($('bulk-from').value);
  const to   = parseDate($('bulk-to').value) || new Date().toISOString().slice(0, 10);
  if (!from || from > to) {
    setStatus('Enter a valid start date (YYYY-MM-DD or MM/DD/YYYY).', 'warn');
    return;
  }

  const s = await loadSettings();
  const err = validateSettings(s);
  if (err) { setStatus(err, 'error'); return; }

  const waitMs = parseInt($('bulk-wait').value) || 1_000;
  const dates  = weekdaysBetween(from, to);
  if (!dates.length) { setStatus('No weekdays in that range.', 'warn'); return; }

  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  if (!tab?.url?.includes('webapps.sftc.org/tr/')) {
    setStatus('Navigate to the SFSC page first.', 'warn');
    return;
  }

  const settings = { token: s.token, repo: s.repo, branch: s.branch || 'master' };

  $('bulk-btn').disabled = true;
  $('bulk-stop').style.display = 'block';
  $('send-btn').disabled = true;
  setStatus(`Starting background scrape of ${dates.length} dates…`, 'loading');

  chrome.runtime.sendMessage(
    { action: 'start-bulk', payload: { dates, tabId: tab.id, settings, waitMs } },
    res => {
      if (res?.error) {
        setStatus('Error starting bulk: ' + res.error, 'error');
        $('bulk-btn').disabled = false;
        $('bulk-stop').style.display = 'none';
      }
      // Otherwise, storage changes will drive the status display
    }
  );
});

$('bulk-stop').addEventListener('click', () => {
  chrome.runtime.sendMessage({ action: 'stop-bulk' });
});
$('auto-scan-btn').addEventListener('click', autoScanUnscanned);
$('jump-first').addEventListener('click', jumpToFirstGap);
$('jump-last').addEventListener('click',  jumpToResume);

// ── Diagnose ──────────────────────────────────────────────────────────────────

$('diag-btn').addEventListener('click', async () => {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  try {
    await chrome.scripting.executeScript({ target: { tabId: tab.id }, files: ['content.js'] });
  } catch (_) {}
  chrome.tabs.sendMessage(tab.id, { action: 'diagnose' }, result => {
    if (chrome.runtime.lastError || !result) {
      setStatus('Could not diagnose — refresh page and try again.', 'error');
      return;
    }
    const msg = result.foundInput
      ? `Input: name="${result.foundInput.name}" id="${result.foundInput.id}" | Form action: ${result.formAction} | Btn: "${result.btnText}" | Forms: ${result.allForms.length}`
      : `No date input found. Forms: ${JSON.stringify(result.allForms)}`;
    setStatus(msg, result.foundInput ? 'warn' : 'error');
    console.log('SFSC diagnose:', JSON.stringify(result, null, 2));
  });
});

// ── Boot ──────────────────────────────────────────────────────────────────────

loadSettings().then(s => {
  $('token').value  = s.token  || '';
  $('repo').value   = s.repo   || 'aimesy/sfsc-tentatives';
  $('branch').value = s.branch || 'master';
  if (!s.token) $('settings').style.display = 'block';
});

// Restore any in-progress background bulk job
chrome.runtime.sendMessage({ action: 'bulk-status' }, job => {
  if (job?.running) updateBulkStatus(job);
});

checkAndDownloadUpdate();
init();
