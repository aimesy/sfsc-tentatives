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

function waitForTabLoad(tabId, timeout = 14_000) {
  return new Promise(resolve => {
    const timer = setTimeout(resolve, timeout);
    function listener(id, info) {
      if (id === tabId && info.status === 'complete') {
        clearTimeout(timer);
        chrome.tabs.onUpdated.removeListener(listener);
        resolve();
      }
    }
    chrome.tabs.onUpdated.addListener(listener);
  });
}

async function injectAndScrape(tabId) {
  await chrome.scripting.executeScript({ target: { tabId }, files: ['content.js'] });
  await sleep(600);
  return new Promise(resolve => {
    chrome.tabs.sendMessage(tabId, { action: 'scrape' }, r =>
      resolve(chrome.runtime.lastError ? null : r)
    );
  });
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
    chrome.runtime.sendMessage({ action: 'download-update' }, dlResult => {
      const banner = $('update-banner');
      if (dlResult?.success) {
        banner.querySelector('span').textContent =
          'Update downloaded — unzip sfsc-extension.zip and reload in chrome://extensions';
      } else {
        banner.querySelector('span').textContent =
          'New version available — download sfsc-extension.zip from GitHub';
      }
      banner.style.display = 'flex';
    });
  });
}

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

// ── Bulk Scrape ───────────────────────────────────────────────────────────────

$('bulk-btn').addEventListener('click', async () => {
  const from = parseDate($('bulk-from').value);
  const to   = parseDate($('bulk-to').value);
  if (!from || !to || from > to) {
    setStatus('Enter a valid date range (YYYY-MM-DD or MM/DD/YYYY).', 'warn');
    return;
  }

  const s = await loadSettings();
  const err = validateSettings(s);
  if (err) { setStatus(err, 'error'); return; }

  const waitMs = parseInt($('bulk-wait').value) || 1_000;
  const dates  = weekdaysBetween(from, to);
  if (!dates.length) { setStatus('No weekdays in that range.', 'warn'); return; }

  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  if (!tab.url?.includes('webapps.sftc.org/tr/')) {
    setStatus('Navigate to the SFSC page first.', 'warn');
    return;
  }

  const [owner, repo] = s.repo.split('/');
  const settings = { token: s.token, owner, repo, branch: s.branch || 'master' };

  bulkRunning = true;
  $('bulk-btn').disabled = true;
  $('bulk-stop').style.display = 'block';
  $('send-btn').disabled = true;

  let committed = 0, skipped = 0, errors = 0;

  for (let i = 0; i < dates.length; i++) {
    if (!bulkRunning) break;

    const date = dates[i];
    setStatus(`[${i + 1}/${dates.length}] ${date}…`, 'loading');

    try {
      await chrome.scripting.executeScript({ target: { tabId: tab.id }, files: ['content.js'] });
    } catch (_) {}

    await sleep(300);

    let data = await new Promise(resolve => {
      chrome.tabs.sendMessage(tab.id, { action: 'fill-and-scrape', date, waitMs }, r =>
        resolve(chrome.runtime.lastError ? { pending: true } : r)
      );
    });

    if (data?.pending) {
      await waitForTabLoad(tab.id);
      data = await injectAndScrape(tab.id);
    }

    if (!data || data.error) {
      errors++;
      setStatus(`[${i + 1}/${dates.length}] ${date} — error: ${data?.error ?? 'no response'}`, 'error');
      await sleep(1500);
      continue;
    }

    if (!data.rulings?.length) {
      skipped++;
      await sleep(200);
      continue;
    }

    const res = await new Promise(resolve =>
      chrome.runtime.sendMessage({ action: 'commit', payload: { ...settings, data } }, resolve)
    );

    if (res?.error)          errors++;
    else if (res?.duplicate) skipped++;
    else                     committed++;

    await sleep(300);
  }

  bulkRunning = false;
  $('bulk-stop').style.display = 'none';
  $('bulk-btn').disabled = false;
  $('send-btn').disabled = false;

  const stopped = committed + skipped + errors < dates.length;
  setStatus(
    `Done: ${committed} committed, ${skipped} skipped, ${errors} errors` +
    (stopped ? ' (stopped early)' : ''),
    errors > 0 ? 'warn' : 'success'
  );
});

$('bulk-stop').addEventListener('click', () => { bulkRunning = false; });

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

checkAndDownloadUpdate();
init();
