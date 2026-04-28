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

// Resolves when the given tab reaches status=complete (or after timeout ms)
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

// ── On open: scrape current tab ───────────────────────────────────────────────

async function init() {
  setStatus('Checking page…', 'loading');

  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });

  if (!tab.url?.includes('webapps.sftc.org/tr/')) {
    setStatus('Navigate to the SFSC Tentative Rulings page first.', 'warn');
    return;
  }

  // Enable bulk controls once we know we're on the right page
  $('bulk-btn').disabled = false;

  try {
    await chrome.scripting.executeScript({ target: { tabId: tab.id }, files: ['content.js'] });
  } catch (_) { /* already injected */ }

  chrome.tabs.sendMessage(tab.id, { action: 'scrape' }, result => {
    if (chrome.runtime.lastError || !result) {
      setStatus('Could not read page. Refresh and try again.', 'error');
      return;
    }
    if (result.error) {
      setStatus(result.error, 'warn');
      return;
    }

    scrapedData = result;
    const n    = result.rulings.length;
    const dept = result.department;

    if (n === 0) {
      setStatus('No rulings found on this page.', 'warn');
      return;
    }

    const warn = result.reported_total && result.reported_total !== n
      ? ` (page reports ${result.reported_total} — scroll to load all)`
      : '';

    setStatus(`Found ${n} ruling${n !== 1 ? 's' : ''} — Dept ${dept}.${warn}`,
              warn ? 'warn' : '');
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
  const from = $('bulk-from').value;
  const to   = $('bulk-to').value;
  if (!from || !to || from > to) {
    setStatus('Enter a valid date range first.', 'warn');
    return;
  }

  const s = await loadSettings();
  const err = validateSettings(s);
  if (err) { setStatus(err, 'error'); return; }

  const waitMs = parseInt($('bulk-wait').value) || 10_000;
  const dates = weekdaysBetween(from, to);
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

    // Inject content script and ask it to navigate to this date
    try {
      await chrome.scripting.executeScript({ target: { tabId: tab.id }, files: ['content.js'] });
    } catch (_) {}

    await sleep(300);

    let data = await new Promise(resolve => {
      chrome.tabs.sendMessage(tab.id, { action: 'fill-and-scrape', date, waitMs }, r =>
        // lastError means the page reloaded mid-flight — treat same as pending
        resolve(chrome.runtime.lastError ? { pending: true } : r)
      );
    });

    // Page reloaded — wait for it to finish, then scrape
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
      skipped++;  // court likely not in session that day
      await sleep(800);
      continue;
    }

    const res = await new Promise(resolve =>
      chrome.runtime.sendMessage({ action: 'commit', payload: { ...settings, data } }, resolve)
    );

    if (res?.error)     { errors++;    }
    else if (res?.duplicate) { skipped++; }
    else                { committed++; }

    await sleep(1200); // be polite to the server
  }

  bulkRunning = false;
  $('bulk-stop').style.display = 'none';
  $('bulk-btn').disabled = false;
  $('send-btn').disabled = false;

  const stopped = !bulkRunning && committed + skipped + errors < dates.length;
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
      ? `Input: name="${result.foundInput.name}" id="${result.foundInput.id}" | Form action: ${result.formAction} | Btn: "${result.btnText}" | Forms on page: ${result.allForms.length}`
      : `No date input found. Forms on page: ${JSON.stringify(result.allForms)}`;
    setStatus(msg, result.foundInput ? 'warn' : 'error');
    console.log('SFSC diagnose:', JSON.stringify(result, null, 2));
  });
});

// ── Settings panel ────────────────────────────────────────────────────────────

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

$('save-btn').addEventListener('click', () => {
  chrome.storage.local.set({
    token:  $('token').value.trim(),
    repo:   $('repo').value.trim(),
    branch: $('branch').value.trim() || 'master',
  }, () => {
    $('settings').style.display = 'none';
    setStatus('Settings saved.', 'success');
    setTimeout(init, 800);
  });
});

// ── Boot ──────────────────────────────────────────────────────────────────────
init();
