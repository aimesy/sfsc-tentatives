// popup.js — runs in the extension popup.

const $ = id => document.getElementById(id);
let scrapedData = null;
// Each popup is scoped to the tab that was active when it opened. Bulk jobs
// are now keyed by tabId in background.js so multiple SFTC tabs can run
// independent scrapes side-by-side; we capture the tabId once on init and
// pass it on every start-bulk / stop-bulk / resume-bulk / status request.
let currentTabId = null;

// Firefox extensions live under moz-extension://; Chrome under chrome-extension://.
const IS_FIREFOX = chrome.runtime.getURL('').startsWith('moz-extension://');

// True when this popup is running inside the standalone detached window
// (chrome.windows.create with type: 'popup'), false when it's running as
// the regular toolbar-icon popup. We use this to hide the Detach button
// once we're already detached, and to re-target SFTC tab queries to the
// last-active normal window rather than the popup window itself.
const IS_DETACHED = new URLSearchParams(location.search).get('detached') === '1';
// When detached, we bind to a specific SFTC tab id passed via URL — the
// "active tab in current window" pattern would otherwise resolve to the
// detached popup window itself, where there is no SFTC page.
const DETACHED_TAB_ID = (() => {
  const raw = new URLSearchParams(location.search).get('tabId');
  const n = parseInt(raw || '', 10);
  return Number.isFinite(n) ? n : null;
})();

// Return the SFTC tab the popup is bound to. In the regular toolbar popup
// this is the active tab of the current window; in the detached popup it's
// the tab id captured at detach time (which may now live in any window).
async function getActiveSftcTab() {
  if (IS_DETACHED && DETACHED_TAB_ID != null) {
    try {
      const tab = await chrome.tabs.get(DETACHED_TAB_ID);
      return tab || null;
    } catch {
      return null;
    }
  }
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  return tab || null;
}

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

// YYYY-MM-DD helpers (localISO, weekdaysBetween, COURT_HOLIDAYS) come from holidays.js.

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

// ── GitHub date helpers ───────────────────────────────────────────────────────

// Cache the department determined from the current tab so multiple bulk
// flows in one popup session don't each pay a scrape round-trip.
let _detectedDept = null;

// Tab → last successfully-detected dept, persisted across popup opens and
// service-worker restarts. After a stop/CAPTCHA cycle the SFTC tab can
// land in a state where content.js can't read the dept (no h4, cleared
// form, post-Cloudflare redirect), and the old code silently fell back
// to '302' — which is how a Dept 301 scan ended up scraping into Dept
// 302's raw folder. We now prefer a previously-known dept for the same
// tab over the '302' default.
const LAST_DEPT_KEY = '_lastDeptByTab';

async function _getCachedDeptForTab(tabId) {
  if (tabId == null) return null;
  const all = await chrome.storage.local.get(LAST_DEPT_KEY);
  return all[LAST_DEPT_KEY]?.[tabId] || null;
}

async function _saveCachedDeptForTab(tabId, dept) {
  if (tabId == null || !dept) return;
  const all = await chrome.storage.local.get(LAST_DEPT_KEY);
  const map = all[LAST_DEPT_KEY] || {};
  if (map[tabId] === String(dept)) return;
  map[tabId] = String(dept);
  await chrome.storage.local.set({ [LAST_DEPT_KEY]: map });
}

async function detectDepartment() {
  if (_detectedDept) return _detectedDept;
  const tab = await getActiveSftcTab();
  const tabId = tab?.id ?? null;
  if (tab?.url?.includes('webapps.sftc.org')) {
    try {
      const results = await new Promise(resolve =>
        chrome.tabs.sendMessage(tab.id, { action: 'scrape' }, r =>
          resolve(chrome.runtime.lastError ? null : r)
        )
      );
      if (results?.department) {
        _detectedDept = String(results.department);
        // Persist so a future popup open against this tab — even after
        // the SFTC page enters a degraded state — can recover the dept.
        _saveCachedDeptForTab(tabId, _detectedDept).catch(() => {});
        return _detectedDept;
      }
    } catch (_) {}
  }
  // Live detection failed (or returned no dept). Prefer the last value we
  // saw for this specific tab over the generic '302' default — it's almost
  // always right, and when it isn't the user can simply navigate to the
  // intended SFSC page and a fresh scrape will overwrite the cache.
  const cached = await _getCachedDeptForTab(tabId);
  if (cached) {
    _detectedDept = cached;
    return cached;
  }
  return '302';
}

async function fetchScannedDates(s) {
  const [owner, repo] = s.repo.split('/');
  // Determine department from current page if possible, default to 302.
  // Cached on _detectedDept so the bulk-start handler can read it without a
  // second scrape round-trip.
  const dept = await detectDepartment();

  // Union three sources, in order of staleness:
  //   • coverage/dept<N>.json — parquet court_dates ∪ raw filenames; only
  //     refreshed when the ingest workflow runs, so it lags behind by
  //     ~throttle window + workflow runtime. Required for the historical
  //     Excel-imports (2014-2024) which have no raw files.
  //   • raw/dept<N>/ listing — live within seconds of any commit.
  //   • _localCommitted log   — survives popup reopen and SW restart, so a
  //     scan stopped mid-flight (or finished but pre-ingest) doesn't replay
  //     its committed dates as "still unscanned".
  const branch = s.branch || 'master';
  const headers = { Authorization: `Bearer ${s.token}`, 'X-GitHub-Api-Version': '2022-11-28' };
  const [covRes, dirRes, storage] = await Promise.allSettled([
    fetch(`https://api.github.com/repos/${owner}/${repo}/contents/coverage/dept${dept}.json?ref=${branch}`, { headers }),
    fetch(`https://api.github.com/repos/${owner}/${repo}/contents/raw/dept${dept}?ref=${branch}`, { headers }),
    chrome.storage.local.get('_localCommitted'),
  ]);

  const covered = new Set();

  if (covRes.status === 'fulfilled' && covRes.value.ok) {
    try {
      const meta = await covRes.value.json();
      const json = JSON.parse(atob((meta.content || '').replace(/\n/g, '')));
      if (Array.isArray(json.covered)) json.covered.forEach(d => covered.add(d));
    } catch (_) { /* fall through */ }
  }

  if (dirRes.status === 'fulfilled' && dirRes.value.ok) {
    try {
      const files = await dirRes.value.json();
      if (Array.isArray(files)) {
        for (const f of files) {
          const iso = f.name?.match(/^(\d{4}-\d{2}-\d{2})/)?.[1];
          if (iso) covered.add(iso);
        }
      }
    } catch (_) { /* fall through */ }
  }

  if (storage.status === 'fulfilled') {
    const TTL = 24 * 60 * 60 * 1000;
    const now = Date.now();
    const local = storage.value._localCommitted?.[dept] || [];
    for (const e of local) {
      if (e?.d && now - (e.t || 0) < TTL) covered.add(e.d);
    }
  }

  if (!covered.size) return null;
  return Array.from(covered).sort();
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
    const iso = localISO(d);
    if ((d.getDay() !== 0 && d.getDay() !== 6) && !COURT_HOLIDAYS.has(iso) && !scanned.has(iso)) {
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
  const from = nextBusinessDay(last);
  $('bulk-from').value = from; $('bulk-from-picker').value = from;
  $('bulk-to').value   = '';   $('bulk-to-picker').value   = '';
  setStatus(`Resuming from ${from} (To = today)`, 'success');
}

// ── Auto-scan: fetch gaps and start bulk immediately ──────────────────────────

// Returns the user's chosen scan direction:
//   'forward'  — earliest unscanned weekday first (default; matches the
//                pre-2026 behaviour).
//   'backward' — most-recent unscanned weekday first; useful for filling
//                in fresh dates while older gaps catch up out of band.
function getScanDirection() {
  const sel = document.querySelector('input[name="scan-direction"]:checked');
  return sel?.value === 'backward' ? 'backward' : 'forward';
}

async function autoScanUnscanned() {
  const s = await loadSettings();
  const err = validateSettings(s);
  if (err) { setStatus(err, 'error'); return; }

  const tab = await getActiveSftcTab();
  if (!tab?.url?.includes('webapps.sftc.org/tr/')) {
    setStatus('Navigate to the SFSC page first.', 'warn');
    return;
  }

  $('auto-scan-btn').disabled = true;
  setStatus('Finding unscanned dates…', 'loading');

  const scannedDates = await fetchScannedDates(s);
  const today = localISO(new Date());
  const department = await detectDepartment();

  let allWeekdays;
  if (scannedDates?.length) {
    allWeekdays = weekdaysBetween(scannedDates[0], today);
  } else {
    const oneYearAgo = new Date();
    oneYearAgo.setFullYear(oneYearAgo.getFullYear() - 1);
    allWeekdays = weekdaysBetween(localISO(oneYearAgo), today);
  }

  const scanned = new Set(scannedDates || []);
  let unscanned = allWeekdays.filter(d => !scanned.has(d));

  if (!unscanned.length) {
    // Distinguish the two "no work to do" cases so the user knows whether
    // to start a sibling tab on a different dept, navigate further back,
    // or just wait. Without this we say "all scanned" for both cases and
    // the user assumes the popup is broken when another tab is mid-scrape.
    const inflight = await new Promise(resolve =>
      chrome.runtime.sendMessage(
        { action: 'inflight-dates', payload: { department } },
        r => resolve((r && typeof r === 'object' && !r.error) ? r : {})
      )
    );
    const inflightCount = Object.keys(inflight || {}).length;
    if (inflightCount > 0) {
      setStatus(
        `All weekdays from ${scannedDates[0]} → today (Dept ${department}) are either committed or being scanned by another tab (${inflightCount} in flight). ` +
        `Either wait for that tab to finish, or set a manual From date earlier than ${scannedDates[0]}.`,
        'warn'
      );
    } else {
      setStatus(
        `All weekdays from ${scannedDates[0]} → today are already scanned for Dept ${department}. ` +
        `Set a manual From date earlier than ${scannedDates[0]} to fill earlier gaps.`,
        'success'
      );
    }
    $('auto-scan-btn').disabled = false;
    return;
  }

  // weekdaysBetween already returns ascending order; reverse for newest-first.
  const direction = getScanDirection();
  if (direction === 'backward') unscanned = [...unscanned].reverse();

  const waitMs   = parseInt($('bulk-wait').value) || 5_000;
  const settings = { token: s.token, repo: s.repo, branch: s.branch || 'master' };

  $('bulk-btn').disabled = true;
  $('bulk-stop').style.display = 'block';
  $('bulk-resume').style.display = 'none';
  $('send-btn').disabled = true;
  const dirNote = direction === 'backward'
    ? 'newest-first'
    : 'oldest-first';
  setStatus(`Starting scan of ${unscanned.length} unscanned dates (${dirNote})…`, 'loading');

  chrome.runtime.sendMessage(
    { action: 'start-bulk', payload: { dates: unscanned, tabId: tab.id, settings, waitMs, department } },
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

// ── Date inputs (text + custom calendar widget) ───────────────────────────────
// The native <input type="date"> picker we used to call via showPicker() was
// flaky inside MV3 popups (Firefox would silently no-op, Chrome would render
// the picker outside the popup viewport on narrow screens) and couldn't
// decorate days with scan status. The custom widget below renders inside the
// popup, highlights dates by archive coverage AND by which dates other tabs
// are currently scraping (so the user can see a sibling tab is mid-scan
// before they pick the same day), and falls back gracefully if the
// inflight/coverage fetches fail.

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

  calBtn.addEventListener('click', e => {
    // stopPropagation so the document-level "click outside" listener
    // doesn't immediately close the popup we're about to open.
    e.stopPropagation();
    openCustomCalendar(text, picker, calBtn);
  });
}

wireDateInput('bulk-from', 'bulk-from-picker', 'cal-from');
wireDateInput('bulk-to',   'bulk-to-picker',   'cal-to');

// ── Custom calendar widget ────────────────────────────────────────────────────
// Shared instance: only one popup is ever open. _calOpen carries the
// inputs that pinned it open and the month currently rendered. The
// status sets are populated lazily on first open and refreshed each
// time so the inflight indicator stays current as sibling tabs claim
// and release dates.

let _calOpen = null;       // { text, picker, btn, monthDate }
let _calCovered = null;    // Set<ISODate>
let _calInflight = null;   // Map<ISODate, {tabId,...}>
let _calLastFetch = 0;     // ms timestamp; rate-limit re-fetches
let _calStatusError = null;

async function refreshCalendarStatus(force = false) {
  // Cache for 30s so flipping months doesn't pound the GitHub API. The
  // inflight set is read from chrome.storage and is essentially free, so
  // we always pull a fresh copy of that even when we're using cached
  // coverage.
  const now = Date.now();
  const stale = !_calCovered || (now - _calLastFetch) > 30_000;
  if (!stale && !force) {
    _calInflight = await loadInflightDates();
    return;
  }
  _calStatusError = null;
  try {
    const dept = await detectDepartment();
    const s    = await loadSettings();
    const err  = validateSettings(s);
    if (err) {
      _calCovered  = new Set();
      _calInflight = new Map();
      _calStatusError = 'Settings missing — coverage shading disabled.';
      _calLastFetch = now;
      return;
    }
    const [scanned, inflight] = await Promise.all([
      fetchScannedDates(s).catch(() => null),
      loadInflightDates(dept),
    ]);
    _calCovered  = new Set(scanned || []);
    _calInflight = inflight;
    _calLastFetch = now;
  } catch (e) {
    _calCovered  = _calCovered  || new Set();
    _calInflight = _calInflight || new Map();
    _calStatusError = `Couldn't fetch scan status: ${e.message}`;
  }
}

async function loadInflightDates(deptHint) {
  // background.js maintains _inFlightClaims keyed by department; the
  // inflight-dates message returns the per-dept claim map. We pass the
  // detected dept so we only get the claims relevant to this calendar
  // (otherwise sibling tabs scraping a different dept would muddy the
  // visualisation).
  const dept = deptHint || await detectDepartment();
  return new Promise(resolve => {
    chrome.runtime.sendMessage(
      { action: 'inflight-dates', payload: { department: dept } },
      r => {
        if (!r || r.error || typeof r !== 'object') return resolve(new Map());
        resolve(new Map(Object.entries(r)));
      }
    );
  });
}

// "Smart default" date used when the bound text input is empty: pick the
// most recent date already in the archive for this dept, advanced one
// weekday if that date is today (since today is presumably about to be
// scanned). Falls back to today when no coverage data is loaded yet.
// The user reported that defaulting to today put them in the wrong
// part of the year — for a contributor filling old gaps the calendar
// should open near the data, not near the wall clock.
function smartDefaultDate() {
  const today = localISO(new Date());
  if (!_calCovered || !_calCovered.size) return today;
  let last = '';
  for (const d of _calCovered) if (d > last) last = d;
  if (!last) return today;
  if (last >= today) return nextBusinessDay(last);
  return last;
}

function openCustomCalendar(text, picker, btn) {
  // Provisional anchor: use the bound text input's value if it's set, or
  // today as a fallback. Once coverage finishes loading, swap to the
  // smart default if the text input is still empty so the calendar
  // opens near the actual data rather than near the wall clock.
  const provisional = parseDate(text.value) || localISO(new Date());
  const pd = new Date(provisional + 'T12:00:00');
  _calOpen = {
    text, picker, btn,
    monthDate: new Date(pd.getFullYear(), pd.getMonth(), 1),
  };
  // Position immediately so the loading state renders in the right place.
  positionCalendar();
  $('cal-pop').classList.add('open');
  $('cal-pop').innerHTML =
    '<div class="cal-status-line cal-status-loading">Loading scan status…</div>';
  refreshCalendarStatus().then(() => {
    if (!_calOpen) return;
    if (!parseDate(text.value)) {
      const iso = smartDefaultDate();
      const d = new Date(iso + 'T12:00:00');
      _calOpen.monthDate = new Date(d.getFullYear(), d.getMonth(), 1);
    }
    renderCalendar();
  });
}

function positionCalendar() {
  if (!_calOpen) return;
  const pop  = $('cal-pop');
  const rect = _calOpen.btn.getBoundingClientRect();
  // Keep within viewport horizontally; the popup is 232px wide.
  const left = Math.min(window.innerWidth - 240,
                        Math.max(8, rect.left + window.scrollX - 50));
  pop.style.left = `${left}px`;
  pop.style.top  = `${rect.bottom + window.scrollY + 4}px`;
}

function closeCustomCalendar() {
  $('cal-pop').classList.remove('open');
  _calOpen = null;
}

function renderCalendar() {
  if (!_calOpen) return;
  const { monthDate } = _calOpen;
  const year  = monthDate.getFullYear();
  const month = monthDate.getMonth();
  const firstDow    = new Date(year, month, 1).getDay();
  const daysInMonth = new Date(year, month + 1, 0).getDate();
  const monthLabel  = monthDate.toLocaleString('en-US', { month: 'long', year: 'numeric' });
  const todayIso    = localISO(new Date());

  let inflightCount = 0;
  let coveredCount  = 0;

  let cells = '';
  for (let i = 0; i < firstDow; i++) cells += '<div class="cal-empty"></div>';
  for (let day = 1; day <= daysInMonth; day++) {
    const dt = new Date(year, month, day);
    const iso = localISO(dt);
    const dow = dt.getDay();
    const isWeekend  = dow === 0 || dow === 6;
    const isHoliday  = COURT_HOLIDAYS.has(iso);
    const isCovered  = _calCovered && _calCovered.has(iso);
    const inflight   = _calInflight && _calInflight.get(iso);
    const isInflight = !!inflight;
    const isToday    = iso === todayIso;

    if (isInflight) inflightCount++;
    if (isCovered)  coveredCount++;

    const classes = ['cal-day'];
    if (isWeekend)  classes.push('cal-weekend');
    if (isHoliday)  classes.push('cal-holiday');
    if (isCovered)  classes.push('cal-covered');
    if (isInflight) classes.push('cal-inflight');
    if (isToday)    classes.push('cal-today');

    const tipParts = [iso];
    if (isHoliday)  tipParts.push('court holiday');
    else if (isWeekend) tipParts.push('weekend');
    if (isCovered)  tipParts.push('already in archive');
    if (isInflight) tipParts.push(`being scanned by tab ${inflight.tabId}`);
    const tip = tipParts.join(' — ');

    cells += `<button class="${classes.join(' ')}" data-iso="${iso}" title="${tip}">${day}</button>`;
  }

  const html =
    '<div class="cal-header">' +
      '<button class="cal-nav" data-dir="-1" title="Previous month">‹</button>' +
      `<span class="cal-title">${monthLabel}</span>` +
      '<button class="cal-nav" data-dir="1" title="Next month">›</button>' +
    '</div>' +
    '<div class="cal-dow">' +
      '<div>S</div><div>M</div><div>T</div><div>W</div>' +
      '<div>T</div><div>F</div><div>S</div>' +
    '</div>' +
    `<div class="cal-grid">${cells}</div>` +
    '<div class="cal-legend">' +
      '<span><i class="cal-swatch" style="background:#e3f3e3"></i>scanned</span>' +
      '<span><i class="cal-swatch" style="background:#fff3cd;border:1px solid #e8c060"></i>in flight</span>' +
      '<span><i class="cal-swatch" style="background:#f0f0f0"></i>holiday</span>' +
      '<span><i class="cal-swatch" style="border:1px solid #1a3a5c"></i>today</span>' +
    '</div>' +
    (_calStatusError
      ? `<div class="cal-status-line cal-status-error">${_calStatusError}</div>`
      : `<div class="cal-status-line">${coveredCount} scanned · ${inflightCount} in flight in this month</div>`);

  const pop = $('cal-pop');
  pop.innerHTML = html;
  positionCalendar();

  pop.querySelectorAll('.cal-nav').forEach(b => {
    b.addEventListener('click', e => {
      e.stopPropagation();
      const dir = parseInt(b.dataset.dir, 10);
      _calOpen.monthDate = new Date(
        _calOpen.monthDate.getFullYear(),
        _calOpen.monthDate.getMonth() + dir, 1);
      // Refresh inflight on each nav so a sibling tab claiming a date
      // mid-session shows up without the user closing/reopening the popup.
      refreshCalendarStatus().then(() => renderCalendar());
    });
  });
  pop.querySelectorAll('.cal-day').forEach(b => {
    b.addEventListener('click', e => {
      e.stopPropagation();
      const iso = b.dataset.iso;
      _calOpen.text.value   = iso;
      _calOpen.picker.value = iso;
      _calOpen.text.classList.remove('invalid');
      closeCustomCalendar();
    });
  });
}

document.addEventListener('click', e => {
  if (!_calOpen) return;
  const pop = $('cal-pop');
  if (pop.contains(e.target) || _calOpen.btn.contains(e.target)) return;
  closeCustomCalendar();
});
document.addEventListener('keydown', e => {
  if (e.key === 'Escape' && _calOpen) closeCustomCalendar();
});

// Pick up storage updates so the calendar's inflight indicator stays
// current while it's open — a sibling tab claiming a new date will fire
// _bulkJobs change which we use as a low-cost proxy for a claim change
// (the exact _inFlightClaims storage key is internal to background.js
// but bulk job updates correlate well with claim activity).
chrome.storage.onChanged.addListener((changes, area) => {
  if (area !== 'local' || !_calOpen) return;
  if (!changes._bulkJobs && !changes._inFlightClaims) return;
  refreshCalendarStatus(true).then(() => { if (_calOpen) renderCalendar(); });
});

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
    const reloadHint = IS_FIREFOX
      ? 'about:debugging → This Firefox → Reload'
      : 'chrome://extensions';
    $('update-banner').querySelector('span').textContent = dlResult?.success
      ? `Downloaded — unzip sfsc-extension.zip and reload in ${reloadHint}`
      : 'Download failed — get sfsc-extension.zip from GitHub';
  });
});

$('update-dismiss').addEventListener('click', () => {
  $('update-banner').style.display = 'none';
});

// ── On open: scrape current tab ───────────────────────────────────────────────

async function init() {
  setStatus('Checking page…', 'loading');

  const tab = await getActiveSftcTab();
  currentTabId = tab?.id ?? null;

  if (!tab?.url?.includes('webapps.sftc.org/tr/')) {
    setStatus(IS_DETACHED
      ? 'The SFSC tab this popup was detached from is gone. Close this window and reopen the popup from a SFSC tab.'
      : 'Navigate to the SFSC Tentative Rulings page first.', 'warn');
    return;
  }

  $('bulk-btn').disabled = false;
  $('auto-scan-btn').disabled = false;

  try {
    await chrome.scripting.executeScript({ target: { tabId: tab.id }, files: ['content.js'] });
  } catch (_) {}

  // Pre-fill the From date from the current page's date input. To is left
  // blank intentionally — the bulk handler treats blank-To as "today", which
  // is what you want 99% of the time, and forcing the user to clear an
  // auto-populated value before opening the calendar picker was friction.
  chrome.tabs.sendMessage(tab.id, { action: 'get-date' }, r => {
    if (!chrome.runtime.lastError && r?.date) {
      if (!$('bulk-from').value) { $('bulk-from').value = r.date; $('bulk-from-picker').value = r.date; }
    }
  });

  chrome.tabs.sendMessage(tab.id, { action: 'scrape' }, result => {
    if (chrome.runtime.lastError || !result) {
      setStatus('Could not read page. Refresh and try again.', 'error');
      return;
    }
    if (result.captchaChallenge) {
      setStatus('Cloudflare CAPTCHA showing on the SFTC tab — solve it, then reopen the popup.', 'error');
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
    if (dept) _detectedDept = String(dept);

    // Pre-fill From from the scraped court date if the page input didn't
    // give us one. Don't touch To.
    if (!$('bulk-from').value && result.rulings[0]?.['Court Date']) {
      const iso = parseDate(result.rulings[0]['Court Date']);
      if (iso) { $('bulk-from').value = iso; $('bulk-from-picker').value = iso; }
    }

    if (n === 0) { setStatus('No rulings found on this page.', 'warn'); return; }

    // Use `!= null` (not truthy) so reported_total === 0 still triggers when
    // the rulings DOM is non-empty — observed in stale-session pages where
    // the resultsCount label shows 0 but cached rulings remain rendered.
    const warn = result.reported_total != null && result.reported_total !== n
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
  // Inject _date from the page's date input so background commitToGitHub can file
  // the commit under the searched date even when rulings happen to be empty.
  const pageDate = parseDate($('bulk-from').value);
  const data = pageDate ? { ...scrapedData, _date: pageDate } : scrapedData;
  chrome.runtime.sendMessage(
    { action: 'commit', payload: { token: s.token, owner, repo, branch: s.branch || 'master', data } },
    res => {
      if (res?.error) {
        setStatus('Error: ' + res.error, 'error');
        $('send-btn').disabled = false;
      } else if (res?.duplicate) {
        setStatus(`Already submitted for this date — skipped. (${res.path})`, 'warn');
        $('send-btn').disabled = false;
      } else if (res?.stale) {
        setStatus(`Page held ${res.staleCount} stale rulings (Court Date ≠ search date) — committed empty marker → ${res.path}`, 'warn');
        $('send-btn').disabled = false;
      } else {
        setStatus(`Committed ${scrapedData.rulings.length} rulings → ${res.path}`, 'success');
      }
    }
  );
});

// ── Bulk status display ───────────────────────────────────────────────────────

function resetBulkButtons() {
  $('bulk-btn').disabled = false;
  $('auto-scan-btn').disabled = false;
  $('bulk-stop').style.display = 'none';
  $('bulk-resume').style.display = 'none';
}

function updateBulkStatus(job) {
  if (!job) return;
  if (job.fatalError) {
    setStatus(job.fatalError, 'error');
    resetBulkButtons();
  } else if (job.done) {
    const flushNote = job.lastFlushError ? ` — last flush: ${job.lastFlushError}` : '';
    // If everything was skipped (sibling tab held the claims, or every date
    // was already in the archive), say so explicitly. The previous "Done: 0
    // committed" wording read like a failure when in fact the run completed
    // cleanly.
    const total = (job.dates?.length || 0);
    let kind = job.errors > 0 ? 'warn' : 'success';
    let line;
    if (total > 0 && job.committed === 0 && job.errors === 0 && job.skipped === total) {
      line = `Done: all ${total} dates were already in the archive or being scanned by another tab — nothing to do here${flushNote}`;
      kind = 'success';
    } else {
      line = `Done: ${job.committed} committed, ${job.skipped} skipped, ${job.errors} errors${flushNote}`;
    }
    setStatus(line, kind);
    resetBulkButtons();
  } else if (!job.running && job.pausedForSession) {
    // SFTC either returned the "session expired" page or Cloudflare hit us
    // with a CAPTCHA challenge. Either way we've auto-reloaded the tab so
    // the user is now staring at the SFTC login / Cloudflare interstitial.
    // The background SW now auto-resumes as soon as the page is back to
    // normal (chrome.tabs.onUpdated event-driven, with a 1-min poll
    // backstop), so the Resume button is just a manual override.
    const reason = job.pauseReason === 'captcha'
      ? 'Cloudflare CAPTCHA challenge'
      : 'Session expired';
    setStatus(
      `${reason} at ${job.index + 1}/${job.dates?.length} (${job.currentDate || '…'}). ` +
      `Solve the challenge in the SFTC tab — the run resumes automatically once the page is back. ` +
      `(${job.committed} committed, ${job.skipped} skipped, ${job.errors} err so far)`,
      'warn'
    );
    $('bulk-btn').disabled = false;
    $('auto-scan-btn').disabled = false;
    $('bulk-stop').style.display = 'none';
    $('bulk-resume').style.display = 'block';
  } else if (!job.running) {
    // Stopped by user — neither fatalError nor done. Show what was completed.
    setStatus(
      `Stopped at ${job.index}/${job.dates?.length} — ` +
      `${job.committed} committed, ${job.skipped} skipped, ${job.errors} errors`,
      'warn'
    );
    resetBulkButtons();
  } else {
    const pct = job.dates?.length ? Math.round(job.index / job.dates.length * 100) : 0;
    setStatus(
      `[${job.index}/${job.dates?.length}] ${job.currentDate || '…'} — ` +
      `${job.committed} saved, ${job.skipped} skipped, ${job.errors} err — ${pct}% ` +
      `(close popup freely)`,
      'loading'
    );
    $('bulk-btn').disabled = true;
    $('bulk-stop').style.display = 'block';
    $('bulk-resume').style.display = 'none';
  }
}

// Keep popup in sync with background job even while open. The status
// strip up top is THIS tab's slice of _bulkJobs; the all-scans panel
// below renders every other tab's job so the user can monitor parallel
// scrapes from a single popup without juggling tabs.
chrome.storage.onChanged.addListener((changes, area) => {
  if (area !== 'local' || !changes._bulkJobs) return;
  const newAll = changes._bulkJobs.newValue || {};
  if (currentTabId != null) {
    const before = changes._bulkJobs.oldValue?.[currentTabId];
    const after  = newAll[currentTabId];
    if (JSON.stringify(before) !== JSON.stringify(after)) updateBulkStatus(after);
  }
  renderAllScans(newAll);
});

function renderAllScans(jobs) {
  const container = $('all-scans');
  const list      = $('all-scans-list');
  if (!container || !list) return;
  // Filter to active or recently-paused jobs from OTHER tabs. A "Done" job
  // from a different tab isn't useful here — the user won't act on it from
  // this popup, and it'd just clutter the panel.
  const entries = Object.entries(jobs || {})
    .filter(([tabId, job]) => {
      if (Number(tabId) === currentTabId) return false;
      if (!job) return false;
      return job.running || job.pausedForSession || (job.fatalError && !job.done);
    });
  if (!entries.length) {
    container.style.display = 'none';
    list.innerHTML = '';
    return;
  }
  container.style.display = 'block';
  list.innerHTML = entries.map(([tabId, job]) => {
    const total   = job.dates?.length || 0;
    const idx     = job.index || 0;
    const pct     = total ? Math.round(idx / total * 100) : 0;
    const dept    = job.department ? `Dept ${esc(job.department)}` : 'Dept ?';
    const counts  = `${job.committed || 0}✓ ${job.skipped || 0}↷ ${job.errors || 0}✗`;
    let stateLabel, stateColor;
    if (job.fatalError) {
      stateLabel = 'fatal';
      stateColor = '#c0392b';
    } else if (job.pausedForSession) {
      stateLabel = job.pauseReason === 'captcha' ? 'CAPTCHA' : 'session';
      stateColor = '#b8860b';
    } else if (job.running) {
      stateLabel = `${pct}%`;
      stateColor = '#2e6da4';
    } else {
      stateLabel = 'idle';
      stateColor = '#888';
    }
    // Date-range line: show what the OTHER tab is responsible for
    // ("scanning N → M") plus where it currently is. Without the range
    // a single tab's status looks like an isolated point and the user
    // can't tell whether it's about to overlap with the dates they're
    // about to start. The range is the actual date span of the run, in
    // chronological order regardless of forward/backward direction.
    const dates  = job.dates || [];
    let rangeLine = '';
    if (dates.length) {
      const first = dates[0];
      const last  = dates[dates.length - 1];
      const lo = first <= last ? first : last;
      const hi = first <= last ? last  : first;
      const dirArrow = first <= last ? '→' : '←';
      rangeLine = `<div style="font-size:0.68rem;color:#666">range ${esc(lo)} ${dirArrow} ${esc(hi)}</div>`;
    }
    const cur = job.currentDate ? esc(job.currentDate) : '…';
    return `<div style="display:flex;justify-content:space-between;align-items:center;padding:0.25rem 0;border-bottom:1px dashed #e8eef8;gap:0.4rem">
      <div style="flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">
        <strong>${dept}</strong> · ${idx}/${total} · now ${cur}
        ${rangeLine}
        <div style="font-size:0.68rem;color:#666">${counts}</div>
      </div>
      <span style="color:${stateColor};font-weight:600;font-size:0.7rem">${stateLabel}</span>
    </div>`;
  }).join('');
}

function esc(s) {
  return String(s ?? '').replace(/[&<>"]/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;',
  }[c]));
}

// Initial render of the all-scans panel — pull the full _bulkJobs map
// once at popup boot so the user sees in-flight runs immediately even if
// nothing has changed in the brief window since the popup opened.
chrome.storage.local.get('_bulkJobs').then(({ _bulkJobs }) => {
  renderAllScans(_bulkJobs || {});
});

// ── Bulk Scrape ───────────────────────────────────────────────────────────────

$('bulk-btn').addEventListener('click', async () => {
  const from = parseDate($('bulk-from').value);
  const to   = parseDate($('bulk-to').value) || localISO(new Date());
  if (!from || from > to) {
    setStatus('Enter a valid start date (YYYY-MM-DD or MM/DD/YYYY).', 'warn');
    return;
  }

  const s = await loadSettings();
  const err = validateSettings(s);
  if (err) { setStatus(err, 'error'); return; }

  const waitMs = parseInt($('bulk-wait').value) || 5_000;
  let dates  = weekdaysBetween(from, to);
  if (!dates.length) { setStatus('No weekdays in that range.', 'warn'); return; }

  const tab = await getActiveSftcTab();
  if (!tab?.url?.includes('webapps.sftc.org/tr/')) {
    setStatus('Navigate to the SFSC page first.', 'warn');
    return;
  }

  // Optional pre-filter: drop weekdays already covered (by parquet, raw/, or
  // the local commit log) so a re-run of an overlapping range doesn't pay
  // the SFTC round-trip just to hit the duplicate guard.
  let skippedCount = 0;
  if ($('skip-scanned').checked) {
    setStatus('Filtering out already-scanned dates…', 'loading');
    const scanned = await fetchScannedDates(s);
    if (scanned?.length) {
      const set = new Set(scanned);
      const before = dates.length;
      dates = dates.filter(d => !set.has(d));
      skippedCount = before - dates.length;
    }
    if (!dates.length) {
      setStatus(`All ${skippedCount} weekday(s) in that range are already scanned.`, 'success');
      return;
    }
  }

  // Apply the same direction radio used by Scan Unscanned Pages — without this
  // the manual range button silently ignored the toggle and always walked
  // oldest-first, even after the user picked "Backward (newest first)".
  const direction = getScanDirection();
  if (direction === 'backward') dates = [...dates].reverse();

  const settings = { token: s.token, repo: s.repo, branch: s.branch || 'master' };

  $('bulk-btn').disabled = true;
  $('bulk-stop').style.display = 'block';
  $('bulk-resume').style.display = 'none';
  $('send-btn').disabled = true;
  const skipNote = skippedCount ? ` (skipping ${skippedCount} already scanned)` : '';
  const dirNote = direction === 'backward' ? ', newest-first' : ', oldest-first';
  setStatus(`Starting background scrape of ${dates.length} dates${dirNote}${skipNote}…`, 'loading');

  const department = await detectDepartment();
  chrome.runtime.sendMessage(
    { action: 'start-bulk', payload: { dates, tabId: tab.id, settings, waitMs, department } },
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
  chrome.runtime.sendMessage({ action: 'stop-bulk', payload: { tabId: currentTabId } });
});

$('bulk-resume').addEventListener('click', async () => {
  const tab = await getActiveSftcTab();
  if (!tab?.url?.includes('webapps.sftc.org/tr/')) {
    setStatus('Navigate to the SFSC page first.', 'warn');
    return;
  }
  // Resume is bound to the tab that owns the paused job. The popup was
  // initially opened against `currentTabId`; if the active tab has since
  // changed, prefer the original (paused) tab so the resume routes correctly.
  const resumeTabId = currentTabId ?? tab.id;
  $('bulk-resume').style.display = 'none';
  $('bulk-stop').style.display = 'block';
  setStatus('Resuming next batch…', 'loading');
  chrome.runtime.sendMessage(
    { action: 'resume-bulk', payload: { tabId: resumeTabId } },
    res => {
      if (res?.error) {
        setStatus('Error resuming: ' + res.error, 'error');
        $('bulk-stop').style.display = 'none';
        $('bulk-resume').style.display = 'block';
      }
    }
  );
});

// ── Hotkey hint ───────────────────────────────────────────────────────────────

// Display the user's actual binding (which they may have customized) and provide
// a one-click jump to chrome://extensions/shortcuts.
chrome.commands.getAll(commands => {
  const cmd = commands.find(c => c.name === 'commit-and-next');
  const key = cmd?.shortcut || '';
  const keyEl = $('hotkey-key');
  const textEl = $('hotkey-text');
  if (!key) {
    textEl.innerHTML = '⚡ no hotkey set — <a href="#" id="set-hotkey" style="color:#5a8fc8">assign one</a> to commit &amp; load next day';
    $('set-hotkey')?.addEventListener('click', e => { e.preventDefault(); openShortcutsPage(); });
  } else {
    keyEl.textContent = key;
  }
});

// Firefox doesn't allow extensions to open about:addons / about:* pages
// programmatically — point users to the right place via the status bar instead.
function openShortcutsPage() {
  if (IS_FIREFOX) {
    setStatus('In Firefox: open about:addons → ⚙ menu → Manage Extension Shortcuts', 'warn');
    return;
  }
  chrome.tabs.create({ url: 'chrome://extensions/shortcuts' });
}

$('hotkey-config').addEventListener('click', e => { e.preventDefault(); openShortcutsPage(); });
$('auto-scan-btn').addEventListener('click', autoScanUnscanned);
$('jump-first').addEventListener('click', jumpToFirstGap);
$('jump-last').addEventListener('click',  jumpToResume);

// Persist the scan direction radio across popup opens so the user doesn't
// have to re-pick newest-first every time they reopen the extension.
chrome.storage.local.get('_scanDirection').then(({ _scanDirection }) => {
  if (_scanDirection === 'backward' || _scanDirection === 'forward') {
    const radio = document.querySelector(`input[name="scan-direction"][value="${_scanDirection}"]`);
    if (radio) radio.checked = true;
  }
});
document.querySelectorAll('input[name="scan-direction"]').forEach(r => {
  r.addEventListener('change', () => {
    chrome.storage.local.set({ _scanDirection: r.value });
  });
});

// Persist "Skip dates already scanned in this range" so contributors who
// always run with it on don't have to re-tick it every time they reopen
// the popup.
chrome.storage.local.get('_skipScanned').then(({ _skipScanned }) => {
  if (typeof _skipScanned === 'boolean') $('skip-scanned').checked = _skipScanned;
});
$('skip-scanned').addEventListener('change', () => {
  chrome.storage.local.set({ _skipScanned: $('skip-scanned').checked });
});

// ── Detach popup into a standalone window ───────────────────────────────────
// MV3 toolbar popups close every time focus leaves them, which is hostile to
// monitoring a long bulk scan: every click on the SFSC tab dismisses the
// progress display. Detach opens this same popup as a standalone browser
// window (chrome.windows.create with type: 'popup') that stays open until
// the user closes it. The window is bound to the SFTC tab id captured at
// detach time so all the per-tab status, claim, and resume routing keeps
// working even though "active tab in current window" no longer points at
// the SFTC page.

if (IS_DETACHED) {
  const btn = $('detach-btn');
  if (btn) btn.style.display = 'none';
} else {
  $('detach-btn').addEventListener('click', async () => {
    const tab = await getActiveSftcTab();
    if (!tab?.id) {
      setStatus('Open this from a SFSC tab to detach the popup.', 'warn');
      return;
    }
    const url = chrome.runtime.getURL('popup.html') + `?detached=1&tabId=${tab.id}`;
    chrome.windows.create({
      url, type: 'popup', width: 380, height: 720, focused: true,
    });
    // Close this transient popup so the user isn't left with two copies.
    window.close();
  });
}

// ── Tooltips toggle ──────────────────────────────────────────────────────────
// Each interactable element carries a data-tip attribute; CSS in popup.html
// renders the tooltip on hover but ONLY when body.tips-on is set. The toggle
// state persists across popup opens via chrome.storage.local.
function applyTipsState(on) {
  document.body.classList.toggle('tips-on', !!on);
  const btn = $('tips-toggle');
  if (btn) {
    btn.classList.toggle('on', !!on);
    btn.textContent = on ? '✓ Tips' : '? Tips';
  }
}
chrome.storage.local.get('_tipsOn').then(({ _tipsOn }) => applyTipsState(!!_tipsOn));
$('tips-toggle').addEventListener('click', async () => {
  const next = !document.body.classList.contains('tips-on');
  applyTipsState(next);
  chrome.storage.local.set({ _tipsOn: next });
});

// ── Diagnose ──────────────────────────────────────────────────────────────────

$('diag-btn').addEventListener('click', async () => {
  const tab = await getActiveSftcTab();
  if (!tab) { setStatus('No SFSC tab found.', 'warn'); return; }
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

// Restore any in-progress background bulk job for THIS tab. Other tabs may
// have their own jobs running independently; those are surfaced only in
// their own popups.
async function restoreOwnTabJob() {
  if (currentTabId == null) {
    const tab = await getActiveSftcTab();
    currentTabId = tab?.id ?? null;
  }
  if (currentTabId == null) return;
  chrome.runtime.sendMessage(
    { action: 'bulk-status', payload: { tabId: currentTabId } },
    job => {
      if (job && (job.running || job.pausedForSession)) updateBulkStatus(job);
    }
  );
}
restoreOwnTabJob();

checkAndDownloadUpdate();
init();
