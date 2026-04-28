// Scrapes the SFSC tentative rulings page and returns structured data.
// Runs on: https://webapps.sftc.org/tr/tr.dll*

// Judge code → full name (derived from sftc-judges-roster-alpha-public-022024.pdf)
const JUDGE_MAP = {
  RBU: 'Richard B. Ulmer Jr.',    RCE: 'Rochelle C. East',
  CK:  'Curtis E.A. Karnow',      RCD: 'Richard C. Darwin',
  JMQ: 'Joseph M. Quinn',         EHG: 'Ernest H. Goldsmith',
  EG:  'Ernest H. Goldsmith',     JPT: 'Judge Pro Tem',
  MB:  'Michael Begert',          SRB: 'Suzanne Ramos Bolanos',
  SMB: 'Susan M. Breall',         TMC: 'Teresa M. Caffese',
  BEC: 'Bruce E. Chan',           RCC: 'Roger C. Chan',
  AYC: 'Andrew Y.S. Cheng',       AMC: 'A. Marisa Chun',
  LC:  'Linda Colfax',            BC:  'Brendan Conroy',
  AC:  'Anne Costin',             CC:  'Charles Crompton',
  HMD: 'Harry M. Dorfman',        MEE: 'Maria E. Evangelista',
  SKF: 'Samuel K. Feng',          BLF: 'Brian L. Ferrall',
  ERF: 'Eric R. Fleming',         DAF: 'Daniel A. Flores',
  SJF: 'Simon J. Frankel',        CG:  'Carolyn Gold',
  ARG: 'Alexandra Robert Gordon', CFH: 'Charles F. Haines',
  CH:  'Chris Hite',              VMH: 'Victor M. Hwang',
  KK:  'Kathleen Kelly',          ACM: 'Anne-Christine Massullo',
  MM:  'Michael McNaughton',      RCM: 'Ross C. Moody',
  SMM: 'Stephen M. Murphy',       VP:  'Vedica Puri',
  MJR: 'Murlene J. Randle',       SMR: 'Sharon M. Reardon',
  MR:  'Michael Rhoads',          RR:  'Russ Roeca',
  JSR: 'Jeffrey S. Ross',         GCS: 'Gerardo C. Sandoval',
  EPS: 'Ethan P. Schulman',       PST: 'Patrick S. Thompson',
  MT:  'Michelle Tong',           CV:  'Christine Van Aken',
  RLW: 'Rebecca L. Wightman',     MFW: 'Monica F. Wiley',
  KW:  'Kenneth Wine',            MEW: 'Mary E. Wiss',
  GLW: 'Garrett L. Wong',         BCW: 'Braden C. Woods',
  ESP: 'Ethan P. Schulman',
  RU:  'Richard B. Ulmer Jr.',    RE:  'Rochelle C. East',
  CEK: 'Curtis E.A. Karnow',      JQ:  'Joseph M. Quinn',
  SB:  'Suzanne Ramos Bolanos',   AYSC:'Andrew Y.S. Cheng',
  BH:  'Judge Pro Tem: Bruce Highman',     DM:  'Judge Pro Tem: David McDonald',
  PC:  'Judge Pro Tem: Peter Catalanotti', TC:  'Judge Pro Tem: Tom Cohen',
  PR:  'Judge Pro Tem: Paul Renne',        SBS: 'Judge Pro Tem: Steven B. Stein',
  AM:  'Judge Pro Tem: Aaron Minnis',      NL:  'Judge Pro Tem: Noah Lebowitz',
  NJL: 'Judge Pro Tem: Noah J. Lebowitz',  PVZ: 'Judge Pro Tem: Peter Van Zandt',
  SM:  'Judge Pro Tem: Steven Murphy',     PJT: 'Judge Pro Tem',
  NJG: 'Judge Pro Tem: Naomi Jane Gray',   DR:  'Judge Pro Tem: Douglas Robbins',
  JF:  'Judge Pro Tem: James Fleming',     GD:  'Gail Dekreon',
  HK:  'Harold E. Kahn',                   HEK: 'Harold E. Kahn',
  MJM: 'Marla J. Miller',                  AJR: 'A James Robertson II',
  PJB: 'Peter J. Busch',                   JKS: 'John K. Stewart',
  AB:  'Angela Bradstreet',
};

function extractJudge(rulingText) {
  const m = rulingText.match(/=\s*\(?\s*\d+\s*\/\s*([A-Za-z]+)\s*\)?\s*\.?\s*$/);
  if (!m) return null;
  const code = m[1].toUpperCase();
  if (code === 'JPT') {
    const pt = rulingText.match(/Pro Tem Judge\s+([A-Z][A-Za-z.]+(?:\s+[A-Z][A-Za-z.]+)*?)(?:,|;|\s+a\s+member|\s+member|\s+has been|\s+recuses)/);
    if (pt) return `Judge Pro Tem: ${pt[1].trim()}`;
    return 'Judge Pro Tem';
  }
  return JUDGE_MAP[code] || null;
}

function scrape() {
  if (/session\s+has\s+expired|your\s+session\s+(has\s+)?expired|session\s+timed?\s+out/i.test(document.body?.innerText || '')) {
    return { sessionExpired: true };
  }

  const container = document.getElementById('resultsRulings');
  if (!container) {
    return { error: 'No results block found. Run a search on this page first.' };
  }

  const countEl = document.getElementById('resultsCount');
  const totalText = countEl ? countEl.textContent : '';
  const totalMatch = totalText.match(/Total Records Found\s+(\d+)/i);
  const reportedTotal = totalMatch ? parseInt(totalMatch[1]) : null;

  const h4 = document.querySelector('h4');
  let department = '302';
  if (h4) {
    const m = h4.textContent.match(/Department\s+(\d+)/i);
    if (m) department = m[1];
  }

  const rulings = [];
  let current = {};

  for (const tr of container.querySelectorAll('tr')) {
    const headerTd = tr.querySelector('td.dataHeader');
    if (!headerTd) {
      if (current['Case Number']) {
        rulings.push({ ...current });
        current = {};
      }
      continue;
    }

    const field = headerTd.textContent.replace(':', '').trim();
    const tds   = tr.querySelectorAll('td');
    const valueTd = tds[2] || tds[tds.length - 1];
    const value   = valueTd ? valueTd.innerText.trim() : '';

    if (['Case Number', 'Case Title', 'Court Date', 'Calendar Matter', 'Rulings'].includes(field)) {
      current[field] = value;
    }
  }
  if (current['Case Number']) rulings.push({ ...current });

  // Auto-populate Judge from the code at the end of each ruling
  for (const r of rulings) {
    if (r.Rulings) {
      const judge = extractJudge(r.Rulings);
      if (judge) r.Judge = judge;
    }
  }

  return {
    department,
    scraped_at:     new Date().toISOString(),
    source_url:     window.location.href,
    reported_total: reportedTotal,
    rulings,
  };
}

// ── Auto-navigation helpers ───────────────────────────────────────────────────

function findDateInput() {
  // 1. Specific known selectors — most reliable, check first
  for (const sel of [
    'input[name="DatePick"]', 'input[id="DatePick"]',
    'input.hasDatepicker',
    'input[name="HearingDt"]', 'input[name="hearingDt"]',
  ]) {
    const el = document.querySelector(sel);
    if (el) return el;
  }

  // 2. Find by "Court Date" label text — only leaf/row elements, not div containers
  //    (div containers may match but querySelector returns the wrong first input)
  for (const el of document.querySelectorAll('label, td, th, p')) {
    if (!/court\s*date/i.test(el.textContent)) continue;

    if (el.tagName === 'LABEL' && el.htmlFor) {
      const inp = document.getElementById(el.htmlFor);
      if (inp) return inp;
    }
    const nested = el.querySelector('input');
    if (nested) return nested;
    const sibling = el.nextElementSibling;
    if (sibling) {
      const inp = sibling.tagName === 'INPUT' ? sibling : sibling.querySelector('input');
      if (inp) return inp;
    }
  }

  // 3. Generic name/id patterns
  for (const sel of ['input[name*="Date" i]', 'input[id*="date" i]', 'input[type="date"]']) {
    const el = document.querySelector(sel);
    if (el) return el;
  }

  // 4. Last resort: first text input in a form
  for (const form of document.querySelectorAll('form')) {
    const t = form.querySelector('input[type="text"]');
    if (t) return t;
  }
  return null;
}

async function fillAndScrape(dateStr, waitMs = 2000) {
  const input = findDateInput();
  if (!input) return { error: 'No date input found on this page.' };

  const jq = window.jQuery || window.$;
  const prevHTML = document.getElementById('resultsRulings')?.innerHTML ?? null;

  if (jq && jq(input).data('datepicker')) {
    // datepicker('setDate') updates internal state and all hidden fields;
    // val() alone only changes the display text and leaves the picker out of sync.
    jq(input).datepicker('setDate', new Date(dateStr + 'T12:00:00'));
    jq(input).trigger('change');
  } else {
    // Fallback: set ISO string directly (matches the expected yy-mm-dd format)
    input.value = dateStr;
    input.dispatchEvent(new Event('input',  { bubbles: true }));
    input.dispatchEvent(new Event('change', { bubbles: true }));
    if (jq) jq(input).trigger('change');
  }

  // Give an AJAX auto-search a moment to fire before looking for a submit button
  await new Promise(r => setTimeout(r, 400));
  const earlyContainer = document.getElementById('resultsRulings');
  if (earlyContainer && earlyContainer.innerHTML !== prevHTML) return scrape();

  // Fall back to explicit form submission (full-page-reload sites)
  const form = input.closest('form');

  function findSearchButton(container) {
    // Standard submit-type buttons first
    const std = container?.querySelector('input[type="submit"], input[type="image"], button[type="submit"]');
    if (std) return std;
    // Any button/input whose visible text matches "search"
    for (const el of (container ?? document).querySelectorAll('button, input[type="button"]')) {
      if (/^\s*search\s*$/i.test(el.value || el.textContent)) return el;
    }
    return null;
  }

  const btn = findSearchButton(form) ?? findSearchButton(document);
  if (btn)       btn.click();
  else if (form) form.submit();
  else           return { error: 'No submit button or auto-search found.' };

  const deadline = Date.now() + waitMs;
  while (Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 500));
    const container = document.getElementById('resultsRulings');
    if (container && container.innerHTML !== prevHTML) return scrape();
  }
  return { pending: true };
}

// ── Message listener ──────────────────────────────────────────────────────────

function diagnose() {
  const input = findDateInput();
  const form  = input?.closest('form');
  const btn   = form?.querySelector('input[type="submit"], input[type="image"], button[type="submit"]')
             ?? document.querySelector('input[type="submit"], input[type="image"], button[type="submit"]');

  const allForms = [...document.querySelectorAll('form')].map(f => ({
    action: f.action,
    method: f.method,
    inputs: [...f.querySelectorAll('input')].map(i => ({
      name: i.name, id: i.id, type: i.type, value: i.value,
    })),
  }));

  return {
    foundInput: input ? { name: input.name, id: input.id, type: input.type } : null,
    formAction: form?.action ?? null,
    btnText:    btn ? (btn.value || btn.textContent).trim() : null,
    allForms,
  };
}

chrome.runtime.onMessage.addListener((msg, _sender, respond) => {
  if (msg.action === 'scrape') {
    respond(scrape());
    return true;
  }
  if (msg.action === 'fill-and-scrape') {
    fillAndScrape(msg.date, msg.waitMs).then(respond);
    return true;
  }
  if (msg.action === 'get-date') {
    const input = findDateInput();
    if (!input?.value) { respond({}); return true; }
    const raw = input.value.trim();
    // Normalise MM/DD/YYYY → YYYY-MM-DD
    const m = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    const date = m
      ? `${m[3]}-${m[1].padStart(2,'0')}-${m[2].padStart(2,'0')}`
      : (/^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : null);
    respond(date ? { date } : {});
    return true;
  }
  if (msg.action === 'diagnose') {
    respond(diagnose());
    return true;
  }
});
