// Scrapes the SFSC tentative rulings page and returns structured data.
// Runs on: https://webapps.sftc.org/tr/tr.dll*

function scrape() {
  const container = document.getElementById('resultsRulings');
  if (!container) {
    return { error: 'No results block found. Run a search on this page first.' };
  }

  const countEl = document.getElementById('resultsCount');
  const totalText = countEl ? countEl.textContent : '';
  const totalMatch = totalText.match(/\d+/);
  const reportedTotal = totalMatch ? parseInt(totalMatch[0]) : null;

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
  // 1. Find by visible label text "Court Date" — handles table-layout CGI forms
  for (const el of document.querySelectorAll('label, td, th')) {
    if (!/court\s*date/i.test(el.textContent)) continue;

    // <label for="...">
    if (el.tagName === 'LABEL' && el.htmlFor) {
      const inp = document.getElementById(el.htmlFor);
      if (inp) return inp;
    }
    // Input nested inside the same cell / label
    const nested = el.querySelector('input');
    if (nested) return nested;
    // Input in the immediately following sibling element
    const sibling = el.nextElementSibling;
    if (sibling) {
      const inp = sibling.tagName === 'INPUT' ? sibling : sibling.querySelector('input');
      if (inp) return inp;
    }
  }

  // 2. Try common name/id patterns
  for (const sel of [
    'input[name="HearingDt"]', 'input[name="hearingDt"]',
    'input[name*="Date" i]',   'input[id*="date" i]',
    'input[type="date"]',
  ]) {
    const el = document.querySelector(sel);
    if (el) return el;
  }

  // 3. Last resort: first text input in a form that has a submit button
  for (const form of document.querySelectorAll('form')) {
    if (form.querySelector('input[type="submit"], button[type="submit"]')) {
      const t = form.querySelector('input[type="text"]');
      if (t) return t;
    }
  }
  return null;
}

async function fillAndScrape(dateStr, waitMs = 7000) {
  const input = findDateInput();
  if (!input) return { error: 'No date input found on this page.' };

  const [y, m, d] = dateStr.split('-');
  input.value = `${m}/${d}/${y}`;
  input.dispatchEvent(new Event('input',  { bubbles: true }));
  input.dispatchEvent(new Event('change', { bubbles: true }));

  // Snapshot current results so we can detect a change
  const prevHTML = document.getElementById('resultsRulings')?.innerHTML ?? null;

  // Click the submit button that belongs to this input's form specifically
  const form = input.closest('form');
  const btn  = form?.querySelector('input[type="submit"], button[type="submit"]')
            ?? document.querySelector('input[type="submit"], button[type="submit"]');
  if (btn)   btn.click();
  else if (form) form.submit();
  else       return { error: 'No submit button found.' };

  // Poll for the results container to change (AJAX case)
  const deadline = Date.now() + waitMs;
  while (Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 500));
    const container = document.getElementById('resultsRulings');
    if (container && container.innerHTML !== prevHTML) return scrape();
  }

  // No DOM change — page probably did a full reload; signal the popup
  return { pending: true };
}

// ── Message listener ──────────────────────────────────────────────────────────

chrome.runtime.onMessage.addListener((msg, _sender, respond) => {
  if (msg.action === 'scrape') {
    respond(scrape());
    return true;
  }
  if (msg.action === 'fill-and-scrape') {
    fillAndScrape(msg.date, msg.waitMs).then(respond);
    return true;
  }
});
