# SFSC Tentative Rulings

A searchable archive of every **tentative ruling** posted by the San Francisco Superior Court. While tentative rulings are freely available at <https://sf.courts.ca.gov/online-services/tentative-rulings>, they can only be found by date and case number. This repo allows numerous data operations that are not possible under that limited format, as discussed below. Departments covered:

- Department 204 (Probate)
- Department 301 (Discovery)
- Department 302 (Civil Law and Motion)
- Department 304 (Asbestos Law and Motion/Discovery)
- Department 501 (Real Property)

**[Open the searchable database →](https://aimesy.github.io/sfsc-tentatives/)**

## How to use the searchable database

Open <https://aimesy.github.io/sfsc-tentatives/>. The first time you visit, a **📥 Database Downloads** menu opens in the upper right; pick the department(s) you want to look at. Your selection is remembered, so the next visit auto-loads them.

Once data is loaded, you have a table you can:

- **Search** — type a case number, party name, or any phrase from the ruling text in the **Search** box.
- **Filter by date range** — set **From** and **To** to a window of hearing dates.
- **Filter by department, judge, or motion type** — use the dropdowns in the toolbar.
- **Add an arbitrary filter** — click **+ Filter** to build conditions like "Motion Type *is* Demurrer AND Outcome *is* Granted / Sustained AND Judge *contains* Karnow".
- **Filter a single column the way Excel does** — click the small **▾** filter icon in any column header to pick the values you want to keep (with a search box and Sort A→Z / Z→A buttons).
- **Sort** — click any column header.
- **See the full ruling** — click any row to open the ruling, the motion caption, the judge's full text, the contestation procedure block, and any remote-appearance instructions.
- **Export to CSV** — click **⬇ CSV** in the upper right to download whatever the current filters return. Opens in Excel or Google Sheets.
- **Charts** — click **📊 Charts** to add visualizations (rulings over time, top motion types, judges, outcome breakdowns, etc.). You can rearrange the panels by dragging them.
- **Tooltips** — toggle the **? Tips** button in the upper right to show short on-hover explanations of every interactive element.

Filters and the current page are saved in the URL, so you can bookmark or share a particular search.

## Browser extension (for contributors)

The browser extension at `sfsc-extension.zip` lets you load the SFSC tentative-rulings page in your browser, click a button, and add the day's rulings directly to the archive on GitHub. You only need this if you want to help fill in missing days or add new departments.

### Install (Chrome / Edge)

1. Download `sfsc-extension.zip` from this repository and unzip it.
2. Open `chrome://extensions`, turn on **Developer mode** (top right), click **Load unpacked**, and pick the unzipped folder.
3. Click the puzzle-piece icon → pin **SFSC Tentative Rulings** to the toolbar.
4. Click the extension's icon → **⚙ Settings**, and paste a GitHub Personal Access Token. (The token authorises the extension to commit on your behalf. Generate one at github.com → Settings → Developer settings → Personal access tokens → "Contents: write" permission on this repository.)

### Install (Firefox)

1. Download and unzip `sfsc-extension.zip`.
2. Open `about:debugging` → **This Firefox** → **Load Temporary Add-on…** → pick `manifest.json` from the unzipped folder. (Temporary add-ons are removed when Firefox closes — for a permanent install, use Firefox Developer Edition or build a signed `.xpi`.)
3. Click the extension icon → **⚙ Settings**, and paste a GitHub PAT.
4. To change the keyboard shortcut: `about:addons` → ⚙ menu → **Manage Extension Shortcuts**.

### Using the extension

Open the SFSC tentative rulings page (<https://webapps.sftc.org/tr/tr.dll>) in your browser, run a search for the date you want, then open the extension popup. The extension reads the page and offers several ways to commit the data:

- **One day at a time** — click **Send to GitHub**.
- **A date range** — fill **From** / **To** and click **Bulk Scrape Range**. The extension steps through every business day in the range, skipping weekends and California court holidays, and commits each day's rulings to the repository.
- **Skip days you've already done** — tick **"Skip dates already scanned in this range"** before clicking **Bulk Scrape Range** and the extension will only request days that aren't already in the archive.
- **Fill in every gap** — **Scan Unscanned Pages** automatically finds every business day from the first scanned date through today that's missing, and scrapes them.
- **Multiple tabs at once** — open the SFSC site in two (or more) browser tabs, click the extension icon in each, and start an independent scrape in each tab. The popup only shows progress for the tab it was opened from, and each tab can be stopped, resumed, or paused without affecting the others.
- **Pause / Resume** — the SFSC site logs you out after roughly fifty searches and challenges you with a CAPTCHA. The extension notices, pauses the run, and reloads the page so you see the CAPTCHA. Solve it, then click **Resume after CAPTCHA** and the run continues at the same date that triggered the timeout — no days lost.
- **Stop / Resume** — **Stop** halts the run. Use **⏭ Resume** to start again from the day after the last commit.
- **Tooltips** — toggle the **?** button in the popup's upper right to display short on-hover explanations of every button and field.
- **Keyboard shortcut** — `Alt+Shift+S` (default) commits the current page and loads the next business day in one keystroke. Useful for stepping through manually.
- **Updates** — when a newer version of the extension is published, the popup shows an **Update available** banner with one-click download.

## Glossary

- **Tentative ruling** — the court's preliminary written ruling on a motion, posted the day before the hearing. Becomes final unless a party "contests" it under the local rules.
- **Department** — a courtroom and the judge assigned to it. Department 204 hears probate matters; Department 301 hears discovery motions; Department 302 hears civil law-and-motion calendars; Department 304 hears asbestos law-and-motion matters and asbestos discovery motions; Department 501 hears real-property matters.
- **Motion type** — the kind of motion (demurrer, summary judgment, motion to compel, anti-SLAPP, etc.). Auto-classified from the calendar caption; you can correct misclassifications by filing a bug report from the ruling's detail view.
- **Outcome** — whether the motion was granted, denied, continued, taken off calendar, etc. Auto-classified from the ruling text; same correction path as motion type.

---

## For developers / archivists

<details>
<summary>Repository layout, data ingestion pipeline, and contribution mechanics</summary>

### Repo layout

| Path | What |
|------|------|
| `tentatives.parquet` | Canonical dataset (all rulings, all departments) — kept for back-compat with downstream scripts |
| `data/tentatives-<N>.parquet` | Per-department slice the data browser fetches on demand |
| `data/manifest.json` | Description of every per-department parquet (rulings, size, latest date) |
| `raw/dept<N>/*.json` | Per-day raw scrapes, organised by department |
| `coverage/dept<N>.json` | Union of dates covered by parquet rows + raw filenames; one of three inputs the extension reads to find unscanned days (alongside the live `raw/dept<N>/` listing and an in-extension commit log) |
| `extension/` | Browser extension source (Chrome + Firefox) |
| `sfsc-extension.zip` | Pre-built, installable extension |
| `index.html` | Static data browser (served via GitHub Pages) |
| `ingest.py` | Merges raw JSON into the parquet |
| `update-readme.py` | Regenerates the per-department parquets, coverage files, and the sections below |

### Ingest

Raw JSON pushed to `raw/dept<N>/` triggers `.github/workflows/ingest.yml`. The workflow throttles back-to-back runs: every push queues a run, runs execute sequentially via the concurrency group, and any run within 60 seconds of the last bot commit exits fast — so a 50-file bulk-scrape burst collapses to roughly one ingest plus quick no-ops. Each pass diffs against the last bot commit, so any file that a previous run missed gets picked up automatically; `workflow_dispatch` with `mode: all-raw` re-ingests every raw JSON if a deeper repair is needed.

Local:

```bash
pip install pandas pyarrow openpyxl
python ingest.py raw/dept302/2026-04-28-120000.json
```

To regenerate the per-department parquets, coverage files, and department sections below:

```bash
pip install pandas pyarrow holidays
python update-readme.py
```

</details>

---

## Departments

<details>
<summary><strong>Department 204 — Probate</strong> &nbsp;·&nbsp; 50,043 rulings &nbsp;·&nbsp; 2015-01-05 → 2026-05-04 &nbsp;·&nbsp; 1,205 hearing days &nbsp;·&nbsp; 1 gap</summary>

50,043 tentative rulings across 1,205 hearing days (2015-01-05 → 2026-05-04).

### Coverage

- **Hearing days with data:** 1,205 of 2,790 weekdays in range (43.2%)
- **Days scanned:** 1,560 (including days the court posted no rulings)
- **Earliest harvested:** 2015-01-05 (same as first hearing day)
- **Latest harvested:** 2026-05-04 (same as last hearing day)


### Gaps (1)

- 2015-01-09 → 2020-01-03

</details>
<details>
<summary><strong>Department 301 — Discovery</strong> &nbsp;·&nbsp; 4,168 rulings &nbsp;·&nbsp; 2024-03-19 → 2026-05-04 &nbsp;·&nbsp; 373 hearing days &nbsp;·&nbsp; 1 gap</summary>

4,168 tentative rulings across 373 hearing days (2024-03-19 → 2026-05-04).

> _Department 301 tentatives are not available online before 2024-03-19 — earlier dates are excluded from gap-finding and bulk scraping._

### Coverage

- **Hearing days with data:** 373 of 522 weekdays in range (71.5%)
- **Days scanned:** 525 (including days the court posted no rulings)
- **Earliest harvested:** 2017-10-02
- **Latest harvested:** 2026-05-04 (same as last hearing day)


### Gaps (1)

- 2017-10-05 → 2024-03-18

</details>
<details>
<summary><strong>Department 302 — Civil Law and Motion</strong> &nbsp;·&nbsp; 60,463 rulings &nbsp;·&nbsp; 2014-01-02 → 2026-05-04 &nbsp;·&nbsp; 2,999 hearing days &nbsp;·&nbsp; 0 gaps</summary>

60,463 tentative rulings across 2,999 hearing days (2014-01-02 → 2026-05-04).

### Coverage

- **Hearing days with data:** 2,999 of 3,038 weekdays in range (98.7%)
- **Days scanned:** 3,052 (including days the court posted no rulings)
- **Earliest harvested:** 2014-01-01
- **Latest harvested:** 2026-05-04 (same as last hearing day)


### Gaps (0)

_None — all weekdays in range are accounted for._

</details>
<details>
<summary><strong>Department 304 — Asbestos Law and Motion/Discovery</strong> &nbsp;·&nbsp; 56 rulings &nbsp;·&nbsp; 2025-04-08 → 2025-06-03 &nbsp;·&nbsp; 9 hearing days &nbsp;·&nbsp; 1 gap</summary>

56 tentative rulings across 9 hearing days (2025-04-08 → 2025-06-03).

### Coverage

- **Hearing days with data:** 9 of 40 weekdays in range (22.5%)
- **Days scanned:** 49 (including days the court posted no rulings)
- **Earliest harvested:** 2025-04-08 (same as first hearing day)
- **Latest harvested:** 2026-05-01


### Gaps (1)

- 2025-06-16 → 2026-04-30

</details>
<details>
<summary><strong>Department 501 — Real Property Court</strong> &nbsp;·&nbsp; 22,231 rulings &nbsp;·&nbsp; 2017-01-03 → 2026-05-04 &nbsp;·&nbsp; 2,261 hearing days &nbsp;·&nbsp; 0 gaps</summary>

22,231 tentative rulings across 2,261 hearing days (2017-01-03 → 2026-05-04).

### Coverage

- **Hearing days with data:** 2,261 of 2,296 weekdays in range (98.5%)
- **Days scanned:** 2,296 (including days the court posted no rulings)
- **Earliest harvested:** 2017-01-03 (same as first hearing day)
- **Latest harvested:** 2026-05-04 (same as last hearing day)


### Gaps (0)

_None — all weekdays in range are accounted for._

</details>
