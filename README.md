# SFSC Tentative Rulings

Daily archive of tentative rulings from the San Francisco Superior Court.

**[Browse the data →](https://aimesy.github.io/sfsc-tentatives/)**

## Repo layout

| Path | What |
|------|------|
| `tentatives.parquet` | Canonical dataset (all rulings, all departments) — kept for back-compat with downstream scripts |
| `data/tentatives-<N>.parquet` | Per-department slice the data browser fetches on demand |
| `data/manifest.json` | Description of every per-department parquet (rulings, size, latest date) |
| `raw/dept<N>/*.json` | Per-day raw scrapes, organised by department |
| `coverage/dept<N>.json` | Union of dates covered by parquet rows + raw filenames; the extension reads this to find unscanned days |
| `extension/` | Browser extension source (Chrome + Firefox) |
| `sfsc-extension.zip` | Pre-built, installable extension |
| `index.html` | Static data browser (served via GitHub Pages) |
| `ingest.py` | Merges raw JSON into the parquet |
| `update-readme.py` | Regenerates the per-department parquets, coverage files, and the sections below |

## Browser extension

Scrapes the [SFSC tentative rulings page](https://webapps.sftc.org/tr/tr.dll) and commits results directly to this repo. Works on Chrome and Firefox.

### Install (Chrome)

1. Download and unzip `sfsc-extension.zip`.
2. `chrome://extensions` → enable **Developer mode** → **Load unpacked** → pick the unzipped folder.
3. Open the popup → **⚙ Settings** → paste a GitHub PAT (needs `Contents: write`).

### Install (Firefox)

1. Download and unzip `sfsc-extension.zip`.
2. `about:debugging` → **This Firefox** → **Load Temporary Add-on…** → pick `manifest.json` from the unzipped folder. (Temporary add-ons are removed when Firefox restarts; for a permanent install, use Firefox Developer Edition or build a signed `.xpi`.)
3. Open the popup → **⚙ Settings** → paste a GitHub PAT (needs `Contents: write`).
4. To rebind the hotkey: `about:addons` → ⚙ menu → **Manage Extension Shortcuts**.

### Use

- **One day** — search a date on the rulings page, click **Send to GitHub**.
- **Date range** — fill From/To, click **Bulk Scrape Range**. Iterates every weekday in range, skipping weekends and California court holidays.
- **Auto-fill gaps** — **Scan Unscanned Pages** finds every weekday from the first scanned date to today that's missing, then bulk-scrapes them.
- **Stop** — halts the bulk run; in-flight commits still finish. Click **Resume** (⏭) to pick up from the day after the last commit.
- **Updates** — the popup checks GitHub for a newer `sfsc-extension.zip` and offers a one-click download.

## Browse

The static data browser at `index.html` lazy-loads each department on demand via the **📥 Database Downloads** dropdown in the header. Each entry shows the download percentage live, surfaces any fetch error inline, and once loaded, exposes a **Remove** action to drop the data and clear the autoload preference. The set of currently-loaded departments is persisted in `localStorage` and restored on the next visit.

## Ingest

Raw JSON pushed to `raw/dept<N>/` triggers `.github/workflows/ingest.yml`. The workflow waits 60 seconds before doing any work — any further pushes within that window cancel the in-flight run and start a fresh one (`cancel-in-progress: true`), so a 50-file bulk-scrape burst gets coalesced into a single ingest pass instead of 50 racing ones. Each pass diffs against the last bot commit, so any file that a previous cancelled run missed gets picked up automatically; `workflow_dispatch` with `mode: all-raw` re-ingests every raw JSON if a deeper repair is needed.

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

---

## Departments

<details>
<summary>**Department 302 — Civil Law & Motion** &nbsp;·&nbsp; 60,473 rulings &nbsp;·&nbsp; Latest: 2026-05-04 &nbsp;·&nbsp; 0 gaps</summary>

60,473 tentative rulings. Latest: 2026-05-04.

### Gaps (0)

_None — all weekdays in range are accounted for._

</details>
<details>
<summary>**Department 501 — Real Property Court** &nbsp;·&nbsp; 532 rulings &nbsp;·&nbsp; Latest: 2026-05-01 &nbsp;·&nbsp; 3 gaps</summary>

532 tentative rulings. Latest: 2026-05-01.

### Gaps (3)

- 2024-02-23
- 2024-03-14 → 2024-04-05
- 2024-04-12 → 2026-04-30

</details>
