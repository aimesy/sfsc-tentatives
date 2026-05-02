# SFSC Tentative Rulings

Daily archive of tentative rulings from the San Francisco Superior Court.

**[Browse the data →](https://aimesy.github.io/sfsc-tentatives/)**

## Repo layout

| Path | What |
|------|------|
| `tentatives.parquet` | Canonical dataset (all rulings, all departments) |
| `raw/dept<N>/*.json` | Per-day raw scrapes, organised by department |
| `extension/` | Browser extension source (Chrome + Firefox) |
| `sfsc-extension.zip` | Pre-built, installable extension |
| `index.html` | Static data browser (served via GitHub Pages) |
| `ingest.py` | Merges raw JSON into the parquet |
| `update-readme.py` | Regenerates the department sections below |

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

## Ingest

Raw JSON pushed to `raw/dept<N>/` triggers `.github/workflows/ingest.yml`, which runs `ingest.py` to merge the new rows into `tentatives.parquet` and refresh this README.

Local:

```bash
pip install pandas pyarrow openpyxl
python ingest.py raw/dept302/2026-04-28-120000.json
```

To regenerate just the department sections below:

```bash
pip install pandas pyarrow holidays
python update-readme.py
```

---

## Departments

<details>
<summary>**Department 302 — Civil Law & Motion** &nbsp;·&nbsp; 58,679 rulings &nbsp;·&nbsp; Latest: 2026-05-04 &nbsp;·&nbsp; 6 gaps</summary>

58,679 tentative rulings. Latest: 2026-05-04.

### Gaps (6)

- 2025-06-02 → 2025-06-17
- 2025-09-26
- 2025-10-07
- 2025-12-19
- 2026-02-05 → 2026-03-02
- 2026-03-04

</details>
