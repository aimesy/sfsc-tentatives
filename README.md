# SFSC Tentative Rulings

Daily archive of tentative rulings from the San Francisco Superior Court.

**[Browse the data →](https://aimesy.github.io/sfsc-tentatives/)**

## Repo layout

| Path | What |
|------|------|
| `tentatives.parquet` | Canonical dataset (all rulings, all departments) |
| `raw/dept<N>/*.json` | Per-day raw scrapes, organised by department |
| `extension/` | Chrome extension source |
| `sfsc-extension.zip` | Pre-built, installable extension |
| `index.html` | Static data browser (served via GitHub Pages) |
| `ingest.py` | Merges raw JSON into the parquet |
| `update-readme.py` | Regenerates the department sections below |

## Chrome extension

Scrapes the [SFSC tentative rulings page](https://webapps.sftc.org/tr/tr.dll) and commits results directly to this repo.

### Install

1. Download and unzip `sfsc-extension.zip`.
2. `chrome://extensions` → enable **Developer mode** → **Load unpacked** → pick the unzipped folder.
3. Open the popup → **⚙ Settings** → paste a GitHub PAT (needs `Contents: write`).

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
<summary>**Department 302 — Civil Law & Motion** &nbsp;·&nbsp; 51,798 rulings &nbsp;·&nbsp; Latest: 2026-05-01 &nbsp;·&nbsp; 42 gaps</summary>

51,798 tentative rulings. Latest: 2026-05-01.

### Gaps (42)

- 2015-05-01
- 2016-09-16
- 2016-10-25
- 2017-06-21 → 2017-06-22
- 2018-03-30
- 2018-04-17 → 2019-12-31
- 2020-04-01 → 2020-05-14
- 2020-05-18
- 2020-05-20
- 2020-05-22
- 2020-05-27
- 2020-06-01
- 2020-06-08
- 2020-06-10
- 2020-06-15
- 2020-06-17
- 2020-08-03 → 2020-08-28
- 2021-05-03 → 2021-05-28
- 2022-09-23
- 2023-09-22
- 2023-12-21
- 2024-04-23 → 2024-04-24
- 2024-05-30
- 2024-07-09 → 2024-07-17
- 2024-09-05
- 2024-09-27
- 2024-10-08
- 2024-10-11 → 2024-10-15
- 2024-10-21
- 2024-11-12
- 2024-12-02
- 2024-12-04
- 2024-12-09 → 2024-12-16
- 2025-01-27 → 2025-02-19
- 2025-02-21
- 2025-04-14
- 2025-06-02 → 2025-06-17
- 2025-09-26
- 2025-10-07
- 2025-12-19
- 2026-02-05 → 2026-03-02
- 2026-03-04

</details>
