# SFSC Tentative Rulings

Tentative rulings from the San Francisco Superior Court, archived by department.

**[Browse the data →](https://aimesy.github.io/sfsc-tentatives/)**

---

## Repository layout

| Path | Description |
|------|-------------|
| `tentatives.parquet` | Canonical dataset (all departments) |
| `raw/dept<N>/` | Raw JSON exports per department |
| `extension/` | Chrome extension source |
| `sfsc-extension.zip` | Installable extension package |
| `ingest.py` | Ingests raw JSON into the parquet |
| `index.html` | Data browser (served via GitHub Pages) |

## Browser extension

The extension scrapes the [SFSC tentative rulings page](https://webapps.sftc.org/tr/tr.dll) and commits results directly to this repository.

### Install

1. Download `sfsc-extension.zip` and unzip it.
2. Open `chrome://extensions`, enable **Developer mode**, click **Load unpacked**, select the unzipped folder.
3. Open the extension popup → **Settings** → enter your GitHub PAT (needs `Contents: write`) and save.

### Use

- **Single day:** navigate to the rulings page, run a search, then click **Send to GitHub**.
- **Bulk scrape:** enter a date range and click **Bulk Scrape Range**. The extension iterates every weekday, commits each date automatically.
- **Stop:** click **Stop** to halt after the current date finishes; nothing in-flight is lost.
- **Updates:** click **📦 Check for updates** to auto-download the latest `sfsc-extension.zip`.

## Ingest pipeline

New JSON files pushed to `raw/dept<N>/` trigger the GitHub Actions workflow (`.github/workflows/ingest.yml`), which runs `ingest.py` to merge them into `tentatives.parquet`.

To ingest locally:

```bash
pip install pandas pyarrow openpyxl
python ingest.py raw/dept302/2026-04-28-120000.json
```

## Maintaining this README

Run `update-readme.py` to refresh the department sections with current stats:

```bash
pip install pandas pyarrow holidays
python update-readme.py
```

---

## Departments

<details>
<summary>**Department 302 — Civil Law & Motion** &nbsp;·&nbsp; 46,113 rulings &nbsp;·&nbsp; Latest: 2026-04-28 &nbsp;·&nbsp; 45 gaps</summary>

46,113 tentative rulings. Latest: 2026-04-28.

### Gaps (45)

- 2015-05-01
- 2016-04-01
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
- 2024-07-05 → 2024-07-22
- 2024-07-29
- 2024-07-31
- 2024-08-02
- 2024-08-06
- 2024-08-08
- 2024-08-12
- 2024-08-14 → 2024-08-27
- 2024-08-29 → 2024-08-30
- 2024-09-04 → 2024-09-16
- 2024-09-18
- 2024-09-20 → 2024-09-24
- 2024-09-26 → 2024-09-30
- 2024-10-02 → 2024-10-17
- 2024-10-21
- 2024-10-23 → 2024-10-28
- 2024-10-30 → 2024-10-31
- 2024-11-04 → 2024-11-18
- 2024-11-20 → 2025-01-22
- 2025-01-24 → 2026-04-23
- 2026-04-27

</details>
