#!/usr/bin/env python3
"""Regenerate README.md from tentatives.parquet — one collapsible section per department."""

import json
import pandas as pd
import holidays as hol
from datetime import date, timedelta
from pathlib import Path

HERE     = Path(__file__).parent
README   = HERE / 'README.md'
COVERAGE = HERE / 'coverage'

DEPT_NAMES = {
    '302': 'Department 302 — Civil Law & Motion',
}

STATIC_TOP = """\
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

"""

def ca_court_holidays(min_year: int, max_year: int) -> set[str]:
    """Return ISO date strings for California court holidays in the given year range.

    Combines CA state public holidays with federal government holidays (to
    capture Columbus Day), then manually adds Lincoln's Birthday (Feb 12),
    which is a California legal holiday (Gov. Code § 6700) not included in
    the holidays library's CA subdivision.
    """
    years = range(min_year, max_year + 1)
    ca_public = hol.country_holidays('US', subdiv='CA', years=years)
    us_gov    = hol.country_holidays('US', categories=hol.GOVERNMENT, years=years)
    combined: set[date] = set(ca_public.keys()) | set(us_gov.keys())

    for year in years:
        lincoln = date(year, 2, 12)
        if lincoln.weekday() == 5:      # Saturday → observe Friday
            lincoln = date(year, 2, 11)
        elif lincoln.weekday() == 6:    # Sunday → observe Monday
            lincoln = date(year, 2, 13)
        combined.add(lincoln)

    return {d.isoformat() for d in combined}


def find_gap_runs(min_date: str, max_date: str, checked: set,
                  court_holidays: set | None = None) -> list[tuple[str, str]]:
    """Returns (start, end) tuples for each gap of missing weekdays.

    Weekends and court holidays are skipped — they don't open or close a gap.
    A gap closes only when a weekday with data is encountered.
    """
    court_holidays = court_holidays or set()
    d   = date.fromisoformat(min_date)
    end = date.fromisoformat(max_date)
    runs = []
    run_start = run_end = None
    while d <= end:
        if d.weekday() < 5 and d.isoformat() not in court_holidays:
            if d.isoformat() not in checked:
                if run_start is None:
                    run_start = d.isoformat()
                run_end = d.isoformat()
            else:
                if run_start is not None:
                    runs.append((run_start, run_end))
                    run_start = run_end = None
        d += timedelta(days=1)
    if run_start is not None:
        runs.append((run_start, run_end))
    return runs

def format_gaps(runs: list[tuple[str, str]]) -> str:
    if not runs:
        return '_None — all weekdays in range are accounted for._'
    lines = []
    for start, end in runs:
        lines.append(f'- {start}' if start == end else f'- {start} → {end}')
    return '\n'.join(lines)

def scraped_dates_for_dept(dept: str) -> set[str]:
    """Dates we have raw scrape evidence for, derived from filenames in
    raw/dept<N>/. A date with a raw file is *not* a gap even if no rulings
    landed in the parquet for it (e.g. the page returned zero tentatives,
    or returned tentatives whose hearings are on a different date)."""
    raw_dir = HERE / 'raw' / f'dept{dept}'
    if not raw_dir.is_dir():
        return set()
    out = set()
    for p in raw_dir.glob('*.json'):
        # Filenames are <YYYY-MM-DD>-<HHMMSS>.json
        stem = p.stem
        if len(stem) >= 10 and stem[4] == '-' and stem[7] == '-':
            out.add(stem[:10])
    return out


def dept_section(dept: str, df_dept: pd.DataFrame) -> str:
    name    = DEPT_NAMES.get(dept, f'Department {dept}')
    count   = len(df_dept)
    dates   = set(df_dept['court_date'].unique())
    scraped = scraped_dates_for_dept(dept)
    checked = dates | scraped

    if not checked:
        return ''

    min_d        = min(checked)
    max_checked  = max(checked)
    latest_data  = max(dates) if dates else max_checked

    holidays = ca_court_holidays(int(min_d[:4]), int(max_checked[:4]))
    gaps     = find_gap_runs(min_d, max_checked, checked, holidays)
    n_gaps   = len(gaps)

    summary = (f'**{name}** &nbsp;·&nbsp; {count:,} rulings'
               f' &nbsp;·&nbsp; Latest: {latest_data}'
               f' &nbsp;·&nbsp; {n_gaps} gap{"s" if n_gaps != 1 else ""}')

    body = f"""\

{count:,} tentative rulings. Latest: {latest_data}.

### Gaps ({n_gaps})

{format_gaps(gaps)}

"""

    return f'<details>\n<summary>{summary}</summary>\n{body}</details>\n'

def write_coverage(dept: str, df_dept: pd.DataFrame):
    """Write coverage/dept<N>.json — the union of dates that appear in the
    parquet (court_date) and dates with a raw scrape file. The browser
    extension uses this to decide which dates still need scraping; without
    it, the extension only sees raw filenames and treats every parquet-only
    date as unscanned (the historical Excel imports populated 2017-2024
    rulings without any raw files)."""
    parquet_dates = set(df_dept['court_date'].dropna().unique())
    file_dates    = scraped_dates_for_dept(dept)
    covered       = sorted(parquet_dates | file_dates)
    COVERAGE.mkdir(exist_ok=True)
    out = COVERAGE / f'dept{dept}.json'
    out.write_text(json.dumps({
        'department': dept,
        'covered':    covered,
        'min':        covered[0] if covered else None,
        'max':        covered[-1] if covered else None,
        'count':      len(covered),
    }, indent=0, separators=(',', ':')))


def main():
    if not (HERE / 'tentatives.parquet').exists():
        print('tentatives.parquet not found'); return

    df = pd.read_parquet(HERE / 'tentatives.parquet')
    df['court_date'] = pd.to_datetime(df['court_date']).dt.date.astype(str)

    sections = ''
    for dept in sorted(df['department'].unique()):
        sub = df[df['department'] == dept]
        sections += dept_section(dept, sub)
        write_coverage(dept, sub)

    content = STATIC_TOP + '## Departments\n\n' + sections
    README.write_text(content)
    print(f'Updated README.md and coverage/ for {len(df["department"].unique())} department(s)')

if __name__ == '__main__':
    main()
