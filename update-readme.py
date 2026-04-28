#!/usr/bin/env python3
"""Regenerate README.md from tentatives.parquet — one collapsible section per department."""

import re
import pandas as pd
import holidays as hol
from datetime import date, timedelta
from pathlib import Path

HERE   = Path(__file__).parent
README = HERE / 'README.md'

DEPT_NAMES = {
    '302': 'Department 302 — Civil Law & Motion',
}

STATIC_TOP = """\
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

def dept_section(dept: str, df_dept: pd.DataFrame) -> str:
    name    = DEPT_NAMES.get(dept, f'Department {dept}')
    count   = len(df_dept)
    dates   = sorted(df_dept['court_date'].unique())
    min_d   = dates[0]
    max_d   = dates[-1]
    checked = set(dates)

    holidays = ca_court_holidays(int(min_d[:4]), int(max_d[:4]))
    gaps     = find_gap_runs(min_d, max_d, checked, holidays)
    n_gaps   = len(gaps)

    summary = (f'**{name}** &nbsp;·&nbsp; {count:,} rulings'
               f' &nbsp;·&nbsp; Latest: {max_d}'
               f' &nbsp;·&nbsp; {n_gaps} gap{"s" if n_gaps != 1 else ""}')

    body = f"""\

{count:,} tentative rulings. Latest: {max_d}.

### Gaps ({n_gaps})

{format_gaps(gaps)}

"""

    return f'<details>\n<summary>{summary}</summary>\n{body}</details>\n'

def main():
    if not (HERE / 'tentatives.parquet').exists():
        print('tentatives.parquet not found'); return

    df = pd.read_parquet(HERE / 'tentatives.parquet')
    df['court_date'] = pd.to_datetime(df['court_date']).dt.date.astype(str)

    sections = ''
    for dept in sorted(df['department'].unique()):
        sections += dept_section(dept, df[df['department'] == dept])

    content = STATIC_TOP + '## Departments\n\n' + sections
    README.write_text(content)
    print(f'Updated README.md — {len(df["department"].unique())} department(s)')

if __name__ == '__main__':
    main()
