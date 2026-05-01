#!/usr/bin/env python3
"""
Ingest tentative rulings and maintain tentatives.parquet + tentatives.db.

Usage:
    python ingest.py                         # rebuild from all source files
    python ingest.py path/to/new.xlsx        # append a new export
    python ingest.py path/to/new.json        # append a new json export (extension output)

tentatives.parquet  — canonical dataset, committed to git (~10 MB)
tentatives.db       — local SQLite for querying, gitignored (~100 MB)

JSON formats accepted:
  Legacy array:  [{Case Number, Case Title, Court Date, Calendar Matter, Rulings}, ...]
  Extension:     {department, scraped_at, source_url, rulings: [...]}
"""

import sys
import json
import re
import sqlite3
import hashlib
from pathlib import Path
from datetime import datetime, date, time

try:
    import pandas as pd
except ImportError:
    sys.exit("Missing dependency: pip install pandas pyarrow openpyxl")

try:
    import openpyxl
except ImportError:
    sys.exit("Missing dependency: pip install openpyxl")

HERE    = Path(__file__).parent
PARQUET = HERE / "tentatives.parquet"
DB_PATH = HERE / "tentatives.db"

# Load judge code → name mapping from judges.json
_judges_path = HERE / "judges.json"
if _judges_path.exists():
    _judge_db = json.loads(_judges_path.read_text())
    JUDGE_CODE_MAP = {k: v["name"] for k, v in _judge_db.get("code_map", {}).items()}
else:
    JUDGE_CODE_MAP = {}


def extract_judge(ruling_text):
    if not ruling_text:
        return None
    # Trailing tag forms observed in the wild:
    #   =(302/CK)  =(D302/CK)  (302/CK)  =(JPT)  =(525/JPT)  =(JPT/525)
    #   +(302/HEK) =(302.JMQ)  =(HEK)    (D302)  =(D525)     =(525)
    # Optional [=+] prefix; required parens; optional D before digits;
    # separator may be / . , or whitespace; optional trailing period.
    m = re.search(r'[=+]?\s*\(\s*([A-Za-z0-9][A-Za-z0-9\s/.,]{0,15})\s*\)\s*\.?\s*$', ruling_text)
    if not m:
        return None
    # Pick the first letter-only run that isn't a bare D dept-marker.
    # Dept-only tags like (D302) yield no code → None (data genuinely lacks a judge code).
    code = next((c.upper() for c in re.findall(r'[A-Za-z]+', m.group(1)) if c.upper() != 'D'), None)
    if not code:
        return None
    if code == 'JPT':
        pt = re.search(
            r'Pro Tem Judge\s+([A-Z][A-Za-z.]+(?:\s+[A-Z][A-Za-z.]+)*?)'
            r'(?:,|;|\s+a\s+member|\s+member|\s+has been|\s+recuses)',
            ruling_text
        )
        if pt:
            return f"Judge Pro Tem: {pt.group(1).strip()}"
        return "Judge Pro Tem"
    return JUDGE_CODE_MAP.get(code)

COLUMNS = ["department", "case_number", "case_title", "court_date", "hearing_time",
           "calendar_matter", "judge", "ruling", "admin_notes", "row_hash"]

# Dept 302's standard contest paragraph is the most reliable boundary between
# substantive ruling text and administrative boilerplate. Anything from this
# sentence onward (the contest instructions, court-reporter notes, remote-
# appearance notes, trailing judge tag) is admin. The match is anchored to
# the start of the sentence and tolerant of minor casing/whitespace variants.
_ADMIN_BOUNDARY_RE = re.compile(
    r'\s*Any\s+part(?:y|ies)\s+who\s+contests?\s+a\s+tentative\s+ruling',
    re.IGNORECASE,
)


def split_admin_notes(ruling):
    """Return (substantive_ruling, admin_notes). Both may be None."""
    if not isinstance(ruling, str) or not ruling:
        return None, None
    m = _ADMIN_BOUNDARY_RE.search(ruling)
    if not m:
        return ruling, None
    substantive = ruling[:m.start()].rstrip(' \t\r\n.,;:')
    admin       = ruling[m.start():].strip()
    return (substantive or None), (admin or None)


def make_hash(case_number, court_date, ruling):
    # Hash on the *substantive* ruling so rows that differ only in the
    # admin boilerplate (e.g. manually-curated rows where the boilerplate
    # was culled vs. scraped rows where it's still attached) dedupe to the
    # same record.
    substantive, _ = split_admin_notes(ruling)
    key = f"{case_number}|{court_date}|{substantive or ''}"
    return hashlib.sha1(key.encode()).hexdigest()


def normalize_date(val):
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date().isoformat()
    if isinstance(val, date):
        return val.isoformat()
    if isinstance(val, str):
        for fmt in ("%Y-%m-%d %I:%M %p", "%b-%d-%Y %I:%M %p", "%Y-%m-%d", "%m/%d/%Y"):
            try:
                return datetime.strptime(val.strip(), fmt).date().isoformat()
            except ValueError:
                continue
        # e.g. "JAN-02-2020 09:00 AM" — try just the date part
        try:
            return datetime.strptime(val.strip().split()[0], "%b-%d-%Y").date().isoformat()
        except ValueError:
            pass
        # e.g. "2026-04-27 09:00 AM" — try just the date part
        try:
            return datetime.strptime(val.strip().split()[0], "%Y-%m-%d").date().isoformat()
        except ValueError:
            return val
    return str(val)


def normalize_time(val):
    if val is None:
        return None
    if isinstance(val, time):
        return val.strftime("%H:%M")
    if isinstance(val, datetime):
        return val.strftime("%H:%M")
    if isinstance(val, str):
        # "09:00 AM" or "2026-04-27 09:00 AM"
        parts = val.strip().split()
        if len(parts) >= 3:
            try:
                return datetime.strptime(f"{parts[1]} {parts[2]}", "%I:%M %p").strftime("%H:%M")
            except ValueError:
                pass
        if len(parts) == 2:
            try:
                return datetime.strptime(f"{parts[0]} {parts[1]}", "%I:%M %p").strftime("%H:%M")
            except ValueError:
                pass
    return str(val) if val else None


def load_xlsm_2014_2018(path, department="302"):
    wb = openpyxl.load_workbook(path, read_only=True, keep_vba=True)
    ws = wb.active
    rows = []
    for r in ws.iter_rows(min_row=2, values_only=True):
        case_number, case_title, court_date, calendar_matter, ruling = r[:5]
        if not any([case_number, ruling]):
            continue
        hearing_time = None
        if isinstance(court_date, datetime) and (court_date.hour or court_date.minute):
            hearing_time = court_date.strftime("%H:%M")
        ruling_text = str(ruling).strip() if ruling else None
        substantive, admin = split_admin_notes(ruling_text)
        rows.append({
            "department":      department,
            "case_number":     str(case_number).strip() if case_number else None,
            "case_title":      str(case_title).strip() if case_title else None,
            "court_date":      normalize_date(court_date),
            "hearing_time":    hearing_time,
            "calendar_matter": str(calendar_matter).strip() if calendar_matter else None,
            "judge":           None,
            "ruling":          substantive,
            "admin_notes":     admin,
        })
    return rows


def load_xlsx_2020_plus(path, department="302"):
    wb = openpyxl.load_workbook(path, read_only=True)
    ws = wb["tentatives"]
    rows = []
    for r in ws.iter_rows(min_row=2, values_only=True):
        case_number, case_title, court_date, hearing_time, calendar_matter, judge, ruling = r[:7]
        if not any([case_number, ruling]):
            continue
        ruling_text = str(ruling).strip() if ruling else None
        substantive, admin = split_admin_notes(ruling_text)
        rows.append({
            "department":      department,
            "case_number":     str(case_number).strip() if case_number else None,
            "case_title":      str(case_title).strip() if case_title else None,
            "court_date":      normalize_date(court_date),
            "hearing_time":    normalize_time(hearing_time),
            "calendar_matter": str(calendar_matter).strip() if calendar_matter else None,
            "judge":           str(judge).strip() if judge else None,
            "ruling":          substantive,
            "admin_notes":     admin,
        })
    return rows


def load_json(path):
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    # Support both formats:
    #   Legacy: [{...}, ...]
    #   Extension: {department, rulings: [{...}, ...]}
    if isinstance(data, list):
        records   = data
        department = "302"
    else:
        records    = data.get("rulings", [])
        department = str(data.get("department", "302"))

    rows = []
    for rec in records:
        court_date_raw = rec.get("Court Date", "")
        ruling_text    = rec.get("Rulings", "").strip() or None
        # Use explicit Judge field if present (scraped by extension), else derive from code
        judge = rec.get("Judge") or extract_judge(ruling_text)
        substantive, admin = split_admin_notes(ruling_text)
        rows.append({
            "department":      department,
            "case_number":     rec.get("Case Number", "").strip() or None,
            "case_title":      rec.get("Case Title", "").strip() or None,
            "court_date":      normalize_date(court_date_raw),
            "hearing_time":    normalize_time(court_date_raw),
            "calendar_matter": rec.get("Calendar Matter", "").strip() or None,
            "judge":           judge,
            "ruling":          substantive,
            "admin_notes":     admin,
        })
    return rows


def detect_and_load(path):
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix == ".json":
        return load_json(path)
    if suffix in (".xlsx", ".xlsm"):
        wb = openpyxl.load_workbook(path, read_only=True, keep_vba=(suffix == ".xlsm"))
        ws = wb.active
        headers = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
        wb.close()
        if "Judge" in headers or "Hearing Time" in headers:
            return load_xlsx_2020_plus(path)
        return load_xlsm_2014_2018(path)
    raise ValueError(f"Unsupported file type: {path}")


def to_df(rows):
    df = pd.DataFrame(rows, columns=COLUMNS)
    df["row_hash"] = df.apply(
        lambda r: make_hash(r["case_number"] or "", r["court_date"] or "", r["ruling"] or ""),
        axis=1,
    )
    return df


def migrate_existing(df: pd.DataFrame) -> pd.DataFrame:
    """Bring an existing parquet up to the current schema:

      - Adds the `admin_notes` column if missing, splitting any boilerplate
        out of the existing `ruling` text.
      - Recomputes `row_hash` from the substantive ruling so old rows
        dedupe against newly-ingested rows that had the boilerplate split.
    """
    if "department" not in df.columns:
        df.insert(0, "department", "302")

    needs_split = "admin_notes" not in df.columns
    if needs_split:
        split = df["ruling"].apply(split_admin_notes)
        df["ruling"]      = split.apply(lambda t: t[0])
        df["admin_notes"] = split.apply(lambda t: t[1])
        df["row_hash"]    = df.apply(
            lambda r: make_hash(r["case_number"] or "", r["court_date"] or "", r["ruling"] or ""),
            axis=1,
        )
        df = df.drop_duplicates(subset="row_hash", keep="first").reset_index(drop=True)

    # Make sure we have all expected columns in the canonical order.
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[COLUMNS]


def merge(existing: pd.DataFrame, new: pd.DataFrame):
    combined = pd.concat([existing, new], ignore_index=True)
    before   = len(combined)
    combined = combined.drop_duplicates(subset="row_hash", keep="first")
    after    = len(combined)
    inserted = after - len(existing)
    skipped  = len(new) - inserted
    return combined.sort_values(["court_date", "department"]).reset_index(drop=True), inserted, skipped


def save_parquet(df: pd.DataFrame):
    df.to_parquet(PARQUET, index=False, compression="zstd")


def save_sqlite(df: pd.DataFrame):
    conn = sqlite3.connect(DB_PATH)
    conn.executescript("""
        DROP TABLE IF EXISTS tentatives;
        CREATE TABLE tentatives (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            department      TEXT,
            case_number     TEXT,
            case_title      TEXT,
            court_date      DATE,
            hearing_time    TEXT,
            calendar_matter TEXT,
            judge           TEXT,
            ruling          TEXT,
            admin_notes     TEXT,
            row_hash        TEXT UNIQUE
        );
        CREATE INDEX IF NOT EXISTS idx_department ON tentatives(department);
        CREATE INDEX IF NOT EXISTS idx_case_number ON tentatives(case_number);
        CREATE INDEX IF NOT EXISTS idx_court_date  ON tentatives(court_date);
        CREATE INDEX IF NOT EXISTS idx_judge       ON tentatives(judge);
    """)
    df.to_sql("tentatives", conn, if_exists="append", index=False,
              method="multi", chunksize=500)
    conn.close()


def summary(df: pd.DataFrame):
    print(f"\nDataset: {len(df):,} rows  |  {df['court_date'].min()} → {df['court_date'].max()}")
    depts = df['department'].dropna().unique()
    print(f"Departments: {', '.join(sorted(depts))}")
    print(f"Parquet: {PARQUET.stat().st_size / 1e6:.1f} MB   DB: {DB_PATH.stat().st_size / 1e6:.1f} MB")


def main():
    existing = pd.read_parquet(PARQUET) if PARQUET.exists() else pd.DataFrame(columns=COLUMNS)
    existing = migrate_existing(existing)

    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            p = Path(arg)
            if not p.exists():
                print(f"Not found: {p}")
                continue
            print(f"Loading {p.name}...")
            new_df = to_df(detect_and_load(p))
            existing, inserted, skipped = merge(existing, new_df)
            print(f"  {inserted} inserted, {skipped} skipped (duplicates)")
    else:
        sources = [
            HERE / "tentatives.xlsm",
            HERE / "sfsc tentatives 01-2020 to 07-2025.xlsx",
        ]
        for src in sources:
            if not src.exists():
                print(f"Not found: {src.name}")
                continue
            print(f"Loading {src.name}...")
            new_df = to_df(detect_and_load(src))
            existing, inserted, skipped = merge(existing, new_df)
            print(f"  {inserted} inserted, {skipped} skipped (duplicates)")

    print("Saving parquet...")
    save_parquet(existing)
    print("Saving SQLite...")
    save_sqlite(existing)
    summary(existing)


if __name__ == "__main__":
    main()
