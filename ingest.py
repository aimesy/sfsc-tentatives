#!/usr/bin/env python3
"""
Ingest tentative rulings and maintain tentatives.parquet + tentatives.db.

Usage:
    python ingest.py                              # rebuild from all source files
    python ingest.py path/to/new.xlsx             # append a new export
    python ingest.py --dept 525 path/to/525.xlsx  # tag the rows with a department
    python ingest.py path/to/new.json             # append a new json export (extension output)

tentatives.parquet  — canonical dataset, committed to git (~10 MB)
tentatives.db       — local SQLite for querying, gitignored (~100 MB)

JSON formats accepted:
  Legacy array:  [{Case Number, Case Title, Court Date, Calendar Matter, Rulings}, ...]
                 (dept must be passed via --dept; defaults to 302 for back-compat)
  Extension:     {department, scraped_at, source_url, rulings: [...]}
                 (dept comes from the wrapper; --dept is ignored if present)
"""

import sys
import json
import re
import argparse
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
           "calendar_matter", "judge", "ruling", "row_hash"]

# "(Part N of M)" suffix on a calendar_matter signals a split ruling whose
# substantive text is broken across multiple records. We strip the suffix to
# group the parts together for concatenation.
_PART_SUFFIX_RE = re.compile(
    r'\s*\(?\s*(?:part|pt)\.?\s+\d+\s+(?:of|/)\s+\d+\s*\)?\s*\.?\s*$',
    re.IGNORECASE,
)
_PART_NUM_RE = re.compile(
    r'\(?\s*(?:part|pt)\.?\s+(\d+)\s+(?:of|/)\s+(\d+)\s*\)?',
    re.IGNORECASE,
)


def normalize_motion_for_split(cm):
    if not cm:
        return ""
    return _PART_SUFFIX_RE.sub("", cm).strip().lower()


def extract_part_num(text):
    if not text:
        return None
    m = _PART_NUM_RE.search(text)
    return int(m.group(1)) if m else None


def make_hash(case_number, court_date, ruling):
    key = f"{case_number}|{court_date}|{ruling or ''}"
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
        rows.append({
            "department":      department,
            "case_number":     str(case_number).strip() if case_number else None,
            "case_title":      str(case_title).strip() if case_title else None,
            "court_date":      normalize_date(court_date),
            "hearing_time":    hearing_time,
            "calendar_matter": str(calendar_matter).strip() if calendar_matter else None,
            "judge":           None,
            "ruling":          str(ruling).strip() if ruling else None,
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
        rows.append({
            "department":      department,
            "case_number":     str(case_number).strip() if case_number else None,
            "case_title":      str(case_title).strip() if case_title else None,
            "court_date":      normalize_date(court_date),
            "hearing_time":    normalize_time(hearing_time),
            "calendar_matter": str(calendar_matter).strip() if calendar_matter else None,
            "judge":           str(judge).strip() if judge else None,
            "ruling":          str(ruling).strip() if ruling else None,
        })
    return rows


def load_json(path, department=None):
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    # Support both formats:
    #   Legacy: [{...}, ...]                                — uses `department`
    #   Extension: {department, rulings: [{...}, ...]}      — wrapper wins
    if isinstance(data, list):
        records = data
        if department is None:
            department = "302"
    else:
        records    = data.get("rulings", [])
        # Wrapper department takes precedence; CLI --dept is a fallback for
        # malformed scrapes that omit the field.
        department = str(data.get("department") or department or "302")

    rows = []
    for rec in records:
        court_date_raw = rec.get("Court Date", "")
        ruling_text    = rec.get("Rulings", "").strip() or None
        # Use explicit Judge field if present (scraped by extension), else derive from code
        judge = rec.get("Judge") or extract_judge(ruling_text)
        rows.append({
            "department":      department,
            "case_number":     rec.get("Case Number", "").strip() or None,
            "case_title":      rec.get("Case Title", "").strip() or None,
            "court_date":      normalize_date(court_date_raw),
            "hearing_time":    normalize_time(court_date_raw),
            "calendar_matter": rec.get("Calendar Matter", "").strip() or None,
            "judge":           judge,
            "ruling":          ruling_text,
        })
    return rows


def detect_and_load(path, department="302"):
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix == ".json":
        return load_json(path, department=department)
    if suffix in (".xlsx", ".xlsm"):
        wb = openpyxl.load_workbook(path, read_only=True, keep_vba=(suffix == ".xlsm"))
        ws = wb.active
        headers = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
        wb.close()
        if "Judge" in headers or "Hearing Time" in headers:
            return load_xlsx_2020_plus(path, department=department)
        return load_xlsm_2014_2018(path, department=department)
    raise ValueError(f"Unsupported file type: {path}")


def to_df(rows):
    df = pd.DataFrame(rows, columns=[c for c in COLUMNS if c != "row_hash"])
    return _add_hash(df)


def _add_hash(df):
    df["row_hash"] = df.apply(
        lambda r: make_hash(r["case_number"] or "", r["court_date"] or "", r["ruling"] or ""),
        axis=1,
    )
    return df[COLUMNS]


def consolidate_splits(df: pd.DataFrame) -> pd.DataFrame:
    """Concatenate split rulings.

    The SFTC site sometimes truncates a long ruling and emits the rest as a
    second record marked "(Part 2 of 2)" (or with the suffix on
    calendar_matter). We treat two rows as parts of the same ruling when they
    share (case_number, court_date) and their calendar_matter is identical
    after the part-suffix is stripped. Within each group we sort by part
    number (taken from ruling text or calendar_matter) and join with two
    newlines. The kept calendar_matter is the de-suffixed form.
    """
    if df.empty:
        return df
    df = df.copy()
    df["_cm_norm"] = df["calendar_matter"].fillna("").apply(normalize_motion_for_split)
    df["_part"] = df["ruling"].fillna("").apply(extract_part_num)
    df["_part"] = df["_part"].fillna(df["calendar_matter"].fillna("").apply(extract_part_num))

    # Stable order before grouping so single-part rows keep their original placement.
    df = df.sort_values(["case_number", "court_date", "_cm_norm", "_part"],
                        na_position="last", kind="stable").reset_index(drop=True)

    keys = ["case_number", "court_date", "_cm_norm"]
    sizes = df.groupby(keys, dropna=False).size()
    multi_keys = set(sizes[sizes > 1].index)

    if not multi_keys:
        return df.drop(columns=["_cm_norm", "_part"])

    def dedupe_by_containment(seq):
        """Drop strings that are substrings of any longer string in `seq`.
        Lets us re-ingest a date whose parquet entry is already consolidated:
        the new individual parts are substrings of the existing merged form
        and get dropped instead of re-concatenated on top of it."""
        unique = sorted({s for s in seq if s}, key=len, reverse=True)
        kept = []
        for r in unique:
            if not any(r in k and r != k for k in kept):
                kept.append(r)
        return kept

    keep_indices = []
    merged_rows = []
    consolidated = 0
    for key, idx in df.groupby(keys, dropna=False).groups.items():
        idx = list(idx)
        if key not in multi_keys:
            keep_indices.extend(idx)
            continue
        sub = df.loc[idx]
        raw_rulings = [r for r in sub["ruling"].fillna("").tolist() if r.strip()]
        rulings = dedupe_by_containment(raw_rulings)
        if len(rulings) <= 1:
            # Already-merged or all-substring case — keep one row whose ruling
            # is the longest (the post-consolidation form, if present).
            longest_idx = max(idx, key=lambda i:
                              len(df.at[i, "ruling"]) if isinstance(df.at[i, "ruling"], str) else 0)
            keep_indices.append(longest_idx)
            consolidated += len(idx) - 1
            continue
        merged_text = "\n\n".join(rulings)
        first = sub.iloc[0].to_dict()
        first["ruling"] = merged_text
        cm = first.get("calendar_matter")
        cm = cm if isinstance(cm, str) else ""
        first["calendar_matter"] = _PART_SUFFIX_RE.sub("", cm).strip() or None
        merged_rows.append(first)
        consolidated += len(idx) - 1

    base = df.loc[keep_indices].drop(columns=["_cm_norm", "_part"])
    if merged_rows:
        merged_df = pd.DataFrame(merged_rows).drop(columns=["_cm_norm", "_part"], errors="ignore")
        out = pd.concat([base, merged_df], ignore_index=True)
    else:
        out = base
    out = _add_hash(out.drop(columns=[c for c in out.columns if c == "row_hash"], errors="ignore"))
    out = out.drop_duplicates(subset="row_hash", keep="first").reset_index(drop=True)
    if consolidated:
        print(f"  consolidated {consolidated} split-ruling row(s) into "
              f"{len(merged_rows)} merged ruling(s)")
    return out


def migrate_existing(df: pd.DataFrame) -> pd.DataFrame:
    """Bring an existing parquet up to the current schema:

      - Drop the legacy `admin_notes` column, folding its text back into
        `ruling` so the full ruling is in one place (browser splits at
        display time now).
      - Recompute `row_hash` from the full ruling.
      - Concatenate split rulings ((Part N of M)) into one record.
    """
    if "department" not in df.columns:
        # One-time migration: parquets predating the multi-dept schema only
        # held Civil Law & Motion (Dept 302) data. New ingests always carry
        # department through, so this branch is never hit twice.
        df.insert(0, "department", "302")

    if "admin_notes" in df.columns:
        # Reattach admin text to the ruling. Old splits always had the boundary
        # phrase at the start of admin_notes, so a single-space join recovers
        # the original substring.
        def rejoin(row):
            r = row.get("ruling")
            a = row.get("admin_notes")
            r = r if isinstance(r, str) else ""
            a = a if isinstance(a, str) else ""
            if r and a:
                return f"{r.rstrip()} {a.lstrip()}"
            return r or a or None
        df["ruling"] = df.apply(rejoin, axis=1)
        df = df.drop(columns=["admin_notes"])

    # consolidate_splits already recomputes the hash and dedupes; we just need
    # to ensure the canonical column order/presence on its way out.
    df = consolidate_splits(df)
    for col in COLUMNS:
        if col not in df.columns:
            df[col] = None
    return df[COLUMNS].reset_index(drop=True)


def merge(existing: pd.DataFrame, new: pd.DataFrame):
    combined = pd.concat([existing, new], ignore_index=True)
    combined = combined.drop_duplicates(subset="row_hash", keep="first")
    # Re-run consolidation after merge — a newly-arrived part may pair with an
    # existing row in the parquet.
    combined = consolidate_splits(combined)
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
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dept", default="302",
                        help="Department to tag rows with when the source file "
                             "doesn't carry one (Excel imports + legacy bare-list "
                             "JSON). Extension-format JSON wrappers always win.")
    parser.add_argument("paths", nargs="*",
                        help="Source files to ingest. If omitted, falls back to "
                             "the original Excel exports in the repo root.")
    args = parser.parse_args()

    existing = pd.read_parquet(PARQUET) if PARQUET.exists() else pd.DataFrame(columns=COLUMNS)
    existing = migrate_existing(existing)

    sources = [Path(p) for p in args.paths] if args.paths else [
        HERE / "tentatives.xlsm",
        HERE / "sfsc tentatives 01-2020 to 07-2025.xlsx",
    ]
    for p in sources:
        if not p.exists():
            print(f"Not found: {p.name if not args.paths else p}")
            continue
        print(f"Loading {p.name} (dept default = {args.dept})...")
        new_df = to_df(detect_and_load(p, department=args.dept))
        existing, inserted, skipped = merge(existing, new_df)
        print(f"  {inserted} inserted, {skipped} skipped (duplicates)")

    print("Saving parquet...")
    save_parquet(existing)
    print("Saving SQLite...")
    save_sqlite(existing)
    summary(existing)


if __name__ == "__main__":
    main()
