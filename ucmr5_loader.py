"""
UCMR5 Loader for Meniscus.

UCMR5 (Fifth Unregulated Contaminant Monitoring Rule) is EPA's PFAS + lithium
monitoring program for large public water systems. Data is released in
quarterly cuts through 2027. This is the most important single dataset for
consumer water quality because:

  1. It's the only systematic national PFAS measurement program
  2. Individual utility CCRs often don't report it clearly
  3. It includes six PFAS that EPA is now regulating under MCLs

Source: https://www.epa.gov/dwucmr/occurrence-data-unregulated-contaminant-monitoring-rule

Usage:
    python ucmr5_loader.py                 # latest published cut
    python ucmr5_loader.py --file path.csv # local file
"""

import argparse
import csv
import logging
import os
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger(__name__)

UCMR5_OCCURRENCE_URL = "https://www.epa.gov/system/files/other-files/ucmr5_occurrence_data.zip"
DOWNLOAD_DIR = Path(os.environ.get("MENISCUS_DATA_DIR", "/tmp/meniscus/ucmr5"))
DB_URL = os.environ["DATABASE_URL"]

# UCMR5 contaminant name → our internal code
CONTAMINANT_MAP = {
    "HFPO-DA": "HFPO-DA",
    "PFBS": "PFBS",
    "PFHxS": "PFHxS",
    "PFNA": "PFNA",
    "PFOA": "PFOA",
    "PFOS": "PFOS",
    "PFBA": "PFBA",
    "PFHxA": "PFHxA",
    "PFPeA": "PFPeA",
    "PFHpA": "PFHpA",
    "NFDHA": "NFDHA",
    "ADONA": "ADONA",
    "4:2FTS": "4_2FTS",
    "6:2FTS": "6_2FTS",
    "8:2FTS": "8_2FTS",
    "PFEESA": "PFEESA",
    "9Cl-PF3ONS": "9Cl-PF3ONS",
    "11Cl-PF3OUdS": "11Cl-PF3OUdS",
    "PFMPA": "PFMPA",
    "PFMBA": "PFMBA",
    "NEtFOSAA": "NEtFOSAA",
    "NMeFOSAA": "NMeFOSAA",
    "PFPeS": "PFPeS",
    "PFHpS": "PFHpS",
    "PFNS": "PFNS",
    "PFDS": "PFDS",
    "PFDoS": "PFDoS",
    "PFUnA": "PFUnA",
    "PFDA": "PFDA",
    "lithium": "1930",
    "Lithium": "1930",
}


@contextmanager
def db_connection():
    conn = psycopg2.connect(DB_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def download_ucmr5(force: bool = False) -> Path:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    target = DOWNLOAD_DIR / "ucmr5_occurrence.zip"
    if target.exists() and not force:
        age_hours = (datetime.now().timestamp() - target.stat().st_mtime) / 3600
        if age_hours < 24:
            log.info(f"Using cached UCMR5 zip ({age_hours:.1f}h old)")
            return target

    log.info(f"Downloading UCMR5 occurrence data")
    with requests.get(UCMR5_OCCURRENCE_URL, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(target, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
    return target


def iter_results(file_path: Path):
    """Stream UCMR5 CSV rows. Handles both zip and raw CSV."""
    import zipfile

    if file_path.suffix == ".zip":
        with zipfile.ZipFile(file_path) as zf:
            for name in zf.namelist():
                if name.lower().endswith(".csv") or name.lower().endswith(".txt"):
                    with zf.open(name) as raw:
                        import io
                        text = io.TextIOWrapper(raw, encoding="utf-8", errors="replace")
                        reader = csv.DictReader(text, delimiter="\t") if name.lower().endswith(".txt") else csv.DictReader(text)
                        for row in reader:
                            yield row
    else:
        with open(file_path, encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield row


def load_results(file_path: Path) -> int:
    """Load UCMR5 occurrence data into raw table."""
    log.info(f"Loading UCMR5 results from {file_path}")

    insert_sql = """
        INSERT INTO raw.ucmr5_result
          (pwsid, pws_name, state, facility_id, sampling_point_id,
           sample_collection_date, contaminant, mrl, analytical_result, unit)
        VALUES (%(pwsid)s, %(name)s, %(state)s, %(facility)s, %(sp)s,
                %(date)s, %(contaminant)s, %(mrl)s, %(result)s, %(unit)s)
    """

    batch = []
    count = 0
    with db_connection() as conn, conn.cursor() as cur:
        for row in iter_results(file_path):
            date_str = row.get("CollectionDate") or row.get("SampleCollectionDate") or ""
            try:
                parsed_date = datetime.strptime(date_str, "%m/%d/%Y").date() if date_str else None
            except ValueError:
                parsed_date = None

            try:
                result_val = float(row.get("AnalyticalResultValue") or row.get("Value") or 0) or None
            except (ValueError, TypeError):
                result_val = None

            try:
                mrl_val = float(row.get("MRL") or 0) or None
            except (ValueError, TypeError):
                mrl_val = None

            batch.append({
                "pwsid": row.get("PWSID"),
                "name": (row.get("PWSName") or "")[:500],
                "state": row.get("State") or row.get("PrimacyAgencyCode"),
                "facility": row.get("FacilityID"),
                "sp": row.get("SamplingPointID"),
                "date": parsed_date,
                "contaminant": row.get("Contaminant"),
                "mrl": mrl_val,
                "result": result_val,
                "unit": row.get("AnalyticalResultUnit") or "ppt",
            })
            if len(batch) >= 5000:
                psycopg2.extras.execute_batch(cur, insert_sql, batch)
                count += len(batch)
                batch.clear()
        if batch:
            psycopg2.extras.execute_batch(cur, insert_sql, batch)
            count += len(batch)

    log.info(f"Loaded {count:,} UCMR5 results")
    return count


def promote_to_results() -> int:
    """Promote UCMR5 results into the normalized contaminant_results table."""
    log.info("Promoting UCMR5 into contaminant_results...")

    sql = """
        INSERT INTO contaminant_results
            (pwsid, contaminant_code, value, unit, sample_date, source, sample_location)
        SELECT
            u.pwsid,
            COALESCE(
                CASE
                    WHEN u.contaminant = 'PFOA' THEN 'PFOA'
                    WHEN u.contaminant = 'PFOS' THEN 'PFOS'
                    WHEN u.contaminant = 'PFHxS' THEN 'PFHxS'
                    WHEN u.contaminant = 'PFNA' THEN 'PFNA'
                    WHEN u.contaminant = 'HFPO-DA' THEN 'HFPO-DA'
                    WHEN u.contaminant = 'PFBS' THEN 'PFBS'
                    WHEN u.contaminant = 'PFBA' THEN 'PFBA'
                    WHEN u.contaminant = 'PFHxA' THEN 'PFHxA'
                    WHEN u.contaminant = 'PFPeA' THEN 'PFPeA'
                    WHEN LOWER(u.contaminant) = 'lithium' THEN '1930'
                    ELSE NULL
                END,
                u.contaminant
            ),
            u.analytical_result,
            u.unit,
            u.sample_collection_date,
            'UCMR5',
            u.sampling_point_id
        FROM raw.ucmr5_result u
        WHERE u.pwsid IN (SELECT pwsid FROM utilities)
          AND u.analytical_result IS NOT NULL
          AND u.sample_collection_date IS NOT NULL
        ON CONFLICT DO NOTHING;
    """
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(sql)
        count = cur.rowcount
    log.info(f"Promoted {count:,} results into contaminant_results")
    return count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", help="Path to local UCMR5 CSV/ZIP")
    parser.add_argument("--force-download", action="store_true")
    args = parser.parse_args()

    if args.file:
        path = Path(args.file)
    else:
        path = download_ucmr5(force=args.force_download)

    load_results(path)
    promote_to_results()
    log.info("UCMR5 load complete")


if __name__ == "__main__":
    main()
