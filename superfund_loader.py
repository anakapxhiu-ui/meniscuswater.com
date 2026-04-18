"""
Superfund / SEMS Loader for Meniscus.

Loads the EPA Superfund Enterprise Management System active sites file,
covering all NPL (National Priorities List), proposed, and archived sites.

Source: https://semspub.epa.gov/ (public CSV download)
"""

import argparse
import csv
import logging
import os
from contextlib import contextmanager
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger(__name__)

SEMS_URL = "https://semspub.epa.gov/work/HQ/400000184.csv"  # Active Sites export
DOWNLOAD_DIR = Path(os.environ.get("MENISCUS_DATA_DIR", "/tmp/meniscus/sems"))
DB_URL = os.environ["DATABASE_URL"]


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


def download_sems() -> Path:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    target = DOWNLOAD_DIR / "sems_active.csv"
    log.info(f"Downloading SEMS from {SEMS_URL}")
    with requests.get(SEMS_URL, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(target, "wb") as f:
            for chunk in r.iter_content(chunk_size=512 * 1024):
                f.write(chunk)
    return target


def load_sites(path: Path) -> int:
    log.info("Loading Superfund sites...")
    insert_sql = """
        INSERT INTO sites
          (external_id, name, site_type, source_system, address, city, state, zip,
           location, hazard_ranking_score, status)
        VALUES
          (%(id)s, %(name)s, %(type)s, 'SEMS', %(addr)s, %(city)s, %(state)s, %(zip)s,
           ST_SetSRID(ST_MakePoint(%(lng)s, %(lat)s), 4326)::geography, %(hrs)s, %(status)s)
        ON CONFLICT (external_id) DO UPDATE SET
          name = EXCLUDED.name,
          status = EXCLUDED.status,
          hazard_ranking_score = EXCLUDED.hazard_ranking_score,
          location = EXCLUDED.location,
          last_updated = NOW();
    """

    count = 0
    with db_connection() as conn, conn.cursor() as cur, open(path, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            lat = _safe_float(row.get("LATITUDE") or row.get("Latitude"))
            lng = _safe_float(row.get("LONGITUDE") or row.get("Longitude"))
            if lat is None or lng is None:
                continue
            npl_status = (row.get("NPL_STATUS_NAME") or row.get("NPLStatus") or "").strip()
            site_type = "superfund_npl" if "proposed" not in npl_status.lower() and "archived" not in npl_status.lower() else \
                        "superfund_proposed" if "proposed" in npl_status.lower() else \
                        "superfund_archived"
            batch.append({
                "id": row.get("EPA_ID") or row.get("SEMS_ID"),
                "name": row.get("SITE_NAME") or row.get("Name"),
                "type": site_type,
                "addr": row.get("ADDRESS"),
                "city": row.get("CITY"),
                "state": row.get("STATE"),
                "zip": row.get("ZIPCODE") or row.get("ZIP"),
                "lat": lat,
                "lng": lng,
                "hrs": _safe_float(row.get("HRS_SCORE")),
                "status": npl_status,
            })
            if len(batch) >= 500:
                psycopg2.extras.execute_batch(cur, insert_sql, batch)
                count += len(batch)
                batch.clear()
        if batch:
            psycopg2.extras.execute_batch(cur, insert_sql, batch)
            count += len(batch)

    log.info(f"Loaded {count:,} Superfund sites")
    return count


def _safe_float(v):
    try:
        return float(v) if v else None
    except (ValueError, TypeError):
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", help="Path to local SEMS CSV")
    args = parser.parse_args()
    path = Path(args.file) if args.file else download_sems()
    load_sites(path)
    log.info("Superfund load complete")


if __name__ == "__main__":
    main()
