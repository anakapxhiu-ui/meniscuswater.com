"""
SDWIS Loader for Meniscus.

Downloads and normalizes EPA Safe Drinking Water Information System data.
SDWIS is released quarterly as a downloadable ZIP from EPA Envirofacts.

Usage:
    python sdwis_loader.py --state TX --mode incremental
    python sdwis_loader.py --full-refresh

Data source: https://www.epa.gov/enviro/envirofacts-data-downloads
The relevant files for us are:
    - SDWA_PUB_WATER_SYSTEMS.csv
    - SDWA_VIOLATIONS_ENFORCEMENT.csv
    - SDWA_LCR_SAMPLES.csv
    - SDWA_SERVICE_AREAS.csv
    - SDWA_GEOGRAPHIC_AREAS.csv
"""

import argparse
import csv
import io
import logging
import os
import zipfile
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator

import psycopg2
import psycopg2.extras
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger(__name__)

SDWIS_DOWNLOAD_URL = "https://echo.epa.gov/files/echodownloads/SDWA_latest_downloads.zip"
DOWNLOAD_DIR = Path(os.environ.get("MENISCUS_DATA_DIR", "/tmp/meniscus/sdwis"))
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


def download_sdwis(force: bool = False) -> Path:
    """Fetch the latest SDWIS bulk download ZIP. Cached for 24h."""
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = DOWNLOAD_DIR / "sdwa_latest.zip"

    if zip_path.exists() and not force:
        age_hours = (datetime.now().timestamp() - zip_path.stat().st_mtime) / 3600
        if age_hours < 24:
            log.info(f"Using cached SDWIS zip ({age_hours:.1f}h old)")
            return zip_path

    log.info(f"Downloading SDWIS from {SDWIS_DOWNLOAD_URL}")
    with requests.get(SDWIS_DOWNLOAD_URL, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
    log.info(f"Downloaded {zip_path.stat().st_size / 1e6:.1f} MB")
    return zip_path


def iter_csv_rows(zip_path: Path, filename: str, state_filter: str | None = None) -> Iterator[dict]:
    """Stream rows from a CSV inside the SDWIS zip without fully extracting."""
    with zipfile.ZipFile(zip_path) as zf:
        target = next((n for n in zf.namelist() if filename.lower() in n.lower()), None)
        if target is None:
            raise FileNotFoundError(f"{filename} not in SDWIS zip")
        log.info(f"Streaming rows from {target}")
        with zf.open(target) as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8", errors="replace")
            reader = csv.DictReader(text)
            for row in reader:
                if state_filter:
                    state = row.get("PRIMACY_AGENCY_CODE") or row.get("STATE_CODE") or ""
                    if state.upper() != state_filter.upper():
                        continue
                yield row


def load_water_systems(zip_path: Path, state_filter: str | None = None) -> int:
    """Load PWS metadata into raw.sdwis_water_system."""
    log.info("Loading water systems...")
    upsert_sql = """
        INSERT INTO raw.sdwis_water_system
          (pwsid, pws_name, primacy_agency_code, epa_region, pws_type_code,
           owner_type_code, population_served_count, primary_source_code)
        VALUES (%(pwsid)s, %(name)s, %(state)s, %(region)s, %(type)s,
                %(owner)s, %(pop)s, %(source)s)
        ON CONFLICT (pwsid) DO UPDATE SET
          pws_name = EXCLUDED.pws_name,
          population_served_count = EXCLUDED.population_served_count,
          primary_source_code = EXCLUDED.primary_source_code,
          ingested_at = NOW();
    """
    batch: list[dict] = []
    count = 0
    with db_connection() as conn, conn.cursor() as cur:
        for row in iter_csv_rows(zip_path, "SDWA_PUB_WATER_SYSTEMS.csv", state_filter):
            batch.append({
                "pwsid": row.get("PWSID"),
                "name": (row.get("PWS_NAME") or "")[:500],
                "state": row.get("PRIMACY_AGENCY_CODE"),
                "region": row.get("EPA_REGION"),
                "type": row.get("PWS_TYPE_CODE"),
                "owner": row.get("OWNER_TYPE_CODE"),
                "pop": int(row["POPULATION_SERVED_COUNT"]) if row.get("POPULATION_SERVED_COUNT", "").isdigit() else None,
                "source": row.get("PRIMARY_SOURCE_CODE"),
            })
            if len(batch) >= 1000:
                psycopg2.extras.execute_batch(cur, upsert_sql, batch)
                count += len(batch)
                batch.clear()
        if batch:
            psycopg2.extras.execute_batch(cur, upsert_sql, batch)
            count += len(batch)
    log.info(f"Loaded {count:,} water systems")
    return count


def promote_to_normalized(state_filter: str | None = None) -> int:
    """Copy relevant columns from raw schema into normalized `utilities` table."""
    log.info("Promoting to normalized schema...")
    sql = """
        INSERT INTO utilities (pwsid, name, state, epa_region, source_type, population_served)
        SELECT
            pwsid,
            pws_name,
            primacy_agency_code,
            epa_region,
            CASE primary_source_code
                WHEN 'SW' THEN 'surface'
                WHEN 'GW' THEN 'groundwater'
                WHEN 'GU' THEN 'groundwater_uinfluence'
                WHEN 'SWP' THEN 'purchased_surface'
                WHEN 'GWP' THEN 'purchased_groundwater'
                ELSE 'unknown'
            END,
            population_served_count
        FROM raw.sdwis_water_system
        WHERE pws_type_code = 'CWS'  -- Community Water Systems only
    """
    params: list = []
    if state_filter:
        sql += " AND primacy_agency_code = %s"
        params.append(state_filter)
    sql += """
        ON CONFLICT (pwsid) DO UPDATE SET
            name = EXCLUDED.name,
            state = EXCLUDED.state,
            population_served = EXCLUDED.population_served,
            source_type = EXCLUDED.source_type,
            last_updated = NOW();
    """
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        count = cur.rowcount
    log.info(f"Promoted {count:,} community water systems")
    return count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", help="Two-letter state code (optional filter)")
    parser.add_argument("--force-download", action="store_true")
    parser.add_argument("--skip-download", action="store_true")
    args = parser.parse_args()

    if not args.skip_download:
        zip_path = download_sdwis(force=args.force_download)
    else:
        zip_path = DOWNLOAD_DIR / "sdwa_latest.zip"
        if not zip_path.exists():
            raise FileNotFoundError("No cached SDWIS file and --skip-download set")

    load_water_systems(zip_path, state_filter=args.state)
    promote_to_normalized(state_filter=args.state)
    log.info("SDWIS load complete")


if __name__ == "__main__":
    main()
