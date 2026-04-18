"""
Seed the contaminants reference table from the JSON knowledge base.
Run once at setup and whenever contaminants.json changes.
"""

import json
import logging
import os
import sys
from contextlib import contextmanager
from pathlib import Path

import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger(__name__)

DB_URL = os.environ["DATABASE_URL"]
JSON_PATH = Path(__file__).parent.parent.parent / "data" / "contaminants.json"


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


def extract_mcl(c: dict) -> tuple[float | None, str | None]:
    """Pull the most relevant MCL and unit from a contaminant record."""
    for key, unit in [
        ("epa_mcl_ppb", "ppb"),
        ("epa_mcl_ppt", "ppt"),
        ("epa_mcl_ppm", "ppm"),
        ("epa_mcl_pci_l", "pCi/L"),
    ]:
        if c.get(key) is not None:
            return c[key], unit
    return None, None


def extract_guideline(c: dict) -> float | None:
    for key in ["ewg_guideline_ppb", "ewg_guideline_ppt", "ewg_guideline_ppm", "ewg_guideline_pci_l"]:
        if c.get(key) is not None:
            return c[key]
    return None


def main():
    with open(JSON_PATH) as f:
        data = json.load(f)

    contaminants = data["contaminants"]
    log.info(f"Loading {len(contaminants)} contaminants into reference table...")

    upsert_sql = """
        INSERT INTO contaminants
          (code, name, aliases, category, epa_mcl_value, epa_mcl_unit,
           epa_mclg_value, epa_action_level_value, ewg_guideline_value, ca_phg_value,
           typical_sources, health_effects_plain, iarc_classification,
           nsf_standards_removing, typical_reduction_jsonb, regulatory_group,
           is_ucmr5, part_of_group)
        VALUES
          (%(code)s, %(name)s, %(aliases)s, %(category)s, %(mcl)s, %(mcl_unit)s,
           %(mclg)s, %(action)s, %(ewg)s, %(phg)s,
           %(sources)s, %(health)s, %(iarc)s,
           %(nsf)s, %(reduction)s, %(reg_group)s,
           %(is_ucmr5)s, %(part_of)s)
        ON CONFLICT (code) DO UPDATE SET
          name = EXCLUDED.name,
          category = EXCLUDED.category,
          epa_mcl_value = EXCLUDED.epa_mcl_value,
          health_effects_plain = EXCLUDED.health_effects_plain,
          nsf_standards_removing = EXCLUDED.nsf_standards_removing,
          typical_reduction_jsonb = EXCLUDED.typical_reduction_jsonb;
    """

    rows = []
    for c in contaminants:
        mcl, mcl_unit = extract_mcl(c)
        rows.append({
            "code": c["code"],
            "name": c["name"],
            "aliases": c.get("aliases", []),
            "category": c["category"],
            "mcl": mcl,
            "mcl_unit": mcl_unit,
            "mclg": c.get("epa_mclg_ppb") or c.get("epa_mclg_ppt") or c.get("epa_mclg_ppm"),
            "action": c.get("epa_action_level_ppb"),
            "ewg": extract_guideline(c),
            "phg": c.get("ca_phg_ppb") or c.get("ca_phg_ppt"),
            "sources": c.get("typical_sources", []),
            "health": c.get("health_effects_plain", ""),
            "iarc": c.get("iarc_classification"),
            "nsf": c.get("nsf_standards_removing", []),
            "reduction": json.dumps(c.get("typical_reduction", {})),
            "reg_group": c.get("regulatory_group"),
            "is_ucmr5": c.get("epa_ucmr5_monitored", False) or c.get("epa_hazard_index_component", False),
            "part_of": c.get("part_of_group"),
        })

    with db_connection() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, upsert_sql, rows)

    log.info(f"Loaded {len(rows)} contaminants")


if __name__ == "__main__":
    main()
