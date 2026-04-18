"""
Meniscus lookup API.

POST /api/lookup
  { "address": "7912 Running Water Dr, Austin, TX 78747" }

Pipeline:
  1. Geocode
  2. Cache check
  3. Resolve to utility (PWSID)
  4. Pull latest contaminant results
  5. Pull nearby industrial/Superfund sites
  6. Score each finding (deterministic)
  7. Call LLM to write the narrative
  8. Match products + installers
  9. Cache and return
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from narrative import generate_narrative
from product_matcher import match_products
from severity import aggregate_profile, score_finding

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger(__name__)

app = FastAPI(title="Meniscus Lookup API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten for prod
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_URL = os.environ["DATABASE_URL"]
MAPBOX_TOKEN = os.environ["MAPBOX_TOKEN"]
CACHE_DAYS = 30
SITE_SEARCH_RADIUS_MILES = 5

import httpx


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


class LookupRequest(BaseModel):
    address: str = Field(..., min_length=5, max_length=500)
    user_context: dict | None = None  # optional: well vs municipal, concerns, etc.


class LookupResponse(BaseModel):
    address: str
    normalized_address: str | None
    coordinates: dict | None
    utility: dict | None
    water_profile: dict
    narrative: str
    recommended_products: list[dict]
    nearby_sites: list[dict]
    data_sources: list[dict]
    cached: bool


def normalize_address(addr: str) -> str:
    return " ".join(addr.strip().upper().split())


def address_hash(addr: str) -> str:
    return hashlib.sha256(normalize_address(addr).encode()).hexdigest()


async def geocode(address: str) -> dict:
    """Mapbox geocoding. Returns {lat, lng, normalized}."""
    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/{address}.json"
    params = {"access_token": MAPBOX_TOKEN, "country": "us", "limit": 1}
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        data = r.json()
    if not data.get("features"):
        raise HTTPException(404, f"Could not geocode: {address}")
    f = data["features"][0]
    lng, lat = f["center"]
    return {
        "lat": lat,
        "lng": lng,
        "normalized": f.get("place_name"),
    }


def resolve_utility(lat: float, lng: float, state_hint: str | None = None) -> dict | None:
    """
    Find the serving utility for this address.

    Strategy order:
      1. Spatial lookup against utilities.service_area polygon (if we have one)
      2. State + county fallback to largest utility
      3. Return None -> frontend shows 'we can't confirm your utility'
    """
    with db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Polygon match first
        cur.execute("""
            SELECT pwsid, name, state, source_type, population_served
            FROM utilities
            WHERE service_area IS NOT NULL
              AND ST_Covers(service_area, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography)
              AND is_active = TRUE
            ORDER BY population_served DESC NULLS LAST
            LIMIT 1
        """, (lng, lat))
        row = cur.fetchone()
        if row:
            row["resolution_method"] = "polygon"
            return dict(row)

        # Fallback: largest community water system in the state
        if state_hint:
            cur.execute("""
                SELECT pwsid, name, state, source_type, population_served
                FROM utilities
                WHERE state = %s AND is_active = TRUE
                ORDER BY population_served DESC NULLS LAST
                LIMIT 1
            """, (state_hint,))
            row = cur.fetchone()
            if row:
                row["resolution_method"] = "state_fallback"
                return dict(row)

    return None


def latest_contaminant_results(pwsid: str, cutoff_months: int = 36) -> list[dict]:
    """Get most recent result per contaminant, with full reference data joined."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=cutoff_months * 30)
    with db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            WITH latest AS (
                SELECT DISTINCT ON (pwsid, contaminant_code)
                    pwsid, contaminant_code, value, unit, sample_date, source, is_violation
                FROM contaminant_results
                WHERE pwsid = %s
                  AND sample_date >= %s
                ORDER BY pwsid, contaminant_code, sample_date DESC
            )
            SELECT
                l.*,
                c.name, c.category, c.epa_mcl_value, c.epa_mcl_unit,
                c.ewg_guideline_value, c.ca_phg_value,
                c.nsf_standards_removing, c.typical_reduction_jsonb,
                c.health_effects_plain, c.iarc_classification,
                c.part_of_group
            FROM latest l
            LEFT JOIN contaminants c ON c.code = l.contaminant_code
            WHERE c.code IS NOT NULL
        """, (pwsid, cutoff))
        return [dict(r) for r in cur.fetchall()]


def nearby_sites(lat: float, lng: float, radius_miles: float = 5) -> list[dict]:
    radius_meters = radius_miles * 1609.34
    with db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT
                name, site_type, status, hazard_ranking_score,
                ST_Distance(location, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography) / 1609.34 AS distance_miles,
                contaminants_of_concern
            FROM sites
            WHERE ST_DWithin(location, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, %s)
            ORDER BY distance_miles ASC
            LIMIT 20
        """, (lng, lat, lng, lat, radius_meters))
        return [dict(r) for r in cur.fetchall()]


def get_cached_lookup(addr_hash: str) -> dict | None:
    with db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT assembled_report, assembled_at
            FROM address_lookups
            WHERE address_hash = %s AND expires_at > NOW()
        """, (addr_hash,))
        row = cur.fetchone()
        return row["assembled_report"] if row else None


def save_cached_lookup(addr_hash: str, addr: str, lat: float, lng: float, pwsid: str | None, report: dict):
    expires = datetime.now(timezone.utc) + timedelta(days=CACHE_DAYS)
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO address_lookups
                (address_hash, input_address, latitude, longitude, location,
                 resolved_pwsid, assembled_report, assembled_at, expires_at)
            VALUES
                (%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                 %s, %s, NOW(), %s)
            ON CONFLICT (address_hash) DO UPDATE SET
                assembled_report = EXCLUDED.assembled_report,
                assembled_at = NOW(),
                expires_at = EXCLUDED.expires_at
        """, (addr_hash, addr, lat, lng, lng, lat, pwsid, json.dumps(report), expires))


@app.post("/api/lookup", response_model=LookupResponse)
async def lookup(req: LookupRequest) -> LookupResponse:
    addr_hash = address_hash(req.address)
    cached = get_cached_lookup(addr_hash)
    if cached and not req.user_context:
        return LookupResponse(**cached, cached=True)

    geo = await geocode(req.address)
    utility = resolve_utility(geo["lat"], geo["lng"])

    if not utility:
        return LookupResponse(
            address=req.address,
            normalized_address=geo["normalized"],
            coordinates={"lat": geo["lat"], "lng": geo["lng"]},
            utility=None,
            water_profile={"max_severity": 0, "headline": "No utility match"},
            narrative="We could not confirm which utility serves this address. This often means the address is served by a private well or a small utility not yet in our database. Consider a comprehensive Tap Score test to characterize your water directly.",
            recommended_products=[],
            nearby_sites=nearby_sites(geo["lat"], geo["lng"]),
            data_sources=[],
            cached=False,
        )

    raw_results = latest_contaminant_results(utility["pwsid"])
    findings = []
    for r in raw_results:
        if r.get("value") is None or r["value"] <= 0:
            continue
        contam_record = {
            "code": r["contaminant_code"],
            "name": r["name"],
            "category": r["category"],
            "epa_mcl_value": float(r["epa_mcl_value"]) if r["epa_mcl_value"] else None,
            "epa_mcl_unit": r["epa_mcl_unit"],
            "ewg_guideline_value": float(r["ewg_guideline_value"]) if r["ewg_guideline_value"] else None,
            "nsf_standards_removing": r["nsf_standards_removing"] or [],
            "typical_reduction_jsonb": r["typical_reduction_jsonb"] or {},
            "sample_date": r["sample_date"].isoformat() if r["sample_date"] else None,
        }
        findings.append(score_finding(float(r["value"]), r["unit"], contam_record))

    profile = aggregate_profile(findings)
    sites = nearby_sites(geo["lat"], geo["lng"])

    narrative_input = {
        "utility": utility,
        "profile": profile,
        "sites": sites[:5],
        "address": req.address,
        "user_context": req.user_context or {},
    }
    narrative = await generate_narrative(narrative_input)

    products = match_products(profile, limit=6)

    data_sources = [
        {"name": "EPA SDWIS", "description": "Safe Drinking Water Information System", "url": "https://www.epa.gov/enviro/sdwis-search"},
        {"name": "EPA UCMR5", "description": "Fifth Unregulated Contaminant Monitoring Rule (PFAS + lithium)", "url": "https://www.epa.gov/dwucmr"},
        {"name": "EPA SEMS", "description": "Superfund Enterprise Management System", "url": "https://cumulis.epa.gov/supercpad/"},
    ]

    report = {
        "address": req.address,
        "normalized_address": geo["normalized"],
        "coordinates": {"lat": geo["lat"], "lng": geo["lng"]},
        "utility": utility,
        "water_profile": profile,
        "narrative": narrative,
        "recommended_products": products,
        "nearby_sites": sites,
        "data_sources": data_sources,
    }

    save_cached_lookup(addr_hash, req.address, geo["lat"], geo["lng"], utility["pwsid"], report)
    return LookupResponse(**report, cached=False)


@app.get("/api/health")
def health():
    return {"status": "ok", "service": "meniscus"}
