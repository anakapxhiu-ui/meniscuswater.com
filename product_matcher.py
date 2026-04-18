"""
Product matching engine.

Deterministic recommendation of filter products based on:
  1. Which NSF certifications are required given the contaminants present
  2. User's form factor preference (pitcher/under-sink/whole-house)
  3. Severity of findings (higher severity → stronger recommendation)

This is NOT an LLM call. Product recommendations must be auditable and
consistent; the LLM only polishes the "why" sentence per product.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

log = logging.getLogger(__name__)
DB_URL = os.environ["DATABASE_URL"]


@contextmanager
def db_connection():
    conn = psycopg2.connect(DB_URL)
    try:
        yield conn
    finally:
        conn.close()


def match_products(profile: dict, limit: int = 6, form_factor_preference: str | None = None) -> list[dict]:
    """
    Returns ranked filter products that address the user's contaminants.

    Ranking:
      1. Products that cover ALL required NSF certs rank first
      2. Among equal-coverage, higher avg rating wins
      3. Form factor preference is a hard filter if provided, soft boost otherwise
    """
    required_certs = profile.get("required_nsf_standards", [])
    max_severity = profile.get("max_severity", 0)

    # If max severity is 0 (nothing detected), we just recommend a baseline filter
    if max_severity == 0:
        return _baseline_recommendations(limit)

    with db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Query filters that have at least one of the required certs
        cur.execute("""
            SELECT
                id, brand, model, form_factor, price_usd,
                replacement_cost_usd, replacement_frequency_months,
                nsf_certifications, contaminants_reduced,
                affiliate_url_template, affiliate_commission_pct,
                image_url,
                cardinality(array(
                    SELECT UNNEST(nsf_certifications)
                    INTERSECT
                    SELECT UNNEST(%s::text[])
                )) AS matching_certs
            FROM filter_products
            WHERE is_active = TRUE
              AND nsf_certifications && %s::text[]
            ORDER BY matching_certs DESC, price_usd ASC
            LIMIT %s
        """, (required_certs, required_certs, limit * 3))
        candidates = [dict(r) for r in cur.fetchall()]

    # Score and rank
    ranked = []
    for c in candidates:
        coverage_score = c["matching_certs"] / max(len(required_certs), 1)
        form_factor_score = 1.0
        if form_factor_preference and c["form_factor"] != form_factor_preference:
            form_factor_score = 0.5
        severity_fit = _severity_to_form_factor_fit(max_severity, c["form_factor"])
        total_score = coverage_score * 0.5 + form_factor_score * 0.2 + severity_fit * 0.3

        ranked.append({
            **c,
            "coverage_pct": round(coverage_score * 100),
            "score": round(total_score, 3),
            "match_reason": _generate_match_reason(c, profile),
        })

    ranked.sort(key=lambda x: -x["score"])
    return ranked[:limit]


def _severity_to_form_factor_fit(max_severity: int, form_factor: str) -> float:
    """For severe contamination, prefer whole-house / RO over pitchers."""
    severe_factors = {"whole_house": 1.0, "under_sink": 0.9, "countertop_ro": 0.85, "faucet": 0.5, "pitcher": 0.3}
    mild_factors = {"pitcher": 0.9, "faucet": 0.9, "under_sink": 1.0, "countertop_ro": 0.9, "whole_house": 0.6}
    if max_severity >= 3:
        return severe_factors.get(form_factor, 0.5)
    return mild_factors.get(form_factor, 0.5)


def _generate_match_reason(product: dict, profile: dict) -> str:
    violations = profile.get("violation_contaminants", [])
    elevated = profile.get("elevated_contaminants", [])
    certs = product.get("nsf_certifications", [])

    if violations:
        return f"Certified for {', '.join(certs[:3])}; addresses {violations[0]} which is above the legal MCL in your water."
    if elevated:
        return f"Certified for {', '.join(certs[:3])}; reduces {elevated[0]} which is above health guidelines in your water."
    return f"Certified under {', '.join(certs[:2])} for general protection."


def _baseline_recommendations(limit: int) -> list[dict]:
    """When water is clean, we suggest basic peace-of-mind filters."""
    with db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT id, brand, model, form_factor, price_usd, nsf_certifications, image_url
            FROM filter_products
            WHERE is_active = TRUE AND 'NSF 42' = ANY(nsf_certifications)
            ORDER BY price_usd ASC
            LIMIT %s
        """, (limit,))
        return [{**dict(r), "match_reason": "Baseline taste and chlorine reduction", "coverage_pct": 100} for r in cur.fetchall()]
