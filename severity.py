"""
Severity scoring engine.

Takes raw contaminant results and deterministically scores each one so that:
  1. Product recommendations are consistent and auditable
  2. LLM narratives get a structured input, not a chance to make things up
  3. We never produce alarming language for low levels, or minimize real findings

Score scale:
  0 - Not detected / below method detection limit
  1 - Detected but below all health guidelines ("trace")
  2 - Above health guideline but below legal MCL ("elevated")
  3 - At or above legal MCL ("violation")
  4 - Significantly above MCL or acute risk
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Severity = Literal[0, 1, 2, 3, 4]

SEVERITY_LABELS = {
    0: "not detected",
    1: "trace",
    2: "elevated",
    3: "above legal limit",
    4: "significantly above legal limit",
}

SEVERITY_TONES = {
    0: "neutral",
    1: "informational",
    2: "attention",
    3: "concern",
    4: "urgent",
}


@dataclass
class ContaminantFinding:
    code: str
    name: str
    category: str
    value: float
    unit: str
    sample_date: str | None
    epa_mcl: float | None
    epa_mcl_unit: str | None
    health_guideline: float | None
    severity: Severity
    severity_label: str
    severity_tone: str
    ratio_to_guideline: float | None
    ratio_to_mcl: float | None
    plain_summary: str  # structured, not narrative
    recommended_nsf_standards: list[str]
    typical_reduction: dict[str, float]


def normalize_units(value: float, from_unit: str, to_unit: str) -> float:
    """Convert between ppm/ppb/ppt. Returns the value in `to_unit`."""
    factors = {"ppm": 1e-3, "ppb": 1.0, "ppt": 1e3, "mg/L": 1e-3, "ug/L": 1.0, "ng/L": 1e3}
    fu = factors.get(from_unit.lower())
    tu = factors.get(to_unit.lower())
    if fu is None or tu is None:
        return value  # unknown units, pass through
    return value * (tu / fu)


def score_finding(
    result_value: float,
    result_unit: str,
    contaminant: dict,
) -> ContaminantFinding:
    """
    Given one result and the contaminant reference record, produce a full finding.

    contaminant is expected to have:
      code, name, category, epa_mcl_value, epa_mcl_unit, ewg_guideline_value,
      nsf_standards_removing, typical_reduction_jsonb, health_effects_plain
    """
    mcl = contaminant.get("epa_mcl_value")
    mcl_unit = contaminant.get("epa_mcl_unit") or result_unit
    guideline = contaminant.get("ewg_guideline_value") or contaminant.get("ca_phg_value")

    # Normalize result into MCL units for apples-to-apples comparison
    value_at_mcl_unit = (
        normalize_units(result_value, result_unit, mcl_unit)
        if mcl_unit and mcl_unit != result_unit
        else result_value
    )

    # Ratios
    ratio_guideline = (value_at_mcl_unit / guideline) if guideline and guideline > 0 else None
    ratio_mcl = (value_at_mcl_unit / mcl) if mcl and mcl > 0 else None

    # Severity logic (deterministic, documented, auditable)
    severity: Severity
    if result_value <= 0:
        severity = 0
    elif ratio_mcl is not None and ratio_mcl >= 2.0:
        severity = 4
    elif ratio_mcl is not None and ratio_mcl >= 1.0:
        severity = 3
    elif ratio_guideline is not None and ratio_guideline >= 1.0:
        severity = 2
    else:
        severity = 1

    # Summary line that's safe to drop into any output
    summary = _build_summary(contaminant["name"], result_value, result_unit, severity, ratio_mcl, ratio_guideline)

    return ContaminantFinding(
        code=contaminant["code"],
        name=contaminant["name"],
        category=contaminant["category"],
        value=result_value,
        unit=result_unit,
        sample_date=contaminant.get("sample_date"),
        epa_mcl=mcl,
        epa_mcl_unit=mcl_unit,
        health_guideline=guideline,
        severity=severity,
        severity_label=SEVERITY_LABELS[severity],
        severity_tone=SEVERITY_TONES[severity],
        ratio_to_guideline=ratio_guideline,
        ratio_to_mcl=ratio_mcl,
        plain_summary=summary,
        recommended_nsf_standards=contaminant.get("nsf_standards_removing", []),
        typical_reduction=contaminant.get("typical_reduction_jsonb", {}) or {},
    )


def _build_summary(name, value, unit, severity, ratio_mcl, ratio_guideline) -> str:
    if severity == 0:
        return f"{name}: not detected"
    if severity == 1:
        return f"{name}: {value} {unit} detected, below health guidelines"
    if severity == 2:
        if ratio_guideline and ratio_guideline >= 10:
            return f"{name}: {value} {unit} — {ratio_guideline:.0f}x above health guideline (legal but elevated)"
        return f"{name}: {value} {unit} — above health guideline (legal)"
    if severity == 3:
        return f"{name}: {value} {unit} — at or above legal limit"
    return f"{name}: {value} {unit} — significantly above legal limit"


def aggregate_profile(findings: list[ContaminantFinding]) -> dict:
    """Roll up findings into a summary used by both LLM and product matching."""
    by_category: dict[str, list[ContaminantFinding]] = {}
    for f in findings:
        by_category.setdefault(f.category, []).append(f)

    max_severity = max((f.severity for f in findings), default=0)

    all_nsf_needed: set[str] = set()
    for f in findings:
        if f.severity >= 2:
            all_nsf_needed.update(f.recommended_nsf_standards)

    headline = _headline_from_severity(max_severity, findings)

    return {
        "max_severity": max_severity,
        "max_severity_label": SEVERITY_LABELS[max_severity],
        "headline": headline,
        "by_category": {
            cat: [_finding_to_dict(f) for f in fs]
            for cat, fs in by_category.items()
        },
        "required_nsf_standards": sorted(all_nsf_needed),
        "violation_contaminants": [f.name for f in findings if f.severity >= 3],
        "elevated_contaminants": [f.name for f in findings if f.severity == 2],
    }


def _headline_from_severity(max_sev: int, findings: list[ContaminantFinding]) -> str:
    if max_sev >= 3:
        violations = [f.name for f in findings if f.severity >= 3]
        return f"Above legal limits: {', '.join(violations)}"
    if max_sev == 2:
        elevated = [f.name for f in findings if f.severity == 2]
        return f"Legal but above health guidelines: {', '.join(elevated[:3])}"
    if max_sev == 1:
        return "Within health guidelines; some contaminants detected at trace levels"
    return "No contaminants of concern detected in available data"


def _finding_to_dict(f: ContaminantFinding) -> dict:
    return {
        "code": f.code,
        "name": f.name,
        "value": f.value,
        "unit": f.unit,
        "severity": f.severity,
        "severity_label": f.severity_label,
        "ratio_to_guideline": f.ratio_to_guideline,
        "ratio_to_mcl": f.ratio_to_mcl,
        "summary": f.plain_summary,
        "sample_date": f.sample_date,
    }
