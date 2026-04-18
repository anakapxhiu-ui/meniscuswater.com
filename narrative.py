"""
LLM narrative generation.

This is deliberately narrow: the LLM is NOT deciding severity, NOT picking
products, NOT recommending medical action. It takes a validated structured
profile and writes it as a clear, calibrated paragraph in the Meniscus voice.

We use Anthropic's Claude Sonnet for this - fast enough for interactive use,
high enough quality to trust with health-adjacent writing.
"""

from __future__ import annotations

import json
import logging
import os

import httpx

log = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]
MODEL = "claude-sonnet-4-6"

SYSTEM_PROMPT = """You are the Meniscus water quality interpreter. You translate technical utility and regulatory data into clear, honest, calibrated prose for a homeowner.

CRITICAL RULES:
1. Every number in your output must come from the input data. Never invent values, never estimate, never round aggressively. If a number isn't in the input, don't write it.
2. Use the severity scoring provided. If profile.max_severity is 1, the headline is reassuring with context. If it's 3+, lead with the violation.
3. Do not provide medical advice. Say "associated with" or "regulatory agencies note" — never "will cause" or "is safe."
4. Do not use marketing language: no "shocking," "did you know," "finally," "the truth about."
5. Respect the reader's intelligence. Write for a curious adult.

VOICE:
- Plain and confident
- Short paragraphs, sentence-level variety
- Name regulatory bodies explicitly (EPA, EWG, California OEHHA)
- Acknowledge uncertainty honestly ("data last updated X")
- End with a clear "what to do" section that matches severity

STRUCTURE:
1. One-sentence headline reflecting profile.max_severity
2. "Your water at a glance": utility name, source, population served
3. "What's in it": organized by category, only mentioning categories with findings
4. "Neighborhood context": only if nearby sites are materially relevant (<3 miles or HRS score notable)
5. "What to do": ranked by impact, specific product categories (not brands — brand matching happens separately)

DO NOT:
- Use bullet points in the final output unless the input specifically has >5 items in a category
- Include headers in markdown format — this is prose
- Recommend specific brand names
- Panic about low-severity findings or minimize high-severity findings

Output is plain prose, no markdown, ready to render in a web UI."""


async def generate_narrative(narrative_input: dict) -> str:
    """Generate the prose report from a structured profile."""
    user_message = _build_user_message(narrative_input)

    async with httpx.AsyncClient(timeout=45) as client:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": MODEL,
                "max_tokens": 1500,
                "system": SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": user_message}],
            },
        )
        resp.raise_for_status()
        data = resp.json()

    text_blocks = [b["text"] for b in data["content"] if b.get("type") == "text"]
    return "\n\n".join(text_blocks).strip()


def _build_user_message(narrative_input: dict) -> str:
    """Render the structured input as a clear context block for the LLM."""
    utility = narrative_input["utility"]
    profile = narrative_input["profile"]
    sites = narrative_input.get("sites", [])
    address = narrative_input["address"]
    user_context = narrative_input.get("user_context", {})

    categories_text = []
    for cat, findings in profile.get("by_category", {}).items():
        lines = []
        for f in findings:
            line = f"- {f['name']}: {f['value']} {f['unit']}"
            if f.get("ratio_to_guideline"):
                line += f" ({f['ratio_to_guideline']:.1f}x health guideline)"
            if f.get("ratio_to_mcl"):
                line += f" ({f['ratio_to_mcl']:.1f}x legal MCL)"
            line += f" — severity {f['severity']} ({f['severity_label']})"
            lines.append(line)
        categories_text.append(f"## {cat}\n" + "\n".join(lines))

    sites_text = []
    for s in sites:
        sites_text.append(
            f"- {s['name']} ({s['site_type']}, {s.get('distance_miles', 0):.1f} miles away, status: {s.get('status', 'unknown')})"
        )

    return f"""Write a water quality report for this address.

ADDRESS: {address}

UTILITY:
- Name: {utility['name']}
- State: {utility['state']}
- Source: {utility['source_type']}
- Population served: {utility['population_served']:,}
- PWSID: {utility['pwsid']}

WATER PROFILE:
- Max severity: {profile['max_severity']} ({profile['max_severity_label']})
- Headline: {profile['headline']}
- Violation contaminants: {profile.get('violation_contaminants', [])}
- Elevated contaminants: {profile.get('elevated_contaminants', [])}
- NSF standards needed for treatment: {profile.get('required_nsf_standards', [])}

CONTAMINANT FINDINGS BY CATEGORY:
{chr(10).join(categories_text) if categories_text else "(no contaminants detected in available data)"}

NEARBY SITES:
{chr(10).join(sites_text) if sites_text else "(none within 5 miles)"}

USER CONTEXT:
{json.dumps(user_context, indent=2) if user_context else "(none provided)"}

Write the report now. Plain prose. No markdown headers. ~400-600 words."""
