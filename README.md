# Meniscus

*Know what's in your water.*

The address-first platform for residential water quality. Enter a US address, get EPA data translated to plain English, and matched recommendations for NSF-certified filters and local WQA-certified installers.

## What's in this repo

```
meniscus/
├── backend/                    # Python + FastAPI + Postgres + PostGIS
│   ├── schema.sql             # Full database schema with spatial indexes
│   ├── api.py                 # /api/lookup endpoint (the main product)
│   ├── severity.py            # Deterministic severity scoring engine
│   ├── narrative.py           # LLM narrative generation (locked-down prompts)
│   ├── product_matcher.py     # NSF certification → filter matching
│   ├── ingest/
│   │   ├── sdwis_loader.py   # EPA Safe Drinking Water Info System loader
│   │   ├── ucmr5_loader.py   # PFAS monitoring data loader
│   │   ├── superfund_loader.py
│   │   └── seed_contaminants.py  # Hydrates reference DB from JSON
│   ├── requirements.txt
│   └── README.md              # Setup + run instructions
│
├── frontend/                   # Next.js 14 + Tailwind
│   ├── app/
│   │   ├── page.tsx           # Address lookup + report UI
│   │   ├── layout.tsx
│   │   └── globals.css
│   ├── tailwind.config.ts     # Meniscus brand palette
│   └── package.json
│
├── data/
│   └── contaminants.json      # 60+ contaminant knowledge base
│                              # (requires toxicologist review before prod)
│
└── docs/
    ├── Meniscus_Founders_Master_Document.md
    ├── Meniscus_Pitch_Deck.pptx        (12 slides)
    ├── Meniscus_Investor_Deck.pptx     (20 slides)
    ├── Meniscus_Financial_Model.xlsx   (24-month P&L)
    └── Sample_Report_Running_Water_Drive.md
```

## Quick start (backend)

```bash
cd backend
# Prereqs: Postgres 14+ with PostGIS, Python 3.11+
createdb meniscus
psql meniscus -f schema.sql

# Env vars: DATABASE_URL, MAPBOX_TOKEN, ANTHROPIC_API_KEY
pip install -r requirements.txt
python ingest/seed_contaminants.py
python ingest/sdwis_loader.py --state TX
python ingest/ucmr5_loader.py
python ingest/superfund_loader.py

uvicorn api:app --reload --port 8000
```

Test:
```bash
curl -X POST http://localhost:8000/api/lookup \
  -H "Content-Type: application/json" \
  -d '{"address": "7912 Running Water Dr, Austin, TX 78747"}'
```

## Quick start (frontend)

```bash
cd frontend
npm install
NEXT_PUBLIC_API_URL=http://localhost:8000 npm run dev
```

## What's NOT in the MVP

- State-level data sources (add per state as you expand)
- Service area polygon matching (falls back to state-level)
- Installer marketplace queries (schema is ready; endpoints come in Phase 2)
- User auth (use Supabase Auth or Clerk)
- Affiliate click tracking (use Rewardful, Impact, or roll your own)

## Production hardening checklist

- [ ] **Toxicologist review** of every entry in `data/contaminants.json`
- [ ] Add Sentry for error tracking
- [ ] Rate limit `/api/lookup`
- [ ] Tighten CORS from `["*"]` to explicit origins
- [ ] Nightly backup of `contaminant_results` (this is your moat)
- [ ] SOC 2 path for data licensing conversations with OEMs

## License

Proprietary. All rights reserved.
