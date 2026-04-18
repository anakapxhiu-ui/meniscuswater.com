-- Meniscus database schema
-- Requires Postgres 14+ with PostGIS 3.x extension
-- Run: psql -d meniscus -f schema.sql

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- =============================================================================
-- Raw ingestion schema (staging tables for EPA data dumps)
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.sdwis_water_system (
    pwsid TEXT PRIMARY KEY,
    pws_name TEXT,
    primacy_agency_code TEXT,
    epa_region TEXT,
    pws_type_code TEXT,
    owner_type_code TEXT,
    population_served_count INTEGER,
    primary_source_code TEXT,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.sdwis_violation (
    violation_id TEXT PRIMARY KEY,
    pwsid TEXT,
    contaminant_code TEXT,
    contaminant_name TEXT,
    violation_category_code TEXT,
    is_health_based_ind TEXT,
    compl_per_begin_date DATE,
    compl_per_end_date DATE,
    violation_status TEXT,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.ucmr5_result (
    id SERIAL PRIMARY KEY,
    pwsid TEXT,
    pws_name TEXT,
    state TEXT,
    facility_id TEXT,
    sampling_point_id TEXT,
    sample_collection_date DATE,
    contaminant TEXT,
    mrl NUMERIC,
    analytical_result NUMERIC,
    unit TEXT,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ucmr5_pwsid ON raw.ucmr5_result(pwsid);
CREATE INDEX IF NOT EXISTS idx_ucmr5_contaminant ON raw.ucmr5_result(contaminant);

CREATE TABLE IF NOT EXISTS raw.echo_facility (
    registry_id TEXT PRIMARY KEY,
    facility_name TEXT,
    street_address TEXT,
    city_name TEXT,
    state_code TEXT,
    zip_code TEXT,
    latitude NUMERIC,
    longitude NUMERIC,
    sic_codes TEXT,
    naics_codes TEXT,
    facility_types_code TEXT,
    tri_flag TEXT,
    npdes_flag TEXT,
    rcra_flag TEXT,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.superfund_site (
    epa_id TEXT PRIMARY KEY,
    site_name TEXT,
    street_address TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    latitude NUMERIC,
    longitude NUMERIC,
    npl_status TEXT,
    hrs_score NUMERIC,
    contaminants_of_concern TEXT,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================================================
-- Normalized application schema
-- =============================================================================

-- Utilities: one row per public water system
CREATE TABLE IF NOT EXISTS utilities (
    pwsid TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    state TEXT NOT NULL,
    epa_region TEXT,
    source_type TEXT,  -- 'surface', 'groundwater', 'purchased', 'mixed'
    population_served INTEGER,
    treatment_methods TEXT[],
    service_area GEOGRAPHY(MULTIPOLYGON, 4326),  -- may be null if we don't have polygon
    service_counties TEXT[],  -- fallback when polygon isn't available
    is_active BOOLEAN DEFAULT TRUE,
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_utilities_service_area ON utilities USING GIST(service_area);
CREATE INDEX IF NOT EXISTS idx_utilities_name_trgm ON utilities USING GIN(name gin_trgm_ops);

-- Contaminant reference table (loaded from contaminants.json)
CREATE TABLE IF NOT EXISTS contaminants (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    aliases TEXT[],
    category TEXT NOT NULL,  -- 'disinfection_byproduct', 'pfas', 'heavy_metal', etc.
    cas_number TEXT,
    epa_mcl_value NUMERIC,
    epa_mcl_unit TEXT,
    epa_mclg_value NUMERIC,
    epa_action_level_value NUMERIC,
    ewg_guideline_value NUMERIC,
    ca_phg_value NUMERIC,
    typical_sources TEXT[],
    health_effects_plain TEXT,
    iarc_classification TEXT,
    nsf_standards_removing TEXT[],
    typical_reduction_jsonb JSONB,
    regulatory_group TEXT,
    is_ucmr5 BOOLEAN DEFAULT FALSE,
    part_of_group TEXT  -- e.g., individual THMs are 'part of' TTHM
);

CREATE INDEX IF NOT EXISTS idx_contaminants_category ON contaminants(category);

-- Every detected result, everywhere. The core data asset.
CREATE TABLE IF NOT EXISTS contaminant_results (
    id BIGSERIAL PRIMARY KEY,
    pwsid TEXT NOT NULL REFERENCES utilities(pwsid),
    contaminant_code TEXT NOT NULL,
    value NUMERIC NOT NULL,
    unit TEXT NOT NULL,
    sample_date DATE,
    source TEXT NOT NULL,  -- 'SDWIS', 'UCMR5', 'state:TX', etc.
    sample_location TEXT,
    is_violation BOOLEAN,
    regulatory_limit_at_sample NUMERIC,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_results_pwsid ON contaminant_results(pwsid);
CREATE INDEX IF NOT EXISTS idx_results_contaminant ON contaminant_results(contaminant_code);
CREATE INDEX IF NOT EXISTS idx_results_sample_date ON contaminant_results(sample_date DESC);
CREATE INDEX IF NOT EXISTS idx_results_pwsid_contaminant ON contaminant_results(pwsid, contaminant_code, sample_date DESC);

-- Industrial sites, Superfund, etc. — anything with a location that matters for risk
CREATE TABLE IF NOT EXISTS sites (
    id BIGSERIAL PRIMARY KEY,
    external_id TEXT UNIQUE,  -- EPA Registry ID, SEMS ID, etc.
    name TEXT NOT NULL,
    site_type TEXT NOT NULL,  -- 'superfund_npl', 'superfund_archived', 'rcra_corracts', 'npdes_major', 'tri_facility', 'brownfield'
    source_system TEXT NOT NULL,  -- 'SEMS', 'ECHO', etc.
    address TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    location GEOGRAPHY(POINT, 4326),
    hazard_ranking_score NUMERIC,
    status TEXT,
    contaminants_of_concern TEXT[],
    last_violation_date DATE,
    last_updated TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sites_location ON sites USING GIST(location);
CREATE INDEX IF NOT EXISTS idx_sites_type ON sites(site_type);
CREATE INDEX IF NOT EXISTS idx_sites_state ON sites(state);

-- Cached address lookups (we don't want to re-geocode and re-assemble for every page view)
CREATE TABLE IF NOT EXISTS address_lookups (
    address_hash TEXT PRIMARY KEY,  -- SHA256 of normalized address
    input_address TEXT NOT NULL,
    normalized_address TEXT,
    latitude NUMERIC,
    longitude NUMERIC,
    location GEOGRAPHY(POINT, 4326),
    resolved_pwsid TEXT,
    resolution_method TEXT,  -- 'polygon', 'county', 'manual', 'simplelab_api'
    assembled_report JSONB,  -- full assembled payload to skip re-fetch
    assembled_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_address_location ON address_lookups USING GIST(location);
CREATE INDEX IF NOT EXISTS idx_address_expires ON address_lookups(expires_at);

-- Filter products for recommendation engine
CREATE TABLE IF NOT EXISTS filter_products (
    id BIGSERIAL PRIMARY KEY,
    brand TEXT NOT NULL,
    model TEXT NOT NULL,
    form_factor TEXT NOT NULL,  -- 'pitcher', 'faucet', 'under_sink', 'countertop_ro', 'whole_house', 'shower'
    price_usd NUMERIC,
    replacement_cost_usd NUMERIC,
    replacement_frequency_months INTEGER,
    nsf_certifications TEXT[],
    contaminants_reduced TEXT[],  -- contaminant codes explicitly listed on cert
    affiliate_program TEXT,
    affiliate_url_template TEXT,
    affiliate_commission_pct NUMERIC,
    product_url TEXT,
    image_url TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    UNIQUE(brand, model)
);

CREATE INDEX IF NOT EXISTS idx_filter_form_factor ON filter_products(form_factor);
CREATE INDEX IF NOT EXISTS idx_filter_certs ON filter_products USING GIN(nsf_certifications);

-- Installers for the marketplace layer
CREATE TABLE IF NOT EXISTS installers (
    id BIGSERIAL PRIMARY KEY,
    business_name TEXT NOT NULL,
    wqa_certified BOOLEAN DEFAULT FALSE,
    wqa_cert_number TEXT,
    license_state TEXT,
    service_area_polygons GEOGRAPHY(MULTIPOLYGON, 4326),
    service_zip_codes TEXT[],
    specializations TEXT[],  -- 'whole_house_carbon', 'ro', 'softener', 'well_water', 'uv'
    avg_rating NUMERIC,
    review_count INTEGER DEFAULT 0,
    lead_fee_usd NUMERIC,
    commission_pct NUMERIC,
    contact_email TEXT,
    contact_phone TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    joined_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_installers_service_area ON installers USING GIST(service_area_polygons);
CREATE INDEX IF NOT EXISTS idx_installers_zip ON installers USING GIN(service_zip_codes);

-- Users
CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    email TEXT UNIQUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    address_hash TEXT,
    profile JSONB  -- concerns, household size, well vs municipal, etc.
);

-- User-uploaded test results (Phase 2)
CREATE TABLE IF NOT EXISTS user_tests (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES users(id),
    address_hash TEXT,
    lab_name TEXT,
    test_date DATE,
    results_jsonb JSONB,
    parsed_status TEXT,  -- 'pending', 'parsed', 'needs_review'
    source_pdf_url TEXT,
    uploaded_at TIMESTAMPTZ DEFAULT NOW()
);

-- Lead tracking for installer marketplace
CREATE TABLE IF NOT EXISTS installer_leads (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT REFERENCES users(id),
    installer_id BIGINT REFERENCES installers(id),
    address_hash TEXT,
    contaminants_to_address TEXT[],
    recommended_solution TEXT,
    lead_status TEXT,  -- 'sent', 'contacted', 'quoted', 'won', 'lost'
    lead_fee_charged NUMERIC,
    commission_earned NUMERIC,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    closed_at TIMESTAMPTZ
);

-- =============================================================================
-- Views for common queries
-- =============================================================================

-- Most recent result per utility + contaminant (the headline numbers)
CREATE OR REPLACE VIEW latest_results AS
SELECT DISTINCT ON (pwsid, contaminant_code)
    pwsid, contaminant_code, value, unit, sample_date, source, is_violation
FROM contaminant_results
ORDER BY pwsid, contaminant_code, sample_date DESC NULLS LAST;

-- Quick lookup: utility + last 3 years of active violations
CREATE OR REPLACE VIEW recent_violations AS
SELECT
    pwsid,
    contaminant_code,
    COUNT(*) AS violation_count,
    MAX(sample_date) AS most_recent
FROM contaminant_results
WHERE is_violation = TRUE
  AND sample_date >= CURRENT_DATE - INTERVAL '3 years'
GROUP BY pwsid, contaminant_code;

COMMENT ON TABLE utilities IS 'One row per US public water system from SDWIS';
COMMENT ON TABLE contaminant_results IS 'All detected results across SDWIS, UCMR5, and state sources. Core data asset.';
COMMENT ON TABLE address_lookups IS 'Cache layer. Never re-run full pipeline if we have a fresh cached result.';
