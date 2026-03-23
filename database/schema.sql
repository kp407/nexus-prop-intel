-- NEXUS ASIA PROP INTEL — Supabase Schema

CREATE TABLE companies (
    company_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    company_name TEXT NOT NULL,
    normalized_name TEXT,
    industry TEXT,
    website TEXT,
    hq_location TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(normalized_name)
);

CREATE TABLE signals (
    signal_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    company_id UUID REFERENCES companies(company_id) ON DELETE CASCADE,
    signal_type TEXT NOT NULL,
    space_type TEXT,
    location TEXT,
    confidence_score NUMERIC(5,2),
    summary TEXT,
    source_url TEXT,
    timestamp TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE documents (
    document_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    signal_id UUID REFERENCES signals(signal_id) ON DELETE CASCADE,
    document_url TEXT,
    raw_text TEXT,
    parsed_text TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE lead_scores (
    company_id UUID REFERENCES companies(company_id) ON DELETE CASCADE PRIMARY KEY,
    score INTEGER DEFAULT 0,
    signal_count INTEGER DEFAULT 0,
    priority_level TEXT DEFAULT 'LOW',
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE signals
    ADD COLUMN fts tsvector
    GENERATED ALWAYS AS (
        to_tsvector('english', coalesce(signal_type,'') || ' ' ||
                               coalesce(location,'') || ' ' ||
                               coalesce(summary,''))
    ) STORED;

CREATE INDEX signals_fts_idx ON signals USING GIN(fts);

ALTER TABLE companies
    ADD COLUMN fts tsvector
    GENERATED ALWAYS AS (
        to_tsvector('english', coalesce(company_name,'') || ' ' ||
                               coalesce(industry,'') || ' ' ||
                               coalesce(hq_location,''))
    ) STORED;

CREATE INDEX companies_fts_idx ON companies USING GIN(fts);
