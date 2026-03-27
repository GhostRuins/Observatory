-- Living Data Observatory — initial schema (TIMESTAMPTZ everywhere for timestamps)

CREATE TABLE IF NOT EXISTS topics (
    id SERIAL PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL,
    color_hex TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sources (
    id SERIAL PRIMARY KEY,
    topic_id INTEGER NOT NULL REFERENCES topics (id) ON DELETE RESTRICT,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    fetch_format TEXT NOT NULL CHECK (fetch_format IN ('json', 'csv', 'xml')),
    refresh_interval_hours INTEGER NOT NULL DEFAULT 24,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_sources_topic_name ON sources (topic_id, name);

CREATE TABLE IF NOT EXISTS datasets (
    id SERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources (id) ON DELETE CASCADE,
    title TEXT,
    raw_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    cleaned_data JSONB,
    chart_config JSONB,
    last_ingested_at TIMESTAMPTZ,
    last_cleaned_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_datasets_source UNIQUE (source_id)
);

CREATE INDEX IF NOT EXISTS idx_datasets_last_ingested ON datasets (last_ingested_at DESC);

CREATE TABLE IF NOT EXISTS source_health (
    id SERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources (id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('success', 'failure')),
    message TEXT,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_source_health_source_checked ON source_health (
    source_id,
    checked_at DESC
);

CREATE TABLE IF NOT EXISTS pending_sources (
    id SERIAL PRIMARY KEY,
    topic_id INTEGER REFERENCES topics (id) ON DELETE SET NULL,
    name TEXT,
    url TEXT NOT NULL,
    candidate_reason TEXT,
    score NUMERIC,
    evaluation_notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id SERIAL PRIMARY KEY,
    run_type TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    success BOOLEAN NOT NULL DEFAULT FALSE,
    message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_finished ON pipeline_runs (finished_at DESC);

INSERT INTO topics (slug, label, color_hex, updated_at)
VALUES
    ('climate', 'Climate', '#1D9E75', NOW()),
    ('health', 'Health', '#D85A30', NOW()),
    ('economics', 'Economics', '#378ADD', NOW()),
    ('politics', 'Politics', '#7F77DD', NOW()),
    ('general', 'General', '#888780', NOW())
ON CONFLICT (slug) DO UPDATE SET
    label = EXCLUDED.label,
    color_hex = EXCLUDED.color_hex,
    updated_at = NOW();
