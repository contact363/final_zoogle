-- =============================================================================
-- ZOOGLE – PostgreSQL Schema
-- Run: psql -U postgres -d zoogle -f scripts/schema.sql
-- =============================================================================

-- Extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;      -- trigram similarity for fuzzy search
CREATE EXTENSION IF NOT EXISTS unaccent;      -- accent-insensitive search

-- =============================================================================
-- USERS
-- =============================================================================
CREATE TABLE IF NOT EXISTS users (
    id              SERIAL PRIMARY KEY,
    email           VARCHAR(255) UNIQUE NOT NULL,
    hashed_password VARCHAR(255) NOT NULL,
    full_name       VARCHAR(255),
    is_active       BOOLEAN DEFAULT TRUE,
    is_admin        BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ
);

-- =============================================================================
-- WEBSITES
-- =============================================================================
CREATE TABLE IF NOT EXISTS websites (
    id               SERIAL PRIMARY KEY,
    name             VARCHAR(255) NOT NULL,
    url              VARCHAR(2048) UNIQUE NOT NULL,
    description      TEXT,
    is_active        BOOLEAN DEFAULT TRUE,
    crawl_enabled    BOOLEAN DEFAULT TRUE,
    machine_count    INTEGER DEFAULT 0,
    last_crawled_at  TIMESTAMPTZ,
    crawl_status     VARCHAR(50) DEFAULT 'pending',  -- pending|running|success|error
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_websites_url ON websites(url);
CREATE INDEX IF NOT EXISTS idx_websites_status ON websites(crawl_status);

-- =============================================================================
-- MACHINES
-- =============================================================================
CREATE TABLE IF NOT EXISTS machines (
    id               SERIAL PRIMARY KEY,
    website_id       INTEGER REFERENCES websites(id) ON DELETE CASCADE,

    -- Core
    machine_type     VARCHAR(100),
    brand            VARCHAR(100),
    model            VARCHAR(200),
    price            NUMERIC(14,2),
    currency         VARCHAR(10) DEFAULT 'USD',
    location         VARCHAR(255),
    description      TEXT,

    -- Source
    machine_url      VARCHAR(2048) NOT NULL,
    website_source   VARCHAR(255),

    -- Normalized fields (populated by normalization pipeline)
    brand_normalized VARCHAR(100),
    model_normalized VARCHAR(200),
    type_normalized  VARCHAR(100),

    -- Media
    thumbnail_url    VARCHAR(2048),
    thumbnail_local  VARCHAR(512),

    -- Full-text search vector (auto-maintained by trigger below)
    search_vector    TSVECTOR,

    -- Deduplication
    content_hash     VARCHAR(64) UNIQUE,

    is_active        BOOLEAN DEFAULT TRUE,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    updated_at       TIMESTAMPTZ
);

-- Standard indexes
CREATE INDEX IF NOT EXISTS idx_machines_type      ON machines(type_normalized);
CREATE INDEX IF NOT EXISTS idx_machines_brand     ON machines(brand_normalized);
CREATE INDEX IF NOT EXISTS idx_machines_location  ON machines(location);
CREATE INDEX IF NOT EXISTS idx_machines_price     ON machines(price);
CREATE INDEX IF NOT EXISTS idx_machines_website   ON machines(website_id);
CREATE INDEX IF NOT EXISTS idx_machines_hash      ON machines(content_hash);
CREATE INDEX IF NOT EXISTS idx_machines_created   ON machines(created_at DESC);

-- GIN indexes for full-text search
CREATE INDEX IF NOT EXISTS idx_machines_search_vector ON machines USING GIN(search_vector);

-- Trigram indexes for partial/fuzzy matching
CREATE INDEX IF NOT EXISTS idx_machines_brand_trgm ON machines USING GIN(brand gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_machines_model_trgm ON machines USING GIN(model gin_trgm_ops);

-- Composite indexes for common filter combos
CREATE INDEX IF NOT EXISTS idx_machines_type_brand ON machines(type_normalized, brand_normalized);

-- =============================================================================
-- TRIGGER: auto-update search_vector on insert/update
-- =============================================================================
CREATE OR REPLACE FUNCTION machines_search_vector_update()
RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector :=
        setweight(to_tsvector('english', coalesce(NEW.brand, '')), 'A') ||
        setweight(to_tsvector('english', coalesce(NEW.model, '')), 'A') ||
        setweight(to_tsvector('english', coalesce(NEW.machine_type, '')), 'B') ||
        setweight(to_tsvector('english', coalesce(NEW.type_normalized, '')), 'B') ||
        setweight(to_tsvector('english', coalesce(NEW.location, '')), 'C') ||
        setweight(to_tsvector('english', coalesce(NEW.description, '')), 'D');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_machines_search_vector ON machines;
CREATE TRIGGER trg_machines_search_vector
    BEFORE INSERT OR UPDATE ON machines
    FOR EACH ROW EXECUTE FUNCTION machines_search_vector_update();

-- =============================================================================
-- MACHINE IMAGES
-- =============================================================================
CREATE TABLE IF NOT EXISTS machine_images (
    id          SERIAL PRIMARY KEY,
    machine_id  INTEGER REFERENCES machines(id) ON DELETE CASCADE,
    image_url   VARCHAR(2048) NOT NULL,
    local_path  VARCHAR(512),
    is_primary  BOOLEAN DEFAULT FALSE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_machine_images_machine ON machine_images(machine_id);

-- =============================================================================
-- MACHINE SPECS
-- =============================================================================
CREATE TABLE IF NOT EXISTS machine_specs (
    id          SERIAL PRIMARY KEY,
    machine_id  INTEGER REFERENCES machines(id) ON DELETE CASCADE,
    spec_key    VARCHAR(100) NOT NULL,
    spec_value  TEXT,
    spec_unit   VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_machine_specs_machine ON machine_specs(machine_id, spec_key);

-- =============================================================================
-- CRAWL LOGS
-- =============================================================================
CREATE TABLE IF NOT EXISTS crawl_logs (
    id                SERIAL PRIMARY KEY,
    website_id        INTEGER REFERENCES websites(id) ON DELETE CASCADE,
    task_id           VARCHAR(255),
    status            VARCHAR(50) NOT NULL,   -- started|running|success|error|stopped
    machines_found    INTEGER DEFAULT 0,
    machines_new      INTEGER DEFAULT 0,
    machines_updated  INTEGER DEFAULT 0,
    machines_skipped  INTEGER DEFAULT 0,
    errors_count      INTEGER DEFAULT 0,
    error_details     TEXT,
    log_output        TEXT,
    started_at        TIMESTAMPTZ DEFAULT NOW(),
    finished_at       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_crawl_logs_website ON crawl_logs(website_id);
CREATE INDEX IF NOT EXISTS idx_crawl_logs_status  ON crawl_logs(status);
CREATE INDEX IF NOT EXISTS idx_crawl_logs_started ON crawl_logs(started_at DESC);

-- =============================================================================
-- SAVED MACHINES
-- =============================================================================
CREATE TABLE IF NOT EXISTS saved_machines (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER REFERENCES users(id) ON DELETE CASCADE,
    machine_id  INTEGER REFERENCES machines(id) ON DELETE CASCADE,
    saved_at    TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_id, machine_id)
);

CREATE INDEX IF NOT EXISTS idx_saved_machines_user ON saved_machines(user_id);

-- =============================================================================
-- SEARCH LOGS
-- =============================================================================
CREATE TABLE IF NOT EXISTS search_logs (
    id             SERIAL PRIMARY KEY,
    query          VARCHAR(512) NOT NULL,
    results_count  INTEGER DEFAULT 0,
    user_id        INTEGER,
    ip_address     VARCHAR(45),
    created_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_search_logs_created ON search_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_search_logs_query   ON search_logs USING GIN(to_tsvector('english', query));
