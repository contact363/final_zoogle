-- Migration 0002: Extend website_training_rules + crawl_logs
-- Run once against your PostgreSQL database.
-- Safe to run multiple times (uses IF NOT EXISTS / DO $$ guards).

-- ── website_training_rules: new columns ──────────────────────────────────────

ALTER TABLE website_training_rules
    ADD COLUMN IF NOT EXISTS crawl_type           VARCHAR(20)   NOT NULL DEFAULT 'auto',
    ADD COLUMN IF NOT EXISTS use_playwright       BOOLEAN       NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS api_url              TEXT,
    ADD COLUMN IF NOT EXISTS api_key              TEXT,
    ADD COLUMN IF NOT EXISTS api_headers_json     TEXT,
    ADD COLUMN IF NOT EXISTS api_data_path        VARCHAR(255),
    ADD COLUMN IF NOT EXISTS api_pagination_param VARCHAR(50),
    ADD COLUMN IF NOT EXISTS api_page_size        INTEGER,
    ADD COLUMN IF NOT EXISTS field_map_json       TEXT,
    ADD COLUMN IF NOT EXISTS product_link_pattern TEXT,
    ADD COLUMN IF NOT EXISTS skip_url_patterns    TEXT,
    ADD COLUMN IF NOT EXISTS request_delay        NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS max_items            INTEGER;

-- ── crawl_logs: dedup tracking columns ───────────────────────────────────────

ALTER TABLE crawl_logs
    ADD COLUMN IF NOT EXISTS machines_updated INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS machines_skipped INTEGER DEFAULT 0;

-- Done.
SELECT 'Migration 0002 applied successfully.' AS result;
