-- Migration 0003: Add stock_number and dedup_key to machines table
-- Safe to run multiple times (uses IF NOT EXISTS).

-- ── machines: new columns ─────────────────────────────────────────────────────

ALTER TABLE machines
    ADD COLUMN IF NOT EXISTS stock_number VARCHAR(100),
    ADD COLUMN IF NOT EXISTS dedup_key    VARCHAR(64);

-- ── Indexes ───────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS ix_machines_stock_website
    ON machines (stock_number, website_id)
    WHERE stock_number IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_machines_dedup_key
    ON machines (dedup_key)
    WHERE dedup_key IS NOT NULL;

-- ── Back-fill dedup_key for existing rows ─────────────────────────────────────
-- Compute dedup_key = SHA-256(upper(brand) | '|' | upper(model) | '|' | '')
-- for rows that don't have a stock number yet.
-- This lets existing records participate in the cross-language dedup logic.

UPDATE machines
SET dedup_key = encode(
    digest(
        upper(coalesce(brand_normalized, ''))
        || '|'
        || upper(coalesce(model_normalized, ''))
        || '|',
        'sha256'
    ),
    'hex'
)
WHERE dedup_key IS NULL
  AND (brand_normalized IS NOT NULL OR model_normalized IS NOT NULL);

SELECT 'Migration 0003 applied successfully.' AS result;
