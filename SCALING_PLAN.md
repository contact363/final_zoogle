# Zoogle — Scaling Plan: 500–1000 Websites + Cross-Site Deduplication

## Current State (March 2026)
- 2 websites (Corelmachine, Zatpatmachines)
- ~5,400 machines total
- Single-process Scrapy crawls, sequential scheduling
- In-website dedup only (brand+model+stock_number per site)

---

## Phase 1 — Crawl Infrastructure (50–100 sites)

### 1.1 Spider Routing
Each new website falls into one of three tiers:

| Tier | Sites | Approach |
|------|-------|----------|
| **API-backed** | Supabase, custom REST | Dedicated spider (like zatpatmachines) — paginate the API, no HTML parsing |
| **SSR/static HTML** | Regular product listing pages | Generic spider with trained rules in `website_training_rules` table |
| **SPA / JS-heavy** | React/Vue SPAs without public API | Generic spider + Playwright mode (headless browser) |

**Action:** Add a `spider_tier` column to `websites` table (`api`, `html`, `spa`). `_run_scrapy` reads this to pick the right mode, removing the hardcoded domain check.

### 1.2 Parallel Crawl Queue
Current: sequential, one-at-a-time.
Target: Celery workers with concurrency=4, each site gets its own task.

```
websites (100) → Celery Beat schedules evenly over 24h
                → 4 workers run in parallel
                → each worker handles 1 site at a time
```

**Action:** Deploy Redis + 4 Celery workers on Render or Railway. `CLOSESPIDER_ITEMCOUNT` removed (already done) — each spider self-terminates when it runs out of pages.

### 1.3 Rate Limiting Per Domain
Add per-domain `request_delay` and `concurrent_requests` to `website_training_rules`. Generic spider reads these at init.

---

## Phase 2 — Cross-Site Deduplication (100–500 sites)

The core problem: the same machine (e.g. "Mazak QTN-200 CNC Lathe, 2019") can appear on 5 different dealer sites with different stock numbers, slightly different descriptions, and different prices.

### 2.1 Dedup Key Design

Current per-site key: `SHA-256(brand|model|stock_number)`
New cross-site key: `SHA-256(normalized_brand|normalized_model|year|condition)`

The `year` and `condition` fields are strong discriminators — a 2019 machine and a 2021 machine are genuinely different listings.

**Schema change:**
```sql
ALTER TABLE machines ADD COLUMN cross_site_dedup_key VARCHAR(64);
CREATE INDEX ix_machines_cross_site_dedup ON machines(cross_site_dedup_key);
```

### 2.2 Master Listing (Canonical Record)
When two machines from different sites share the same `cross_site_dedup_key`:
- Keep **all** records (each has its own price, dealer, URL)
- Mark one as the **canonical** master: `is_canonical = True`
- Others point to it via `canonical_machine_id`
- Search results show the canonical + "X more dealers" link

**Schema:**
```sql
ALTER TABLE machines ADD COLUMN is_canonical BOOLEAN DEFAULT TRUE;
ALTER TABLE machines ADD COLUMN canonical_machine_id INTEGER REFERENCES machines(id);
```

### 2.3 Dedup Pipeline Stage
Add a new `CrossSiteDeduplicationPipeline` (priority 390, after per-site DeduplicationPipeline):

```python
# In DatabasePipeline.process_item():
cross_key = build_cross_site_dedup_key(brand, model, year, condition)
canonical = db.query(Machine).filter(
    Machine.cross_site_dedup_key == cross_key,
    Machine.is_canonical == True,
).first()
if canonical:
    machine.is_canonical = False
    machine.canonical_machine_id = canonical.id
```

### 2.4 Search Result Grouping
Search returns canonical machines only (`is_canonical=True`).
Detail page shows: "Also available at N other dealers" with price comparison.

---

## Phase 3 — Quality & Scale (500–1000 sites)

### 3.1 Crawl Health Dashboard
- Per-website "expected count" field — alert if crawl returns <80% of expected
- Automatic re-crawl trigger if count drops >20%
- Email/Slack alert for sites that error 3 crawls in a row → auto-disable

### 3.2 Incremental Crawl (Changed Pages Only)
For sites with sitemaps or RSS feeds:
- Download sitemap → diff against last crawl's URL list
- Only re-crawl URLs that changed (HTTP ETag / Last-Modified)
- Full re-crawl every 7 days regardless

### 3.3 Image CDN
At 1000 sites × 5000 machines × 1 image = 5M images.
Store on Cloudflare R2 (free egress) instead of local disk.

### 3.4 Search Scaling
Current: PostgreSQL full-text search.
At 500k+ machines: migrate to Elasticsearch or Typesense for sub-100ms search.
Schema stays the same — search service swaps the backend.

---

## Implementation Priority Order

| Priority | Task | Effort |
|----------|------|--------|
| 1 | Deploy Redis + Celery workers (parallel crawls) | 1 day |
| 2 | Add `spider_tier` to websites table | 2 hrs |
| 3 | Cross-site dedup key + schema migration | 1 day |
| 4 | CrossSiteDeduplicationPipeline | 1 day |
| 5 | Search grouping by canonical | 1 day |
| 6 | Crawl health alerts | 1 day |
| 7 | Incremental crawl (sitemap diff) | 3 days |
| 8 | Image CDN migration | 2 days |
| 9 | Elasticsearch migration | 1 week |

---

## Database Migration (run after deploying code)

```sql
-- Already handled by auto_migrate on startup if using SQLAlchemy:
-- machines.last_crawled_at  (added in this release)

-- Phase 2 additions:
ALTER TABLE machines ADD COLUMN cross_site_dedup_key VARCHAR(64);
ALTER TABLE machines ADD COLUMN is_canonical BOOLEAN DEFAULT TRUE;
ALTER TABLE machines ADD COLUMN canonical_machine_id INTEGER REFERENCES machines(id);
CREATE INDEX ix_machines_cross_site_dedup ON machines(cross_site_dedup_key);

-- websites tier:
ALTER TABLE websites ADD COLUMN spider_tier VARCHAR(20) DEFAULT 'html';
UPDATE websites SET spider_tier = 'api' WHERE url LIKE '%zatpatmachines%';
UPDATE websites SET spider_tier = 'html' WHERE url LIKE '%corelmachine%';
```
