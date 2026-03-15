"""
WebsiteTrainingRules — per-website crawl configuration.

Instead of creating a new spider for every website we store all site-specific
extraction rules here.  GenericSpider reads this row at spider init and adapts
its behaviour accordingly — one spider handles hundreds of websites.

Crawl modes
───────────
  auto       Default.  Spider auto-detects API vs HTML vs SPA.
  html       Force HTML/CSS extraction only (no API probing).
  api        Spider hits api_url directly (REST/JSON endpoint).
  playwright Force JS rendering for every page (slow but reliable for SPAs).

API config (used when crawl_type="api")
────────────────────────────────────────
  api_url              Full URL to the REST endpoint (pagination added automatically).
  api_key              Bearer token / API key injected as Authorization header.
  api_headers_json     Additional headers as a JSON object string.
  api_data_path        Dot-notation path to the results array in the JSON response.
                       E.g. "data", "results", "items", "response.products"
  api_pagination_param Query-param name for pagination.  "offset" or "page" (default "page").
  api_page_size        Items per page to request (default 100).
  field_map_json       JSON object mapping API field names → MachineItem fields.
                       E.g. {"title": "model_name", "brand": "brands.name"}

URL filtering
─────────────
  product_link_pattern  Regex that a URL must match to be treated as a product detail page.
                        Leave blank to use GenericSpider's built-in heuristics.
  skip_url_patterns     JSON list of regex patterns; matching URLs are skipped entirely.
                        Always merged with the built-in non-machine pattern list.

Request control
───────────────
  request_delay         Per-site download delay (seconds).  Overrides global setting.
  use_playwright        Force Playwright JS rendering for this site.
  max_items             Override CLOSESPIDER_ITEMCOUNT for this site.
"""
from sqlalchemy import (
    Boolean, Column, Integer, Numeric, String,
    DateTime, Text, ForeignKey, func,
)
from sqlalchemy.orm import relationship
from app.database import Base


class WebsiteTrainingRules(Base):
    __tablename__ = "website_training_rules"

    id         = Column(Integer, primary_key=True, index=True)
    website_id = Column(
        Integer,
        ForeignKey("websites.id", ondelete="CASCADE"),
        unique=True,
        nullable=False,
        index=True,
    )

    # ── Crawl mode ────────────────────────────────────────────────────────────
    crawl_type    = Column(String(20), nullable=False, default="auto")
    use_playwright = Column(Boolean, nullable=False, default=False)

    # ── HTML / CSS selectors ──────────────────────────────────────────────────
    listing_selector     = Column(Text, nullable=True)
    title_selector       = Column(Text, nullable=True)
    url_selector         = Column(Text, nullable=True)
    description_selector = Column(Text, nullable=True)
    image_selector       = Column(Text, nullable=True)
    price_selector       = Column(Text, nullable=True)
    category_selector    = Column(Text, nullable=True)
    pagination_selector  = Column(Text, nullable=True)

    # ── Direct REST / JSON API config ─────────────────────────────────────────
    api_url              = Column(Text, nullable=True)
    api_key              = Column(Text, nullable=True)
    api_headers_json     = Column(Text, nullable=True)   # JSON object string
    api_data_path        = Column(String(255), nullable=True)   # "data", "results.items"
    api_pagination_param = Column(String(50),  nullable=True)   # "offset" | "page"
    api_page_size        = Column(Integer,     nullable=True)   # items per request

    # JSON object: API key name → MachineItem field name
    # E.g. {"model_name": "model", "brands.name": "brand"}
    field_map_json       = Column(Text, nullable=True)

    # ── URL filtering ─────────────────────────────────────────────────────────
    # Regex: URL must match this to be queued as a product detail page
    product_link_pattern = Column(Text, nullable=True)

    # JSON list of regex strings — matching URLs are skipped entirely
    skip_url_patterns    = Column(Text, nullable=True)

    # ── Request control ───────────────────────────────────────────────────────
    request_delay = Column(Numeric(5, 2), nullable=True)   # seconds
    max_items     = Column(Integer,       nullable=True)   # CLOSESPIDER_ITEMCOUNT override

    # ── Timestamps ────────────────────────────────────────────────────────────
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # ── Relationship ──────────────────────────────────────────────────────────
    website = relationship("Website", back_populates="training_rules")
