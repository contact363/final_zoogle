from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey, func
from sqlalchemy.orm import relationship
from app.database import Base


class WebsiteTrainingRules(Base):
    """
    Per-website CSS selector configuration.

    When a row exists for a website the Scrapy spider uses these selectors
    instead of (or in addition to) its generic auto-discovery logic.
    All selector fields are optional — the spider falls back gracefully to
    automatic extraction for any selector that is left blank.
    """

    __tablename__ = "website_training_rules"

    id         = Column(Integer, primary_key=True, index=True)
    website_id = Column(
        Integer,
        ForeignKey("websites.id", ondelete="CASCADE"),
        unique=True,   # one config row per website
        nullable=False,
        index=True,
    )

    # ── Core selectors ────────────────────────────────────────────────────────
    # CSS selector that matches the repeating card / listing container.
    # Example: ".product-card", "li.machine-item", "article.listing"
    listing_selector     = Column(Text, nullable=True)

    # CSS selector for the machine title *within* each card.
    # Example: "h2.title::text", ".product-name a::text"
    title_selector       = Column(Text, nullable=True)

    # CSS selector for the detail-page link *within* each card.
    # Example: "a.card-link::attr(href)", "h3 a::attr(href)"
    url_selector         = Column(Text, nullable=True)

    # CSS selector for the machine description *within* each card or detail page.
    # Example: ".description::text", "p.summary::text"
    description_selector = Column(Text, nullable=True)

    # CSS selector for the primary image *within* each card.
    # Example: "img::attr(src)", "img::attr(data-src)"
    image_selector       = Column(Text, nullable=True)

    # CSS selector for the price *within* each card.
    # Example: ".price::text", "span.asking-price::text"
    price_selector       = Column(Text, nullable=True)

    # CSS selector for the machine category *within* each card or listing page.
    # Example: ".category::text", "nav.breadcrumb li:last-child::text"
    category_selector    = Column(Text, nullable=True)

    # CSS selector for the "next page" or pagination link.
    # Example: "a.next::attr(href)", "a[rel='next']::attr(href)"
    pagination_selector  = Column(Text, nullable=True)

    # ── Timestamps ────────────────────────────────────────────────────────────
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # ── Relationship ──────────────────────────────────────────────────────────
    website = relationship("Website", back_populates="training_rules")
