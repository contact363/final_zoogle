"""extend website_training_rules with API config and scheduling fields

Revision ID: 0002_extend_training_rules
Revises: 0001
Create Date: 2026-03-15

New columns added to website_training_rules:
  - crawl_type           VARCHAR(20)  DEFAULT 'auto'
  - use_playwright       BOOLEAN      DEFAULT FALSE
  - api_url              TEXT
  - api_key              TEXT
  - api_headers_json     TEXT
  - api_data_path        VARCHAR(255)
  - api_pagination_param VARCHAR(50)
  - api_page_size        INTEGER
  - field_map_json       TEXT
  - product_link_pattern TEXT
  - skip_url_patterns    TEXT
  - request_delay        NUMERIC(5,2)
  - max_items            INTEGER

New columns added to crawl_logs:
  - machines_updated     INTEGER  DEFAULT 0
  - machines_skipped     INTEGER  DEFAULT 0
"""
from alembic import op
import sqlalchemy as sa


# ── identifiers ───────────────────────────────────────────────────────────────
revision = "0002_extend_training_rules"
down_revision = None     # set to your previous revision id if you have one
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── website_training_rules: new columns ───────────────────────────────────
    with op.batch_alter_table("website_training_rules") as batch:
        batch.add_column(sa.Column("crawl_type",           sa.String(20),   nullable=True,  server_default="auto"))
        batch.add_column(sa.Column("use_playwright",       sa.Boolean(),    nullable=True,  server_default="false"))
        batch.add_column(sa.Column("api_url",              sa.Text(),       nullable=True))
        batch.add_column(sa.Column("api_key",              sa.Text(),       nullable=True))
        batch.add_column(sa.Column("api_headers_json",     sa.Text(),       nullable=True))
        batch.add_column(sa.Column("api_data_path",        sa.String(255),  nullable=True))
        batch.add_column(sa.Column("api_pagination_param", sa.String(50),   nullable=True))
        batch.add_column(sa.Column("api_page_size",        sa.Integer(),    nullable=True))
        batch.add_column(sa.Column("field_map_json",       sa.Text(),       nullable=True))
        batch.add_column(sa.Column("product_link_pattern", sa.Text(),       nullable=True))
        batch.add_column(sa.Column("skip_url_patterns",    sa.Text(),       nullable=True))
        batch.add_column(sa.Column("request_delay",        sa.Numeric(5,2), nullable=True))
        batch.add_column(sa.Column("max_items",            sa.Integer(),    nullable=True))

    # ── crawl_logs: new tracking columns (idempotent guard) ───────────────────
    # machines_updated and machines_skipped may already exist in some deployments
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    existing_cols = {c["name"] for c in inspector.get_columns("crawl_logs")}

    with op.batch_alter_table("crawl_logs") as batch:
        if "machines_updated" not in existing_cols:
            batch.add_column(sa.Column("machines_updated", sa.Integer(), nullable=True, server_default="0"))
        if "machines_skipped" not in existing_cols:
            batch.add_column(sa.Column("machines_skipped", sa.Integer(), nullable=True, server_default="0"))


def downgrade() -> None:
    with op.batch_alter_table("crawl_logs") as batch:
        batch.drop_column("machines_skipped")
        batch.drop_column("machines_updated")

    with op.batch_alter_table("website_training_rules") as batch:
        for col in (
            "max_items", "request_delay", "skip_url_patterns",
            "product_link_pattern", "field_map_json", "api_page_size",
            "api_pagination_param", "api_data_path", "api_headers_json",
            "api_key", "api_url", "use_playwright", "crawl_type",
        ):
            batch.drop_column(col)
