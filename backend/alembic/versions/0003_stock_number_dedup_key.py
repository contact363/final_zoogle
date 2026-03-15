"""add stock_number and dedup_key to machines table

Revision ID: 0003_stock_number_dedup_key
Revises: 0002_extend_training_rules
Create Date: 2026-03-15

Changes:
  machines.stock_number  VARCHAR(100)  — dealer reference / stock number
  machines.dedup_key     VARCHAR(64)   — cross-language deduplication hash
"""
from alembic import op
import sqlalchemy as sa


revision      = "0003_stock_number_dedup_key"
down_revision = "0002_extend_training_rules"
branch_labels = None
depends_on    = None


def upgrade() -> None:
    with op.batch_alter_table("machines") as batch:
        batch.add_column(sa.Column("stock_number", sa.String(100), nullable=True))
        batch.add_column(sa.Column("dedup_key",    sa.String(64),  nullable=True))

    op.create_index(
        "ix_machines_stock_website",
        "machines",
        ["stock_number", "website_id"],
        postgresql_where=sa.text("stock_number IS NOT NULL"),
    )
    op.create_index(
        "ix_machines_dedup_key",
        "machines",
        ["dedup_key"],
        postgresql_where=sa.text("dedup_key IS NOT NULL"),
    )

    # Back-fill dedup_key for existing rows (PostgreSQL-specific)
    op.execute("""
        UPDATE machines
        SET dedup_key = encode(
            digest(
                upper(coalesce(brand_normalized, ''))
                || '|' || upper(coalesce(model_normalized, '')) || '|',
                'sha256'
            ),
            'hex'
        )
        WHERE dedup_key IS NULL
          AND (brand_normalized IS NOT NULL OR model_normalized IS NOT NULL)
    """)


def downgrade() -> None:
    op.drop_index("ix_machines_dedup_key",      table_name="machines")
    op.drop_index("ix_machines_stock_website",  table_name="machines")
    with op.batch_alter_table("machines") as batch:
        batch.drop_column("dedup_key")
        batch.drop_column("stock_number")
