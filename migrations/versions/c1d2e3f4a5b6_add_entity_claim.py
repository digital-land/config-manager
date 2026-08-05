"""add entity_claim table

Revision ID: c1d2e3f4a5b6
Revises: c0d1e2f3a4b5
Create Date: 2026-08-05 00:00:01.000000

"""

import sqlalchemy as sa
from alembic import op

revision = "c1d2e3f4a5b6"
down_revision = "c0d1e2f3a4b5"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "entity_claim",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("collection", sa.Text(), nullable=False),
        sa.Column("entity", sa.BigInteger(), nullable=False),
        sa.Column("branch", sa.Text(), nullable=False),
        sa.Column("request_id", sa.Text(), nullable=False),
        sa.Column("claimed_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "collection",
            "entity",
            "branch",
            name="uq_entity_claim_collection_entity_branch",
        ),
    )
    op.create_index(
        "ix_entity_claim_collection_branch",
        "entity_claim",
        ["collection", "branch"],
    )


def downgrade():
    op.drop_index("ix_entity_claim_collection_branch", table_name="entity_claim")
    op.drop_table("entity_claim")
