"""add request_meta source_flow column

Revision ID: b9c0d1e2f3a4
Revises: 7b8c9d0e1f2a
Create Date: 2026-07-28 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

revision = "b9c0d1e2f3a4"
down_revision = "7b8c9d0e1f2a"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "request_meta",
        sa.Column("source_flow", sa.Text(), nullable=True),
    )


def downgrade():
    op.drop_column("request_meta", "source_flow")
