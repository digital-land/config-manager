"""add request_meta dispatched_at column

Revision ID: c0d1e2f3a4b5
Revises: b8c9d0e1f2a3
Create Date: 2026-08-05 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

revision = "c0d1e2f3a4b5"
down_revision = "b8c9d0e1f2a3"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "request_meta",
        sa.Column("dispatched_at", sa.DateTime(), nullable=True),
    )


def downgrade():
    op.drop_column("request_meta", "dispatched_at")
