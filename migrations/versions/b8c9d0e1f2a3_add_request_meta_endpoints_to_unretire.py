"""add request_meta endpoints_to_unretire column

Revision ID: b8c9d0e1f2a3
Revises: b9c0d1e2f3a4
Create Date: 2026-07-27 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

revision = "b8c9d0e1f2a3"
down_revision = "b9c0d1e2f3a4"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "request_meta",
        sa.Column("endpoints_to_unretire", sa.Text(), nullable=True),
    )


def downgrade():
    op.drop_column("request_meta", "endpoints_to_unretire")
