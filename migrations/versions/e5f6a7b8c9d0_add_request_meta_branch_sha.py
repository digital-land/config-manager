"""add request_meta branch_sha column

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-07-14 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

revision = "e5f6a7b8c9d0"
down_revision = "d4e5f6a7b8c9"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "request_meta",
        sa.Column("branch_sha", sa.Text(), nullable=True),
    )


def downgrade():
    op.drop_column("request_meta", "branch_sha")
