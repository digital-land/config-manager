"""add request_meta entity_redirects column

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
Create Date: 2026-07-01 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

revision = "d4e5f6a7b8c9"
down_revision = "c3d4e5f6a7b8"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "request_meta",
        sa.Column("entity_redirects", sa.Text(), nullable=True),
    )


def downgrade():
    op.drop_column("request_meta", "entity_redirects")
