"""drop request_meta entity_redirects column

Revision ID: 7b8c9d0e1f2a
Revises: f6a7b8c9d0e1
Create Date: 2026-07-22 00:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

revision = "7b8c9d0e1f2a"
down_revision = "f6a7b8c9d0e1"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_column("request_meta", "entity_redirects")


def downgrade():
    op.add_column(
        "request_meta",
        sa.Column("entity_redirects", sa.Text(), nullable=True),
    )
