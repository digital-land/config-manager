"""add request_meta table

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-04-22 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa

revision = "b2c3d4e5f6a7"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "request_meta",
        sa.Column("request_id", sa.Text(), nullable=False),
        sa.Column("retire_endpoint", sa.Boolean(), nullable=False, server_default="false"),
        sa.PrimaryKeyConstraint("request_id"),
    )


def downgrade():
    op.drop_table("request_meta")
