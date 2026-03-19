"""add service_lock table

Revision ID: a1b2c3d4e5f6
Revises: 5dbe33b2bfb5
Create Date: 2026-03-17 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa

revision = "a1b2c3d4e5f6"
down_revision = "5dbe33b2bfb5"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "service_lock",
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("locked_by", sa.Text(), nullable=False),
        sa.Column("locked_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("name"),
    )


def downgrade():
    op.drop_table("service_lock")
