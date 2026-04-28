"""update request_meta endpoints column

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-04-28 00:00:00.000000

"""

from alembic import op
import sqlalchemy as sa

revision = "c3d4e5f6a7b8"
down_revision = "b2c3d4e5f6a7"
branch_labels = None
depends_on = None


def upgrade():
    op.drop_column("request_meta", "retire_endpoint")
    op.add_column(
        "request_meta",
        sa.Column("endpoints_to_retire", sa.Text(), nullable=True),
    )


def downgrade():
    op.drop_column("request_meta", "endpoints_to_retire")
    op.add_column(
        "request_meta",
        sa.Column(
            "retire_endpoint",
            sa.Boolean(),
            nullable=False,
            server_default="false",
        ),
    )
