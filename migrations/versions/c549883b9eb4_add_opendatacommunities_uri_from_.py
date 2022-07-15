"""Add opendatacommunities_uri from digital-land/specification

Revision ID: c549883b9eb4
Revises: eb1a3fcba74e
Create Date: 2022-07-11 09:40:48.984998

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "c549883b9eb4"
down_revision = "eb1a3fcba74e"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "organisation", sa.Column("opendatacommunities_uri", sa.Text(), nullable=True)
    )
    op.add_column(
        "organisation", sa.Column("parliament_thesaurus", sa.Text(), nullable=True)
    )
    op.add_column("organisation", sa.Column("prefix", sa.Text(), nullable=True))
    op.add_column("organisation", sa.Column("reference", sa.Text(), nullable=True))


def downgrade():
    op.drop_column("organisation", "opendatacommunities_uri")
    op.drop_column("organisation", "parliament_thesaurus")
    op.drop_column("organisation", "prefix")
    op.drop_column("organisation", "reference")
