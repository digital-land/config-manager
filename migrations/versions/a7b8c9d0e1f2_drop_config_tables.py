"""drop unused config tables

Removes the pipeline/config schema that was only read and written by the
removed source/dataset/endpoint/report blueprints and the retired
``flask data load`` / ``flask publish changes`` CLI commands. The running
service only uses ``service_lock`` and ``request_meta``.

The tables are dropped with ``IF EXISTS ... CASCADE`` so the migration is
robust to environments where a subset was already absent and does not depend
on foreign-key drop ordering.

Revision ID: a7b8c9d0e1f2
Revises: f6a7b8c9d0e1
Create Date: 2026-07-24

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "a7b8c9d0e1f2"
down_revision = "f6a7b8c9d0e1"
branch_labels = None
depends_on = None


# Config tables in reverse creation order (children before parents). CASCADE
# makes the ordering non-critical, but we keep it dependency-safe regardless.
CONFIG_TABLES = [
    "transform",
    "source_dataset",
    "skip",
    "patch",
    "lookup",
    "filter",
    "default_value",
    "dataset_field",
    "convert",
    "concat",
    "combine",
    "column",
    "_default",
    "source",
    "pipeline",
    "field",
    "dataset",
    "typology",
    "organisation",
    "licence",
    "endpoint",
    "datatype",
    "collection",
    "attribution",
]


def upgrade():
    for table in CONFIG_TABLES:
        op.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
    op.execute("DROP TYPE IF EXISTS publication_status")


def downgrade():
    # These tables and the associated ORM models were permanently removed.
    # Recreating them faithfully is out of scope; recover the schema from the
    # initial migration (f8c9f47c8797) and subsequent revisions in git history
    # if it is ever needed again.
    raise NotImplementedError(
        "Downgrade is not supported: the config tables were permanently removed. "
        "Recover from migration f8c9f47c8797 in git history if required."
    )
