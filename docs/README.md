# Manage service — documentation

The Manage service (config-manager) is a Flask app for the "add data" and
"assign entities" operator workflows. After the [dead-code cleanup](#removed-blueprints),
the service consists of three active Flask blueprints:

| Blueprint | URL prefix | Purpose |
|---|---|---|
| `base` | `/` | Landing page (`index`), service-lock toggles |
| `auth` | `/auth` | GitHub OAuth login and admin-team checks |
| `datamanager` (+ `assign_entities`) | `/datamanager`, `/assign-entities` | The add-data and assign-entities operator workflows |

All operator functionality is reached from the landing page (`base.index`) and
lives in the `datamanager` blueprint. It is the only blueprint with substantial
logic, and the only one with documentation and tests.

## Contents

- [`datamanager/architecture.md`](datamanager/architecture.md) — structure of the
  `datamanager` blueprint (router, controllers, services, utils) and how to work within it
- [`datamanager/add-data.md`](datamanager/add-data.md) — the **Add data** user flow, step by step
- [`datamanager/assign-entities.md`](datamanager/assign-entities.md) — the **Assign entities**
  (batch) flow end to end, and how it shares steps with Add data
- [`datamanager/github-add.md`](datamanager/github-add.md) — the GitHub workflow-dispatch that adds
  data to the `config` repo, and the stale-assessment guard that runs at confirmation

## Removed blueprints

The following blueprints were **registered but unreachable from the live UI** and were
removed during the cleanup (no page or nav linked to them; they were only reachable by
typing a URL directly):

- `source`, `dataset`, `report` (`/reporting`), `schema`, `endpoint`, `publisher`

Their views, templates, `data_access` query modules, static JS bundles, and the old
cross-blueprint navigation (`layouts/layout.html`, `macro/primary-nav.html`) were removed
along with them. If any of this functionality is needed again, recover it from git history.

## Removed config database + CLI

Those blueprints were also the only readers/writers of the **pipeline/config schema**
in Postgres (collections, datasets, sources, endpoints, pipeline rules, etc.). With the
editing UI gone, that schema and its tooling were retired:

- The config ORM models were removed from [`application/db/models.py`](../application/db/models.py),
  which now defines only `ServiceLock` and `RequestMeta` — the two tables the running
  service actually uses.
- The `flask data load` / `flask data drop` (dev seeding) and `flask publish changes`
  (Postgres → `digital-land/config` repo) CLI commands were removed, along with
  `application/publish/` (the pydantic export models) and the `PyGithub` dependency.
- Migration [`a7b8c9d0e1f2_drop_config_tables`](../migrations/versions/a7b8c9d0e1f2_drop_config_tables.py)
  drops the 24 config tables and the `publication_status` enum. It runs automatically via
  `flask db upgrade` on deploy. **This is destructive and has no downgrade** — recover the
  schema from migration `f8c9f47c8797` in git history if it is ever needed again.

## Keeping docs up to date

A non-blocking GitHub Actions check ([`.github/workflows/docs-check.yml`](../.github/workflows/docs-check.yml))
posts a reminder on pull requests that change application code without updating anything in
`docs/`. It is a nudge, not a merge gate — update the relevant docs when behaviour changes.
