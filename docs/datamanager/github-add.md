# Add-data GitHub workflow

When a user confirms an add-data (or assign-entities) submission, config-manager triggers a GitHub
Action in the `digital-land/config` repo that commits the assessed data onto a config branch and
opens/updates a PR. This document describes that trigger and the workflow it runs.

The trigger lives in `services/github.py` (`trigger_add_data_async_workflow`, called from
`controllers/preview.py`). The workflow itself is `.github/workflows/add-data-async-script.yml`
("Add Data From Async API") in `digital-land/config`, running `bin/add_data.py`.

## How it works

1. Fetches the full request from the async API using `request_id`.
2. Validates the request `status` is `COMPLETE` with no error.
3. Resolves the target branch (see [Branch behaviour](#branch-behaviour)).
4. Appends rows to the relevant collection/pipeline CSVs (see [CSV files updated](#csv-files-updated)).
5. Commits and creates or updates a PR against `main`.

## Triggering the workflow

**Endpoint:** `POST {GITHUB_API_BASE_URL}/repos/digital-land/config/dispatches`

**Headers:**
- `Accept: application/vnd.github+json`
- `Authorization: Bearer <INSTALLATION_ACCESS_TOKEN>`
- `X-GitHub-Api-Version: 2022-11-28`

**Payload:**
```json
{
  "event_type": "add-data-async-script",
  "client_payload": {
    "request_id": "RW37P9DNRYSTK2eEByDGeq",
    "triggered_by": "your-name-or-system",
    "branch": "config-manager-update",
    "retire_endpoints": "hash1,hash2",
    "environment": "production"
  }
}
```

| Field | Required | Purpose |
| --- | --- | --- |
| `event_type` | yes | Must be `add-data-async-script` |
| `client_payload.request_id` | yes | ID of a `COMPLETE` async request |
| `client_payload.triggered_by` | no | Who/what triggered it (used in commit/PR content) |
| `client_payload.branch` | no | Target branch — see [Branch behaviour](#branch-behaviour) |
| `client_payload.retire_endpoints` | no | Comma-separated endpoint hashes to end-date |
| `client_payload.environment` | no | Async API environment: `development` \| `staging` \| `production` (default `staging`) |

The branch config-manager sends is its `CONFIG_REPO_BRANCH` setting — `config-manager-update` in
production, `test-config-manager-update` in development.

## Branch behaviour

`bin/add_data.py` (`resolve_branch`) is branch-agnostic; the `branch` parameter controls how it
creates or updates branches and PRs.

- **No `branch`** — a new branch `add-data-async/{collection}-{timestamp}` is created and a PR is
  opened against `main`.
- **`branch` given, open PR exists** — checks out the branch, appends on top, and updates the
  existing PR body with the new submission label. This batches multiple submissions into one PR.
- **`branch` given, branch exists but no open PR** — checks out the branch, appends, opens a new
  PR against `main`.
- **`branch` given, branch does not exist** — creates a fresh branch with that name and opens a
  PR against `main`.
- **Test mode** (`--test`) — commits to a `test/{branch}` branch as a **draft** PR that must not
  be merged. config-manager does not send this today; it uses a dedicated test branch instead.

Only a PR from `config-manager-update → main` is auto-merged
(`auto-merge-config-manager.yml`); other branch names (e.g. `test-config-manager-update`) open a
normal PR that is never auto-merged.

## Commit messages and PR content

Each submission produces a commit and PR label:

```
add-{dataset}-{organisation}-{triggered_by}
```

e.g. `add-article-4-direction-area-local-authority:SKP-matt`. When batched onto one branch, the PR
body accumulates all labels.

## Async API service

The workflow fetches request data from `{ASYNC_API_BASE_URL}/requests/{request_id}`, where the base
URL is resolved from the `environment` in the payload:

| environment | base URL |
| --- | --- |
| `development` | `http://development-pub-async-api-lb-…elb.amazonaws.com` |
| `staging` (default) | `http://staging-pub-async-api-lb-…elb.amazonaws.com` |
| `production` | `http://production-pub-async-api-lb-…elb.amazonaws.com` |

### Expected response shape

```json
{
  "params": {
    "collection": "article-4-direction",
    "dataset": "article-4-direction-area",
    "organisation": "local-authority:SKP",
    "column_mapping": { "geom": "geometry" },
    "authoritative": true
  },
  "status": "COMPLETE",
  "response": {
    "data": {
      "endpoint-summary": { "new_endpoint_entry": {}, "endpoint_url_in_endpoint_csv": false },
      "source-summary": { "new_source_entry": {}, "documentation_url_in_source_csv": false },
      "pipeline-summary": { "new-entities": [], "entity-organisation": [], "old-entity": [] }
    },
    "error": null
  }
}
```

## CSV files updated

| File | Source | Condition |
| --- | --- | --- |
| `collection/{collection}/endpoint.csv` | `endpoint-summary.new_endpoint_entry` | `endpoint_url_in_endpoint_csv` is false |
| `collection/{collection}/source.csv` | `source-summary.new_source_entry` | `documentation_url_in_source_csv` is false |
| `pipeline/{collection}/lookup.csv` | `pipeline-summary.new-entities` | array non-empty |
| `pipeline/{collection}/column.csv` | `params.column_mapping` | mapping non-empty |
| `pipeline/{collection}/entity-organisation.csv` | `pipeline-summary.entity-organisation` | `params.authoritative` true, and not an overlap/error |
| `pipeline/{collection}/old-entity.csv` | `pipeline-summary.old-entity` | array non-empty |

Retiring endpoints (`retire_endpoints`) does not append rows — it sets `end-date` on the matching
rows in `collection/{collection}/endpoint.csv` and `source.csv`.

## Authentication

config-manager triggers the dispatch as a GitHub App (`services/github.py`), using
`GITHUB_APP_ID`, `GITHUB_APP_INSTALLATION_ID`, and `GITHUB_APP_PRIVATE_KEY`. The workflow in the
config repo authenticates separately to fetch the request and push commits.

## Error handling

The workflow fails if `request_id` is empty, the request cannot be fetched, its status is not
`COMPLETE`, the response contains an error, the `collection` has no matching `collection/` and
`pipeline/` directories, or there are no changes to commit.

## Related

- [stale-check.md](stale-check.md) — how config-manager avoids committing stale entity numbers when
  the branch advances between assessment and confirmation.
- [Assign Entities architecture](../assign-entities/architecture.md) — how Assign Entities
  selections are sent to async.
- [architecture.md](architecture.md) — the datamanager blueprint structure.
