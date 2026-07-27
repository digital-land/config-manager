# Add-data GitHub workflow

When a user confirms an add-data (or assign-entities) submission, config-manager triggers a GitHub
Action in the `digital-land/config` repo that commits the assessed data onto a config branch and
opens/updates a PR.

The trigger lives in `services/github.py` (`trigger_add_data_async_workflow`, called from
`controllers/preview.py`). The workflow is `.github/workflows/add-data-async-script.yml` in
`digital-land/config`, running `bin/add_data.py`. It fetches the request from the async API,
validates `status` is `COMPLETE` with no error, appends rows to the relevant collection/pipeline
CSVs, then commits and creates/updates a PR against `main`.

## Triggering the workflow

**Endpoint:** `POST {GITHUB_API_BASE_URL}/repos/digital-land/config/dispatches` — sent as a GitHub
App (`GITHUB_APP_ID`, `GITHUB_APP_INSTALLATION_ID`, `GITHUB_APP_PRIVATE_KEY`), with
`X-GitHub-Api-Version: 2022-11-28`.

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

Only `event_type` (must be `add-data-async-script`) and `client_payload.request_id` (a `COMPLETE`
async request) are required. `branch` defaults per [Branch behaviour](#branch-behaviour);
`retire_endpoints` end-dates the matching endpoint/source rows; `environment` (`development` |
`staging` | `production`, default `staging`) selects the async API base URL the workflow reads.

The branch config-manager sends is its `CONFIG_REPO_BRANCH` setting — `config-manager-update` in
production, `test-config-manager-update` in development.

## Branch behaviour

`bin/add_data.py` (`resolve_branch`) is branch-agnostic; the `branch` parameter controls branch/PR
creation:

- **No `branch`** — creates `add-data-async/{collection}-{timestamp}` and opens a PR against `main`.
- **`branch` given** — checks out (or creates) it, appends on top, and updates the open PR if there
  is one or opens a new one. This batches multiple submissions into a single PR, whose body
  accumulates one label per submission: `add-{dataset}-{organisation}-{triggered_by}`.

Only a `config-manager-update → main` PR is auto-merged (`auto-merge-config-manager.yml`); other
branch names (e.g. `test-config-manager-update`) open a normal PR that is never auto-merged.

## CSV files updated

| File | Source | Condition |
| --- | --- | --- |
| `collection/{collection}/endpoint.csv` | `endpoint-summary.new_endpoint_entry` | `endpoint_url_in_endpoint_csv` is false |
| `collection/{collection}/source.csv` | `source-summary.new_source_entry` | `documentation_url_in_source_csv` is false |
| `pipeline/{collection}/lookup.csv` | `pipeline-summary.new-entities` | array non-empty |
| `pipeline/{collection}/column.csv` | `params.column_mapping` | mapping non-empty |
| `pipeline/{collection}/entity-organisation.csv` | `pipeline-summary.entity-organisation` | `params.authoritative` true, and not an overlap/error |
| `pipeline/{collection}/old-entity.csv` | `pipeline-summary.old-entity` | array non-empty |

The workflow fails if `request_id` is empty, the request cannot be fetched, its status is not
`COMPLETE`, the response contains an error, the `collection` has no matching `collection/` and
`pipeline/` directories, or there are no changes to commit.

## Stale-assessment guard

Adding data is two steps: **assess** (the async worker reads the shared branch, assigns free entity
numbers, and freezes them into the result) and **confirm** (the user reviews, then the workflow
above commits the frozen numbers). The review page can sit open a long time; if another submission
advances the shared branch in between, the frozen numbers can collide — the same entity number used
twice, surfacing later as a failed PR a developer has to unpick by hand. This guard blocks the
confirm when that has happened.

**Baseline at submission.** `record_branch_baseline` (`controllers/preview.py`, called from both
`_submit_add_data_preview` in `controllers/form.py` and `_submit_assign_entities_request` in
`controllers/flagged_resources.py`) records the branch HEAD SHA the assessment is based on onto
`RequestMeta.branch_sha`. It:

- skips new-branch submissions (no shared state to race);
- baselines against `main` when `config-manager-update` does not exist yet (it is created lazily by
  the first commit), via `get_config_baseline_sha` (`services/github.py`) — matching what the async
  worker reads when the branch is absent;
- reads only the branch HEAD (one API call, no waiting) so submit stays fast;
- **fails open** — any error is logged and skipped rather than blocking the submission.

**Check at confirmation.** Before triggering the commit workflow, `handle_add_data_confirm`
(`controllers/preview.py`) first **waits for any in-flight `add-data-async-script` workflow to
finish** (`wait_for_add_data_workflow_idle`, `services/github.py`) so it compares a settled branch,
bounded by `ADD_DATA_WORKFLOW_WAIT_TIMEOUT`; the confirm page shows a "Submitting…" panel during the
wait. It then compares the baseline via `config_branch_changed_for_collection`
(`GET /repos/digital-land/config/compare/{baseline_sha}...{head}`, where `head` is the shared branch
or `main`):

- returns "changed" only if a changed file lives under `pipeline/{collection}/`, so other
  collections do not block each other;
- **fails closed** — treats the branch as changed on a diverged/force-pushed history, a truncated
  (>=300-file) diff, or any API error, so a possibly-stale result is never let through.

If the branch changed, the confirm is blocked and the user sees
`templates/datamanager/add-data-stale.html` with a "Re-run transform" action, routing back to
`datamanager.check_results` for the recorded `check_request_id` (or the flow's start page). The
guard only runs when submitting onto the shared branch **and** a baseline was captured, so requests
predating this feature pass through unchanged.

### Configuration (`config/config.py`)

| Setting | Default | Purpose |
| --- | --- | --- |
| `CONFIG_REPO_BRANCH` | `config-manager-update` (prod), `test-config-manager-update` (dev) | Shared branch to commit to / check against |
| `ADD_DATA_WORKFLOW_WAIT_TIMEOUT` | `60` (seconds) | Max wait at confirm for an in-flight workflow before comparing |
| `ADD_DATA_WORKFLOW_POLL_INTERVAL` | `5` (seconds) | Poll interval while waiting |

The gunicorn `--timeout` in the `Procfile` (120s) is kept comfortably above the wait timeout so a
web worker is not killed while the confirm request waits.

> **Note:** there is no CI check for duplicate entity numbers before the auto-merged PR lands, and
> the commit workflow has no concurrency group — both are worth adding as defence-in-depth but are
> out of scope for this guard.

## Related

- [add-data.md](add-data.md) — the Add data user flow that leads to this commit workflow.
- [Assign Entities architecture](assign-entities.md) — how Assign Entities
  selections are sent to async.
- [architecture.md](architecture.md) — the datamanager blueprint structure.
