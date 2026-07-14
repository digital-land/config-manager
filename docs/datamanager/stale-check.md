# Add-data stale-assessment check

## What problem this solves

Adding data is a two-step, human-in-the-loop flow:

1. **Assess** — the user submits a transform. The async worker reads the current state of the
   shared config branch (`config-manager-update`), works out which entity numbers are free, and
   assigns new ones. These numbers are frozen into the stored result.
2. **Confirm** — the user reviews the result and confirms. A GitHub Action then commits the frozen
   numbers onto the shared branch.

The review page can sit open for a long time. If another submission advances the shared branch in
the meantime, the frozen entity numbers can collide with numbers that were assigned in between —
the same entity number ends up used twice, which only surfaces later as a merge conflict / failed
PR that a developer has to unpick by hand.

The stale-check detects that the branch moved for this collection between assessment and
confirmation, and blocks the confirm with a prompt to re-run.

## How it works

### 1. Capture a baseline at submission

When a transform is submitted, `record_branch_baseline` (`controllers/preview.py`) records the
config branch HEAD SHA the assessment is based on, on the `RequestMeta` row (`branch_sha` column).
It is called from both submission sites:

- `_submit_add_data_preview` (`controllers/form.py`) — add-data flow
- `_submit_assign_entities_request` (`controllers/flagged_resources.py`) — assign-entities flow

Behaviour:

- **New-branch submissions** (no shared branch) are skipped — there is no shared state to race.
- **Lazy branch / main fallback.** `config-manager-update` is created lazily by the first
  add-data commit, so early in a cycle it does not exist and reading its HEAD 404s. In that case
  `get_config_baseline_sha` (`services/github.py`) baselines against `main` instead — which is
  what the async worker reads when the branch is absent. Without this the baseline would be empty
  and the check would silently do nothing.
- **Wait for in-flight commits.** If an `add-data-async-script` workflow is mid-push when we read
  HEAD, the baseline would capture a commit the worker has not seen yet. So we first wait
  (bounded) for any active run to finish — `wait_for_add_data_workflow_idle` /
  `add_data_workflow_running` (`services/github.py`). This is a no-op in the common case and
  capped so it never hangs the request.
- **Fails open.** Any error reading the SHA is logged and skipped rather than blocking the
  submission.

### 2. Check at confirmation

Before triggering the commit workflow, `handle_add_data_confirm` (`controllers/preview.py`)
compares the baseline against the current branch state via
`config_branch_changed_for_collection` (`services/github.py`):

- Uses the GitHub compare API, `GET /repos/digital-land/config/compare/{baseline_sha}...{head}`.
- `head` is the shared branch if it exists, otherwise `main` (mirroring the baseline fallback).
- Returns "changed" only if a changed file lives under `pipeline/{collection}/`, so submissions
  to other collections do not block each other.
- **Fails closed** — treats the branch as changed on a diverged/force-pushed history, a truncated
  (>=300-file) diff, or any API error, so a possibly-stale result is never let through.

If the branch changed for the collection, the confirm is blocked and the user is shown
`templates/datamanager/add-data-stale.html` with a "Re-run transform" action. Otherwise the commit
workflow is triggered as normal.

The check only runs when submitting onto the shared branch **and** a baseline was captured — so
requests created before this feature (no `branch_sha`) pass through unchanged.

## Configuration

In `config/config.py`:

| Setting | Default | Purpose |
| --- | --- | --- |
| `CONFIG_REPO_BRANCH` | `config-manager-update` (prod), `test-config-manager-update` (dev) | Shared branch to commit to / check against |
| `ADD_DATA_WORKFLOW_WAIT_TIMEOUT` | `60` (seconds) | Max wait for an in-flight workflow before capturing HEAD |
| `ADD_DATA_WORKFLOW_POLL_INTERVAL` | `5` (seconds) | Poll interval while waiting |

The gunicorn `--timeout` in the `Procfile` (120s) is kept comfortably above the wait timeout so a
web worker is not killed mid-wait.

## Design intent

- Correctness lives in the confirm-time check, which **fails closed**.
- The submission-side pieces (baseline capture, workflow-wait, main fallback) only exist to keep
  the baseline accurate and cut false-positive re-run prompts; they **fail open** so they can
  never block a submission over an infrastructure hiccup.

## Related

The stored numbers are committed by the `add-data-async-script` workflow in the `digital-land/config`
repo (`bin/add_data.py`), which is branch-agnostic and operates on whatever branch is passed to it.
There is no CI check for duplicate entity numbers before that PR auto-merges, and the commit
workflow has no concurrency group — both are worth adding as defence-in-depth but are out of scope
for this check.
