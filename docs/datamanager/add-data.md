# Add data — user flow

The **Add data** flow (the "Add data" card on the landing page) lets an operator add a new or
updated data endpoint for a dataset + organisation (a provision). The tool runs the endpoint through the async
check/transform pipeline, previews the entities that would be created (the rows that are appended into the lookup.csv), and — on confirmation —
commits the resulting config to the `digital-land/config` repo as a PR request.

All routes are on the `datamanager` blueprint (`/datamanager`, login-required, blocked while the
Add Data [service lock](#service-lock) is held). View functions live in `router.py` and delegate to
the controllers named below.

## The steps

```
dashboard -> initial form → check-results → add-data form (optional) → check-transform → entities-preview → confirm → success
```

### 1. Dashboard (`datamanager.dashboard_get` / `dashboard_add`, `controllers/form.py`)

`GET /` renders `dashboard_add.html`: the operator picks a **dataset** and **organisation** and
enters the **endpoint URL** (or uses `GET/POST /import`, `handle_dashboard_add_import`, to paste a
CSV). `POST /` validates the form and calls `submit_request` (`services/async_api.py`) to create a
**check** request on the async API, then redirects to the check-results page for that `request_id`.

A dashboard can also be pre-filled from an existing request (`fetch_request`), which supports magic
links that jump straight into the flow.

### 2. Check results (`datamanager.check_results`, `controllers/check.py`)

`GET /check-results/<request_id>` renders `check-results-loading.html` while the async check is
pending (the page polls), then `check-results.html` once complete: converted rows and any issues,
with an inline **column-mapping** UI. From here the operator can:

- **Re-run the check** with corrected column mappings — `POST /check-results/<id>`
  (`handle_check_resubmit`) submits a new check request and redirects back to check-results.
- **Proceed to Add data** — links to the add-data form.

### 3. Add-data form (`datamanager.add_data`, `controllers/form.py`)

`GET/POST /add-data/<request_id>` renders `add-data.html` to collect the remaining fields —
`documentation_url`, `licence`, `start_date`, `authoritative`, and whether the endpoint is new.
These are held in `session["add_data_fields"]`; if they are already all present the form is skipped
and submission happens directly. On submit, `_submit_add_data_preview` calls `submit_request` to
create the **preview** request, records the config-branch baseline for the
[stale-assessment guard](github-add.md#stale-assessment-guard) (`record_branch_baseline`), and
redirects to check-transform.

### 4. Check transform (`datamanager.check_transform`, `controllers/transform.py`)

`GET /check-transform/<request_id>` renders `check-transform-loading.html` then
`check-transform.html`: the transformed facts, issue logs, and an entity-growth view, plus the
option to select **endpoints to retire**. `POST /check-transform/<id>` (`check_transform_post`,
`router.py`) stores the chosen `retire_endpoints` on the `RequestMeta` row and redirects to the
entities preview.

> `transform.py` is shared with the [Assign entities](assign-entities.md) flow and branches on the
> calling endpoint; see that doc for the differences.

### 5. Entities preview (`datamanager.entities_preview`, `controllers/preview.py`)

`GET /add-data/<request_id>/entities` renders `entities_preview.html`: a final review of the new
entities, any old→new entity redirects, and the organisation summary. This is the confirmation
point.

### 6. Confirm → commit (`datamanager.add_data_confirm_async`, `controllers/preview.py`)

`POST /add-data/<request_id>/confirm-async` runs `handle_add_data_confirm`, which applies the
[stale-assessment guard](github-add.md#stale-assessment-guard) (waits for any in-flight commit
workflow, then fails closed if the config branch moved for this collection) and, if clear, calls
`trigger_add_data_async_workflow` to dispatch the GitHub Action that commits the config. Success
renders `add-data-success.html`; a stale result renders `add-data-stale.html` with a "Re-run
transform" action.

The commit workflow itself — payload, branch behaviour, and which CSVs it writes — is documented in
[github-add.md](github-add.md).

## Service lock

The Add Data flow is disabled while a `ServiceLock` named `add_data` is held (toggled from the
landing page). The `datamanager` before-request guard redirects to the landing page with a
`add_data_blocked_by` note while the lock is set.

## Related

- [assign-entities.md](assign-entities.md) — the sibling flow that shares the transform/preview/commit steps.
- [github-add.md](github-add.md) — the GitHub commit workflow and the stale-assessment guard.
- [architecture.md](architecture.md) — the datamanager blueprint structure.
