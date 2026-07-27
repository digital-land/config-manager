# Assign entities — user flow

The **Assign entities** flow (the "Assign entities" card on the landing page) is the batch
counterpart to [Add data](add-data.md). Instead of one endpoint, the operator uploads a CSV of
**flagged resources** — resources whose check failed because they need entity numbers assigned —
and works through them one at a time, assigning entity numbers and committing the result.

Routes are on the **`assign_entities`** blueprint (`/assign-entities`, login-required, blocked while
the Assign Entities [service lock](#service-lock) is held). Its view functions are in the
datamanager `router.py`; the flow-specific logic is in `controllers/flagged_resources.py`, but the
transform, preview, and commit steps are **shared with Add data**.

## The steps

```
start → import CSV → summary (pick a resource) → check-results → [shared] entities-preview → confirm → success
```

### 1. Start (`assign_entities.flagged_resources_start`)

`GET/POST /` renders `flagged-resources-start.html` — an intro page that leads into the CSV import.

### 2. Import the flagged-resources CSV (`assign_entities.flagged_resources_import`)

`GET/POST /import` renders `flagged-resources-import.html`, where the operator uploads or pastes a
CSV. The required columns (`REQUIRED_COLUMNS` in `flagged_resources.py`) are:

```
dataset, resource, organisation, reference, status, entities_created, error_code, message
```

The CSV is parsed and validated, stored in the session, and the operator is redirected to the
summary. (Oversized uploads/pastes return a friendly 413 via the blueprint's
`RequestEntityTooLarge` handler.)

### 3. Summary — pick a resource (`assign_entities.flagged_resources_summary`)

`GET /resources` renders `flagged-resources-summary.html`: the flagged resources grouped by
dataset/organisation and error type. The operator submits one resource
(`POST /resource`, `flagged_resource_submit`), which resolves its dataset/collection/organisation,
calls `submit_request` to create an **assign-entities** request, records the config-branch baseline
for the [stale-assessment guard](github-add.md#stale-assessment-guard), and redirects to that
resource's check-results page.

### 4. Check results (`assign_entities.flagged_resource_detail`)

`GET /check-results/<request_id>` calls the **shared** `handle_check_transform`
(`controllers/transform.py`) but renders `assign-entities-check-results.html` and passes
`transform_endpoint="assign_entities.flagged_resource_detail"`. Because the transform controller is
assign-aware, this view additionally surfaces the flagged error messages and — for the
`conservation-area` dataset — a duplicate-candidates (dedup) tab for selecting entity redirects.

`POST /check-results/<request_id>` (`flagged_resource_detail_post`, `router.py`) records the chosen
`entity_redirects` on the `RequestMeta` row and **redirects into the shared Add-data path** at
`datamanager.entities_preview`.

### 5. Preview → confirm → commit (shared with Add data)

From the entities preview onward the flow is identical to Add data steps 5–6: review on
`entities_preview.html`, then confirm via `datamanager.add_data_confirm_async`, which applies the
stale-assessment guard and dispatches the GitHub commit workflow. `source_flow="assign_entities"` is
threaded through so the "return" links point back to the assign-entities start page rather than the
dashboard. See [add-data.md](add-data.md#5-entities-preview-datamanagerentities_preview-controllerspreviewpy)
and [github-add.md](github-add.md).

## Relationship to Add data

The two flows differ only at the front:

| | Add data | Assign entities |
| --- | --- | --- |
| Input | one endpoint via a form | a CSV of flagged resources, worked one at a time |
| Blueprint | `datamanager` | `assign_entities` |
| Front-end controller | `form.py` | `flagged_resources.py` |
| Check/results template | `check-transform.html` | `assign-entities-check-results.html` |

From `entities_preview` onward they share `preview.py`, `transform.py`, the async/GitHub services,
and the commit workflow.

## Service lock

The flow is disabled while a `ServiceLock` named `assign_entities` is held (toggled from the landing
page). The `assign_entities` before-request guard redirects to the landing page with an
`assign_entities_blocked_by` note while the lock is set.

## Related

- [add-data.md](add-data.md) — the sibling flow and the shared preview/confirm steps.
- [github-add.md](github-add.md) — the GitHub commit workflow and the stale-assessment guard.
- [architecture.md](architecture.md) — the datamanager blueprint structure.
