# Assign Entities - Architecture

This document describes the Assign Entities process end to end: how config-manager starts async
processing, how entity and redirect selections are represented, what the preview shows, which
async result fields become config rows, and where to look when the flow behaves unexpectedly.

Assign Entities is not a separate async request type. It is a specialised config-manager journey
that submits an async `add_data` request for an existing resource hash, then reuses the add-data
preview and GitHub commit flow once async has produced the final config rows.

---

## Repositories involved

| Repo | Responsibility |
| --- | --- |
| `config-manager` | User journey, selection UI, validation, async request creation, preview, GitHub dispatch |
| `async-request-backend` | Processes the resource, filters selected entities, assigns entity numbers, generates old-entity rows |
| `digital-land/config` | Fetches the completed async request and appends returned CSV rows |

config-manager does not calculate entity number ranges and does not create `old-entity.csv` rows
itself. It passes selected references to async, then displays and commits whatever async returns.

---

## Code map

| Area | File | Role |
| --- | --- | --- |
| Routes | `application/blueprints/datamanager/router.py` | Assign Entities routes, selection POST handling, replacement async request |
| Start/import | `application/blueprints/datamanager/controllers/flagged_resources.py` | Direct resource/dataset entry, flagged-resources CSV import, async request submission |
| Results page | `application/blueprints/datamanager/controllers/transform.py` | Builds Entities and Dedup tab data from async response |
| Shared results template | `application/templates/components/check-transform-base.html` | Entities tab table, entity checkboxes, selection count, shared button state |
| Dedup template | `application/templates/datamanager/assign-entities-check-results.html` | Dedup tab, duplicate redirect checkboxes, hidden changed flag |
| Preview | `application/blueprints/datamanager/controllers/preview.py` | Displays async-generated lookup, entity-organisation, and old-entity rows |
| GitHub dispatch | `application/blueprints/datamanager/services/github.py` | Triggers the config repo workflow with the completed async request id |
| Redirect parsing | `application/blueprints/datamanager/services/duplicates.py` | Validates submitted Dedup checkbox values against async candidates |

---

## Full process

### 1. Start Assign Entities

The user starts at `/assign-entities/` by entering a dataset/resource pair, or by uploading/pasting
a flagged-resources CSV.

`controllers/flagged_resources.py` resolves:

- `dataset`
- `collection`
- `resource`
- resource `organisation`, from resource metadata when possible
- shared `github_branch` from `CONFIG_REPO_BRANCH`

It then submits an async request using `_submit_assign_entities_request`.

```json
{
  "type": "add_data",
  "resource": "resource-hash",
  "dataset": "conservation-area",
  "collection": "conservation-area",
  "authoritative": true,
  "github_branch": "config-manager-update",
  "organisationName": "local-authority:ABC",
  "organisation": "local-authority:ABC",
  "return_endpoint": "assign_entities.flagged_resources_start"
}
```

At this point no explicit selection is sent, so async treats the request as "assign all new
entities".

### 2. Show async result

`router.py` fetches the request from async and delegates to `handle_check_transform` in
`controllers/transform.py`.

The page is rendered from:

- `response-details` rows fetched by `fetch_response_details(request_id)`
- platform entities from the planning data API, for comparison and row categories
- `response-details.transformed_row`, for Assign Entities rows
- request param `excluded_references`, for Assign Entities checked state
- `pipeline-summary.duplicate-candidates`, for the Dedup tab
- `pipeline-summary.old-entity`, for preselected duplicate redirects

### 3. Select entities

The Entities tab includes a checkbox column only in the Assign Entities flow.

| Row category | Meaning | Checkbox |
| --- | --- | --- |
| `new` | Reference is not already on the platform | enabled |
| `changed` | Entity exists on the platform but differs from the resource | disabled |
| `in_both` | Entity exists and matches | disabled |
| `existing` | Entity exists on the platform only | disabled |

Each enabled checkbox submits the row reference as its value:

```text
REF-1
```

The reference is the selection identity. Entity numbers are not used as the selection key because
the point of the flow is to decide which references should receive numbers.

Initial checkbox state comes from async request params:

- missing or empty `excluded_references`: all selectable `new` rows are checked
- non-empty `excluded_references`: matching references are unchecked

Search and pagination render only a subset of rows. To avoid losing selections from other pages,
the template posts the visible row values separately and `router.py` merges the visible changes back
into the current full selection before it submits a replacement async request.

### 4. Select duplicate redirects

The Dedup tab is currently shown for Assign Entities `conservation-area` requests.

Candidate rows come from `pipeline-summary.duplicate-candidates`. Initial checked state comes from
`pipeline-summary.old-entity`, not from a score threshold in config-manager.

When an entity is not selected, any duplicate checkbox for that entity is disabled. This matters
because async can only redirect old entity numbers to new entity numbers it is assigning in the
same processed selection.

Rows in `pipeline-summary.old-entity` that are not present in request param `selected_redirects`
are treated as async auto-selected. Those checkboxes stay checked and locked in the UI so the user
cannot remove redirects generated by async policy.

### 5. Process entities

Changing either the Entities tab or the Dedup tab changes the submit button text to
**Process entities**.

On POST, `flagged_resource_detail_post`:

1. Fetches the current async response.
2. Parses selected entity checkbox reference values.
3. Calculates excluded references from the visible rows and current request params.
4. Parses selected Dedup checkbox values against `duplicate-candidates`.
5. Merges visible entity selections with the current excluded references.
6. Filters Dedup redirects to exclude references that will not receive entity numbers.
7. Submits a replacement async request with `excluded_references` and `selected_redirects`.
8. Redirects to the new request's Assign Entities check-results page.

The replacement request looks like:

```json
{
  "type": "add_data",
  "resource": "resource-hash",
  "dataset": "conservation-area",
  "collection": "conservation-area",
  "authoritative": true,
  "github_branch": "config-manager-update",
  "organisationName": "local-authority:ABC",
  "organisation": "local-authority:ABC",
  "return_endpoint": "assign_entities.flagged_resources_start",
  "excluded_references": [
    "REF-2"
  ],
  "selected_redirects": [
    {
      "reference": "REF-1",
      "old_entity_number": "100"
    }
  ]
}
```

`excluded_references` contains only references that should not receive entity numbers.
An empty list means nothing was excluded from assignment.

### 6. Preview

Once the user continues from the refreshed Assign Entities result, the add-data preview shows the
completed async output. It does not recalculate Assign Entities rows.

| Preview section | Source |
| --- | --- |
| Rows that will create new entities | `pipeline-summary.new-entities` |
| Rows that will NOT create new entities | `params.excluded_references` |
| Entity organisation CSV | `pipeline-summary.entity-organisation` |
| Old Entity Summary / `old-entity.csv` | `pipeline-summary.old-entity` |

The "Number of redirects" value is the number of rendered `old-entity` rows.

### 7. Confirm and commit

On confirmation, config-manager calls `trigger_add_data_async_workflow` with:

- `request_id`
- `triggered_by`
- `github_branch`
- `retire_endpoints`
- `environment`

The config repo workflow uses the `request_id` to fetch the completed async result and appends:

- `pipeline/{collection}/lookup.csv` from `pipeline-summary.new-entities`
- `pipeline/{collection}/entity-organisation.csv` from `pipeline-summary.entity-organisation`
- `pipeline/{collection}/old-entity.csv` from `pipeline-summary.old-entity`

---

## Entity number behaviour

Entity number generation happens in async, before the result is returned to config-manager.

Selection is applied before async adds lookup entries, so excluded references do not consume
numbers. That means selection should not create gaps in the assigned range. If gaps appear in a
preview, debug async's generated `pipeline-summary.new-entities` and the branch it assessed
against, not config-manager's preview code.

`entity-organisation` is also async-owned. config-manager only renders the returned
`pipeline-summary.entity-organisation` rows and the config repo appends those rows when
authoritative and valid.

---

## Gotchas

### Empty excluded references means assign all

Async treats missing or empty `excluded_references` as "exclude nothing". The UI can send
an empty list when every selectable reference is selected.

### Transformed rows vs `new-entities`

The Entities tab renders from transformed rows and platform comparison data. It expects async to
return transformed rows for all resource entities, selected or not. The preview uses
`pipeline-summary.new-entities`, which should contain only the rows async will actually assign.

### Existing rows are visible but not selectable

Existing, changed, and in-both rows keep their row category and colour semantics. Their checkboxes
are disabled because config-manager must only submit references eligible for new entity numbers.

### Dedup selection depends on entity selection

A Dedup checkbox can be disabled even when it is a good duplicate candidate. If the corresponding
new entity is not selected, the redirect cannot be submitted because async will not assign that new
entity number.

### Auto-selected redirects are inferred

config-manager treats old-entity rows returned by async but absent from request param
`selected_redirects` as auto-selected. Those rows are locked in the UI. This is an inference from
the async result and params, not a separate stored flag in config-manager.

### Button text is stateful

The shared results page changes "Continue to preview" to "Process entities" when either entity
selection or Dedup selection changes. If this does not happen, inspect the hidden
`entity_selection_changed` input and the checkbox data attributes in the rendered HTML.

### Preview is not the selection editor

The preview only shows async output. If a row is missing from preview, go back to the Assign
Entities check-results page and process a new selection. The preview should not be used to add or
remove selected references.

### Branch state can make results stale

Assign Entities uses the same stale-assessment protection as add-data. If the shared config branch
moves for the collection between assessment and confirmation, the confirm page can block and ask
the user to re-run.

---

## Debugging

### The page shows all entities selected after processing

Check the replacement async request params:

- Does `excluded_references` contain the references that were unchecked?
- Is it a list of reference strings?
- Do the reference values match the transformed row references?

If `excluded_references` is missing or empty, async will process all new entities.

### A selected row disappeared from the Entities tab

Check whether async returned the row in `response-details.transformed_row`. The Assign Entities tab
expects transformed rows for all resource entities, including excluded candidates. The preview is
allowed to contain only assigned rows in `pipeline-summary.new-entities`.

### A selected row does not appear in preview

Inspect the completed async response:

- `pipeline-summary.new-entities`
- `pipeline-summary.entity-organisation`
- `pipeline-summary.old-entity`
- `response.error`

The preview uses those fields directly. If the row is not there, the issue is earlier than preview.

### Dedup checkbox is disabled

Check:

- The candidate's `new_reference` or `reference`
- The request param `excluded_references`
- The rendered row's `redirect_can_select`
- Whether the row is locked because it is in `pipeline-summary.old-entity` but absent from
  `selected_redirects`

### Dedup selection was not sent to async

Check the POST body from the Assign Entities page:

- `entity_selection_changed` should be `true`
- selected duplicate checkboxes should post as `entity_redirects`
- parsed redirects must match a row in `pipeline-summary.duplicate-candidates`
- redirects for excluded references are deliberately filtered out

### Preview old-entity count is wrong

The count is the number of rows rendered from `pipeline-summary.old-entity`. Check the async result
first.

### GitHub PR does not contain old-entity rows

Check `pipeline-summary.old-entity` in the completed async result fetched by the config repo
workflow.

### Entity numbers look stale or collide

Check:

- `github_branch` sent in the async request
- the `RequestMeta.branch_sha` baseline captured for the request
- whether the confirm-time stale check blocked or was skipped
- whether another add-data workflow changed `pipeline/{collection}/` while the page was open

---

## Useful focused tests

```bash
./.venv/bin/pytest tests/unit/blueprints/datamanager/controllers/test_transform.py -q
./.venv/bin/pytest tests/unit/blueprints/datamanager/controllers/test_add.py -q
./.venv/bin/pytest tests/acceptance/blueprints/datamanager/test_flagged_resources.py -q
```

Use the focused tests when changing the selection or preview flow. Run the broader datamanager
suite before merging changes that touch shared templates or GitHub dispatch behaviour.

---

## Related

- [Datamanager GitHub workflow](../datamanager/github-add.md)
- [Datamanager stale-assessment check](../datamanager/stale-check.md)
- [Datamanager blueprint architecture](../datamanager/architecture.md)
