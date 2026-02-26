# Datamanager Blueprint — Architecture

This document describes the structure of the `datamanager` blueprint and how to work within it. Read this before adding new routes, services, or utilities.

---

## Directory layout

```
application/blueprints/datamanager/
├── architecture.md         # This file
├── router.py               # Blueprint definition, URL rules, auth guard
├── config.py               # External API URL builders
├── controllers/
│   ├── __init__.py         # ControllerError exception
│   ├── form.py             # Dashboard GET/POST and import handlers
│   ├── check.py            # Check results display and resubmit
│   └── add.py              # Entities preview and async confirm
├── services/
│   ├── async_api.py        # Async request API client
│   ├── dataset.py          # Dataset lookups and autocomplete
│   ├── organisation.py     # Organisation lookups and formatting
│   └── github.py           # GitHub App auth and workflow triggers
└── utils/
    ├── __init__.py         # Shared helpers: error handling, table building, CSV preview
    ├── configure.py        # Column mapping row builder
    └── csv_formats.py      # CSV format builders per dataset type
```

---

## Layers and responsibilities

### `router.py` — HTTP layer

The single entry point for the blueprint.

- Creates the `datamanager_bp` Flask `Blueprint` with `url_prefix="/datamanager"`
- Registers the `require_login` before-request guard
- Registers the `handle_error` blueprint-level error handler
- Registers the `inject_now` context processor
- Defines thin view functions that delegate immediately to a controller
- Registers all URL rules with `blueprint.add_url_rule()`

**Rule:** router view functions should contain minimal logic. If you find yourself writing more than a handful of lines inside a view function, move the logic into a controller.

---

### `controllers/` — Orchestration layer

Controllers receive a request context and orchestrate the workflow: validate inputs, call services, build template context, return a rendered response or redirect.

| File | Handles |
|---|---|
| `form.py` | Dashboard GET, dashboard POST (form submit), CSV import GET/POST, add-data form |
| `check.py` | Check results display (GET), resubmit with updated column mappings (POST) |
| `add.py` | Entities preview, async confirm (trigger GitHub workflow) |

#### `ControllerError`

Defined in `controllers/__init__.py`. Raise this for any expected, user-facing failure:

```python
from .controllers import ControllerError

raise ControllerError("Could not find the requested dataset.")
```

The router catches `ControllerError` and renders `datamanager/error.html` with `e.message`. Do not use it for unexpected exceptions — let those propagate to the blueprint-level error handler.

---

### `services/` — External integration layer

Each service owns one domain. Services are stateless functions (plus module-level caches where needed). They do not import from controllers or the router.

#### `async_api.py`

Client for the async request API.

| Function | Description |
|---|---|
| `submit_request(params)` | POST to `/requests`, returns `request_id` |
| `fetch_request(request_id)` | GET `/requests/<id>`, returns parsed dict |
| `fetch_response_details(request_id, limit)` | Paginated GET of response details, returns aggregated list |

Raises `AsyncAPIError(message, status_code, detail)` on failure.

#### `dataset.py`

Lookups against the planning data datasets endpoint. Results cached for **5 minutes**.

| Function | Description |
|---|---|
| `get_dataset_options()` | Sorted list of dataset names |
| `get_dataset_id(name)` | Dataset ID for a given name |
| `get_collection_id(name)` | Collection ID for a given name |
| `get_dataset_name(dataset_id)` | Human name for a dataset ID |
| `search_datasets(query, limit)` | Case-insensitive name search for autocomplete |

#### `organisation.py`

Organisation lookups from the provision CSV and datasette. Two separate caches:

- Provision orgs per dataset: **5 minutes** (`_provision_cache`)
- Full org code → name mapping: **10 minutes** (`_org_mapping_cache`)

| Function | Description |
|---|---|
| `get_provision_orgs_for_dataset(dataset_id)` | List of org codes provisioned for a dataset |
| `get_organisation_name(code)` | Display name for an org code (falls back to code) |
| `is_valid_organisation(code)` | Whether an org code exists |
| `format_org_options(org_codes)` | Format codes as `[{code, label}]` dicts for UI dropdowns |

#### `github.py`

GitHub App authentication and workflow dispatch.

- `trigger_add_data_async_workflow(...)` — the primary public function; handles JWT generation, installation token fetch, and workflow dispatch internally
- Custom exceptions: `GitHubAppError`, `GitHubAppAuthError`, `GitHubWorkflowError`

---

### `utils/` — Shared helpers

Pure helper functions with no dependency on Flask request context (except where noted). Safe to call from controllers or other utils.

#### `utils/__init__.py`

| Symbol | Description |
|---|---|
| `REQUESTS_TIMEOUT` | Default timeout (20 s) — import this in services instead of hardcoding |
| `handle_error(e)` | Blueprint error handler — renders `datamanager/error.html` with a 500 |
| `inject_now()` | Context processor injecting `now` (datetime) into templates |
| `get_spec_fields_union(dataset_id)` | Union of global + dataset-scoped field definitions from datasette |
| `order_table_fields(fields)` | Orders fields with `reference` first, `name` second |
| `read_raw_csv_preview(source_url, max_rows)` | Fetch and parse the first N rows of a remote CSV |
| `build_check_tables(column_field_log, resp_details)` | Build converted, transformed, and issue-log table dicts for templates |

#### `utils/configure.py`

- `build_column_mapping_rows(...)` — builds UI rows for the column-mapping form

#### `utils/csv_formats.py`

Format-specific CSV builders used in the add-data preview:

- `build_lookup_csv_preview()`
- `build_endpoint_csv_preview()`
- `build_source_csv_preview()`
- `build_column_csv_preview()`
- `build_entity_organisation_csv()`

---

> **Note:** `config.py` currently also re-exports `get_request_api_endpoint` from the top-level `config/config.py`. The intention is to eventually consolidate all URL config here.

---


Errors at any layer:
- **Service errors** (`AsyncAPIError`, `GitHubAppError`, etc.) — catch in the controller, either recover or raise `ControllerError`
- **`ControllerError`** — caught by the router view function, renders `datamanager/error.html`
- **Unexpected exceptions** — caught by `datamanager_bp.errorhandler(Exception)` → `handle_error`, renders `datamanager/error.html` with a 500


## Testing

Tests mirror the layered structure:

```
tests/
├── unit/blueprints/datamanager/
│   ├── services/       # Unit tests per service (mock HTTP calls)
│   ├── controllers/    # Controller unit tests (mock services)
│   └── utils/          # Utility function tests
├── integration/blueprints/datamanager/
└── acceptance/blueprints/datamanager/
    └── test_add_data_journey.py
```

- Service tests should mock `requests.get` / `requests.post`
- Controller tests should mock service functions
- Do not test the router directly — that is covered by integration/acceptance tests
