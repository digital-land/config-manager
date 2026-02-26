# Add Data via Async API Workflow

This workflow is an alternative to `add-data-rest-api.yml` that avoids GitHub's 10KB payload limit by fetching data from an external async API service instead of receiving CSV rows inline.

## How It Works

Instead of sending all CSV row data in the dispatch payload, you send only a `request_id`. The workflow then:

1. Fetches the full request data from the async API service
2. Validates the request status is `COMPLETE` with no errors
3. Transforms the JSON response into CSV rows for each file
4. Appends to the appropriate collection/pipeline CSV files
5. Creates a branch and PR

## Triggering the Workflow

**Endpoint:** `POST https://api.github.com/repos/digital-land/config/dispatches`

**Required headers:**
- `Accept: application/vnd.github+json`
- `Authorization: Bearer <INSTALLATION_ACCESS_TOKEN>`
- `X-GitHub-Api-Version: 2022-11-28`

**Payload:**
```json
{
  "event_type": "add-data-async",
  "client_payload": {
    "request_id": "RW37P9DNRYSTK2eEByDGeq",
    "triggered_by": "your-name-or-system"
  }
}
```

### Required Fields
- `event_type`: Must be `"add-data-async"`
- `client_payload.request_id`: The ID of a completed async API request

### Optional Fields
- `client_payload.triggered_by`: Identifier for who/what triggered the workflow

## Async API Service

The workflow fetches request data from:

```
{ASYNC_API_BASE_URL}/requests/{request_id}
```

**Default base URL:** `http://development-pub-async-api-lb-69142969.eu-west-2.elb.amazonaws.com`

To override, set the `ASYNC_API_BASE_URL` repository variable in GitHub Settings > Secrets and variables > Actions > Variables.

### Expected Response Format

The async API must return a JSON response with this structure:

```json
{
  "params": {
    "collection": "article-4-direction",
    "dataset": "article-4-direction-area",
    "column_mapping": { "geom": "geometry" },
    "authoritative": true
  },
  "status": "COMPLETE",
  "response": {
    "data": {
      "endpoint-summary": {
        "new_endpoint_entry": { ... },
        "endpoint_url_in_endpoint_csv": false
      },
      "source-summary": {
        "new_source_entry": { ... },
        "documentation_url_in_source_csv": false
      },
      "pipeline-summary": {
        "new-entities": [ ... ],
        "entity-organisation": [ ... ]
      }
    },
    "error": null
  }
}
```

## CSV Files Updated

The workflow transforms the JSON response into CSV rows for:

| File | Source in response | Condition |
|------|-------------------|-----------|
| `collection/{collection}/endpoint.csv` | `endpoint-summary.new_endpoint_entry` | `endpoint_url_in_endpoint_csv` is false |
| `collection/{collection}/source.csv` | `source-summary.new_source_entry` | `documentation_url_in_source_csv` is false |
| `pipeline/{collection}/lookup.csv` | `pipeline-summary.new-entities` | Array is non-empty |
| `pipeline/{collection}/column.csv` | `params.column_mapping` | Mapping is non-empty |
| `pipeline/{collection}/entity-organisation.csv` | `pipeline-summary.entity-organisation` | `params.authoritative` is true |

**Note:** `entity-organisation.csv` is only updated when `params.authoritative` is `true`.

## Comparison with REST API Workflow

| | `add-data-rest-api.yml` | `add-data-async.yml` |
|---|---|---|
| **Input** | Pre-formatted CSV rows in payload | Just a `request_id` |
| **Payload size** | Limited by GitHub's 10KB cap | Minimal (~100 bytes) |
| **Data source** | Caller provides CSV data | Fetched from async API |
| **Duplicate detection** | None (caller's responsibility) | Uses `endpoint_url_in_endpoint_csv` and `documentation_url_in_source_csv` flags |

## Authentication

Uses the same GitHub App authentication as the REST API workflow. See [ADD-DATA-README.md](ADD-DATA-README.md#authentication) for setup details.

## Error Handling

The workflow will fail if:

- `request_id` is empty
- The async API request cannot be fetched
- The request status is not `COMPLETE`
- The response contains an error
- The `collection` from the request params doesn't have matching directories in `collection/` and `pipeline/`
- There are no changes to commit

## Monitoring

After triggering, monitor the workflow in the Actions tab under "Add Data via Async API". The commit message and PR body will include the request ID for traceability.
