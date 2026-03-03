# Add Data via Async API Workflow

This workflow adds data to collection and pipeline CSV files by fetching a completed request from the async API service and transforming the response into the appropriate CSV rows.

## How It Works

1. Fetches the full request data from the async API service using the provided `request_id`
2. Validates the request status is `COMPLETE` with no errors
3. Resolves the target branch (see [Branch Behaviour](#branch-behaviour))
4. Transforms the JSON response into CSV rows for each file
5. Appends to the appropriate collection/pipeline CSV files
6. Commits and creates or updates a PR

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
    "triggered_by": "your-name-or-system",
    "branch": "optional-branch-name"
  }
}
```

### Required Fields
- `event_type`: Must be `"add-data-async"`
- `client_payload.request_id`: The ID of a completed async API request

### Optional Fields
- `client_payload.triggered_by`: Identifier for who/what triggered the workflow (used in commit messages and PR content)
- `client_payload.branch`: Target branch name — see [Branch Behaviour](#branch-behaviour)

## Branch Behaviour

The `branch` parameter controls how the workflow creates or updates branches and PRs.

### No `branch` provided
A new branch is created with an auto-generated name:
```
add-data-async/{collection}-{timestamp}
```
A new PR is opened against `main`.

### `branch` provided — open PR exists
The workflow checks out the existing branch, appends the new data on top of it, and updates the existing PR body to include the new submission label. This allows multiple data submissions to be batched into a single PR.

### `branch` provided — no open PR, branch exists in git
The workflow checks out the existing branch and appends the new data on top of it, then opens a new PR against `main`.

### `branch` provided — no open PR, branch does not exist
The workflow creates a fresh branch with the given name and opens a new PR against `main`.

## Commit Messages and PR Content

Every submission produces a commit and PR label in the format:

```
add-{dataset}-{organisation}-{triggered_by}
```

For example:
```
add-article-4-direction-area-local-authority:SKP-matt
```

When multiple submissions are batched onto the same branch, the PR body accumulates all labels:
```
add-article-4-direction-area-local-authority:SKP-matt
add-article-4-direction-area-local-authority:EXE-matt
add-conservation-area-local-authority:SKP-matt
```

## Async API Service

The workflow fetches request data from:

```
{ASYNC_API_BASE_URL}/requests/{request_id}
```

**Default base URL:** `http://development-pub-async-api-lb-69142969.eu-west-2.elb.amazonaws.com`

To override, set the `ASYNC_API_BASE_URL` repository variable in GitHub Settings > Secrets and variables > Actions > Variables.

### Expected Response Format

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

| File | Source in response | Condition |
|------|-------------------|-----------|
| `collection/{collection}/endpoint.csv` | `endpoint-summary.new_endpoint_entry` | `endpoint_url_in_endpoint_csv` is false |
| `collection/{collection}/source.csv` | `source-summary.new_source_entry` | `documentation_url_in_source_csv` is false |
| `pipeline/{collection}/lookup.csv` | `pipeline-summary.new-entities` | Array is non-empty |
| `pipeline/{collection}/column.csv` | `params.column_mapping` | Mapping is non-empty |
| `pipeline/{collection}/entity-organisation.csv` | `pipeline-summary.entity-organisation` | `params.authoritative` is true |

**Note:** `entity-organisation.csv` is only updated when `params.authoritative` is `true`.

## Authentication

Uses GitHub App authentication. The following secrets must be set in the repository:
- `APP_ID` — the GitHub App's numeric ID
- `APP_PRIVATE_KEY` — the full PEM private key

See [ADD-DATA-README.md](ADD-DATA-README.md#authentication) for full setup details.

## Error Handling

The workflow will fail if:

- `request_id` is empty
- The async API request cannot be fetched
- The request status is not `COMPLETE`
- The response contains an error
- The `collection` from the request params doesn't have matching directories in `collection/` and `pipeline/`
- There are no changes to commit

## Monitoring

After triggering, monitor the workflow in the Actions tab under "Add Data via Async API". The commit message and PR body will include the dataset, organisation, and triggered-by identifier for traceability.
