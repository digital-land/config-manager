# Add Data via REST API Workflow

This workflow allows you to append data to collection and pipeline CSV files by triggering it via GitHub's REST API.

## Triggering the Workflow

The workflow is triggered via GitHub's repository dispatch API using a GitHub App for authentication.

**Endpoint:** `POST https://api.github.com/repos/digital-land/config/dispatches`

**Required headers:**
- `Accept: application/vnd.github+json`
- `Authorization: Bearer <INSTALLATION_ACCESS_TOKEN>`
- `X-GitHub-Api-Version: 2022-11-28`

**Payload example:**
```json
{
  "event_type": "add-data-via-api",
  "client_payload": {
    "collection": "article-4-direction",
    "triggered_by": "your-name-or-system",
    "lookup_csv_rows": [
      "article-4-direction-area,,,,local-authority:SKP,A4a-14-test,7010009246,,,",
      "article-4-direction-area,,,,local-authority:SKP,A4a-18-test,7010009247,,,"
    ],
    "endpoint_csv_rows": [
      "fc506934d0ccc949f0f1ecd12ff8992587598e40950b067f2e1fc874b260a17a,https://raw.githubusercontent.com/digital-land/..."
    ],
    "column_csv_rows": [
      "article-4-direction-area,fc506934d0ccc949f0f1ecd12ff8992587598e40950b067f2e1fc874b260a17a,,geom,geometry,,,"
    ]
  }
}
```


## Payload Structure

### Required Fields
- `event_type`: Must be `"add-data-via-api"` (this matches the workflow trigger)
- `client_payload.collection`: The collection name (e.g., `"article-4-direction"`)

### Optional Fields

All CSV row fields are optional. Only provide the ones you want to update:

- `client_payload.lookup_csv_rows`: Array of CSV rows to append to `pipeline/{collection}/lookup.csv`
- `client_payload.endpoint_csv_rows`: Array of CSV rows to append to `collection/{collection}/endpoint.csv`
- `client_payload.column_csv_rows`: Array of CSV rows to append to `pipeline/{collection}/column.csv`
- `client_payload.source_csv_rows`: Array of CSV rows to append to `collection/{collection}/source.csv`
- `client_payload.triggered_by`: Optional identifier for who/what triggered the workflow

## CSV Row Formats

Each CSV row should be a complete comma-separated string matching the expected format for that file:

### lookup.csv
Format: `prefix,resource,endpoint,entry-number,organisation,reference,entity,entry-date,start-date,end-date`

Example:
```
article-4-direction-area,,,,local-authority:SKP,A4a-14-test,7010009246,,,
```

### endpoint.csv
Format: `endpoint,endpoint-url,parameters,plugin,entry-date,start-date,end-date`

Example:
```
fc506934d0ccc949f0f1ecd12ff8992587598e40950b067f2e1fc874b260a17a,https://example.com/data.json,,,2025-01-29T00:00:00Z,,
```

### column.csv
Format: `dataset,endpoint,resource,column,field,start-date,end-date,entry-date`

Example:
```
article-4-direction-area,fc506934d0ccc949f0f1ecd12ff8992587598e40950b067f2e1fc874b260a17a,,geom,geometry,,,
```

### source.csv
Format: `source,attribution,collection,documentation-url,endpoint,licence,organisation,pipelines,entry-date,start-date,end-date`

Example:
```
fc506934d0ccc949f0f1ecd12ff8992587598e40950b067f2e1fc874b260a17a,,article-4-direction,,,OGL3,local-authority:SKP,article-4-direction-area,2025-01-29T00:00:00Z,,
```

## Authentication

This workflow uses GitHub App authentication for organization repositories.

### GitHub App Setup

**You'll need:**
- **App ID:** Found in your GitHub App settings page at `https://github.com/organizations/{org}/settings/apps/{app-name}`
- **Installation ID:** Found in the URL when viewing the installation: `https://github.com/organizations/{org}/settings/installations/{installation-id}`
  - Alternatively, use the GitHub API: `GET /orgs/{org}/installations`
- **Private key:** PEM file downloaded when creating the app (only available once at creation time)

**Required permissions for the GitHub App:**
- Repository permissions:
  - **Contents:** Read & Write (to create branches and PRs)
  - **Pull requests:** Read & Write (to create PRs)
  - **Workflows:** Read & Write (to trigger workflows)

**Authentication flow:**
1. Generate a JWT (JSON Web Token) using your App's private key
2. Exchange the JWT for an installation access token
3. Use the installation access token to trigger the workflow

See the examples below for implementation details.

### Finding Your App ID and Installation ID

**App ID:**
1. Go to your organization settings: `https://github.com/organizations/{your-org}/settings/apps`
2. Click on your app
3. The App ID is shown in the "About" section

**Installation ID:**
1. Go to your organization settings: `https://github.com/organizations/{your-org}/settings/installations`
2. Click "Configure" on your app
3. The installation ID is in the URL: `.../installations/{installation-id}`

Or use the API:
```bash
# Using a JWT token for your app
curl -H "Authorization: Bearer YOUR_JWT" \
  https://api.github.com/orgs/{your-org}/installations
```

## What Happens

When the workflow is triggered:

1. **Validates inputs**: Checks that the collection exists in both `collection/` and `pipeline/` directories
2. **Appends data**: Adds the provided CSV rows to the respective files
3. **Creates a branch**: Creates a new branch named `add-data-api/{collection}-{timestamp}`
4. **Commits changes**: Commits the updated CSV files with a detailed message
5. **Creates a PR**: Opens a pull request with:
   - Summary of what was added
   - The actual CSV rows added to each file
   - Link to the workflow run
   - Triggered by information

## GitHub App Authentication

### Example with bash script

Use the provided `trigger-with-github-app.sh` script:

**Basic example (single rows):**
```bash
./trigger-with-github-app.sh \
  --app-id 123456 \
  --installation-id 789012 \
  --private-key-path ~/github-app-private-key.pem \
  --collection article-4-direction \
  --triggered-by "my-system" \
  --lookup "article-4-direction-area,,,,local-authority:SKP,A4a-14-test,7010009246,,," \
  --endpoint "abc123,https://example.com/data.json,,,2025-01-29T00:00:00Z,,"
```

**Multiple rows of same type:**
```bash
./trigger-with-github-app.sh \
  --app-id 123456 \
  --installation-id 789012 \
  --private-key-path ~/github-app-private-key.pem \
  --collection article-4-direction \
  --lookup "article-4-direction-area,,,,local-authority:SKP,A4a-14-test,7010009246,,," \
  --lookup "article-4-direction-area,,,,local-authority:SKP,A4a-18-test,7010009247,,," \
  --lookup "article-4-direction-area,,,,local-authority:SKP,A4a-19-test,7010009248,,,"
```

**All CSV types together:**
```bash
./trigger-with-github-app.sh \
  --app-id 123456 \
  --installation-id 789012 \
  --private-key-path ~/github-app-private-key.pem \
  --collection article-4-direction \
  --triggered-by "automated-ingestion" \
  --endpoint "fc506934d0ccc949f0f1ecd12ff8992587598e40950b067f2e1fc874b260a17a,https://example.com/data.json,,,2025-01-29T00:00:00Z,," \
  --source "fc506934d0ccc949f0f1ecd12ff8992587598e40950b067f2e1fc874b260a17a,,article-4-direction,,,OGL3,local-authority:SKP,article-4-direction-area,2025-01-29T00:00:00Z,," \
  --column "article-4-direction-area,fc506934d0ccc949f0f1ecd12ff8992587598e40950b067f2e1fc874b260a17a,,geom,geometry,,," \
  --lookup "article-4-direction-area,,,,local-authority:SKP,A4a-14-test,7010009246,,,"
```

**Using environment variables:**
```bash
export GITHUB_APP_ID=123456
export GITHUB_INSTALLATION_ID=789012
export GITHUB_APP_PRIVATE_KEY_PATH=~/github-app-private-key.pem

./trigger-with-github-app.sh \
  --app-id "$GITHUB_APP_ID" \
  --installation-id "$GITHUB_INSTALLATION_ID" \
  --private-key-path "$GITHUB_APP_PRIVATE_KEY_PATH" \
  --collection article-4-direction \
  --lookup "article-4-direction-area,,,,local-authority:SKP,A4a-14-test,7010009246,,,"
```

### Example with Python

```python
import time
import jwt
import requests
from datetime import datetime, timedelta

# GitHub App credentials
APP_ID = "your_app_id"
INSTALLATION_ID = "your_installation_id"
PRIVATE_KEY_PATH = "/path/to/private-key.pem"

# Repository info
REPO_OWNER = "digital-land"
REPO_NAME = "config"

def generate_jwt(app_id, private_key_path):
    """Generate a JWT for GitHub App authentication"""
    with open(private_key_path, 'r') as key_file:
        private_key = key_file.read()

    payload = {
        'iat': int(time.time()),
        'exp': int(time.time()) + 600,  # 10 minutes
        'iss': app_id
    }

    return jwt.encode(payload, private_key, algorithm='RS256')

def get_installation_token(jwt_token, installation_id):
    """Exchange JWT for an installation access token"""
    url = f"https://api.github.com/app/installations/{installation_id}/access_tokens"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {jwt_token}",
        "X-GitHub-Api-Version": "2022-11-28"
    }

    response = requests.post(url, headers=headers)
    response.raise_for_status()
    return response.json()['token']

def trigger_workflow(access_token, repo_owner, repo_name, payload):
    """Trigger the repository dispatch workflow"""
    url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/dispatches"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {access_token}",
        "X-GitHub-Api-Version": "2022-11-28"
    }

    response = requests.post(url, headers=headers, json=payload)
    return response

# Main execution
if __name__ == "__main__":
    # Step 1: Generate JWT
    jwt_token = generate_jwt(APP_ID, PRIVATE_KEY_PATH)

    # Step 2: Get installation access token
    access_token = get_installation_token(jwt_token, INSTALLATION_ID)

    # Step 3: Prepare payload
    payload = {
        "event_type": "add-data-via-api",
        "client_payload": {
            "collection": "article-4-direction",
            "triggered_by": "python-script",
            "lookup_csv_rows": [
                "article-4-direction-area,,,,local-authority:SKP,A4a-14-test,7010009246,,,",
                "article-4-direction-area,,,,local-authority:SKP,A4a-18-test,7010009247,,,",
            ],
            "endpoint_csv_rows": [
                "fc506934d0ccc949f0f1ecd12ff8992587598e40950b067f2e1fc874b260a17a,https://example.com/data.json,,,2025-01-29T00:00:00Z,,"
            ],
            "column_csv_rows": [
                "article-4-direction-area,fc506934d0ccc949f0f1ecd12ff8992587598e40950b067f2e1fc874b260a17a,,geom,geometry,,,"
            ],
        }
    }

    # Step 4: Trigger workflow
    response = trigger_workflow(access_token, REPO_OWNER, REPO_NAME, payload)

    if response.status_code == 204:
        print("✓ Workflow triggered successfully!")
    else:
        print(f"✗ Error: {response.status_code}")
        print(response.text)
```

**Dependencies:** `pip install PyJWT cryptography requests`

## Monitoring the Workflow

After triggering, you can monitor the workflow run:

1. Go to the Actions tab in the GitHub repository
2. Look for "Add Data via REST API" workflows
3. The most recent run will show the progress

Note: The API call returns 204 No Content immediately, but the workflow runs asynchronously. Check the Actions tab to see the actual execution.

## Troubleshooting

### 401 Unauthorized
- Check that your JWT is correctly signed with the private key
- Verify the App ID is correct
- Ensure the JWT hasn't expired (max 10 minutes)

### 404 Not Found
- Verify the Installation ID is correct for your organization
- Ensure the app is installed on the organization
- Check that the repository name and owner are correct

### 403 Forbidden
- Verify the GitHub App has the required permissions
- Ensure the app installation has access to this repository
- Check that the "Workflows" permission is enabled

### Workflow doesn't trigger
- Verify the `event_type` is exactly `"add-data-via-api"`
- Check that the collection name is correct and the directory exists
- Look for workflow run failures in the Actions tab

### JWT Generation Issues
- Ensure the private key file is in PEM format
- Check that the private key hasn't been regenerated (old keys won't work)
- Verify line endings in the PEM file are correct (LF, not CRLF)

## Error Handling

The workflow will fail if:

- The collection name is empty
- The collection directory doesn't exist in `collection/`
- The pipeline directory doesn't exist in `pipeline/`
- There are no changes to commit (all provided CSV rows were empty)

## Security Considerations

- **Private Key Security:** Never commit your GitHub App private key to version control
- **Use Secrets Management:** Store the private key in a secure secrets manager (AWS Secrets Manager, HashiCorp Vault, etc.)
- **Environment Variables:** Use environment variables for App ID and Installation ID
- **Token Expiration:** Installation access tokens expire after 1 hour - generate a fresh token for each API call
- **App Permissions:** Only grant the minimum required permissions to the GitHub App
- **Audit Logging:** All workflow triggers are logged in GitHub Actions for audit purposes
- **Repository Access:** Only GitHub Apps with proper installation permissions can trigger this workflow
