"""
GitHub App service for authenticating and triggering workflows.
"""

import json
import time
import logging
import requests
import jwt
from flask import current_app

logger = logging.getLogger(__name__)


class GitHubAppError(Exception):
    """Base exception for GitHub App errors"""

    pass


class GitHubAppAuthError(GitHubAppError):
    """Raised when authentication fails"""

    pass


class GitHubWorkflowError(GitHubAppError):
    """Raised when workflow trigger fails"""

    pass


def generate_jwt(app_id: str, private_key: str) -> str:
    """
    Generate a JWT for GitHub App authentication.

    Args:
        app_id: GitHub App ID
        private_key: PEM-formatted private key

    Returns:
        JWT token string

    Raises:
        GitHubAppAuthError: If JWT generation fails
    """
    try:
        payload = {
            "iat": int(time.time()),
            "exp": int(time.time()) + 600,  # 10 minutes (max allowed)
            "iss": app_id,
        }

        token = jwt.encode(payload, private_key, algorithm="RS256")
        logger.debug(f"Generated JWT for App ID: {app_id}")
        return token
    except Exception as e:
        logger.error(f"Failed to generate JWT: {e}")
        raise GitHubAppAuthError(f"JWT generation failed: {e}")


def get_installation_token(jwt_token: str, installation_id: str) -> str:
    """
    Exchange JWT for an installation access token.

    Args:
        jwt_token: JWT token from generate_jwt()
        installation_id: GitHub App installation ID

    Returns:
        Installation access token

    Raises:
        GitHubAppAuthError: If token exchange fails
    """
    url = f"https://api.github.com/app/installations/{installation_id}/access_tokens"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {jwt_token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    try:
        response = requests.post(url, headers=headers, timeout=10)
        response.raise_for_status()
        token = response.json()["token"]
        logger.info(
            f"Successfully obtained installation token for installation {installation_id}"
        )
        return token
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to get installation token: {e}")
        if hasattr(e, "response") and e.response is not None:
            logger.error(f"Response: {e.response.text}")
        raise GitHubAppAuthError(f"Failed to get installation token: {e}")


def trigger_add_data_workflow(
    collection: str,
    lookup_csv_rows: list = None,
    endpoint_csv_rows: list = None,
    source_csv_rows: list = None,
    column_csv_rows: list = None,
    triggered_by: str = "config-manager",
) -> dict:
    """
    Trigger the 'add-data-via-api' workflow in the digital-land/config repository.

    Args:
        collection: Collection name (e.g., "article-4-direction")
        lookup_csv_rows: List of CSV row strings for lookup.csv
        endpoint_csv_rows: List of CSV row strings for endpoint.csv
        source_csv_rows: List of CSV row strings for source.csv
        column_csv_rows: List of CSV row strings for column.csv
        triggered_by: Identifier for who/what triggered this

    Returns:
        dict with keys:
            - success (bool): Whether the trigger was successful
            - status_code (int): HTTP status code
            - message (str): Success or error message

    Raises:
        GitHubWorkflowError: If workflow trigger fails
    """
    # Get config
    app_id = current_app.config.get("GITHUB_APP_ID")
    installation_id = current_app.config.get("GITHUB_APP_INSTALLATION_ID")
    private_key = current_app.config.get("GITHUB_APP_PRIVATE_KEY")

    if not all([app_id, installation_id, private_key]):
        error_msg = "GitHub App credentials not configured"
        logger.info(error_msg)
        raise GitHubWorkflowError(error_msg)

    try:
        # Step 1: Generate JWT
        logger.info(f"Generating JWT for App ID: {app_id}")
        jwt_token = generate_jwt(app_id, private_key)

        # Step 2: Get installation access token
        logger.info(f"Getting installation token for installation: {installation_id}")
        access_token = get_installation_token(jwt_token, installation_id)

        # Step 3: Build payload
        client_payload = {"collection": collection, "triggered_by": triggered_by}

        # Add CSV rows only if provided
        if lookup_csv_rows:
            client_payload["lookup_csv_rows"] = lookup_csv_rows
        if endpoint_csv_rows:
            client_payload["endpoint_csv_rows"] = endpoint_csv_rows
        if source_csv_rows:
            client_payload["source_csv_rows"] = source_csv_rows
        if column_csv_rows:
            client_payload["column_csv_rows"] = column_csv_rows

        payload = {"event_type": "add-data-via-api", "client_payload": client_payload}

        # GitHub enforces a 10KB limit on client_payload
        client_payload_size = len(json.dumps(client_payload).encode("utf-8"))
        client_payload_kb = client_payload_size / 1024
        logger.info(f"client_payload size: {client_payload_kb:.1f}KB")
        if client_payload_size > 10240:
            raise GitHubWorkflowError(
                f"client_payload is {client_payload_kb:.1f}KB, exceeds GitHub's 10KB limit"
            )

        # Step 4: Trigger workflow
        logger.info(f"Triggering workflow for collection: {collection}")
        logger.debug(f"Payload: {payload}")

        url = "https://api.github.com/repos/digital-land/config/dispatches"
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {access_token}",
            "X-GitHub-Api-Version": "2022-11-28",
        }

        response = requests.post(url, headers=headers, json=payload, timeout=10)

        if response.status_code == 204:
            logger.info(f"Successfully triggered workflow for collection: {collection}")
            return {
                "success": True,
                "status_code": 204,
                "message": f"Workflow triggered successfully for collection '{collection}'",
            }
        else:
            error_msg = (
                f"Unexpected status code: {response.status_code} - {response.text}"
            )
            logger.error(error_msg)
            return {
                "success": False,
                "status_code": response.status_code,
                "message": f"Failed to trigger workflow: {error_msg}",
            }

    except GitHubAppError as e:
        # Re-raise GitHub-specific errors
        logger.exception(f"Unexpected github error triggering workflow: {e}")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error triggering workflow: {e}")
        raise GitHubWorkflowError(f"Unexpected error: {e}")
