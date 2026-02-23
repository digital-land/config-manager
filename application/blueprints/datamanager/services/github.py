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


def trigger_add_data_async_workflow(
    request_id: str,
    triggered_by: str = "config-manager",
) -> dict:
    """
    Trigger the 'add-data-async' workflow in the digital-land/config repository.

    Instead of sending CSV data in the payload (which can exceed GitHub's 10KB limit),
    this sends only a request_id. The workflow fetches the full data from the async API.
    """
    app_id = current_app.config.get("GITHUB_APP_ID")
    installation_id = current_app.config.get("GITHUB_APP_INSTALLATION_ID")
    private_key = current_app.config.get("GITHUB_APP_PRIVATE_KEY")

    if not all([app_id, installation_id, private_key]):
        error_msg = "GitHub App credentials not configured"
        logger.info(error_msg)
        raise GitHubWorkflowError(error_msg)

    try:
        logger.info(f"Generating JWT for App ID: {app_id}")
        jwt_token = generate_jwt(app_id, private_key)

        logger.info(f"Getting installation token for installation: {installation_id}")
        access_token = get_installation_token(jwt_token, installation_id)

        payload = {
            "event_type": "add-data-async",
            "client_payload": {
                "request_id": request_id,
                "triggered_by": triggered_by,
            },
        }

        logger.info(f"Triggering async workflow for request_id: {request_id}")
        logger.debug(f"Payload: {payload}")

        url = "https://api.github.com/repos/digital-land/config/dispatches"
        headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {access_token}",
            "X-GitHub-Api-Version": "2022-11-28",
        }

        response = requests.post(url, headers=headers, json=payload, timeout=10)

        if response.status_code == 204:
            logger.info(
                f"Successfully triggered async workflow for request_id: {request_id}"
            )
            return {
                "success": True,
                "status_code": 204,
                "message": f"Async workflow triggered successfully for request '{request_id}'",
            }
        else:
            error_msg = (
                f"Unexpected status code: {response.status_code} - {response.text}"
            )
            logger.error(error_msg)
            return {
                "success": False,
                "status_code": response.status_code,
                "message": f"Failed to trigger async workflow: {error_msg}",
            }

    except GitHubAppError as e:
        logger.exception(f"Unexpected github error triggering async workflow: {e}")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error triggering async workflow: {e}")
        raise GitHubWorkflowError(f"Unexpected error: {e}")
