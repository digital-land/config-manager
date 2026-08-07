"""Submission-time writers for the config-manager-owned RequestMeta table.

These record per-request metadata (which flow created it, the config branch
baseline) that the async request's own params can't carry, so downstream pages
can behave correctly without re-inferring it.
"""

import json
import logging

from application.db.models import RequestMeta
from application.extensions import db

from ..services.github import GitHubAppError, get_config_baseline_sha

logger = logging.getLogger(__name__)


def load_json_list(value: str | None) -> list:
    """Parse a JSON list stored in a RequestMeta text column, tolerating missing or
    malformed values (returns an empty list)."""
    if not value:
        return []
    try:
        loaded = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    return loaded if isinstance(loaded, list) else []


def record_source_flow(request_id, source_flow):
    """Persist which flow (``add_data`` / ``assign_entities``) created this request.

    The entities-preview and confirm pages live under the datamanager blueprint
    but are shared with the assign-entities flow. Recording the originating flow
    at submission time lets those shared pages apply the correct process lock
    without inferring it from the request's params shape.
    """
    if not request_id:
        return
    meta = db.session.get(RequestMeta, request_id)
    if meta is None:
        meta = RequestMeta(request_id=request_id, source_flow=source_flow)
        db.session.add(meta)
    else:
        meta.source_flow = source_flow
    db.session.commit()


def record_branch_baseline(request_id, github_branch, check_request_id=None):
    """
    Capture the config branch HEAD at assessment-submission time so that, when the
    user later confirms, we can detect whether the branch advanced underneath the
    assessment (which would make the assigned entity numbers stale).
    """
    if not github_branch:
        return
    try:
        # Quick HEAD read only - the workflow-idle wait happens at confirm time (the
        # decision point), so submission stays fast.
        sha = get_config_baseline_sha(github_branch)
    except GitHubAppError as e:
        logger.warning("Could not capture branch baseline for %s: %s", request_id, e)
        return
    if not sha:
        return

    meta = db.session.get(RequestMeta, request_id)
    if meta is None:
        meta = RequestMeta(
            request_id=request_id,
            branch_sha=sha,
            check_request_id=check_request_id,
        )
        db.session.add(meta)
    else:
        meta.branch_sha = sha
        if check_request_id:
            meta.check_request_id = check_request_id
    db.session.commit()
