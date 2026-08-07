"""
Add-data confirm handler.

Turns a reviewed transform into a commit: it guards against submitting stale or
duplicate data (idempotency, staleness, entity-number clashes), then triggers the
GitHub workflow that appends the rows to the config branch. Shared by the add-data
and assign-entities flows.
"""

import logging
from datetime import datetime

from flask import render_template, session, url_for
from sqlalchemy.exc import IntegrityError

from application.db.models import RequestMeta
from application.extensions import db

from . import ControllerError
from .request_meta import load_json_list
from ..services.async_api import AsyncAPIError, fetch_request
from ..services.entity_claims import (
    claim_entities,
    entity_clashes,
    release_claims,
    release_others,
)
from ..services.github import (
    config_branch_changed_for_collection,
    trigger_add_data_async_workflow,
    GitHubAppError,
    GitHubWorkflowError,
)

logger = logging.getLogger(__name__)


def _default_return_url(source_flow):
    return (
        url_for("assign_entities.flagged_resources_start")
        if source_flow == "assign_entities"
        else url_for("datamanager.dashboard_get")
    )


def _confirm_user_login():
    return (session.get("user") or {}).get("login", "unknown")


def _is_admin():
    return bool((session.get("user") or {}).get("is_admin"))


def _new_entity_numbers(req):
    """Entity numbers frozen into the assessment. They arrive as strings in the
    response payload, so cast and skip blanks."""
    data = (req.get("response") or {}).get("data") or {}
    new_entities = (data.get("pipeline-summary") or {}).get("new-entities") or []
    numbers = []
    for entry in new_entities:
        value = str((entry or {}).get("entity") or "").strip()
        if not value:
            continue
        try:
            numbers.append(int(value))
        except ValueError:
            continue
    return numbers


def _ensure_request_meta(request_id):
    """Get-or-create the RequestMeta row so the atomic dispatch gate has a row to
    update. Concurrent inserts race on the primary key; the loser re-reads."""
    meta = db.session.get(RequestMeta, request_id)
    if meta is not None:
        return meta
    meta = RequestMeta(request_id=request_id)
    db.session.add(meta)
    try:
        db.session.commit()
    except IntegrityError:
        db.session.rollback()
        meta = db.session.get(RequestMeta, request_id)
    return meta


def _release_dispatch(request_id):
    """Undo the atomic dispatch claim + entity claims after a failed dispatch, so a
    legitimate retry is not blocked by our own aborted attempt."""
    db.session.query(RequestMeta).filter_by(request_id=request_id).update(
        {"dispatched_at": None}, synchronize_session=False
    )
    db.session.commit()
    release_claims(request_id)


def _render_stale(
    request_meta, collection, github_branch, source_flow, return_url, unverified=False
):
    # Prefer sending the user back to the check-results page they started from.
    check_request_id = request_meta.check_request_id if request_meta else None
    if check_request_id:
        rerun_url = url_for("datamanager.check_results", request_id=check_request_id)
    else:
        rerun_url = return_url or _default_return_url(source_flow)
    return render_template(
        "datamanager/add-data-stale.html",
        collection=collection,
        github_branch=github_branch,
        source_flow=source_flow,
        return_url=rerun_url,
        unverified=unverified,
    )


def _render_already_submitted(source_flow, return_url):
    return render_template(
        "datamanager/add-data-already-submitted.html",
        source_flow=source_flow,
        return_url=return_url or _default_return_url(source_flow),
    )


def _render_entity_clash(
    request_id, collection, github_branch, entities, source_flow, return_url
):
    return render_template(
        "datamanager/add-data-entity-clash.html",
        request_id=request_id,
        collection=collection,
        github_branch=github_branch,
        entities=entities,
        can_override=_is_admin(),
        source_flow=source_flow,
        return_url=return_url or _default_return_url(source_flow),
    )


def handle_add_data_confirm(
    request_id,
    github_branch: str | None = None,
    source_flow: str = "add_data",
    return_url: str | None = None,
    override: bool = False,
):
    request_meta = db.session.get(RequestMeta, request_id)
    endpoints_to_retire = (
        load_json_list(request_meta.endpoints_to_retire) if request_meta else []
    )
    endpoints_to_unretire = (
        load_json_list(request_meta.endpoints_to_unretire) if request_meta else []
    )

    # first guard against a concurrent double-submit, should be relatively rare
    if request_meta and request_meta.dispatched_at:
        return _render_already_submitted(source_flow, return_url)

    # New-branch submissions don't touch a shared branch so no check needed.
    if github_branch:
        try:
            req = fetch_request(request_id)
        except AsyncAPIError as e:
            logger.warning(
                "Blocking confirm for %s: request fetch failed: %s", request_id, e
            )
            return _render_stale(
                request_meta,
                None,
                github_branch,
                source_flow,
                return_url,
                unverified=True,
            )

        collection = (req.get("params") or {}).get("collection")
        baseline_sha = request_meta.branch_sha if request_meta else None

        # Fail closed when we cannot verify the assessment is current.
        if not baseline_sha:
            logger.info("Blocking confirm for %s: no baseline captured", request_id)
            return _render_stale(
                request_meta,
                collection,
                github_branch,
                source_flow,
                return_url,
                unverified=True,
            )
        try:
            # Guard 2: staleness check against GitHub - has the branch already moved
            # for this collection since the assessment? (In-flight submissions that
            # haven't committed yet are caught by the entity-claim guard below, so we
            # don't block the request waiting for workflows to settle.)
            changed = bool(collection) and config_branch_changed_for_collection(
                baseline_sha, github_branch, collection
            )
        except GitHubAppError as e:
            logger.warning(
                "Blocking confirm for %s: staleness check failed: %s", request_id, e
            )
            return _render_stale(
                request_meta,
                collection,
                github_branch,
                source_flow,
                return_url,
                unverified=True,
            )
        if changed:
            logger.info(
                "Blocking stale confirm for request %s: %s advanced for collection %s",
                request_id,
                github_branch,
                collection,
            )
            return _render_stale(
                request_meta, collection, github_branch, source_flow, return_url
            )

        # Guard number 3, Entity-claim guard: block if these numbers are already claimed on this branch
        # by another in-flight submission (closes the window before the commit lands).
        entities = _new_entity_numbers(req)
        if collection and entities:
            clashes = entity_clashes(collection, github_branch, request_id, entities)
            if clashes:
                # Only an admin may override; the flag alone is not enough (the form
                # is admin-gated in the UI, but enforce it server-side too).
                if not (override and _is_admin()):
                    logger.info(
                        "Blocking confirm for %s: entity clash on %s/%s: %s",
                        request_id,
                        collection,
                        github_branch,
                        clashes,
                    )
                    return _render_entity_clash(
                        request_id,
                        collection,
                        github_branch,
                        clashes,
                        source_flow,
                        return_url,
                    )
                logger.warning(
                    "Admin %s overriding entity clash for %s on %s/%s: %s",
                    _confirm_user_login(),
                    request_id,
                    collection,
                    github_branch,
                    clashes,
                )
                release_others(collection, github_branch, clashes)
            if not claim_entities(collection, github_branch, request_id, entities):
                # Concurrent claim conflict -> fail closed.
                logger.info(
                    "Blocking confirm for %s: concurrent entity claim on %s/%s",
                    request_id,
                    collection,
                    github_branch,
                )
                return _render_entity_clash(
                    request_id,
                    collection,
                    github_branch,
                    entities,
                    source_flow,
                    return_url,
                )

    # Atomic dispatch gate: only one POST can flip dispatched_at from NULL, so a
    # concurrent or retried confirm can't double-submit.
    _ensure_request_meta(request_id)
    won_slot = (
        db.session.query(RequestMeta)
        .filter_by(request_id=request_id, dispatched_at=None)
        .update({"dispatched_at": datetime.utcnow()}, synchronize_session=False)
    )
    db.session.commit()
    if not won_slot:
        return _render_already_submitted(source_flow, return_url)

    try:
        result = trigger_add_data_async_workflow(
            request_id=request_id,
            triggered_by=f"{_confirm_user_login()}",
            github_branch=github_branch,
            endpoints_to_retire=endpoints_to_retire,
            endpoints_to_unretire=endpoints_to_unretire,
        )
    except GitHubWorkflowError as e:
        _release_dispatch(request_id)
        logger.exception(f"GitHub async workflow error: {e}")
        raise ControllerError(f"GitHub workflow error: {str(e)}") from e

    if not result["success"]:
        _release_dispatch(request_id)
        logger.error(f"Failed to trigger async workflow: {result['message']}")
        raise ControllerError(f"Failed to trigger async workflow: {result['message']}")

    logger.info(f"Successfully triggered async workflow for request_id: {request_id}")
    return render_template(
        "datamanager/add-data-success.html",
        message=result["message"],
        github_branch=github_branch,
        source_flow=source_flow,
        return_url=return_url or _default_return_url(source_flow),
    )
