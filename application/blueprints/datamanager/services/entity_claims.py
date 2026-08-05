"""
Entity-number claim registry.

Add-data assigns entity numbers per collection at assessment time and freezes them
into the request result, but they are not visible on the config branch until the
GitHub Action commits ~1-2 minutes later. During that window a concurrent
same-collection submission is assessed against the same HEAD and gets the *same*
numbers, and the confirm-time branch compare can't see a not-yet-committed conflict.

This registry records the numbers a confirm is about to commit, scoped to the target
branch, so a second confirm that would overlap is caught immediately - with no
dependence on the (laggy) GitHub Actions API. Entries expire after a TTL that only has
to bridge assessment->commit; the branch compare takes over once the commit lands.
"""

import logging
from datetime import datetime, timedelta

from flask import current_app
from sqlalchemy.exc import IntegrityError

from application.db.models import EntityClaim
from application.extensions import db

logger = logging.getLogger(__name__)


def _cleanup_expired(collection: str, branch: str) -> None:
    ttl = current_app.config.get("ENTITY_CLAIM_TTL_SECONDS", 2 * 60 * 60)
    cutoff = datetime.utcnow() - timedelta(seconds=ttl)
    db.session.query(EntityClaim).filter(
        EntityClaim.collection == collection,
        EntityClaim.branch == branch,
        EntityClaim.claimed_at < cutoff,
    ).delete(synchronize_session=False)
    db.session.commit()


def entity_clashes(
    collection: str, branch: str, request_id: str, entities: list[int]
) -> list[int]:
    """
    Return the entity numbers already claimed on this branch by a *different* request
    (after expiring stale claims). Empty list means safe to claim.
    """
    if not entities:
        return []
    _cleanup_expired(collection, branch)
    rows = (
        db.session.query(EntityClaim)
        .filter(
            EntityClaim.collection == collection,
            EntityClaim.branch == branch,
            EntityClaim.entity.in_(entities),
            EntityClaim.request_id != request_id,
        )
        .all()
    )
    return sorted({row.entity for row in rows})


def claim_entities(
    collection: str, branch: str, request_id: str, entities: list[int]
) -> bool:
    """
    Claim `entities` for this request on this branch. Idempotent for the same request
    (re-claiming our own rows is fine). Returns True on success; False if a concurrent
    confirm claimed one of these numbers between the clash check and here (the unique
    constraint fires) - callers should fail closed on False.
    """
    if not entities:
        return True
    owned = {
        row.entity
        for row in db.session.query(EntityClaim)
        .filter(
            EntityClaim.collection == collection,
            EntityClaim.branch == branch,
            EntityClaim.entity.in_(entities),
            EntityClaim.request_id == request_id,
        )
        .all()
    }
    for entity in entities:
        if entity not in owned:
            db.session.add(
                EntityClaim(
                    collection=collection,
                    entity=entity,
                    branch=branch,
                    request_id=request_id,
                )
            )
    try:
        db.session.commit()
        return True
    except IntegrityError as e:
        db.session.rollback()
        logger.warning(
            "Concurrent entity claim conflict for %s on %s/%s: %s",
            request_id,
            collection,
            branch,
            e,
        )
        return False


def release_others(collection: str, branch: str, entities: list[int]) -> None:
    """Drop any existing claims for these entities on this branch (admin override:
    take ownership of numbers another request had claimed)."""
    if not entities:
        return
    db.session.query(EntityClaim).filter(
        EntityClaim.collection == collection,
        EntityClaim.branch == branch,
        EntityClaim.entity.in_(entities),
    ).delete(synchronize_session=False)
    db.session.commit()


def release_claims(request_id: str) -> None:
    """Drop all claims for a request (dispatch failed, so its numbers were never
    committed and must not block a retry)."""
    db.session.query(EntityClaim).filter(
        EntityClaim.request_id == request_id
    ).delete(synchronize_session=False)
    db.session.commit()
