"""Persistent status tracking for Assign Entities resources."""

from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

from sqlalchemy import tuple_

from application.db.models import AssignEntityResource
from application.extensions import db
from application.utils import utcnow

IN_PROGRESS = "in_progress"
PROCESSED = "processed"


def set_assign_entity_resource_status(
    resource: str,
    dataset: str,
    organisation: str | None,
    status: str | None,
    actor_username: str,
):
    """Create or update the status for a resource, dataset, and organisation."""
    organisation = organisation or ""
    record = db.session.get(AssignEntityResource, (resource, dataset, organisation))
    if record is None:
        record = AssignEntityResource(
            resource=resource,
            dataset=dataset,
            organisation=organisation,
        )
        db.session.add(record)

    record.status = status
    record.actor_username = actor_username
    record.updated_at = utcnow()
    db.session.commit()
    return record


def get_assign_entity_resource_statuses(
    resource_keys: list[tuple[str, str, str | None]],
) -> dict[tuple[str, str, str], AssignEntityResource]:
    """Return persisted status records keyed by resource, dataset, organisation."""
    keys = [
        (resource, dataset, organisation or "")
        for resource, dataset, organisation in resource_keys
    ]
    if not keys:
        return {}
    records = []
    for start in range(0, len(keys), 300):
        records.extend(
            AssignEntityResource.query.filter(
                tuple_(
                    AssignEntityResource.resource,
                    AssignEntityResource.dataset,
                    AssignEntityResource.organisation,
                ).in_(keys[start : start + 300])
            ).all()
        )
    uk_now = utcnow().replace(tzinfo=timezone.utc).astimezone(
        ZoneInfo("Europe/London")
    )
    uk_cutoff = datetime.combine(uk_now.date(), time(11, 10), tzinfo=uk_now.tzinfo)
    cutoff = uk_cutoff.astimezone(timezone.utc).replace(tzinfo=None)
    return {
        (record.resource, record.dataset, record.organisation): record
        for record in records
        if record.status != IN_PROGRESS or record.updated_at >= cutoff
    }
