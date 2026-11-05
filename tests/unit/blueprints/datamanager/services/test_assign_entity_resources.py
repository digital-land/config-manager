from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from application.blueprints.datamanager.services.assign_entity_resources import (
    IN_PROGRESS,
    PROCESSED,
    get_assign_entity_resource_statuses,
    set_assign_entity_resource_status,
)
from application.db.models import AssignEntityResource
from application.extensions import db


@pytest.mark.parametrize(
    ("now", "cutoff"),
    [
        (datetime(2026, 1, 15, 6, 30), datetime(2026, 1, 15, 6, 0)),
        (datetime(2026, 7, 15, 5, 30), datetime(2026, 7, 15, 5, 0)),
    ],
)
def test_get_statuses_expires_previous_day_in_progress_but_keeps_processed(
    app, now, cutoff
):
    with app.app_context():
        suffix = str(cutoff.month)
        previous_day_key = (f"previous-{suffix}", "tree", "")
        boundary_key = (f"boundary-{suffix}", "tree", "")
        processed_key = (f"processed-{suffix}", "tree", "")
        db.session.add_all(
            [
                AssignEntityResource(
                    resource=previous_day_key[0],
                    dataset="tree",
                    organisation="",
                    status=IN_PROGRESS,
                    actor_username="yesterday-user",
                    updated_at=cutoff - timedelta(seconds=1),
                ),
                AssignEntityResource(
                    resource=boundary_key[0],
                    dataset="tree",
                    organisation="",
                    status=IN_PROGRESS,
                    actor_username="today-user",
                    updated_at=cutoff,
                ),
                AssignEntityResource(
                    resource=processed_key[0],
                    dataset="tree",
                    organisation="",
                    status=PROCESSED,
                    actor_username="processed-user",
                    updated_at=cutoff - timedelta(days=365),
                ),
            ]
        )
        db.session.commit()

        with patch(
            "application.blueprints.datamanager.services.assign_entity_resources.utcnow",
            return_value=now,
        ):
            statuses = get_assign_entity_resource_statuses(
                [previous_day_key, boundary_key, processed_key]
            )

        assert previous_day_key not in statuses
        assert statuses[boundary_key].status == IN_PROGRESS
        assert statuses[boundary_key].actor_username == "today-user"
        assert statuses[processed_key].status == PROCESSED
        assert statuses[processed_key].actor_username == "processed-user"


def test_starting_expired_resource_records_current_user_and_timestamp(app):
    with app.app_context():
        key = ("restarted-resource", "tree", "")
        db.session.add(
            AssignEntityResource(
                resource=key[0],
                dataset=key[1],
                organisation=key[2],
                status=IN_PROGRESS,
                actor_username="yesterday-user",
                updated_at=datetime(2020, 1, 1),
            )
        )
        db.session.commit()
        current_time = datetime(2026, 7, 15, 23, 30)

        with patch(
            "application.blueprints.datamanager.services.assign_entity_resources.utcnow",
            return_value=current_time,
        ):
            record = set_assign_entity_resource_status(
                key[0], key[1], key[2], IN_PROGRESS, "today-user"
            )

        assert record.status == IN_PROGRESS
        assert record.actor_username == "today-user"
        assert record.updated_at == current_time
