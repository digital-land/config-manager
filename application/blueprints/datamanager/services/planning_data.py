import logging

import requests
from flask import current_app

from ..utils import REQUESTS_TIMEOUT

logger = logging.getLogger(__name__)


def get_entities_for_organisation_and_dataset(
    organisation_entity: int | str, dataset: str
) -> list:
    """
    Fetch all authoritative entities for a given organisation entity number and dataset
    from the planning data /entity.json endpoint. Handles pagination.
    Returns a list of entity dicts.
    """
    planning_url = current_app.config.get("PLANNING_BASE_URL")
    url = (
        f"{planning_url}/entity.json"
        f"?organisation_entity={organisation_entity}"
        f"&dataset={dataset}"
        f"&quality=authoritative"
        f"&limit=100"
    )

    entities = []
    page = 0
    while url:
        page += 1
        try:
            response = requests.get(url, timeout=REQUESTS_TIMEOUT)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            logger.error(
                f"Failed to fetch entities (page {page}) for organisation_entity="
                f"{organisation_entity} dataset={dataset}: {e}",
                exc_info=True,
            )
            break

        entities.extend(data.get("entities", []))

        next_url = (data.get("links") or {}).get("next")
        if next_url:
            if next_url.startswith("/"):
                url = f"{planning_url.rstrip('/')}{next_url}"
            else:
                url = next_url
        else:
            url = None

    logger.info(
        f"Fetched {len(entities)} entities for organisation_entity={organisation_entity} "
        f"dataset={dataset} in {page} page(s)"
    )
    return entities
