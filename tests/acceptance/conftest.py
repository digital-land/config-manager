from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def sync_config_branch():
    """Journey tests use a successful GitHub sync; service tests cover failures."""
    with patch(
        "application.blueprints.datamanager.services.async_api.sync_config_branch"
    ) as sync:
        yield sync
