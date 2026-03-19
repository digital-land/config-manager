import time
from unittest.mock import MagicMock, patch

import pytest

import application.blueprints.datamanager.services.organisation as org_module
from application.blueprints.datamanager.services.organisation import (
    format_org_options,
    get_organisation_name,
    get_provision_orgs_for_dataset,
    is_valid_organisation,
)


@pytest.fixture(autouse=True)
def clear_caches():
    """Reset module-level caches between tests to avoid cross-test contamination."""
    org_module._provision_cache.clear()
    org_module._org_mapping_cache["data"] = None
    org_module._org_mapping_cache["expires_at"] = 0
    yield
    org_module._provision_cache.clear()
    org_module._org_mapping_cache["data"] = None
    org_module._org_mapping_cache["expires_at"] = 0


def _make_provision_response(csv_text):
    mock_resp = MagicMock()
    mock_resp.text = csv_text
    mock_resp.raise_for_status = MagicMock()
    return mock_resp


def _make_org_mapping_response(rows, next_url=None):
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {"rows": rows, "next_url": next_url}
    return mock_resp


PROVISION_CSV = (
    "dataset,organisation\n"
    "brownfield-land,local-authority:ABC\n"
    "brownfield-land,local-authority:DEF\n"
    "brownfield-land,local-authority:ABC\n"  # duplicate — should be deduplicated
    "conservation-area,local-authority:XYZ\n"
)

ORG_ROWS = [
    {"organisation": "local-authority:ABC", "name": "ABC Council"},
    {"organisation": "local-authority:DEF", "name": "DEF Borough"},
    {"organisation": "local-authority:XYZ", "name": "XYZ District"},
]


class TestGetProvisionOrgsForDataset:
    def test_returns_unique_orgs_for_dataset(self, app):
        with app.app_context():
            with patch(
                "application.blueprints.datamanager.services.organisation.requests.get"
            ) as mock_get:
                mock_get.return_value = _make_provision_response(PROVISION_CSV)
                result = get_provision_orgs_for_dataset("brownfield-land")

        assert result == ["local-authority:ABC", "local-authority:DEF"]
        mock_get.assert_called_once()

    def test_does_not_include_other_datasets(self, app):
        with app.app_context():
            with patch(
                "application.blueprints.datamanager.services.organisation.requests.get"
            ) as mock_get:
                mock_get.return_value = _make_provision_response(PROVISION_CSV)
                result = get_provision_orgs_for_dataset("conservation-area")

        assert result == ["local-authority:XYZ"]

    def test_cache_hit_does_not_refetch(self, app):
        with app.app_context():
            with patch(
                "application.blueprints.datamanager.services.organisation.requests.get"
            ) as mock_get:
                mock_get.return_value = _make_provision_response(PROVISION_CSV)
                get_provision_orgs_for_dataset("brownfield-land")
                result = get_provision_orgs_for_dataset("brownfield-land")

        assert result == ["local-authority:ABC", "local-authority:DEF"]
        mock_get.assert_called_once()  # fetched only once; second call is a cache hit

    def test_http_error_returns_empty_list(self, app):
        with app.app_context():
            with patch(
                "application.blueprints.datamanager.services.organisation.requests.get"
            ) as mock_get:
                mock_get.side_effect = Exception("Network error")
                result = get_provision_orgs_for_dataset("brownfield-land")

        assert result == []

    def test_http_error_with_stale_cache_returns_stale_data(self, app):
        # Seed a stale (expired) cache entry
        org_module._provision_cache["brownfield-land"] = {
            "data": ["local-authority:STALE"],
            "expires_at": time.monotonic() - 1,  # already expired
        }
        with app.app_context():
            with patch(
                "application.blueprints.datamanager.services.organisation.requests.get"
            ) as mock_get:
                mock_get.side_effect = Exception("Network error")
                result = get_provision_orgs_for_dataset("brownfield-land")

        assert result == ["local-authority:STALE"]

    def test_returns_empty_list_for_unknown_dataset(self, app):
        with app.app_context():
            with patch(
                "application.blueprints.datamanager.services.organisation.requests.get"
            ) as mock_get:
                mock_get.return_value = _make_provision_response(PROVISION_CSV)
                result = get_provision_orgs_for_dataset("unknown-dataset")

        assert result == []


class TestGetOrgMapping:
    def test_normal_fetch_builds_mapping(self, app):
        with app.app_context():
            with patch(
                "application.blueprints.datamanager.services.organisation.requests.get"
            ) as mock_get:
                mock_get.return_value = _make_org_mapping_response(ORG_ROWS)
                name = get_organisation_name("local-authority:ABC")

        assert name == "ABC Council"

    def test_paginated_response_fetches_all_pages(self, app):
        page1_rows = [{"organisation": "local-authority:ABC", "name": "ABC Council"}]
        page2_rows = [{"organisation": "local-authority:DEF", "name": "DEF Borough"}]

        page1_resp = MagicMock()
        page1_resp.raise_for_status = MagicMock()
        page1_resp.json.return_value = {
            "rows": page1_rows,
            "next_url": "/organisation.json?_shape=objects&_size=max&_next=2",
        }

        page2_resp = MagicMock()
        page2_resp.raise_for_status = MagicMock()
        page2_resp.json.return_value = {"rows": page2_rows, "next_url": None}

        with app.app_context():
            with patch(
                "application.blueprints.datamanager.services.organisation.requests.get"
            ) as mock_get:
                mock_get.side_effect = [page1_resp, page2_resp]
                abc_name = get_organisation_name("local-authority:ABC")
                # Cache is now warm — do not re-fetch
                def_name = get_organisation_name("local-authority:DEF")

        assert abc_name == "ABC Council"
        assert def_name == "DEF Borough"
        assert mock_get.call_count == 2  # two pages fetched

    def test_unknown_code_returns_code_itself(self, app):
        with app.app_context():
            with patch(
                "application.blueprints.datamanager.services.organisation.requests.get"
            ) as mock_get:
                mock_get.return_value = _make_org_mapping_response(ORG_ROWS)
                name = get_organisation_name("local-authority:UNKNOWN")

        assert name == "local-authority:UNKNOWN"

    def test_stale_cache_returned_on_error(self, app):
        # Pre-seed the cache with stale data and expired timestamp
        org_module._org_mapping_cache["data"] = {
            "local-authority:CACHED": "Cached Council"
        }
        org_module._org_mapping_cache["expires_at"] = time.monotonic() - 1

        with app.app_context():
            with patch(
                "application.blueprints.datamanager.services.organisation.requests.get"
            ) as mock_get:
                mock_get.side_effect = Exception("Network error")
                name = get_organisation_name("local-authority:CACHED")

        assert name == "Cached Council"

    def test_cache_hit_does_not_refetch(self, app):
        with app.app_context():
            with patch(
                "application.blueprints.datamanager.services.organisation.requests.get"
            ) as mock_get:
                mock_get.return_value = _make_org_mapping_response(ORG_ROWS)
                get_organisation_name("local-authority:ABC")
                get_organisation_name("local-authority:DEF")

        mock_get.assert_called_once()  # second call served from cache


class TestIsValidOrganisation:
    def test_valid_org_returns_true(self, app):
        with app.app_context():
            with patch(
                "application.blueprints.datamanager.services.organisation.requests.get"
            ) as mock_get:
                mock_get.return_value = _make_org_mapping_response(ORG_ROWS)
                result = is_valid_organisation("local-authority:ABC")

        assert result is True

    def test_invalid_org_returns_false(self, app):
        with app.app_context():
            with patch(
                "application.blueprints.datamanager.services.organisation.requests.get"
            ) as mock_get:
                mock_get.return_value = _make_org_mapping_response(ORG_ROWS)
                result = is_valid_organisation("local-authority:NONEXISTENT")

        assert result is False


class TestFormatOrgOptions:
    def test_returns_correct_code_and_label_dicts(self, app):
        with app.app_context():
            with patch(
                "application.blueprints.datamanager.services.organisation.requests.get"
            ) as mock_get:
                mock_get.return_value = _make_org_mapping_response(ORG_ROWS)
                result = format_org_options(
                    ["local-authority:ABC", "local-authority:DEF"]
                )

        assert result == [
            {
                "code": "local-authority:ABC",
                "label": "ABC Council (local-authority:ABC)",
            },
            {
                "code": "local-authority:DEF",
                "label": "DEF Borough (local-authority:DEF)",
            },
        ]

    def test_unknown_code_falls_back_to_code_in_label(self, app):
        with app.app_context():
            with patch(
                "application.blueprints.datamanager.services.organisation.requests.get"
            ) as mock_get:
                mock_get.return_value = _make_org_mapping_response(ORG_ROWS)
                result = format_org_options(["local-authority:UNKNOWN"])

        assert result == [
            {
                "code": "local-authority:UNKNOWN",
                "label": "local-authority:UNKNOWN (local-authority:UNKNOWN)",
            }
        ]

    def test_empty_list_returns_empty_list(self, app):
        with app.app_context():
            with patch(
                "application.blueprints.datamanager.services.organisation.requests.get"
            ) as mock_get:
                mock_get.return_value = _make_org_mapping_response(ORG_ROWS)
                result = format_org_options([])

        assert result == []
