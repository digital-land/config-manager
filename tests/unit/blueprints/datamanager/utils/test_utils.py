from unittest.mock import patch, Mock

from application.blueprints.datamanager.utils import (
    get_spec_fields_union,
    order_table_fields,
    read_raw_csv_preview,
)


class TestOrderTableFields:
    def test_reference_and_name_are_promoted_to_front(self):
        all_fields = [
            "name",
            "reference",
            "address",
            "postcode",
            "created_date",
            "updated_date",
        ]
        expected = [
            "reference",
            "name",
            "address",
            "postcode",
            "created_date",
            "updated_date",
        ]
        assert order_table_fields(all_fields) == expected

    def test_only_reference_field(self):
        assert order_table_fields(["address", "reference", "postcode"]) == [
            "reference",
            "address",
            "postcode",
        ]

    def test_only_name_field(self):
        assert order_table_fields(["address", "name", "postcode"]) == [
            "name",
            "address",
            "postcode",
        ]

    def test_no_reference_or_name(self):
        all_fields = ["address", "postcode", "geometry"]
        assert order_table_fields(all_fields) == all_fields

    def test_case_insensitive_matching(self):
        all_fields = ["Address", "REFERENCE", "Name", "postcode"]
        expected = ["REFERENCE", "Name", "Address", "postcode"]
        assert order_table_fields(all_fields) == expected

    def test_only_exact_match_promoted(self):
        all_fields = ["reference", "name", "other_reference", "display_name"]
        expected = ["reference", "name", "other_reference", "display_name"]
        assert order_table_fields(all_fields) == expected


class TestReadRawCsvPreview:
    def test_success(self):
        csv_content = "header1,header2\nvalue1,value2\nvalue3,value4"
        mock_response = Mock()
        mock_response.content = csv_content.encode("utf-8")
        with patch(
            "application.blueprints.datamanager.utils.requests.get",
            return_value=mock_response,
        ):
            headers, rows = read_raw_csv_preview("http://example.com/test.csv")
        assert headers == ["header1", "header2"]
        assert rows == [["value1", "value2"], ["value3", "value4"]]

    def test_empty_url_returns_empty(self):
        headers, rows = read_raw_csv_preview("")
        assert headers == []
        assert rows == []

    def test_strips_bom(self):
        csv_content = "\ufeffheader1,header2\nvalue1,value2"
        mock_response = Mock()
        mock_response.content = csv_content.encode("utf-8")
        with patch(
            "application.blueprints.datamanager.utils.requests.get",
            return_value=mock_response,
        ):
            headers, rows = read_raw_csv_preview("http://example.com/test.csv")
        assert headers == ["header1", "header2"]

    def test_respects_max_rows(self):
        csv_content = "header1,header2\n" + "\n".join(f"v{i},v{i}" for i in range(100))
        mock_response = Mock()
        mock_response.content = csv_content.encode("utf-8")
        with patch(
            "application.blueprints.datamanager.utils.requests.get",
            return_value=mock_response,
        ):
            _, rows = read_raw_csv_preview("http://example.com/test.csv", max_rows=5)
        assert len(rows) == 5

    def test_request_failure_returns_empty(self):
        with patch(
            "application.blueprints.datamanager.utils.requests.get",
            side_effect=Exception("timeout"),
        ):
            headers, rows = read_raw_csv_preview("http://example.com/test.csv")
        assert headers == []
        assert rows == []


class TestGetSpecFieldsUnion:
    def test_returns_fields_sorted(self, app):
        mock_response = Mock()
        mock_response.json.return_value = [
            {"field": "ZField"},
            {"field": "AField"},
            {"field": "MField"},
        ]
        mock_response.raise_for_status.return_value = None
        with app.app_context():
            with patch(
                "application.blueprints.datamanager.utils.requests.get",
                return_value=mock_response,
            ):
                result = get_spec_fields_union("test-dataset")
        assert result == sorted(result, key=str.lower)

    def test_no_dataset_makes_single_request(self, app):
        mock_response = Mock()
        mock_response.json.return_value = [{"field": "global_field"}]
        mock_response.raise_for_status.return_value = None
        with app.app_context():
            with patch(
                "application.blueprints.datamanager.utils.requests.get",
                return_value=mock_response,
            ) as mock_get:
                result = get_spec_fields_union(None)
        assert "global_field" in result
        assert mock_get.call_count == 1

    def test_request_failure_returns_empty(self, app):
        with app.app_context():
            with patch(
                "application.blueprints.datamanager.utils.requests.get",
                side_effect=Exception("Network error"),
            ):
                result = get_spec_fields_union("test-dataset")
        assert result == []
