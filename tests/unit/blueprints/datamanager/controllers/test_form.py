from application.blueprints.datamanager.controllers.form import _has_all_add_data_fields


class TestHasAllAddDataFields:
    def test_returns_true_when_all_fields_present(self):
        fields = {
            "documentation_url": "https://example.gov.uk/docs",
            "licence": "ogl3",
            "start_date": "2024-01-01",
            "authoritative": True,
        }
        assert _has_all_add_data_fields(fields) is True

    def test_returns_false_when_documentation_url_missing(self):
        fields = {
            "documentation_url": "",
            "licence": "ogl3",
            "start_date": "2024-01-01",
            "authoritative": True,
        }
        assert not _has_all_add_data_fields(fields)

    def test_returns_false_when_authoritative_is_none(self):
        fields = {
            "documentation_url": "https://example.gov.uk/docs",
            "licence": "ogl3",
            "start_date": "2024-01-01",
            "authoritative": None,
        }
        assert not _has_all_add_data_fields(fields)
