from unittest.mock import patch

PENDING_RESULT = {
    "status": "PENDING",
    "response": None,
    "params": {
        "organisationName": "local-authority-eng:ABC",
        "dataset": "brownfield-land",
    },
}


class TestCheckResultsRoute:
    def test_renders_loading_template_when_pending(self, client):
        with patch(
            "application.blueprints.datamanager.router.fetch_request",
            return_value=PENDING_RESULT,
        ):
            with patch(
                "application.blueprints.datamanager.controllers.check.get_organisation_name",
                return_value="Test Org",
            ):
                with patch(
                    "application.blueprints.datamanager.controllers.check.get_dataset_name",
                    return_value="Brownfield Land",
                ):
                    response = client.get("/datamanager/check-results/test-id")
        assert response.status_code == 200
        assert b"loading" in response.data.lower() or b"check" in response.data.lower()

    def test_returns_error_when_org_code_missing(self, client):
        result_no_org = {**PENDING_RESULT, "params": {}}
        with patch(
            "application.blueprints.datamanager.router.fetch_request",
            return_value=result_no_org,
        ):
            response = client.get("/datamanager/check-results/test-id")
        assert response.status_code == 200
        assert b"govuk-error-summary" in response.data

    def test_resubmit_redirects_to_new_check(self, client):
        result = {
            **PENDING_RESULT,
            "params": {
                **PENDING_RESULT["params"],
                "column_mapping": {"OldColumn": "name"},
            },
        }
        with patch(
            "application.blueprints.datamanager.controllers.check.fetch_request",
            return_value=result,
        ), patch(
            "application.blueprints.datamanager.controllers.check.submit_request",
            return_value="new-check-id",
        ) as submit_request:
            response = client.post(
                "/datamanager/check-results/test-id",
                data={"field_map[name]": "MyColumn"},
            )
        assert response.status_code == 302
        assert "new-check-id" in response.headers["Location"]
        submitted_params = submit_request.call_args.args[0]
        assert submitted_params["column_mapping"] == {"MyColumn": "name"}
