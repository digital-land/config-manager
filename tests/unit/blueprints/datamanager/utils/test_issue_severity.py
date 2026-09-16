import pytest

from application.blueprints.datamanager.controllers.transform import (
    _build_issue_log_table,
)
from application.blueprints.datamanager.utils import build_check_tables


@pytest.mark.parametrize("page", ["check", "transform"])
@pytest.mark.parametrize(
    "severity, colour",
    [("critical", "red"), ("error", "yellow"), ("CRITICAL", "red"), ("warning", None)],
)
def test_issue_table_severity_badges(app, page, severity, colour):
    details = [{"issue_logs": [{"severity": severity}]}]
    table = (
        build_check_tables([], details)[2]
        if page == "check"
        else _build_issue_log_table(details)
    )
    with app.app_context():
        html = app.jinja_env.from_string(
            '{% from "components/table.html" import table %}{{ table(data) }}'
        ).render(data=table)
    assert severity in html
    if colour:
        assert f'class="govuk-tag govuk-tag--{colour}"' in html
    else:
        assert "govuk-tag" not in html
