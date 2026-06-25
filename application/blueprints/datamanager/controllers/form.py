import csv
import logging
import re
from datetime import date, datetime
from io import StringIO

from flask import (
    current_app,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

from ..services.async_api import (
    AsyncAPIError,
    fetch_request,
    submit_request,
)

from ..services.dataset import (
    get_collection_id,
    get_dataset_id,
    get_dataset_name,
    search_datasets,
)
from ..services.organisation import (
    format_org_options,
    get_provision_orgs_for_dataset,
    is_valid_organisation,
)

logger = logging.getLogger(__name__)


def _is_admin():
    if not current_app.config.get("AUTHENTICATION_ON", True):
        return True
    return bool((session.get("user") or {}).get("is_admin"))


def _organisation_display_name(org_code: str, org_values: list) -> str:
    for org in org_values:
        if org.get("code") == org_code:
            return org.get("label", org_code)

    return org_code


def _get_org_values_for_dataset(dataset_id: str) -> list:
    try:
        org_codes = get_provision_orgs_for_dataset(dataset_id)
        return format_org_options(org_codes)
    except Exception as e:
        logger.warning(f"Failed to fetch organisations for {dataset_id}: {e}")
        return []


def handle_dashboard_get():
    form = {}
    errors = {}
    org_values = []
    dataset_input = ""
    request_id = ""

    # Autocomplete dataset names for UI suggestions
    if request.args.get("autocomplete"):
        query = request.args["autocomplete"]
        return jsonify(search_datasets(query))

    # Autocomplete all orgs provisioned against a dataset name (for UI suggestions)
    if request.args.get("get_orgs_for"):
        dataset_name = request.args["get_orgs_for"]
        dataset_id = get_dataset_id(dataset_name)
        if not dataset_id:
            return jsonify([])

        try:
            org_codes = get_provision_orgs_for_dataset(dataset_id)
            org_values = format_org_options(org_codes)
        except Exception as e:
            logger.error(f"Failed to fetch organisations: {e}")
            return jsonify([])
        return jsonify(org_values)

    # Pre-fill form from async request details when a request ID is provided.
    if request.args.get("import_data") != "true" and request.args.get("requestId"):
        request_id = request.args.get("requestId", "")
        request_params = {}
        try:
            request_params = fetch_request(request_id).get("params", {})
        except AsyncAPIError as e:
            logger.warning(f"Could not fetch request details for {request_id}: {e}")
            request_id = ""

        if request_params:
            dataset_param = request_params.get("dataset", "")
            dataset_input = get_dataset_name(dataset_param, default=dataset_param)
            dataset_id = get_dataset_id(dataset_input) if dataset_input else ""

            org_value = request_params.get("organisationName", "")
            endpoint_url = request_params.get("url", "")
            doc_url = request.args.get("documentationUrl", "")
            geom_type = (request_params.get("geom_type") or "").strip()

            if dataset_id:
                org_values = _get_org_values_for_dataset(dataset_id)

            form = {
                "dataset": dataset_input,
                "organisation": org_value,
                "organisation_display_name": _organisation_display_name(
                    org_value, org_values
                ),
                "endpoint_url": endpoint_url,
                "documentation_url": doc_url,
                "geom_type": geom_type,
            }

    # Pre-fill form from imported CSV data if available
    elif request.args.get("import_data") == "true":
        csv_dataset_id = request.args.get("dataset", "")
        dataset_input = get_dataset_name(csv_dataset_id, default=csv_dataset_id)
        dataset_id = csv_dataset_id

        org_value = request.args.get("organisation", "")
        if dataset_id:
            org_values = _get_org_values_for_dataset(dataset_id)

        form = {
            "dataset": dataset_input,
            "organisation": org_value,
            "endpoint_url": request.args.get("endpoint_url", ""),
            "documentation_url": request.args.get("documentation_url", ""),
            "licence": request.args.get("licence", ""),
        }

        start_date = request.args.get("start_date", "")
        if start_date:
            try:
                dt = datetime.fromisoformat(start_date)
                form["start_day"] = str(dt.day)
                form["start_month"] = str(dt.month)
                form["start_year"] = str(dt.year)
            except Exception:
                pass

        session.pop("import_csv_data", None)

    if (
        not form.get("start_day")
        and not form.get("start_month")
        and not form.get("start_year")
    ):
        today = date.today()
        form["start_day"] = str(today.day)
        form["start_month"] = str(today.month)
        form["start_year"] = str(today.year)

    return render_template(
        "datamanager/dashboard_add.html",
        dataset_input=dataset_input,
        org_values=org_values,
        form=form,
        errors=errors,
        is_admin=_is_admin(),
        request_id=request_id,
        is_magic_link_prefilled=bool(request_id),
    )


# Handle form submission in POST route, TODO: consider schema validation module for form here
def handle_dashboard_add():
    form = request.form.to_dict()
    errors = {}
    request_id = form.get("request_id", "").strip()

    # Set the the dataset id and collection id.
    dataset_input = form.get("dataset", "").strip()
    dataset_id = get_dataset_id(dataset_input)
    collection_id = get_collection_id(dataset_input)

    # Get list of orgs provisioned for this dataset in both code and display format.
    org_values = []
    if dataset_id:
        org_values = _get_org_values_for_dataset(dataset_id)

    submitted_org_code = form.get("organisation", "").strip()
    org_code_input = submitted_org_code
    endpoint_url = form.get("endpoint_url", "").strip()
    doc_url = form.get("documentation_url", "").strip()
    licence = (form.get("licence") or "ogl3").strip().lower()
    authoritative = form.get("authoritative", "").strip().lower() or None
    endpoint_parameters = {}
    max_page_size_raw = form.get("max_page_size", "").strip()
    page_size_raw = form.get("page_size", "").strip()
    if max_page_size_raw.isdigit():
        endpoint_parameters["max_page_size"] = int(max_page_size_raw)
    if page_size_raw.isdigit():
        endpoint_parameters["page_size"] = int(page_size_raw)

    day = (form.get("start_day") or "").strip()
    month = (form.get("start_month") or "").strip()
    year = (form.get("start_year") or "").strip()
    start_date_str = None

    if all([day, month, year]):
        try:
            start_date_str = date(int(year), int(month), int(day)).isoformat()
        except (ValueError, TypeError):
            errors["start_date"] = True
    elif any([day, month, year]):
        errors["start_date"] = True

    org_warning = form.get("org_warning", "false") == "true"

    # Validate organisation input code
    org_code_input = org_code_input if is_valid_organisation(org_code_input) else None
    org_codes_set = {o["code"] for o in org_values} if org_values else set()

    # Core required fields - check for errors
    errors.update(
        {
            "dataset": not dataset_input,
            "organisation": (
                org_warning
                or (org_codes_set and org_code_input not in org_codes_set)
                or not org_code_input
            ),
            "endpoint_url": (
                not endpoint_url or not re.match(r"https?://[^\s]+", endpoint_url)
            ),
            "authoritative": authoritative not in ("yes", "no"),
        }
    )

    # Submit Data and set session with add data variables
    geom_type = form.get("geom_type", "").strip() or None

    if not any(errors.values()):
        column_mapping = {"WKT": "geometry"} if geom_type == "polygon" else {}
        payload = {
            "params": {
                "type": "check_url",
                "collection": collection_id,
                "dataset": dataset_id,
                "url": endpoint_url,
                "organisationName": org_code_input,
                "geom_type": geom_type,
                "column_mapping": column_mapping or None,
                "endpoint_parameters": endpoint_parameters or None,
            }
        }
        session["add_data_fields"] = {
            "documentation_url": doc_url,
            "licence": licence,
            "start_date": start_date_str,
            "column_mapping": {},
            "authoritative": authoritative == "yes",
            "github_new": form.get("github_new", "true").strip() != "false",
            "endpoint_parameters": endpoint_parameters,
        }

        try:
            if request_id:
                logger.info(f"Reusing request ID: {request_id}")
            else:
                logger.info("Creating a new check request")
                request_id = submit_request(payload["params"])
            return redirect(
                url_for(
                    "datamanager.check_results",
                    request_id=request_id,
                )
            )
        except AsyncAPIError as e:
            raise Exception(f"Check tool submission failed: {e.detail}") from e

    # Re-render form with errors
    if request_id:
        form["organisation_display_name"] = _organisation_display_name(
            submitted_org_code, org_values
        )

    return render_template(
        "datamanager/dashboard_add.html",
        dataset_input=dataset_input,
        org_values=org_values,
        form=form,
        errors=errors,
        is_admin=_is_admin(),
        is_magic_link_prefilled=bool(request_id),
        request_id=request_id,
    )


def handle_dashboard_add_import():
    """
    Handle import of endpoint configuration from CSV.
    User pastes CSV data, validates it, then redirects to dashboard with pre-filled form.
    """
    errors = {}
    csv_data = ""

    if request.method == "POST":
        mode = request.form.get("mode", "").strip()

        uploaded_file = request.files.get("csv_file")
        if uploaded_file and uploaded_file.filename:
            try:
                csv_data = uploaded_file.read().decode("utf-8").strip()
            except Exception as e:
                errors["csv_data"] = f"Could not read uploaded file: {str(e)}"
        else:
            csv_data = request.form.get("csv_data", "").strip()

        if mode == "parse" and not errors:
            try:
                reader = csv.DictReader(StringIO(csv_data))
                rows = list(reader)

                if not rows:
                    errors["csv_data"] = "No data found in CSV"
                elif len(rows) > 1:
                    errors["csv_data"] = "CSV should contain only one row of data"
                else:
                    parsed_data = rows[0]
                    required_fields = ["organisation", "pipelines", "endpoint-url"]
                    missing = [
                        f for f in required_fields if not parsed_data.get(f, "").strip()
                    ]
                    if missing:
                        errors["csv_data"] = (
                            f"Missing required fields: {', '.join(missing)}"
                        )

                    if not errors:
                        return redirect(
                            url_for(
                                "datamanager.dashboard_get",
                                import_data="true",
                                dataset=parsed_data.get("pipelines", ""),
                                organisation=parsed_data.get("organisation", ""),
                                endpoint_url=parsed_data.get("endpoint-url", ""),
                                documentation_url=parsed_data.get(
                                    "documentation-url", ""
                                ),
                                start_date=parsed_data.get("start-date", ""),
                                plugin=parsed_data.get("plugin", ""),
                                licence=parsed_data.get("licence", ""),
                            )
                        )

            except Exception as e:
                errors["csv_data"] = f"Invalid CSV format: {str(e)}"

    return render_template(
        "datamanager/dashboard_add_import.html", csv_data=csv_data, errors=errors
    )


def _submit_add_data_preview(request_id, add_data_fields):
    """Submit an add_data async request and redirect to check-transform."""
    check_req = fetch_request(request_id)
    check_params = check_req.get("params", {})

    params = {
        "type": "add_data",
        "preview": True,
        "collection": check_params.get("collection"),
        "dataset": check_params.get("dataset"),
        "url": check_params.get("url"),
        "organisationName": check_params.get("organisationName"),
        "organisation": check_params.get("organisationName"),
        "column_mapping": check_params.get("column_mapping", {}),
        "documentation_url": add_data_fields["documentation_url"],
        "licence": add_data_fields["licence"],
        "start_date": add_data_fields["start_date"],
        "authoritative": add_data_fields["authoritative"],
        "geom_type": check_params.get("geom_type"),
        "github_branch": (
            current_app.config.get("CONFIG_REPO_BRANCH") or None
            if not add_data_fields.get("github_new", True)
            else None
        ),
        "endpoint_parameters": check_params.get("endpoint_parameters") or None,
    }

    preview_id = submit_request(params)
    return redirect(url_for("datamanager.check_transform", request_id=preview_id))


def _has_all_add_data_fields(add_data_fields):
    """Check whether all required add_data fields are present in session."""
    return (
        add_data_fields.get("documentation_url")
        and add_data_fields.get("licence")
        and add_data_fields.get("start_date")
        and add_data_fields.get("authoritative") is not None
        and add_data_fields.get("github_new") is not None
    )


def _parse_start_date(start_date_str):
    """Split an ISO date string into start_day, start_month, start_year form fields."""
    if not start_date_str:
        return {}
    try:
        d = date.fromisoformat(start_date_str)
        return {
            "start_day": str(d.day),
            "start_month": str(d.month),
            "start_year": str(d.year),
        }
    except (ValueError, TypeError):
        return {}


def handle_add_data(request_id):
    """
    This will only show a form if required fields for add_data are not already in session.
    Otherwise it will submit directly.
    This is to allow for future use where the check tool can be jumped directly in.
    """
    add_data_fields = session.get("add_data_fields", {})

    # If all fields already set in session, submit directly — no form needed
    if _has_all_add_data_fields(add_data_fields):
        return _submit_add_data_preview(request_id, add_data_fields)

    # GET — show the form pre-filled with whatever we have
    if request.method == "GET":
        return render_template(
            "datamanager/add-data.html",
            request_id=request_id,
            form={
                "documentation_url": add_data_fields.get("documentation_url", ""),
                "licence": add_data_fields.get("licence", ""),
                "authoritative": add_data_fields.get("authoritative"),
                "github_new": (
                    "false" if add_data_fields.get("github_new") is False else "true"
                ),
                **(_parse_start_date(add_data_fields.get("start_date"))),
            },
        )

    # POST — validate the form submission
    form = request.form.to_dict()
    doc_url = form.get("documentation_url", "").strip()
    licence = form.get("licence", "").strip()
    authoritative = form.get("authoritative", "").strip().lower() or None
    github_new_raw = form.get("github_new", "").strip()

    d = (form.get("start_day") or "").strip()
    m = (form.get("start_month") or "").strip()
    y = (form.get("start_year") or "").strip()
    start_date = f"{y}-{m.zfill(2)}-{d.zfill(2)}" if (d and m and y) else ""

    errors = {}
    if authoritative not in ("yes", "no"):
        errors["authoritative"] = True
    if not doc_url:
        errors["documentation_url"] = True
    if not start_date:
        errors["start_date"] = True
    if github_new_raw not in ("true", "false"):
        errors["github_new"] = True

    if errors:
        return render_template(
            "datamanager/add-data.html", request_id=request_id, form=form, errors=errors
        )

    # Save to session and submit
    add_data_fields = {
        "documentation_url": doc_url,
        "licence": licence,
        "start_date": start_date,
        "column_mapping": session.get("add_data_fields", {}).get("column_mapping", {}),
        "authoritative": authoritative == "yes",
        "github_new": github_new_raw != "false",
    }
    session["add_data_fields"] = add_data_fields

    return _submit_add_data_preview(request_id, add_data_fields)
