import json
import logging
import os
import re
import traceback
from datetime import datetime
from io import StringIO

import requests
import csv
from flask import (
    Blueprint,
    current_app,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
    session,
)

from application.utils import get_request_api_endpoint
from application.services.github import (
    trigger_add_data_workflow,
    trigger_add_data_async_workflow,
    GitHubWorkflowError,
)
from shapely import wkt
from shapely.geometry import mapping

from .utils import (
    REQUESTS_TIMEOUT,
    build_column_csv_preview,
    build_endpoint_csv_preview,
    build_entity_organisation_csv,
    build_lookup_csv_preview,
    build_source_csv_preview,
    fetch_all_response_details,
    get_organisation_code_mapping,
    get_provision_orgs_for_dataset,
    handle_error,
    inject_now,
    order_table_fields,
    read_raw_csv_preview,
)

datamanager_bp = Blueprint("datamanager", __name__, url_prefix="/datamanager")
logger = logging.getLogger(__name__)
headers = {"Content-Type": "application/json", "Accept": "application/json"}
planning_base_url = os.getenv("PLANNING_URL", "https://www.planning.data.gov.uk")

datamanager_bp.errorhandler(Exception)(handle_error)
datamanager_bp.context_processor(inject_now)


@datamanager_bp.before_request
def require_login():
    """Require login for all datamanager routes"""
    if current_app.config.get("AUTHENTICATION_ON", True):
        if session.get("user") is None:
            return redirect(url_for("auth.login", next=request.url))


@datamanager_bp.route("/")
def index():
    return render_template("datamanager/index.html", datamanager={"name": "Dashboard"})


@datamanager_bp.route("/add/import", methods=["GET", "POST"])
def dashboard_add_import():
    """
    Route to import endpoint configuration from CSV. (usually created by provide service)
    User pastes CSV data, confirms it, then redirects to dashboard_add with pre-filled form.
    """
    errors = {}
    csv_data = ""
    parsed_data = None

    if request.method == "POST":
        mode = request.form.get("mode", "").strip()
        csv_data = request.form.get("csv_data", "").strip()

        if mode == "parse":
            # Parse the CSV
            try:
                reader = csv.DictReader(StringIO(csv_data))
                rows = list(reader)

                if not rows:
                    errors["csv_data"] = "No data found in CSV"
                elif len(rows) > 1:
                    errors["csv_data"] = "CSV should contain only one row of data"
                else:
                    parsed_data = rows[0]
                    # Validate required fields
                    required_fields = ["organisation", "pipelines", "endpoint-url"]
                    missing = [
                        f for f in required_fields if not parsed_data.get(f, "").strip()
                    ]
                    if missing:
                        errors["csv_data"] = (
                            f"Missing required fields: {', '.join(missing)}"
                        )

                    if not errors:
                        # Redirect directly to dashboard_add with query params to pre-fill the form
                        return redirect(
                            url_for(
                                "datamanager.dashboard_add",
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


# TODO: This function needs to be broken up, why are querying and form handling in the same function?
@datamanager_bp.route("/add", methods=["GET", "POST"])
def dashboard_add():
    planning_url = f"{planning_base_url}/dataset.json?_labels=on&_size=max"
    try:
        ds_response = requests.get(
            planning_url,
            timeout=REQUESTS_TIMEOUT,
            headers={"User-Agent": "Planning Data - Manage"},
        ).json()
    except Exception as e:
        logger.exception("Error fetching datasets")
        raise Exception("Failed to fetch dataset list") from e

    # only datasets that have a collection
    datasets = [d for d in ds_response.get("datasets", []) if "collection" in d]
    dataset_options = sorted([d["name"] for d in datasets])

    # 🔧 build BOTH maps (forward and reverse)
    name_to_dataset_id = {d["name"]: d["dataset"] for d in datasets}
    name_to_collection_id = {d["name"]: d["collection"] for d in datasets}
    dataset_id_to_name = {d["dataset"]: d["name"] for d in datasets}  # reverse lookup

    # autocomplete dataset options
    if request.args.get("autocomplete"):
        query = request.args["autocomplete"].lower()
        matches = [name for name in dataset_options if query in name.lower()]
        return jsonify(matches[:10])

    # fetch orgs for a dataset name (for UI suggestions)
    if request.args.get("get_orgs_for"):
        dataset_name = request.args["get_orgs_for"]
        dataset_id = name_to_dataset_id.get(dataset_name)
        if not dataset_id:
            return jsonify([])

        try:
            org_codes = get_provision_orgs_for_dataset(dataset_id)
            org_code_mapping = get_organisation_code_mapping()
            # Format codes with names for display: "Name (CODE)"
            selected_orgs = [
                f"{org_code_mapping.get(code, code)} ({code})" for code in org_codes
            ]
        except Exception as e:
            logger.error(f"Failed to fetch organisations: {e}")
            return jsonify([])
        return jsonify(selected_orgs)

    form = {}
    errors = {}
    selected_orgs = []
    dataset_input = ""
    dataset_id = None
    collection_id = None

    # Pre-fill form from imported CSV data if available
    if request.args.get("import_data") == "true":
        # CSV contains dataset ID (e.g., "conservation-area-document")
        # but form needs dataset NAME (display name)
        csv_dataset_id = request.args.get("dataset", "")
        dataset_input = dataset_id_to_name.get(
            csv_dataset_id, csv_dataset_id
        )  # convert ID to name
        dataset_id = csv_dataset_id
        collection_id = name_to_collection_id.get(dataset_input)

        org_value = request.args.get("organisation", "")  # e.g., "local-authority:FOE"

        # Fetch org list for this dataset to convert org_value to display format
        org_display = org_value  # fallback
        if dataset_id:
            org_codes = get_provision_orgs_for_dataset(dataset_id)
            org_code_mapping = get_organisation_code_mapping()
            # Format codes with names for display: "Name (CODE)"
            selected_orgs = [
                f"{org_code_mapping.get(code, code)} ({code})" for code in org_codes
            ]
            # If org_value is a code, look up its display name
            if org_value in org_code_mapping:
                org_display = f"{org_code_mapping[org_value]} ({org_value})"

        form = {
            "dataset": dataset_input,  # dataset NAME for display
            "organisation": org_display,
            "endpoint_url": request.args.get("endpoint_url", ""),
            "documentation_url": request.args.get("documentation_url", ""),
            "licence": request.args.get("licence", ""),
        }

        # Parse start_date if provided
        start_date = request.args.get("start_date", "")
        if start_date:
            try:
                dt = datetime.fromisoformat(start_date)
                form["start_day"] = str(dt.day)
                form["start_month"] = str(dt.month)
                form["start_year"] = str(dt.year)
            except Exception:
                pass

        # Clear import data from session
        session.pop("import_csv_data", None)

    if request.method == "POST":
        form = request.form.to_dict()
        logger.debug("Received form POST data:")
        logger.debug(json.dumps(form, indent=2))

        mode = form.get("mode", "").strip()
        dataset_input = form.get("dataset", "").strip()
        dataset_id = name_to_dataset_id.get(dataset_input)
        collection_id = name_to_collection_id.get(dataset_input)

        # Preload org list + build a reverse map we'll use on submit
        org_codes = []
        org_code_mapping = {}
        if dataset_id:
            org_codes = get_provision_orgs_for_dataset(dataset_id)
            org_code_mapping = get_organisation_code_mapping()
            # Format codes with names for display: "Name (CODE)"
            selected_orgs = [
                f"{org_code_mapping.get(code, code)} ({code})" for code in org_codes
            ]
        else:
            selected_orgs = []

        # TODO: This feels wrong as a practice for form completion
        if mode == "final":
            # what the user submitted from the select/input
            org_input = (form.get("organisation") or "").strip()
            endpoint_url = form.get("endpoint_url", "").strip()
            doc_url = form.get("documentation_url", "").strip()

            # ✅ licence defaults to 'ogl' if blank
            licence = (form.get("licence") or "ogl").strip().lower()

            authoritative_raw = form.get("authoritative", "").strip()
            if authoritative_raw:
                authoritative = authoritative_raw.lower() in ("true", "yes", "1")
            else:
                authoritative = None

            # ✅ start_date defaults to today if user leaves all blank; partial dates still error
            day = (form.get("start_day") or "").strip()
            month = (form.get("start_month") or "").strip()
            year = (form.get("start_year") or "").strip()
            start_date_str = None
            if any([day, month, year]):
                if day and month and year:
                    try:
                        start_date_str = (
                            datetime(int(year), int(month), int(day)).date().isoformat()
                        )
                    except Exception:
                        errors["start_date"] = True
                else:
                    errors["start_date"] = True
            else:
                start_date_str = datetime.utcnow().date().isoformat()

            org_warning = form.get("org_warning", "false") == "true"

            # Extract code from org_input "Label (CODE)" and resolve using org_code_mapping
            org_value = None
            m = re.search(r"\(([^)]+)\)$", org_input)
            if m:
                code = m.group(1).strip()
                # Check if this code exists in the mapping
                if code in org_code_mapping:
                    org_value = code

            # Core required fields
            errors.update(
                {
                    "dataset": not dataset_input,
                    "organisation": (
                        org_warning
                        or (selected_orgs and org_input not in selected_orgs)
                        or not org_value
                    ),
                    "endpoint_url": (
                        not endpoint_url
                        or not re.match(r"https?://[^\s]+", endpoint_url)
                    ),
                    "authoritative": authoritative is None,
                }
            )
            # Optional fields validation (doc_url only if present)
            if doc_url and not re.match(
                r"^https?://[^\s/]+\.(gov\.uk|org\.uk)(/.*)?$", doc_url
            ):
                errors["documentation_url"] = True

            if not any(errors.values()):
                # Params are being patched here that won't work, like documentation_url ???
                payload = {
                    "params": {
                        "type": "check_url",
                        # ✅ send the correct ids
                        "collection": collection_id,
                        "dataset": dataset_id,
                        "url": endpoint_url,
                        # optional extras, but we now always send defaults
                        "documentation_url": doc_url or None,
                        "licence": licence,  # ✅ defaulted above
                        "start_date": start_date_str,  # ✅ defaulted above
                        # ✅ send the REAL entity reference (prefix:CODE) here
                        "organisation": org_value,
                        "organisationName": org_value,  # display name for UI
                        "authoritative": authoritative,
                    }
                }
                # TODO: Is this the best way to save data to then be used for add data task?
                session["required_fields"] = {
                    "collection": collection_id,
                    "dataset": dataset_id,
                    "url": endpoint_url,
                    "organisation": org_value,
                    "authoritative": authoritative,
                }
                session["optional_fields"] = {
                    "documentation_url": doc_url,
                    "licence": licence,
                    "start_date": start_date_str,
                    "column_mapping": {},
                }

                logger.info("Sending payload to request API:")
                logger.debug(json.dumps(payload, indent=2))

                try:
                    async_api = f"{get_request_api_endpoint()}/requests"
                    response = requests.post(
                        async_api, json=payload, timeout=REQUESTS_TIMEOUT
                    )

                    logger.info(f"request-api responded with {response.status_code}")
                    try:
                        logger.debug(json.dumps(response.json(), indent=2))
                    except Exception:
                        logger.debug((response.text or "")[:2000])

                    if response.status_code == 202:
                        request_id = response.json()["id"]
                        return redirect(
                            url_for(
                                "datamanager.check_results",
                                request_id=request_id,
                                organisation=org_value,  # pass value along
                            )
                        )
                    else:
                        try:
                            detail = response.json()
                        except Exception:
                            detail = response.text
                        raise Exception(
                            f"Check tool submission failed ({response.status_code}): {detail}"
                        )
                except Exception as e:
                    traceback.print_exc()
                    raise Exception(f"Backend error: {e}")

    return render_template(
        "datamanager/dashboard_add.html",
        dataset_input=dataset_input,
        selected_orgs=selected_orgs,
        form=form,
        errors=errors,
        dataset_options=dataset_options,
    )


@datamanager_bp.route("/check-results/<request_id>")
def check_results(request_id):
    try:
        async_api = get_request_api_endpoint()
        # organisation = request.args.get("organisation", "Your organisation")

        # Fetch summary
        response = requests.get(
            f"{async_api}/requests/{request_id}", timeout=REQUESTS_TIMEOUT
        )
        response.raise_for_status()
        if response.status_code != 200 or response.json().get("status") == "FAILED":
            return (
                render_template(
                    "datamanager/error.html", message="Check failed or not found"
                ),
                404,
            )

        result = response.json()
        organisation = result.get("params", {}).get(
            "organisationName", "Unknown organisation"
        )

        logger.info(
            f"Result status : {result.get('status')} for request_id: {request_id}"
        )

        result.setdefault("params", {}).setdefault("organisation", organisation)
        logger.info(f"result: {result}")

        # Still processing?
        if (
            result.get("status") in ["PENDING", "PROCESSING", "QUEUED"]
            or result.get("response") is None
        ):
            return render_template(
                "datamanager/check-results-loading.html", result=result
            )

        response_data = result.get("response")
        if not response_data or response_data.get("data") is None:
            error_msg = "No data returned from check"
            if response_data and response_data.get("error"):
                error_msg = response_data.get("error").get("errMsg", error_msg)
            return render_template(
                "datamanager/error.html",
                message=error_msg,
            )
        else:
            # Fetch all response details using multiple calls
            resp_details = fetch_all_response_details(async_api, request_id)
            logger.info(f"Environment check - async_api: {async_api}")
            logger.info(f"Environment check - request_id: {request_id}")
            logger.info(f"Environment check - resp_details type: {type(resp_details)}")
            logger.info(
                f"Environment check - resp_details length: {len(resp_details) if resp_details else 'None'}"
            )

            # TODO: This whole test is pointless?
            # Check if response-details endpoint exists and is accessible
            try:
                test_response = requests.get(
                    f"{async_api}/requests/{request_id}/response-details",
                    params={"offset": 0, "limit": 1},
                    timeout=REQUESTS_TIMEOUT,
                )
                logger.info(
                    f"Response-details endpoint test - Status: {test_response.status_code}"
                )
                logger.info(
                    f"Response-details endpoint test - Headers: {dict(test_response.headers)}"
                )
                if test_response.status_code == 200:
                    test_data = test_response.json()
                    logger.info(f"Response-details test data: {test_data}")
                else:
                    logger.error(
                        f"Response-details endpoint failed: {test_response.text}"
                    )
            except Exception as e:
                logger.error(f"Response-details endpoint test failed: {e}")

            total_rows = len(resp_details)

            # --- TRUST BACKEND ONLY ---
            data = (result.get("response") or {}).get("data") or {}
            entity_summary = data.get("pipeline-summary") or {}
            new_entities = data.get("new-entities") or []

            # exact lists as provided by BE
            existing_entities_list = data.get("existing-entities") or []

            # exact counts as provided by BE (fallback to 0 if missing)
            existing_count = int(entity_summary.get("existing-in-resource") or 0)
            new_count = int(entity_summary.get("new-in-resource") or 0)

            # Extract pipeline-issues if present
            pipeline_issues = entity_summary.get("pipeline-issues") or []
            logger.debug(f"pipeline-issues count: {len(pipeline_issues)}")

            logger.debug(f"BE data keys: {list(data.keys())}")
            logger.debug(f"pipeline-summary: {entity_summary}")
            logger.debug(f"existing-entities len: {len(existing_entities_list)}")

            # minimal message that only reflects BE numbers
            new_entities_message = (
                f"{existing_count} existing; {new_count} new (from backend)."
            )
            show_entities_link = new_count > 0
            logger.info(
                f"resp_details count: {len(resp_details) if resp_details else 0}"
            )
            if resp_details and len(resp_details) > 0:
                logger.debug(f"First resp_detail: {resp_details[0]}")
            else:
                logger.warning("No response details found for table building")

            # Geometry mapping - always use all data
            geometries = []
            logger.debug(f"Processing {len(resp_details)} rows for geometries")

            for row in resp_details:
                converted_row = row.get("converted_row") or {}
                transformed_row = row.get("transformed_row") or []

                # Find geometry from transformed_row
                geometry_entry = None
                if isinstance(transformed_row, list):
                    geometry_entry = next(
                        (
                            item
                            for item in transformed_row
                            if isinstance(item, dict)
                            and (
                                item.get("field") == "geometry"
                                or item.get("field") == "point"
                            )
                        ),
                        None,
                    )
                if geometry_entry and geometry_entry.get("value"):
                    try:
                        shapely_geom = wkt.loads(geometry_entry["value"])
                        geom = mapping(shapely_geom)
                        geometries.append(
                            {
                                "type": "Feature",
                                "geometry": geom,
                                "properties": {
                                    "reference": converted_row.get("reference")
                                    or converted_row.get("Reference")
                                    or f"Entry {row.get('entry_number')}",
                                    "name": converted_row.get("name", ""),
                                },
                            }
                        )
                    except Exception as e:
                        logger.warning(
                            f"Error parsing geometry for entry {row.get('entry_number')}: {e}"
                        )
                        continue
            logger.debug(f"Found {len(geometries)} geometries")
            # Generate boundary URL dynamically based on dataset and geometries
            boundary_geojson_url = ""
            # Get organisation from result params, fallback to request args
            org_from_params = result.get("params", {}).get("organisation", organisation)
            logger.info(f"Organisation from params: {org_from_params}")
            try:
                if ":" in org_from_params:
                    dataset_id, lpa_id = org_from_params.split(":", 1)
                    entity_url = f"{planning_base_url}/entity.json"
                    url = f"{entity_url}?dataset={dataset_id}&reference={lpa_id}"
                    resp = requests.get(url)
                    resp.raise_for_status()
                    d = resp.json()
                    logger.info(f"Entity data response: {d}")
                    entity = (
                        d.get("entities", [])[0] if d and d.get("entities") else None
                    )
                    logger.info(f"Entity: {entity}")
                    if not entity:
                        boundary_geojson_url = {
                            "type": "FeatureCollection",
                            "features": [],
                        }
                    else:
                        reference = (
                            entity.get("local-planning-authority")
                            if entity.get("reference")
                            else ""
                        )
                        if not reference:
                            boundary_geojson_url = {
                                "type": "FeatureCollection",
                                "features": [],
                            }
                        else:
                            boundary_url = f"{planning_base_url}/entity.geojson?reference={reference}"
                            boundary_geojson_url = requests.get(boundary_url).json()
                else:
                    # If organisation format is not as expected, set empty boundary
                    boundary_geojson_url = {"type": "FeatureCollection", "features": []}
            except Exception as e:
                logger.warning(f"Failed to fetch boundary data: {e}")
                boundary_geojson_url = {"type": "FeatureCollection", "features": []}
            # Error parsing (unchanged)
            logger.debug(f"data: {data}")
            error_summary = data.get("error-summary", []) or []
            column_field_log = data.get("column-field-log", []) or []

            # ---- Table build with leadingFields and trailingFields support ----
            logger.info(
                f"Table build - Starting with {len(resp_details) if resp_details else 0} response details"
            )
            table_headers, formatted_rows = [], []
            if resp_details and len(resp_details) > 0:
                logger.info(
                    f"Table build - Processing {len(resp_details)} response details"
                )
                first_row = (resp_details[0] or {}).get("converted_row", {}) or {}
                logger.info(
                    f"Table build - First row keys: {list(first_row.keys()) if first_row else 'No keys'}"
                )
                logger.info(
                    f"Table build - First row sample: {dict(list(first_row.items())[:3]) if first_row else 'Empty'}"
                )

                if first_row:  # Only proceed if first_row has data
                    # Order fields using helper function
                    table_headers = order_table_fields(first_row.keys())
                    logger.info(f"Table build - Headers ordered: {table_headers}")

                    for row in resp_details:
                        converted = row.get("converted_row") or {}
                        if not all(
                            str(value).strip() == "" for value in converted.values()
                        ):
                            formatted_rows.append(
                                {
                                    "columns": {
                                        col: {"value": str(converted.get(col, ""))}
                                        for col in table_headers
                                    }
                                }
                            )
                else:
                    logger.error("Table build - First row is empty, cannot build table")
            else:
                logger.error(
                    f"Table build - No response details available. resp_details: {resp_details}"
                )

            table_params = {
                "columns": table_headers,
                "fields": table_headers,
                "rows": formatted_rows,
                "columnNameProcessing": "none",
            }
            logger.info(
                f"Table build - Final table_params: columns={len(table_headers)}, rows={len(formatted_rows)}"
            )
            logger.info(
                f"Table build - Sample table_params: {json.dumps(table_params, indent=2)}"
            )
            # ---- END Table build ----

            # checks
            must_fix, fixable, passed_checks = [], [], []
            for err in error_summary:
                fixable.append(err)
            for col in column_field_log:
                if not col.get("missing"):
                    passed_checks.append(f"All rows have {col.get('field')} present")
                else:
                    must_fix.append(f"Missing required field: {col.get('field')}")
            allow_add_data = len(must_fix) == 0

            params = result.get("params", {}) or {}
            optional_missing = not (
                params.get("documentation_url")
                and params.get("licence")
                and params.get("start_date")
            )
            show_error = not allow_add_data

            entities_preview_url = url_for(
                "datamanager.entities_preview", request_id=request_id
            )

            # compact preview (first 5) – exactly what BE sent for "new-entities"
            entity_preview_rows = []
            for e in (new_entities or [])[:5]:
                entity_preview_rows.append(
                    {
                        "reference": str(e.get("reference", "")).strip(),
                        "prefix": e.get("prefix") or "",
                        "organisation": e.get("organisation") or "",
                        "entity": str(e.get("entity", "")).strip(),
                    }
                )

            return render_template(
                "datamanager/check-results.html",
                result=result,
                geometries=geometries,
                must_fix=must_fix,
                fixable=fixable,
                passed_checks=passed_checks,
                allow_add_data=allow_add_data,
                show_error=show_error,
                optional_missing=optional_missing,
                table_params=table_params,
                total_rows=total_rows,
                page=1,
                page_size=total_rows,
                showing_start=1 if total_rows > 0 else 0,
                showing_end=total_rows,
                # entity summary bits (BE only):
                new_entities_message=new_entities_message,
                new_entity_count=new_count,
                existing_entity_count=existing_count,
                existing_entities_list=existing_entities_list,
                show_entities_link=show_entities_link,
                entities_preview_url=entities_preview_url,
                entity_preview_rows=entity_preview_rows,
                boundary_geojson_url=boundary_geojson_url,
                request_id=request_id,
                pipeline_issues=pipeline_issues,
            )

    except Exception as e:
        traceback.print_exc()
        raise Exception(f"Error fetching results from backend: {e}")


# # Check in case documentation_url, licence, start_date, are missing??? Not sure what this really does
# @datamanager_bp.route("/check-results/optional-submit", methods=["GET", "POST"])
# def optional_fields_submit():
#     form = request.form.to_dict()
#     request_id = form.get("request_id")
#     documentation_url = form.get("documentation_url", "").strip()
#     licence = form.get("licence", "").strip()
#     start_day = form.get("start_day", "").strip()
#     start_month = form.get("start_month", "").strip()
#     start_year = form.get("start_year", "").strip()

#     start_date = None
#     if start_day and start_month and start_year:
#         start_date = f"{start_year}-{start_month.zfill(2)}-{start_day.zfill(2)}"

#     # 🔹 Save in backend
#     async_api = get_request_api_endpoint()
#     # Again these params won't be saved?
#     payload = {
#         "params": {
#             "type": "check_url",
#             "documentation_url": documentation_url or None,
#             "licence": licence or None,
#             "start_date": start_date,
#         }
#     }
#     logger.info("Submitting optional fields to request API")
#     try:
#         requests.patch(
#             f"{async_api}/requests/{request_id}", json=payload, timeout=REQUESTS_TIMEOUT
#         )
#         logger.info(f"Successfully updated request {request_id} in request-api")
#         logger.debug(json.dumps(payload, indent=2))
#     except Exception as e:
#         logger.error(f"Failed to update request {request_id} in request-api: {e}")

#     return redirect(url_for("datamanager.add_data", request_id=request_id))


# Currently submits data for add data, and shows additional form for missed stuff
@datamanager_bp.route("/add-data", methods=["GET", "POST"])
def add_data():
    async_api = get_request_api_endpoint()

    # all optional fields from session (if any)
    existing_doc = session.get("optional_fields", {}).get("documentation_url")
    existing_lic = session.get("optional_fields", {}).get("licence")
    existing_start = session.get("optional_fields", {}).get("start_date")

    # authoritative from required fields (now required)
    existing_auth = session.get("required_fields", {}).get("authoritative")

    # TODO: Break this function up / out
    def _submit_preview(
        doc_url: str, licence: str, start_date: str, authoritative: bool
    ):
        # all required fields from session
        params = session.get("required_fields", {}).copy()
        logger.debug(f"Using required fields from session: {params}")

        # Include column mapping if it exists
        column_mapping = session.get("optional_fields", {}).get("column_mapping", {})
        logger.debug(f"Using column mapping from session: {column_mapping}")
        if column_mapping:
            params["column_mapping"] = column_mapping
            logger.info(
                f"Including column mapping in add_data preview: {column_mapping}"
            )

        params.update(
            {
                "type": "add_data",
                "preview": True,  # ← PREVIEW mode
                "documentation_url": doc_url,
                "licence": licence,
                "start_date": start_date,
                "authoritative": authoritative,
            }
        )
        try:
            logger.info("add_data Preview - outgoing payload:")
            logger.debug(json.dumps(params, indent=2))
        except Exception:
            pass
        r = requests.post(
            f"{async_api}/requests", json={"params": params}, timeout=REQUESTS_TIMEOUT
        )

        try:
            logger.info(f"add_data preview - request-api responded {r.status_code}")
        except Exception:
            pass

        if r.status_code == 202:
            preview_id = (r.json() or {}).get("id")
            return redirect(
                url_for("datamanager.entities_preview", request_id=preview_id)
            )
        detail = (
            r.json()
            if "application/json" in (r.headers.get("content-type") or "")
            else r.text
        )
        raise Exception(f"Preview submission failed ({r.status_code}): {detail}")

    if request.method == "GET":
        if (
            existing_doc
            and existing_lic
            and existing_start
            and (existing_auth is not None)
        ):
            # All required fields are present → use EXISTING payload to preview
            return _submit_preview(
                existing_doc, existing_lic, existing_start, existing_auth
            )

        # Pre-populate form with any existing data for the initial GET request
        form_data = {
            "licence": existing_lic,
            "documentation_url": existing_doc,
            "authoritative": existing_auth,
        }
        return render_template("datamanager/add-data.html", form=form_data)

    # POST – user submitted optional fields + authoritative
    form = request.form.to_dict()
    doc_url = (form.get("documentation_url") or existing_doc or "").strip()
    licence = (form.get("licence") or existing_lic or "").strip()
    authoritative_raw = form.get("authoritative") or existing_auth
    if not authoritative_raw or str(authoritative_raw).strip() == "":
        authoritative = None
    elif str(authoritative_raw).strip().lower() in ("true", "yes", "1"):
        authoritative = True
    else:
        authoritative = False

    d = (form.get("start_day") or "").strip()
    m = (form.get("start_month") or "").strip()
    y = (form.get("start_year") or "").strip()
    start_date = (
        f"{y}-{m.zfill(2)}-{d.zfill(2)}"
        if (d and m and y)
        else (existing_start or "").strip()
    )

    # Validate required fields
    errors = {}
    if authoritative is None:
        errors["authoritative"] = True

    if not (doc_url and licence and start_date) or errors:
        # Still missing something – re-show optional screen with errors
        return render_template("datamanager/add-data.html", form=form, errors=errors)

    # Remember locally (optional)
    session["optional_fields"] = {
        "documentation_url": doc_url,
        "licence": licence,
        "start_date": start_date,
    }

    # Update required_fields with authoritative value
    if "required_fields" not in session:
        session["required_fields"] = {}
    session["required_fields"]["authoritative"] = authoritative

    # Now preview with the UPDATED payload
    return _submit_preview(doc_url, licence, start_date, authoritative)


# --- Configure column screen ---
@datamanager_bp.route(
    "/check-results/<request_id>/configure-columns", methods=["GET", "POST"]
)
def configure_column_mapping(request_id):
    async_api = get_request_api_endpoint()

    # 1) Load the original request summary
    r = requests.get(f"{async_api}/requests/{request_id}", timeout=REQUESTS_TIMEOUT)
    if r.status_code != 200:
        return render_template("error.html", message="Original request not found"), 404

    req = r.json()
    params = req.get("params", {}) or {}
    organisation = params.get("organisationName", "Local Authority")
    dataset_id = params.get("dataset", "")
    print(organisation, dataset_id)
    source_url = params.get("url", "")

    # 2) Get column-field-log to understand current mapping
    data_blob = (req.get("response") or {}).get("data") or {}
    column_field_log = data_blob.get("column-field-log", []) or []

    # Build current mapping: column -> field
    mapping = {}
    for entry in column_field_log:
        col = entry.get("column")
        field = entry.get("field")
        if col and field:
            mapping[col] = field

    # 3) Get only missing fields for dropdown options (where missing: true)
    spec_fields = [
        entry.get("field")
        for entry in column_field_log
        if entry.get("missing") is True and entry.get("field")
    ]

    # 4) Get raw CSV headers and preview rows
    raw_headers, raw_rows = read_raw_csv_preview(source_url)

    # Build raw table params for display at top
    raw_table_params = {
        "columns": raw_headers,
        "fields": raw_headers,
        "rows": [
            {
                "columns": {
                    raw_headers[i]: {"value": row[i]} for i in range(len(raw_headers))
                }
            }
            for row in raw_rows
        ],
        "columnNameProcessing": "none",
    }

    # 5) Build display rows for mapping interface
    display_rows = []
    for header in raw_headers:
        mapped_field = mapping.get(header, "")
        display_rows.append(
            {
                "field": header,
                "mapped_to": mapped_field,
                "is_mapped": bool(mapped_field),
            }
        )

    # 6) Handle POST: user submitted new mappings
    if request.method == "POST":
        form = request.form.to_dict()
        new_mapping = {}

        for header in raw_headers:
            chosen_spec = (form.get(f"map[{header}]") or "").strip()
            if chosen_spec and chosen_spec != "__NOT_MAPPED__":
                new_mapping[header] = chosen_spec

        # Store the column mapping in optional_fields
        # Must reassign the dict for Flask to detect the change
        optional_fields = session.get("optional_fields", {})
        optional_fields["column_mapping"] = new_mapping
        session["optional_fields"] = optional_fields
        logger.info(f"Stored column mapping in session: {new_mapping}")

        # Get geom_type from form or fall back to params
        geom_type = (form.get("geom_type") or "").strip() or params.get("geom_type")

        # Submit new check with updated mapping
        payload = {
            "params": {
                "type": "check_url",
                "collection": params.get("collection"),
                "dataset": dataset_id,
                "url": source_url,
                "documentation_url": params.get("documentation_url"),
                "licence": params.get("licence"),
                "start_date": params.get("start_date"),
                "column_mapping": new_mapping or None,
                "geom_type": geom_type,
                "organisation": params.get("organisation"),
                "organisationName": organisation,
            }
        }

        logger.info("Re-check payload (configure):")
        logger.debug(json.dumps(payload, indent=2))

        try:
            new_req = requests.post(
                f"{async_api}/requests", json=payload, timeout=REQUESTS_TIMEOUT
            )
            if new_req.status_code == 202:
                new_id = new_req.json()["id"]
                return redirect(url_for("datamanager.check_results", request_id=new_id))
            else:
                detail = (
                    new_req.json()
                    if "application/json" in (new_req.headers.get("content-type") or "")
                    else new_req.text
                )
                return render_template(
                    "error.html",
                    message=f"Re-check submission failed ({new_req.status_code}): {detail}",
                )
        except Exception as e:
            traceback.print_exc()
            return render_template("error.html", message=f"Backend error: {e}")

    # 7) Render the configure page
    return render_template(
        "datamanager/configure.html",
        request_id=request_id,
        organisation=organisation,
        dataset=dataset_id,
        raw_table_params=raw_table_params,
        rows=display_rows,
        spec_options=spec_fields,
    )


@datamanager_bp.route("/add-data/progress/<request_id>")
def add_data_progress(request_id):
    # fallback message in case BE didn’t return one
    message = request.args.get("msg", "Entity assignment is in progress")
    return render_template(
        "datamanager/add-data-progress.html", request_id=request_id, message=message
    )


@datamanager_bp.route("/add-data/<request_id>")
def add_data_result(request_id):
    async_api = get_request_api_endpoint()

    # 1. Fetch the result of the 'add_data' job
    try:
        r = requests.get(f"{async_api}/requests/{request_id}", timeout=REQUESTS_TIMEOUT)
        r.raise_for_status()
        result = r.json() or {}
    except Exception as e:
        traceback.print_exc()
        return (
            render_template(
                "error.html", message=f"Could not load assignment result: {e}"
            ),
            500,
        )

    # 2. If still processing, re-render the progress page
    if (
        result.get("status") in ["PENDING", "PROCESSING", "QUEUED"]
        or result.get("response") is None
    ):
        message = (result.get("response") or {}).get(
            "message", "Entity assignment is still in progress"
        )
        return render_template(
            "datamanager/add-data-progress.html", request_id=request_id, message=message
        )

    # 3. Extract data for the template
    data = (result.get("response") or {}).get("data", {})
    headline = {
        "new": data.get("new-entity-count", 0),
        "min_after": data.get("min-entity-after", "N/A"),
        "max_after": data.get("max-entity-after", "N/A"),
    }
    lookup_path = data.get("lookup-path", "")

    # 4. Build the results table
    assigned_entities = data.get("new-entities", [])
    columns = ["reference", "prefix", "organisation", "entity"]
    rows = []
    for entity in assigned_entities:
        rows.append(
            {
                "columns": {
                    "reference": {"value": entity.get("reference", "")},
                    "prefix": {"value": entity.get("prefix", "")},
                    "organisation": {"value": entity.get("organisation", "")},
                    "entity": {"value": entity.get("entity", "")},
                }
            }
        )

    table_params = {
        "columns": columns,
        "fields": columns,
        "rows": rows,
        "columnNameProcessing": "none",
    }

    return render_template(
        "datamanager/add-entities-result.html",
        headline=headline,
        lookup_path=lookup_path,
        table_params=table_params,
    )


@datamanager_bp.route("/add-data/<request_id>/entities")
def entities_preview(request_id):

    async_api = get_request_api_endpoint()

    r = requests.get(f"{async_api}/requests/{request_id}", timeout=REQUESTS_TIMEOUT)
    if r.status_code != 200:
        return render_template("error.html", message="Preview not found"), 404
    req = r.json() or {}
    logger.info(f"response Json is:{json.dumps(req, indent=2)}")
    # Loading state
    if (
        req.get("status") in {"PENDING", "PROCESSING", "QUEUED"}
        or req.get("response") is None
    ):
        return render_template(
            "datamanager/add-data-preview-loading.html", request_id=request_id
        )
    # Error state

    response_payload = req.get("response") or {}
    response_error = response_payload.get("error")
    if response_error:
        error_message = response_error.get("errMsg") or "Unknown error"
        return render_template("datamanager/error.html", message=error_message)

    # Fetch all response details using multiple calls
    all_details = fetch_all_response_details(async_api, request_id)

    data = response_payload.get("data") or {}
    pipeline_summary = data.get("pipeline-summary") or {}
    new_entities = pipeline_summary.get("new-entities") or []
    endpoint_summary = data.get("endpoint-summary") or {}
    source_summary_data = data.get("source-summary") or {}

    # Existing entities preference
    existing_entities_list = pipeline_summary.get("existing-entities") or []

    # Extract pipeline-issues if present
    pipeline_issues = pipeline_summary.get("pipeline-issues") or []
    logger.debug(f"pipeline-issues count: {len(pipeline_issues)}")

    # Build lookup CSV preview using utility function
    table_params, lookup_csv_text = build_lookup_csv_preview(new_entities)

    # Existing entities table
    ex_cols = ["reference", "entity"]
    ex_rows = [
        {"columns": {c: {"value": (e.get(c) or "")} for c in ex_cols}}
        for e in existing_entities_list
    ]
    existing_table_params = {
        "columns": ex_cols,
        "fields": ex_cols,
        "rows": ex_rows,
        "columnNameProcessing": "none",
    }

    # Build endpoint CSV preview using utility function
    (
        endpoint_already_exists,
        endpoint_url,
        endpoint_csv_table_params,
        endpoint_csv_text,
    ) = build_endpoint_csv_preview(endpoint_summary)
    endpoint_csv_body = ""

    # Build source CSV preview using utility function
    source_summary, source_csv_table_params, source_csv_text = build_source_csv_preview(
        source_summary_data
    )
    source_csv_body = ""

    # Build column CSV preview using utility function
    params = req.get("params", {}) or {}
    dataset_id = params.get("dataset", "")
    column_mapping = params.get("column_mapping", {})
    (
        column_csv_table_params,
        column_csv_text,
        has_column_mapping,
    ) = build_column_csv_preview(column_mapping, dataset_id, endpoint_summary)

    # Build entity-organisation CSV preview (only for authoritative data)
    authoritative = params.get("authoritative", False)
    entity_org_table_params = None
    entity_org_csv_text = ""
    has_entity_org = False
    entity_org_warning = None

    if authoritative:
        entity_organisation_data = pipeline_summary.get("entity-organisation") or []
        if entity_organisation_data:
            (
                entity_org_table_params,
                entity_org_csv_text,
                has_entity_org,
            ) = build_entity_organisation_csv(entity_organisation_data)
        else:
            entity_org_warning = "No entity-organisation data found"
    else:
        entity_org_warning = (
            "This must be manually created currently for non-authoritative data"
        )

    return render_template(
        "datamanager/entities_preview.html",
        request_id=request_id,
        new_count=int(pipeline_summary.get("new-in-resource") or 0),
        existing_count=int(pipeline_summary.get("existing-in-resource") or 0),
        breakdown=data.get("new-entity-breakdown") or [],
        # endpoint_summary=endpoint_summary,
        endpoint_already_exists=endpoint_already_exists,
        endpoint_url=endpoint_url,
        table_params=table_params,
        lookup_csv_text=lookup_csv_text,
        existing_table_params=existing_table_params,
        # CSV previews
        endpoint_csv_table_params=endpoint_csv_table_params,
        endpoint_csv_text=endpoint_csv_text,
        endpoint_csv_body=endpoint_csv_body,
        source_csv_table_params=source_csv_table_params,
        source_csv_text=source_csv_text,
        source_csv_body=source_csv_body,
        # NEW: summary for source.csv
        source_summary=source_summary,
        # Column mapping CSV
        column_csv_table_params=column_csv_table_params,
        column_csv_text=column_csv_text,
        has_column_mapping=has_column_mapping,
        all_details=all_details,
        total_count=len(all_details),
        back_url=url_for(
            "datamanager.add_data",
            request_id=(req.get("params") or {}).get("source_request_id") or request_id,
        ),
        pipeline_issues=pipeline_issues,
        dataset=dataset_id,
        # Entity organisation CSV
        entity_org_table_params=entity_org_table_params,
        entity_org_csv_text=entity_org_csv_text,
        has_entity_org=has_entity_org,
        entity_org_warning=entity_org_warning,
    )


@datamanager_bp.route("/add-data/<request_id>/confirm", methods=["POST"])
def add_data_confirm(request_id):
    """
    Confirm and trigger GitHub workflow to add data to the config repo.
    """
    async_api = get_request_api_endpoint()

    # Fetch the request data from async API
    r = requests.get(f"{async_api}/requests/{request_id}", timeout=REQUESTS_TIMEOUT)
    if r.status_code != 200:
        return render_template("error.html", message="Preview not found"), 404

    req = r.json() or {}
    response_payload = req.get("response") or {}
    data = response_payload.get("data") or {}
    params = req.get("params", {}) or {}

    # Extract CSV data
    pipeline_summary = data.get("pipeline-summary") or {}
    endpoint_summary = data.get("endpoint-summary") or {}
    source_summary_data = data.get("source-summary") or {}

    # Get collection from source entry
    source_entry = (
        source_summary_data.get("new_source_entry")
        or source_summary_data.get("existing_source_entry")
        or {}
    )
    collection = source_entry.get("collection")

    if not collection:
        logger.warning(
            f"Collection not found in source entry. Source summary keys: {source_summary_data.keys()}"
        )
        return render_template(
            "datamanager/error.html", message="Collection not found in source entry"
        )

    # Build lookup CSV rows
    new_entities = pipeline_summary.get("new-entities") or []
    _, lookup_csv_text = build_lookup_csv_preview(new_entities)
    lookup_csv_rows = lookup_csv_text.split("\n") if lookup_csv_text else []

    # Build endpoint CSV rows
    _, _, _, endpoint_csv_text = build_endpoint_csv_preview(endpoint_summary)
    endpoint_csv_rows = [endpoint_csv_text] if endpoint_csv_text else []

    # Build source CSV rows
    _, _, source_csv_text = build_source_csv_preview(
        source_summary_data, include_headers=False
    )
    source_csv_rows = [source_csv_text] if source_csv_text else []

    # Build column CSV rows
    dataset = params.get("dataset", "")
    column_mapping = params.get("column_mapping", {})
    _, column_csv_text, _ = build_column_csv_preview(
        column_mapping, dataset, endpoint_summary, include_headers=False
    )
    column_csv_rows = column_csv_text.split("\n") if column_csv_text else []

    # Build entity-organisation CSV rows (only for authoritative data)
    entity_organisation_csv_rows = []
    authoritative = params.get("authoritative", False)
    if authoritative:
        entity_organisation_data = pipeline_summary.get("entity-organisation") or []
        if entity_organisation_data:
            _, entity_org_csv_text, _ = build_entity_organisation_csv(
                entity_organisation_data, include_headers=False
            )
            entity_organisation_csv_rows = (
                entity_org_csv_text.split("\n") if entity_org_csv_text else []
            )

    try:
        # Trigger the GitHub workflow
        logger.info(f"Triggering GitHub workflow for collection: {collection}")
        logger.debug(
            f"Lookup rows: {len(lookup_csv_rows)}, Endpoint rows: {len(endpoint_csv_rows)}, Source rows: {len(source_csv_rows)}, Column rows: {len(column_csv_rows)}, Entity-org rows: {len(entity_organisation_csv_rows)}"  # noqa
        )

        result = trigger_add_data_workflow(
            collection=collection,
            lookup_csv_rows=lookup_csv_rows if lookup_csv_rows else None,
            endpoint_csv_rows=endpoint_csv_rows if endpoint_csv_rows else None,
            source_csv_rows=source_csv_rows if source_csv_rows else None,
            column_csv_rows=column_csv_rows if column_csv_rows else None,
            entity_organisation_csv_rows=(
                entity_organisation_csv_rows if entity_organisation_csv_rows else None
            ),
            triggered_by=f"config-manager-user-{session.get('user', {}).get('login', 'unknown')}",
        )

        if result["success"]:
            logger.info(f"Successfully triggered workflow for collection: {collection}")

            return render_template(
                "datamanager/add-data-success.html",
                collection=collection,
                new_entity_count=len(new_entities),
                message=result["message"],
            )
        else:
            logger.error(f"Failed to trigger workflow: {result['message']}")
            return render_template(
                "datamanager/error.html",
                message=f"Failed to trigger workflow: {result['message']}",
            )

    except GitHubWorkflowError as e:
        logger.exception(f"GitHub workflow error: {e}")
        return render_template(
            "datamanager/error.html", message=f"GitHub workflow error: {str(e)}"
        )
    except Exception as e:
        logger.exception(f"Unexpected error triggering workflow: {e}")
        return render_template(
            "datamanager/error.html", message=f"Unexpected error: {str(e)}"
        )


@datamanager_bp.route("/add-data/<request_id>/confirm-async", methods=["POST"])
def add_data_confirm_async(request_id):
    """
    Confirm and trigger the async GitHub workflow to add data to the config repo.
    Instead of sending CSV rows (which can exceed GitHub's 10KB payload limit),
    this sends only the request_id. The workflow fetches data from the async API.
    """
    try:
        logger.info(f"Triggering async GitHub workflow for request_id: {request_id}")

        result = trigger_add_data_async_workflow(
            request_id=request_id,
            triggered_by=f"config-manager-user-{session.get('user', {}).get('login', 'unknown')}",
        )

        if result["success"]:
            logger.info(
                f"Successfully triggered async workflow for request_id: {request_id}"
            )
            return render_template(
                "datamanager/add-data-success.html",
                collection="",
                new_entity_count=0,
                message=result["message"],
            )
        else:
            logger.error(f"Failed to trigger async workflow: {result['message']}")
            return render_template(
                "datamanager/error.html",
                message=f"Failed to trigger async workflow: {result['message']}",
            )

    except GitHubWorkflowError as e:
        logger.exception(f"GitHub async workflow error: {e}")
        return render_template(
            "datamanager/error.html", message=f"GitHub workflow error: {str(e)}"
        )
    except Exception as e:
        logger.exception(f"Unexpected error triggering async workflow: {e}")
        return render_template(
            "datamanager/error.html", message=f"Unexpected error: {str(e)}"
        )
