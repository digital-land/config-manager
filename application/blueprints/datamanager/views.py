import json
import logging
import os
import re
import traceback
from datetime import datetime
from dotenv import load_dotenv

import requests
from flask import (
    Blueprint,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
    session,
)

from application.utils import get_request_api_endpoint
from shapely import wkt
from shapely.geometry import mapping
import csv
from io import StringIO

# Load .env file
load_dotenv()

datamanager_bp = Blueprint("datamanager", __name__, url_prefix="/datamanager")
logger = logging.getLogger(__name__)
headers = {"Content-Type": "application/json", "Accept": "application/json"}
planning_base_url = os.getenv("PLANNING_URL", "https://www.planning.data.gov.uk")
datasette_base_url = os.getenv(
    "DATASETTE_BASE_URL", "https://datasette.planning.data.gov.uk/digital-land"
)

REQUESTS_TIMEOUT = 20  # seconds


@datamanager_bp.errorhandler(Exception)
def handle_error(e):
    logger.exception(f"Error: {e}")
    return render_template("datamanager/error.html", message=str(e)), 500


# put this near the top, replacing your current get_spec_fields_from_datasette
def get_spec_fields_union(dataset_id):
    """
    Return the union of:
      - global field list (all datasets)
      - dataset-scoped field list (if dataset_id is provided)
    Keep original casing; de-duplicate exact strings; stable order.
    """
    base = (f"{datasette_base_url}/dataset_field.json",)
    headers = {"Accept": "application/json", "User-Agent": "Planning Data - Manage"}

    def _fetch(url):
        try:
            r = requests.get(url, timeout=REQUESTS_TIMEOUT, headers=headers)
            r.raise_for_status()
            rows = r.json() or []
            return [
                (row.get("field") or "").strip() for row in rows if row.get("field")
            ]
        except Exception as e:
            logger.warning(f"dataset_field fetch failed: {e} for {url}")
            return []

    # global list (all datasets)
    global_fields = _fetch(f"{base}?_shape=array")

    # dataset-filtered list (e.g. camel-case BFL items)
    dataset_fields = (
        _fetch(f"{base}?_shape=array&dataset={dataset_id}") if dataset_id else []
    )

    # union, preserve first-seen order, exact-string dedupe
    seen, out = set(), []
    for f in global_fields + dataset_fields:
        if f and f not in seen:
            seen.add(f)
            out.append(f)

    # optional: sort by lowercase while preserving casing (comment out if you prefer raw order)
    out.sort(key=lambda x: x.lower())
    return out


def order_table_fields(all_fields):
    leading = []
    trailing = []

    for field in all_fields:
        if field.lower() == "reference":
            # Insert at the beginning (splice(0, 0, field) equivalent)
            leading.insert(0, field)
        elif field.lower() == "name":
            # Append to leading
            leading.append(field)
        else:
            # All other fields go to trailing
            trailing.append(field)

    # Combine leading and trailing
    ordered_fields = leading + trailing

    return ordered_fields


def fetch_all_response_details(
    async_api: str, request_id: str, limit: int = 50
) -> list:
    """
    Fetch all response details with multiple calls using the specified limit.
    Similar to the pattern used in submit repository.
    """
    all_details = []
    offset = 0
    logger.info(
        f"Fetching response details - API: {async_api}, Request ID: {request_id}"
    )

    while True:
        try:
            url = f"{async_api}/requests/{request_id}/response-details"
            params = {"offset": offset, "limit": limit}
            logger.debug(f"Fetching batch - URL: {url}, Params: {params}")

            response = requests.get(url, params=params, timeout=REQUESTS_TIMEOUT)
            content_length = getattr(response, "content", None)
            content_length = (
                len(content_length) if content_length is not None else "N/A"
            )
            logger.info(
                f"Batch response - Status: {response.status_code}, Content-Length: {content_length}"
            )

            response.raise_for_status()
            batch = response.json() or []
            logger.info(f"Batch parsed - Items: {len(batch)}")

            if not batch:
                logger.info("No more batches available")
                break

            # Log sample of first batch for debugging
            if offset == 0 and batch:
                logger.info(
                    f"First batch sample - Item keys: {list(batch[0].keys()) if batch[0] else 'Empty item'}"
                )
                if batch[0] and "converted_row" in batch[0]:
                    converted_sample = batch[0]["converted_row"]
                    if converted_sample:
                        logger.info(
                            f"First converted_row sample: {dict(list(converted_sample.items())[:3])}"
                        )
                    else:
                        logger.info("Empty converted_row")

            all_details.extend(batch)

            if len(batch) < limit:
                logger.info(f"Last batch received - Total items: {len(all_details)}")
                break

            offset += limit

        except Exception as e:
            logger.error(f"Failed to fetch batch at offset {offset}: {e}")
            logger.error(f"Response status: {getattr(response, 'status_code', 'N/A')}")
            response_text = getattr(response, "text", "N/A")
            if hasattr(response_text, "__getitem__"):
                logger.error(f"Response text: {response_text[:500]}")
            else:
                logger.error(f"Response text: {response_text}")
            break

    logger.info(f"Total response details fetched: {len(all_details)}")
    return all_details


def read_raw_csv_preview(source_url: str, max_rows: int = 50):
    """
    Fetch the CSV at source_url and return (headers, first N rows).
    """
    headers_, rows = [], []
    if not source_url:
        return headers_, rows
    try:
        resp = requests.get(source_url, timeout=REQUESTS_TIMEOUT)
        text = resp.content.decode("utf-8", errors="ignore")
        reader = csv.reader(StringIO(text))
        first = next(reader, None)
        if first is None:
            return headers_, rows
        headers_ = [h.strip().lstrip("\ufeff") for h in first if h is not None]
        for i, r in enumerate(reader):
            if i >= max_rows:
                break
            vals = (r + [""] * len(headers_))[: len(headers_)]
            rows.append(vals)
    except Exception as e:
        logger.warning(f"Could not fetch/parse CSV preview: {e}")
    return headers_, rows


@datamanager_bp.context_processor
def inject_now():
    return {"now": datetime}


@datamanager_bp.route("/")
def index():
    return render_template("datamanager/index.html", datamanager={"name": "Dashboard"})


@datamanager_bp.route("/dashboard/config", methods=["GET"])
def dashboard_config():
    # you can pass any context you like here
    return render_template(
        "datamanager/dashboard_config.html", datamanager={"name": "Dashboard"}
    )


@datamanager_bp.route("/dashboard/add/import", methods=["GET", "POST"])
def dashboard_add_import():
    """
    Route to import endpoint configuration from CSV.
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
                        # Store in session for confirm page
                        session["import_csv_data"] = parsed_data
                        return redirect(
                            url_for("datamanager.dashboard_add_import_confirm")
                        )

            except Exception as e:
                errors["csv_data"] = f"Invalid CSV format: {str(e)}"

    return render_template(
        "datamanager/dashboard_add_import.html", csv_data=csv_data, errors=errors
    )


@datamanager_bp.route("/dashboard/add/import/confirm", methods=["GET", "POST"])
def dashboard_add_import_confirm():
    """
    Confirmation page for imported CSV data.
    Shows parsed data and allows user to proceed to dashboard_add.
    """
    parsed_data = session.get("import_csv_data")
    if not parsed_data:
        return redirect(url_for("datamanager.dashboard_add_import"))

    if request.method == "POST":
        # Redirect to dashboard_add with query params to pre-fill the form
        return redirect(
            url_for(
                "datamanager.dashboard_add",
                import_data="true",
                dataset=parsed_data.get("pipelines", ""),
                organisation=parsed_data.get("organisation", ""),
                endpoint_url=parsed_data.get("endpoint-url", ""),
                documentation_url=parsed_data.get("documentation-url", ""),
                start_date=parsed_data.get("start-date", ""),
                plugin=parsed_data.get("plugin", ""),
                licence=parsed_data.get("licence", ""),
            )
        )

    return render_template(
        "datamanager/dashboard_add_import_confirm.html", data=parsed_data
    )


@datamanager_bp.route("/dashboard/add", methods=["GET", "POST"])
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

    base_provision_url = f"{datasette_base_url}/provision.json"
    # fetch orgs for a dataset name (for UI suggestions)
    if request.args.get("get_orgs_for"):
        dataset_name = request.args["get_orgs_for"]
        dataset_id = name_to_dataset_id.get(dataset_name)
        if not dataset_id:
            return jsonify([])

        provision_url = (
            f"{base_provision_url}?_labels=on&_size=max&dataset={dataset_id}"
        )
        try:
            provision_rows = (
                requests.get(
                    provision_url,
                    timeout=REQUESTS_TIMEOUT,
                    headers={"User-Agent": "Planning Data - Manage"},
                )
                .json()
                .get("rows", [])
            )
        except Exception:
            provision_rows = []
        selected_orgs = [
            f"{row['organisation']['label']} ({row['organisation']['value'].split(':', 1)[1]})"
            for row in provision_rows
        ]
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
            provision_url = (
                f"{base_provision_url}?_labels=on&_size=max&dataset={dataset_id}"
            )
            try:
                provision_rows = (
                    requests.get(
                        provision_url,
                        timeout=REQUESTS_TIMEOUT,
                        headers={"User-Agent": "Planning Data - Manage"},
                    )
                    .json()
                    .get("rows", [])
                )
                selected_orgs = [
                    f"{row['organisation']['label']} ({row['organisation']['value'].split(':', 1)[1]})"
                    for row in provision_rows
                ]
                # Find the matching org display string
                for row in provision_rows:
                    if row.get("organisation", {}).get("value") == org_value:
                        code = row["organisation"]["value"].split(":", 1)[1]
                        org_display = f"{row['organisation']['label']} ({code})"
                        break
            except Exception as e:
                logger.warning(f"Failed to fetch orgs for dataset {dataset_id}: {e}")

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

        column_mapping_str = form.get("column_mapping", "{}").strip()
        geom_type = form.get("geom_type", "").strip()
        try:
            column_mapping = (
                json.loads(column_mapping_str) if column_mapping_str else {}
            )
        except json.JSONDecodeError:
            errors["column_mapping"] = True
            column_mapping = {}

        # Preload org list + build a reverse map we'll use on submit
        org_label_to_value = {}
        if dataset_id:
            provision_url = (
                f"{base_provision_url}?_labels=on&_size=max&dataset={dataset_id}"
            )
            try:
                provision_rows = (
                    requests.get(
                        provision_url,
                        timeout=REQUESTS_TIMEOUT,
                        headers={"User-Agent": "Planning Data - Manage"},
                    )
                    .json()
                    .get("rows", [])
                )
            except Exception:
                provision_rows = []
            selected_orgs = [
                f"{row['organisation']['label']} ({row['organisation']['value'].split(':', 1)[1]})"
                for row in provision_rows
            ]
            # 🔑 map "Label (CODE)" -> "prefix:CODE"
            org_label_to_value = {
                f"{row['organisation']['label']} ({row['organisation']['value'].split(':', 1)[1]})": row[
                    "organisation"
                ][
                    "value"
                ]
                for row in provision_rows
                if row.get("organisation", {}).get("value")
            }

        if mode == "final":
            # what the user submitted from the select/input
            org_input = (form.get("organisation") or "").strip()
            endpoint_url = form.get("endpoint_url", "").strip()
            doc_url = form.get("documentation_url", "").strip()

            # ✅ licence defaults to 'ogl' if blank
            licence = (form.get("licence") or "ogl").strip().lower()

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

            # resolve to real entity ref (prefix:CODE) using our map
            org_value = org_label_to_value.get(org_input)

            # Fallback: guess prefix from dataset + code in parentheses
            if not org_value:
                m = re.search(r"\(([^)]+)\)$", org_input)
                code = m.group(1).strip() if m else ""
                if code:
                    # minimal rule-of-thumb by dataset
                    if dataset_id == "brownfield-land":
                        org_value = f"local-authority-eng:{code}"
                    elif dataset_id == "listed-building":
                        org_value = f"government-organisation:{code}"
                    # add extra dataset rules here if needed

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
                }
            )

            # Optional fields validation (doc_url only if present)
            if doc_url and not re.match(
                r"^https?://[^\s/]+\.(gov\.uk|org\.uk)(/.*)?$", doc_url
            ):
                errors["documentation_url"] = True

            if not any(errors.values()):
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
                        # ✅ send the REAL entity reference (prefix:CODE)
                        "organisation": org_value,
                    }
                }
                session["required_fields"] = {
                    "collection": collection_id,
                    "dataset": dataset_id,
                    "url": endpoint_url,
                    "organisation": org_value,
                }
                session["optional_fields"] = {
                    "documentation_url": doc_url,
                    "licence": licence,
                    "start_date": start_date_str,
                }

                if column_mapping:
                    payload["params"]["column_mapping"] = column_mapping
                if geom_type:
                    payload["params"]["geom_type"] = geom_type

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
        organisation = request.args.get("organisation", "Your organisation")

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
        logger.info(f"Environment: {os.getenv('ENVIRONMENT', 'local')}")
        logger.info(f"API endpoint: {async_api}")
        logger.info(f"Result status: {result.get('status')}")
        logger.info(f"Result keys: {list(result.keys())}")
        if result.get("response"):
            logger.info(f"Response keys: {list(result.get('response', {}).keys())}")
            if result.get("response", {}).get("data"):
                logger.info(
                    f"Data keys: {list(result.get('response', {}).get('data', {}).keys())}"
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
            entity_summary = data.get("entity-summary") or {}
            new_entities = data.get("new-entities") or []

            # exact lists as provided by BE
            existing_entities_list = data.get("existing-entities") or []

            # exact counts as provided by BE (fallback to 0 if missing)
            existing_count = int(entity_summary.get("existing-in-resource") or 0)
            new_count = int(entity_summary.get("new-in-resource") or 0)

            logger.debug(f"BE data keys: {list(data.keys())}")
            logger.debug(f"entity-summary: {entity_summary}")
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
            )

    except Exception as e:
        traceback.print_exc()
        raise Exception(f"Error fetching results from backend: {e}")


@datamanager_bp.route("/check-results/optional-submit", methods=["GET", "POST"])
def optional_fields_submit():
    form = request.form.to_dict()
    request_id = form.get("request_id")
    documentation_url = form.get("documentation_url", "").strip()
    licence = form.get("licence", "").strip()
    start_day = form.get("start_day", "").strip()
    start_month = form.get("start_month", "").strip()
    start_year = form.get("start_year", "").strip()

    start_date = None
    if start_day and start_month and start_year:
        start_date = f"{start_year}-{start_month.zfill(2)}-{start_day.zfill(2)}"

    # 🔹 Save in backend
    async_api = get_request_api_endpoint()
    payload = {
        "params": {
            "type": "check_url",
            "documentation_url": documentation_url or None,
            "licence": licence or None,
            "start_date": start_date,
        }
    }
    logger.info("Submitting optional fields to request API")
    try:
        requests.patch(
            f"{async_api}/requests/{request_id}", json=payload, timeout=REQUESTS_TIMEOUT
        )
        logger.info(f"Successfully updated request {request_id} in request-api")
        logger.debug(json.dumps(payload, indent=2))
    except Exception as e:
        logger.error(f"Failed to update request {request_id} in request-api: {e}")

    return redirect(url_for("datamanager.add_data", request_id=request_id))


@datamanager_bp.route("/check-results/add-data", methods=["GET", "POST"])
def add_data():
    async_api = get_request_api_endpoint()

    # all optional fields from session (if any)
    existing_doc = session.get("optional_fields", {}).get("documentation_url")
    existing_lic = session.get("optional_fields", {}).get("licence")
    existing_start = session.get("optional_fields", {}).get("start_date")

    def _submit_preview(doc_url: str, licence: str, start_date: str):
        # all required fields from session
        params = session.get("required_fields", {}).copy()
        logger.debug(f"Using required fields from session: {params}")
        params.update(
            {
                "type": "add_data",
                "preview": True,  # ← PREVIEW mode
                "documentation_url": doc_url,
                "licence": licence,
                "start_date": start_date,
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
        if existing_doc and existing_lic and existing_start:
            # Optional fields are present → use EXISTING payload to preview
            return _submit_preview(existing_doc, existing_lic, existing_start)

        # Pre-populate form with any existing data for the initial GET request
        form_data = {
            "documentation_url": existing_doc,
            "licence": existing_lic,
        }
        return render_template("datamanager/add-data.html", form=form_data)

    # POST – user submitted optional fields
    form = request.form.to_dict()
    doc_url = (form.get("documentation_url") or existing_doc or "").strip()
    licence = (form.get("licence") or existing_lic or "").strip()

    d = (form.get("start_day") or "").strip()
    m = (form.get("start_month") or "").strip()
    y = (form.get("start_year") or "").strip()
    start_date = (
        f"{y}-{m.zfill(2)}-{d.zfill(2)}"
        if (d and m and y)
        else (existing_start or "").strip()
    )

    if not (doc_url and licence and start_date):
        # Still missing something – re-show optional screen
        return render_template("datamanager/add-data.html", form=form)

    # Remember locally (optional)
    session["optional_fields"] = {
        "documentation_url": doc_url,
        "licence": licence,
        "start_date": start_date,
    }

    # Now preview with the UPDATED payload
    return _submit_preview(doc_url, licence, start_date)


@datamanager_bp.route("/check-results/<request_id>/add-data/confirm", methods=["POST"])
def add_data_confirm(request_id):
    async_api = get_request_api_endpoint()

    # request_id is the PREVIEW add_data id
    pr = requests.get(f"{async_api}/requests/{request_id}", timeout=REQUESTS_TIMEOUT)
    if pr.status_code != 200:
        return render_template("error.html", message="Preview not found"), 404
    preview_req = pr.json() or {}
    params = (preview_req.get("params") or {}).copy()
    params["type"] = "add_data"
    params["preview"] = False  # commit!

    submit = requests.post(
        f"{async_api}/requests", json={"params": params}, timeout=REQUESTS_TIMEOUT
    )
    if submit.status_code == 202:
        body = submit.json() or {}
        new_id = body.get("id")
        msg = body.get("message") or "Entity assignment is in progress"
        session.pop("optional_fields", None)
        return redirect(
            url_for("datamanager.add_data_progress", request_id=new_id, msg=msg)
        )

    detail = (
        submit.json()
        if "application/json" in (submit.headers.get("content-type") or "")
        else submit.text
    )
    raise Exception(f"Add data submission failed ({submit.status_code}): {detail}")


# --- Configure screen: CSV + MUST-FIX on left, smart dropdowns on right ---
@datamanager_bp.route("/configure/<request_id>", methods=["GET", "POST"])
def configure(request_id):
    async_api = get_request_api_endpoint()

    # 1) Load the original request summary
    r = requests.get(f"{async_api}/requests/{request_id}", timeout=REQUESTS_TIMEOUT)
    if r.status_code != 200:
        return render_template("error.html", message="Original request not found"), 404

    req = r.json()
    params = req.get("params", {}) or {}
    organisation = params.get(
        "organisation", request.args.get("organisation", "Your organisation")
    )
    dataset_id = params.get("dataset", "") or ""
    source_url = params.get("url", "") or ""
    existing_geom_type = params.get("geom_type") or ""

    # 2) Existing mapping known by backend (prefer echoed response over params)
    existing_raw_to_spec = (
        (req.get("response") or {}).get("data", {}).get("column-mapping")
        or params.get("column_mapping")
        or {}
    )
    existing_raw_to_spec_ci = {
        (k or "").strip().lower(): (v or "").strip()
        for k, v in existing_raw_to_spec.items()
    }

    # Build reverse map SPEC -> RAW from existing mapping
    spec_to_raw = {}
    for raw, spec in existing_raw_to_spec.items():
        if raw and spec and spec not in spec_to_raw:
            spec_to_raw[spec] = raw

    # 3) Full specification list (scoped union)
    spec_fields = get_spec_fields_union(dataset_id)
    spec_lookup_lower = {(f or "").strip().lower(): f for f in spec_fields}

    # 4) CSV headers + preview
    raw_headers, raw_rows = read_raw_csv_preview(source_url)

    # 5) Must-fix spec fields from last check
    data_blob = (req.get("response") or {}).get("data") or {}
    cfl = data_blob.get("column-field-log", []) or []
    must_fix_specs = [
        row["field"] for row in cfl if row.get("missing") and row.get("field")
    ]

    # 6) Helper to normalise names for auto-match
    def _norm(s: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

    spec_by_norm = {_norm(f): f for f in spec_fields}

    # 7) POST: collect mappings
    if request.method == "POST":
        form = request.form.to_dict()
        new_mapping: dict[str, str] = {}

        # A) RAW rows: map_raw[<raw>] -> <spec>
        chosen_spec_by_raw = {}
        for raw in raw_headers:
            chosen_spec = (form.get(f"map_raw[{raw}]") or "").strip()
            if chosen_spec and chosen_spec != "__NOT_MAPPED__":
                new_mapping[raw] = chosen_spec
                chosen_spec_by_raw[raw] = chosen_spec

        # Reverse of the current form choices too (SPEC -> RAW), preferred over existing
        spec_to_raw_from_form = {}
        for raw, spec in chosen_spec_by_raw.items():
            spec_to_raw_from_form.setdefault(spec, raw)

        # B) MUST-FIX rows
        for mustfix_spec in must_fix_specs:
            chosen_source_spec = (
                form.get(f"map_spec_to_spec[{mustfix_spec}]") or ""
            ).strip()
            if not chosen_source_spec or chosen_source_spec == "__NOT_MAPPED__":
                continue
            source_raw = spec_to_raw_from_form.get(
                chosen_source_spec
            ) or spec_to_raw.get(chosen_source_spec)
            if source_raw:
                new_mapping[source_raw] = mustfix_spec

        geom_type = (form.get("geom_type") or "").strip()

        payload = {
            "params": {
                "type": "check_url",
                "collection": params.get("collection"),
                "dataset": dataset_id,
                "url": source_url,
                "documentation_url": params.get("documentation_url"),
                "licence": params.get("licence"),
                "start_date": params.get("start_date"),
                "column_mapping": new_mapping or None,  # RAW -> SPEC
                "geom_type": geom_type or None,
                "organisation": organisation,
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
                return redirect(url_for("datamanager.configure", request_id=new_id))
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

    # 8) Build raw preview table model
    def table_from_csv(headers, rows):
        if not headers or not rows:
            return {
                "columns": [],
                "fields": [],
                "rows": [],
                "columnNameProcessing": "none",
            }
        out_rows = []
        for r_ in rows:
            out_rows.append(
                {"columns": {headers[i]: {"value": r_[i]} for i in range(len(headers))}}
            )
        return {
            "columns": headers,
            "fields": headers,
            "rows": out_rows,
            "columnNameProcessing": "none",
        }

    raw_table_params = table_from_csv(raw_headers, raw_rows)

    # 9) Transformed preview (always defined)
    transformed_table_params = {
        "columns": [],
        "fields": [],
        "rows": [],
        "columnNameProcessing": "none",
    }
    try:
        if (req.get("status") not in ["PENDING", "PROCESSING", "QUEUED"]) and (
            req.get("response") is not None
        ):
            # Fetch first 50 rows for preview (not all data)
            details = (
                requests.get(
                    f"{async_api}/requests/{request_id}/response-details",
                    params={"offset": 0, "limit": 50},
                    timeout=REQUESTS_TIMEOUT,
                ).json()
                or []
            )
            if details:
                first = (details[0] or {}).get("converted_row", {}) or {}
                t_columns = list(first.keys())
                t_rows = []
                for d in details:
                    conv = d.get("converted_row") or {}
                    t_rows.append(
                        {"columns": {c: {"value": conv.get(c, "")} for c in t_columns}}
                    )
                transformed_table_params = {
                    "columns": t_columns,
                    "fields": t_columns,
                    "rows": t_rows,
                    "columnNameProcessing": "none",
                }
    except Exception:
        traceback.print_exc()

    # 10) Build UI rows (must-fix first)
    csv_rows = []
    for raw in raw_headers:
        raw_key_ci = (raw or "").strip().lower()
        mapped_spec = existing_raw_to_spec_ci.get(raw_key_ci, "")
        if not mapped_spec:
            auto = spec_by_norm.get(_norm(raw))
            if auto:
                mapped_spec = auto
        if mapped_spec:
            mapped_spec = spec_lookup_lower.get(
                mapped_spec.strip().lower(), mapped_spec
            )
        csv_rows.append(
            {
                "kind": "raw",
                "label": raw,
                "preselect": mapped_spec,
                "mapped": bool(mapped_spec),
            }
        )

    mustfix_rows = []
    for spec in must_fix_specs:
        mapped_raw = spec_to_raw.get(spec, "")
        mustfix_rows.append(
            {
                "kind": "mustfix",
                "label": spec,
                "preselect": mapped_raw,
                "mapped": bool(mapped_raw),
            }
        )

    display_rows = mustfix_rows + csv_rows

    # 11) Render
    return render_template(
        "datamanager/configure.html",
        request_id=request_id,
        organisation=organisation,
        dataset=dataset_id,
        raw_table_params=raw_table_params,
        transformed_table_params=transformed_table_params,
        rows=display_rows,
        spec_options=spec_fields,
        raw_options=raw_headers,
        must_fix_specs=must_fix_specs,
        existing_geom_type=existing_geom_type,
    )


@datamanager_bp.route("/add-data/progress/<request_id>")
def add_data_progress(request_id):
    # fallback message in case BE didn’t return one
    message = request.args.get("msg", "Entity assignment is in progress")
    return render_template(
        "datamanager/add-data-progress.html", request_id=request_id, message=message
    )


@datamanager_bp.route("/add-data/result/<request_id>")
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


@datamanager_bp.route("/check-results/<request_id>/entities")
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

    # Fetch all response details using multiple calls
    all_details = fetch_all_response_details(async_api, request_id)

    data = (req.get("response") or {}).get("data") or {}
    entity_summary = data.get("entity-summary") or {}
    new_entities = entity_summary.get("new-entities") or []
    endpoint_summary = data.get("endpoint-summary") or {}
    source_summary_data = data.get("source-summary") or {}

    # Existing entities preference
    existing_entities_list = entity_summary.get("existing-entities") or []

    # New entities table
    cols = ["reference", "prefix", "organisation", "entity"]
    rows = [
        {"columns": {c: {"value": (e.get(c) or "")} for c in cols}}
        for e in new_entities
    ]
    logger.info(f"New entities rows: {rows}")
    table_params = {
        "columns": cols,
        "fields": cols,
        "rows": rows,
        "columnNameProcessing": "none",
    }

    lookup_csv_text = ""

    fields = [
        "prefix",
        "resource",
        "endpoint",
        "entry-number",
        "organisation",
        "reference",
        "entity",
        "entry-date",
        "start-date",
        "end-date",
    ]

    lookup_csv_text = "\n".join(
        ",".join(item.get(field, "") for field in fields) for item in new_entities
    )

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

    # ---------- endpoint.csv preview ----------
    endpoint_csv_table_params = None
    endpoint_csv_text = ""
    endpoint_csv_body = ""

    ep_cols = [
        "endpoint",
        "endpoint-url",
        "parameters",
        "plugin",
        "entry-date",
        "start-date",
        "end-date",
    ]
    endpoint_already_exists = (
        "Yes" if endpoint_summary.get("endpoint_url_in_endpoint_csv") is True else "No"
    )
    logger.info(f"Endpoint already exists: {endpoint_already_exists}")
    if endpoint_summary.get("endpoint_url_in_endpoint_csv"):
        end_point_entry = endpoint_summary.get("existing_endpoint_entry", {})
        endpoint_url = end_point_entry.get("endpoint-url", "")
    else:
        end_point_entry = endpoint_summary.get("new_endpoint_entry", {})
        endpoint_url = end_point_entry.get("endpoint-url", "")
        endpoint_csv_text = ",".join([end_point_entry.get(col, "") for col in ep_cols])
    ep_row = [str(end_point_entry.get(col, "") or "") for col in ep_cols]
    endpoint_csv_table_params = {
        "columns": ep_cols,
        "fields": ep_cols,
        "rows": [{"columns": {c: {"value": v} for c, v in zip(ep_cols, ep_row)}}],
        "columnNameProcessing": "none",
    }

    # ---------- source.csv preview + summary ----------
    source_csv_table_params = None
    source_csv_text = ""
    source_csv_body = ""
    source_summary = None  # <<— new
    src_source = {}

    src_cols = [
        "source",
        "attribution",
        "collection",
        "documentation-url",
        "endpoint",
        "licence",
        "organisation",
        "pipelines",
        "entry-date",
        "start-date",
        "end-date",
    ]
    # src_source = endpoint_summary.get("new_source_entry") or None
    source_present = source_summary_data.get("documentation_url_in_source_csv")
    will_create_source_text = "No" if source_present else "Yes"
    if not source_present:
        src_source = source_summary_data.get("new_source_entry", {})
        src_row = [str(src_source.get(col, "") or "") for col in src_cols]
        source_csv_table_params = {
            "columns": src_cols,
            "fields": src_cols,
            "rows": [{"columns": {c: {"value": v} for c, v in zip(src_cols, src_row)}}],
            "columnNameProcessing": "none",
        }
        source_csv_text = ",".join(src_cols) + "\n" + ",".join(src_row)
    else:
        src_source = source_summary_data.get("existing_source_entry", {})
        src_row = [str(src_source.get(col, "") or "") for col in src_cols]
        source_csv_table_params = {
            "columns": src_cols,
            "fields": src_cols,
            "rows": [{"columns": {c: {"value": v} for c, v in zip(src_cols, src_row)}}],
            "columnNameProcessing": "none",
        }
    logger.info(f"Will create source1: {will_create_source_text}")
    # Build summary panel model (like Endpoint Summary)
    source_summary = {
        "will_create": will_create_source_text,
        "source": src_source.get("source", ""),
        "collection": src_source.get("collection", ""),
        "organisation": src_source.get("organisation", ""),
        "endpoint": src_source.get("endpoint", ""),
        "licence": src_source.get("licence", ""),
        "pipelines": src_source.get("pipelines", ""),
        "entry_date": src_source.get("entry-date", ""),
        "start_date": src_source.get("start-date", ""),
        "end_date": src_source.get("end-date", ""),
        "documentation_url": src_source.get("documentation-url", ""),
        "attribution": src_source.get("attribution", ""),
    }

    return render_template(
        "datamanager/entities_preview.html",
        request_id=request_id,
        new_count=int(entity_summary.get("new-in-resource") or 0),
        existing_count=int(entity_summary.get("existing-in-resource") or 0),
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
        all_details=all_details,
        total_count=len(all_details),
        back_url=url_for(
            "datamanager.add_data",
            request_id=(req.get("params") or {}).get("source_request_id") or request_id,
        ),
    )
