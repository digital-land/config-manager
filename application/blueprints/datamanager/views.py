import json
import re
import traceback
from datetime import datetime
 
import requests
from flask import Blueprint, abort, jsonify, redirect, render_template, request, url_for, session
 
from application.utils import get_request_api_endpoint
from shapely import wkt
from shapely.geometry import mapping
 
datamanager_bp = Blueprint("datamanager", __name__, url_prefix="/datamanager")
headers = {"Content-Type": "application/json", "Accept": "application/json"}
 
REQUESTS_TIMEOUT = 20  # seconds
 
 
@datamanager_bp.context_processor
def inject_now():
    return {"now": datetime}
 
 
@datamanager_bp.route("/")
def index():
    return render_template("datamanager/index.html", datamanager={"name": "Dashboard"})
 
 
@datamanager_bp.route("/dashboard/add", methods=["GET", "POST"])
def dashboard_add():
    try:
        ds_response = requests.get(
            "https://www.planning.data.gov.uk/dataset.json?_labels=on&_size=max",
            timeout=REQUESTS_TIMEOUT,
        ).json()
    except Exception as e:
        print("Error fetching datasets:", e)
        abort(500, "Failed to fetch dataset list")
 
    # only datasets that have a collection
    datasets = [d for d in ds_response.get("datasets", []) if "collection" in d]
    dataset_options = sorted([d["name"] for d in datasets])
    # 🔧 build BOTH maps
    name_to_dataset_id = {d["name"]: d["dataset"] for d in datasets}
    name_to_collection_id = {d["name"]: d["collection"] for d in datasets}
 
    # autocomplete dataset options
    if request.args.get("autocomplete"):
        query = request.args["autocomplete"].lower()
        matches = [name for name in dataset_options if query in name.lower()]
        return jsonify(matches[:10])
 
    # fetch orgs for a dataset name
    if request.args.get("get_orgs_for"):
        dataset_name = request.args["get_orgs_for"]
        dataset_id = name_to_dataset_id.get(dataset_name)
        if not dataset_id:
            return jsonify([])
        provision_url = f"https://datasette.planning.data.gov.uk/digital-land/provision.json?_labels=on&_size=max&dataset={dataset_id}"
        try:
            provision_rows = requests.get(provision_url, timeout=REQUESTS_TIMEOUT).json().get("rows", [])
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
 
    if request.method == "POST":
        form = request.form.to_dict()
        print("🔍 Received form POST data:")
        print(json.dumps(form, indent=2))
 
        mode = form.get("mode", "").strip()
        dataset_input = form.get("dataset", "").strip()
        dataset_id = name_to_dataset_id.get(dataset_input)
        collection_id = name_to_collection_id.get(dataset_input)
 
        column_mapping_str = form.get("column_mapping", "{}").strip()
        geom_type = form.get("geom_type", "").strip()
        try:
            column_mapping = json.loads(column_mapping_str) if column_mapping_str else {}
        except json.JSONDecodeError:
            errors["column_mapping"] = True
            column_mapping = {}
 
        if dataset_id:
            provision_url = f"https://datasette.planning.data.gov.uk/digital-land/provision.json?_labels=on&_size=max&dataset={dataset_id}"
            try:
                provision_rows = requests.get(provision_url, timeout=REQUESTS_TIMEOUT).json().get("rows", [])
            except Exception:
                provision_rows = []
            selected_orgs = [
                f"{row['organisation']['label']} ({row['organisation']['value'].split(':', 1)[1]})"
                for row in provision_rows
            ]
 
        if mode == "final":
            organisation = form.get("organisation", "").strip()
            endpoint_url = form.get("endpoint_url", "").strip()
            doc_url = form.get("documentation_url", "").strip()
            licence = form.get("licence", "").strip()
            day = form.get("start_day", "").strip()
            month = form.get("start_month", "").strip()
            year = form.get("start_year", "").strip()
            org_warning = form.get("org_warning", "false") == "true"
 
            # Core required fields
            errors.update({
                "dataset": not dataset_input,
                "organisation": (org_warning or (selected_orgs and organisation not in selected_orgs)),
                "endpoint_url": (not endpoint_url or not re.match(r"https?://[^\s]+", endpoint_url)),
            })
 
            # Optional fields validation (doc_url only if present)
            if doc_url and not re.match(r"^https?://.*\.(gov\.uk|org\.uk)(/.*)?$", doc_url):
                errors["documentation_url"] = True
 
            try:
                if day and month and year:
                    datetime(int(year), int(month), int(day))
                elif any([day, month, year]):
                    errors["start_date"] = True
            except Exception:
                errors["start_date"] = True
 
            if not any(errors.values()):
                payload = {
                    "params": {
                        "type": "check_url",
                        # ✅ send the correct ids
                        "collection": collection_id,
                        "dataset": dataset_id,
                        "url": endpoint_url,
                        # optional extras, null if empty
                        "documentation_url": doc_url or None,
                        "licence": licence or None,
                        "start_date": f"{year}-{month.zfill(2)}-{day.zfill(2)}" if day and month and year else None,
                    }
                }
                session["optional_fields"] = {
                    "documentation_url": doc_url,
                    "licence": licence,
                    "start_date": f"{year}-{month.zfill(2)}-{day.zfill(2)}" if day and month and year else None
                }
 
                if column_mapping:
                    payload["params"]["column_mapping"] = column_mapping
                if geom_type:
                    payload["params"]["geom_type"] = geom_type
 
                # 🔍 DEBUG: print payload to terminal
                print("📦 Sending payload to request API:")
                print(json.dumps(payload, indent=2))    
 
                try:
                    async_api = f"{get_request_api_endpoint()}/requests"
                    response = requests.post(async_api, json=payload, timeout=REQUESTS_TIMEOUT)
 
                    if response.status_code == 202:
                        request_id = response.json()["id"]
                        return redirect(
                            url_for(
                                "datamanager.check_results",
                                request_id=request_id,
                                organisation=organisation
                            )
                        )
                    else:
                        # 🔎 bubble up backend validation/trace to help debugging
                        try:
                            detail = response.json()
                        except Exception:
                            detail = response.text
                        abort(500, f"Check tool submission failed ({response.status_code}): {detail}")
                except Exception as e:
                    traceback.print_exc()
                    abort(500, f"Backend error: {e}")
 
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
        page = int(request.args.get("page", 1))
        page_size = 50
        offset = (page - 1) * page_size
 
        # Fetch summary
        response = requests.get(f"{async_api}/requests/{request_id}", timeout=REQUESTS_TIMEOUT)
        if response.status_code != 200:
            return render_template("error.html", message="Check failed or not found"), 404
 
        result = response.json()
        result.setdefault("params", {}).setdefault("organisation", organisation)
 
        # Still processing?
        if result.get("status") in ["PENDING", "PROCESSING", "QUEUED"] or result.get("response") is None:
            return render_template("datamanager/check-results-loading.html", result=result)
 
        # Row details (paged)
        resp_details = requests.get(
            f"{async_api}/requests/{request_id}/response-details",
            params={"offset": offset, "limit": page_size},
            timeout=REQUESTS_TIMEOUT
        ).json() or []
 
        total_rows = result.get("response", {}).get("data", {}).get("row-count", len(resp_details))
 
        table_headers, formatted_rows = [], []
        if resp_details:
            first_row = (resp_details[0] or {}).get("converted_row", {}) or {}
            table_headers = list(first_row.keys())
            for row in resp_details:
                converted = (row.get("converted_row") or {})
                formatted_rows.append({
                    "columns": {col: {"value": converted.get(col, "")} for col in table_headers}
                })
 
        table_params = {
            "columns": table_headers,
            "fields": table_headers,
            "rows": formatted_rows,
            "columnNameProcessing": "none"
        }
 
        showing_start = offset + 1 if total_rows > 0 else 0
        showing_end = min(offset + page_size, total_rows)
 
        # Geometry mapping
        geometries = []
        for row in resp_details:
            geom = row.get("geometry")
            if not geom:
                wkt_str = (row.get("converted_row") or {}).get("Point")
                if wkt_str:
                    try:
                        shapely_geom = wkt.loads(wkt_str)
                        geom = mapping(shapely_geom)
                    except Exception:
                        geom = None
            if geom:
                geometries.append({
                    "type": "Feature",
                    "geometry": geom,
                    "properties": {
                        "reference": (row.get("converted_row") or {}).get("Reference") or f"Entry {row.get('entry_number')}"
                    }
                })
 
        # Error parsing
        data = (result.get("response") or {}).get("data") or {}
        error_summary = data.get("error-summary", [])
        column_field_log = data.get("column-field-log", [])
 
        must_fix, fixable, passed_checks = [], [], []
        for err in error_summary:
            # Treat all returned summary items as fixable (non-blocking) unless your backend marks them blocking.
            # If your backend can flag blocking ones, split here accordingly.
            fixable.append(err)
 
        for col in column_field_log:
            if not col.get("missing"):
                passed_checks.append(f"All rows have {col.get('field')} present")
            else:
                must_fix.append(f"Missing required field: {col.get('field')}")
 
        # Enable Add data only when there are no Must fix issues
        allow_add_data = len(must_fix) == 0
 
        # Optional info presence (only informational for the page; the /add-data route enforces it)
        params = result.get("params", {}) or {}
        optional_missing = not (
            params.get("documentation_url") and
            params.get("licence") and
            params.get("start_date")
        )
 
        # Show the top red error summary only when there are blocking issues
        show_error = not allow_add_data
 
        return render_template(
            "datamanager/check-results.html",
            result=result,
            geometries=geometries,
            must_fix=must_fix,
            fixable=fixable,
            passed_checks=passed_checks,
            allow_add_data=allow_add_data,
            show_error=show_error,
            optional_missing=optional_missing,  # for a soft hint if you want
            table_params=table_params,
            page=page,
            total_rows=total_rows,
            page_size=page_size,
            showing_start=showing_start,
            showing_end=showing_end,
        )
 
    except Exception as e:
        traceback.print_exc()
        abort(500, f"Error fetching results from backend: {e}")
 
@datamanager_bp.route("/check-results/optional-submit", methods=["POST"])
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
            "documentation_url": documentation_url or None,
            "licence": licence or None,
            "start_date": start_date
        }
    }
    try:
        requests.patch(
            f"{async_api}/requests/{request_id}",
            json=payload,
            timeout=REQUESTS_TIMEOUT
        )
    except Exception as e:
        print(f"❌ Failed to update request {request_id} in request-api: {e}")
 
    return redirect(url_for("datamanager.check_results", request_id=request_id))
 
 
@datamanager_bp.route("/check-results/<request_id>/add-data", methods=["GET"])
def add_data(request_id):
    async_api = get_request_api_endpoint()
    response = requests.get(f"{async_api}/requests/{request_id}", timeout=REQUESTS_TIMEOUT)
    result = response.json()
    params = result.get("params", {})
 
    doc_url = params.get("documentation_url") or session.get("optional_fields", {}).get("documentation_url")
    licence = params.get("licence") or session.get("optional_fields", {}).get("licence")
    start_date = params.get("start_date") or session.get("optional_fields", {}).get("start_date")
 
    if doc_url and licence and start_date:
        session.pop("optional_fields", None)
        return redirect(url_for("datamanager.check_results", request_id=request_id))
 
    return render_template("datamanager/add-data.html", request_id=request_id)
 
 
@datamanager_bp.route("/dashboard/debug-payload", methods=["POST"])
def debug_payload():
    form = request.form.to_dict()
 
    dataset = (form.get("dataset") or "").strip()
    endpoint = (form.get("endpoint_url") or "").strip()
 
    # ✅ Optional fields from the form
    documentation_url = (form.get("documentation_url") or "").strip()
    licence = (form.get("licence") or "").strip()
    day = (form.get("start_day") or "").strip()
    month = (form.get("start_month") or "").strip()
    year = (form.get("start_year") or "").strip()
 
    # Build start_date if all parts present
    start_date = None
    if day and month and year:
        start_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
 
    payload = {
        "params": {
            "type": "check_url",
            "collection": dataset,  # this is just a preview; actual submit uses ids
            "dataset": dataset,
            "url": endpoint,
            "documentation_url": documentation_url or None,
            "licence": licence or None,
            "start_date": start_date
        }
    }
 
    # Optional extras if you use them
    column_mapping_str = (form.get("column_mapping") or "").strip()
    geom_type = (form.get("geom_type") or "").strip()
    if column_mapping_str:
        try:
            payload["params"]["column_mapping"] = json.loads(column_mapping_str)
        except Exception as e:
            payload["params"]["column_mapping"] = f"⚠️ Invalid JSON: {e}"
    if geom_type:
        payload["params"]["geom_type"] = geom_type
 
    return render_template("datamanager/debug_payload.html", payload=payload)
 
 