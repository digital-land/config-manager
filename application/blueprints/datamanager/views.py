import json
import re
import traceback
from datetime import datetime
 
import requests
from flask import Blueprint, abort, jsonify, redirect, render_template, request, url_for
 
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
 
    datasets = [d for d in ds_response.get("datasets", []) if "collection" in d]
    dataset_options = sorted([d["name"] for d in datasets])
    name_to_dataset_id = {d["name"]: d["dataset"] for d in datasets}
 
    if request.args.get("autocomplete"):
        query = request.args["autocomplete"].lower()
        matches = [name for name in dataset_options if query in name.lower()]
        return jsonify(matches[:10])
 
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
 
    if request.method == "POST":
        form = request.form.to_dict()
        print("🔍 Received form POST data:")
        print(json.dumps(form, indent=2))
 
        mode = form.get("mode", "").strip()
        dataset_input = form.get("dataset", "").strip()
        dataset_id = name_to_dataset_id.get(dataset_input)
 
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
            day = form.get("start_day", "").strip()
            month = form.get("start_month", "").strip()
            year = form.get("start_year", "").strip()
            org_warning = form.get("org_warning", "false") == "true"
 
            errors.update({
                "dataset": not dataset_input,
                "organisation": (org_warning or (selected_orgs and organisation not in selected_orgs)),
                "endpoint_url": (not endpoint_url or not re.match(r"https?://[^\s]+", endpoint_url)),
            })
 
            if doc_url and not re.match(r"^https?://.*\.(gov\.uk|org\.uk)(/.*)?$", doc_url):
                errors["documentation_url"] = True
 
            try:
                datetime(int(year), int(month), int(day))
            except Exception:
                errors["start_date"] = True
 
            if not any(errors.values()):
                payload = {
                    "params": {
                        "type": "check_url",
                        "collection": dataset_id,
                        "dataset": dataset_id,
                        "url": endpoint_url,
                    }
                }
                if column_mapping:
                    payload["params"]["column_mapping"] = column_mapping
                if geom_type:
                    payload["params"]["geom_type"] = geom_type
 
                try:
                    async_api = f"{get_request_api_endpoint()}/requests"
                    response = requests.post(async_api, json=payload, timeout=REQUESTS_TIMEOUT)
 
                    if response.status_code == 202:
                        request_id = response.json()["id"]
                        return redirect(
                            url_for("datamanager.check_results", request_id=request_id, organisation=organisation)
                        )
                    else:
                        abort(500, f"Check tool submission failed with status {response.status_code}")
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
 
 
# ✅ ADD THIS FUNCTION
def fetch_all_details(async_api: str, request_id: str, page_size: int = 50):
    rows = []
    offset = 0
    while True:
        url = f"{async_api}/requests/{request_id}/response-details"
        try:
            resp = requests.get(url, params={"offset": offset, "limit": page_size}, timeout=20)
        except Exception as e:
            print(f"❌ Error fetching details (offset {offset}): {e}")
            break
 
        if resp.status_code != 200:
            print(f"❌ Failed to fetch details (offset {offset}):", resp.status_code)
            break
 
        try:
            batch = resp.json()
        except Exception as e:
            print(f"❌ Failed to decode JSON: {e}")
            break
 
        if not batch:
            break
 
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
 
    return rows
 
 
@datamanager_bp.route("/check-results/<request_id>")

def check_results(request_id):
    try:
        async_api = get_request_api_endpoint()
        organisation = request.args.get("organisation", "Your organisation")

        # Pagination
        page = int(request.args.get("page", 1))
        page_size = 50
        offset = (page - 1) * page_size
 
        # 1️⃣ Fetch summary
        response = requests.get(f"{async_api}/requests/{request_id}", timeout=REQUESTS_TIMEOUT)
        if response.status_code != 200:
            return render_template("error.html", message="Check failed or not found"), 404
 
        result = response.json()
        result.setdefault("params", {}).setdefault("organisation", organisation)
 
        # DEBUG LOGGING
        print("🔎 Debug result JSON:")
        print(json.dumps(result, indent=2))
 
        # If still processing or no response payload yet
        if result.get("status") in ["PENDING", "PROCESSING", "QUEUED"] or result.get("response") is None:
            return render_template("datamanager/check-results-loading.html", result=result)
 
        # 2️⃣ Fetch paginated row-level details
        resp_details = requests.get(
            f"{async_api}/requests/{request_id}/response-details",
            params={"offset": offset, "limit": page_size},
            timeout=REQUESTS_TIMEOUT
        ).json()
 
        # 3️⃣ Safe data extraction
        response_data = (result.get("response") or {}).get("data") or {}
        total_rows = response_data.get("row-count", len(resp_details))
 
        # 4️⃣ Extract headers and prepare rows for table
        table_headers = []
        formatted_rows = []

        if resp_details:
            first_row = resp_details[0].get("converted_row", {})
            table_headers = list(first_row.keys())

            for row in resp_details:
                converted = row.get("converted_row", {})
                formatted_rows.append({
                    "columns": {
                        col: {"value": converted.get(col, "")} for col in table_headers
                    }
                })
 
        table_params = {
            "columns": table_headers,
            "fields": table_headers,
            "rows": formatted_rows,
            "columnNameProcessing": "none"
        }
 
        # Showing range for UI
        showing_start = offset + 1 if total_rows > 0 else 0
        showing_end = min(offset + page_size, total_rows)

        # 5️⃣ Build geometries for Leaflet map
        geometries = []
        for row in resp_details:
            geom = row.get("geometry")
            if not geom:
                # fallback to WKT
                wkt_str = (row.get("converted_row") or {}).get("Point")
                if wkt_str:
                    try:
                        shapely_geom = wkt.loads(wkt_str)
                        geom = mapping(shapely_geom)
                    except:
                        geom = None
            if geom:
                geometries.append({
                    "type": "Feature",
                    "geometry": geom,
                    "properties": {
                        "reference": (row.get("converted_row") or {}).get("Reference") or f"Entry {row.get('entry_number')}"
                    }

                })
 
        # 6️⃣ Process checks and errors
        error_summary = response_data.get("error-summary", [])
        column_field_log = response_data.get("column-field-log", [])
        must_fix, fixable, passed_checks = [], [], []

        for err in error_summary:
            fixable.append(err)
        for col in column_field_log:
            if not col.get("missing"):
                passed_checks.append(f"All rows have {col.get('field')} present")
            else:
                must_fix.append(f"Missing required field: {col.get('field')}")
        allow_add_data = len(must_fix) == 0
 
        # ✅ Render the results page
        return render_template(
            "datamanager/check-results.html",
            result=result,
            geometries=geometries,
            must_fix=must_fix,
            fixable=fixable,
            passed_checks=passed_checks,
            allow_add_data=allow_add_data,
            show_error=not allow_add_data,
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

 
@datamanager_bp.route("/dashboard/debug-payload", methods=["POST"])
def debug_payload():
    form = request.form.to_dict()
    dataset = form.get("dataset", "").strip()
    endpoint = form.get("endpoint_url", "").strip()
 
    payload = {
        "params": {
            "type": "check_url",
            "collection": dataset,
            "dataset": dataset,
            "url": endpoint,
        }
    }
 
    # Optional fields
    column_mapping_str = form.get("column_mapping", "").strip()
    geom_type = form.get("geom_type", "").strip()
    if column_mapping_str:
        try:
            payload["params"]["column_mapping"] = json.loads(column_mapping_str)
        except Exception as e:
            payload["params"]["column_mapping"] = f"⚠️ Invalid JSON: {e}"
    if geom_type:
        payload["params"]["geom_type"] = geom_type
 
    return render_template("datamanager/debug_payload.html", payload=payload)
 
 