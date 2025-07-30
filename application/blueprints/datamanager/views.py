import json
import re
import traceback
from datetime import datetime

import requests
from flask import Blueprint, abort, jsonify, redirect, render_template, request, url_for

from application.utils import get_request_api_endpoint

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
    # Fetch dataset list for the form
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

    # --- Autocomplete support ---
    if request.args.get("autocomplete"):
        query = request.args["autocomplete"].lower()
        matches = [name for name in dataset_options if query in name.lower()]
        return jsonify(matches[:10])

    # --- Get orgs for selected dataset ---
    if request.args.get("get_orgs_for"):
        dataset_name = request.args["get_orgs_for"]
        dataset_id = name_to_dataset_id.get(dataset_name)
        if not dataset_id:
            return jsonify([])
        provision_url = (
            "https://datasette.planning.data.gov.uk/digital-land/provision.json"
            "?_labels=on&_size=max&dataset={dataset_id}"
        ).format(dataset_id=dataset_id)
        try:
            provision_rows = requests.get(provision_url, timeout=REQUESTS_TIMEOUT).json().get("rows", [])
        except Exception:
            provision_rows = []
        selected_orgs = [
            f"{row['organisation']['label']} ({row['organisation']['value'].split(':', 1)[1]})"
            for row in provision_rows
        ]
        return jsonify(selected_orgs)

    # --- Form submission logic ---
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
            provision_url = (
                "https://datasette.planning.data.gov.uk/digital-land/provision.json"
                "?_labels=on&_size=max&dataset={dataset_id}"
            ).format(dataset_id=dataset_id)
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

            errors.update(
                {
                    "dataset": not dataset_input,
                    "organisation": (org_warning or (selected_orgs and organisation not in selected_orgs)),
                    "endpoint_url": (not endpoint_url or not re.match(r"https?://[^\s]+", endpoint_url)),
                }
            )

            if doc_url and not re.match(r"^https?://.*\.(gov\.uk|org\.uk)(/.*)?$", doc_url):
                errors["documentation_url"] = True

            try:
                # Validate that a real date can be constructed
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

                print("✅ FINAL PAYLOAD:")
                print(json.dumps(payload, indent=2))

                try:
                    async_api = f"{get_request_api_endpoint()}/requests"
                    print("🚀 Sending payload to backend:")
                    print(json.dumps(payload, indent=2))
                    response = requests.post(async_api, json=payload, timeout=REQUESTS_TIMEOUT)

                    print("✅ BACKEND RESPONSE:")
                    print("Status Code:", response.status_code)
                    print("Body:", response.text)

                    if response.status_code == 202:
                        request_id = response.json()["id"]
                        return redirect(
                            url_for(
                                "datamanager.check_results",
                                request_id=request_id,
                                organisation=organisation,
                            )
                        )
                    else:
                        abort(500, f"Check tool submission failed with status {response.status_code}")
                except Exception as e:
                    print("🔥 EXCEPTION during backend POST:")
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


@datamanager_bp.route("/dashboard/debug-payload", methods=["POST"])
def debug_payload():
    form = request.form.to_dict()
    dataset_name = form.get("dataset", "").strip()

    # Reuse the same lookup logic to get dataset_id
    try:
        ds_response = requests.get(
            "https://www.planning.data.gov.uk/dataset.json?_labels=on&_size=max",
            timeout=REQUESTS_TIMEOUT,
        ).json()
        datasets = [d for d in ds_response.get("datasets", []) if "collection" in d]
        name_to_dataset_id = {d["name"]: d["dataset"] for d in datasets}
    except Exception as e:
        return f"Error fetching datasets: {e}", 500

    dataset_id = name_to_dataset_id.get(dataset_name)

    if not dataset_id:
        return (f"<p>❌ Dataset ID not found for name: <code>{dataset_name}</code></p>", 400)

    column_mapping_str = form.get("column_mapping", "{}").strip()
    geom_type = form.get("geom_type", "").strip()

    try:
        column_mapping = json.loads(column_mapping_str) if column_mapping_str else {}
    except json.JSONDecodeError:
        column_mapping = {}

    payload = {
        "params": {
            "type": "check_url",
            "collection": dataset_id,
            "dataset": dataset_id,
            "url": form.get("endpoint_url", "").strip(),
        }
    }

    if column_mapping:
        payload["params"]["column_mapping"] = column_mapping
    if geom_type:
        payload["params"]["geom_type"] = geom_type

    return render_template("datamanager/debug_payload.html", payload=payload)


# ---------- Helpers for results ----------

def fetch_all_details(async_api: str, request_id: str, page_size: int = 50):
    """
    Pulls every page of /requests/{id}/response-details and returns a single list.
    """
    rows = []
    offset = 0

    while True:
        url = f"{async_api}/requests/{request_id}/response-details"
        try:
            resp = requests.get(url, params={"offset": offset, "limit": page_size}, timeout=REQUESTS_TIMEOUT)
        except Exception as e:
            print(f"❌ Exception fetching details (offset {offset}): {e}")
            break

        if resp.status_code != 200:
            print(f"❌ Failed to fetch details (offset {offset}):", resp.status_code, resp.text[:300])
            break

        try:
            batch = resp.json()
        except Exception as e:
            print("❌ JSON decode error for details:", e)
            break

        if not batch:
            break

        rows.extend(batch)
        print(f"✅ Fetched {len(batch)} rows (total {len(rows)})")

        if len(batch) < page_size:
            break  # last page
        offset += page_size

    return rows


def build_issue_index(details):
    """
    Flattens per-row issue_logs into a simple list of dictionaries:
    [{entry_number, severity, issue_type, field, description}, ...]
    """
    issues = []
    for row in details or []:
        entry_no = row.get("entry_number")
        for iss in (row.get("issue_logs") or []):
            issues.append(
                {
                    "entry_number": entry_no,
                    "severity": iss.get("severity"),
                    "issue_type": iss.get("issue-type"),
                    "field": iss.get("field"),
                    "description": iss.get("description"),
                }
            )
    return issues


@datamanager_bp.route("/check-results/<request_id>")
def check_results(request_id):
    try:
        async_api = get_request_api_endpoint()

        # 1) Fetch the request envelope (status + summary)
        response = requests.get(f"{async_api}/requests/{request_id}", timeout=REQUESTS_TIMEOUT)
        if response.status_code != 200:
            return "Check failed or not found", 404

        print("✅ Response from backend:")
        print(response.text)
        result = response.json()

        # Keep organisation from querystring for the header if provided
        organisation = request.args.get("organisation")
        if organisation:
            result.setdefault("params", {})
            result["params"]["organisation"] = organisation

        # 2) While NEW/PROCESSING, show the loading screen
        if result.get("status") in ["NEW", "PROCESSING"]:
            return render_template(
                "datamanager/check-results-loading.html",
                result=result,
                request_id=request_id,
            )

        # 3) COMPLETE/FAILED -> fetch details (paginated)
        details_json = fetch_all_details(async_api, request_id, page_size=50)

        # 4) Derive helpers
        issues = build_issue_index(details_json)

        # Build must-fix (blocking) and fixable lists
        blocking = []
        non_blocking = []

        for it in issues:
            label = it.get("description") or f"{it.get('issue_type') or ''} - {it.get('field') or ''}".strip(" -")
            if it.get("severity") == "error":
                blocking.append(label)
            elif it.get("severity") == "warning":
                non_blocking.append(label)

        # Add error-summary as non-blocking
        summary_list = []
        try:
            summary_list = result.get("response", {}).get("data", {}).get("error-summary", []) or []
        except Exception:
            summary_list = []
        non_blocking.extend(summary_list)

        # Remove duplicates
        def uniq(seq):
            seen = set()
            out = []
            for s in seq:
                if s not in seen:
                    out.append(s)
                    seen.add(s)
            return out

        must_fix = uniq(blocking)
        fixable = uniq(non_blocking)

        allow_add_data = len(must_fix) == 0

        # Optional: geometries (for map)
        geometries = None

        # Optional: build columns from first converted_row
        columns = []
        if details_json and isinstance(details_json[0].get("converted_row"), dict):
            columns = list(details_json[0]["converted_row"].keys())

        # 5) Render the normal results template
        return render_template(
            "datamanager/check-results.html",
            result=result,
            detailed_data=details_json,
            columns=columns,
            must_fix=must_fix,
            fixable=fixable,
            allow_add_data=allow_add_data,
            geometries=geometries,
        )

    except Exception as e:
        traceback.print_exc()
        abort(500, "Error fetching results from backend")


# ---------- Static demo page (unchanged, handy for design) ----------
@datamanager_bp.route("/check-results/static")
def check_results_static():
    result = {
        "params": {
            "dataset": "article-4-direction",
            "organisation": "Plymouth City Council",
            "url": "https://example.com/plymouth-data.csv",
        },
        "created": "20 June 2025 at 9:15am",
        "modified": "2025-06-20 09:18:00",
        "status": "COMPLETE",
        "response": {
            "data": {
                "error-summary": [
                    "4 rows have start-date fields not in YYYY-MM-DD format",
                    "2 rows have entry-date fields that must be in the past",
                    "documentation-url must be a valid URL",
                    "44 rows are missing document-url",
                ],
                "column-field-log": [
                    {"field": "reference", "missing": False},
                    {"field": "geometry", "missing": False},
                ],
            }
        },
    }

    # 🟥 Blocking errors (example)
    must_fix = [
        "reference column missing",
        "geometry column missing",
    ]
    # 🟨 Fixable warnings
    fixable = result["response"]["data"]["error-summary"]

    geojson_data = {
        "type": "Feature",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [-4.2, 50.4], [-4.1, 50.4], [-4.1, 50.3], [-4.2, 50.3], [-4.2, 50.4]
            ]],
        },
        "properties": {"name": "Sample Area"},
    }

    return render_template(
        "datamanager/check-results-static.html",
        result=result,
        geometries=geojson_data,
        detailed_data=[],
        show_error=True,
        must_fix=must_fix,
        fixable=fixable,
        allow_add_data=False,
    )


@datamanager_bp.route("/dashboard/config")
def dashboard_config():
    return render_template("datamanager/dashboard_config.html")
