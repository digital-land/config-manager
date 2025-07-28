import json
import re
import traceback
from datetime import datetime

import requests
from flask import Blueprint, abort, jsonify, redirect, render_template, request, url_for

from application.utils import get_request_api_endpoint

datamanager_bp = Blueprint("datamanager", __name__, url_prefix="/datamanager")
headers = {"Content-Type": "application/json", "Accept": "application/json"}


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
            "https://www.planning.data.gov.uk/dataset.json?_labels=on&_size=max"
        ).json()
    except Exception as e:
        print("Error fetching datasets:", e)
        abort(500, "Failed to fetch dataset list")

    datasets = [d for d in ds_response["datasets"] if "collection" in d]
    dataset_options = sorted([d["name"] for d in datasets])
    name_to_dataset_id = {d["name"]: d["dataset"] for d in datasets}

    # --- Autocomplete support ---
    if request.args.get("autocomplete"):
        query = request.args["autocomplete"].lower()
        matches = [name for name in dataset_options if query in name.lower()]
        return jsonify(matches[:10])

    # --- Get orgs for dataset ---
    if request.args.get("get_orgs_for"):
        dataset_name = request.args["get_orgs_for"]
        dataset_id = name_to_dataset_id.get(dataset_name)
        if not dataset_id:
            return jsonify([])
        provision_url = f"https://datasette.planning.data.gov.uk/digital-land/provision.json?_labels=on&_size=max&dataset={dataset_id}"
        provision_rows = requests.get(provision_url).json().get("rows", [])
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
            column_mapping = (
                json.loads(column_mapping_str) if column_mapping_str else {}
            )
        except json.JSONDecodeError:
            errors["column_mapping"] = True
            column_mapping = {}

        if dataset_id:
            provision_url = f"https://datasette.planning.data.gov.uk/digital-land/provision.json?_labels=on&_size=max&dataset={dataset_id}"
            provision_rows = requests.get(provision_url).json().get("rows", [])
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
                    "organisation": (
                        org_warning
                        or (selected_orgs and organisation not in selected_orgs)
                    ),
                    "endpoint_url": not endpoint_url
                    or not re.match(r"https?://[^\s]+", endpoint_url),
                }
            )

            if doc_url and not re.match(
                r"^https?://.*\.(gov\.uk|org\.uk)(/.*)?$", doc_url
            ):
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

                print("✅ FINAL PAYLOAD:")
                print(json.dumps(payload, indent=2))

                # ✅ Send to backend
                try:
                    async_api = f"{get_request_api_endpoint()}/requests"
                    # ✅ Log the payload right before sending it
                    print("🚀 Sending payload to backend:")
                    print(json.dumps(payload, indent=2))
                    response = requests.post(async_api, json=payload)

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
                        abort(
                            500,
                            f"Check tool submission failed with status {response.status_code}",
                        )
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
            "https://www.planning.data.gov.uk/dataset.json?_labels=on&_size=max"
        ).json()
        datasets = [d for d in ds_response["datasets"] if "collection" in d]
        name_to_dataset_id = {d["name"]: d["dataset"] for d in datasets}
    except Exception as e:
        return f"Error fetching datasets: {e}", 500

    dataset_id = name_to_dataset_id.get(dataset_name)

    if not dataset_id:
        return (
            f"<p>❌ Dataset ID not found for name: <code>{dataset_name}</code></p>",
            400,
        )

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


@datamanager_bp.route("/check-results/<request_id>")
def check_results(request_id):
    try:
        async_api = get_request_api_endpoint()
        response = requests.get(f"{async_api}/requests/{request_id}")
        if response.status_code != 200:
            return "Check failed or not found", 404
        print("✅ Response from backend:")
        print(response.text)
        result = response.json()
        # organisation = request.args.get("organisation")
        # if organisation:
        #     result["params"]["organisation"] = organisation

        details_response = requests.get(
            f"{async_api}/requests/{request_id}/response-details"
        )

        if details_response.status_code == 200:
            try:
                print(details_response.text)
                details_json = details_response.json()

                print("✅ details_json loaded:", json.dumps(details_json[:1], indent=2))
            except Exception as e:
                print("❌ JSON decode error:", e)
                detailed_data = []
        else:
            print("❌ Failed to get response-details:", details_response.status_code)
            detailed_data = []

        return render_template(
            "datamanager/check-results.html", result=result, detailed_data=details_json
        )

    except Exception as e:
        traceback.print_exc()
        abort(500, "Error fetching results from backend")


@datamanager_bp.route("/dashboard/config")
def dashboard_config():
    return render_template("datamanager/dashboard_config.html")
