import re
from datetime import datetime

import requests
from flask import Blueprint, jsonify, render_template, request

datamanager_bp = Blueprint("datamanager", __name__, url_prefix="/datamanager")


@datamanager_bp.context_processor
def inject_now():
    return {"now": datetime}


@datamanager_bp.route("/")
def index():
    datamanager = {"name": "Dashboard"}
    return render_template("datamanager/index.html", datamanager=datamanager)


@datamanager_bp.route("/dashboard/add", methods=["GET", "POST"])
def dashboard_add():
    # Fetch dataset list
    # Fetch dataset list with no row cap
    ds_response = requests.get(
        "https://www.planning.data.gov.uk/dataset.json?_labels=on&_size=max"
    ).json()
    datasets = [d for d in ds_response["datasets"] if "collection" in d]

    dataset_options = sorted([d["name"] for d in datasets])
    name_to_dataset_id = {d["name"]: d["dataset"] for d in datasets}

    # --- AJAX endpoint: autocomplete datasets ---
    if request.args.get("autocomplete"):
        query = request.args["autocomplete"].lower()
        matches = [name for name in dataset_options if query in name.lower()]
        return jsonify(matches[:10])

    # --- AJAX endpoint: get orgs for selected dataset ---
    if request.args.get("get_orgs_for"):
        dataset_name = request.args["get_orgs_for"]
        dataset_id = name_to_dataset_id.get(dataset_name)
        if not dataset_id:
            return jsonify([])

        # ✅ Use _size=max to avoid row cap
        provision_url = (
            f"https://datasette.planning.data.gov.uk/digital-land/provision.json"
            f"?_labels=on&_size=max&dataset={dataset_id}"
        )
        provision_rows = requests.get(provision_url).json().get("rows", [])

        selected_orgs = []
        for row in provision_rows:
            org_label = row["organisation"]["label"]
            org_value = row["organisation"]["value"].split(":", 1)[1]
            selected_orgs.append(f"{org_label} ({org_value})")

        return jsonify(selected_orgs)

    # --- Normal GET/POST form handling ---
    form = {}
    errors = {}
    selected_orgs = []
    dataset_input = ""
    mode = ""
    dataset_id = None

    if request.method == "POST":
        form = request.form.to_dict()
        mode = form.get("mode", "").strip()
        dataset_input = form.get("dataset", "").strip()
        dataset_id = name_to_dataset_id.get(dataset_input)

        # ✅ Use _size=max in POST as well
        if dataset_id:
            provision_url = (
                f"https://datasette.planning.data.gov.uk/digital-land/provision.json"
                f"?_labels=on&_size=max&dataset={dataset_id}"
            )
            provision_rows = requests.get(provision_url).json().get("rows", [])

            for row in provision_rows:
                org_label = row["organisation"]["label"]
                org_value = row["organisation"]["value"].split(":", 1)[1]
                selected_orgs.append(f"{org_label} ({org_value})")

        # Fallback: assign 'None' if no orgs found and none entered
        if dataset_id and not selected_orgs and not form.get("organisation"):
            form["organisation"] = "None"

        # Handle lookup mode: skip validation
        if mode == "lookup":
            return render_template(
                "dashboard_add.html",
                dataset_input=dataset_input,
                selected_orgs=selected_orgs,
                form=form,
                errors=errors,
                dataset_options=dataset_options,
            )

        # Final mode: validate input
        if mode == "final":
            organisation = form.get("organisation", "").strip()
            endpoint_url = form.get("endpoint_url", "").strip()
            doc_url = form.get("documentation_url", "").strip()
            day = form.get("start_day", "").strip()
            month = form.get("start_month", "").strip()
            year = form.get("start_year", "").strip()

            org_warning = form.get("org_warning", "false") == "true"

            # Validate fields
            errors = {
                "dataset": not dataset_input,
                "organisation": (
                    org_warning
                    or (selected_orgs and organisation not in selected_orgs)
                    or (not selected_orgs and organisation != "None")
                ),
                "endpoint_url": not endpoint_url
                or not re.match(r"https?://[^\s]+", endpoint_url),
            }

            # Validate optional documentation URL
            if doc_url and not re.match(
                r"^https?://.*\.(gov\.uk|org\.uk)(/.*)?$", doc_url
            ):
                errors["documentation_url"] = True

            # Validate date fields
            try:
                datetime(int(year), int(month), int(day))
            except (ValueError, TypeError):
                errors["start_date"] = True

            # ✅ If all inputs pass validation, return JSON
            if not any(errors.values()):
                return jsonify(
                    {
                        "dataset": dataset_input,
                        "organisation": organisation,
                        "endpoint_url": endpoint_url,
                        "documentation_url": doc_url,
                        "start_date": f"{year}-{month}-{day}",
                        "licence": form.get("licence"),
                    }
                )

    # Default GET or failed POST
    return render_template(
        "datamanager/dashboard_add.html",
        dataset_input=dataset_input,
        selected_orgs=selected_orgs,
        form=form,
        errors=errors,
        dataset_options=dataset_options,
    )


@datamanager_bp.route("/dashboard/config")
def dashboard_config():
    """
    Render the dashboard configuration page.
    """
    return render_template("datamanager/dashboard_config.html")
