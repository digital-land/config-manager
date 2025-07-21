
import datetime
import os
import requests
import json
import re
from datetime import datetime
from flask import Blueprint, abort, jsonify, redirect, render_template, request, url_for



datamanager_bp = Blueprint("datamanager", __name__, url_prefix="/datamanager")


@datamanager_bp.context_processor
def inject_now():
    return {'now': datetime}

@datamanager_bp.route("/")
def index():
    datamanager = {"name": "Dashboard"}  
    return render_template("datamanager/index.html", datamanager=datamanager)

@datamanager_bp.route('/dashboard/add', methods=['GET', 'POST'])
def dashboard_add():
    ds_response = requests.get("https://www.planning.data.gov.uk/dataset.json").json()
    datasets = ds_response['datasets']
    dataset_options = sorted([d['name'] for d in datasets])
    name_to_dataset_id = {d['name']: d['dataset'] for d in datasets}

    # --- AJAX endpoint: autocomplete datasets ---
    if request.args.get('autocomplete'):
        query = request.args['autocomplete'].lower()
        matches = [name for name in dataset_options if query in name.lower()]
        return jsonify(matches[:10])

    # --- AJAX endpoint: get orgs for selected dataset ---
    if request.args.get('get_orgs_for'):
        dataset_name = request.args['get_orgs_for']
        dataset_id = name_to_dataset_id.get(dataset_name)
        if not dataset_id:
            return jsonify([])

        provision_rows = requests.get(
            "https://datasette.planning.data.gov.uk/digital-land/provision.json?_labels=on"
        ).json()['rows']

        selected_orgs = []
        for row in provision_rows:
            if row['dataset']['value'] == dataset_id:
                org_label = row['organisation']['label']
                org_value = row['organisation']['value'].split(':', 1)[1]
                selected_orgs.append(f"{org_label} ({org_value})")

        return jsonify(selected_orgs)

    # --- Normal GET/POST form handling ---
    provision_rows = requests.get(
        "https://datasette.planning.data.gov.uk/digital-land/provision.json?_labels=on"
    ).json()['rows']

    form = {}
    errors = {}
    selected_orgs = []
    dataset_input = ''
    mode = ''
    dataset_id = None

    if request.method == 'POST':
        form = request.form.to_dict()
        mode = form.get('mode', '').strip()
        dataset_input = form.get('dataset', '').strip()
        dataset_id = name_to_dataset_id.get(dataset_input)

        # 🟡 Always fetch orgs if dataset is selected
        if dataset_id:
            for row in provision_rows:
                if row['dataset']['value'] == dataset_id:
                    org_label = row['organisation']['label']
                    org_value = row['organisation']['value'].split(':', 1)[1]
                    selected_orgs.append(f"{org_label} ({org_value})")

        # 🔁 Lookup mode: skip validation, just return form with orgs loaded
        if mode == 'lookup':
            return render_template(
                'dashboard_add.html',
                dataset_input=dataset_input,
                selected_orgs=selected_orgs,
                form=form,
                errors=errors,
                dataset_options=dataset_options
            )

        # ✅ Final submission: validate form inputs
        if mode == 'final':
            errors = {
                'dataset': not dataset_input,
                'organisation': not form.get('organisation'),
                'endpoint_url': not form.get('endpoint_url'),
            }

            # Endpoint URL check
            endpoint_url = form.get('endpoint_url', '').strip()
            if not re.match(r'https?://[^\s]+', endpoint_url):
                errors['endpoint_url'] = True

            # Documentation URL check
            doc_url = form.get('documentation_url', '').strip()
            if doc_url and not re.match(r"^https?://.*\.(gov\.uk|org\.uk)(/.*)?$", doc_url):
                errors['documentation_url'] = True

            # Start date validation
            day = form.get('start_day', '').strip()
            month = form.get('start_month', '').strip()
            year = form.get('start_year', '').strip()

            if not (day.isdigit() and month.isdigit() and year.isdigit()):
                errors['start_date'] = True
            else:
                try:
                    datetime(int(year), int(month), int(day))
                except ValueError:
                    errors['start_date'] = True

            # ✅ If all OK, return success as JSON
            if not any(errors.values()):
                return jsonify({
                    'dataset': dataset_input,
                    'organisation': form['organisation'],
                    'endpoint_url': endpoint_url,
                    'documentation_url': doc_url,
                    'start_date': f"{year}-{month}-{day}",
                    'licence': form.get('licence')
                })

    # 🧾 Final fallback: render page (GET, or POST with errors)
    return render_template(
        'datamanager/dashboard_add.html',
        dataset_input=dataset_input,
        selected_orgs=selected_orgs,
        form=form,
        errors=errors,
        dataset_options=dataset_options
    )
   

@datamanager_bp.route("/dashboard/config")
def dashboard_config():
    """
    Render the dashboard configuration page.
    """
    return render_template("datamanager/dashboard_config.html")


