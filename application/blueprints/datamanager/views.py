
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


@datamanager_bp.route('/dashboard/add', methods=['GET','POST'])
def dashboard_add():
    # Fetch datasets & provision map
    ds = requests.get("https://www.planning.data.gov.uk/dataset.json").json()['datasets']
    pr = requests.get(
        "https://datasette.planning.data.gov.uk/digital-land/provision.json?_labels=on"
    ).json()['rows']
    provision_map = {}
    for r in pr:
        label = r['dataset']['label']
        org_full = r['organisation']['value']
        org_name = org_full.split(':', 1)[1]
        org_lab = r['organisation']['label']
        provision_map.setdefault(label, []).append(f"{org_lab} ({org_name})")

    dataset_input = ''
    selected_orgs = []
    form = {}
    errors = {}
    mode = ''

    if request.method == 'POST':
        # Determine mode: 'lookup' or 'final'
        form = request.form.to_dict()
        mode = form.get('mode', '')
        dataset_input = form.get('dataset', '').strip()
        selected_orgs = provision_map.get(dataset_input, []) if dataset_input else []

        # Only validate on final submission
        if mode == 'final':
            errors = {
                'dataset': not dataset_input,
                'organisation': not form.get('organisation'),
                'endpoint_url': not form.get('endpoint_url'),
            }

            endpoint_url = form.get('endpoint_url', '').strip()

            if  not endpoint_url or not re.match(r'https?:\/\/[^\s+]', endpoint_url):
             errors['endpoint_url'] = True

            doc_url = form.get('documentation_url', '').strip()
            if doc_url:
                # Allow empty, but if filled, must match .gov.uk or .org.uk
                if not re.match(r"^https?://.*\.(gov\.uk|org\.uk)(/.*)?$", doc_url):
                    errors['documentation_url'] = True

   
            day = form.get('start_day', '').strip()
            month = form.get('start_month', '').strip()
            year = form.get('start_year', '').strip()

            # Validating the start date
            if not (day.isdigit() and month.isdigit() and year.isdigit()):
                errors['start_date'] = True  # 'Date must contain only digits in DD/MM/YYYY format.'
            try:
                dd = int(day)
                mm = int(month)
                yyyy = int(year)
            # Try to construct a date
                date_obj = datetime(yyyy, mm, dd)
            except ValueError:
                errors['start_date'] = True  # 'Enter a valid date in DD/MM/YYYY

            if not any(errors.values()):
                return jsonify({
                    'dataset': dataset_input,
                    'organisation': form['organisation'],
                    'endpoint_url': form['endpoint_url'],
                    'documentation_url': doc_url,
                    'start_date': f"{form['start_year']}-{form['start_month']}-{form['start_day']}",
                    'licence': form.get('licence')
                })
           
    # Render the form in either GET, lookup, or final-with-errors mode
    return render_template(
        'datamanager/dashboard_add.html',
        dataset_input=dataset_input,
        selected_orgs=selected_orgs,
        form=form,
        errors=errors
    )


@datamanager_bp.route("/dashboard/config")
def dashboard_config():
    """
    Render the dashboard configuration page.
    """
    return render_template("datamanager/dashboard_config.html")


