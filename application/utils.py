import csv
import datetime
import hashlib
import io
from functools import wraps

import requests
from dateutil.relativedelta import relativedelta
from requests import HTTPError

from config.config import Config


def compute_hash(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def compute_md5_hash(value):
    return hashlib.md5(value.encode("utf-8")).hexdigest()


def check_url_reachable(url):
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.ok
    except HTTPError as e:
        print(e)
        return False


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        from flask import current_app, redirect, request, session, url_for

        if current_app.config.get("AUTHENTICATION_ON", True):
            if session.get("user") is None:
                return redirect(url_for("auth.login", next=request.url))
        return f(*args, **kwargs)

    return decorated_function


def csv_dict_to_string(csv_rows):
    out = io.StringIO()
    fieldnames = csv_rows[0].keys()
    writer = csv.DictWriter(
        out, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL, lineterminator="\n"
    )
    writer.writeheader()
    for row in csv_rows:
        writer.writerow(row)
    content = out.getvalue()
    out.close()
    return content


def index_by(key_field, dict_list):
    idx = {}
    for d in dict_list:
        if key_field in d.keys():
            idx.setdefault(d[key_field], {})
            idx[d[key_field]] = d
    return idx


def this_month():
    n = datetime.datetime.now()
    return datetime.strptime(n, "%Y-%m")


def months_since(start_date):
    end_date = datetime.datetime.now()
    return (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)


def month_dict(num_months):
    counts = {}
    today = datetime.datetime.now()
    for m in list(reversed(range(0, num_months + 1))):
        counts.setdefault((today - relativedelta(months=m)).strftime("%Y-%m"), 0)
    return counts


def recent_dates(days=1, frmt="%Y-%m-%d"):
    today = datetime.datetime.now()
    return [(today - datetime.timedelta(d)).strftime(frmt) for d in range(1, days + 1)]


def yesterday(string=False, frmt="%Y-%m-%d"):
    yesterday = datetime.datetime.now() - datetime.timedelta(1)
    if string:
        return yesterday.strftime(frmt)
    return yesterday


def resources_per_publishers(resources):
    publishers = {}
    for resource in resources:
        publishers.setdefault(resource["organisation"], [])
        if not resource["resource"] in publishers[resource["organisation"]]:
            publishers[resource["organisation"]].append(resource["resource"])
    return publishers


def filter_off_btns(filters):
    # used by all index pages with filter options
    btns = []
    for filter, value in filters.items():
        filters_copy = filters.copy()
        del filters_copy[filter]
        btns.append({"filter": filter, "value": value, "url_params": filters_copy})
    return btns


def create_dict(keys_list, values_list):
    zip_iterator = zip(keys_list, values_list)
    return dict(zip_iterator)


def index_with_list(key_field, dict_list):
    idx = {}
    for d in dict_list:
        if key_field in d.keys():
            idx.setdefault(d[key_field], [])
            idx[d[key_field]].append(d)
    return idx


def split_organisation_id(organisation):
    parts = organisation.split(":")
    return parts[0], parts[1]


def get_request_api_endpoint():
    """
    Returns the async request backend API endpoint based on the ENVIRONMENT variable.
    ENVIRONMENT: local | development | staging | production
    Default environment is local
    """
    env = Config.ENVIRONMENT

    mapping = {
        "local": "http://localhost:8000",
        "development": "http://development-pub-async-api-lb-69142969.eu-west-2.elb.amazonaws.com",
        "staging": "http://staging-pub-async-api-lb-12493311.eu-west-2.elb.amazonaws.com",
        "production": "http://development-pub-async-api-lb-69142969.eu-west-2.elb.amazonaws.com",
    }

    return mapping.get(env, mapping["local"])
