import csv
import hashlib
import io
from functools import wraps

import requests
from requests import HTTPError


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


def csv_data_to_buffer(csv_rows):
    out = io.StringIO()
    fieldnames = csv_rows[0].keys()
    writer = csv.DictWriter(
        out, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n"
    )
    writer.writeheader()
    for row in csv_rows:
        writer.writerow(row)
    buffer = io.BytesIO()
    buffer.write(out.getvalue().encode())
    buffer.seek(0)
    out.close()
    return buffer


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        from flask import current_app, redirect, request, session, url_for

        if current_app.config.get("AUTHENTICATION_ON", True):
            if session.get("user") is None:
                return redirect(url_for("auth.login", next=request.url))
        return f(*args, **kwargs)

    return decorated_function
