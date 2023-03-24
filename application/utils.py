import hashlib
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


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        from flask import current_app, redirect, request, session, url_for

        if current_app.config.get("AUTHENTICATION_ON", True):
            if session.get("user") is None:
                return redirect(url_for("auth.login", next=request.url))
        return f(*args, **kwargs)

    return decorated_function
