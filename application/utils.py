import hashlib

import requests
from flask.json import JSONEncoder
from requests import HTTPError
from sqlalchemy.orm import DeclarativeMeta


def compute_hash(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def compute_md5_hash(value):
    return hashlib.md5(value.encode("utf-8")).hexdigest()


class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj.__class__, DeclarativeMeta):
            return obj.to_dict()
        return super(CustomJSONEncoder, self).default(obj)


def check_url_reachable(url):
    try:
        resp = requests.head(url)
        resp.raise_for_status()
        return True
    except HTTPError as e:
        print(e)
        return False
