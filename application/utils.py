import hashlib

from flask.json import JSONEncoder
from sqlalchemy.orm import DeclarativeMeta


def compute_hash(value):
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj.__class__, DeclarativeMeta):
            return obj.to_dict()
        return super(CustomJSONEncoder, self).default(obj)
