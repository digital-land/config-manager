import datetime

from application.extensions import db


def render_field_value(obj, attr):
    field_value = getattr(obj, attr)
    if isinstance(field_value, db.Model):
        return getattr(field_value, attr)
    return field_value


def split_filter(s, d):
    return s.split(d)


def clean_int_filter(s):
    # s might be passed in as undefined so need to test for that
    if not s or s == "":
        s = "0"
    if isinstance(s, str):
        s = s.replace(",", "")
        return int(s)
    return s


def days_since(d):
    today = datetime.datetime.now()
    if not isinstance(d, datetime.datetime):
        d = datetime.datetime.strptime(d, "%Y-%m-%d")
    delta = today - d
    return delta.days


def remove_query_param_filter(v, filter_name, current_str):
    query_str = str(current_str)
    if f"{filter_name}={v}" in query_str:
        s = query_str.replace(f"{filter_name}={v}", "")
        return "?" + s.strip("&")
    return "?" + query_str
