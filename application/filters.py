from application.extensions import db


def render_field_value(obj, attr):
    field_value = getattr(obj, attr)
    if isinstance(field_value, db.Model):
        return getattr(field_value, attr)
    return field_value


def csp_nonce():
    return "8IBTHwOdqNKAWeKl7plt8g=="
