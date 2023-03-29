from flask_wtf import FlaskForm
from wtforms import SelectField, StringField, validators


def _none_filter(val):
    if not val:
        return None
    return val


class EditRuleForm(FlaskForm):
    dataset_id = StringField(
        "Dataset", validators=[validators.Optional()], filters=[_none_filter]
    )
    column = StringField(
        "Column", validators=[validators.Optional()], filters=[_none_filter]
    )
    endpoint_id = StringField(
        "Endpoint", validators=[validators.Optional()], filters=[_none_filter]
    )
    field_id = SelectField("Field", choices=[], validate_choice=False)
    resource = StringField(
        "Resource", validators=[validators.Optional()], filters=[_none_filter]
    )
    separator = StringField(
        "Separator", validators=[validators.Optional()], filters=[_none_filter]
    )
    entry_date = StringField(
        "Entry date", validators=[validators.Optional()], filters=[_none_filter]
    )
    start_date = StringField(
        "Start date", validators=[validators.Optional()], filters=[_none_filter]
    )
    end_date = StringField(
        "End date", validators=[validators.Optional()], filters=[_none_filter]
    )
