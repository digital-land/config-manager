from flask_wtf import FlaskForm
from wtforms import SelectField, StringField


class EditRuleForm(FlaskForm):
    dataset_id = StringField("Dataset")
    column = StringField("Column")
    endpoint = StringField("Endpoint")
    field = SelectField("Field")
    resource = StringField("Resource")
    separator = StringField("Separator")
    entry_date = StringField("Entry date")
    start_date = StringField("Start date")
    end_date = StringField("End date")
