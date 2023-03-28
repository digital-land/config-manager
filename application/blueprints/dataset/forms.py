from flask_wtf import FlaskForm
from wtforms import SelectField, StringField


class BaseForm(FlaskForm):
    dataset_id = StringField("Dataset")
    entry_date = StringField("Entry date")
    start_date = StringField("Start date")
    end_date = StringField("End date")


class EditColumnForm(BaseForm):
    column = StringField("Column")
    endpoint = StringField("Endpoint")
    field = SelectField("Field")
    resource = StringField("Resource")


class EditCombineForm(BaseForm):
    column = StringField("Column")
    endpoint = StringField("Endpoint")
    field = SelectField("Field")
    resource = StringField("Resource")


class EditConcatForm(FlaskForm):
    pass


class EditConvertForm(FlaskForm):
    pass


PIPELINE_FORMS = {
    "column": EditColumnForm,
    "combine": EditCombineForm,
    "concat": EditConcatForm,
    "convert": EditConvertForm,
    "default": None,
    "default-value": None,
    "filter": None,
    "lookup": None,
    "patch": None,
    "skip": None,
    "transform": None,
}
