from flask_wtf import FlaskForm
from wtforms import RadioField, SelectField, StringField, TextAreaField, ValidationError
from wtforms.validators import URL, DataRequired

from application.models import Dataset


def same_collection(form, field):
    datasets_ids = field.data.split(";")
    if len(datasets_ids) > 1:
        datasets = Dataset.query.filter(Dataset.dataset.in_(datasets_ids)).all()
        collections = set([d.collection for d in datasets])
        if len(collections) > 1:
            raise ValidationError(
                "All the datasets you select must belong to the same collection"
            )


class SearchForm(FlaskForm):
    source = StringField(
        "Source", validators=[DataRequired(message="Enter a source hash")]
    )


class EditSourceForm(FlaskForm):
    documentation_url = StringField("Documentation url")
    attribution = StringField("Attribution")
    licence = StringField("Licence")
    start_date = StringField("Start date")


class NewSourceForm(EditSourceForm):
    endpoint_url = StringField(
        "Url",
        validators=[
            DataRequired(message="Please provide a url"),
            URL(message="Please provide a valid URL"),
        ],
    )
    dataset = StringField(
        "Dataset",
        validators=[
            DataRequired(message="Please provide a dataset"),
            same_collection,
        ],
    )
    organisation = SelectField(
        "Organisation",
        validators=[DataRequired(message="Please provide an organisation ID")],
    )


class ArchiveForm(FlaskForm):
    confirm = RadioField(
        "Are you sure you want to archive the source",
        validators=[DataRequired("You must select one")],
        choices=[
            ("Yes", "I want to archive this source"),
            ("No", "I don't want to archive this source"),
        ],
    )
    notes = TextAreaField("Why are you archiving this source?")
