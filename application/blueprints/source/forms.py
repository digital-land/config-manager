from flask_wtf import FlaskForm
from wtforms import RadioField, SelectField, StringField
from wtforms.validators import URL, DataRequired


class SearchForm(FlaskForm):
    source = StringField(
        "Source", validators=[DataRequired(message="Enter a source hash")]
    )


class NewSourceForm(FlaskForm):
    endpoint = StringField(
        "Url",
        validators=[
            DataRequired(message="Please provide a url"),
            URL(message="Please provide a valid URL"),
        ],
    )
    dataset = SelectField(
        "Dataset", validators=[DataRequired(message="Please provide a dataset")]
    )
    organisation = SelectField(
        "Organisation",
        validators=[DataRequired(message="Please provide an organisation ID")],
    )


class SourceForm(NewSourceForm):
    documentation_url = StringField("Documentation url")
    attribution = StringField("Attribution")
    licence = StringField("Licence")
    start_date = StringField("Start date")


class ArchiveForm(FlaskForm):
    confirm = RadioField(
        "Are you sure you want to archive the source",
        validators=[DataRequired("You must select one")],
        choices=[
            ("Yes", "I want to archive this source"),
            ("No", "I don't want to archive this source"),
        ],
    )
