from flask_wtf import FlaskForm
from wtforms import RadioField, SelectField, StringField, TextAreaField
from wtforms.validators import URL, DataRequired


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
    # dataset = SelectField(
    #     "Dataset", validators=[DataRequired(message="Please provide a dataset")]
    # )
    dataset = StringField(
        "Dataset", validators=[DataRequired(message="Please provide a dataset")]
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
