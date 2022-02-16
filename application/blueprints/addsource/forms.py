from flask_wtf import FlaskForm
from wtforms import SelectField, StringField
from wtforms.validators import URL, DataRequired


class SearchForm(FlaskForm):
    source = StringField(
        "Source", validators=[DataRequired(message="Enter a source hash")]
    )


class SourceForm(FlaskForm):
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
