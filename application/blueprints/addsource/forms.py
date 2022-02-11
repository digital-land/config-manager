from flask_wtf import FlaskForm
from wtforms import StringField
from wtforms.validators import DataRequired, URL


class SourceForm(FlaskForm):
    endpoint = StringField(
        "Url",
        validators=[
            DataRequired(message="Please provide a url"),
            URL(message="Please provide a valid URL"),
        ],
    )
    dataset = StringField(
        "Dataset", validators=[DataRequired(message="Please provide a dataset")]
    )
    organisation = StringField(
        "Organisation ID",
        validators=[DataRequired(message="Please provide an organisation ID")],
    )
