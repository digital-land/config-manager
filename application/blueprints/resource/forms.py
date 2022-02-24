from flask_wtf import FlaskForm
from wtforms import StringField
from wtforms.validators import DataRequired


class SearchForm(FlaskForm):
    resource = StringField(
        "Resource", validators=[DataRequired(message="Enter a resource hash")]
    )
