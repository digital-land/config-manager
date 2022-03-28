from flask_wtf import FlaskForm
from wtforms import SelectField, StringField
from wtforms.validators import DataRequired


class SearchForm(FlaskForm):
    resource = StringField(
        "Resource", validators=[DataRequired(message="Enter a resource hash")]
    )


class MappingForm(FlaskForm):
    column = SelectField(
        "Column",
        validators=[
            DataRequired(message="Enter a column name represent in the resource")
        ],
    )
    field = SelectField(
        "Field", validators=[DataRequired(message="Enter field name from schema")]
    )
