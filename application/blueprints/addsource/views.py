from flask import render_template, Blueprint

from application.blueprints.addsource.forms import SourceForm

addsource = Blueprint("addsource", __name__, url_prefix="/add-a-source")


@addsource.route("/", methods=["GET", "POST"])
def index():
    form = SourceForm()
    if form.validate_on_submit():
        pass
    return render_template("source/create.html", form=form)
