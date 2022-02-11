from flask import render_template, Blueprint


addsource = Blueprint("addsource", __name__, url_prefix="/add-a-source")


@addsource.route("/")
@addsource.route("/index")
def index():
    return render_template("index.html", title="Add a source")
