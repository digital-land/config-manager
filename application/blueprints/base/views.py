from flask import render_template, Blueprint


base = Blueprint("base", __name__)


@base.route("/")
@base.route("/index")
def index():
    return render_template("index.html")
