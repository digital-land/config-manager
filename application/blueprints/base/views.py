from flask import Blueprint, render_template

base = Blueprint("base", __name__)


@base.route("/")
@base.route("/index")
def index():
    return render_template("index.html")


@base.route("/healthz")
def healthz():
    return "OK", 200
