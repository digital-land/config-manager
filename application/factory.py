# -*- coding: utf-8 -*-
"""
Flask app factory class
"""

from flask import Flask
from flask.cli import load_dotenv

load_dotenv()


def create_app(config_filename):
    """
    App factory function
    """
    app = Flask(__name__)
    app.config.from_object(config_filename)
    app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 10

    register_blueprints(app)
    register_context_processors(app)
    register_templates(app)
    register_filters(app)
    register_extensions(app)

    return app


def register_blueprints(app):
    """
    Import and register blueprints
    """

    from application.blueprints.base.views import base

    app.register_blueprint(base)

    from application.blueprints.addsource.views import addsource

    app.register_blueprint(addsource)


def register_context_processors(app):
    """
    Add template context variables and functions
    """

    def base_context_processor():
        return {"assetPath": "/static"}

    app.context_processor(base_context_processor)


def register_filters(app):
    from digital_land_frontend.filters import commanum_filter, hex_to_rgb_string_filter

    app.add_template_filter(commanum_filter, name="commanum")
    app.add_template_filter(hex_to_rgb_string_filter, name="hex_to_rgb")


def register_extensions(app):
    """
    Import and register flask extensions and initialize with app object
    """
    pass


def register_templates(app):
    """
    Register templates from packages
    """
    from jinja2 import PackageLoader, PrefixLoader, ChoiceLoader

    multi_loader = ChoiceLoader(
        [
            app.jinja_loader,
            PrefixLoader(
                {
                    "govuk_frontend_jinja": PackageLoader("govuk_frontend_jinja"),
                    "digital-land-frontend": PackageLoader("digital_land_frontend"),
                }
            ),
        ]
    )
    app.jinja_loader = multi_loader
