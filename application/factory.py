# -*- coding: utf-8 -*-
"""
Flask app factory class
"""

from flask import Flask
from flask.cli import load_dotenv

from application.models import *  # noqa
from application.utils import CustomJSONEncoder

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
    register_commands(app)

    app.json_encoder = CustomJSONEncoder

    return app


def register_blueprints(app):
    """
    Import and register blueprints
    """

    from application.blueprints.base.views import base

    app.register_blueprint(base)

    from application.blueprints.source.views import source_bp

    app.register_blueprint(source_bp)

    from application.blueprints.endpoint.views import endpoint_bp

    app.register_blueprint(endpoint_bp)

    from application.blueprints.resource.views import resource_bp

    app.register_blueprint(resource_bp)


def register_context_processors(app):
    """
    Add template context variables and functions
    """

    def base_context_processor():
        return {"assetPath": "/static"}

    app.context_processor(base_context_processor)


def register_filters(app):
    from digital_land_frontend.filters import (
        commanum_filter,
        hex_to_rgb_string_filter,
        make_link_filter,
    )

    app.add_template_filter(commanum_filter, name="commanum")
    app.add_template_filter(hex_to_rgb_string_filter, name="hex_to_rgb")
    app.add_template_filter(make_link_filter, name="makelink")

    from application.filters import datasets_string_filter

    app.add_template_filter(datasets_string_filter, name="datasets_string")


def register_extensions(app):
    """
    Import and register flask extensions and initialize with app object
    """
    from application.extensions import db, migrate

    db.init_app(app)
    migrate.init_app(app=app)


def register_templates(app):
    """
    Register templates from packages
    """
    from jinja2 import ChoiceLoader, PackageLoader, PrefixLoader

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


def register_commands(app):

    from application.commands import management_cli

    app.cli.add_command(management_cli)
