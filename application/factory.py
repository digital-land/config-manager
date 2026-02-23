# -*- coding: utf-8 -*-
"""
Flask app factory class
"""

# import os.path
# import sentry_sdk
import logging
from flask import Flask
from flask.cli import load_dotenv

from application.db.models import *  # noqa

load_dotenv()


DIGITAL_LAND_GITHUB_URL = "https://raw.githubusercontent.com/digital-land"


def configure_logging(app):
    """
    Configure logging for the application
    """
    # Set up logging format
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(log_format)

    # Configure root logger
    root_logger = logging.getLogger()

    # Set log level based on environment
    if app.config.get("TESTING"):
        log_level = logging.DEBUG
    elif app.config.get("DEBUG"):
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    root_logger.setLevel(log_level)

    # Console handler
    if not root_logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    # Configure application logger
    app.logger.setLevel(log_level)


def create_app(config_filename):
    """
    App factory function
    """
    app = Flask(__name__)
    app.config.from_object(config_filename)
    app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 10
    app.config["DEBUG"] = True

    configure_logging(app)

    register_blueprints(app)
    register_context_processors(app)
    register_templates(app)
    register_filters(app)
    register_extensions(app)
    register_commands(app)

    # get_specification(app)

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

    from application.blueprints.auth.views import auth_bp

    app.register_blueprint(auth_bp)

    from application.blueprints.dataset.views import dataset_bp

    app.register_blueprint(dataset_bp)

    from application.blueprints.datamanager.router import datamanager_bp

    app.register_blueprint(datamanager_bp)

    from application.blueprints.schema.views import schema_bp

    app.register_blueprint(schema_bp)

    from application.blueprints.report.views import report_bp

    app.register_blueprint(report_bp)

    from application.blueprints.publisher.views import publisher_pages

    app.register_blueprint(publisher_pages)


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

    from application.filters import (
        clean_int_filter,
        days_since,
        remove_query_param_filter,
        render_field_value,
        split_filter,
    )

    app.add_template_filter(commanum_filter, name="commanum")
    app.add_template_filter(hex_to_rgb_string_filter, name="hex_to_rgb")
    app.add_template_filter(make_link_filter, name="makelink")
    app.add_template_filter(render_field_value, name="render_field_value")
    app.add_template_filter(split_filter, name="split")
    app.add_template_filter(clean_int_filter, name="to_int")
    app.add_template_filter(days_since, name="days_since")
    app.add_template_filter(remove_query_param_filter, name="remove_query_param")


def register_extensions(app):
    """
    Import and register flask extensions and initialize with app object
    """
    from application.extensions import db, migrate, oauth, talisman

    db.init_app(app)
    migrate.init_app(app)
    oauth.init_app(app)

    from flask_sslify import SSLify

    sslify = SSLify(app)  # noqa

    oauth.register(
        name="github",
        client_id=app.config["GITHUB_CLIENT_ID"],
        client_secret=app.config["GITHUB_CLIENT_SECRET"],
        access_token_url="https://github.com/login/oauth/access_token",
        access_token_params=None,
        authorize_url="https://github.com/login/oauth/authorize",
        authorize_params=None,
        api_base_url="https://api.github.com/",
        client_kwargs={"scope": "user:email read:org"},
    )

    if app.config["ENV"] == "production":
        # content security policy for talisman
        # SELF = "'self'"
        # csp = {
        #     "font-src": SELF,
        #     "script-src": [
        #         SELF,
        #         "*.google-analytics.com",
        #         "'sha256-+6WnXIl4mbFTCARd8N3COQmT3bJJmo32N8q8ZSQAIcU='",
        #         "'sha256-vTIO5fI4O36AP9+OzV3oS3SxijRPilL7mJYDUwnwwqk='",
        #         "'sha256-icLIt+1VXFav7q50YdfAHSFYWsMvSawaYWwo5ocWp5A='",
        #         "'sha256-ACotEtBlkqjCUAsddlA/3p2h7Q0iHuDXxk577uNsXwA='",
        #     ],
        #     "style-src": [
        #         SELF,
        #         "'unsafe-hashes'",
        #         "'sha256-biLFinpqYMtWHmXfkA1BPeCY0/fNt46SAZ+BBk5YUog='",
        #     ],
        #     "default-src": SELF,
        #     "connect-src": [SELF, "*.google-analytics.com", "*.doubleclick.net"],
        #     "img-src": [SELF, "*.google.co.uk", "*.google.com"],
        # }

        talisman.init_app(
            app,
            content_security_policy=None,
            # content_security_policy_nonce_in=["script-src", "style-src"],
        )


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
    from application.commands import publish_cli
    from application.data_commands import data_cli

    app.cli.add_command(data_cli)
    app.cli.add_command(publish_cli)
