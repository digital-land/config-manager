# -*- coding: utf-8 -*-
"""
Flask app factory class
"""

# import os.path
import logging
import sentry_sdk
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


def register_sentry(app):
    """
    Initialise Sentry error reporting, if a DSN has been configured
    """
    dsn = app.config.get("SENTRY_DSN")

    if not app.config.get("SENTRY_ENABLED") or not dsn:
        app.logger.info("Sentry is not enabled - no errors will be reported")
        return

    sentry_sdk.init(
        dsn=dsn,
        environment=app.config.get("ENVIRONMENT"),
        release=app.config.get("SENTRY_RELEASE"),
        traces_sample_rate=app.config.get("SENTRY_TRACES_SAMPLE_RATE"),
        profiles_sample_rate=app.config.get("SENTRY_PROFILES_SAMPLE_RATE"),
        debug=app.config.get("SENTRY_DEBUG"),
        enable_logs=True,
        # send_default_pii is left off (the SDK default). Turning it on would not
        # identify the user anyway - Sentry's Flask integration reads the user from
        # flask_login, which this app doesn't use - but it would attach cookies,
        # and our signed-cookie session carries a replayable login.
    )

    app.logger.info(
        "Sentry enabled for environment '%s'", app.config.get("ENVIRONMENT")
    )


def create_app(config_filename):
    """
    App factory function
    """
    app = Flask(__name__)
    app.config.from_object(config_filename)
    from config.config import configure_auth_signing_key

    configure_auth_signing_key(app.config)
    app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 10

    configure_logging(app)
    register_sentry(app)

    register_blueprints(app)
    register_context_processors(app)
    register_templates(app)
    register_filters(app)
    register_extensions(app)

    # get_specification(app)

    return app


def register_blueprints(app):
    """
    Import and register blueprints
    """

    from application.blueprints.base.views import base

    app.register_blueprint(base)

    from application.blueprints.auth.views import auth_bp

    app.register_blueprint(auth_bp)

    from application.blueprints.datamanager.router import (
        assign_entities_bp,
        datamanager_bp,
    )

    app.register_blueprint(datamanager_bp)
    app.register_blueprint(assign_entities_bp)


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
    from application.extensions import cache, db, migrate, oauth, talisman

    cache.init_app(app)

    db.init_app(app)
    migrate.init_app(app)
    oauth.init_app(app)

    from flask_sslify import SSLify

    sslify = SSLify(app, skips=["health"])  # noqa

    oauth.register(
        name="github",
        client_id=app.config["GITHUB_CLIENT_ID"],
        client_secret=app.config["GITHUB_CLIENT_SECRET"],
        access_token_url="https://github.com/login/oauth/access_token",
        access_token_params=None,
        authorize_url="https://github.com/login/oauth/authorize",
        authorize_params=None,
        api_base_url=f"{app.config['GITHUB_API_BASE_URL']}/",
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
