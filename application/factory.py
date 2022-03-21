# -*- coding: utf-8 -*-
"""
Flask app factory class
"""
import os.path

import requests
from flask import Flask
from flask.cli import load_dotenv

from application.models import *  # noqa
from application.utils import CustomJSONEncoder

load_dotenv()


DIGITAL_LAND_GITHUB_URL = "https://raw.githubusercontent.com/digital-land"


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

    get_specification(app)
    get_organisations(app)

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

    from application.blueprints.collection.views import collection_bp

    app.register_blueprint(collection_bp)

    from application.blueprints.auth.views import auth_bp

    app.register_blueprint(auth_bp)

    from application.blueprints.pipeline.views import pipeline_bp

    app.register_blueprint(pipeline_bp)


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
    from application.extensions import db, migrate, oauth, talisman

    db.init_app(app)
    migrate.init_app(app)
    oauth.init_app(app)

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
        SELF = "'self'"
        csp = {
            "font-src": SELF,
            "script-src": [
                SELF,
                "*.google-analytics.com",
                "'sha256-+6WnXIl4mbFTCARd8N3COQmT3bJJmo32N8q8ZSQAIcU='",
                "'sha256-vTIO5fI4O36AP9+OzV3oS3SxijRPilL7mJYDUwnwwqk='",
                "'sha256-icLIt+1VXFav7q50YdfAHSFYWsMvSawaYWwo5ocWp5A='",
            ],
            "style-src": [
                SELF,
                "'unsafe-hashes'",
                "'sha256-biLFinpqYMtWHmXfkA1BPeCY0/fNt46SAZ+BBk5YUog='",
            ],
            "default-src": SELF,
            "connect-src": ["*.google-analytics.com", "*.doubleclick.net"],
            "img-src": [SELF, "*.google.co.uk", "*.google.com"],
        }

        talisman.init_app(
            app,
            content_security_policy=csp,
            content_security_policy_nonce_in=["script-src"],
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

    from application.commands import management_cli

    app.cli.add_command(management_cli)


def get_specification(app):

    specification_dir = os.path.join(app.config["PROJECT_ROOT"], "specification")
    if not os.path.exists(specification_dir):
        os.mkdir(specification_dir)

    specification_files = [
        "collection",
        "dataset",
        "dataset-schema",
        "schema",
        "schema-field",
        "field",
        "datatype",
        "typology",
        "pipeline",
        "theme",
    ]

    for file in specification_files:

        spec_file = os.path.join(specification_dir, f"{file}.csv")
        spec_url = (
            f"{DIGITAL_LAND_GITHUB_URL}/specification/main/specification/{file}.csv"
        )

        if not os.path.exists(spec_file):
            print(f"Downloading {spec_url} to {spec_file}")
            resp = requests.get(spec_url)
            resp.raise_for_status()
            outfile_name = os.path.join(specification_dir, f"{file}.csv")
            with open(outfile_name, "w") as file:
                file.write(resp.content.decode("utf-8"))
        else:
            print(f"{spec_url} already downloaded to {spec_file}")

    print("Specification done")


def get_organisations(app):

    ORGANISATION_URL = f"{DIGITAL_LAND_GITHUB_URL}/organisation-dataset/main/collection/organisation.csv"

    organisation_dir = os.path.join(app.config["PROJECT_ROOT"], "var/cache")
    if not os.path.exists(organisation_dir):
        os.makedirs(organisation_dir)

    organisation_file = os.path.join(organisation_dir, "organisation.csv")

    if not os.path.exists(organisation_file):
        print(f"Downloading {ORGANISATION_URL} to {organisation_file}")
        resp = requests.get(ORGANISATION_URL)
        resp.raise_for_status()
        with open(organisation_file, "w") as file:
            file.write(resp.content.decode("utf-8"))
    else:
        print(f"{ORGANISATION_URL} already downloaded to {organisation_file}")
