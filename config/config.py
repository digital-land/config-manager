# -*- coding: utf-8 -*-
import base64
import os

basedir = os.path.abspath(os.path.dirname(__file__))


class Config:
    ENV = "production"
    APP_ROOT = os.path.abspath(os.path.dirname(__file__))
    PROJECT_ROOT = os.path.abspath(os.path.join(APP_ROOT, os.pardir))
    SECRET_KEY = os.getenv("SECRET_KEY")
    DATABASE_URL = os.getenv("DATABASE_URL")
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://")
    SQLALCHEMY_DATABASE_URI = DATABASE_URL
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    GITHUB_CLIENT_ID = os.getenv("GITHUB_CLIENT_ID")
    GITHUB_CLIENT_SECRET = os.getenv("GITHUB_CLIENT_SECRET")
    GITHUB_APP_ID = os.getenv("GITHUB_APP_ID")
    GITHUB_APP_INSTALLATION_ID = os.getenv("GITHUB_APP_INSTALLATION_ID")
    GITHUB_APP_PRIVATE_KEY = base64.b64decode(
        os.getenv("GITHUB_APP_PRIVATE_KEY", "")
    ).decode("utf-8")
    SAFE_URLS = set(os.getenv("SAFE_URLS", "").split(","))
    AUTHENTICATION_ON = True
    S3_BUCKET_URL = (
        "https://digital-land-production-collection-dataset.s3.eu-west-2.amazonaws.com"
    )
    # Config repo branch to commit to. If/when this application is used
    # to edit config then change to push to main branch - until then default to update-test
    CONFIG_REPO_BRANCH = os.getenv("CONFIG_REPO_BRANCH", "config-manager-update")
    ENVIRONMENT = os.getenv("ENVIRONMENT", "local").lower()

    # Datasette base URL
    DATASETTE_BASE_URL = os.getenv(
        "DATASETTE_BASE_URL", "https://datasette.planning.data.gov.uk/digital-land"
    )

    # Provision data source
    PROVISION_CSV_URL = os.getenv(
        "PROVISION_CSV_URL",
        "https://raw.githubusercontent.com/digital-land/specification/refs/heads/main/specification/provision.csv",
    )


class DevelopmentConfig(Config):
    DEBUG = True
    ENV = "development"
    WTF_CSRF_ENABLED = False
    SAFE_URLS = {"localhost:5000"}
    AUTHENTICATION_ON = True

    # Override to load private key from file path for development
    _key_path = os.getenv("GITHUB_APP_PRIVATE_KEY_PATH")
    if _key_path and os.path.exists(_key_path):
        with open(_key_path, "r") as f:
            GITHUB_APP_PRIVATE_KEY = f.read()


class TestConfig(Config):
    ENV = "test"
    DEBUG = True
    TESTING = True
    AUTHENTICATION_ON = False
    SECRET_KEY = "testing"
