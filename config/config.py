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
    GITHUB_API_BASE_URL = os.getenv("GITHUB_API_BASE_URL", "https://api.github.com")
    GITHUB_ORG = os.getenv("GITHUB_ORG", "digital-land")
    GITHUB_ADMIN_TEAM_SLUGS = os.getenv(
        "GITHUB_ADMIN_TEAM_SLUGS", "manage-service-admins"
    )
    SAFE_URLS = set(os.getenv("SAFE_URLS", "").split(","))
    AUTHENTICATION_ON = True
    S3_BUCKET_URL = (
        "https://digital-land-production-collection-dataset.s3.eu-west-2.amazonaws.com"
    )
    # Config repo branch to commit to. If/when this application is used
    # to edit config then change to push to main branch - until then default to update-test
    CONFIG_REPO_BRANCH = os.getenv("CONFIG_REPO_BRANCH", "config-manager-update")
    ENVIRONMENT = os.getenv("ENVIRONMENT", "local").lower()

    # Short, temporary block on a dispatched submission's entity numbers against a
    # concurrent same-collection submission. It only bridges the window between
    # dispatch and the commit landing on the branch - after which the GitHub compare
    # check (the main guard) catches the change - so a few minutes is enough, and
    # kept short so a failed/abandoned submission doesn't lock those numbers for long.
    ENTITY_CLAIM_TTL_SECONDS = int(os.getenv("ENTITY_CLAIM_TTL_SECONDS", str(10 * 60)))

    CACHE_TYPE = "SimpleCache"
    CACHE_DEFAULT_TIMEOUT = 300

    # Paged API fetches (response details, platform entities) run their pages in
    # parallel. Timeout is per page and must stay well under the CDN's 30s origin
    # timeout so one slow page can't use up the whole page budget.
    HTTP_PAGE_MAX_WORKERS = int(os.getenv("HTTP_PAGE_MAX_WORKERS", "8"))
    HTTP_PAGE_TIMEOUT = int(os.getenv("HTTP_PAGE_TIMEOUT", "10"))

    # Planning Data base URL
    PLANNING_BASE_URL = os.getenv("PLANNING_URL", "https://www.planning.data.gov.uk")

    # Datasette base URL
    DATASETTE_BASE_URL = os.getenv(
        "DATASETTE_BASE_URL", "https://datasette.planning.data.gov.uk/digital-land"
    )

    # Provision data source
    PROVISION_CSV_URL = os.getenv(
        "PROVISION_CSV_URL",
        "https://raw.githubusercontent.com/digital-land/specification/refs/heads/main/specification/provision.csv",
    )

    # Dataset field specification
    DATASET_FIELD_CSV_URL = os.getenv(
        "DATASET_FIELD_CSV_URL",
        "https://raw.githubusercontent.com/digital-land/specification/refs/heads/main/specification/dataset-field.csv",
    )

    # Dataset and collection specification
    DATASET_CSV_URL = os.getenv(
        "DATASET_CSV_URL",
        "https://raw.githubusercontent.com/digital-land/specification/refs/heads/main/specification/dataset.csv",
    )

    # Sentry error reporting. The DSN is injected as a secret by terraform
    # (/${stage}/config-manager/sentry_dsn), so Sentry stays off wherever the DSN
    # isn't set - local dev included, unless you put one in your .env.
    SENTRY_DSN = os.getenv("SENTRY_DSN")
    SENTRY_ENABLED = os.getenv("SENTRY_ENABLED", "true").lower() == "true"
    # Sample rates are kept low - this is a low-traffic internal app and traces are
    # only for performance work. Errors are always sent in full regardless of these.
    SENTRY_TRACES_SAMPLE_RATE = float(os.getenv("SENTRY_TRACING_SAMPLE_RATE", "0.01"))
    SENTRY_PROFILES_SAMPLE_RATE = float(
        os.getenv("SENTRY_PROFILES_SAMPLE_RATE", "0.01")
    )
    SENTRY_DEBUG = os.getenv("SENTRY_DEBUG", "false").lower() == "true"
    SENTRY_RELEASE = os.getenv("GIT_COMMIT")


class DevelopmentConfig(Config):
    DEBUG = True
    ENV = "development"
    WTF_CSRF_ENABLED = False
    SAFE_URLS = {"localhost:5000"}
    AUTHENTICATION_ON = False

    # Commit dev submissions to a separate long-lived branch so they never touch the
    # production config-manager-update branch (which auto-merges to main). Still
    # overridable via the CONFIG_REPO_BRANCH env var.
    CONFIG_REPO_BRANCH = os.getenv("CONFIG_REPO_BRANCH", "test-config-manager-update")

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
    SQLALCHEMY_DATABASE_URI = "sqlite:///:memory:"
    SENTRY_ENABLED = False


def get_request_api_endpoint():
    """
    Returns an explicit async request backend API endpoint when configured,
    otherwise selects one based on the ENVIRONMENT variable.
    ENVIRONMENT: local | development | staging | production
    Default environment is local
    """
    override = os.getenv("REQUEST_API_BASE_URL")
    if override:
        return override.rstrip("/")

    env = os.getenv("ENVIRONMENT", "local").lower()

    mapping = {
        "local": "http://localhost:8000",
        "development": "https://pub-async.development.planning.data.gov.uk",
        "staging": "https://pub-async.staging.planning.data.gov.uk",
        "production": "https://pub-async.planning.data.gov.uk",
    }

    return mapping.get(env, mapping["local"])
