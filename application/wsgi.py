import os

from application.factory import create_app

# Development disables authentication, so it must be selected explicitly.
app = create_app(os.getenv("FLASK_CONFIG") or "config.Config")
