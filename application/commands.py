# import base64
# import os

import click

# import github
from flask.cli import AppGroup

from application.db.models import Collection, PublicationStatus

publish_cli = AppGroup("publish")


@publish_cli.command("changes")
@click.option(
    "--test", default=False, help="Show what files will be updated without committing"
)
def publish_config(test):
    # GITHUB_APP_ID = os.getenv("GITHUB_APP_ID")
    # GITHUB_APP_PRIVATE_KEY = base64.b64decode(
    #     os.getenv("GITHUB_APP_PRIVATE_KEY")
    # ).decode("utf-8")
    # CONFIG_REPO_BRANCH = os.getenv("CONFIG_REPO_BRANCH")
    # g = github.GithubIntegration(GITHUB_APP_ID, GITHUB_APP_PRIVATE_KEY)
    # token = g.get_access_token(g.get_installation("digital-land", "config").id).token
    # gh = github.Github(login_or_token=token)
    # repo = gh.get_repo("digital-land/config")

    collections = Collection.query.filter(
        Collection.publication_status == PublicationStatus.DRAFT
    ).all()

    for collection in collections:
        print("publish sources and endpoints for", collection.collection)
        if collection.pipeline.publication_status == PublicationStatus.DRAFT:
            print("publish rules for pipeline", collection.pipeline.pipeline)
