import base64
import os

import github
from flask.cli import AppGroup

from application.db.models import Collection, PublicationStatus
from application.utils import csv_dict_to_string

publish_cli = AppGroup("publish")


@publish_cli.command("changes")
def publish_config():
    app_id = os.getenv("GITHUB_APP_ID")
    private_key = base64.b64decode(os.getenv("GITHUB_APP_PRIVATE_KEY")).decode("utf-8")
    branch = os.getenv("CONFIG_REPO_BRANCH")

    g = github.GithubIntegration(app_id, private_key)
    token = g.get_access_token(g.get_installation("digital-land", "config").id).token
    gh = github.Github(login_or_token=token)
    repo = gh.get_repo("digital-land/config")

    for collection in Collection.query.all():
        if collection.publication_status == PublicationStatus.DRAFT:
            print("Publish sources and endpoints for", collection.collection)
            _publish_collection_config(collection, repo, branch)
        # if collection.pipeline.publication_status == PublicationStatus.DRAFT:
        #     print("publish rules for pipeline", collection.pipeline.pipeline)
        #     _publish_pipeline_config(collection.pipeline, repo)


def _publish_collection_config(collection, repo, branch, test):
    branch_sha = repo.get_branch(branch).commit.sha
    base_tree = repo.get_git_tree(sha=branch_sha)

    sources = []
    for source in collection.sources:
        sources.append(source.to_csv_dict())

    data = csv_dict_to_string(sources)
    blob = repo.create_git_blob(data, "utf-8")

    path = f"collection/{collection.collection}/source.csv"
    source_element = github.InputGitTreeElement(
        path=path, mode="100644", type="blob", sha=blob.sha
    )

    # get endpoint files and push with same commit above as new input git tree element
    # for endpoint in collection.endpoints and commit with source_element:

    tree = repo.create_git_tree([source_element], base_tree)
    parent = repo.get_git_commit(sha=branch_sha)
    message = f"Commit update of sources for {collection.collection}"
    commit = repo.create_git_commit(message, tree, [parent])
    branch_refs = repo.get_git_ref(f"heads/{branch}")
    branch_refs.edit(sha=commit.sha)
    print(f"Commited: {commit.sha}")


def _publish_pipeline_config(pipeline, repo):
    for rule_type, rules in pipeline.get_pipeline_rules().items():
        if rules:
            print("publish", rule_type, rules)
        else:
            print("no", rule_type, "to publish")
