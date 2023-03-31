import base64
import logging
import os
import sys

import github
from flask.cli import AppGroup

from application.db.models import Collection, PublicationStatus
from application.extensions import db
from application.publish.models import (
    ColumnModel,
    CombineModel,
    ConcatModel,
    ConvertModel,
    DefaultModel,
    DefaultValueModel,
    EndpointModel,
    FilterModel,
    PatchModel,
    SkipModel,
    SourceModel,
    TransformModel,
)
from application.utils import csv_dict_to_string

logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


publish_cli = AppGroup("publish")


PUBLISH_MODEL_CLASSES = {
    "column": ColumnModel,
    "combine": CombineModel,
    "concat": ConcatModel,
    "conver": ConvertModel,
    "default": DefaultModel,
    "default_model": DefaultValueModel,
    "patch": PatchModel,
    "skip": SkipModel,
    "transform": TransformModel,
    "filter": FilterModel,
}


@publish_cli.command("changes")
def publish_config():
    app_id = os.getenv("GITHUB_APP_ID")
    private_key = base64.b64decode(os.getenv("GITHUB_APP_PRIVATE_KEY")).decode("utf-8")
    branch = os.getenv("CONFIG_REPO_BRANCH")

    g = github.GithubIntegration(app_id, private_key)
    token = g.get_access_token(g.get_installation("digital-land", "config").id).token
    gh = github.Github(login_or_token=token)
    repo = gh.get_repo("digital-land/config")

    for collection in Collection.query.order_by(Collection.name).all():
        if collection.publication_status == PublicationStatus.DRAFT:
            logger.info(f"Publish sources and endpoints for {collection.collection}")
            _publish_collection_config(collection, repo, branch)
            collection.publication_status = PublicationStatus.PUBLISHED
            db.session.add(collection)
            db.session.commit()
        else:
            logger.info(f"Collection {collection.collection} has no updates to publish")

        if collection.pipeline.publication_status == PublicationStatus.DRAFT:
            logger.info("Publish rules for pipeline", collection.pipeline.pipeline)
            _publish_pipeline_config(collection.pipeline, repo, branch)
            collection.pipeline.publication_status = PublicationStatus.PUBLISHED
            db.session.add(collection)
            db.session.commit()
        else:
            logger.info(
                f"Pipeline {collection.pipeline.pipeline} has no updates to publish"
            )


def _publish_collection_config(collection, repo, branch_name):
    branch = repo.get_branch(branch_name)
    branch_sha = branch.commit.sha
    base_tree = repo.get_git_tree(sha=branch_sha)

    sources = []
    for source in collection.sources:
        sources.append(SourceModel.from_orm(source).dict(by_alias=True))

    data = csv_dict_to_string(sources)
    blob = repo.create_git_blob(data, "utf-8")

    path = f"collection/{collection.collection}/source.csv"
    source_element = github.InputGitTreeElement(
        path=path, mode="100644", type="blob", sha=blob.sha
    )

    endpoints = []
    for endpoint in collection.endpoints:
        endpoints.append(EndpointModel.from_orm(endpoint).dict(by_alias=True))

    data = csv_dict_to_string(endpoints)
    blob = repo.create_git_blob(data, "utf-8")

    path = f"collection/{collection.collection}/endpoint.csv"
    endpoint_element = github.InputGitTreeElement(
        path=path, mode="100644", type="blob", sha=blob.sha
    )

    tree = repo.create_git_tree([source_element, endpoint_element], base_tree)
    parent = repo.get_git_commit(sha=branch_sha)
    message = f"Commit update of sources for {collection.collection}"
    commit = repo.create_git_commit(message, tree, [parent])
    branch_refs = repo.get_git_ref(f"heads/{branch_name}")
    branch_refs.edit(sha=commit.sha)
    logger.info(f"Commited collection config - commit sha: {commit.sha}")


def _publish_pipeline_config(pipeline, repo, branch_name):
    branch = repo.get_branch(branch_name)
    branch_sha = branch.commit.sha
    base_tree = repo.get_git_tree(sha=branch_sha)
    elements = []

    for rule_type, rules in pipeline.get_pipeline_rules().items():
        if rules:
            to_publish = []
            publish_model = PUBLISH_MODEL_CLASSES.get(rule_type, None)
            if publish_model is None:
                logger.info(
                    "Can't publish rule type {{rule_type}}. No model defined yet"
                )
                continue

            logger.info(f"Publish {len(rules)} {rule_type} rules")
            for rule in rules:
                to_publish.append(publish_model.from_orm(rule).dict(by_alias=True))

            data = csv_dict_to_string(to_publish)
            blob = repo.create_git_blob(data, "utf-8")

            path = f"pipeline/{pipeline.pipeline}/{rule_type}.csv"
            element = github.InputGitTreeElement(
                path=path, mode="100644", type="blob", sha=blob.sha
            )
            elements.append(element)
        else:
            logger.info(f"No {rule_type} to publish for {pipeline.pipeline}")

    if elements:
        tree = repo.create_git_tree(elements, base_tree)
        parent = repo.get_git_commit(sha=branch_sha)
        message = f"Commit update of pipeline config for {pipeline.pipeline}"
        commit = repo.create_git_commit(message, tree, [parent])
        branch_refs = repo.get_git_ref(f"heads/{branch}")
        branch_refs.edit(sha=commit.sha)
        logger.info(
            f"Commited pipeline {pipeline.pipeline} config - commit sha: {commit.sha}"
        )
