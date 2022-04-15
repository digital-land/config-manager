import csv
import os
from itertools import islice
from typing import NamedTuple

from application.models import Organisation


class Workspace(NamedTuple):

    collection_dir: str
    endpoint_csv: str
    pipeline_dir: str
    specification_dir: str
    resource_dir: str
    transformed_dir: str
    column_field_dir: str
    issue_dir: str
    dataset_resource_dir: str
    organisation_path: str

    @staticmethod
    def factory(source, dataset, temp_dir, project_root_dir):

        pipeline_file_fields = {
            "column": ["dataset", "resource", "column", "field"],
            "concat": [
                "dataset",
                "resource",
                "field",
                "fields",
                "separator",
                "entry-date",
                "start-date",
                "end-date",
            ],
            "convert": ["dataset", "resource", "script"],
            "default": [
                "dataset",
                "resource",
                "field",
                "default-field",
                "entry-date",
                "start-date",
                "end-date",
            ],
            "filter": ["dataset", "resource", "field", "pattern"],
            "lookup": ["prefix", "resource", "organisation", "reference", "entity"],
            "patch": ["dataset", "resource", "field", "pattern", "value"],
            "skip": ["dataset", "resource", "pattern"],
            "transform": ["dataset", "field", "replacement-field"],
        }

        other_pipeline_files = {
            "column": dataset.columns,
            "concat": dataset.concats,
            "default": dataset.defaults,
        }

        collection_dir = os.path.join(temp_dir, "collection")
        if not os.path.exists(collection_dir):
            os.makedirs(collection_dir)

        endpoint_csv = os.path.join(collection_dir, "endpoint.csv")
        endpoint_data = source.endpoint.to_csv_dict()

        with open(endpoint_csv, "w") as csvfile:
            fieldnames = endpoint_data.keys()
            writer = csv.DictWriter(
                csvfile, fieldnames=fieldnames, lineterminator="\r\n"
            )
            writer.writeheader()
            writer.writerow(endpoint_data)

        source_csv = os.path.join(collection_dir, "source.csv", lineterminator="\r\n")
        source_data = source.to_csv_dict()
        with open(source_csv, "w") as csvfile:
            fieldnames = source_data.keys()
            writer = csv.DictWriter(
                csvfile, fieldnames=fieldnames, lineterminator="\r\n"
            )
            writer.writeheader()
            writer.writerow(source_data)

        pipeline_dir = os.path.join(temp_dir, "pipeline")
        if not os.path.exists(pipeline_dir):
            os.makedirs(pipeline_dir)

        # TODO build from db data instead of fetch from github
        # for now create default empty files for those we don't
        # have in db yet
        for filename, fields in pipeline_file_fields.items():
            if filename not in other_pipeline_files.keys():
                file = os.path.join(pipeline_dir, f"{filename}.csv")
                with open(file, "w") as f:
                    writer = csv.DictWriter(f, fieldnames=fields, lineterminator="\r\n")
                    writer.writeheader()

        for filename, collection in other_pipeline_files.items():
            file = os.path.join(pipeline_dir, f"{filename}.csv")
            rows = [item.to_csv_dict() for item in collection]
            if rows:
                fieldnames = rows[0].keys()
            else:
                fieldnames = pipeline_file_fields[filename]
            with open(file, "w") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\r\n")
                writer.writeheader()
                for r in rows:
                    writer.writerow(r)

        specification_dir = os.path.join(project_root_dir, "specification")

        resource_dir = os.path.join(collection_dir, "resource")
        if not os.path.exists(resource_dir):
            os.makedirs(resource_dir)

        transformed_dir = os.path.join(temp_dir, "transformed", dataset.dataset)
        if not os.path.exists(transformed_dir):
            os.makedirs(transformed_dir)

        column_field_dir = os.path.join(temp_dir, "var/column-field", dataset.dataset)
        if not os.path.exists(column_field_dir):
            os.makedirs(column_field_dir)

        issue_dir = os.path.join(temp_dir, "issue", dataset.dataset)
        if not os.path.exists(issue_dir):
            os.makedirs(issue_dir)

        dataset_resource_dir = os.path.join(
            temp_dir, "var/dataset-resource", dataset.dataset
        )
        if not os.path.exists(dataset_resource_dir):
            os.makedirs(dataset_resource_dir)

        organisation_dir = os.path.join(
            temp_dir,
            "var/cache",
        )
        if not os.path.exists(organisation_dir):
            os.makedirs(organisation_dir)

        organisation_path = os.path.join(organisation_dir, "organisation.csv")

        organisations = [org.to_csv_dict() for org in Organisation.query.all()]
        with open(organisation_path, "w") as csvfile:
            fieldnames = organisations[0].keys()
            writer = csv.DictWriter(
                csvfile, fieldnames=fieldnames, lineterminator="\r\n"
            )
            writer.writeheader()
            for org in organisations:
                writer.writerow(org)

        return Workspace(
            collection_dir=collection_dir,
            endpoint_csv=endpoint_csv,
            pipeline_dir=pipeline_dir,
            specification_dir=specification_dir,
            resource_dir=resource_dir,
            transformed_dir=transformed_dir,
            column_field_dir=column_field_dir,
            issue_dir=issue_dir,
            dataset_resource_dir=dataset_resource_dir,
            organisation_path=organisation_path,
        )


def convert_and_truncate_resource(api, workspace, resource_hash, limit=10):
    input_path = os.path.join(workspace.resource_dir, resource_hash)
    output_path = os.path.join(workspace.resource_dir, f"{resource_hash}_converted.csv")

    api.convert_cmd(input_path, output_path)

    with open(output_path) as file:
        reader = csv.DictReader(file)
        resource_fields = reader.fieldnames
        truncated_resource_rows = list(islice(reader, limit))

    # overwrite local copy of resource with first n rows of converted data
    with open(input_path, "w") as file:
        writer = csv.DictWriter(file, fieldnames=resource_fields, lineterminator="\r\n")
        writer.writeheader()
        for row in truncated_resource_rows:
            writer.writerow(row)

    output_path = os.path.join(workspace.transformed_dir, f"{resource_hash}.csv")
    return resource_fields, input_path, output_path, truncated_resource_rows
