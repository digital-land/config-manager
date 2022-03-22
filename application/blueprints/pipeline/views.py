import csv
import os
import tempfile
from itertools import islice
from pathlib import Path

import requests
from digital_land.api import DigitalLandApi
from flask import Blueprint, abort, current_app, jsonify

from application.models import Organisation, Source
from application.utils import login_required

pipeline_bp = Blueprint("pipeline", __name__, url_prefix="/pipeline")

collection_dirs = ["specification", "pipeline"]
pipeline_dirs = ["transformed", "issue", "var/column-field", "var/dataset-resource"]
pipeline_files = [
    "column.csv",
    "concat.csv",
    "convert.csv",
    "default.csv",
    "filter.csv",
    "lookup.csv",
    "patch.csv",
    "pipeline-callback.py",
    "skip.csv",
    "transform.csv",
]


@pipeline_bp.get("/<string:source>")
@login_required
def run(source):
    source_obj = Source.query.get(source)
    if source_obj is None:
        return abort(404)

    # TODO what happens in cases where more than one dataset? which name do we use? which fields?
    dataset_obj = source_obj.datasets[0]
    dataset = dataset_obj.dataset
    expected_fields = [field.field for field in dataset_obj.fields]

    with tempfile.TemporaryDirectory() as temp_dir:
        collection_dir = os.path.join(temp_dir, "collection")
        Path(collection_dir).mkdir(parents=True, exist_ok=True)
        endpoint_csv = os.path.join(collection_dir, "endpoint.csv")
        endpoint_data = source_obj.endpoint.to_csv_dict()
        with open(endpoint_csv, "w") as csvfile:
            fieldnames = endpoint_data.keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow(endpoint_data)

        source_csv = os.path.join(collection_dir, "source.csv")
        source_data = source_obj.to_csv_dict()
        with open(source_csv, "w") as csvfile:
            fieldnames = source_data.keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerow(source_data)

        pipeline_dir = os.path.join(temp_dir, "pipeline")
        if not os.path.exists(pipeline_dir):
            os.makedirs(pipeline_dir)
        for file in pipeline_files:
            pipeline_url = f"https://raw.githubusercontent.com/digital-land/{dataset}-collection/main/pipeline/{file}"
            resp = requests.get(pipeline_url)
            if resp.status_code != 200:
                continue
            outfile = f"{os.path.join(pipeline_dir, file)}"
            with open(outfile, "w") as f:
                f.write(resp.content.decode("utf-8"))

        specification_dir = os.path.join(
            current_app.config["PROJECT_ROOT"], "specification"
        )

        api = DigitalLandApi(False, dataset, pipeline_dir, specification_dir)
        api.collect_cmd(endpoint_csv, collection_dir)

        resource_dir = os.path.join(collection_dir, "resource")
        resources = os.listdir(resource_dir)
        if not resources:
            print("No resource collected")
            # do something to let user know we couldn't collect resource
        else:
            resource_hash = resources[0]
            for directory in pipeline_dirs:
                d = os.path.join(temp_dir, directory)
                if not os.path.exists(d):
                    os.makedirs(d)
            # convert - discard anything over 20 lines
            resource_input_path = os.path.join(resource_dir, resource_hash)
            output_path = os.path.join(resource_dir, f"{resource_hash}_converted.csv")

            api.convert_cmd(resource_input_path, output_path)

            with open(output_path) as file:
                reader = csv.DictReader(file)
                resource_fields = reader.fieldnames
                truncated_resource_rows = list(
                    islice(reader, current_app.config.get("ROW_LIMIT", 11))
                )

            # overwrite resource with first n rows of converted data
            with open(resource_input_path, "w") as file:
                writer = csv.DictWriter(file, fieldnames=resource_fields)
                writer.writeheader()
                for row in truncated_resource_rows:
                    writer.writerow(row)

            output_dir = os.path.join(temp_dir, "transformed", dataset)
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)

            output_path = os.path.join(output_dir, f"{resource_hash}.csv")

            column_field_dir = os.path.join(temp_dir, "var/column-field", dataset)
            if not os.path.exists(column_field_dir):
                os.makedirs(column_field_dir)

            issue_dir = os.path.join(temp_dir, "issue", dataset)
            if not os.path.exists(issue_dir):
                os.makedirs(issue_dir)

            dataset_resource_dir = os.path.join(
                temp_dir, "var/dataset-resource", dataset
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
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for org in organisations:
                    writer.writerow(org)

            api.pipeline_cmd(
                resource_input_path,
                output_path,
                collection_dir,
                None,
                issue_dir,
                organisation_path,
                column_field_dir=column_field_dir,
                dataset_resource_dir=dataset_resource_dir,
            )
            transformed = []
            with open(output_path) as file:
                reader = csv.DictReader(file)
                for row in reader:
                    transformed.append(row)

            issues = []
            issue_file = os.path.join(issue_dir, f"{resource_hash}.csv")
            with open(issue_file) as file:
                reader = csv.DictReader(file)
                for row in reader:
                    issues.append(row)

    return jsonify(
        {
            "transformed": transformed,
            "issues": issues,
            "resource_fields": resource_fields,
            "expected_fields": expected_fields,
        }
    )
