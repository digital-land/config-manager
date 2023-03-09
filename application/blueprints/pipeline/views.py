from flask import Blueprint, jsonify, render_template

from application.db.models import Pipeline
from application.export.models import PipelineModel
from application.spec_helpers import get_expected_pipeline_specs

pipeline_bp = Blueprint("pipeline", __name__, url_prefix="/pipeline")


def get_pipelines_by_typology(pipelines, typology, match=True):
    if match:
        return [
            pipeline
            for pipeline in pipelines
            if pipeline.dataset.typology.typology == typology
        ]
    return [
        pipeline
        for pipeline in pipelines
        if pipeline.dataset.typology.typology != typology
    ]


@pipeline_bp.get("/")
def index():
    pipelines = Pipeline.query.order_by(Pipeline.name).all()
    return render_template(
        "pipeline/index.html",
        pipelines=get_pipelines_by_typology(pipelines, "category", False),
        category_pipelines=get_pipelines_by_typology(pipelines, "category"),
    )


@pipeline_bp.get("/<string:pipeline_id>")
def pipeline(pipeline_id):
    pipeline = Pipeline.query.get(pipeline_id)
    specification_pipelines = get_expected_pipeline_specs()
    return render_template(
        "pipeline/pipeline.html",
        pipeline=pipeline,
        specification_pipelines=specification_pipelines,
    )


@pipeline_bp.get("/<string:pipeline_id>/configuration")
def download_pipeline(pipeline_id):
    p = Pipeline.query.get(pipeline_id)
    pipeline = PipelineModel.from_orm(p)
    return jsonify(pipeline.dict(by_alias=True))


# @pipeline_bp.get("/<string:source>")
# def run(source):
#     source_obj = Source.query.get(source)
#     if source_obj is None:
#         return abort(404)

#     # TODO what happens in cases where more than one dataset? which name do we use? which fields?
#     dataset_obj = source_obj.datasets[0]
#     dataset = dataset_obj.dataset
#     expected_fields = [field.field for field in dataset_obj.fields]

#     with tempfile.TemporaryDirectory() as temp_dir:

#         workspace = Workspace.factory(
#             source_obj, dataset_obj, temp_dir, current_app.config["PROJECT_ROOT"]
#         )
#         api = None
#         # digital land python was de objectified at some point and DigitalLandApi class removed
#         # api = DigitalLandApi(
#         #     False, dataset, workspace.pipeline_dir, workspace.specification_dir
#         # )

#         api.collect_cmd(workspace.endpoint_csv, workspace.collection_dir)

#         resources = os.listdir(workspace.resource_dir)

#         if not resources:
#             print("No resource collected")
#             return abort(400)
#         else:
#             resource_hash = resources[0]
#             limit = int(request.args.get("limit")) if request.args.get("limit") else 10
#             (
#                 resource_fields,
#                 input_path,
#                 output_path,
#                 resource_rows,
#             ) = convert_and_truncate_resource(api, workspace, resource_hash, limit)

#             api.pipeline_cmd(
#                 input_path,
#                 output_path,
#                 workspace.collection_dir,
#                 None,
#                 workspace.issue_dir,
#                 workspace.organisation_path,
#                 column_field_dir=workspace.column_field_dir,
#                 dataset_resource_dir=workspace.dataset_resource_dir,
#             )

#             transformed = []
#             with open(output_path) as file:
#                 reader = csv.DictReader(file)
#                 for row in reader:
#                     transformed.append(row)

#             issues = []
#             issue_file = os.path.join(workspace.issue_dir, f"{resource_hash}.csv")
#             with open(issue_file) as file:
#                 reader = csv.DictReader(file)
#                 for row in reader:
#                     issues.append(row)

#     return jsonify(
#         {
#             "transformed": transformed,
#             "issues": issues,
#             "resource_fields": resource_fields,
#             "expected_fields": expected_fields,
#             "resource_rows": resource_rows,
#         }
#     )
