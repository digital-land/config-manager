from flask import Blueprint, render_template

from application.data_access.datasette_queries import get_overview

report_bp = Blueprint("reporting", __name__, url_prefix="/reporting")


@report_bp.get("/")
@report_bp.get("/overview")
def overview():
    # reporting_summary = get_resporting_summary()
    # reporting_ts = get_resporting_ts()

    # get tabbed elements
    # odp_endpoint_summary = reporting_summary.groupby('dataset')  .....
    # odp_active_resource_summary = ...
    # dataset_summary = ....
    # organisation_summary =  ...

    # # get summary metric dict
    # summary_metrics = get_summary_metrics(reporting_summary,reporting_ts)

    # content_type_counts = sorted(
    #     get_content_type_counts(),
    #     key=lambda x: x["resource_count"],
    #     reverse=True,
    # )

    overview = get_overview()

    # return render_template(
    #     "overview.html",
    #     summary_metrics=summary_metrics,
    #     summary_ts=summary_ts,
    #     odp_endpoint_summary,
    #     odp_active_resource_summary,
    #     dataset_summary,
    #     organisation_summary
    # )
    return render_template("reporting/overview.html", overview=overview)


# @report_bp.get("/dataset")
# def datasets(dataset_id):
#     dataset = Dataset.query.get(dataset_id)

#     if dataset is None or dataset.collection_id is None:
#         return abort(404)

#     specification_pipelines = get_expected_pipeline_specs()

#     rule_counts = count_pipeline_rules(dataset.collection.pipeline)

#     return render_template(
#         "dataset/dataset.html",
#         pipeline=dataset.collection.pipeline,
#         dataset=dataset,
#         specification_pipelines=specification_pipelines,
#         rule_counts=rule_counts,
#     )


# @report_bp.get("dataset/<string:dataset_id>")
# def dataset(dataset_id, rule_type_name):
#     limited = False
#     dataset = Dataset.query.get(dataset_id)

#     if dataset is None or dataset.collection_id is None:
#         return abort(404)

#     # # check if name is one of allowable rule types
#     specification_pipelines = get_expected_pipeline_specs()
#     if rule_type_name not in specification_pipelines.keys():
#         return abort(404)

#     rules = getattr(dataset.collection.pipeline, rule_type_name)

#     if len(rules) > 1000:
#         rules = rules[:1000]
#         limited = True

#     return render_template(
#         "dataset/rules.html",
#         dataset=dataset,
#         rule_type_name=rule_type_name,
#         rule_type_specification=specification_pipelines[rule_type_name],
#         rules=rules,
#         limited=limited,
#     )


# @report_bp.get("/organisation")
# def organisations(dataset_id):
#     dataset = Dataset.query.get(dataset_id)

#     if dataset is None or dataset.collection_id is None:
#         return abort(404)

#     return render_template(
#         "dataset/sources.html",
#         dataset=dataset,
#     )


# @report_bp.get("/organisation/<string:organisation_id>")
# def organisation(dataset_id, rule_type_name, rule_id):
#     dataset = Dataset.query.get(dataset_id)

#     if dataset is None or dataset.collection_id is None:
#         return abort(404)

#     specification_pipelines = get_expected_pipeline_specs()
#     if rule_type_name not in specification_pipelines.keys():
#         return abort(404)

#     if rule_id == "new":
#         # create empty rule except for dataset

#         form = EditRuleForm(dataset_id=dataset.dataset)
#         form.field_id.choices = [(field.field, field.field) for field in dataset.fields]
#         rule = {"dataset": dataset.dataset}
#     else:
#         rule = get_rule(rule_id, rule_type_name)
#         if rule is None:
#             return abort(404)

#         form = EditRuleForm(obj=rule, rule_type=rule_type_name)
#         if hasattr(form, "field_id"):
#             form.field_id.choices = [
#                 (field.field, field.field) for field in dataset.fields
#             ]
#             if rule.field:
#                 form.field_id.data = rule.field.field

#     rule_type_specification = specification_pipelines[rule_type_name]
#     form_field_names = _get_form_field_names(rule_type_specification)

#     return render_template(
#         "dataset/editrule.html",
#         form=form,
#         dataset=dataset,
#         rule_type_name=rule_type_name,
#         rule_type_specification=rule_type_specification,
#         form_field_names=form_field_names,
#         rule=rule,
#     )


# @report_bp.post("contribution/<string:dataset_id>/<string:organisation_id>")
# def contribution(dataset_id, organisation_id):
#     form = EditRuleForm(request.form)
#     if form.validate_on_submit():
#         dataset = Dataset.query.get(dataset_id)
#         if dataset is None:
#             return abort(404)

#         if rule_id == "new":
#             rule_class = PIPELINE_MODELS[rule_type_name]
#             rule = rule_class()
#             rule.pipeline = dataset.collection.pipeline
#         else:
#             rule = get_rule(rule_id, rule_type_name)
#         if rule is None:
#             return abort(404)

#         form.populate_obj(rule)
#         rule.pipeline.publication_status = PublicationStatus.DRAFT.name
#         db.session.add(rule)
#         db.session.commit()
#         return redirect(
#             url_for(
#                 "dataset.rule_type",
#                 dataset_id=dataset_id,
#                 rule_type_name=rule_type_name,
#             )
#         )
#     return render_template("dataset/editrule.html", form=form)

# @report_bp.post("endpoint")
# def endpoints(dataset_id, organisation_id):
#     pass

# @report_bp.get("endpoint/<string:endpoint>")
# def contribution(dataset_id, organisation_id):
#     pass

# @report_bp.post("resource/")
# def resources(dataset_id, organisation_id):
#     pass
