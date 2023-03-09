from application.db.models import Dataset

# hard code names of pipeline specifications until there
# is a way to extract the list from specification
PIPELINE_SPECIFICATIONS = [
    "column",
    "combine",
    "concat",
    "convert",
    "default",
    "default-value",
    "filter",
    "lookup",
    "patch",
    "skip",
    "transform",
]


def get_expected_pipeline_specs():
    specs = {}
    for pipeline_rule_type in PIPELINE_SPECIFICATIONS:
        specs.setdefault(pipeline_rule_type, {})
        specs[pipeline_rule_type] = Dataset.query.filter_by(
            dataset=pipeline_rule_type
        ).first()
    return specs
