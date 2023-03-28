from application.db.models import (
    Column,
    Combine,
    Concat,
    Convert,
    Dataset,
    Default,
    DefaultValue,
    Filter,
    Lookup,
    Patch,
    Skip,
    Transform,
)

PIPELINE_MODELS = {
    "column": Column,
    "combine": Combine,
    "concat": Concat,
    "convert": Convert,
    "default": Default,
    "default-value": DefaultValue,
    "filter": Filter,
    "lookup": Lookup,
    "patch": Patch,
    "skip": Skip,
    "transform": Transform,
}


# hard code names of pipeline specifications until there
# is a way to extract the list from specification
PIPELINE_SPECIFICATIONS = list(PIPELINE_MODELS.keys())


def get_expected_pipeline_specs():
    datasets = Dataset.query.filter(Dataset.dataset.in_(PIPELINE_SPECIFICATIONS)).all()
    specs = {spec: dataset for spec, dataset in zip(PIPELINE_SPECIFICATIONS, datasets)}
    return specs


def count_pipeline_rules(pipeline):
    exclude_list = ["lookup"]
    rule_types = [
        rule_type_name.replace("-", "_") for rule_type_name in PIPELINE_SPECIFICATIONS
    ]
    s = sum(
        [
            len(getattr(pipeline, n))
            for n in rule_types
            if n not in exclude_list and hasattr(pipeline, n)
        ]
    )
    return {"pipeline": s, "lookup": len(pipeline.lookup)}
