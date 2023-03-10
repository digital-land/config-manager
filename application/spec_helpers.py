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
    datasets = Dataset.query.filter(Dataset.dataset.in_(PIPELINE_SPECIFICATIONS)).all()
    specs = {spec: dataset for spec, dataset in zip(PIPELINE_SPECIFICATIONS, datasets)}
    return specs
