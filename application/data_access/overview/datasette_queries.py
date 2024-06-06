# from application.caching import get
from application.data_access.datasette_utils import get_datasette_query
from application.utils import create_dict

DATASETTE_URL = "https://datasette.planning.data.gov.uk"


# TODO - this data is not in digital land db but each dataset has own
# database so unless we download every sqlite db for each dataset
# this will have to carry on using datasette for the moment
def fetch_resource_from_dataset(database_name, resource):
    query_lines = [
        "SELECT",
        "*",
        "FROM",
        "dataset_resource",
        "WHERE",
        f"resource = '{resource}'",
    ]
    query_str = " ".join(query_lines)
    rows = get_datasette_query("digital-land", query_str)

    # return [dict(zip(columns, row)) for row in rows.to_numpy()]
    return create_dict(rows["columns"], rows["rows"][0])


def fetch_entry_count(database_name, resource):
    r = fetch_resource_from_dataset(database_name, resource)
    return r["entry_count"]
