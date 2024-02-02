# import pandas as pd


# def get_datasette_http():
#     """
#     Function to return  http for the use of querying  datasette,
#     specifically to add retries for larger queries
#     """
#     retry_strategy = Retry(total=3, status_forcelist=[400], backoff_factor=0)

#     adapter = HTTPAdapter(max_retries=retry_strategy)

#     http = requests.Session()
#     http.mount("https://", adapter)
#     http.mount("http://", adapter)

#     return http


# def get_datasette_query(db, sql, url="https://datasette.planning.data.gov.uk"):
#     url = f"{url}/{db}.json"
#     params = {"sql": sql, "_shape": "array"}
#     try:
#         http = get_datasette_http()
#         resp = http.get(url, params=params)
#         resp.raise_for_status()
#         df = pd.DataFrame.from_dict(resp.json())
#         return df
#     except Exception as e:
#         logging.warning(e)
#         return None


# def get_active_endpoints()
#     sql = """
#         select t3.pipeline as dataset ,t2.organisation, COUNT(t1.endpoint) as active_endpoint_count
#         from endpoint t1
#         left join source t2 on t1.endpoint=t2.endpoint
#         left join source_pipeline t3 on t2.source = t3.source
#         where t1.end_date = ''
#         and t2.organisation != ''
#         and t3.pipeline is not null
#         group by t2.organisation,t3.pipeline
#     """
#     get_datasette_query()

# def get_unhealthy_endpoints()

# def get_datasets_summary():
#     # get all the datasets listed with their active status
#     all_datasets = index_by("dataset", get_datasets())
#     missing = []

#     # add the publisher coverage numbers
#     dataset_coverage = publisher_coverage()
#     for d in dataset_coverage:
#         if all_datasets.get(d["pipeline"]):
#             all_datasets[d["pipeline"]] = {**all_datasets[d["pipeline"]], **d}
#         else:
#             missing.append(d["pipeline"])

#     # add the total resource count
#     dataset_resource_counts = resources_by_dataset()
#     for d in dataset_resource_counts:
#         if all_datasets.get(d["pipeline"]):
#             all_datasets[d["pipeline"]] = {**all_datasets[d["pipeline"]], **d}
#         else:
#             missing.append(d["pipeline"])

#     # add the first and last resource dates
#     dataset_resource_dates = first_and_last_resource()
#     for d in dataset_resource_dates:
#         if all_datasets.get(d["pipeline"]):
#             all_datasets[d["pipeline"]] = {**all_datasets[d["pipeline"]], **d}
#         else:
#             missing.append(d["pipeline"])

#     return all_datasets
