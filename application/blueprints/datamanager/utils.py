import csv
import logging
from datetime import datetime
from io import StringIO

import requests
from dotenv import load_dotenv
from flask import current_app, render_template

# Load .env file for this module
load_dotenv()

logger = logging.getLogger(__name__)

REQUESTS_TIMEOUT = 20  # seconds


def handle_error(e):
    logger.exception(f"Error: {e}")
    return render_template("datamanager/error.html", message=str(e)), 500


def get_provision_orgs_for_dataset(dataset_id: str) -> list:
    """
    Fetch organisation codes for a given dataset from the provision CSV.
    Returns a list of organisation codes.
    """
    try:
        provision_url = current_app.config.get("PROVISION_CSV_URL")
        response = requests.get(provision_url, timeout=REQUESTS_TIMEOUT)
        response.raise_for_status()
        reader = csv.DictReader(StringIO(response.text))
        orgs = []
        seen = set()
        for row in reader:
            if row.get("dataset") == dataset_id:
                org_code = row.get("organisation", "")
                if org_code and org_code not in seen:
                    orgs.append(org_code)
                    seen.add(org_code)
        return orgs
    except Exception as e:
        logger.error(f"Failed to fetch provision orgs for dataset: {e}")
        return []


def get_organisation_code_mapping() -> dict:
    """
    Build a mapping from organisation code to organisation name.
    Fetches from the organisation.json datasette endpoint.
    Handles pagination to fetch all records.
    Returns a dict: {code: name}
    """
    org_mapping = {}
    try:
        datasette_url = current_app.config.get("DATASETTE_BASE_URL")
        # Fetch with objects shape - returns list of dicts with column names as keys
        # Use _size=max to get all records in one request
        url = f"{datasette_url}/organisation.json?_shape=objects&_size=max"

        page_count = 0
        while url:
            page_count += 1

            response = requests.get(url, timeout=REQUESTS_TIMEOUT)
            response.raise_for_status()
            data = response.json()

            # Extract rows from the response
            rows = data.get("rows", []) if isinstance(data, dict) else data

            # Build the mapping from the list of dictionaries
            for row in rows:
                if isinstance(row, dict):
                    code = row.get("organisation")
                    name = row.get("name")
                    if code and name:
                        org_mapping[code] = name

            # Check for next page
            url = data.get("next_url") if isinstance(data, dict) else None
            if url:
                # Make relative URLs absolute
                if url.startswith("/"):
                    url = f"{datasette_url.rstrip('/')}{url}"

        logger.info(
            f"Built organisation mapping with {len(org_mapping)} entries from {page_count} page(s)"
        )

    except Exception as e:
        logger.error(f"Failed to fetch organisation mapping: {e}", exc_info=True)

    return org_mapping


# put this near the top, replacing your current get_spec_fields_from_datasette
def get_spec_fields_union(dataset_id):
    """
    Return the union of:
      - global field list (all datasets)
      - dataset-scoped field list (if dataset_id is provided)
    Keep original casing; de-duplicate exact strings; stable order.
    """
    datasette_url = current_app.config.get("DATASETTE_BASE_URL")
    base = (f"{datasette_url}/dataset_field.json",)
    headers = {"Accept": "application/json", "User-Agent": "Planning Data - Manage"}

    def _fetch(url):
        try:
            r = requests.get(url, timeout=REQUESTS_TIMEOUT, headers=headers)
            r.raise_for_status()
            rows = r.json() or []
            return [
                (row.get("field") or "").strip() for row in rows if row.get("field")
            ]
        except Exception as e:
            logger.warning(f"dataset_field fetch failed: {e} for {url}")
            return []

    # global list (all datasets)
    global_fields = _fetch(f"{base}?_shape=array")

    # dataset-filtered list (e.g. camel-case BFL items)
    dataset_fields = (
        _fetch(f"{base}?_shape=array&dataset={dataset_id}") if dataset_id else []
    )

    # union, preserve first-seen order, exact-string dedupe
    seen, out = set(), []
    for f in global_fields + dataset_fields:
        if f and f not in seen:
            seen.add(f)
            out.append(f)

    # optional: sort by lowercase while preserving casing (comment out if you prefer raw order)
    out.sort(key=lambda x: x.lower())
    return out


def order_table_fields(all_fields):
    leading = []
    trailing = []

    for field in all_fields:
        if field.lower() == "reference":
            # Insert at the beginning (splice(0, 0, field) equivalent)
            leading.insert(0, field)
        elif field.lower() == "name":
            # Append to leading
            leading.append(field)
        else:
            # All other fields go to trailing
            trailing.append(field)

    # Combine leading and trailing
    ordered_fields = leading + trailing

    return ordered_fields


def fetch_all_response_details(
    async_api: str, request_id: str, limit: int = 50
) -> list:
    """
    Fetch all response details with multiple calls using the specified limit.
    Similar to the pattern used in submit repository.
    """
    all_details = []
    offset = 0
    logger.info(
        f"Fetching response details - API: {async_api}, Request ID: {request_id}"
    )

    while True:
        try:
            url = f"{async_api}/requests/{request_id}/response-details"
            params = {"offset": offset, "limit": limit}
            logger.debug(f"Fetching batch - URL: {url}, Params: {params}")

            response = requests.get(url, params=params, timeout=REQUESTS_TIMEOUT)
            content_length = getattr(response, "content", None)
            content_length = (
                len(content_length) if content_length is not None else "N/A"
            )
            logger.info(
                f"Batch response - Status: {response.status_code}, Content-Length: {content_length}"
            )

            response.raise_for_status()
            batch = response.json() or []
            logger.info(f"Batch parsed - Items: {len(batch)}")

            if not batch:
                logger.info("No more batches available")
                break

            # Log sample of first batch for debugging
            if offset == 0 and batch:
                logger.info(
                    f"First batch sample - Item keys: {list(batch[0].keys()) if batch[0] else 'Empty item'}"
                )
                if batch[0] and "converted_row" in batch[0]:
                    converted_sample = batch[0]["converted_row"]
                    if converted_sample:
                        logger.info(
                            f"First converted_row sample: {dict(list(converted_sample.items())[:3])}"
                        )
                    else:
                        logger.info("Empty converted_row")

            all_details.extend(batch)

            if len(batch) < limit:
                logger.info(f"Last batch received - Total items: {len(all_details)}")
                break

            offset += limit

        except Exception as e:
            logger.error(f"Failed to fetch batch at offset {offset}: {e}")
            logger.error(f"Response status: {getattr(response, 'status_code', 'N/A')}")
            response_text = getattr(response, "text", "N/A")
            if hasattr(response_text, "__getitem__"):
                logger.error(f"Response text: {response_text[:500]}")
            else:
                logger.error(f"Response text: {response_text}")
            break

    logger.info(f"Total response details fetched: {len(all_details)}")
    return all_details


def read_raw_csv_preview(source_url: str, max_rows: int = 50):
    """
    Fetch the CSV at source_url and return (headers, first N rows).
    """
    headers_, rows = [], []
    if not source_url:
        return headers_, rows
    try:
        resp = requests.get(source_url, timeout=REQUESTS_TIMEOUT)
        text = resp.content.decode("utf-8", errors="ignore")
        reader = csv.reader(StringIO(text))
        first = next(reader, None)
        if first is None:
            return headers_, rows
        headers_ = [h.strip().lstrip("\ufeff") for h in first if h is not None]
        for i, r in enumerate(reader):
            if i >= max_rows:
                break
            vals = (r + [""] * len(headers_))[: len(headers_)]
            rows.append(vals)
    except Exception as e:
        logger.warning(f"Could not fetch/parse CSV preview: {e}")
    return headers_, rows


def build_lookup_csv_preview(new_entities: list) -> tuple:
    """
    Build lookup CSV preview for new entities.
    Returns (table_params, csv_text)
    """
    cols = ["reference", "prefix", "organisation", "entity"]
    rows = [
        {"columns": {c: {"value": (e.get(c) or "")} for c in cols}}
        for e in new_entities
    ]
    logger.info(f"New entities rows: {rows}")
    table_params = {
        "columns": cols,
        "fields": cols,
        "rows": rows,
        "columnNameProcessing": "none",
    }

    fields = [
        "prefix",
        "resource",
        "endpoint",
        "entry-number",
        "organisation",
        "reference",
        "entity",
        "entry-date",
        "start-date",
        "end-date",
    ]

    csv_text = "\n".join(
        ",".join(item.get(field, "") for field in fields) for item in new_entities
    )

    return table_params, csv_text


def build_endpoint_csv_preview(endpoint_summary: dict) -> tuple:
    """
    Build endpoint CSV preview.
    Returns (endpoint_already_exists, endpoint_url, table_params, csv_text)
    """
    ep_cols = [
        "endpoint",
        "endpoint-url",
        "parameters",
        "plugin",
        "entry-date",
        "start-date",
        "end-date",
    ]
    endpoint_already_exists = (
        "Yes" if endpoint_summary.get("endpoint_url_in_endpoint_csv") is True else "No"
    )
    logger.info(f"Endpoint already exists: {endpoint_already_exists}")

    endpoint_csv_text = ""
    if endpoint_summary.get("endpoint_url_in_endpoint_csv"):
        end_point_entry = endpoint_summary.get("existing_endpoint_entry", {})
        endpoint_url = end_point_entry.get("endpoint-url", "")
    else:
        end_point_entry = endpoint_summary.get("new_endpoint_entry", {})
        endpoint_url = end_point_entry.get("endpoint-url", "")
        endpoint_csv_text = ",".join([end_point_entry.get(col, "") for col in ep_cols])

    ep_row = [str(end_point_entry.get(col, "") or "") for col in ep_cols]
    endpoint_csv_table_params = {
        "columns": ep_cols,
        "fields": ep_cols,
        "rows": [{"columns": {c: {"value": v} for c, v in zip(ep_cols, ep_row)}}],
        "columnNameProcessing": "none",
    }

    return (
        endpoint_already_exists,
        endpoint_url,
        endpoint_csv_table_params,
        endpoint_csv_text,
    )


def build_source_csv_preview(
    source_summary_data: dict, include_headers: bool = True
) -> tuple:
    """
    Build source CSV preview and summary.
    Returns (source_summary, table_params, csv_text)

    Args:
        source_summary_data: Source summary data from API response
        include_headers: If True, include CSV headers in csv_text. Default True.
    """
    src_cols = [
        "source",
        "attribution",
        "collection",
        "documentation-url",
        "endpoint",
        "licence",
        "organisation",
        "pipelines",
        "entry-date",
        "start-date",
        "end-date",
    ]

    source_present = source_summary_data.get("documentation_url_in_source_csv")
    will_create_source_text = "No" if source_present else "Yes"

    source_csv_text = ""
    if not source_present:
        src_source = source_summary_data.get("new_source_entry", {})
        src_row = [str(src_source.get(col, "") or "") for col in src_cols]
        source_csv_table_params = {
            "columns": src_cols,
            "fields": src_cols,
            "rows": [{"columns": {c: {"value": v} for c, v in zip(src_cols, src_row)}}],
            "columnNameProcessing": "none",
        }
        if include_headers:
            source_csv_text = ",".join(src_cols) + "\n" + ",".join(src_row)
        else:
            source_csv_text = ",".join(src_row)
    else:
        src_source = source_summary_data.get("existing_source_entry", {})
        src_row = [str(src_source.get(col, "") or "") for col in src_cols]
        source_csv_table_params = {
            "columns": src_cols,
            "fields": src_cols,
            "rows": [{"columns": {c: {"value": v} for c, v in zip(src_cols, src_row)}}],
            "columnNameProcessing": "none",
        }

    logger.info(f"Will create source: {will_create_source_text}")

    # Build summary panel model (like Endpoint Summary)
    source_summary = {
        "will_create": will_create_source_text,
        "source": src_source.get("source", ""),
        "collection": src_source.get("collection", ""),
        "organisation": src_source.get("organisation", ""),
        "endpoint": src_source.get("endpoint", ""),
        "licence": src_source.get("licence", ""),
        "pipelines": src_source.get("pipelines", ""),
        "entry_date": src_source.get("entry-date", ""),
        "start_date": src_source.get("start-date", ""),
        "end_date": src_source.get("end-date", ""),
        "documentation_url": src_source.get("documentation-url", ""),
        "attribution": src_source.get("attribution", ""),
    }

    return source_summary, source_csv_table_params, source_csv_text


def build_column_csv_preview(
    column_mapping: dict,
    dataset_id: str,
    endpoint_summary: dict,
    include_headers: bool = True,
) -> tuple:
    """
    Build column CSV preview from column mapping data.
    Returns (column_csv_table_params, column_csv_text, has_column_mapping)

    Args:
        column_mapping: Column mapping dict from request params
        dataset_id: Dataset ID
        endpoint_summary: Endpoint summary data from API response
        include_headers: If True, include CSV headers in csv_text. Default True.
    """
    column_csv_table_params = None
    column_csv_text = ""
    has_column_mapping = False

    logger.info("Entities preview: Checking for column mapping")
    logger.info(f"Column mapping data: {column_mapping}")

    if column_mapping:
        has_column_mapping = True

        # Get endpoint info from the request
        endpoint_hash = endpoint_summary.get("new_endpoint_entry", {}).get(
            "endpoint", ""
        ) or endpoint_summary.get("existing_endpoint_entry", {}).get("endpoint", "")

        # Build column CSV rows
        col_csv_cols = [
            "dataset",
            "endpoint",
            "resource",
            "column",
            "field",
            "start-date",
            "end-date",
            "entry-date",
        ]

        col_csv_rows = []
        # Build rows from the mapping dict
        for column_name, field_name in column_mapping.items():
            col_csv_rows.append(
                {
                    "dataset": dataset_id,
                    "endpoint": endpoint_hash,
                    "resource": "",
                    "column": column_name,
                    "field": field_name,
                    "start-date": "",
                    "end-date": "",
                    "entry-date": "",
                }
            )

        # Build table params
        column_csv_table_params = {
            "columns": col_csv_cols,
            "fields": col_csv_cols,
            "rows": [
                {"columns": {c: {"value": row.get(c, "")} for c in col_csv_cols}}
                for row in col_csv_rows
            ],
            "columnNameProcessing": "none",
        }

        # Build CSV text
        if include_headers:
            column_csv_text = ",".join(col_csv_cols) + "\n"
            for row in col_csv_rows:
                column_csv_text += (
                    ",".join([row.get(c, "") for c in col_csv_cols]) + "\n"
                )
            column_csv_text = column_csv_text.strip()
        else:
            csv_rows = []
            for row in col_csv_rows:
                csv_rows.append(",".join([row.get(c, "") for c in col_csv_cols]))
            column_csv_text = "\n".join(csv_rows)

    return column_csv_table_params, column_csv_text, has_column_mapping


def inject_now():
    return {"now": datetime}
