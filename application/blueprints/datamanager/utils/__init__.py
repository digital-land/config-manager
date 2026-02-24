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


def build_entity_organisation_csv(
    entity_organisation_data: list, include_headers: bool = True
) -> tuple:
    """
    Build entity-organisation CSV preview.
    Returns (table_params, csv_text, has_data)

    Args:
        entity_organisation_data: List of entity-organisation entries from pipeline-summary
        include_headers: If True, include CSV headers in csv_text. Default True.
    """
    cols = ["dataset", "entity-minimum", "entity-maximum", "organisation"]

    if not entity_organisation_data:
        return None, "", False

    # Build table rows
    rows = []
    for entry in entity_organisation_data:
        rows.append(
            {
                "columns": {
                    "dataset": {"value": str(entry.get("dataset", ""))},
                    "entity-minimum": {"value": str(entry.get("entity-minimum", ""))},
                    "entity-maximum": {"value": str(entry.get("entity-maximum", ""))},
                    "organisation": {"value": str(entry.get("organisation", ""))},
                }
            }
        )

    table_params = {
        "columns": cols,
        "fields": cols,
        "rows": rows,
        "columnNameProcessing": "none",
    }

    # Build CSV text
    csv_lines = []
    if include_headers:
        csv_lines.append(",".join(cols))

    for entry in entity_organisation_data:
        csv_lines.append(
            ",".join(
                [
                    str(entry.get("dataset", "")),
                    str(entry.get("entity-minimum", "")),
                    str(entry.get("entity-maximum", "")),
                    str(entry.get("organisation", "")),
                ]
            )
        )

    csv_text = "\n".join(csv_lines)

    return table_params, csv_text, True


def build_check_tables(column_field_log, resp_details):
    """Build converted, transformed, and issue log table params from check response data.

    Returns (converted_table, transformed_table, issue_log_table) where each
    is a dict compatible with the table() macro in components/table.html.
    """
    # Converted table: entry_number + columns from column_field_log "column" key
    # plus any extra fields found in converted_row data (e.g. geom)
    converted_headers = ["entry_number"] + [
        entry["column"] for entry in column_field_log if entry.get("column")
    ]
    known_columns = set(converted_headers)
    # Also extract spec fields for use in column mapping UI
    spec_fields = {entry["field"] for entry in column_field_log if entry.get("field")}

    # First pass: discover any extra fields present in the data but not in column_field_log
    unmapped_columns = set()
    for row in resp_details:
        converted = row.get("converted_row") or {}
        for key in converted:
            if key not in known_columns:
                converted_headers.append(key)
                known_columns.add(key)
                unmapped_columns.add(key)

    converted_rows = []
    for row in resp_details:
        converted = row.get("converted_row") or {}
        if any(str(v).strip() for v in converted.values()):
            columns = {"entry_number": {"value": str(row.get("entry_number", ""))}}
            columns.update({
                col: {"value": str(converted.get(col, ""))}
                for col in converted_headers if col != "entry_number"
            })
            converted_rows.append({"columns": columns})

    converted_table = {
        "columns": converted_headers,
        "fields": converted_headers,
        "rows": converted_rows,
        "columnNameProcessing": "none",
        "unmapped_columns": unmapped_columns,
    }

    # Transformed table: entry_number + columns from column_field_log "field" key
    transformed_headers = ["entry_number"] + [
        entry["field"] for entry in column_field_log if entry.get("field")
    ]
    transformed_rows = []
    for row in resp_details:
        transformed = row.get("transformed_row") or []
        field_values = {
            item["field"]: item.get("value", "")
            for item in transformed
            if isinstance(item, dict) and item.get("field")
        }
        if any(str(v).strip() for v in field_values.values()):
            columns = {"entry_number": {"value": str(row.get("entry_number", ""))}}
            columns.update({
                col: {"value": str(field_values.get(col, ""))}
                for col in transformed_headers if col != "entry_number"
            })
            transformed_rows.append({"columns": columns})

    transformed_table = {
        "columns": transformed_headers,
        "fields": transformed_headers,
        "rows": transformed_rows,
        "columnNameProcessing": "none",
    }

    # Issue log table: flattened from all rows' issue_logs
    issue_log_headers = [
        "entry-number", "field", "issue-type", "severity",
        "message", "description", "value", "responsibility",
    ]
    issue_log_rows = []
    for row in resp_details:
        for issue in (row.get("issue_logs") or []):
            issue_log_rows.append({
                "columns": {
                    col: {"value": str(issue.get(col, ""))}
                    for col in issue_log_headers
                }
            })

    issue_log_table = {
        "columns": issue_log_headers,
        "fields": issue_log_headers,
        "rows": issue_log_rows,
        "columnNameProcessing": "none",
    }

    return converted_table, transformed_table, issue_log_table, spec_fields


def inject_now():
    return {"now": datetime}
