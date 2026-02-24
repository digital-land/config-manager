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
