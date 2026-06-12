import json
import logging

logger = logging.getLogger(__name__)


def build_lookup_csv_preview(new_entities: list) -> dict:
    """
    Build lookup CSV preview for new entities.
    Returns table_params
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

    return table_params


def build_endpoint_csv_preview(
    endpoint_summary: dict, endpoint_parameters: dict | None = None
) -> tuple:
    """
    Build endpoint CSV preview.
    Returns (endpoint_already_exists, endpoint_url, table_params)
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

    if endpoint_summary.get("endpoint_url_in_endpoint_csv"):
        end_point_entry = endpoint_summary.get("existing_endpoint_entry", {})
        endpoint_url = end_point_entry.get("endpoint-url", "")
    else:
        end_point_entry = endpoint_summary.get("new_endpoint_entry", {})
        endpoint_url = end_point_entry.get("endpoint-url", "")
        if endpoint_parameters:
            end_point_entry = {
                **end_point_entry,
                "parameters": json.dumps(endpoint_parameters),
            }

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
    )


def build_source_csv_preview(source_summary_data: dict) -> tuple:
    """
    Build source CSV preview and summary.
    Returns (source_summary, table_params)

    Args:
        source_summary_data: Source summary data from API response
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
    source_already_exists_text = "Yes" if source_present else "No"

    pipelines_append_required = source_summary_data.get("pipelines_append_required")

    if not source_present:
        src_source = source_summary_data.get("new_source_entry", {})
        if pipelines_append_required:
            src_source = {
                **src_source,
                "pipelines": pipelines_append_required.get(
                    "updated", src_source.get("pipelines", "")
                ),
            }
        src_row = [str(src_source.get(col, "") or "") for col in src_cols]
        source_csv_table_params = {
            "columns": src_cols,
            "fields": src_cols,
            "rows": [{"columns": {c: {"value": v} for c, v in zip(src_cols, src_row)}}],
            "columnNameProcessing": "none",
        }
    else:
        src_source = source_summary_data.get("existing_source_entry", {})
        if pipelines_append_required:
            src_source = {
                **src_source,
                "pipelines": pipelines_append_required.get(
                    "updated", src_source.get("pipelines", "")
                ),
            }
        src_row = [str(src_source.get(col, "") or "") for col in src_cols]
        source_csv_table_params = {
            "columns": src_cols,
            "fields": src_cols,
            "rows": [{"columns": {c: {"value": v} for c, v in zip(src_cols, src_row)}}],
            "columnNameProcessing": "none",
        }

    logger.info(f"Source already exists: {source_already_exists_text}")

    source_summary = {
        "already_exists": source_already_exists_text,
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

    return source_summary, source_csv_table_params


def build_column_csv_preview(
    column_mapping: dict,
    dataset_id: str,
    endpoint_summary: dict,
) -> tuple:
    """
    Build column CSV preview from column mapping data.
    Returns (column_csv_table_params, has_column_mapping)

    Args:
        column_mapping: Column mapping dict from request params
        dataset_id: Dataset ID
        endpoint_summary: Endpoint summary data from API response
    """
    column_csv_table_params = None
    has_column_mapping = False

    logger.info("Entities preview: Checking for column mapping")
    logger.info(f"Column mapping data: {column_mapping}")

    if column_mapping:
        has_column_mapping = True

        endpoint_hash = endpoint_summary.get("new_endpoint_entry", {}).get(
            "endpoint", ""
        ) or endpoint_summary.get("existing_endpoint_entry", {}).get("endpoint", "")

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

        column_csv_table_params = {
            "columns": col_csv_cols,
            "fields": col_csv_cols,
            "rows": [
                {"columns": {c: {"value": row.get(c, "")} for c in col_csv_cols}}
                for row in col_csv_rows
            ],
            "columnNameProcessing": "none",
        }

    return column_csv_table_params, has_column_mapping


def build_entity_organisation_csv(entity_organisation_data: list) -> tuple:
    """
    Build entity-organisation CSV preview.
    Returns (table_params, has_data)

    Args:
        entity_organisation_data: List of entity-organisation entries from pipeline-summary
    """
    cols = ["dataset", "entity-minimum", "entity-maximum", "organisation"]

    if not entity_organisation_data:
        return None, False

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

    return table_params, True
