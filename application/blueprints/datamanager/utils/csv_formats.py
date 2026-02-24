import logging

logger = logging.getLogger(__name__)


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
