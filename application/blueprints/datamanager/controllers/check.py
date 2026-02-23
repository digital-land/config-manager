import logging
from datetime import datetime

import requests
from flask import redirect, render_template, request, url_for
from shapely import wkt
from shapely.geometry import mapping

from . import ControllerError
from ..config import (
    get_entity_geojson_url,
    get_entity_search_url,
)
from ..services.async_api import (
    AsyncAPIError,
    fetch_request,
    fetch_response_details,
    submit_request,
)
from ..services.dataset import (
    get_dataset_name,
)
from ..services.organisation import (
    get_organisation_name,
)
from ..utils import (
    build_check_tables,
)
from ..utils.configure import (
    build_column_mapping_rows,
)

logger = logging.getLogger(__name__)


def handle_check_results(request_id, result):
    # Extract org code, TODO bit hacky as submit and manage use this param for code / value
    organisation_code = result.get("params", {}).get("organisationName")
    dataset_id = result.get("params", {}).get("dataset")
    if not organisation_code:
        raise ControllerError("Organisation code missing from result params")

    result["params"]["organisation_display"] = get_organisation_name(organisation_code)
    result["params"]["dataset_display"] = get_dataset_name(dataset_id, default=dataset_id)

    # Format date checked
    raw_date = result.get("modified") or result.get("created") or ""
    try:
        dt = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
        result["date_checked"] = dt.strftime("%-d %B %Y at %H:%M")
    except (ValueError, AttributeError):
        result["date_checked"] = raw_date

    # Check If Still Processing
    if (
        result.get("status") in ["PENDING", "PROCESSING", "QUEUED"]
        or result.get("response") is None
    ):
        return render_template(
            "datamanager/check-results-loading.html", result=result
        )

    # Check async error
    response_data = result.get("response")
    if not response_data or response_data.get("data") is None:
        error_msg = "No data returned from check"
        if response_data and response_data.get("error"):
            error_msg = response_data.get("error").get("errMsg", error_msg)
        raise ControllerError(error_msg)

    # Fetch all response details using multiple calls
    resp_details = fetch_response_details(request_id)

    # Geometry mapping creation
    geometries = []
    for row in resp_details:
        converted_row = row.get("converted_row") or {}
        transformed_row = row.get("transformed_row") or []

        geometry_entry = None
        if isinstance(transformed_row, list):
            geometry_entry = next(
                (
                    item
                    for item in transformed_row
                    if isinstance(item, dict)
                    and (
                        item.get("field") == "geometry"
                        or item.get("field") == "point"
                    )
                ),
                None,
            )
        if geometry_entry and geometry_entry.get("value"):
            try:
                shapely_geom = wkt.loads(geometry_entry["value"])
                geom = mapping(shapely_geom)
                geometries.append(
                    {
                        "type": "Feature",
                        "geometry": geom,
                        "properties": {
                            "reference": converted_row.get("reference")
                            or converted_row.get("Reference")
                            or f"Entry {row.get('entry_number')}",
                            "name": converted_row.get("name", ""),
                        },
                    }
                )
            except Exception as e:
                logger.warning(
                    f"Error parsing geometry for entry {row.get('entry_number')}: {e}"
                )
                continue

    # Generate boundary GeoJSON for the LPA
    boundary_geojson_url = ""
    try:
        if ":" in organisation_code:
            dataset_id, lpa_id = organisation_code.split(":", 1)
            resp = requests.get(get_entity_search_url(dataset_id, lpa_id))
            resp.raise_for_status()
            d = resp.json()
            entity = (
                d.get("entities", [])[0] if d and d.get("entities") else None
            )
            if not entity:
                boundary_geojson_url = {
                    "type": "FeatureCollection",
                    "features": [],
                }
            else:
                reference = (
                    entity.get("local-planning-authority")
                    if entity.get("reference")
                    else ""
                )
                if not reference:
                    boundary_geojson_url = {
                        "type": "FeatureCollection",
                        "features": [],
                    }
                else:
                    boundary_geojson_url = requests.get(get_entity_geojson_url(reference)).json()
        else:
            boundary_geojson_url = {"type": "FeatureCollection", "features": []}
    except Exception as e:
        logger.warning(f"Failed to fetch boundary data: {e}")
        boundary_geojson_url = {"type": "FeatureCollection", "features": []}

    # Error summary parsing from overall response
    data = (result.get("response") or {}).get("data") or {}
    error_summary = data.get("error-summary", []) or []
    column_field_log = data.get("column-field-log", []) or []

    # Build converted, transformed and issue log tables
    converted_table, transformed_table, issue_log_table = build_check_tables(column_field_log, resp_details)

    # Build column mapping rows for inline configure UI
    unmapped_columns = converted_table.get("unmapped_columns", set())
    mapping_rows = build_column_mapping_rows(column_field_log, unmapped_columns)

    # Checks
    must_fix, fixable, passed_checks = [], [], []
    for err in error_summary:
        fixable.append(err)
    for col in column_field_log:
        if not col.get("missing"):
            passed_checks.append(f"All rows have {col.get('field')} present")
        else:
            must_fix.append(f"Missing required field: {col.get('field')}")
    allow_add_data = len(must_fix) == 0

    return render_template(
        "datamanager/check-results.html",
        result=result,
        geometries=geometries,
        must_fix=must_fix,
        fixable=fixable,
        passed_checks=passed_checks,
        allow_add_data=allow_add_data,
        converted_table=converted_table,
        transformed_table=transformed_table,
        issue_log_table=issue_log_table,
        boundary_geojson_url=boundary_geojson_url,
        request_id=request_id,
        mapping_rows=mapping_rows,
    )


def handle_check_resubmit(request_id):
    """Re-run a check with updated pipeline configuration.

    Reads the original request params and merges in any user-submitted
    pipeline config (currently column mappings). Submits a new check
    and redirects to the results page.
    """
    try:
        req = fetch_request(request_id)
    except AsyncAPIError:
        return render_template("datamanager/error.html", message="Original request not found"), 404

    params = req.get("params", {}) or {}

    # Collect user-submitted column mappings
    column_mapping = {}
    form = request.form.to_dict()
    for key, value in form.items():
        if key.startswith("map[") and key.endswith("]"):
            col_name = key[4:-1]
            field_value = value.strip()
            if field_value:
                column_mapping[col_name] = field_value

    # Submit new check with updated config
    payload_params = {
        "type": "check_url",
        "collection": params.get("collection"),
        "dataset": params.get("dataset"),
        "url": params.get("url"),
        "documentation_url": params.get("documentation_url"),
        "licence": params.get("licence"),
        "start_date": params.get("start_date"),
        "column_mapping": column_mapping or None,
        "geom_type": params.get("geom_type"),
        "organisation": params.get("organisation"),
        "organisationName": params.get("organisationName"),
    }

    try:
        new_id = submit_request(payload_params)
        return redirect(url_for("datamanager.check_results", request_id=new_id))
    except AsyncAPIError as e:
        return render_template(
            "datamanager/error.html",
            message=f"Re-check submission failed: {e.detail}",
        )
