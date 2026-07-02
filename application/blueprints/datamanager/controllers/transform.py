import logging
import re

import requests
from flask import render_template, request as flask_request
from shapely import wkt
from shapely.geometry import mapping

from . import ControllerError
from ..config import get_entity_geojson_url, get_entity_search_url
from ..services.async_api import fetch_response_details
from ..services.dataset import get_dataset_name, get_dataset_typology
from ..services.organisation import get_org_entity, get_organisation_name
from ..services.doc_crawler import check_endpoint_in_doc, is_gov_uk_url
from ..services.endpoint import get_endpoint_urls_for_hashes
from ..services.planning_data import (
    get_entities_for_organisation_and_dataset,
    get_entity_count_for_organisation_and_dataset,
)

logger = logging.getLogger(__name__)

_TRANSFORM_COLS = [
    "entry_number",
    "entity",
    "field",
    "value",
    "start-date",
    "end-date",
    "reference-entity",
]

_ISSUE_COLS = [
    "entry-number",
    "field",
    "issue-type",
    "severity",
    "message",
    "description",
    "value",
    "responsibility",
]

_ENTITY_COL_EXCLUDE = {
    "prefix",
    "typology",
    "organisation-entity",
    "organisation",
    "end-date",
    "dataset",
}
_ENTITY_COL_PRIORITY = ["entity", "reference", "name"]
_ROWS_PER_PAGE = 500
_PLATFORM_ENTITY_LIMIT = 10000
_GEO_FIELDS = {"geometry", "point"}
_DATE_PREFIX_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})[T ]")
_CHANGED_VALUE_MAX_LEN = 200
# Hausdorff-distance tolerance in EPSG:4326 degrees (~10cm). Large enough to
# absorb reprocessing noise (precision, vertex ordering, geometry-type wrapping)
# but small enough to detect a genuinely moved boundary or point.
_GEO_TOLERANCE = 1e-6


def _normalise_entity_id(raw) -> str:
    if raw is None or raw == "":
        return ""
    try:
        return str(int(float(str(raw))))
    except (ValueError, TypeError):
        return str(raw)


def _normalise_field_value(value) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    # Platform values are often typed (100 vs "100.0" from the pipeline).
    try:
        f = float(s)
        if f == int(f):
            return str(int(f))
    except (ValueError, OverflowError):
        pass
    # Platform datetimes vs pipeline dates: compare on the date part only.
    m = _DATE_PREFIX_RE.match(s)
    if m:
        return m.group(1)
    return s


def _geometries_differ(res_wkt: str, plat_wkt: str) -> bool:
    """
    True when two WKT geometries represent meaningfully different shapes.

    Uses Hausdorff distance so that reprocessing artefacts (coordinate
    precision, vertex ordering, POINT vs MULTIPOINT wrapping) are ignored,
    while a genuine move of a boundary or point is detected.
    """
    try:
        g1 = wkt.loads(res_wkt)
        g2 = wkt.loads(plat_wkt)
    except Exception:
        # Unparseable — fall back to an exact text comparison.
        return str(res_wkt).strip() != str(plat_wkt).strip()
    if g1.is_empty or g2.is_empty:
        return g1.is_empty != g2.is_empty
    try:
        return g1.hausdorff_distance(g2) > _GEO_TOLERANCE
    except Exception:
        return not g1.equals(g2)


def _diff_entity_fields(resource_fields: dict, platform_entity: dict) -> dict:
    """
    Return {column: platform_value} for fields whose resource value differs
    from the platform entity's value. Only fields the resource provided are
    compared. Geometry/point are compared by shape (see _geometries_differ)
    rather than raw WKT, since platform geometry is reprocessed and never
    matches the submitted text.
    """
    changed = {}
    for col, res_val in resource_fields.items():
        if col == "entity" or col in _ENTITY_COL_EXCLUDE:
            continue
        plat_val = platform_entity.get(col)
        if col in _GEO_FIELDS:
            res_has = bool(str(res_val or "").strip())
            plat_has = bool(str(plat_val or "").strip())
            if res_has != plat_has:
                changed[col] = "on platform" if plat_has else "(no value on platform)"
            elif res_has and plat_has and _geometries_differ(res_val, plat_val):
                changed[col] = "(different geometry on platform)"
            continue
        if _normalise_field_value(res_val) != _normalise_field_value(plat_val):
            changed[col] = str(
                plat_val if plat_val not in (None, "") else "(no value on platform)"
            )[:_CHANGED_VALUE_MAX_LEN]
    return changed


def _build_entities_data(resp_details: list, platform_entities: list) -> dict:
    """
    Pivot transformed facts from resp_details by entity and combine with
    platform entities. Returns a dict with 'columns' and 'rows', where each
    row has 'fields' (dict) and 'is_new' (bool).
    """
    pivoted = {}
    for item in resp_details:
        facts = item.get("transformed_row") or []
        if not isinstance(facts, list) or not facts:
            continue
        entity_id = _normalise_entity_id(facts[0].get("entity", ""))
        if not entity_id:
            continue
        pivoted[entity_id] = {
            fact.get("field", ""): fact.get("value", "")
            for fact in facts
            if fact.get("field")
        }

    platform_by_id = {
        _normalise_entity_id(e.get("entity", "")): e for e in platform_entities
    }
    platform_entity_ids = set(platform_by_id)
    in_both_ids = set(pivoted.keys()) & platform_entity_ids

    all_col_keys = set(_ENTITY_COL_PRIORITY)
    for fields in pivoted.values():
        all_col_keys.update(fields.keys())
    for e in platform_entities:
        all_col_keys.update(e.keys())
    all_col_keys -= _ENTITY_COL_EXCLUDE
    columns = _ENTITY_COL_PRIORITY + sorted(all_col_keys - set(_ENTITY_COL_PRIORITY))

    rows = []
    for entity_id, fields in pivoted.items():
        entity_on_platform = entity_id in in_both_ids
        changed_fields = (
            _diff_entity_fields(fields, platform_by_id[entity_id])
            if entity_on_platform
            else {}
        )
        # Category drives both the row colour and the table filter:
        #   new      - only in this resource (green)
        #   changed  - on the platform and in this resource, with a difference (orange)
        #   in_both  - on the platform and in this resource, unchanged (yellow)
        if not entity_on_platform:
            category = "new"
        elif changed_fields:
            category = "changed"
        else:
            category = "in_both"
        rows.append(
            {
                "fields": {
                    col: (entity_id if col == "entity" else str(fields.get(col, "")))
                    for col in columns
                },
                "is_new": category == "new",
                "is_in_both": category == "changed",
                "category": category,
                "changed_fields": changed_fields,
            }
        )
    for entity_id, e in platform_by_id.items():
        if entity_id not in pivoted:
            rows.append(
                {
                    "fields": {col: str(e.get(col, "")) for col in columns},
                    "is_new": False,
                    "is_in_both": False,
                    "category": "existing",
                    "changed_fields": {},
                }
            )

    return {"columns": columns, "rows": rows}


def _entity_row_matches_search(row: dict, search_query: str) -> bool:
    if not search_query:
        return True

    fields = row.get("fields") or {}
    row_text = " ".join(str(value) for value in fields.values()).lower()
    return search_query.lower() in row_text


def _entity_row_matches_filter(row: dict, category_filter: str) -> bool:
    if not category_filter:
        return True
    return row.get("category") == category_filter


def _resolve_existing_endpoints(source_summary: dict) -> list:
    existing_endpoints = (
        source_summary.get("existing_endpoint_for_organisation_dataset") or []
    )
    if isinstance(existing_endpoints, str):
        existing_endpoints = [existing_endpoints] if existing_endpoints else []
    if existing_endpoints:
        endpoint_data = get_endpoint_urls_for_hashes(existing_endpoints)
        existing_endpoints = [
            {
                "endpoint": h,
                "endpoint-url": endpoint_data.get(h, {}).get("endpoint_url", ""),
                "end-date": endpoint_data.get(h, {}).get("end_date", ""),
            }
            for h in existing_endpoints
        ]
    return existing_endpoints


def _fetch_platform_entities(organisation_code: str, dataset_id: str) -> tuple:
    org_entity = get_org_entity(organisation_code)
    existing_count = (
        get_entity_count_for_organisation_and_dataset(org_entity, dataset_id)
        if org_entity is not None
        else 0
    )
    platform_too_large = existing_count > _PLATFORM_ENTITY_LIMIT
    platform_entities = (
        get_entities_for_organisation_and_dataset(org_entity, dataset_id)
        if org_entity is not None and not platform_too_large
        else []
    )
    return platform_entities, platform_too_large, existing_count


def _build_entity_growth_check(new_count: int, existing_count: int) -> dict:
    if existing_count > 0:
        growth_pct = round((new_count / existing_count) * 100, 1)
        growth_error = growth_pct > 10
    else:
        growth_pct = None
        growth_error = False

    return {
        "new_count": new_count,
        "existing_count": existing_count,
        "growth_pct": growth_pct,
        "error": growth_error,
    }


def _paginate_entity_data(
    all_resp_details: list,
    platform_entities: list,
    entity_page: int,
    entity_search: str,
    entity_filter: str = "",
) -> tuple:
    entity_start_offset = (entity_page - 1) * _ROWS_PER_PAGE
    entities_data_full = _build_entities_data(all_resp_details, platform_entities)
    if entity_search or entity_filter:
        entities_data_full["rows"] = [
            row
            for row in entities_data_full["rows"]
            if _entity_row_matches_search(row, entity_search)
            and _entity_row_matches_filter(row, entity_filter)
        ]
    has_next_entity_page = (
        len(entities_data_full["rows"]) > entity_start_offset + _ROWS_PER_PAGE
    )
    entity_page_rows = entities_data_full["rows"][
        entity_start_offset : entity_start_offset + _ROWS_PER_PAGE
    ]
    entity_page_start = (
        entity_page_rows[0]["fields"].get("entity", "") if entity_page_rows else ""
    )
    entity_page_end = (
        entity_page_rows[-1]["fields"].get("entity", "") if entity_page_rows else ""
    )
    entities_data = {
        "columns": entities_data_full["columns"],
        "rows": entity_page_rows,
    }
    return entities_data, has_next_entity_page, entity_page_start, entity_page_end


def _build_transform_table(resp_details: list) -> dict:
    rows = []
    for item in resp_details:
        entry_number = str(item.get("entry_number", ""))
        for fact in item.get("transformed_row") or []:
            if not isinstance(fact, dict):
                continue
            row = {
                "entry_number": entry_number,
                "entity": str(fact.get("entity", "")),
                "field": str(fact.get("field", "")),
                "value": str(fact.get("value", "")),
                "start-date": str(fact.get("start-date", "")),
                "end-date": str(fact.get("end-date", "")),
                "reference-entity": str(fact.get("reference-entity", "")),
            }
            rows.append({"columns": {c: {"value": row[c]} for c in _TRANSFORM_COLS}})
    return {
        "columns": _TRANSFORM_COLS,
        "fields": _TRANSFORM_COLS,
        "rows": rows,
        "columnNameProcessing": "none",
    }


def _build_issue_log_table(resp_details: list) -> dict:
    rows = []
    for item in resp_details:
        for issue in item.get("issue_logs") or []:
            cols = {}
            for col in _ISSUE_COLS:
                val = str(issue.get(col, ""))
                if col == "severity" and val.lower() == "error":
                    cols[col] = {
                        "value": val,
                        "html": (
                            '<span style="background-color:#d4351c;color:white;'
                            'padding:2px 8px;border-radius:3px;">error</span>'
                        ),
                    }
                else:
                    cols[col] = {"value": val}
            rows.append({"columns": cols})
    return {
        "columns": _ISSUE_COLS,
        "fields": _ISSUE_COLS,
        "rows": rows,
        "columnNameProcessing": "none",
    }


def _build_geometry_features(
    platform_entities: list, all_resp_details: list, dataset_id: str
) -> list:
    platform_by_id = {
        _normalise_entity_id(str(e.get("entity", ""))): e
        for e in platform_entities
        if e.get("entity", "")
    }
    platform_entity_ids = set(platform_by_id)
    resource_entity_ids = set()
    for item in all_resp_details:
        facts = item.get("transformed_row") or []
        if isinstance(facts, list) and facts:
            entity_id = _normalise_entity_id(facts[0].get("entity", ""))
            if entity_id:
                resource_entity_ids.add(entity_id)

    features = []

    for entity in platform_entities:
        entity_id = _normalise_entity_id(str(entity.get("entity", "")))
        if entity_id in resource_entity_ids:
            continue
        geom_wkt = entity.get("geometry") or entity.get("point")
        if not geom_wkt:
            continue
        try:
            geom = mapping(wkt.loads(geom_wkt))
            features.append(
                {
                    "type": "Feature",
                    "geometry": geom,
                    "properties": {
                        "reference": entity.get("reference", ""),
                        "name": entity.get("name", ""),
                        "status": "existing",
                    },
                }
            )
        except Exception as e:
            logger.warning(
                "Error parsing geometry for platform entity %s: %s", entity_id, e
            )

    for item in all_resp_details:
        converted_row = item.get("converted_row") or {}
        transformed_row = item.get("transformed_row") or []
        if not isinstance(transformed_row, list) or not transformed_row:
            continue
        entity_id = _normalise_entity_id(transformed_row[0].get("entity", ""))
        geometry_entry = next(
            (
                f
                for f in transformed_row
                if isinstance(f, dict) and f.get("field") in ("geometry", "point")
            ),
            None,
        )
        if not geometry_entry or not geometry_entry.get("value"):
            continue
        if entity_id in platform_entity_ids:
            resource_fields = {
                f.get("field", ""): f.get("value", "")
                for f in transformed_row
                if isinstance(f, dict) and f.get("field")
            }
            # Same four categories as the entities table: a resource entity
            # already on the platform is "changed" if a field differs, else
            # "in_both" (present but unchanged).
            changed = _diff_entity_fields(resource_fields, platform_by_id[entity_id])
            status = "changed" if changed else "in_both"
        else:
            status = "new"
        try:
            geom = mapping(wkt.loads(geometry_entry["value"]))
            features.append(
                {
                    "type": "Feature",
                    "geometry": geom,
                    "properties": {
                        "reference": (
                            converted_row.get("reference")
                            or converted_row.get("Reference")
                            or f"Entry {item.get('entry_number')}"
                        ),
                        "name": converted_row.get("name", ""),
                        "status": status,
                    },
                }
            )
        except Exception as e:
            logger.warning(
                "Error parsing geometry for resource entry %s: %s",
                item.get("entry_number"),
                e,
            )

    return features


def _fetch_boundary_geojson(organisation_code: str) -> dict:
    empty = {"type": "FeatureCollection", "features": []}
    try:
        if ":" not in organisation_code:
            return empty
        lpa_prefix, lpa_id = organisation_code.split(":", 1)
        resp = requests.get(get_entity_search_url(lpa_prefix, lpa_id))
        resp.raise_for_status()
        d = resp.json()
        entity = d.get("entities", [])[0] if d and d.get("entities") else None
        if not entity:
            return empty
        reference = (
            entity.get("local-planning-authority") if entity.get("reference") else ""
        )
        if not reference:
            return empty
        return requests.get(get_entity_geojson_url(reference)).json()
    except Exception as e:
        logger.warning("Failed to fetch boundary data for %s: %s", organisation_code, e)
        return empty


def handle_check_transform(
    request_id,
    req,
    transform_endpoint="datamanager.check_transform",
    template_name="datamanager/check-transform.html",
    flagged_errors=None,
    flagged_error_abbreviations=None,
    flagged_error_messages=None,
):
    """Display transformed facts and issue logs from response-details for a request.
    Compare this with platform entities for the organisation and dataset,
    with logic to handle comparing geometries and normalising field values.

    Shows a loading page while the async job is still running, and the full
    transformed data once it completes.
    """
    params = req.get("params") or {}
    organisation_code = params.get("organisationName") or params.get("organisation", "")
    dataset_id = params.get("dataset", "")
    resource_hash = params.get("resource", "")
    organisation_display = get_organisation_name(organisation_code)
    dataset_display = get_dataset_name(dataset_id, default=dataset_id)

    endpoint_url = params.get("url", "")
    documentation_url = params.get("documentation_url", "")

    status = req.get("status")

    if status == "FAILED":
        response_payload = req.get("response") or {}
        response_error = response_payload.get("error")
        raise ControllerError(
            response_error.get("errMsg")
            if response_error
            else "Async job failed with no error information"
        )

    if status in {"PENDING", "PROCESSING", "QUEUED"} or req.get("response") is None:
        # Pre-warm the cache so the result is ready when the job completes.
        if endpoint_url and documentation_url:
            check_endpoint_in_doc(documentation_url, endpoint_url)
        return render_template(
            "datamanager/check-transform-loading.html",
            request_id=request_id,
            organisation_display=organisation_display,
            dataset_display=dataset_display,
            transform_endpoint=transform_endpoint,
        )

    # Fetch the response details and platform entities for the organisation and dataset.
    all_resp_details = fetch_response_details(request_id)
    platform_entities, platform_too_large, existing_count = _fetch_platform_entities(
        organisation_code, dataset_id
    )

    response_payload = req.get("response") or {}
    response_data = response_payload.get("data") or {}
    source_summary = response_data.get("source-summary") or {}
    existing_endpoints = _resolve_existing_endpoints(source_summary)
    pipelines_append_required = source_summary.get("pipelines_append_required")

    pipeline_summary = response_data.get("pipeline-summary") or {}
    new_count = int(pipeline_summary.get("new-in-resource") or 0)

    entity_growth_check = _build_entity_growth_check(new_count, existing_count)

    # Calculate pagination for transformed facts and issue logs, and for entities.
    page_number = max(1, int(flask_request.args.get("page_number", 1)))
    start_offset = (page_number - 1) * _ROWS_PER_PAGE
    resp_details = all_resp_details[start_offset : start_offset + _ROWS_PER_PAGE]
    page_start = start_offset + 1
    page_end = start_offset + len(resp_details)
    has_next_page = len(all_resp_details) > start_offset + _ROWS_PER_PAGE
    entity_page = max(1, int(flask_request.args.get("entity_page", 1)))

    entity_search = flask_request.args.get("entity_search", "").strip()
    entity_filter = flask_request.args.get("entity_filter", "").strip()

    # Build three paginated tables: transformed facts, issue logs, and entities.
    # The entities table is built from the transformed facts and the platform entities.
    entities_data, has_next_entity_page, entity_page_start, entity_page_end = (
        _paginate_entity_data(
            all_resp_details,
            platform_entities,
            entity_page,
            entity_search,
            entity_filter,
        )
    )
    transformed_table = _build_transform_table(resp_details)
    issue_log_table = _build_issue_log_table(resp_details)

    # Build a GeoJSON feature collection for the map if needed, including any platform entities not in the resource,
    # and any new or updated resource entities with geometry.
    if get_dataset_typology(dataset_id) == "geography":
        geometries = _build_geometry_features(
            platform_entities, all_resp_details, dataset_id
        )
        boundary_geojson = (
            _fetch_boundary_geojson(organisation_code) if geometries else None
        )
    else:
        geometries = []
        boundary_geojson = None

    # Checks for whether endpoint is found in documentation url
    endpoint_in_doc = check_endpoint_in_doc(documentation_url, endpoint_url)
    doc_is_gov_uk = is_gov_uk_url(documentation_url)
    endpoint_is_gov_uk = is_gov_uk_url(endpoint_url)

    return render_template(
        template_name,
        request_id=request_id,
        transform_endpoint=transform_endpoint,
        organisation_display=organisation_display,
        dataset_display=dataset_display,
        transformed_table=transformed_table,
        issue_log_table=issue_log_table,
        existing_endpoints=existing_endpoints,
        pipelines_append_required=pipelines_append_required,
        entity_growth_check=entity_growth_check,
        entities_data=entities_data,
        platform_too_large=platform_too_large,
        existing_count=existing_count,
        page_number=page_number,
        has_next_page=has_next_page,
        page_start=page_start,
        page_end=page_end,
        entity_page=entity_page,
        entity_search=entity_search,
        entity_filter=entity_filter,
        has_next_entity_page=has_next_entity_page,
        entity_page_start=entity_page_start,
        entity_page_end=entity_page_end,
        endpoint_in_doc=endpoint_in_doc,
        doc_is_gov_uk=doc_is_gov_uk,
        endpoint_is_gov_uk=endpoint_is_gov_uk,
        endpoint_url=endpoint_url,
        documentation_url=documentation_url,
        resource_hash=resource_hash,
        geometries=geometries,
        boundary_geojson=boundary_geojson,
        flagged_errors=flagged_errors or [],
        flagged_error_abbreviations=flagged_error_abbreviations or [],
        flagged_error_messages=flagged_error_messages or [],
    )
