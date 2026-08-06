import json
import logging
import os
import re

import requests
from flask import current_app, render_template, request as flask_request
from shapely import wkt
from shapely.geometry import mapping

from . import ControllerError
from application.utils import compute_hash
from ..services.async_api import fetch_response_details
from ..services.dataset import get_dataset_name, get_dataset_typology
from ..services.duplicates import REDIRECT_STATUSES
from ..services.organisation import get_org_entity, get_organisation_name
from ..services.doc_crawler import check_endpoint_in_doc, is_gov_uk_url
from ..services.endpoint import (
    get_endpoint_log_summary_for_hashes,
    get_endpoint_info_for_hashes,
)
from ..services.planning_data import (
    get_entities_for_organisation_and_dataset,
    get_entity_count_for_organisation_and_dataset,
)
from ..utils import REQUESTS_TIMEOUT

logger = logging.getLogger(__name__)


def _entity_search_url(dataset_id, reference):
    base = current_app.config["PLANNING_BASE_URL"]
    return f"{base}/entity.json?dataset={dataset_id}&reference={reference}"


def _entity_geojson_url(reference):
    base = current_app.config["PLANNING_BASE_URL"]
    return f"{base}/entity.geojson?reference={reference}"


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
    "entry-date",  # This is excluded as platform entities use entry date at point of ingestion
    "dataset",
}
_ENTITY_COL_PRIORITY = ["entity", "reference", "name"]
_DUPLICATE_FIXED_FIELDS = {
    "entity",
    "reference",
    "name",
    "entry-date",
    "entry_date",
    "end-date",
    "end_date",
    "organisation",
    "organisation-entity",
    "organisation_entity",
}
_ROWS_PER_PAGE = 500
_PLATFORM_ENTITY_LIMIT = 10000
_GEO_FIELDS = {"geometry", "point"}
_DATE_PREFIX_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})[T ]")
_CHANGED_VALUE_MAX_LEN = 200
# Hausdorff-distance tolerance in EPSG:4326 degrees. 1e-4 deg is roughly 10m
# of perpendicular deviation at UK latitudes: large enough to absorb
# reprocessing noise (coordinate precision, vertex ordering/sliding,
# geometry-type wrapping) but still small enough to detect a genuinely moved
# boundary.
_GEO_TOLERANCE = 1e-4


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
    row has 'fields' (dict), 'category' (str), and 'changed_fields' (dict).
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
                "category": category,
                "changed_fields": changed_fields,
            }
        )
    for entity_id, e in platform_by_id.items():
        if entity_id not in pivoted:
            rows.append(
                {
                    "fields": {col: str(e.get(col, "")) for col in columns},
                    "category": "existing",
                    "changed_fields": {},
                }
            )

    return {"columns": columns, "rows": rows}


def _normalise_excluded_references(excluded_references) -> set:
    """Return explicitly excluded references from async request params.

    A missing or empty param means no references were excluded from assignment.
    """
    if not excluded_references:
        return set()
    if not isinstance(excluded_references, list):
        return set()
    return {
        str(reference).strip()
        for reference in excluded_references
        if str(reference).strip()
    }


def _entity_selection_form_value(reference: str) -> str:
    return reference


def _add_assign_entities_selection_metadata(
    entities_data: dict,
    excluded_references,
) -> dict:
    """Add checkbox metadata to Assign Entities rows.

    Only rows categorised as new can be assigned entity numbers. Async request
    params contain references excluded from assignment, so every selectable row
    starts checked unless its reference is explicitly excluded.
    """
    excluded_reference_set = _normalise_excluded_references(excluded_references)

    for row in entities_data.get("rows", []):
        fields = row.get("fields") or {}
        reference = str(fields.get("reference", "")).strip()
        can_select = row.get("category") == "new" and bool(reference)
        selected = can_select and reference not in excluded_reference_set
        row["entity_selection"] = {
            "can_select": can_select,
            "selected": selected,
            "form_value": (
                _entity_selection_form_value(reference) if reference else ""
            ),
        }

    return entities_data


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


def _dedup_candidate_redirect_key(candidate: dict) -> tuple[str, str]:
    return (
        str(candidate.get("old_entity", "") or "").strip(),
        str(candidate.get("entity", "") or "").strip(),
    )


def _old_entity_redirect_key(row: dict) -> tuple[str, str]:
    return (
        str(row.get("old-entity", "") or row.get("old_entity", "") or "").strip(),
        str(row.get("entity", "") or "").strip(),
    )


def _dedup_candidate_selected_entity_reference(candidate: dict) -> str:
    return str(
        candidate.get("new_reference", "") or candidate.get("reference", "") or ""
    ).strip()


def _dedup_candidate_selected_redirect_key(candidate: dict) -> tuple[str, str]:
    """Return the key used to compare a Dedup candidate with selected_redirects."""
    return (
        _dedup_candidate_selected_entity_reference(candidate),
        str(candidate.get("old_entity", "") or "").strip(),
    )


def _selected_redirect_key(redirect: dict) -> tuple[str, str]:
    """Return the async selected_redirects key for a submitted redirect param."""
    return (
        str(redirect.get("reference", "") or "").strip(),
        str(
            redirect.get("old_entity_number", "")
            or redirect.get("old_entity", "")
            or ""
        ).strip(),
    )


def _selected_redirect_status(redirect: dict) -> str:
    status = str(redirect.get("status", "") or "301").strip()
    return status if status in REDIRECT_STATUSES else "301"


def _dedup_candidate_form_value(candidate: dict, status: str = "") -> str:
    return json.dumps(
        {
            "old_entity": candidate.get("old_entity", ""),
            "dataset": candidate.get("dataset", ""),
            "new_reference": candidate.get("new_reference", ""),
            "status": status,
        },
        separators=(",", ":"),
    )


def _dedup_candidate_field_maps(candidate: dict) -> tuple[dict, dict]:
    old_fields = (
        dict(candidate.get("old_fields") or {})
        if isinstance(candidate.get("old_fields"), dict)
        else {}
    )
    new_fields = (
        dict(candidate.get("new_fields") or {})
        if isinstance(candidate.get("new_fields"), dict)
        else {}
    )
    return old_fields, new_fields


def _dedup_dynamic_columns(candidates: list[dict]) -> list[str]:
    fields = {
        str(field)
        for candidate in candidates
        for field_map in (candidate.get("old_fields"), candidate.get("new_fields"))
        if isinstance(field_map, dict)
        for field in field_map
    }
    excluded_fields = _ENTITY_COL_EXCLUDE | _DUPLICATE_FIXED_FIELDS
    return sorted(field for field in fields if field not in excluded_fields)


def _show_dedup_tab(
    is_assign_entities: bool, dataset_id: str, dataset_typology: str
) -> bool:
    return is_assign_entities and (
        dataset_id == "conservation-area" or dataset_typology != "geography"
    )


def _prepare_duplicate_candidates(
    candidates: list[dict],
    old_entity_rows: list[dict] | None = None,
    excluded_references=None,
    selected_redirects=None,
    new_entity_rows: list[dict] | None = None,
    existing_entity_rows: list[dict] | None = None,
) -> list[dict]:
    """Prepare Dedup candidates for rendering and selection.

    Async's ``pipeline-summary.old-entity`` is the source of initial redirect
    preselection. Rows present in old-entity but absent from request
    ``selected_redirects`` are inferred to be async auto-selected and are locked
    in the UI so users cannot untick redirects generated by async policy.
    """
    preselected_redirects = {
        key
        for key in (
            _old_entity_redirect_key(row)
            for row in (old_entity_rows or [])
            if isinstance(row, dict)
        )
        if all(key)
    }
    selected_redirect_statuses = {
        _selected_redirect_key(redirect): _selected_redirect_status(redirect)
        for redirect in (selected_redirects or [])
        if isinstance(redirect, dict) and all(_selected_redirect_key(redirect))
    }
    selected_retirement_old_entities = {
        str(
            redirect.get("old_entity_number", "")
            or redirect.get("old_entity", "")
            or ""
        ).strip()
        for redirect in (selected_redirects or [])
        if isinstance(redirect, dict) and _selected_redirect_status(redirect) == "410"
    }
    selected_retirement_old_entities.discard("")
    rendered_retirement_old_entities = set()
    excluded_reference_set = _normalise_excluded_references(excluded_references)
    new_entity_ids = {
        str(row.get("entity", "") or "").strip()
        for row in (new_entity_rows or [])
        if isinstance(row, dict) and str(row.get("entity", "") or "").strip()
    }
    existing_entity_ids = {
        str(row.get("entity", "") or "").strip()
        for row in (existing_entity_rows or [])
        if isinstance(row, dict) and str(row.get("entity", "") or "").strip()
    }
    target_classification_available = bool(new_entity_ids or existing_entity_ids)
    prepared_candidates = []
    for candidate in candidates:
        candidate_redirect_key = _dedup_candidate_redirect_key(candidate)
        selected_redirect_key = _dedup_candidate_selected_redirect_key(candidate)
        target_entity = str(candidate.get("entity", "") or "").strip()
        target_requires_assignment = target_entity in new_entity_ids
        target_is_known = (
            target_requires_assignment or target_entity in existing_entity_ids
        )
        auto_select = candidate_redirect_key in preselected_redirects
        old_entity = str(candidate.get("old_entity", "") or "").strip()
        retirement_selected = (
            old_entity in selected_retirement_old_entities
            and old_entity not in rendered_retirement_old_entities
        )
        if retirement_selected:
            rendered_retirement_old_entities.add(old_entity)
        manually_selected = (
            selected_redirect_key in selected_redirect_statuses or retirement_selected
        )
        redirect_selected = auto_select or manually_selected
        redirect_status = (
            "410"
            if retirement_selected
            else selected_redirect_statuses.get(
                selected_redirect_key, "301" if auto_select else ""
            )
        )
        old_fields, new_fields = _dedup_candidate_field_maps(candidate)
        prepared_candidates.append(
            {
                **candidate,
                "auto_select": auto_select,
                "redirect_selected": redirect_selected,
                "redirect_locked": auto_select and not manually_selected,
                "redirect_can_select": redirect_status == "410"
                or (
                    _dedup_candidate_selected_entity_reference(candidate)
                    not in excluded_reference_set
                    and (target_is_known or not target_classification_available)
                ),
                "target_requires_assignment": target_requires_assignment,
                "redirect_status": redirect_status,
                "old_fields": old_fields,
                "new_fields": new_fields,
                "form_value": _dedup_candidate_form_value(candidate, redirect_status),
            }
        )
    return prepared_candidates


def _count_categories(rows: list) -> dict:
    counts = {"new": 0, "changed": 0, "in_both": 0, "existing": 0}
    for row in rows:
        category = row.get("category")
        if category in counts:
            counts[category] += 1
    return counts


def _date_only(date_str: str) -> str:
    """Truncate an ISO datetime (``YYYY-MM-DDThh:mm:ssZ``) to ``YYYY-MM-DD``."""
    return date_str[:10] if date_str else ""


def _resolve_existing_endpoints(
    source_summary: dict, current_endpoint_url: str = ""
) -> list:
    existing_endpoints = (
        source_summary.get("existing_endpoint_for_organisation_dataset") or []
    )
    if isinstance(existing_endpoints, str):
        existing_endpoints = [existing_endpoints] if existing_endpoints else []
    # The same endpoint hash can appear on more than one source.csv row for an
    # org/dataset; retiring matches on the hash so it actions every row at once.
    # Collapse duplicates so the retire table shows one row per endpoint.
    existing_endpoints = list(dict.fromkeys(existing_endpoints))
    if not existing_endpoints:
        return existing_endpoints

    endpoint_data = get_endpoint_info_for_hashes(existing_endpoints)
    log_data = get_endpoint_log_summary_for_hashes(existing_endpoints)
    # The endpoint hash is sha256(endpoint_url), so this matches the endpoint
    # being added when it is already present in the endpoint CSV.
    current_hash = compute_hash(current_endpoint_url) if current_endpoint_url else None

    existing_endpoints = [
        {
            "endpoint": h,
            "endpoint-url": endpoint_data.get(h, {}).get("endpoint_url", ""),
            "entry-date": _date_only(endpoint_data.get(h, {}).get("entry_date", "")),
            "end-date": _date_only(endpoint_data.get(h, {}).get("end_date", "")),
            "latest-status": log_data.get(h, {}).get("latest_status", ""),
            "latest-log-entry-date": _date_only(
                log_data.get(h, {}).get("latest_log_entry_date", "")
            ),
            "is_retired": bool(endpoint_data.get(h, {}).get("end_date", "")),
            "is_current": h == current_hash,
        }
        for h in existing_endpoints
    ]
    existing_endpoints.sort(key=lambda e: e["entry-date"] or "", reverse=True)
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


def _paginate_entity_data(
    all_resp_details: list,
    platform_entities: list,
    entity_page: int,
    entity_search: str,
    entity_filter: str = "",
    include_selection: bool = False,
    excluded_references=None,
) -> tuple:
    entity_start_offset = (entity_page - 1) * _ROWS_PER_PAGE
    entities_data_full = _build_entities_data(all_resp_details, platform_entities)

    # Counts cover every entity, independent of the current search/filter, so the
    # summary boxes always show the full picture.
    category_counts = _count_categories(entities_data_full["rows"])
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
    if include_selection:
        entities_data = _add_assign_entities_selection_metadata(
            entities_data,
            excluded_references,
        )
    return (
        entities_data,
        has_next_entity_page,
        entity_page_start,
        entity_page_end,
        category_counts,
    )


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


def _representative_point(shapely_geom, point_wkt=None) -> list:
    """Return [lon, lat] for a marker. Prefers an explicit POINT wkt; otherwise
    uses representative_point() which is guaranteed to sit inside the shape."""
    if point_wkt:
        try:
            p = wkt.loads(point_wkt)
            return [p.x, p.y]
        except Exception:
            pass
    p = shapely_geom.representative_point()
    return [p.x, p.y]


def _point_feature(shapely_geom, point_wkt, properties: dict) -> dict:
    props = dict(properties)
    props["has_polygon"] = shapely_geom.geom_type in ("Polygon", "MultiPolygon")
    return {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": _representative_point(shapely_geom, point_wkt),
        },
        "properties": props,
    }


def _build_geometry_features(
    platform_entities: list, all_resp_details: list, dataset_id: str
) -> tuple:
    """Return (polygon_features, point_features).

    polygon_features keep the original geometry for the boundary layers;
    point_features carry one representative point per entity for clustering.
    """
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
    points = []

    for entity in platform_entities:
        entity_id = _normalise_entity_id(str(entity.get("entity", "")))
        if entity_id in resource_entity_ids:
            continue
        geom_wkt = entity.get("geometry") or entity.get("point")
        if not geom_wkt:
            continue
        try:
            shp = wkt.loads(geom_wkt)
            properties = {
                "entity": entity_id,
                "reference": entity.get("reference", ""),
                "name": entity.get("name", ""),
                "status": "existing",
            }
            features.append(
                {"type": "Feature", "geometry": mapping(shp), "properties": properties}
            )
            points.append(_point_feature(shp, entity.get("point"), properties))
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
        geom_fact = next(
            (
                f
                for f in transformed_row
                if isinstance(f, dict) and f.get("field") == "geometry"
            ),
            None,
        )
        point_fact = next(
            (
                f
                for f in transformed_row
                if isinstance(f, dict) and f.get("field") == "point"
            ),
            None,
        )
        shape_wkt = (geom_fact or {}).get("value") or (point_fact or {}).get("value")
        if not shape_wkt:
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
            shp = wkt.loads(shape_wkt)
            properties = {
                "entity": entity_id,
                "reference": (
                    converted_row.get("reference")
                    or converted_row.get("Reference")
                    or f"Entry {item.get('entry_number')}"
                ),
                "name": converted_row.get("name", ""),
                "status": status,
            }
            features.append(
                {"type": "Feature", "geometry": mapping(shp), "properties": properties}
            )
            points.append(
                _point_feature(shp, (point_fact or {}).get("value"), properties)
            )
        except Exception as e:
            logger.warning(
                "Error parsing geometry for resource entry %s: %s",
                item.get("entry_number"),
                e,
            )

    return features, points


def fetch_boundary_geojson(organisation_code: str) -> dict:
    """Fetch the LPA boundary GeoJSON for an organisation.

    Shared by the transform and check-results pages. Every upstream call is
    given an explicit timeout so an unresponsive service can't block the request
    thread; any failure falls back to an empty FeatureCollection with a warning.
    """
    empty = {"type": "FeatureCollection", "features": []}
    try:
        if ":" not in organisation_code:
            return empty
        lpa_prefix, lpa_id = organisation_code.split(":", 1)
        resp = requests.get(
            _entity_search_url(lpa_prefix, lpa_id), timeout=REQUESTS_TIMEOUT
        )
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
        return requests.get(
            _entity_geojson_url(reference), timeout=REQUESTS_TIMEOUT
        ).json()
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
    excluded_references = params.get("excluded_references")
    dataset_id = params.get("dataset", "")
    is_assign_entities = transform_endpoint == "assign_entities.flagged_resource_detail"
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
    existing_endpoints = _resolve_existing_endpoints(source_summary, endpoint_url)
    pipelines_append_required = source_summary.get("pipelines_append_required")
    pipeline_summary = response_data.get("pipeline-summary") or {}
    dataset_typology = get_dataset_typology(dataset_id)
    show_dedup_tab = _show_dedup_tab(is_assign_entities, dataset_id, dataset_typology)
    raw_duplicate_candidates = pipeline_summary.get("duplicate-candidates") or []
    old_entity_rows = pipeline_summary.get("old-entity") or []
    selected_redirects = params.get("selected_redirects") or []
    logger.info(
        "Dedup visibility decision: worker_pid=%s request_id=%s request_path=%s "
        "flask_endpoint=%s transform_endpoint=%s dataset_id=%r "
        "dataset_typology=%r is_assign_entities=%s show_dedup_tab=%s "
        "raw_duplicate_candidate_count=%s old_entity_count=%s "
        "selected_redirect_count=%s",
        os.getpid(),
        request_id,
        flask_request.path,
        flask_request.endpoint,
        transform_endpoint,
        dataset_id,
        dataset_typology,
        is_assign_entities,
        show_dedup_tab,
        len(raw_duplicate_candidates),
        len(old_entity_rows),
        len(selected_redirects),
    )
    duplicate_candidates = _prepare_duplicate_candidates(
        raw_duplicate_candidates if show_dedup_tab else [],
        old_entity_rows,
        excluded_references=excluded_references,
        selected_redirects=selected_redirects,
        new_entity_rows=pipeline_summary.get("new-entities") or [],
        existing_entity_rows=pipeline_summary.get("existing-entities") or [],
    )
    duplicate_columns = _dedup_dynamic_columns(duplicate_candidates)

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
    (
        entities_data,
        has_next_entity_page,
        entity_page_start,
        entity_page_end,
        category_counts,
    ) = _paginate_entity_data(
        all_resp_details,
        platform_entities,
        entity_page,
        entity_search,
        entity_filter,
        include_selection=is_assign_entities,
        excluded_references=excluded_references,
    )
    transformed_table = _build_transform_table(resp_details)
    issue_log_table = _build_issue_log_table(resp_details)

    # Build a GeoJSON feature collection for the map if needed, including any platform entities not in the resource,
    # and any new or updated resource entities with geometry.
    if dataset_typology == "geography":
        geometries, geometry_points = _build_geometry_features(
            platform_entities, all_resp_details, dataset_id
        )
        boundary_geojson = (
            fetch_boundary_geojson(organisation_code) if geometries else None
        )
    else:
        geometries = []
        geometry_points = []
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
        category_counts=category_counts,
        has_next_entity_page=has_next_entity_page,
        entity_page_start=entity_page_start,
        entity_page_end=entity_page_end,
        endpoint_in_doc=endpoint_in_doc,
        doc_is_gov_uk=doc_is_gov_uk,
        endpoint_is_gov_uk=endpoint_is_gov_uk,
        endpoint_url=endpoint_url,
        documentation_url=documentation_url,
        resource_hash=resource_hash,
        is_assign_entities=is_assign_entities,
        show_dedup_tab=show_dedup_tab,
        duplicate_candidates=duplicate_candidates,
        duplicate_columns=duplicate_columns,
        planning_entity_base_url=(
            current_app.config.get(
                "PLANNING_BASE_URL", "https://www.planning.data.gov.uk"
            ).rstrip("/")
            + "/entity"
        ),
        geometries=geometries,
        geometry_points=geometry_points,
        boundary_geojson=boundary_geojson,
        flagged_errors=flagged_errors or [],
        flagged_error_abbreviations=flagged_error_abbreviations or [],
        flagged_error_messages=flagged_error_messages or [],
    )
