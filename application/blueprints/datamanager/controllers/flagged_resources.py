import json
import logging
import tempfile
import uuid
from io import BytesIO, StringIO
from pathlib import Path

import pandas as pd
from flask import redirect, render_template, request, session, url_for

from application.data_access.overview.digital_land_queries import get_resource

from . import ControllerError
from .transform import handle_check_transform
from ..services.async_api import AsyncAPIError, fetch_request, submit_request
from ..services.dataset import get_collection_id, get_dataset_id, get_dataset_name


REQUIRED_COLUMNS = [
    "dataset",
    "resource",
    "organisation",
    "reference",
    "status",
    "entities_created",
    "error_code",
    "message",
]

_CACHE_DIR = Path(tempfile.gettempdir()) / "config-manager-flagged-resources"
logger = logging.getLogger(__name__)

ERROR_ABBREVIATIONS = {
    "current_resource_empty": {
        "abbreviation": "CRE",
        "description": "Resource empty",
    },
    "current_resource_no_new_entities": {
        "abbreviation": "NNE",
        "description": "No new entities",
    },
    "duplicate_entity_all_fields": {
        "abbreviation": "DEAF",
        "description": "Resource contains duplicates with existing entities (all fields)",
    },
    "duplicate_reference_organisation_in_new_resource": {
        "abbreviation": "DRON",
        "description": (
            "Resource contains duplicate entities (organisation and reference)"
        ),
    },
    "duplicate_reference_organisation": {
        "abbreviation": "DRO",
        "description": (
            "Resource contains duplicates with existing entities "
            "(Reference and organisation only)"
        ),
    },
    "missing_organisation": {
        "abbreviation": "MO",
        "description": (
            "Resource contain entities with missing organisation value"
        ),
    },
    "missing_reference": {
        "abbreviation": "MR",
        "description": "Resource contain entities with missing reference values",
    },
    "invalid_uri_issue": {
        "abbreviation": "IUI",
        "description": (
            "Resource has known issues with invalid URIs that require manual review."
        ),
    },
    "large_number_of_new_entities": {
        "abbreviation": "EG",
        "description": "Entity growth is above threshold",
    },
    "previous_resource_not_found": {
        "abbreviation": "PRNF",
        "description": "Previous resource not found",
    },
    "previous_resource_empty": {
        "abbreviation": "PRE",
        "description": "Previous resource is empty",
    },
}

ERROR_SORT_ORDER = {
    "EG": 0,
    "CRE": 1,
    "NNE": 2,
    "DEAF": 3,
    "DRON": 4,
    "DRO": 5,
    "MO": 6,
    "MR": 7,
    "IUI": 8,
    "PRE": 9,
    "PRNF": 10,
}


def _normalise_frame(df):
    df = df.fillna("")
    for column in REQUIRED_COLUMNS:
        df[column] = df[column].astype(str).str.strip()
    return df


def _validate_error_codes(df):
    error_rows = df["status"].str.lower().eq("error")
    missing_codes = error_rows & df["error_code"].eq("")
    if missing_codes.any():
        raise ValueError("Rows with status 'error' must include an error_code")


def _read_csv_upload(uploaded_file):
    contents = uploaded_file.read()
    if not contents:
        raise ValueError("Upload a CSV file")

    df = pd.read_csv(BytesIO(contents), dtype=str, keep_default_na=False)
    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    df = _normalise_frame(df[REQUIRED_COLUMNS])
    if df.empty:
        raise ValueError("No data found in CSV")
    _validate_error_codes(df)
    return df


def _read_csv_text(csv_data):
    csv_data = (csv_data or "").strip()
    if not csv_data:
        raise ValueError("Enter CSV data")

    df = pd.read_csv(StringIO(csv_data), dtype=str, keep_default_na=False)
    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    df = _normalise_frame(df[REQUIRED_COLUMNS])
    if df.empty:
        raise ValueError("No data found in CSV")
    _validate_error_codes(df)
    return df


def _serialise_rows(df):
    return df.to_dict(orient="records")


def _store_rows(df):
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_key = uuid.uuid4().hex
    cache_path = _CACHE_DIR / f"{cache_key}.json"
    cache_path.write_text(json.dumps(_serialise_rows(df)), encoding="utf-8")
    session["flagged_resource_cache_key"] = cache_key
    session.pop("flagged_resource_rows", None)


def _frame_from_session():
    cache_key = session.get("flagged_resource_cache_key")
    rows = []
    if cache_key:
        cache_path = _CACHE_DIR / f"{cache_key}.json"
        if cache_path.exists():
            try:
                rows = json.loads(cache_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                rows = []
    if not rows:
        rows = session.get("flagged_resource_rows") or []
    if not rows:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)
    return _normalise_frame(pd.DataFrame(rows, columns=REQUIRED_COLUMNS))


def _has_error(row):
    return str(row.get("status", "")).strip().lower() == "error"


def _error_abbreviation(error_code):
    mapped_error = ERROR_ABBREVIATIONS.get(str(error_code).strip().lower())
    if mapped_error:
        return mapped_error["abbreviation"]
    return error_code.upper()


def _error_description(error_code, message):
    mapped_error = ERROR_ABBREVIATIONS.get(str(error_code).strip().lower())
    if mapped_error:
        return mapped_error["description"]
    return message


def _summarise_errors(rows):
    errors = []
    seen = set()
    for row in rows:
        if not _has_error(row):
            continue

        error_code = row.get("error_code", "")
        if error_code and error_code not in seen:
            errors.append(
                {
                    "code": error_code,
                    "abbreviation": _error_abbreviation(error_code),
                    "message": _error_description(
                        error_code, row.get("message", "")
                    ),
                }
            )
            seen.add(error_code)

    return errors


def _build_error_key(resources):
    error_key = []
    seen = set()
    for resource in resources:
        for error in resource["errors"]:
            key = error["abbreviation"]
            if key in seen:
                continue
            error_key.append(error)
            seen.add(key)
    return sorted(
        error_key,
        key=lambda error: (
            ERROR_SORT_ORDER.get(error["abbreviation"], 99),
            error["abbreviation"],
        ),
    )


def _resource_error_sort_key(resource):
    error_order = min(
        (
            ERROR_SORT_ORDER.get(error["abbreviation"], 99)
            for error in resource["errors"]
        ),
        default=99,
    )
    row_order = {"entity_growth": 0, "orange": 1, "red": 2}.get(
        resource["row_type"], 3
    )
    return (
        row_order,
        error_order,
        resource["dataset"],
        resource["organisation"],
        resource["resource"],
    )


def _group_resources(df):
    if df.empty:
        return []

    resources = []
    grouped = df.groupby(["resource", "dataset", "organisation"], dropna=False)
    for (resource, dataset, organisation), group in grouped:
        rows = group.to_dict(orient="records")
        errors = _summarise_errors(rows)
        if not any(_has_error(row) for row in rows):
            continue

        error_abbreviations = {error["abbreviation"] for error in errors}
        if error_abbreviations == {"EG"}:
            row_type = "entity_growth"
            row_colour = ""
        elif "EG" in error_abbreviations:
            row_type = "orange"
            row_colour = "orange"
        else:
            row_type = "red"
            row_colour = "red"

        resources.append(
            {
                "resource": resource,
                "dataset": dataset,
                "organisation": organisation,
                "errors": errors,
                "row_type": row_type,
                "row_colour": row_colour,
                "is_entity_growth_only": row_type == "entity_growth",
                "rows": len(rows),
            }
        )

    return sorted(resources, key=_resource_error_sort_key)


def _resolve_dataset_and_collection(dataset_input):
    dataset_id = get_dataset_id(dataset_input) or dataset_input
    dataset_name = get_dataset_name(dataset_id, default=dataset_input)
    collection_id = (
        get_collection_id(dataset_input)
        or get_collection_id(dataset_name)
        or dataset_id
    )
    return dataset_id, collection_id


def _organisation_from_cached_rows(resource, dataset_id):
    df = _frame_from_session()
    if df.empty:
        return None

    matches = df[df["resource"] == resource]
    if dataset_id:
        dataset_matches = matches[matches["dataset"] == dataset_id]
        if not dataset_matches.empty:
            matches = dataset_matches

    if matches.empty:
        return None

    organisation = matches.iloc[0].get("organisation", "")
    return organisation or None


def _get_resource_organisation(resource, dataset_id):
    try:
        resource_rows = get_resource(resource) or []
    except Exception as e:
        logger.warning("Could not fetch resource details for %s: %s", resource, e)
        resource_rows = []

    matching_rows = resource_rows
    if dataset_id:
        matching_rows = [
            row
            for row in resource_rows
            if dataset_id in (row.get("pipeline", "") or "").split(";")
        ] or resource_rows

    for row in matching_rows:
        organisation = row.get("organisation")
        if organisation:
            return organisation

    return _organisation_from_cached_rows(resource, dataset_id)


def _submit_assign_entities_request(dataset_input, resource):
    dataset_id, collection_id = _resolve_dataset_and_collection(dataset_input)
    organisation = _get_resource_organisation(resource, dataset_id)
    params = {
        "type": 'add_data',
        "resource": resource,
        "dataset": dataset_id,
        "collection": collection_id,
        "authoritative": True
    }
    if organisation:
        params["organisationName"] = organisation
        params["organisation"] = organisation
    return submit_request(params)


def handle_flagged_resources_start():
    errors = {}
    form = {
        "dataset": request.form.get("dataset", "").strip(),
        "resource": request.form.get("resource", "").strip(),
    }

    if request.method == "POST":
        has_direct_input = bool(form["dataset"] or form["resource"])

        if has_direct_input:
            if not form["dataset"]:
                errors["dataset"] = "Enter a dataset"
            if not form["resource"]:
                errors["resource"] = "Enter a resource"
            if not errors:
                try:
                    request_id = _submit_assign_entities_request(
                        form["dataset"], form["resource"]
                    )
                except AsyncAPIError as e:
                    raise Exception(
                        f"Assign entities submission failed: {e.detail}"
                    ) from e
                return redirect(
                    url_for("assign_entities.flagged_resource_detail", request_id=request_id)
                )
        else:
            errors["form"] = "Enter a dataset and resource"

    return render_template(
        "datamanager/flagged-resources-start.html",
        errors=errors,
        form=form,
    )


def handle_flagged_resources_import():
    errors = {}
    csv_data = request.form.get("csv_data", "").strip()

    if request.method == "POST":
        uploaded_file = request.files.get("csv_file")
        has_upload = bool(uploaded_file and uploaded_file.filename)

        try:
            df = (
                _read_csv_upload(uploaded_file)
                if has_upload
                else _read_csv_text(csv_data)
            )
        except ValueError as e:
            errors["csv_data"] = str(e)
        else:
            _store_rows(df)
            return redirect(url_for("assign_entities.flagged_resources_summary"))

    return render_template(
        "datamanager/flagged-resources-import.html",
        csv_data=csv_data,
        errors=errors,
        required_columns=REQUIRED_COLUMNS,
    )


def handle_flagged_resources_summary():
    df = _frame_from_session()
    if df.empty:
        return redirect(url_for("assign_entities.flagged_resources_start"))

    resources = _group_resources(df)
    return render_template(
        "datamanager/flagged-resources-summary.html",
        resources=resources,
        error_key=_build_error_key(resources),
        resource_count=len(resources),
    )


def handle_flagged_resource_submit():
    dataset = request.args.get("dataset", "").strip()
    resource = request.args.get("resource", "").strip()
    if not dataset or not resource:
        raise ControllerError("Dataset and resource are required")

    try:
        request_id = _submit_assign_entities_request(dataset, resource)
    except AsyncAPIError as e:
        raise Exception(f"Assign entities submission failed: {e.detail}") from e

    return redirect(
        url_for("assign_entities.flagged_resource_detail", request_id=request_id)
    )


def handle_flagged_resource_detail(request_id):
    req = fetch_request(request_id)
    return handle_check_transform(
        request_id,
        req,
        transform_endpoint="assign_entities.flagged_resource_detail",
    )
