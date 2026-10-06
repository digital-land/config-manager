import json
import logging
import tempfile
import uuid
import zipfile
import zlib
from datetime import datetime
from fnmatch import fnmatch
from io import BytesIO, StringIO
from pathlib import Path

import pandas as pd
from flask import current_app, redirect, render_template, request, session, url_for

from application.data_access.overview.digital_land_queries import get_resource

from . import ControllerError
from .request_meta import record_branch_baseline, record_source_flow
from .transform import handle_check_transform
from ..services.async_api import AsyncAPIError, fetch_request, submit_request
from ..services.dataset import get_collection_id, get_dataset_id, get_dataset_name
from ..services.assign_entity_resources import (
    IN_PROGRESS,
    get_assign_entity_resource_statuses,
    set_assign_entity_resource_status,
)
from ..services.github import (
    GitHubAppError,
    GitHubArtifactError,
    MAX_ARTIFACT_ARCHIVE_BYTES,
    download_batch_assign_artifact,
    get_latest_batch_assign_artifacts,
)

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
        "description": ("Resource contain entities with missing organisation value"),
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
    previous_cache_key = session.get("flagged_resource_cache_key")
    if previous_cache_key:
        previous_cache_path = _CACHE_DIR / f"{previous_cache_key}.json"
        if previous_cache_path.exists():
            previous_cache_path.unlink()

    cache_key = uuid.uuid4().hex
    cache_path = _CACHE_DIR / f"{cache_key}.json"
    cache_path.write_text(json.dumps(_serialise_rows(df)), encoding="utf-8")
    session["flagged_resource_cache_key"] = cache_key


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
                    "message": _error_description(error_code, row.get("message", "")),
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
    row_order = {"entity_growth": 0, "yellow": 1, "red": 2}.get(resource["row_type"], 3)
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
            row_type = "yellow"
            row_colour = "yellow"
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


def _submit_assign_entities_request(
    dataset_input,
    resource,
    organisation=None,
    return_endpoint=None,
    excluded_references=None,
    selected_redirects=None,
):
    dataset_id, collection_id = _resolve_dataset_and_collection(dataset_input)
    organisation = organisation or _get_resource_organisation(resource, dataset_id)
    params = {
        "type": "add_data",
        "resource": resource,
        "dataset": dataset_id,
        "collection": collection_id,
        "authoritative": True,
        "github_branch": current_app.config.get("CONFIG_REPO_BRANCH") or None,
    }
    if organisation:
        params["organisationName"] = organisation
        params["organisation"] = organisation
    if return_endpoint:
        params["return_endpoint"] = return_endpoint
    if excluded_references is not None:
        params["excluded_references"] = excluded_references
    if selected_redirects is not None:
        params["selected_redirects"] = selected_redirects
    preview_id = submit_request(params)
    record_source_flow(preview_id, "assign_entities")
    record_branch_baseline(preview_id, params["github_branch"])
    return preview_id


def _artifact_page_context():
    artifacts = []
    artifact_lookup_failed = False
    try:
        artifacts = get_latest_batch_assign_artifacts()
    except GitHubAppError:
        artifact_lookup_failed = True
        logger.exception("Unable to load batch assign artifacts")

    github_org = current_app.config.get("GITHUB_ORG", "digital-land")
    display_artifacts = []
    for artifact in artifacts:
        csv_members = []
        if artifact.get("name") == "batch-assign-single-source-output":
            size_in_bytes = artifact.get("size_in_bytes")
            if not (
                isinstance(size_in_bytes, (int, float))
                and size_in_bytes >= MAX_ARTIFACT_ARCHIVE_BYTES
            ):
                try:
                    archive_bytes = download_batch_assign_artifact(artifact["id"])
                    csv_members = _list_artifact_csv_members(archive_bytes)
                except (GitHubArtifactError, ValueError):
                    logger.exception(
                        "Unable to list CSV files in artifact %s", artifact["id"]
                    )

        workflow_run = artifact.get("workflow_run") or {}
        run_id = workflow_run.get("id")
        created_at = artifact.get("created_at") or ""
        try:
            created_at_display = datetime.fromisoformat(
                created_at.replace("Z", "+00:00")
            ).strftime("%-d %B %Y at %H:%M UTC")
        except ValueError:
            created_at_display = created_at

        size_in_bytes = artifact.get("size_in_bytes")
        size_display = _format_artifact_size(size_in_bytes)

        display_artifact = {
            **artifact,
            "created_at_display": created_at_display,
            "size_display": size_display,
            "is_too_large": (
                isinstance(size_in_bytes, (int, float))
                and size_in_bytes >= MAX_ARTIFACT_ARCHIVE_BYTES
            ),
            "workflow_run_url": (
                f"https://github.com/{github_org}/config/actions/runs/{run_id}"
                if run_id
                else None
            ),
        }
        if csv_members:
            display_artifacts.extend(
                {
                    **display_artifact,
                    "artifact_name": artifact["name"],
                    "name": member.rsplit("/", 1)[-1],
                    "csv_member": member,
                    "size_display": _format_artifact_size(member_size),
                    "is_too_large": member_size >= MAX_ARTIFACT_ARCHIVE_BYTES,
                }
                for member, member_size in csv_members
            )
        else:
            display_artifacts.append(display_artifact)

    show_artifact_size = any(
        artifact["size_display"] is not None for artifact in display_artifacts
    )
    return display_artifacts, artifact_lookup_failed, show_artifact_size


def _list_artifact_csv_members(archive_bytes):
    """Return CSV member names from a batch-assign artifact ZIP."""
    try:
        archive = zipfile.ZipFile(BytesIO(archive_bytes))
    except zipfile.BadZipFile as e:
        raise ValueError("The GitHub artifact is not a valid ZIP file.") from e

    try:
        return [
            (member.filename, member.file_size)
            for member in archive.infolist()
            if not member.is_dir() and member.filename.lower().endswith(".csv")
        ]
    except (zipfile.BadZipFile, EOFError, RuntimeError, zlib.error) as e:
        raise ValueError("The GitHub artifact ZIP could not be read.") from e
    finally:
        archive.close()


def _format_artifact_size(size_in_bytes):
    if not isinstance(size_in_bytes, (int, float)) or size_in_bytes < 0:
        return None

    size = float(size_in_bytes)
    units = ("bytes", "KB", "MB", "GB")
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return (
                f"{int(size)} {unit}"
                if unit == "bytes"
                else f"{size:.1f} {unit}"
            )
        size /= 1024


def _read_artifact_csv(archive_bytes, csv_member=None):
    """Read the batch-assign summary CSV from an artifact ZIP."""
    try:
        archive = zipfile.ZipFile(BytesIO(archive_bytes))
    except zipfile.BadZipFile as e:
        raise ValueError("The GitHub artifact is not a valid ZIP file.") from e

    try:
        matching_files = [
            member
            for member in archive.infolist()
            if not member.is_dir()
            and (
                member.filename == csv_member
                if csv_member
                else fnmatch(
                    member.filename.rsplit("/", 1)[-1].lower(),
                    "batch_assign_summary*.csv",
                )
            )
        ]
        if not matching_files:
            if csv_member:
                raise ValueError("The selected CSV file is not in the artifact.")
            raise ValueError("The artifact does not contain a batch_assign_summary CSV file.")
        if len(matching_files) > 1:
            if csv_member:
                raise ValueError("The selected CSV file appears more than once in the artifact.")
            raise ValueError("The artifact contains more than one batch_assign_summary CSV file.")

        member = matching_files[0]
        if member.file_size >= MAX_ARTIFACT_ARCHIVE_BYTES:
            raise ValueError("The CSV file in the artifact must be smaller than 20 MB. Please upload it manually")
        return _read_csv_upload(BytesIO(archive.read(member)))
    except (zipfile.BadZipFile, EOFError, RuntimeError, zlib.error) as e:
        raise ValueError("The GitHub artifact ZIP could not be read.") from e
    finally:
        archive.close()


def _render_flagged_resources_start(errors=None, form=None, artifact_error=None):
    artifacts, artifact_lookup_failed, show_artifact_size = _artifact_page_context()
    return render_template(
        "datamanager/flagged-resources-start.html",
        errors=errors or {},
        form=form or {"dataset": "", "resource": ""},
        artifacts=artifacts,
        artifact_lookup_failed=artifact_lookup_failed,
        show_artifact_size=show_artifact_size,
        artifact_error=artifact_error,
    )


def handle_flagged_resources_start(artifact_error=None):
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
                        form["dataset"],
                        form["resource"],
                        return_endpoint="assign_entities.flagged_resources_start",
                    )
                except AsyncAPIError as e:
                    raise ControllerError(
                        f"Assign entities submission failed: {e.detail}"
                    ) from e
                return redirect(
                    url_for(
                        "assign_entities.flagged_resource_detail", request_id=request_id
                    )
                )
        else:
            errors["form"] = "Enter a dataset and resource"

    return _render_flagged_resources_start(errors, form, artifact_error)


def handle_flagged_artifact_assign(artifact_id):
    try:
        archive_bytes = download_batch_assign_artifact(artifact_id)
        df = _read_artifact_csv(archive_bytes, request.form.get("csv_member"))
    except (GitHubArtifactError, ValueError) as e:
        return _render_flagged_resources_start(artifact_error=str(e))

    _store_rows(df)
    return redirect(url_for("assign_entities.flagged_resources_summary"))


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
    statuses = get_assign_entity_resource_statuses(
        [
            (resource["resource"], resource["dataset"], resource["organisation"])
            for resource in resources
        ]
    )
    for resource in resources:
        resource["processing"] = statuses.get(
            (resource["resource"], resource["dataset"], resource["organisation"])
        )
    error_category_counts = {
        row_type: sum(resource["row_type"] == row_type for resource in resources)
        for row_type in ("entity_growth", "yellow", "red")
    }
    return render_template(
        "datamanager/flagged-resources-summary.html",
        resources=resources,
        error_key=_build_error_key(resources),
        error_category_counts=error_category_counts,
    )


def handle_flagged_resource_submit():
    dataset = request.form.get("dataset", "").strip()
    resource = request.form.get("resource", "").strip()
    organisation = request.form.get("organisation", "").strip() or None
    errors = [
        error for error in request.form.get("errors", "").strip().split(",") if error
    ]
    if not dataset or not resource:
        raise ControllerError("Dataset and resource are required")

    try:
        request_id = _submit_assign_entities_request(
            dataset,
            resource,
            organisation,
            return_endpoint="assign_entities.flagged_resources_summary",
        )
    except AsyncAPIError as e:
        raise ControllerError(f"Assign entities submission failed: {e.detail}") from e

    actor_username = (session.get("user") or {}).get("login", "unknown")
    try:
        set_assign_entity_resource_status(
            resource, dataset, organisation, IN_PROGRESS, actor_username
        )
    except Exception:
        logger.exception("Could not record Assign Entities status for %s", resource)

    return redirect(
        url_for(
            "assign_entities.flagged_resource_detail",
            request_id=request_id,
            errors=",".join(errors),
        )
    )


def handle_flagged_resource_detail(request_id):
    req = fetch_request(request_id)
    flagged_errors = [
        error for error in request.args.get("errors", "").strip().split(",") if error
    ]
    flagged_error_messages = [_error_description(error, "") for error in flagged_errors]
    return handle_check_transform(
        request_id,
        req,
        transform_endpoint="assign_entities.flagged_resource_detail",
        template_name="datamanager/assign-entities-check-results.html",
        flagged_errors=flagged_errors,
        flagged_error_messages=flagged_error_messages,
    )
