import json
import re
import traceback
from datetime import datetime

import requests
from flask import Blueprint, abort, jsonify, redirect, render_template, request, url_for, session

from application.utils import get_request_api_endpoint
from shapely import wkt
from shapely.geometry import mapping
import csv
from io import StringIO

datamanager_bp = Blueprint("datamanager", __name__, url_prefix="/datamanager")
headers = {"Content-Type": "application/json", "Accept": "application/json"}

REQUESTS_TIMEOUT = 20  # seconds

def normalize_org_curie(org: str, dataset_id: str | None = None) -> str:
    """
    Convert 'local-authority:CODE' -> 'local-authority-eng:CODE'.
    Leaves anything else unchanged. Safe to call multiple times.
    """
    if not org:
        return org
    org = org.strip()
    if org.startswith("local-authority:"):
        return org.replace("local-authority:", "local-authority-eng:", 1)
    return org

# put this near the top, replacing your current get_spec_fields_from_datasette
def get_spec_fields_union(dataset_id: str | None) -> list[str]:
    """
    Return the union of:
      - global field list (all datasets)
      - dataset-scoped field list (if dataset_id is provided)
    Keep original casing; de-duplicate exact strings; stable order.
    """
    base = "https://datasette.planning.data.gov.uk/digital-land/dataset_field.json"
    headers = {"Accept": "application/json"}

    def _fetch(url: str) -> list[str]:
        try:
            r = requests.get(url, timeout=REQUESTS_TIMEOUT, headers=headers)
            r.raise_for_status()
            rows = r.json() or []
            return [(row.get("field") or "").strip() for row in rows if row.get("field")]
        except Exception as e:
            print(f"⚠️ dataset_field fetch failed: {e} for {url}")
            return []

    # global list (all datasets)
    global_fields = _fetch(f"{base}?_shape=array")

    # dataset-filtered list (e.g. camel-case BFL items)
    dataset_fields = _fetch(f"{base}?_shape=array&dataset={dataset_id}") if dataset_id else []

    # union, preserve first-seen order, exact-string dedupe
    seen, out = set(), []
    for f in global_fields + dataset_fields:
        if f and f not in seen:
            seen.add(f)
            out.append(f)

    # optional: sort by lowercase while preserving casing (comment out if you prefer raw order)
    out.sort(key=lambda x: x.lower())
    return out


def read_raw_csv_preview(source_url: str, max_rows: int = 50):
    """
    Fetch the CSV at source_url and return (headers, first N rows).
    """
    headers, rows = [], []
    if not source_url:
        return headers, rows
    try:
        resp = requests.get(source_url, timeout=REQUESTS_TIMEOUT)
        text = resp.content.decode("utf-8", errors="ignore")
        reader = csv.reader(StringIO(text))
        first = next(reader, None)
        if first is None:
            return headers, rows
        headers = [h.strip().lstrip("\ufeff") for h in first if h is not None]
        for i, r in enumerate(reader):
            if i >= max_rows:
                break
            vals = (r + [""] * len(headers))[: len(headers)]
            rows.append(vals)
    except Exception as e:
        print(f"⚠️ Could not fetch/parse CSV preview: {e}")
    return headers, rows


@datamanager_bp.context_processor
def inject_now():
    return {"now": datetime}


@datamanager_bp.route("/")
def index():
    return render_template("datamanager/index.html", datamanager={"name": "Dashboard"})


@datamanager_bp.route("/dashboard/add", methods=["GET", "POST"])
def dashboard_add():
    try:
        ds_response = requests.get(
            "https://www.planning.data.gov.uk/dataset.json?_labels=on&_size=max",
            timeout=REQUESTS_TIMEOUT,
        ).json()
    except Exception as e:
        print("Error fetching datasets:", e)
        abort(500, "Failed to fetch dataset list")

    # only datasets that have a collection
    datasets = [d for d in ds_response.get("datasets", []) if "collection" in d]
    dataset_options = sorted([d["name"] for d in datasets])

    # 🔧 build BOTH maps
    name_to_dataset_id = {d["name"]: d["dataset"] for d in datasets}
    name_to_collection_id = {d["name"]: d["collection"] for d in datasets}

    # autocomplete dataset options
    if request.args.get("autocomplete"):
        query = request.args["autocomplete"].lower()
        matches = [name for name in dataset_options if query in name.lower()]
        return jsonify(matches[:10])

    # fetch orgs for a dataset name (for UI suggestions)
    if request.args.get("get_orgs_for"):
        dataset_name = request.args["get_orgs_for"]
        dataset_id = name_to_dataset_id.get(dataset_name)
        if not dataset_id:
            return jsonify([])
        provision_url = f"https://datasette.planning.data.gov.uk/digital-land/provision.json?_labels=on&_size=max&dataset={dataset_id}"
        try:
            provision_rows = requests.get(provision_url, timeout=REQUESTS_TIMEOUT).json().get("rows", [])
        except Exception:
            provision_rows = []
        selected_orgs = [
            f"{row['organisation']['label']} ({row['organisation']['value'].split(':', 1)[1]})"
            for row in provision_rows
        ]
        return jsonify(selected_orgs)

    form = {}
    errors = {}
    selected_orgs = []
    dataset_input = ""
    dataset_id = None
    collection_id = None

    if request.method == "POST":
        form = request.form.to_dict()
        print("🔍 Received form POST data:")
        print(json.dumps(form, indent=2))

        mode = form.get("mode", "").strip()
        dataset_input = form.get("dataset", "").strip()
        dataset_id = name_to_dataset_id.get(dataset_input)
        collection_id = name_to_collection_id.get(dataset_input)

        column_mapping_str = form.get("column_mapping", "{}").strip()
        geom_type = form.get("geom_type", "").strip()
        try:
            column_mapping = json.loads(column_mapping_str) if column_mapping_str else {}
        except json.JSONDecodeError:
            errors["column_mapping"] = True
            column_mapping = {}

        # Preload org list + build a reverse map we'll use on submit
        org_label_to_value = {}
        if dataset_id:
            provision_url = f"https://datasette.planning.data.gov.uk/digital-land/provision.json?_labels=on&_size=max&dataset={dataset_id}"
            try:
                provision_rows = requests.get(provision_url, timeout=REQUESTS_TIMEOUT).json().get("rows", [])
            except Exception:
                provision_rows = []
            selected_orgs = [
                f"{row['organisation']['label']} ({row['organisation']['value'].split(':', 1)[1]})"
                for row in provision_rows
            ]
            # 🔑 map "Label (CODE)" -> "prefix:CODE"
            org_label_to_value = {
                f"{row['organisation']['label']} ({row['organisation']['value'].split(':', 1)[1]})":
                row['organisation']['value']
                for row in provision_rows
                if row.get("organisation", {}).get("value")
            }

        if mode == "final":
            # what the user submitted from the select/input
            org_input = (form.get("organisation") or "").strip()
            endpoint_url = form.get("endpoint_url", "").strip()
            doc_url = form.get("documentation_url", "").strip()
            licence = form.get("licence", "").strip()
            day = form.get("start_day", "").strip()
            month = form.get("start_month", "").strip()
            year = form.get("start_year", "").strip()
            org_warning = form.get("org_warning", "false") == "true"

            # resolve to real entity ref (prefix:CODE) using our map
            org_value = org_label_to_value.get(org_input)

            # Fallback: guess prefix from dataset + code in parentheses
            if not org_value:
                m = re.search(r"\(([^)]+)\)$", org_input)
                code = (m.group(1).strip() if m else "")
                if code:
                    # minimal rule-of-thumb by dataset
                    if dataset_id == "brownfield-land":
                        org_value = f"local-authority-eng:{code}"
                    elif dataset_id == "listed-building":
                        org_value = f"government-organisation:{code}"
                    # add extra dataset rules here if needed

            # ✅ Ensure canonical CURIE even if user typed local-authority:CODE
            org_value = normalize_org_curie(org_value or org_input, dataset_id)

            # Core required fields
            errors.update({
                "dataset": not dataset_input,
                "organisation": (org_warning or (selected_orgs and org_input not in selected_orgs) or not org_value),
                "endpoint_url": (not endpoint_url or not re.match(r"https?://[^\s]+", endpoint_url)),
            })

            # Optional fields validation (doc_url only if present)
            if doc_url and not re.match(r"^https?://.*\.(gov\.uk|org\.uk)(/.*)?$", doc_url):
                errors["documentation_url"] = True

            try:
                if day and month and year:
                    datetime(int(year), int(month), int(day))
                elif any([day, month, year]):
                    errors["start_date"] = True
            except Exception:
                errors["start_date"] = True

            if not any(errors.values()):
                payload = {
                    "params": {
                        "type": "check_url",
                        # ✅ send the correct ids
                        "collection": collection_id,
                        "dataset": dataset_id,
                        "url": endpoint_url,
                        # optional extras, null if empty
                        "documentation_url": doc_url or None,
                        "licence": licence or None,
                        "start_date": f"{year}-{month.zfill(2)}-{day.zfill(2)}" if day and month and year else None,
                        # ✅ send the REAL entity reference (prefix:CODE)
                        "organisation": org_value,
                    }
                }
                session["optional_fields"] = {
                    "documentation_url": doc_url,
                    "licence": licence,
                    "start_date": f"{year}-{month.zfill(2)}-{day.zfill(2)}" if day and month and year else None
                }

                if column_mapping:
                    payload["params"]["column_mapping"] = column_mapping
                if geom_type:
                    payload["params"]["geom_type"] = geom_type

                print("📦 Sending payload to request API:")
                print(json.dumps(payload, indent=2))

                try:
                    async_api = f"{get_request_api_endpoint()}/requests"
                    response = requests.post(async_api, json=payload, timeout=REQUESTS_TIMEOUT)

                    if response.status_code == 202:
                        request_id = response.json()["id"]
                        return redirect(
                            url_for(
                                "datamanager.check_results",
                                request_id=request_id,
                                organisation=org_value  # pass value along
                            )
                        )
                    else:
                        try:
                            detail = response.json()
                        except Exception:
                            detail = response.text
                        abort(500, f"Check tool submission failed ({response.status_code}): {detail}")
                except Exception as e:
                    traceback.print_exc()
                    abort(500, f"Backend error: {e}")

    return render_template(
        "datamanager/dashboard_add.html",
        dataset_input=dataset_input,
        selected_orgs=selected_orgs,
        form=form,
        errors=errors,
        dataset_options=dataset_options,
    )

@datamanager_bp.route("/check-results/<request_id>")
def check_results(request_id):
    try:
        async_api = get_request_api_endpoint()
        organisation = request.args.get("organisation", "Your organisation")
        page = int(request.args.get("page", 1))
        page_size = 50
        offset = (page - 1) * page_size

        # Fetch summary
        response = requests.get(f"{async_api}/requests/{request_id}", timeout=REQUESTS_TIMEOUT)
        if response.status_code != 200:
            return render_template("error.html", message="Check failed or not found"), 404

        result = response.json()
        result.setdefault("params", {}).setdefault("organisation", organisation)

        # Still processing?
        if result.get("status") in ["PENDING", "PROCESSING", "QUEUED"] or result.get("response") is None:
            return render_template("datamanager/check-results-loading.html", result=result)

        # Row details (paged, includes per-row issue logs)
        resp_details = requests.get(
            f"{async_api}/requests/{request_id}/response-details",
            params={"offset": offset, "limit": page_size},
            timeout=REQUESTS_TIMEOUT
        ).json() or []

        total_rows = result.get("response", {}).get("data", {}).get("row-count", len(resp_details))

        # Geometry mapping
        geometries = []
        for row in resp_details:
            geom = row.get("geometry")
            if not geom:
                wkt_str = (row.get("converted_row") or {}).get("Point")
                if wkt_str:
                    try:
                        shapely_geom = wkt.loads(wkt_str)
                        geom = mapping(shapely_geom)
                    except Exception:
                        geom = None
            if geom:
                geometries.append({
                    "type": "Feature",
                    "geometry": geom,
                    "properties": {
                        "reference": (row.get("converted_row") or {}).get("Reference") or f"Entry {row.get('entry_number')}"
                    }
                })

        # Error parsing
        data = (result.get("response") or {}).get("data") or {}
        error_summary = data.get("error-summary", [])
        column_field_log = data.get("column-field-log", [])

        # --- NEW ENTITY PREVIEW ---
        # 1) Prefer backend-provided preview
        new_entities = data.get("new-entities", [])  # list[dict]
        new_entity_count = len(new_entities)

        new_entities_message = (
            "No new entities detected for this source."
            if new_entity_count == 0
            else f"{new_entity_count} new entities proposed. These will be created when you add data."
        )

        # 2) If not provided, infer from per-row issue_logs
        if not new_entities:
            inferred = []
            for d in resp_details:
                issue_logs = (d.get("issue_logs") or d.get("detail", {}).get("issue_logs") or [])
                converted = (d.get("converted_row") or {})
                for iss in issue_logs:
                    t = (iss.get("issue-type") or iss.get("type") or "").lower()
                    if "unknown entity" in t:
                        ref_val = (converted.get("Reference")
                                   or converted.get("reference")
                                   or iss.get("reference")
                                   or "")
                        if ref_val:
                            inferred.append({
                                "reference": str(ref_val).strip(),
                                "prefix": iss.get("prefix") or "",
                                "organisation": iss.get("organisation") or ""
                            })
                            break
            new_entities = inferred

        # Build fast lookup set of references
        new_entity_refs = {
            str((e or {}).get("reference", "")).strip()
            for e in (new_entities or [])
            if (e or {}).get("reference")
        }
        new_entity_count = len(new_entity_refs)

        # Build table (with extra "New entity?" column)
        table_headers, formatted_rows = [], []
        if resp_details:
            first_row = (resp_details[0] or {}).get("converted_row", {}) or {}
            table_headers = list(first_row.keys())

            # Decide the reference column to read for marking
            ref_col = None
            if table_headers:
                for cand in table_headers:
                    if cand.lower() == "reference":
                        ref_col = cand
                        break
                if not ref_col:
                    for cand in table_headers:
                        if "ref" in cand.lower():
                            ref_col = cand
                            break

            # Add the extra column once
            extra_col = "New entity?"
            if extra_col not in table_headers:
                table_headers.append(extra_col)

            for row in resp_details:
                converted = (row.get("converted_row") or {})
                row_cols = {col: {"value": converted.get(col, "")} for col in table_headers if col != extra_col}

                ref_val = (converted.get(ref_col) if ref_col else "")
                is_new = "Yes" if (str(ref_val).strip() in new_entity_refs) else ""
                row_cols[extra_col] = {"value": is_new}

                formatted_rows.append({"columns": row_cols})

        table_params = {
            "columns": table_headers,
            "fields": table_headers,
            "rows": formatted_rows,
            "columnNameProcessing": "none"
        }

        showing_start = offset + 1 if total_rows > 0 else 0
        showing_end = min(offset + page_size, total_rows)

        must_fix, fixable, passed_checks = [], [], []
        for err in error_summary:
            fixable.append(err)

        for col in column_field_log:
            if not col.get("missing"):
                passed_checks.append(f"All rows have {col.get('field')} present")
            else:
                must_fix.append(f"Missing required field: {col.get('field')}")

        allow_add_data = len(must_fix) == 0

        params = result.get("params", {}) or {}
        optional_missing = not (
            params.get("documentation_url") and
            params.get("licence") and
            params.get("start_date")
        )
        show_error = not allow_add_data

        entities_preview_url = url_for("datamanager.entities_preview", request_id=request_id)

        return render_template(
            "datamanager/check-results.html",
            result=result,
            geometries=geometries,
            must_fix=must_fix,
            fixable=fixable,
            passed_checks=passed_checks,
            allow_add_data=allow_add_data,
            show_error=show_error,
            optional_missing=optional_missing,
            table_params=table_params,
            page=page,
            total_rows=total_rows,
            page_size=page_size,
            showing_start=showing_start,
            showing_end=showing_end,
            new_entity_count=new_entity_count,
            new_entities_message=new_entities_message,
            entities_preview_url=entities_preview_url,
        )

    except Exception as e:
        traceback.print_exc()
        abort(500, f"Error fetching results from backend: {e}")


@datamanager_bp.route("/check-results/optional-submit", methods=["GET","POST"])
def optional_fields_submit():
    form = request.form.to_dict()
    request_id = form.get("request_id")
    documentation_url = form.get("documentation_url", "").strip()
    licence = form.get("licence", "").strip()
    start_day = form.get("start_day", "").strip()
    start_month = form.get("start_month", "").strip()
    start_year = form.get("start_year", "").strip()

    start_date = None
    if start_day and start_month and start_year:
        start_date = f"{start_year}-{start_month.zfill(2)}-{start_day.zfill(2)}"

    # 🔹 Save in backend
    async_api = get_request_api_endpoint()
    payload = {
        "params": {
            "type": "check_url",
            "documentation_url": documentation_url or None,
            "licence": licence or None,
            "start_date": start_date
        }
    }
    print("\n 📦 Submitting optional fields to request API: \n")
    try:
        requests.patch(
            f"{async_api}/requests/{request_id}",
            json=payload,
            timeout=REQUESTS_TIMEOUT
        )
        print(f"✅ Successfully updated request {request_id} in request-api")
        print(json.dumps(payload, indent=2))
    except Exception as e:
        print(f"❌ Failed to update request {request_id} in request-api: {e}")

    return redirect(url_for( "datamanager.add_data",request_id=request_id))

    return redirect(url_for("datamanager.check_results", request_id=request_id))


@datamanager_bp.route("/dashboard/debug-payload", methods=["POST"])
def debug_payload():
    form = request.form.to_dict()

    dataset = (form.get("dataset") or "").strip()
    endpoint = (form.get("endpoint_url") or "").strip()

    # ✅ Optional fields from the form
    documentation_url = (form.get("documentation_url") or "").strip()
    licence = (form.get("licence") or "").strip()
    day = (form.get("start_day") or "").strip()
    month = (form.get("start_month") or "").strip()
    year = (form.get("start_year") or "").strip()

    # Build start_date if all parts present
    start_date = None
    if day and month and year:
        start_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"

    payload = {
        "params": {
            "type": "check_url",
            "collection": dataset,  # this is just a preview; actual submit uses ids
            "dataset": dataset,
            "url": endpoint,
            "documentation_url": documentation_url or None,
            "licence": licence or None,
            "start_date": start_date
        }
    }

    # Optional extras if you use them
    column_mapping_str = (form.get("column_mapping") or "").strip()
    geom_type = (form.get("geom_type") or "").strip()
    if column_mapping_str:
        try:
            payload["params"]["column_mapping"] = json.loads(column_mapping_str)
        except Exception as e:
            payload["params"]["column_mapping"] = f"⚠️ Invalid JSON: {e}"
    if geom_type:
        payload["params"]["geom_type"] = geom_type

    return render_template("datamanager/debug_payload.html", payload=payload)

@datamanager_bp.route("/check-results/<request_id>/add-data", methods=["GET", "POST"])
def add_data(request_id):
    async_api = get_request_api_endpoint()

    # Load original request (for dataset/collection/url/column_mapping/geom_type/etc.)
    try:
        resp = requests.get(f"{async_api}/requests/{request_id}", timeout=REQUESTS_TIMEOUT)
        resp.raise_for_status()
    except Exception as e:
        abort(500, f"Failed to load original request {request_id}: {e}")

    result = resp.json() or {}
    params = (result.get("params") or {}).copy()

    # Existing optional values (backend/session)
    existing_doc = params.get("documentation_url") or session.get("optional_fields", {}).get("documentation_url")
    existing_lic = params.get("licence") or session.get("optional_fields", {}).get("licence")
    existing_start = params.get("start_date") or session.get("optional_fields", {}).get("start_date")

    if request.method == "GET":
        if not (existing_doc and existing_lic and existing_start):
            # Show optional fields screen
            return render_template("datamanager/add-data.html", request_id=request_id)
        # Everything present already — you could auto-submit, but better to let user confirm
        return render_template("datamanager/add-data.html", request_id=request_id)

    # POST (from add-data.html "Continue" OR the Add data button if you post directly)
    form = request.form.to_dict()

    # Prefer submitted values; fall back to existing ones
    doc_url = (form.get("documentation_url") or existing_doc or "").strip()
    licence = (form.get("licence") or existing_lic or "").strip()

    day = (form.get("start_day") or "").strip()
    month = (form.get("start_month") or "").strip()
    year = (form.get("start_year") or "").strip()
    if day and month and year:
        start_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    else:
        start_date = (existing_start or "").strip()

    # Minimal validation — ensure we have all three before submitting add_data
    if not (doc_url and licence and start_date):
        # Re-render optional screen (could pass errors/form back if you like)
        return render_template("datamanager/add-data.html", request_id=request_id)

    # Best-effort: persist optional fields on the original request
    try:
        requests.patch(
            f"{async_api}/requests/{request_id}",
            json={"params": {"documentation_url": doc_url, "licence": licence, "start_date": start_date}},
            timeout=REQUESTS_TIMEOUT,
        )
    except Exception as e:
        print(f"❌ Failed to update request {request_id} in request-api: {e}")

    # Build and submit add_data job
    submit_params = params.copy()
    submit_params["type"] = "add_data"
    submit_params["documentation_url"] = doc_url
    submit_params["licence"] = licence
    submit_params["source_request_id"] = request_id
    submit_params["start_date"] = start_date
    # Optional provenance
    # submit_params["source_request_id"] = request_id

    payload = {"params": submit_params}
    try:
        print("📦 Submitting add_data payload:")
        print(json.dumps(payload, indent=2))
    except Exception:
        pass

    try:
        submit = requests.post(f"{async_api}/requests", json=payload, timeout=REQUESTS_TIMEOUT)
    except Exception as e:
        traceback.print_exc()
        abort(500, f"Backend error submitting add_data: {e}")

    if submit.status_code == 202:
        body = {}
        try:
            body = submit.json() or {}
        except Exception:
            pass

        new_request_id = body.get("id")
        msg = body.get("message") or "Entity assignment is in progress"

        session.pop("optional_fields", None)
        return redirect(url_for(
            "datamanager.add_data_progress",
            request_id=new_request_id,
            msg=msg
        ))

    else:
        try:
            detail = submit.json()
        except Exception:
            detail = submit.text
        abort(500, f"Add data submission failed ({submit.status_code}): {detail}")

# --- Configure screen: CSV + MUST-FIX on left, smart dropdowns on right ---
@datamanager_bp.route("/configure/<request_id>", methods=["GET", "POST"])
def configure(request_id):
    async_api = get_request_api_endpoint()

    # 1) Load the original request summary
    r = requests.get(f"{async_api}/requests/{request_id}", timeout=REQUESTS_TIMEOUT)
    if r.status_code != 200:
        return render_template("error.html", message="Original request not found"), 404

    req = r.json()
    params = req.get("params", {}) or {}
    organisation = params.get("organisation", request.args.get("organisation", "Your organisation"))
    dataset_id = params.get("dataset", "") or ""
    source_url = params.get("url", "") or ""
    existing_geom_type = params.get("geom_type") or ""

    # 2) Existing mapping known by backend (prefer echoed response over params)
    existing_raw_to_spec = (
        (req.get("response") or {}).get("data", {}).get("column-mapping")
        or params.get("column_mapping")
        or {}
    )
    # case-insensitive view of mapping (raw -> spec)
    existing_raw_to_spec_ci = {
        (k or "").strip().lower(): (v or "").strip()
        for k, v in existing_raw_to_spec.items()
    }

    # Build reverse map SPEC -> RAW from existing mapping
    spec_to_raw = {}
    for raw, spec in existing_raw_to_spec.items():
        if raw and spec and spec not in spec_to_raw:
            spec_to_raw[spec] = raw

    # 3) Full specification list (scoped union)
    spec_fields = get_spec_fields_union(dataset_id)
    spec_lookup_lower = {(f or "").strip().lower(): f for f in spec_fields}

    # 4) CSV headers + preview
    raw_headers, raw_rows = read_raw_csv_preview(source_url)

    # 5) Must-fix spec fields from last check
    data_blob = (req.get("response") or {}).get("data") or {}
    cfl = data_blob.get("column-field-log", []) or []
    must_fix_specs = [row["field"] for row in cfl if row.get("missing") and row.get("field")]

    # 6) Helper to normalise names for auto-match
    def _norm(s: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", (s or "").lower())
    spec_by_norm = {_norm(f): f for f in spec_fields}

    # 7) POST: collect mappings
    if request.method == "POST":
        form = request.form.to_dict()
        new_mapping: dict[str, str] = {}

        # A) RAW rows: map_raw[<raw>] -> <spec>
        chosen_spec_by_raw = {}
        for raw in raw_headers:
            chosen_spec = (form.get(f"map_raw[{raw}]") or "").strip()
            if chosen_spec and chosen_spec != "__NOT_MAPPED__":
                new_mapping[raw] = chosen_spec
                chosen_spec_by_raw[raw] = chosen_spec

        # Reverse of the current form choices too (SPEC -> RAW), preferred over existing
        spec_to_raw_from_form = {}
        for raw, spec in chosen_spec_by_raw.items():
            spec_to_raw_from_form.setdefault(spec, raw)

        # B) MUST-FIX rows: user picks a SPEC "source" to feed each must-fix SPEC
        # name="map_spec_to_spec[<mustfix_spec>]" in the template
        for mustfix_spec in must_fix_specs:
            chosen_source_spec = (form.get(f"map_spec_to_spec[{mustfix_spec}]") or "").strip()
            if not chosen_source_spec or chosen_source_spec == "__NOT_MAPPED__":
                continue

            # Prefer RAW resolved from this form submission; fall back to previous mapping
            source_raw = (
                spec_to_raw_from_form.get(chosen_source_spec)
                or spec_to_raw.get(chosen_source_spec)
            )

            if source_raw:
                # This RAW should now fulfil the must-fix spec
                new_mapping[source_raw] = mustfix_spec

        geom_type = (form.get("geom_type") or "").strip()

        # ✅ Ensure organisation CURIE is canonical before sending
        organisation = normalize_org_curie(organisation, dataset_id)

        payload = {
            "params": {
                "type": "check_url",
                "collection": params.get("collection"),
                "dataset": dataset_id,
                "url": source_url,
                "documentation_url": params.get("documentation_url"),
                "licence": params.get("licence"),
                "start_date": params.get("start_date"),
                "column_mapping": new_mapping or None,  # RAW -> SPEC
                "geom_type": geom_type or None,
                "organisation": organisation,
            }
        }
        print("\n📦 Re-check payload (configure):\n", json.dumps(payload, indent=2))

        try:
            new_req = requests.post(f"{async_api}/requests", json=payload, timeout=REQUESTS_TIMEOUT)
            if new_req.status_code == 202:
                new_id = new_req.json()["id"]
                return redirect(url_for("datamanager.configure", request_id=new_id))
            else:
                detail = (
                    new_req.json() if "application/json" in (new_req.headers.get("content-type") or "")
                    else new_req.text
                )
                return render_template(
                    "error.html",
                    message=f"Re-check submission failed ({new_req.status_code}): {detail}"
                )
        except Exception as e:
            traceback.print_exc()
            return render_template("error.html", message=f"Backend error: {e}")

    # 8) Build raw preview table model
    def table_from_csv(headers, rows):
        if not headers or not rows:
            return {"columns": [], "fields": [], "rows": [], "columnNameProcessing": "none"}
        out_rows = []
        for r_ in rows:
            out_rows.append({"columns": {headers[i]: {"value": r_[i]} for i in range(len(headers))}})
        return {"columns": headers, "fields": headers, "rows": out_rows, "columnNameProcessing": "none"}

    raw_table_params = table_from_csv(raw_headers, raw_rows)

    # 9) Transformed preview (always defined)
    transformed_table_params = {"columns": [], "fields": [], "rows": [], "columnNameProcessing": "none"}
    try:
        if (req.get("status") not in ["PENDING", "PROCESSING", "QUEUED"]) and (req.get("response") is not None):
            details = requests.get(
                f"{async_api}/requests/{request_id}/response-details",
                params={"offset": 0, "limit": 50},
                timeout=REQUESTS_TIMEOUT
            ).json() or []
            if details:
                first = (details[0] or {}).get("converted_row", {}) or {}
                t_columns = list(first.keys())
                t_rows = []
                for d in details:
                    conv = (d.get("converted_row") or {})
                    t_rows.append({"columns": {c: {"value": conv.get(c, "")} for c in t_columns}})
                transformed_table_params = {
                    "columns": t_columns,
                    "fields": t_columns,
                    "rows": t_rows,
                    "columnNameProcessing": "none",
                }
    except Exception:
        traceback.print_exc()

    # 10) Build UI rows (must-fix first)
    csv_rows = []
    for raw in raw_headers:
        raw_key_ci = (raw or "").strip().lower()
        mapped_spec = existing_raw_to_spec_ci.get(raw_key_ci, "")
        if not mapped_spec:
            auto = spec_by_norm.get(_norm(raw))
            if auto:
                mapped_spec = auto
        if mapped_spec:
            mapped_spec = spec_lookup_lower.get(mapped_spec.strip().lower(), mapped_spec)
        csv_rows.append({"kind": "raw", "label": raw, "preselect": mapped_spec, "mapped": bool(mapped_spec)})

    mustfix_rows = []
    for spec in must_fix_specs:
        mapped_raw = spec_to_raw.get(spec, "")
        mustfix_rows.append({"kind": "mustfix", "label": spec, "preselect": mapped_raw, "mapped": bool(mapped_raw)})

    display_rows = mustfix_rows + csv_rows

    # 11) Render
    return render_template(
        "datamanager/configure.html",
        request_id=request_id,
        organisation=organisation,
        dataset=dataset_id,
        raw_table_params=raw_table_params,
        transformed_table_params=transformed_table_params,
        rows=display_rows,
        spec_options=spec_fields,
        raw_options=raw_headers,
        must_fix_specs=must_fix_specs,
        existing_geom_type=existing_geom_type,
    )

@datamanager_bp.route("/add-data/progress/<request_id>")
def add_data_progress(request_id):
    # fallback message in case BE didn’t return one
    message = request.args.get("msg", "Entity assignment is in progress")
    return render_template(
        "datamanager/add-data-progress.html",
        request_id=request_id,
        message=message,
    )

@datamanager_bp.route("/check-results/<request_id>/entities")
def entities_preview(request_id):
    async_api = get_request_api_endpoint()

    # ---- load the summary (top-level response) ----
    r = requests.get(f"{async_api}/requests/{request_id}", timeout=REQUESTS_TIMEOUT)
    if r.status_code != 200:
        return render_template("error.html", message="Request not found"), 404
    req = r.json() or {}
    data = (req.get("response") or {}).get("data") or {}

    proposed = data.get("new-entities") or []          # [{reference,prefix,organisation}]
    entity_summary = data.get("entity-summary") or {}  # {new-in-resource, existing-in-resource, new-entity-breakdown}

    # ---- load response-details (can be dict->rows, list of dicts, or list of JSON strings) ----
    raw = requests.get(
        f"{async_api}/requests/{request_id}/response-details",
        params={"offset": 0, "limit": 1000},
        timeout=REQUESTS_TIMEOUT
    ).json() | []
    if isinstance(raw, dict):
        details_raw = raw.get("rows") or raw.get("data") or []
    else:
        details_raw = raw

    def _coerce_detail(item):
        # Accept plain dict with converted_row, dict with "detail", or JSON string
        if isinstance(item, dict):
            if "detail" in item:
                det = item["detail"]
                if isinstance(det, str):
                    try:
                        return json.loads(det) or {}
                    except Exception:
                        return {}
                return det or {}
            return item
        if isinstance(item, str):
            try:
                return json.loads(item) or {}
            except Exception:
                return {}
        return {}

    details = [_coerce_detail(d) for d in details_raw]

    # ---- build ref -> line-number map from details ----
    def _ref_from_conv(conv: dict) -> str:
        for key in ("Reference", "reference", "ListEntry", "Entity reference", "entity_reference", "entity-reference", "ref"):
            v = conv.get(key)
            if v not in (None, ""):
                return str(v).strip()
        for k, v in conv.items():
            if "ref" in (k or "").lower() and v not in (None, ""):
                return str(v).strip()
        return ""

    ref_to_line = {}
    for d in details:
        conv = (d.get("converted_row") or {})
        entry_no = d.get("entry_number") or d.get("line-number")
        if not isinstance(conv, dict):
            conv = {}
        ref = _ref_from_conv(conv)
        if ref and entry_no is not None and str(ref) not in ref_to_line:
            ref_to_line[str(ref)] = entry_no

    # ---- table rows ----
    rows = []
    line_num = 1
    for e in (proposed or []):
        if not isinstance(e, dict):
            continue
        ref = str(e.get("reference", "")).strip()
        line_num += 1
        rows.append({
            "line-number": ref_to_line.get(ref) or line_num,
            "reference": ref,
            "prefix": e.get("prefix") or "",
            "organisation": e.get("organisation") or "",
        })

    def _ln(v):
        try:
            return int(v.get("line-number") or 0)
        except Exception:
            return 0

    rows.sort(key=lambda r: (_ln(r), r.get("reference", "")))

    columns = ["line-number", "reference", "prefix", "organisation"]
    table_params = {
        "columns": columns,
        "fields": columns,
        "rows": [{"columns": {c: {"value": r.get(c, "")} for c in columns}} for r in rows],
        "columnNameProcessing": "none",
    }

    new_count = entity_summary.get("new-in-resource")
    if new_count in (None, "", 0):
        new_count = len(proposed or [])
    existing_count = entity_summary.get("existing-in-resource") or 0

    return render_template(
        "datamanager/entities_preview.html",
        request_id=request_id,
        new_count=int(new_count or 0),
        existing_count=int(existing_count or 0),
        breakdown=entity_summary.get("new-entity-breakdown") or [],
        table_params=table_params,
        back_url=url_for("datamanager.check_results", request_id=request_id),
    )
