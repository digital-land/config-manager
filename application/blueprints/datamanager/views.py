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
        print(f"⚠️ Could not fetch/parse CSV preview: {e}")
    return headers_, rows


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
                f"{row['organisation']['label']} ({row['organisation']['value'].split(':', 1)[1]})": row[
                    "organisation"
                ]["value"]
                for row in provision_rows
                if row.get("organisation", {}).get("value")
            }

        if mode == "final":
            # what the user submitted from the select/input
            org_input = (form.get("organisation") or "").strip()
            endpoint_url = form.get("endpoint_url", "").strip()
            doc_url = form.get("documentation_url", "").strip()

            # ✅ licence defaults to 'ogl' if blank
            licence = (form.get("licence") or "ogl").strip().lower()

            # ✅ start_date defaults to today if user leaves all blank; partial dates still error
            day = (form.get("start_day") or "").strip()
            month = (form.get("start_month") or "").strip()
            year = (form.get("start_year") or "").strip()
            start_date_str = None
            if any([day, month, year]):
                if day and month and year:
                    try:
                        start_date_str = datetime(int(year), int(month), int(day)).date().isoformat()
                    except Exception:
                        errors["start_date"] = True
                else:
                    errors["start_date"] = True
            else:
                start_date_str = datetime.utcnow().date().isoformat()

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

            # Core required fields
            errors.update(
                {
                    "dataset": not dataset_input,
                    "organisation": (org_warning or (selected_orgs and org_input not in selected_orgs) or not org_value),
                    "endpoint_url": (not endpoint_url or not re.match(r"https?://[^\s]+", endpoint_url)),
                }
            )

            # Optional fields validation (doc_url only if present)
            if doc_url and not re.match(r"^https?://[^\s/]+\.(gov\.uk|org\.uk)(/.*)?$", doc_url):
                errors["documentation_url"] = True

            if not any(errors.values()):
                payload = {
                    "params": {
                        "type": "check_url",
                        # ✅ send the correct ids
                        "collection": collection_id,
                        "dataset": dataset_id,
                        "url": endpoint_url,
                        # optional extras, but we now always send defaults
                        "documentation_url": doc_url or None,
                        "licence": licence,  # ✅ defaulted above
                        "start_date": start_date_str,  # ✅ defaulted above
                        # ✅ send the REAL entity reference (prefix:CODE)
                        "organisation": org_value,
                    }
                }
                session["optional_fields"] = {
                    "documentation_url": doc_url,
                    "licence": licence,
                    "start_date": start_date_str,
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

                    print(f"⬅️ request-api responded with {response.status_code}")
                    try:
                        print(json.dumps(response.json(), indent=2))
                    except Exception:
                        print((response.text or "")[:2000])

                    if response.status_code == 202:
                        request_id = response.json()["id"]
                        return redirect(
                            url_for(
                                "datamanager.check_results",
                                request_id=request_id,
                                organisation=org_value,  # pass value along
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

        # Row details (paged)
        details_response = requests.get(
            f"{async_api}/requests/{request_id}/response-details",
            params={"offset": offset, "limit": page_size},
            timeout=REQUESTS_TIMEOUT,
        )
        details_response.raise_for_status()
        resp_details = details_response.json() or []

        total_rows = (result.get("response") or {}).get("data", {}).get("row-count", len(resp_details))

        # --- TRUST BACKEND ONLY ---
        data = (result.get("response") or {}).get("data") or {}
        entity_summary = data.get("entity-summary") or {}
        new_entities = data.get("new-entities") or []

        # exact lists as provided by BE
        existing_entities_list = data.get("existing-entities") or []

        # exact counts as provided by BE (fallback to 0 if missing)
        existing_count = int(entity_summary.get("existing-in-resource") or 0)
        new_count = int(entity_summary.get("new-in-resource") or 0)

        print("🔎 BE data keys:", list(data.keys()))
        print("🔎 entity-summary:", entity_summary)
        print("🔎 existing-entities len:", len(existing_entities_list))

        # minimal message that only reflects BE numbers
        new_entities_message = f"{existing_count} existing; {new_count} new (from backend)."
        show_entities_link = new_count > 0

        # Geometry mapping (unchanged)
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
                geometries.append(
                    {
                        "type": "Feature",
                        "geometry": geom,
                        "properties": {
                            "reference": (row.get("converted_row") or {}).get("Reference")
                            or f"Entry {row.get('entry_number')}",
                        },
                    }
                )

        # Error parsing (unchanged)
        error_summary = data.get("error-summary", []) or []
        column_field_log = data.get("column-field-log", []) or []

        # ---- Table build (no FE-added 'Entity status') ----
        table_headers, formatted_rows = [], []
        if resp_details:
            first_row = (resp_details[0] or {}).get("converted_row", {}) or {}
            table_headers = list(first_row.keys())

            for row in resp_details:
                converted = (row.get("converted_row") or {})
                formatted_rows.append(
                    {"columns": {col: {"value": converted.get(col, "")} for col in table_headers}}
                )

        table_params = {
            "columns": table_headers,
            "fields": table_headers,
            "rows": formatted_rows,
            "columnNameProcessing": "none",
        }

        showing_start = offset + 1 if total_rows > 0 else 0
        showing_end = min(offset + page_size, total_rows)

        # checks
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
            params.get("documentation_url")
            and params.get("licence")
            and params.get("start_date")
        )
        show_error = not allow_add_data

        entities_preview_url = url_for("datamanager.entities_preview", request_id=request_id)

        # compact preview (first 5) – exactly what BE sent for "new-entities"
        entity_preview_rows = []
        for e in (new_entities or [])[:5]:
            entity_preview_rows.append(
                {
                    "reference": str(e.get("reference", "")).strip(),
                    "prefix": e.get("prefix") or "",
                    "organisation": e.get("organisation") or "",
                    "entity": str(e.get("entity", "")).strip(),
                }
            )

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
            # entity summary bits (BE only):
            new_entities_message=new_entities_message,
            new_entity_count=new_count,
            existing_entity_count=existing_count,
            existing_entities_list=existing_entities_list,
            show_entities_link=show_entities_link,
            entities_preview_url=entities_preview_url,
            entity_preview_rows=entity_preview_rows,
        )

    except Exception as e:
        traceback.print_exc()
        abort(500, f"Error fetching results from backend: {e}")


@datamanager_bp.route("/check-results/optional-submit", methods=["GET", "POST"])
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
            "start_date": start_date,
        }
    }
    print("\n 📦 Submitting optional fields to request API: \n")
    try:
        requests.patch(f"{async_api}/requests/{request_id}", json=payload, timeout=REQUESTS_TIMEOUT)
        print(f"✅ Successfully updated request {request_id} in request-api")
        print(json.dumps(payload, indent=2))
    except Exception as e:
        print(f"❌ Failed to update request {request_id} in request-api: {e}")

    return redirect(url_for("datamanager.add_data", request_id=request_id))


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
            "start_date": start_date,
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

    # Load the original CHECK request (we reuse its params and artefacts via source_request_id)
    resp = requests.get(f"{async_api}/requests/{request_id}", timeout=REQUESTS_TIMEOUT)
    if resp.status_code != 200:
        return render_template("error.html", message="Original request not found"), 404
    original = resp.json() or {}
    base = (original.get("params") or {}).copy()

    # Anything we already know (from BE or session)
    existing_doc   = base.get("documentation_url") or session.get("optional_fields", {}).get("documentation_url")
    existing_lic   = base.get("licence")           or session.get("optional_fields", {}).get("licence")
    existing_start = base.get("start_date")        or session.get("optional_fields", {}).get("start_date")

    def _submit_preview(doc_url: str, licence: str, start_date: str):
        params = base.copy()
        params.update({
            "type": "add_data",
            "preview": True,                 # ← PREVIEW mode
            "source_request_id": request_id, # ← reuse check_url artefacts in BE
            "documentation_url": doc_url,
            "licence": licence,
            "start_date": start_date,
        })
        try:
            print("\n 📦 add_data Preview -outgoing payload: \n")
            print(json.dumps(params, indent=2))
        except Exception:
            pass    
        r = requests.post(f"{async_api}/requests", json={"params": params}, timeout=REQUESTS_TIMEOUT)
        
        try:
            print(f"add_data preview -request-api responded {r.status_code}")
        except Exception:
            pass
           
        if r.status_code == 202:
            preview_id = (r.json() or {}).get("id")
            return redirect(url_for("datamanager.entities_preview", request_id=preview_id))
        detail = r.json() if "application/json" in (r.headers.get("content-type") or "") else r.text
        abort(500, f"Preview submission failed ({r.status_code}): {detail}")

    if request.method == "GET":
        if existing_doc and existing_lic and existing_start:
            # Optional fields are present → use EXISTING payload to preview
            return _submit_preview(existing_doc, existing_lic, existing_start)
        
        # Pre-populate form with any existing data for the initial GET request
        form_data = {
            "documentation_url": existing_doc,
            "licence": existing_lic,
        }
        return render_template("datamanager/add-data.html", request_id=request_id, form=form_data)

    # POST – user submitted optional fields
    form = request.form.to_dict()
    doc_url = (form.get("documentation_url") or existing_doc or "").strip()
    licence = (form.get("licence") or existing_lic or "").strip()

    d = (form.get("start_day") or "").strip()
    m = (form.get("start_month") or "").strip()
    y = (form.get("start_year") or "").strip()
    start_date = f"{y}-{m.zfill(2)}-{d.zfill(2)}" if (d and m and y) else (existing_start or "").strip()

    if not (doc_url and licence and start_date):
        # Still missing something – re-show optional screen
        return render_template("datamanager/add-data.html", request_id=request_id, form=form)

    # Remember locally (optional)
    session["optional_fields"] = {"documentation_url": doc_url, "licence": licence, "start_date": start_date}

    # Now preview with the UPDATED payload
    return _submit_preview(doc_url, licence, start_date)

@datamanager_bp.route("/check-results/<request_id>/add-data/confirm", methods=["POST"])
def add_data_confirm(request_id):
    async_api = get_request_api_endpoint()

    # request_id is the PREVIEW add_data id
    pr = requests.get(f"{async_api}/requests/{request_id}", timeout=REQUESTS_TIMEOUT)
    if pr.status_code != 200:
        return render_template("error.html", message="Preview not found"), 404
    preview_req = pr.json() or {}
    params = (preview_req.get("params") or {}).copy()
    params["type"] = "add_data"
    params["preview"] = False  # commit!

    submit = requests.post(f"{async_api}/requests", json={"params": params}, timeout=REQUESTS_TIMEOUT)
    if submit.status_code == 202:
        body = submit.json() or {}
        new_id = body.get("id")
        msg = body.get("message") or "Entity assignment is in progress"
        session.pop("optional_fields", None)
        return redirect(url_for("datamanager.add_data_progress", request_id=new_id, msg=msg))

    detail = submit.json() if "application/json" in (submit.headers.get("content-type") or "") else submit.text
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
    existing_raw_to_spec_ci = {(k or "").strip().lower(): (v or "").strip() for k, v in existing_raw_to_spec.items()}

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

        # B) MUST-FIX rows
        for mustfix_spec in must_fix_specs:
            chosen_source_spec = (form.get(f"map_spec_to_spec[{mustfix_spec}]") or "").strip()
            if not chosen_source_spec or chosen_source_spec == "__NOT_MAPPED__":
                continue
            source_raw = spec_to_raw_from_form.get(chosen_source_spec) or spec_to_raw.get(chosen_source_spec)
            if source_raw:
                new_mapping[source_raw] = mustfix_spec

        geom_type = (form.get("geom_type") or "").strip()

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
                    new_req.json()
                    if "application/json" in (new_req.headers.get("content-type") or "")
                    else new_req.text
                )
                return render_template("error.html", message=f"Re-check submission failed ({new_req.status_code}): {detail}")
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
                timeout=REQUESTS_TIMEOUT,
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
    return render_template("datamanager/add-data-progress.html", request_id=request_id, message=message)


@datamanager_bp.route("/check-results/<request_id>/entities")
def entities_preview(request_id):
    async_api = get_request_api_endpoint()

    r = requests.get(f"{async_api}/requests/{request_id}", timeout=REQUESTS_TIMEOUT)
    if r.status_code != 200:
        return render_template("error.html", message="Preview not found"), 404
    req = r.json() or {}

    # Loading state
    if req.get("status") in {"PENDING", "PROCESSING", "QUEUED"} or req.get("response") is None:
        return render_template("datamanager/add-data-preview-loading.html", request_id=request_id)

    data = (req.get("response") or {}).get("data") or {}
    entity_summary   = data.get("entity-summary") or {}
    new_entities     = data.get("new-entities") or []
    endpoint_summary = data.get("endpoint_url_validation") or {}

    # Existing entities preference
    existing_entities_list = (
        entity_summary.get("existing-entity-breakdown")
        or data.get("existing-entities")
        or []
    )

    # New entities table
    cols = ["reference", "prefix", "organisation", "entity"]
    rows = [{"columns": {c: {"value": (e.get(c) or "")} for c in cols}} for e in new_entities]
    table_params = {"columns": cols, "fields": cols, "rows": rows, "columnNameProcessing": "none"}

    # Existing entities table
    ex_cols = ["reference", "entity"]
    ex_rows = [{"columns": {c: {"value": (e.get(c) or "")} for c in ex_cols}} for e in existing_entities_list]
    existing_table_params = {"columns": ex_cols, "fields": ex_cols, "rows": ex_rows, "columnNameProcessing": "none"}

    # ---------- endpoint.csv preview ----------
    endpoint_csv_table_params = None
    endpoint_csv_text = ""
    endpoint_csv_body = ""

    ep_cols = ["endpoint", "endpoint-url", "parameters", "plugin", "entry-date", "start-date", "end-date"]
    if endpoint_summary.get("columns"):
        ep_cols = endpoint_summary["columns"]

    ep_source = None
    if endpoint_summary.get("found_in_endpoint_csv") and endpoint_summary.get("existing_row"):
        ep_source = endpoint_summary["existing_row"]
    elif endpoint_summary.get("new_endpoint_entry"):
        ep_source = endpoint_summary["new_endpoint_entry"]

    if ep_source:
        ep_row = [str(ep_source.get(col, "") or "") for col in ep_cols]
        endpoint_csv_table_params = {
            "columns": ep_cols,
            "fields": ep_cols,
            "rows": [{"columns": {c: {"value": v} for c, v in zip(ep_cols, ep_row)}}],
            "columnNameProcessing": "none",
        }
        endpoint_csv_text = ",".join(ep_cols) + "\n" + ",".join(ep_row)
        endpoint_csv_body = ",".join(ep_row)

    # ---------- source.csv preview + summary ----------
    source_csv_table_params = None
    source_csv_text = ""
    source_csv_body = ""
    source_summary = None  # <<— new

    src_cols = [
        "source","attribution","collection","documentation-url","endpoint",
        "licence","organisation","pipelines","entry-date","start-date","end-date"
    ]
    src_source = endpoint_summary.get("new_source_entry") or None
    if src_source:
        src_row = [str(src_source.get(col, "") or "") for col in src_cols]
        source_csv_table_params = {
            "columns": src_cols,
            "fields": src_cols,
            "rows": [{"columns": {c: {"value": v} for c, v in zip(src_cols, src_row)}}],
            "columnNameProcessing": "none",
        }
        source_csv_text = ",".join(src_cols) + "\n" + ",".join(src_row)
        source_csv_body = ",".join(src_row)

        # Build summary panel model (like Endpoint Summary)
        source_summary = {
            "will_create": True,
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

    return render_template(
        "datamanager/entities_preview.html",
        request_id=request_id,
        new_count=int(entity_summary.get("new-in-resource") or 0),
        existing_count=int(entity_summary.get("existing-in-resource") or 0),
        breakdown=data.get("new-entity-breakdown") or [],
        endpoint_summary=endpoint_summary,
        table_params=table_params,
        existing_table_params=existing_table_params,
        # CSV previews
        endpoint_csv_table_params=endpoint_csv_table_params,
        endpoint_csv_text=endpoint_csv_text,
        endpoint_csv_body=endpoint_csv_body,
        source_csv_table_params=source_csv_table_params,
        source_csv_text=source_csv_text,
        source_csv_body=source_csv_body,
        # NEW: summary for source.csv
        source_summary=source_summary,
        back_url=url_for(
            "datamanager.add_data",
            request_id=(req.get("params") or {}).get("source_request_id") or request_id,
        ),
    )
