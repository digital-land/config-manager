import json
import re

REDIRECT_STATUSES = {"301", "410"}


def _normalise_entity_id(raw) -> str:
    if raw is None or raw == "":
        return ""
    if isinstance(raw, int):
        return str(raw)
    if isinstance(raw, float):
        return str(int(raw)) if raw.is_integer() else str(raw)

    raw_str = str(raw)
    if not re.match(r"^\s*[+-]?\d+(?:\.0+)?\s*$", raw_str):
        return raw_str

    try:
        return str(int(float(raw_str)))
    except (ValueError, TypeError):
        return raw_str


def parse_selected_redirects(
    values: list[str],
    duplicate_candidates: list[dict],
    excluded_references=None,
) -> list[dict]:
    selected = []
    valid_retirement_keys = set()
    valid_redirect_keys = set()
    for candidate in duplicate_candidates:
        old_entity = _normalise_entity_id(candidate.get("old_entity", ""))
        dataset = str(candidate.get("dataset", "") or "")
        new_reference = str(
            candidate.get("new_reference", "") or candidate.get("reference", "") or ""
        )
        valid_retirement_keys.add((old_entity, dataset))
        if new_reference:
            valid_redirect_keys.add((old_entity, dataset, new_reference))
    seen_old_entities = set()
    excluded_references = {
        str(reference or "").strip()
        for reference in (excluded_references or [])
        if str(reference or "").strip()
    }

    for value in values:
        try:
            row = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(row, dict):
            continue

        old_entity = _normalise_entity_id(row.get("old_entity", ""))
        dataset = str(row.get("dataset", "") or "")
        new_reference = str(row.get("new_reference", "") or "")
        row_status = str(row.get("status", "") or "").strip()
        status = row_status if row_status in REDIRECT_STATUSES else "301"
        if not old_entity or not dataset:
            continue
        if status == "410":
            if (old_entity, dataset) not in valid_retirement_keys:
                continue
        elif (
            not new_reference
            or (old_entity, dataset, new_reference) not in valid_redirect_keys
        ):
            continue
        if old_entity in seen_old_entities:
            continue

        if status == "410":
            selected.append({"old_entity_number": old_entity, "status": status})
            seen_old_entities.add(old_entity)
        elif new_reference not in excluded_references:
            selected.append(
                {
                    "reference": new_reference,
                    "old_entity_number": old_entity,
                    "status": status,
                }
            )
            seen_old_entities.add(old_entity)

    return selected
