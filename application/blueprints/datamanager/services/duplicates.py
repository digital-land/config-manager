import json
import re


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


def _candidate_key(candidate: dict) -> tuple[str, str, str]:
    return (
        _normalise_entity_id(candidate.get("old_entity", "")),
        _normalise_entity_id(candidate.get("entity", "")),
        str(candidate.get("dataset", "") or ""),
    )


def parse_selected_redirects(
    values: list[str], duplicate_candidates: list[dict] | None = None
) -> list[dict]:
    selected = []
    valid_keys = (
        {_candidate_key(candidate) for candidate in duplicate_candidates}
        if duplicate_candidates is not None
        else None
    )
    seen_old_entities = set()

    for value in values:
        try:
            row = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(row, dict):
            continue

        old_entity = _normalise_entity_id(row.get("old_entity", ""))
        entity = _normalise_entity_id(row.get("entity", ""))
        dataset = str(row.get("dataset", "") or "")
        if not old_entity or not entity or not dataset:
            continue
        if valid_keys is not None and (old_entity, entity, dataset) not in valid_keys:
            continue
        if old_entity in seen_old_entities:
            continue
        seen_old_entities.add(old_entity)

        selected.append(
            {
                "old_entity": old_entity,
                "entity": entity,
                "dataset": dataset,
                "old_reference": str(row.get("old_reference", "") or ""),
                "new_reference": str(row.get("new_reference", "") or ""),
                "match_type": str(row.get("match_type", "") or ""),
                "notes": str(
                    row.get("notes", "")
                    or "Redirect duplicate entity selected in Assign Entities"
                ),
            }
        )

    return selected
