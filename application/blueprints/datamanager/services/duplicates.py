import json

REDIRECT_NOTE = "Redirect duplicate entity selected in Assign Entities"


def _normalise_entity_id(raw) -> str:
    if raw is None or raw == "":
        return ""
    try:
        return str(int(float(str(raw))))
    except (ValueError, TypeError):
        return str(raw)


def parse_selected_redirects(values: list[str]) -> list[dict]:
    selected = []
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

        selected.append(
            {
                "old_entity": old_entity,
                "entity": entity,
                "dataset": dataset,
                "old_reference": str(row.get("old_reference", "") or ""),
                "new_reference": str(row.get("new_reference", "") or ""),
                "match_type": str(row.get("match_type", "") or ""),
                "notes": str(row.get("notes", "") or REDIRECT_NOTE),
            }
        )

    return selected
