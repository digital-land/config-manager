import json

import jsonpickle


def datasets_string_filter(datasets):
    s = ""
    for d in datasets:
        s = s + d.name + ";"
    return s


def debug(thing):
    return f"<script>console.log({json.dumps(json.loads(jsonpickle.encode(thing)), indent=2)});</script>"
