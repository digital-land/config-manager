def datasets_string_filter(datasets):
    s = ""
    for d in datasets:
        s = s + d.name + ";"
    return s
