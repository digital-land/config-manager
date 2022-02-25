def datasets_string_filter(datasets):
    return ", ".join([dataset.get("name") for dataset in datasets])
