def generate_sql_where_str(filters, mappings={}):
    if len(filters.keys()) == 0:
        return ""
    clauses = []
    for filter, value in filters.items():
        column = filter
        if filter in mappings.keys():
            column = mappings[filter]
        clauses.append("{} LIKE :{}".format(column, filter))
    return "WHERE " + " AND ".join(clauses)
