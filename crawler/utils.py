def sanitize_table_path(path):
    # NOTE: version can contain - in dates (e.g. 2020-10-01)
    return path.replace("/", "__").replace("-", "_")
