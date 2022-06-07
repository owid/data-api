# TODO: don't create full text search index yet, it can be an expensive operation

# log.info("create.full_text_search_index")
# _create_full_text_search_index(con)


def _create_full_text_search_index(con):
    # NOTE: path is a unique identifier of a table
    # NOTE: we include numbers
    con.execute(
        "PRAGMA create_fts_index('table_meta', 'path', '*', stopwords='english', overwrite=1, ignore='(\\.|[^a-z0-9])+')"
    )
