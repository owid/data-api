from pathlib import Path

import duckdb
import structlog
import typer

log = structlog.get_logger()


def main(
    duckdb_path: Path = Path("duck.db"),
) -> None:
    assert duckdb_path.exists(), "DuckDB database path does not exist"

    log.info("table.full_text_index.start")

    con = duckdb.connect(duckdb_path.as_posix())
    cols = [
        "title",
        "description",
        "path",
        "unit",
        "short_name",
    ]
    _create_full_text_search_index(con, "meta_variables", "path", cols)
    log.info("table.full_text_index.end")


def _create_full_text_search_index(
    con, table_name: str, primary_key: str, columns: list[str] = ["*"]
):
    # NOTE: path is a unique identifier (primary key probably)
    # NOTE: we include numbers (for SDG goals for instance)
    cols_to_index = ",".join([f"'{c}'" for c in columns])
    con.execute(
        f"""PRAGMA create_fts_index(
            '{table_name}',
            '{primary_key}',
            {cols_to_index},
            stopwords='english',
            overwrite=1,
            ignore='(\\.|[^a-z0-9])+')"""
    )


if __name__ == "__main__":
    typer.run(main)
