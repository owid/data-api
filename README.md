# data-api

API for accessing data from our data catalog.

This project was generated via [manage-fastapi](https://ycd.github.io/manage-fastapi/). We might re-generate the project with a [different template](https://fastapi.tiangolo.com/advanced/templates/) based on our production requirements.


## Crawler

Crawler is a script that goes through all backported datasets and replicates them to local DuckDB. It might be run as a [background task](https://fastapi.tiangolo.com/tutorial/background-tasks/) of an API in the future. Crawler creates tables `meta_tables` and `meta_variables` in DuckDB with all metadata and it also replicates tables from ETL catalog in there. Table names are underscored table paths, e.g. path `backport/owid/latest/dataset_941_technology_adoption__isard__1942__and_others/dataset_941_technology_adoption__isard__1942__and_others` gets table name `backport__owid__latest__dataset_941_technology_adoption__isard__1942__and_others__dataset_941_technology_adoption__isard__1942__and_others`. This is unnecessarily verbose, but it doesn't not matter now.

We don't crawl other channels than `backport` yet.

Usage:

```
python crawler/crawl_metadata.py
```

## API

Copy `.env.example` into `.env` and update it as you like. After you build `duck.db` with crawler, run the API with `uvicorn app.main:app --reload`.

Docs are available at http://127.0.0.1:8000/v1/docs.

### Sample Queries

- http GET http://127.0.0.1:8000/health
- http GET http://127.0.0.1:8000/v1/variableById/data/42539
- http GET http://127.0.0.1:8000/v1/variableById/metadata/42539


## Tests

Integration tests work with sample data saved in `tests/sample_duck.db`. Regenerate it with

```
rm tests/sample_duck.db
python crawler/crawl_metadata.py --include dataset_941 --duckdb-path tests/sample_duck.db
```
