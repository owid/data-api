# data-api

API for accessing data from our data catalog.

This project was generated via [manage-fastapi](https://ycd.github.io/manage-fastapi/). We might re-generate the project with a [different template](https://fastapi.tiangolo.com/advanced/templates/) based on our production requirements.

To run all the checks and make sure you have everything set up correctly, try

```
make test
```


## Crawler

Crawler is a script that goes through all backported datasets and replicates them to local DuckDB. It might be run as a [background task](https://fastapi.tiangolo.com/tutorial/background-tasks/) of an API in the future. Crawler creates tables `meta_datasets`, `meta_tables`, and `meta_variables` in DuckDB with all metadata and it also replicates tables from ETL catalog in there. Table names are underscored table paths, e.g. path `backport/owid/latest/dataset_941_technology_adoption__isard__1942__and_others/dataset_941_technology_adoption__isard__1942__and_others` gets table name `backport__owid__latest__dataset_941_technology_adoption__isard__1942__and_others__dataset_941_technology_adoption__isard__1942__and_others`. This is unnecessarily verbose, but it doesn't not matter now.

Crawler compares checksums of **datasets** to decide if a dataset needs to be updated. We cannot do it on a table level because we don't use table checksums.

We only crawl `garden` and `backport` channels right now.

Run `make crawl` to crawl the entire database (this would take veeeery long) or crawl only sample datasets with

```
python crawler/crawl.py --include 'dataset_941|ggdc_maddison'
```

or just a garden channel

```
python crawler/crawl.py --include 'garden'
```


## API

Copy `.env.example` into `.env` and update it as you like. After you build `duck.db` with crawler, run the API with `uvicorn app.main:app --reload`.

Docs are available at http://127.0.0.1:8000/v1/docs.

### Sample Queries

Sample queries written in [httpie](https://httpie.io/)

```
http GET http://127.0.0.1:8000/health
http GET http://127.0.0.1:8000/v1/variableById/data/42539
http GET http://127.0.0.1:8000/v1/variableById/metadata/42539
http GET http://127.0.0.1:8000/v1/dataset/data/garden/owid/latest/covid/covid.csv
http GET http://127.0.0.1:8000/v1/dataset/metadata/garden/ggdc/2020-10-01/ggdc_maddison/maddison_gdp
http GET http://127.0.0.1:8000/v1/dataset/data/backport/owid/latest/dataset_5576_ggdc_maddison__2020_10_01/dataset_5576_ggdc_maddison__2020_10_01.feather
http POST http://127.0.0.1:8000/v1/sql sql=="PRAGMA show_tables;" type==csv
http POST http://127.0.0.1:8000/v1/sql sql=="select * from garden__ggdc__2020_10_01__ggdc_maddison__maddison_gdp limit 10;" type==csv
```

## Tests

Integration tests work with sample data saved in `tests/sample_duck.db`. Regenerate it with `make testdb`.


## Development

It is useful to recreate sample DB for testing and run tests right after that for debugging with

```
make testdb && pytest -s tests/test_v1.py
```

## Full-text search

- all variables are given the same weight, we should reconsider that
- negation queries are not supported yet (could be useful for interactive exclusion of datasets)
