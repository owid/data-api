#
#  Makefile
#

.PHONY: etl

include default.mk

SRC = app crawler tests

help:
	@echo 'Available commands:'
	@echo
	@echo '  make crawl     Crawl ETL catalog'
	@echo '  make api       Run API server'
	@echo '  make test      Run all linting and unit tests'
	@echo '  make testdb    Rebuild test DB'
	@echo '  make watch     Run all tests, watching for changes'
	@echo '  make clobber   Delete non-reference data and .venv'
	@echo


watch-all:
	.venv/bin/watchmedo shell-command -c 'clear; make unittest; (cd vendor/owid-catalog-py && make unittest)' --recursive --drop .

test-all: test
	cd vendor/owid-catalog-py && make test

watch: .venv
	.venv/bin/watchmedo shell-command -c 'clear; make check-formatting lint check-typing coverage' --recursive --drop .

.submodule-init:
	@echo '==> Initialising submodules'
	git submodule update --init
	touch $@

.venv: pyproject.toml poetry.toml poetry.lock .submodule-init
	@echo '==> Installing packages'
	poetry install
	# poetry freezes when downloading orjson for some reason, so we need to install it manually
	# try to fix this in the future
	poetry run pip install orjson
	touch $@

check-typing: .venv
	# @echo '==> Checking types'
	# .venv/bin/mypy $(SRC)
	@echo '==> WARNING: Checking types is disabled!'

coverage: .venv
	@echo '==> Unit testing with coverage'
	.venv/bin/pytest --cov=app --cov-report=term-missing tests

crawl: .venv
	@echo '==> Crawl ETL catalog'
	python crawler/crawl_metadata.py

api: .venv
	@echo '==> Running API'
	.venv/bin/uvicorn app.main:app --reload

testdb: .venv
	@echo '==> Rebuild test DB'
	rm -f tests/sample_duck.db
	python crawler/crawl_metadata.py --include 'dataset_941|ggdc_maddison' --duckdb-path tests/sample_duck.db

clobber: clean
	find . -name .venv | xargs rm -rf
	find . -name .mypy_cache | xargs rm -rf
