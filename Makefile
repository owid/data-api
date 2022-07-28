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
	@echo '  make run       Run API and Catalog in the background
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
	@echo '==> Copy .env.example to .env if missing'
	cp -n .env.example .env || true
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
	python crawler/crawl.py

api: .venv
	@echo '==> Running API'
	.venv/bin/hypercorn app.main:app --reload

testdb: .venv
	@echo '==> Rebuild test DB'
	rm -f tests/sample_duck.db
	python crawler/crawl.py --include 'dataset_941|ggdc_maddison' --duckdb-path tests/sample_duck.db

clobber: clean
	find . -name .venv | xargs rm -rf
	find . -name .mypy_cache | xargs rm -rf

run: .venv
	@echo 'Running API and Catalog in the background:'
	-kill $(lsof -t -i:8000)
	-kill $(lsof -t -i:8001)
	nohup make api > api.log 2> api.err < /dev/null &
	nohup python -m demo.demo > demo.log 2> demo.err < /dev/null &
