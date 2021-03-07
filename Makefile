checkfiles = asyncmy/ tests/ benchmark/ conftest.py
black_opts = -l 100 -t py38
py_warn = PYTHONDEVMODE=1
MYSQL_PASS ?= "123456"

up:
	@poetry update

deps:
	@poetry install

style: deps
	isort -src $(checkfiles)
	black $(black_opts) $(checkfiles)

check: deps
	black --check $(black_opts) $(checkfiles) || (echo "Please run 'make style' to auto-fix style issues" && false)
	flake8 $(checkfiles)
	bandit -x tests,benchmark -r $(checkfiles)

test: deps
	$(py_warn) MYSQL_PASS=$(MYSQL_PASS) pytest

build: deps
	@poetry build

ci: check test
