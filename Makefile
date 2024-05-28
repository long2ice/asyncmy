checkfiles = asyncmy/ tests/ examples/ conftest.py build.py
py_warn = PYTHONDEVMODE=1
MYSQL_PASS ?= "123456"

up:
	@poetry update

deps:
	@poetry install

_style:
	@isort -src $(checkfiles)
	@black $(checkfiles)

style: deps _style

_check:
	@black --check $(checkfiles) || (echo "Please run 'make style' to auto-fix style issues" && false)
	@ruff check $(checkfiles)
	@mypy $(checkfiles)

check: deps _check

_test:
	$(py_warn) MYSQL_PASS=$(MYSQL_PASS) pytest

test: deps _test

clean:
	@rm -rf *.so && rm -rf build && rm -rf dist && rm -rf asyncmy/*.c && rm -rf asyncmy/*.so && rm -rf asyncmy/*.html

build: clean
	@poetry build

benchmark: deps
	@python benchmark/main.py

ci: deps _check _test
