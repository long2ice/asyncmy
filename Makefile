checkfiles = asyncmy/ tests/ examples/ conftest.py build.py
py_warn = PYTHONDEVMODE=1
MYSQL_PASS ?= "123456"

up:
	@uv lock --upgrade

deps:
	uv sync --active --inexact --all-groups --all-extras

_style:
	@ruff format $(checkfiles)
	@ruff check --fix $(checkfiles)

style: deps _style

_check:
	@ruff format --check $(checkfiles) || (echo "Please run 'make style' to auto-fix style issues" && false)
	@ruff check $(checkfiles)
	@mypy $(checkfiles)

check: deps _check

_test:
	$(py_warn) MYSQL_PASS=$(MYSQL_PASS) pytest

test: deps _test

clean:
	@rm -rf *.so && rm -rf build && rm -rf dist && rm -rf asyncmy/*.c && rm -rf asyncmy/*.so && rm -rf asyncmy/*.html

build: clean
	@uv build

benchmark: deps
	@python benchmark/main.py

ci: build _check _test
