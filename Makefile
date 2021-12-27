checkfiles = asyncmy/ tests/ examples/ conftest.py build.py
py_warn = PYTHONDEVMODE=1
MYSQL_PASS ?= "123456"

up:
	@poetry update

deps: 
	@poetry install

style: deps
	@isort -src $(checkfiles)
	@black $(checkfiles)

check: deps
	@black --check $(checkfiles) || (echo "Please run 'make style' to auto-fix style issues" && false)
	@flake8 $(checkfiles)
	@bandit -x tests -r $(checkfiles)
	@mypy $(checkfiles)

test: deps
	$(py_warn) MYSQL_PASS=$(MYSQL_PASS) pytest

clean:
	@rm -rf *.so && rm -rf build && rm -rf dist && rm -rf asyncmy/*.c && rm -rf asyncmy/*.so

build:
	@pip install cython
	@poetry build

benchmark: deps
	@python benchmark/main.py

ci: check test
