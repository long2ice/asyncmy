checkfiles = asyncmy/ tests/ examples/ conftest.py build.py
py_warn = PYTHONDEVMODE=1
MYSQL_PASS ?= "123456"

up:
	@poetry update

deps: 
	@poetry install

style: deps
	poetry run isort -src $(checkfiles)
	poetry run black $(checkfiles)

check: deps
	poetry run black --check $(checkfiles) || (echo "Please run 'make style' to auto-fix style issues" && false)
	poetry run flake8 $(checkfiles)
	poetry run bandit -x tests -r $(checkfiles)
	poetry run mypy $(checkfiles)

test: deps
	$(py_warn) MYSQL_PASS=$(MYSQL_PASS) poetry run pytest

clean:
	@rm -rf *.so && rm -rf build && rm -rf dist && rm -rf asyncmy/*.c && rm -rf asyncmy/*.so

build: deps
	@poetry build

benchmark: deps
	python benchmark/main.py

ci: check test
