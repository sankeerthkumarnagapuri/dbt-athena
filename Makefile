include .env
export

CHANGED_FILES := $(shell git ls-files --modified --other --exclude-standard)
CHANGED_FILES_IN_BRANCH := $(shell git diff --name-only $(shell git merge-base origin/main HEAD))

.PHONY : install_deps setup pre-commit pre-commit-in-branch pre-commit-all test help

install_deps:  ## Install all dependencies.
	pip install -r dev-requirements.txt
	pip install -e .

setup:  ## Install all dependencies and setup pre-commit
	make install_deps
	pre-commit install
	make .env

.env:  ## Generate .env file
	@cp .env.example $@

test:  ## Run tests.
	make unit_test
	make functional_test

unit_test:  ## Run unit tests.
	pytest --cov=dbt --cov-report=html:htmlcov tests/unit

functional_test: .env  ## Run functional tests.
	pytest -n auto tests/functional

pre-commit:  ## check modified and added files (compared to last commit!) with pre-commit.
	pre-commit run --files $(CHANGED_FILES)

pre-commit-in-branch:  ## check changed since origin/main files with pre-commit.
	pre-commit run --files $(CHANGED_FILES_IN_BRANCH)

pre-commit-all:  ## Check all files in working directory with pre-commit.
	pre-commit run --all-files

help:  ## Show this help.
	@egrep -h '\s##\s' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m  %-30s\033[0m %s\n", $$1, $$2}'

ca-login:
	aws codeartifact login --tool pip --repository mdata --domain mtx --domain-owner 955578949754 --region us-east-1

ca-geturl:
	aws codeartifact get-repository-endpoint --domain mtx --domain-owner 955578949754 --repository mdata --format pypi --region us-east-1

ca-build:
	# python -m pip install --upgrade build
	python -m build

ca-upload:
	# python -m pip install --upgrade twine
	$(eval AUTH_TOKEN=$(shell aws codeartifact get-authorization-token --domain mtx --domain-owner 955578949754 --region us-east-1 --query 'authorizationToken' --output text))
	# @echo "Using authorization token: $(AUTH_TOKEN)"
	twine upload --repository-url https://mtx-955578949754.d.codeartifact.us-east-1.amazonaws.com/pypi/mdata/ --verbose --username aws --password $(AUTH_TOKEN) dist/*

ca-pip-extra:
	export PIP_EXTRA_INDEX_URL=https://pypi.org/simple/
	export PIP_EXTRA_INDEX_URL=https://aws:$(aws codeartifact get-authorization-token --domain mtx --domain-owner 955578949754 --region us-east-1 --query authorizationToken --output text)@mtx-955578949754.d.codeartifact.us-east-1.amazonaws.com/pypi/mdata/simple/
	