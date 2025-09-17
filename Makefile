.DEFAULT_GOAL := help

help:
	@echo
	@echo "    ____  __                  _                ____        __       "
	@echo "   / __ \/ /___ _____  ____  (_)___  ____ _   / __ \____ _/ /_____ _"
	@echo "  / /_/ / / __ \`/ __ \/ __ \/ / __ \/ __ \`/  / / / / __ \`/ __/ __ \`/"
	@echo " / ____/ / /_/ / / / / / / / / / / / /_/ /  / /_/ / /_/ / /_/ /_/ / "
	@echo "/_/   /_/\__,_/_/ /_/_/ /_/_/_/ /_/\__, /  /_____/\__,_/\__/\__,_/  "
	@echo "                                  /____/                            "
	@echo
	@echo "Usage: make <action>"
	@echo
	@cat $(MAKEFILE_LIST) | grep -E '^[a-zA-Z_-]+:.*?## .*$$' | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-10s\033[0m %s\n", $$1, $$2}'
	@echo

.PHONY: init
init:: ## Install python and node.js dependencies
	python -m pip install --upgrade pip
	python -m pip install pip-tools
	python -m piptools sync requirements/dev-requirements.txt requirements/requirements.txt
	python -m pre_commit install
	npm install

.PHONY: reqs
reqs: ## Install python dependencies
	python -m piptools compile requirements/dev-requirements.in
	python -m piptools compile requirements/requirements.in

.PHONY: sync
sync: ## Sync python dependencies if requirements change
	python -m piptools sync requirements/requirements.txt requirements/dev-requirements.txt

.PHONY: upgrade
upgrade: ## Upgrade python dependencies
	python -m piptools compile --upgrade requirements/dev-requirements.in
	python -m piptools compile --upgrade requirements/requirements.in
	python -m piptools sync requirements/requirements.txt requirements/dev-requirements.txt

.PHONY: black
black: ## Format code with black
	black .

.PHONY: black-check
black-check: ## Check code format with black
	black --check .

.PHONY: flake8
flake8: ## Lint code with flake8
	flake8 .

.PHONY: isort
isort: ## Implement isort to sort imports
	isort --profile black .

.PHONY: lint
lint: black-check flake8 ## Lint code with black and flake8

.PHONY: run
run:: ## Run app locally
	flask run

.PHONY: watch
watch: ## Run app with auto-reload on code changes
	npm run watch

.PHONY: upgrade-db
upgrade-db: ## Upgrade the database to the latest migration
	@echo "Now running 'upgrade-db'....."
	flask db upgrade

.PHONY: downgrade-db
downgrade-db: ## Downgrade the database to the previous migration
	flask db downgrade

.PHONY: load-data
load-data: ## Load sample data into the database
	flask data load --spec 1 --config 1

.PHONY: drop-data
drop-data: ## Drop all data from the database
	flask data drop

.PHONY: test-unit
test-unit: ## Run unit tests
	@echo "Running Unit test...."
	@echo "Not yet implemented"

.PHONY: test-integration
test-integration: ## Run integration tests
	@echo "Running Integration test...."
	@echo "Not yet implemented"

.PHONY: test-acceptance
test-acceptance: ## Run acceptance testsß
	@echo "Running Acceptance test...."
	@echo "Not yet implemented"
