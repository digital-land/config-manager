init:
	npm install
	python -m pip install --upgrade pip setuptools wheel
	python -m pip install -r requirements.txt

reqs:
	python -m piptools compile requirements/dev-requirements.in
	python -m piptools compile requirements/requirements.in

sync:
	python -m piptools sync requirements/requirements.txt requirements/dev-requirements.txt

upgrade:
	python -m piptools compile --upgrade requirements/dev-requirements.in
	python -m piptools compile --upgrade requirements/requirements.in
	python -m piptools sync requirements/requirements.txt requirements/dev-requirements.txt

black:
	black .

black-check:
	black --check .

flake8:
	flake8 .

isort:
	isort --profile black .

lint: black-check flake8

run::
	flask run

watch:
	npm run watch

upgrade-db:
	@echo "Now running 'upgrade-db'....."
	flask db upgrade

downgrade-db:
	flask db downgrade

load-data:
	flask data load --spec 1 --config 1

drop-data:
	flask data drop

test-unit:
	@echo "Running Unit tests...."
	python -m pytest tests/unit/ -v

test-integration:
	@echo "Running Integration tests...."
# 	@echo "Not yet implemented"
	python -m pytest tests/integration/ -v

test-acceptance:
	@echo "Running Acceptance tests...."
# 	@echo "Not yet implemented"
	python -m pytest tests/acceptance/ -v

test-accessibility:
	@echo "Running Accessibility tests...."
	@echo "Not yet implemented"

test:: test-coverage
# 	@echo "Running all tests...."
# 	python -m pytest tests/ -v

test-coverage:: coverage-unit coverage-integration coverage-acceptance

coverage-unit:
	pytest --cov=application.blueprints.datamanager tests/unit/ -v

coverage-integration:
	pytest --cov=application.blueprints.datamanager --cov-append --cov-fail-under=80 tests/integration/ -v

coverage-acceptance:
	pytest --cov=application.blueprints.datamanager --cov-append --cov-fail-under=80 tests/acceptance/ -v

