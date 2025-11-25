init:
	mkdir -p application/static/javascripts
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
	@echo "Running Unit test...."
	@echo "Not yet implemented"

test-integration:
	@echo "Running Integration test...."
	@echo "Not yet implemented"

test-acceptance:
	@echo "Running Acceptance test...."
	@echo "Not yet implemented"

test-accessibility:
	@echo "Running Accessibility test...."
	@echo "Not yet implemented"
