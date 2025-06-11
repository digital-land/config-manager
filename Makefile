init::
	python -m pip install --upgrade pip
	python -m pip install pip-tools git+https://github.com/digital-land/digital-land-frontend.git#egg=digital_land_frontend
	python -m piptools compile requirements/dev-requirements.in
	python -m piptools compile requirements/requirements.in
	python -m piptools sync requirements/dev-requirements.txt requirements/requirements.txt
	python -m pre_commit install
	npm install

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
	flask db upgrade

downgrade-db:
	flask db downgrade

load-data:
	flask data load --spec 1 --config 1

drop-data:
	flask data drop
