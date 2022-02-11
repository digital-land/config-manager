init::
	python -m pip install --upgrade pip
	python -m pip install pip-tools
	python -m piptools sync requirements/requirements.txt requirements/dev-requirements.txt
	python -m pip install -r requirements/dev-requirements.txt
	python -m pre_commit install
	npm install

black:
	black .

black-check:
	black --check .

flake8:
	flake8 --exclude .venv,node_modules

lint: black-check flake8

run::
	flask run

watch:
	npm run watch
