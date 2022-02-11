init::
	pip install --upgrade pip setuptools
	pip install -r requirements.txt
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
