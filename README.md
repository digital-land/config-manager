# Data manager

## Getting started

Create a python virtualenv then run:

    make init

Create a local development postgres db

    createdb data_manager

Create or update db schema

    make upgrade-db

Load data

    make load-data

Drop local data

    make drop-data

To run the application run:

    make run


To test out github oauth2 locally you need to set the following in a `.env` file:

    GITHUB_CLIENT_ID=[the id]
    GITHUB_CLIENT_SECRET=[the secret]


## Adding new python packages to the project

This project uses pip-tools to manage requirements files. [https://pypi.org/project/pip-tools/](https://pypi.org/project/pip-tools/)

When using fresh checkout of this repository, then make init will take care of the initial of packages from the checked
in requirements and dev-requirements files.

These instructions are only for when you add new libraries to the project.

To add a production dependency to the main aapplication, add the package to the [requirements.in](requirements.in)
file.

Then run the piptools compile command on the "in" file:

    python -m piptools compile requirements/requirements.in

That will generate a new requirements.txt file in the requirements directory.


To add a development library, add a line to [dev-requirements.in](dev-requirements.in).

Note that the first line of that file is:

"-r requirements.txt" which constrains the versions of dependencies in the requirements.txt file generated in previous step.

    python -m piptools compile requirements/dev-requirements.in

Then to install all the packages run:

    python -m piptools sync requirements/requirements.txt requirements/dev-requirements.txt
