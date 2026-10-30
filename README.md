# Config manager

## Getting started

`make dev-up` starts the full development stack in Docker, including the async
backend. The steps below are for running Config Manager directly on your machine.

Create a python virtualenv then run:

    make init

Create a local development postgres db

    createdb config_manager

Create or update db schema

    make upgrade-db

### Optional: work on the async backend locally

A separate checkout is useful when developing Config Manager and async together;
`make dev-up` does not need one. To use a separate backend, run Config Manager
with `make run` and start the backend from its own checkout:

```sh
cd ..
git clone https://github.com/digital-land/async-request-backend.git
cd async-request-backend
```

Before starting it, check that `request-api` publishes `127.0.0.1:8000:8000` in
its `docker-compose.yml`. With Docker running:

```sh
docker compose up -d --build
```

This builds the API and worker from your checkout and starts the database and
LocalStack. Rerun the command after backend changes to rebuild it. Run one backend
stack at a time to avoid port and container-name conflicts.

`make run` uses `http://localhost:8000` by default. You can also set the endpoint
explicitly in Config Manager's `.env`:

```dotenv
REQUEST_API_BASE_URL=http://localhost:8000
```

### Run config-manager

Return to the config-manager directory and run:

    make run

Transform pages use comparisons prepared by async when available. Older requests
fall back to a local comparison with a 10-second waiting limit.

`DevelopmentConfig` disables login and token signing for `make run` and
`make dev-up`. Production keeps authentication enabled.

`make dev-up` pulls the published async API image and sets
`AUTHENTICATION_ON=false`. To develop the backend too, use the optional checkout
steps above.

To test out github app integration locally you need to set the following in a `.env` file:

    GITHUB_CLIENT_ID=[the id]
    GITHUB_CLIENT_SECRET=[the secret]
    GITHUB_APP_ID=[github app id]
    GITHUB_APP_PRIVATE_KEY=[base64 encoded private key for github app]
    CONFIG_BRANCH=[the config repo brach this app pushes to]

Speak to @ashimali about where to find the above


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

## Frontend build

This project uses the [@digital-land-gov-uk/digital-land-frontend](https://github.com/digital-land/digital-land-frontend) npm package to manage frontend asset compilation and copying.

### How it works

The build process uses [nps (npm-package-scripts)](https://www.npmjs.com/package/nps) to organize build tasks. The base scripts come from digital-land-frontend, with project-specific overrides in [package-scripts.js](package-scripts.js).

### Build tools

- **SASS**: Compiles SCSS to CSS
- **Rollup**: Bundles custom JavaScript files
- **copyfiles**: Copies vendor assets (govuk-frontend, accessible-autocomplete)

### Build commands

Build all assets (runs automatically after `npm install`):
```bash
npm run build
```

Watch for changes during development:
```bash
npm run watch
```

Copy vendor assets:
```bash
npm run copy
```

### Generated files

All static assets are generated during build and output to `application/static/`:
- `application/static/stylesheets/` - Compiled CSS
- `application/static/javascripts/` - Bundled JS
- `application/static/images/` - Image assets
- `application/static/govuk/` - GOV.UK Frontend assets

These directories are git-ignored and should never be committed.

### Project-specific customizations

The [package-scripts.js](package-scripts.js) file extends digital-land-frontend with:
- Custom vendor asset copying (accessible-autocomplete, MOJ components)
- GOV.UK Frontend JavaScript bundle

### Digital Land Frontend Configuration

Frontend build paths are configured in [digital-land-frontend.config.json](digital-land-frontend.config.json). `govukOutputPath` is set to `application/static/` so GOV.UK Frontend fonts and images are copied to `application/static/govuk/assets/`, where the compiled styles expect them.
