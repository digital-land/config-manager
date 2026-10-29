# Config manager

## Getting started

Select Node.js 24.22.1 (used for frontend tooling) before installing dependencies:

```sh
nvm install
nvm use
```

Create a python virtualenv then run:

    make init

Create a local development postgres db

    createdb config_manager

Create or update db schema

    make upgrade-db

To run the application run:

    make run


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

This project uses the [@planning-data/digital-land-frontend](https://github.com/digital-land/digital-land-frontend) npm package to manage frontend asset compilation and copying.

`make init` uses `npm install`; CI and Docker use `npm ci`. Both run the
post-install asset build. After changing
dependencies, run `npm install` with the pinned Node version and commit the
updated `package-lock.json` alongside `package.json`.

CI and deployment builds read `.nvmrc`. The main Dockerfile and
`docker-compose.yaml` default to Node 24.22.1. Keep these defaults, `.nvmrc`,
and the package engine requirements aligned when upgrading. Both the frontend
and dev Docker stages use the same official Node image; production uses Python
and contains only the generated frontend assets, without Node or `node_modules`.

To explicitly use the pinned version for Docker or Compose:

```sh
docker build --build-arg NODE_VERSION="$(cat .nvmrc)" -t config-manager .
NODE_VERSION="$(cat .nvmrc)" docker compose up --build
```

The separate `Dockerfile.dev` used by `make dev-up` and
`docker-compose-dev.yaml` is a legacy setup and is not covered by this upgrade.

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
- Override for govuk-frontend v5 asset paths (fixes compatibility issue)
- Custom vendor asset copying (accessible-autocomplete, MOJ components)
- GOV.UK Frontend JavaScript bundle

See the comments in package-scripts.js for details on why these overrides are necessary.

### Digital Land Frontend Configuration

Frontend build paths are configured in [digital-land-frontend.config.json](digital-land-frontend.config.json).
