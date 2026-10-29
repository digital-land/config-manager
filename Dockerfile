ARG NODE_VERSION=24.22.1
FROM node:${NODE_VERSION}-trixie-slim AS node

# Build frontend assets (postinstall runs the copy and build scripts into application/static)
FROM node AS frontend

WORKDIR /app

COPY package.json package-lock.json package-scripts.js rollup.config.js digital-land-frontend.config.json ./
COPY src ./src

RUN npm ci

# Runtime image shared by dev and production
FROM python:3.13-slim-trixie AS base

# Working dir
WORKDIR /app

ENV FLASK_CONFIG=config.Config
ENV FLASK_APP=application.wsgi:app
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=5000
ENV FLASK_DEBUG=0

# install system deps
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    postgresql-client rsync make curl bash && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies (git is only needed to fetch git+ requirements)
COPY requirements/requirements.txt requirements/requirements.txt
RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    python -m pip install --no-cache-dir --upgrade pip setuptools wheel && \
    python -m pip install --no-cache-dir -r requirements/requirements.txt && \
    apt-get purge -y --auto-remove git && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd --system appuser && \
    useradd --system --gid appuser --create-home appuser && \
    chown appuser:appuser /app

# Copy application source and built frontend assets
COPY --chown=appuser:appuser . .
COPY --from=frontend --chown=appuser:appuser /app/application/static ./application/static

EXPOSE 5000

# Migrate once before starting the server; Compose overrides CMD for local work.
ENTRYPOINT ["sh", "./docker-entrypoint.sh"]
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "2", "--threads", "4", "--access-logfile", "-", "application.wsgi:app"]

# Local development image (docker-compose.yaml) - adds Node.js for npm ci and asset watching
FROM base AS dev

# chokidar-cli uses SHELL when running asset rebuild commands.
ENV SHELL=/bin/bash

RUN apt-get update && \
    apt-get install -y --no-install-recommends libstdc++6 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm && \
    ln -s ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx

USER appuser

# Production image - must remain the last stage so a plain `docker build` produces it
FROM base AS production

USER appuser
