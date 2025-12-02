FROM python:3.10-slim

# Working dir
WORKDIR /app

ENV FLASK_CONFIG=config.DevelopmentConfig
ENV FLASK_APP=application.wsgi:app
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=5000
ENV FLASK_DEBUG=1

# install system deps
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    postgresql-client rsync make \
    git curl build-essential libpq-dev bash && \
    curl -fsSL https://deb.nodesource.com/setup_16.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /venv

ENV PATH="/venv/bin:$PATH"

# Copy application source
COPY . .

# Install deps
RUN mkdir -p application/static/javascripts/govuk application/static/stylesheets && \
    npm install && \
    npm run copy || true && \
    curl -o application/static/javascripts/accessible-autocomplete.min.js https://unpkg.com/accessible-autocomplete@2.0.4/dist/accessible-autocomplete.min.js && \
    curl -o application/static/stylesheets/accessible-autocomplete.min.css https://unpkg.com/accessible-autocomplete@2.0.4/dist/accessible-autocomplete.min.css && \
    curl -o application/static/javascripts/govuk/govuk-frontend.js https://unpkg.com/govuk-frontend@5.9.0/dist/govuk-frontend.min.js && \
    curl -o application/static/javascripts/digital-land-frontend.js https://raw.githubusercontent.com/digital-land/digital-land-frontend/main/dist/digital-land-frontend.js || echo 'DL Frontend not available' && \
    python -m pip install --upgrade pip setuptools wheel && \
    python -m pip install -r requirements.txt

EXPOSE 5000

ENTRYPOINT ["sh", "-c", "flask db upgrade && flask run"]
