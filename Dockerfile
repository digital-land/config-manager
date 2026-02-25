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

# Install Node.js dependencies and build assets
RUN npm install

# Install Python dependencies
RUN python -m pip install --upgrade pip setuptools wheel && \
    python -m pip install -r requirements/requirements.txt


EXPOSE 5000

ENTRYPOINT ["sh", "-c", "flask db upgrade && flask run"]
