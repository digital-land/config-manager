FROM python:3.10-slim

# Working dir
WORKDIR /app

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
RUN npm install && \
	python -m pip install --upgrade pip setuptools wheel && \
	python -m pip install -r requirements.txt

ENTRYPOINT ["sh", "-c", "flask db upgrade && exec gunicorn --bind 0.0.0.0:5000 application.wsgi:app"]
