FROM python:3.10-slim

# Working dir
WORKDIR /app

# install system deps
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    postgresql-client \
    git curl build-essential libpq-dev bash && \
    curl -fsSL https://deb.nodesource.com/setup_16.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /venv \
&& /venv/bin/pip install --upgrade pip setuptools wheel

ENV PATH="/venv/bin:$PATH"

# Copy dependency manifests
COPY requirements/requirements.txt .
COPY package*.json .

# Install Python deps, build Node assets, then prune devDependencies
RUN /venv/bin/pip install --no-cache-dir -r requirements.txt && \
  npm ci && \
  npm run postinstall && \
  npm prune

# Copy application source
COPY . .

RUN chmod +x entrypoint.sh

ENTRYPOINT ["sh", "entrypoint.sh"]

CMD ["gunicorn", "--bind", "0.0.0.0:5000", "application.wsgi:app"]
