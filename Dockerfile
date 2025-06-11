FROM python:3.10-slim

# OS packages & Node JS setup
RUN apt-get update && \
    apt-get install -y --no-install-recommends postgresql-client \
    make rsync git curl gnupg build-essential libpq-dev bash && \
    curl -fsSL https://deb.nodesource.com/setup_16.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    rm -rf /var/lib/apt/lists/*

# Working dir
WORKDIR /app

# Disable pip caching
ENV PIP_NO_CACHE_DIR=1

# Suppress Node warnning/deprecations
ENV NODE_NO_WARNINGS=1

# Create & activate virtualenv
RUN python3 -m venv /venv
ENV PATH="/venv/bin:$PATH"

# Copy code
COPY . .
COPY entrypoint.sh /usr/local/bin/
RUN make init && chmod +x /usr/local/bin/entrypoint.sh

# TODO: might not be needed
RUN npm install --no-save concurrently

ENTRYPOINT [ "entrypoint.sh" ]
