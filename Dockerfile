FROM python:3.10-slim

# OS packages & Node JS setup # --fix-missing ca-certificates
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    postgresql-client rsync \
    make git curl gnupg build-essential libpq-dev bash graphviz && \
    curl -fsSL https://deb.nodesource.com/setup_16.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    rm -rf /var/lib/apt/lists/*

# Working dir
WORKDIR /app

# Suppress Node warnning/deprecations
# Todo: TO BE REMOVED LATER
RUN npm config set update-notifier false && \
    npm config set strict-ssl false

# Create & activate virtualenv
RUN python3 -m venv /venv

# Todo: Some might need TO BE REMOVED LATER
ENV PATH="/venv/bin:$PATH"
ENV NODE_NO_WARNINGS=1
ENV NO_UPDATE_NOTIFIER=1
# Skip verification
ENV NODE_TLS_REJECT_UNAUTHORIZED=0
ENV GIT_SSL_NO_VERIFY=1

# Copy code
COPY . .
COPY entrypoint.sh /usr/local/bin/

RUN chmod +x /usr/local/bin/entrypoint.sh && \
    make init

ENTRYPOINT [ "entrypoint.sh" ]
