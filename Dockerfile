FROM python:3.10-alpine

RUN apk add --no-cache \
    git \
    proj \
    proj-util \
    proj-dev \
    gcc \
    g++ \
    musl-dev \
    geos \
    geos-dev \
    libpq \
    nodejs \
    npm \
    rsync

ENV PROJ_DIR=/usr
ENV PORT=80

# We copy only the requirements files across here,
# meaning we can avoid rebuilding packages if only
# the application code has changed.
COPY ./requirements /app/requirements
WORKDIR /app
RUN pip install --upgrade pip
RUN pip install \
    --no-cache-dir \
    --extra-index-url https://alpine-wheels.github.io/index \
    --requirement requirements/requirements.txt

COPY . /app

ENV FLASK_ENV=production
ENV FLASK_CONFIG=config.Config
ENV FLASK_APP=application.wsgi:app

EXPOSE $PORT

RUN npm install

# TODO Instrument gunicorn and create seperate targets for production and dev
# CMD flask db upgrade && gunicorn -b 0.0.0.0:$PORT application.wsgi:app --timeout 120 --workers=2 --threads=4 --worker-class=gthread
CMD flask db upgrade && flask run -p $PORT -h "0.0.0.0"
