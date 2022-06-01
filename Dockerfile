FROM python:3.10-alpine

RUN apk add --no-cache git proj proj-util proj-dev gcc g++ musl-dev geos geos-dev libpq

ENV PROJ_DIR=/usr

# We copy only the requirements files across here,
# meaning we can avoid rebuilding packages if only
# the application code has changed.
COPY ./requirements.txt /app/requirements.txt
COPY ./requirements /app/requirements
WORKDIR /app
RUN pip install --upgrade pip
RUN pip install \
    --no-cache-dir \
    --extra-index-url https://alpine-wheels.github.io/index \
    --requirement requirements.txt

COPY . /app

ENV FLASK_ENV=production
ENV FLASK_CONFIG=config.Config
ENV FLASK_APP=application.wsgi:app

EXPOSE $PORT
CMD flask db upgrade && gunicorn -b 0.0.0.0:$PORT application.wsgi:app --timeout 120 --workers=2 --threads=4 --worker-class=gthread
