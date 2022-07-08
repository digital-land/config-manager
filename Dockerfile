FROM python:3.10 as base

RUN set -ex; \
    apt-get update; \
    apt-get upgrade --assume-yes; \
    apt-get install --assume-yes  \
        build-essential \
        curl \
        git \
        python3.10 \
        python3-pip \
        rsync

ENV PORT=80

# We copy only the requirements files across here,
# meaning we can avoid rebuilding packages if only
# the application code has changed.
COPY ./requirements /app/requirements
WORKDIR /app
RUN pip install --upgrade pip
RUN pip install \
    --no-cache-dir \
    --requirement requirements/requirements.txt

COPY . /app
RUN set -ex; \
  curl -s https://deb.nodesource.com/setup_16.x | bash;\
  apt-get install --assume-yes nodejs; \
  npm install;

ENV FLASK_CONFIG=config.Config
ENV FLASK_APP=application.wsgi:app

EXPOSE $PORT

RUN npm install

FROM base AS development
ENV FLASK_ENV=development
RUN pip install \
    --no-cache-dir \
    --editable \
    .[dev]
CMD flask db upgrade && flask run -p $PORT -h 0.0.0.0

FROM base AS live
ENV FLASK_ENV=production
RUN rm -rf tests
RUN set -ex; \
  apt-get remove --assume-yes \
    nodejs \
    build-essential; \
  apt-get clean; \
  apt-get autoclean --assume-yes
CMD gunicorn -b 0.0.0.0:$PORT application.wsgi:app --timeout 120 --workers=2 --threads=4 --worker-class=gthread
