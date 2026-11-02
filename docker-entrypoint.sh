#!/bin/sh
set -e

flask db upgrade

# Replace the shell so Gunicorn receives container shutdown signals directly.
exec "$@"
