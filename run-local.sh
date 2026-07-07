#!/usr/bin/env bash
# Run the API against the docker-compose db/redis containers, without
# needing the api/worker/beat/etc. services themselves in Docker.
#
# Why this exists: .env's DATABASE_HOST=db / REDIS_URL=redis://redis:6379
# are Docker Compose service names — they only resolve for containers on
# the compose network. A bare `uvicorn main:app` on the host hits
# `socket.gaierror: Temporary failure in name resolution` on "db". This
# script points at the same db/redis containers via their host-exposed
# ports (docker-compose.yml: db 5432->54322, redis 6379->63799) instead.
#
# Requires: `docker compose up -d db redis` already running (or `docker ps`
# shows pios-backend-main_db_1 / _redis_1 healthy), and .venv/ set up
# (pip install -r requirements.txt).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

export DATABASE_HOST=localhost
export DATABASE_PORT=54322
export DATABASE_PASSWORD='Pwd@123'
export REDIS_URL='redis://localhost:63799/0'
export CELERY_BROKER_URL='redis://localhost:63799/0'
export CELERY_RESULT_BACKEND='redis://localhost:63799/1'

exec .venv/bin/uvicorn main:app --host 0.0.0.0 --port 9000 --reload
