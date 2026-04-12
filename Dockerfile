# ── Groovon Production Dockerfile ──────────────────────────────
# Multi-stage build for a slim Django + Celery image.
# Build:  docker build -t groovon .
# Run:    docker-compose up
# ───────────────────────────────────────────────────────────────

FROM python:3.12-slim AS base

# Prevent Python from writing .pyc files and enable unbuffered logs
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# System deps for psycopg (PostgreSQL) and general build tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gcc libpq-dev curl && \
    rm -rf /var/lib/apt/lists/*

# ── Dependencies ──────────────────────────────────────────────
WORKDIR /app

COPY webapp/requirements.txt /app/webapp/requirements.txt
RUN pip install --no-cache-dir -r webapp/requirements.txt

# ── Application code ──────────────────────────────────────────
# Copy the full project — engine, pipeline, sources, sinks are
# needed by the Celery worker tasks.
COPY . /app/

# ── Static files ──────────────────────────────────────────────
WORKDIR /app/webapp
RUN DJANGO_SETTINGS_MODULE=config.settings.base \
    DJANGO_SECRET_KEY=build-only-collectstatic \
    python manage.py collectstatic --noinput

# ── Runtime ───────────────────────────────────────────────────
EXPOSE 8000

# Default entrypoint: the startup script handles migrate + gunicorn
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

CMD ["/app/start.sh"]
