#!/bin/sh
set -e

cd /app/webapp

echo "🔄 Running migrations..."
python manage.py migrate --noinput

echo "👤 Ensuring superuser..."
python manage.py ensure_superuser || true

# If CONTAINER_ROLE=worker, start Celery instead of Gunicorn
if [ "$CONTAINER_ROLE" = "worker" ]; then
    echo "🏭 Starting Celery Worker..."
    exec celery -A config.celery_app worker \
        -l info \
        --concurrency=${CELERY_CONCURRENCY:-2}
else
    echo "🚀 Starting Gunicorn..."
    exec gunicorn config.wsgi:application \
        --bind 0.0.0.0:${PORT:-8000} \
        --workers ${GUNICORN_WORKERS:-3} \
        --timeout ${GUNICORN_TIMEOUT:-120} \
        --access-logfile - \
        --error-logfile -
fi
