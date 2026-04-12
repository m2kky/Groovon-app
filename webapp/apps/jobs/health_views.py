"""Health check endpoint for Coolify / load-balancer monitoring."""

from django.db import connection
from django.http import JsonResponse


def health_check(request):
    """Return 200 OK when the application is alive and the DB is reachable."""
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
        db_ok = True
    except Exception:
        db_ok = False

    status = "ok" if db_ok else "degraded"
    code = 200 if db_ok else 503

    return JsonResponse(
        {"status": status, "database": db_ok},
        status=code,
    )
