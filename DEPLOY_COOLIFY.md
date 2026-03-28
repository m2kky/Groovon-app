# Deploy Groovon on Coolify

This guide assumes your code is in this repo and you want a production deployment.

## 1) Environment Variables

Use the file below as your source template:

- `/.env.production.example`

In Coolify, set these variables for both **web** and **worker** services:

- `DJANGO_SETTINGS_MODULE=config.settings.production`
- `DJANGO_SECRET_KEY=<strong-random-secret>`
- `DJANGO_ALLOWED_HOSTS=your-domain.com,www.your-domain.com`
- `DJANGO_CSRF_TRUSTED_ORIGINS=https://your-domain.com,https://www.your-domain.com`
- `DJANGO_DEBUG=False`
- `CELERY_BROKER_URL=redis://redis:6379/0`
- `CELERY_RESULT_BACKEND=redis://redis:6379/0`
- `GROOVON_INSECURE_SSL=False`
- API keys you need (`GOOGLE_API_KEY`, `SPOTIFY_*`, etc.)

Superuser bootstrap variables (also in Coolify env):

- `DJANGO_SUPERUSER_USERNAME`
- `DJANGO_SUPERUSER_EMAIL`
- `DJANGO_SUPERUSER_PASSWORD`
- `DJANGO_SUPERUSER_UPDATE_PASSWORD=False`

## 2) Create Services in Coolify

## 2.1 Web Service

- **Build Command**
```bash
pip install -r webapp/requirements.txt
```

- **Start Command**
```bash
cd webapp && python manage.py migrate && python manage.py collectstatic --noinput && python manage.py ensure_superuser && gunicorn config.wsgi:application --bind 0.0.0.0:${PORT:-8000} --workers 3 --timeout 120
```

## 2.2 Worker Service (Celery)

Create a second service from the same repo.

- **Build Command**
```bash
pip install -r webapp/requirements.txt
```

- **Start Command**
```bash
cd webapp && celery -A config.celery_app worker -l info --concurrency=2
```

Without the worker, background jobs will stay pending.

## 3) Verify Production Health

Run these in the web container shell after deploy:

```bash
cd webapp
python manage.py check --deploy
python manage.py ensure_superuser --dry-run
```

Expected result:

- `check --deploy` shows no issues.
- `ensure_superuser --dry-run` says user exists or would be created.

## 4) Login and Accounts

- Admin login: `/admin/` using the superuser from env.
- Normal users can self-register at `/accounts/register/`.

If you want admin-only onboarding, disable/remove registration route in:

- `webapp/apps/accounts/urls.py`

## 5) Common Production Notes

- Keep `DJANGO_DEBUG=False`.
- Never use `GROOVON_INSECURE_SSL=True` in production.
- Use HTTPS domain and ensure DNS points to Coolify.
- Keep web and worker env variables in sync.
