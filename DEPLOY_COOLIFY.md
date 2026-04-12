# 🚀 Deploy Groovon on Coolify

Complete guide to deploy the Groovon web app on Coolify with PostgreSQL, Redis, and Celery.

---

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌──────────────┐
│   Coolify    │     │  PostgreSQL │     │    Redis     │
│  (Reverse    │────▶│   (DB)      │     │  (Broker)    │
│   Proxy)     │     └─────────────┘     └──────────────┘
│              │            ▲                    ▲
│   HTTPS      │            │                    │
│   Port 443   │     ┌──────┴────────────────────┤
│              │     │                           │
└──────┬───────┘     │    ┌───────────┐    ┌─────┴──────┐
       │             ├───▶│  Web      │    │  Worker    │
       └─────────────┘    │  Gunicorn │    │  Celery    │
                          │  :8000    │    │            │
                          └───────────┘    └────────────┘
                                ▲                ▲
                                │   Shared       │
                                └── /app/media ──┘
```

---

## Step 1: Prepare Your Repo

Make sure these files exist in your repo root:

```
Dockerfile          ← builds the image
start.sh            ← entrypoint (migrate + gunicorn)
.dockerignore       ← keeps image small
nixpacks.toml       ← fallback build hint
webapp/             ← Django project
engine.py           ← pipeline engine
pipeline/           ← pipeline modules
sources/            ← data sources
sinks/              ← output sinks
```

Push everything to your Git repo (GitHub/GitLab/etc):

```bash
git add Dockerfile start.sh .dockerignore
git commit -m "Add Docker production setup"
git push origin main
```

---

## Step 2: Create Resources in Coolify

### 2.1 PostgreSQL Database

1. Go to **Resources** → **+ New** → **Database** → **PostgreSQL**
2. Use **PostgreSQL 16** image
3. Set:
   - Database Name: `groovon`
   - Username: `groovon`
   - Password: *(generate a strong one)*
4. Note the **Internal URL**: `postgres://groovon:<password>@<service-name>:5432/groovon`

### 2.2 Redis

1. Go to **Resources** → **+ New** → **Database** → **Redis**
2. Use **Redis 7** image
3. Note the **Internal URL**: `redis://<service-name>:6379/0`

### 2.3 Web Service (Django + Gunicorn)

1. Go to **Resources** → **+ New** → **Application**
2. Connect your Git repo
3. Settings:
   - **Build Pack**: Dockerfile
   - **Dockerfile Location**: `/Dockerfile`
   - **Port**: `8000`
4. Add a **Persistent Storage** volume:
   - Source: `/app/media`
   - Name: `groovon-media`
5. Set **Health Check**:
   - Path: `/_health/`
   - Port: `8000`
   - Interval: `30s`
6. Set all environment variables (see Step 3)

### 2.4 Worker Service (Celery)

1. Go to **Resources** → **+ New** → **Application**
2. Connect the **same Git repo**
3. Settings:
   - **Build Pack**: Dockerfile
   - **Dockerfile Location**: `/Dockerfile`
   - **Custom Start Command**: `sh -c "cd /app/webapp && celery -A config.celery_app worker -l info --concurrency=2"`
4. **No port exposure** needed (it's a background worker)
5. Add the **same** persistent storage volume:
   - Source: `/app/media`
   - Name: `groovon-media` (same as web!)
6. Set **all the same** environment variables as the web service

---

## Step 3: Environment Variables

> ⚠️ **Set these on BOTH web and worker services!**

### Required (Django Core)

| Variable | Value | Notes |
|----------|-------|-------|
| `DJANGO_SETTINGS_MODULE` | `config.settings.production` | **Must be this exact value** |
| `DJANGO_SECRET_KEY` | *(generate 50+ char random)* | `python -c "import secrets; print(secrets.token_urlsafe(64))"` |
| `DJANGO_DEBUG` | `False` | **Never True in production** |
| `DJANGO_ALLOWED_HOSTS` | `your-domain.com,www.your-domain.com` | Your actual domain(s) |
| `DJANGO_CSRF_TRUSTED_ORIGINS` | `https://your-domain.com,https://www.your-domain.com` | Must include `https://` |
| `DJANGO_SERVE_MEDIA` | `True` | Serve uploads via Django |
| `DJANGO_MEDIA_ROOT` | `/app/media` | Must match volume mount |

### Required (Infrastructure)

| Variable | Value | Notes |
|----------|-------|-------|
| `DATABASE_URL` | `postgres://groovon:<pw>@<pg-service>:5432/groovon` | From Step 2.1 |
| `CELERY_BROKER_URL` | `redis://<redis-service>:6379/0` | From Step 2.2 |
| `CELERY_RESULT_BACKEND` | `redis://<redis-service>:6379/0` | Same as broker |

### Required (Admin Bootstrap)

| Variable | Value | Notes |
|----------|-------|-------|
| `DJANGO_SUPERUSER_USERNAME` | `admin` | Created on first deploy |
| `DJANGO_SUPERUSER_EMAIL` | `admin@your-domain.com` | Admin email |
| `DJANGO_SUPERUSER_PASSWORD` | *(strong password)* | Change after first login |
| `DJANGO_SUPERUSER_UPDATE_PASSWORD` | `False` | Set to True to force reset |

### Optional (SSL — defaults are fine)

| Variable | Default | Notes |
|----------|---------|-------|
| `DJANGO_SECURE_SSL_REDIRECT` | `True` | Coolify handles SSL, keep True |
| `DJANGO_SECURE_HSTS_SECONDS` | `31536000` | 1 year HSTS |
| `DJANGO_SECURE_HSTS_INCLUDE_SUBDOMAINS` | `True` | |
| `DJANGO_SECURE_HSTS_PRELOAD` | `True` | |
| `GROOVON_INSECURE_SSL` | `False` | **Never True in production** |

### Optional (Performance)

| Variable | Default | Notes |
|----------|---------|-------|
| `GUNICORN_WORKERS` | `3` | 2× CPU cores + 1 |
| `GUNICORN_TIMEOUT` | `120` | Seconds per request |
| `CONN_MAX_AGE` | `600` | DB connection reuse (seconds) |
| `DJANGO_LOG_LEVEL` | `INFO` | Set to `WARNING` when stable |

### API Keys (Pipeline — set on BOTH services)

| Variable | Where to get it |
|----------|----------------|
| `SPOTIFY_CLIENT_ID` | [Spotify Developer](https://developer.spotify.com/) |
| `SPOTIFY_CLIENT_SECRET` | Same |
| `LASTFM_API_KEY` | [Last.fm API](https://www.last.fm/api/account/create) |
| `YOUTUBE_API_KEY` | [Google Cloud Console](https://console.cloud.google.com/) |
| `DISCOGS_TOKEN` | [Discogs Developer](https://www.discogs.com/settings/developers) |
| `GENIUS_ACCESS_TOKEN` | [Genius API](https://genius.com/api-clients) |
| `GOOGLE_KG_API_KEY` | Google Cloud Console (same as YouTube) |
| `SERPER_API_KEY` | [Serper.dev](https://serper.dev/) |
| `SETLISTFM_API_KEY` | [Setlist.fm API](https://api.setlist.fm/docs/1.0/index.html) |
| `TICKETMASTER_API_KEY` | [Ticketmaster Developer](https://developer.ticketmaster.com/) |
| `SEATGEEK_CLIENT_ID` | [SeatGeek Platform](https://platform.seatgeek.com/) |
| `BANDSINTOWN_APP_ID` | [Bandsintown API](https://app.swaggerhub.com/apis/Bandsintown/PublicAPI/3.0.1) |
| `OPENROUTER_API_KEY` | [OpenRouter](https://openrouter.ai/) |
| `SCRAPINGBEE_API_KEY` | [ScrapingBee](https://www.scrapingbee.com/) |
| `SUPABASE_URL` | [Supabase](https://supabase.com/) (optional) |
| `SUPABASE_KEY` | Same (optional) |

---

## Step 4: Deploy

1. Click **Deploy** on the web service
2. Click **Deploy** on the worker service
3. Wait for both to go green ✅

### What happens on first deploy:

```
🔄 Running migrations...        ← creates all DB tables
👤 Ensuring superuser...        ← creates admin from env vars
🚀 Starting Gunicorn...         ← app is live
```

---

## Step 5: Verify

### Health Check
```
curl https://your-domain.com/_health/
# → {"status": "ok", "database": true}
```

### Admin Panel
```
https://your-domain.com/admin/
# Login with DJANGO_SUPERUSER_USERNAME / DJANGO_SUPERUSER_PASSWORD
```

### Dashboard
```
https://your-domain.com/
# Requires login — redirects to /accounts/login/
```

---

## Step 6: Domain & SSL

1. In Coolify, go to the web service → **Settings** → **Domain**
2. Add your domain: `your-domain.com`
3. Enable **SSL** (Let's Encrypt) — Coolify does this automatically
4. Make sure your DNS A record points to the Coolify server IP

---

## Common Issues

### "CSRF verification failed"
→ Make sure `DJANGO_CSRF_TRUSTED_ORIGINS` has `https://your-domain.com` (with `https://`)

### "DisallowedHost"
→ Add the domain to `DJANGO_ALLOWED_HOSTS`

### Jobs stuck on "Pending"
→ Check that the Celery worker service is running and has the same env vars

### Media uploads not found by worker
→ Both web and worker must share the **same** persistent volume at `/app/media`

### "Bad Gateway" or 502 errors
→ Check the web service logs. Usually means gunicorn crashed or migration failed

---

## Local Testing with Docker

```bash
# 1. Copy and fill API keys
cp .env.production.local.example .env.production.local
# Edit .env.production.local with your API keys

# 2. Build and run
docker-compose up --build

# 3. Test
curl http://localhost:8000/_health/
# Open http://localhost:8000/admin/ (admin / admin123)
```
