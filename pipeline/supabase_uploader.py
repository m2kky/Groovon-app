"""
pipeline/supabase_uploader.py — Upload enriched artist profiles to Supabase.

Reads profiles_rich.json (or accepts a list directly) and upserts each artist
into the Supabase `artists` table.  Handles:
- Conflict resolution via on_conflict (upsert by normalized_name)
- Batch inserts (configurable chunk size)
- Dry-run mode (logs what *would* be uploaded without touching DB)
- Detailed summary logging

Usage (standalone):
    python -m pipeline.supabase_uploader profiles_rich.json
    python -m pipeline.supabase_uploader profiles_rich.json --dry-run

Usage (as library):
    from pipeline.supabase_uploader import upload_profiles
    upload_profiles(profiles, dry_run=False)
"""

import json, logging, urllib.request, urllib.parse, sys, os

from pipeline.config import SSL_CTX

log = logging.getLogger(__name__)


def _supabase_post(url: str, key: str, rows: list[dict], table: str = "artists",
                   on_conflict: str = "normalized_name", dry_run: bool = False) -> int:
    """POST a batch of rows to Supabase REST API.  Returns count of rows sent."""
    if dry_run:
        log.info(f"   [DRY-RUN] Would upsert {len(rows)} rows into '{table}'")
        return len(rows)

    endpoint = f"{url.rstrip('/')}/rest/v1/{table}"
    payload = json.dumps(rows, ensure_ascii=False).encode("utf-8")

    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": f"resolution=merge-duplicates",
    }

    req = urllib.request.Request(endpoint, data=payload, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=30) as resp:
            status = resp.status
            if status in (200, 201):
                return len(rows)
            else:
                body = resp.read().decode()
                log.warning(f"   Supabase returned {status}: {body[:300]}")
                return 0
    except Exception as exc:
        log.error(f"   Supabase POST failed: {exc}")
        return 0


def _profile_to_row(profile: dict) -> dict:
    """Map a rich profile dict to a flat Supabase row."""
    platforms = profile.get("platforms", {})
    emails = profile.get("emails", [])
    locale = profile.get("locale", {})

    return {
        "normalized_name": profile.get("normalized_name", ""),
        "name":            profile.get("name", ""),
        "genre":           profile.get("genre", "Don't Box Me!"),
        "genre_source":    profile.get("genre_source"),
        "all_genres":      profile.get("all_genres", []),
        "bio":             (profile.get("bio") or "")[:2000],  # cap at 2k chars
        "city":            locale.get("city", ""),
        "state":           locale.get("state", ""),
        "country":         locale.get("country", ""),
        "confidence":      profile.get("confidence", "LOW"),
        "profile_score":   profile.get("profile_score", 0),
        # Platform URLs
        "spotify_url":     platforms.get("spotify", ""),
        "deezer_url":      platforms.get("deezer", ""),
        "apple_music_url": platforms.get("apple_music", ""),
        "youtube_url":     platforms.get("youtube", ""),
        "soundcloud_url":  platforms.get("soundcloud", ""),
        "bandcamp_url":    platforms.get("bandcamp", ""),
        "instagram_url":   platforms.get("instagram", ""),
        "facebook_url":    platforms.get("facebook", ""),
        "twitter_url":     platforms.get("twitter", ""),
        "tiktok_url":      platforms.get("tiktok", ""),
        "website_url":     platforms.get("website", ""),
        # Emails (up to 3)
        "email1":          emails[0] if len(emails) > 0 else "",
        "email2":          emails[1] if len(emails) > 1 else "",
        "email3":          emails[2] if len(emails) > 2 else "",
        # Metrics
        "spotify_followers":    profile.get("spotify_followers", 0),
        "spotify_monthly":      profile.get("spotify_monthly_listeners", 0),
        "soundcloud_followers": profile.get("soundcloud_followers", 0),
        "bandsintown_trackers": profile.get("bandsintown_trackers", 0),
        "is_touring":           profile.get("is_touring", False),
        "platform_count":       len(platforms),
        # Rich data stored as JSONB
        "top_songs":       profile.get("top_songs", []),
        "related_artists": profile.get("related_artists", []),
        "all_urls":        profile.get("urls", []),
    }


def upload_profiles(profiles: list[dict], dry_run: bool = False,
                    batch_size: int = 50,
                    supabase_url: str | None = None,
                    supabase_key: str | None = None) -> dict:
    """Upsert artist profiles to Supabase.

    Returns a summary dict: {total, uploaded, skipped, errors}.
    """
    from pipeline.config import SUPABASE_URL, SUPABASE_KEY
    url = supabase_url or SUPABASE_URL
    key = supabase_key or SUPABASE_KEY

    if not url or not key:
        log.error("❌ SUPABASE_URL / SUPABASE_KEY not set — skipping upload")
        return {"total": len(profiles), "uploaded": 0, "skipped": len(profiles), "errors": 0}

    summary = {"total": len(profiles), "uploaded": 0, "skipped": 0, "errors": 0}

    rows = []
    for p in profiles:
        if not p.get("normalized_name"):
            summary["skipped"] += 1
            continue
        rows.append(_profile_to_row(p))

    log.info(f"📤 Uploading {len(rows)} profiles to Supabase {'(DRY-RUN)' if dry_run else ''}...")

    # Batch upload
    for i in range(0, len(rows), batch_size):
        chunk = rows[i : i + batch_size]
        sent = _supabase_post(url, key, chunk, dry_run=dry_run)
        if sent:
            summary["uploaded"] += sent
        else:
            summary["errors"] += len(chunk)

    log.info(f"   ✅ Upload complete: {summary['uploaded']}/{summary['total']} "
             f"(skipped={summary['skipped']}, errors={summary['errors']})")
    return summary


# ── CLI entry point ─────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Upload profiles_rich.json to Supabase")
    parser.add_argument("json_file", help="Path to profiles_rich.json")
    parser.add_argument("--dry-run", action="store_true", help="Log what would be uploaded without touching DB")
    parser.add_argument("--batch-size", type=int, default=50, help="Rows per API call (default: 50)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-6s %(message)s")

    with open(args.json_file, "r", encoding="utf-8") as f:
        profiles = json.load(f)

    result = upload_profiles(profiles, dry_run=args.dry_run, batch_size=args.batch_size)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
