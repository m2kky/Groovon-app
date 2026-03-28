"""
pipeline/supabase_sync.py — Stub for future Supabase integration (Item 7).

Provides a SupabaseSync class that will eventually push artist profiles,
events, and pipeline runs to Supabase.  For now every method is a safe
no-op that logs what *would* happen.

Usage:
    from pipeline.supabase_sync import SupabaseSync
    syncer = SupabaseSync()          # reads SUPABASE_URL / SUPABASE_KEY from env
    syncer.upsert_artist(profile)
    syncer.upsert_event(event)
    syncer.log_run(stats)
"""

import json, logging, os

log = logging.getLogger(__name__)


class SupabaseSync:
    """Stub Supabase client — all methods are no-ops until credentials are set."""

    def __init__(self):
        self.url = os.getenv("SUPABASE_URL", "")
        self.key = os.getenv("SUPABASE_KEY", "")
        self.enabled = bool(self.url and self.key)
        if not self.enabled:
            log.info("   ⚠️  Supabase sync disabled (SUPABASE_URL / SUPABASE_KEY not set)")

    # ── Artists ──

    def upsert_artist(self, profile: dict) -> bool:
        """Insert or update an artist row.  Returns True on success."""
        if not self.enabled:
            return False
        name = profile.get("name", "?")
        log.debug(f"   [supabase-stub] would upsert artist: {name}")
        # TODO: real implementation
        # from supabase import create_client
        # sb = create_client(self.url, self.key)
        # sb.table("artists").upsert({...}).execute()
        return False

    def upsert_artists_batch(self, profiles: list[dict]) -> int:
        """Batch upsert. Returns count of successful upserts."""
        if not self.enabled:
            return 0
        log.debug(f"   [supabase-stub] would batch-upsert {len(profiles)} artists")
        return 0

    # ── Events ──

    def upsert_event(self, event: dict) -> bool:
        """Insert or update an event row."""
        if not self.enabled:
            return False
        log.debug(f"   [supabase-stub] would upsert event: {event.get('title', '?')}")
        return False

    # ── Pipeline runs ──

    def log_run(self, stats: dict) -> bool:
        """Log pipeline execution metadata (duration, counts, etc.)."""
        if not self.enabled:
            return False
        log.debug(f"   [supabase-stub] would log run: {json.dumps(stats, default=str)}")
        return False
