"""
sinks.supabase_sink — Upload enriched profiles to Supabase.

Wraps the existing `pipeline.supabase_uploader.upload_profiles` function.

Config keys:
    dry_run  (bool) – if True, skip actual upload (default: False)
"""

from __future__ import annotations

import logging
from typing import Any

from sinks.base import OutputSink

log = logging.getLogger(__name__)


class SupabaseSink(OutputSink):
    """Upload enriched profiles to Supabase via the existing uploader."""

    name = "supabase"

    def __init__(self, *, config: dict[str, Any] | None = None):
        super().__init__(config=config)
        self._dry_run: bool = self.config.get("dry_run", False)

    def write(
        self,
        *,
        classified: list[dict],
        verify_cache: dict,
        enrichment_cache: dict,
        profile_cache: dict,
        output_rows: list[dict],
    ) -> str | None:
        from pipeline.supabase_uploader import upload_profiles

        profiles = list(profile_cache.values())
        try:
            result = upload_profiles(profiles, dry_run=self._dry_run)
            msg = f"uploaded {result['uploaded']}/{result['total']}"
            log.info(f"📤 SupabaseSink: {msg}")
            return msg
        except Exception as exc:
            log.error(f"SupabaseSink: upload failed → {exc}")
            return None
