"""
sources.scraper_source — Ingest events from the archived scraper output.

Reads the JSON files produced by the _archive/scraper/ spiders and converts
each record into the canonical event dict.

Config keys:
    json_path  (str)  – path to a scraper-output JSON file (required)
    city       (str)  – optional city override (if not in the JSON)
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from sources.base import EventSource
from pipeline.event_model import make_event

log = logging.getLogger(__name__)


class ScraperSource(EventSource):
    """Read events from a scraper-output JSON file."""

    name = "scraper"

    def __init__(self, *, config: dict[str, Any] | None = None):
        super().__init__(config=config)
        self._path: str = self.config.get("json_path", "")
        self._city_override: str = self.config.get("city", "")

    def validate(self) -> bool:
        if not self._path or not os.path.isfile(self._path):
            log.error(f"ScraperSource: file not found → {self._path}")
            return False
        return True

    def fetch(self) -> list[dict]:
        log.info(f"📖 ScraperSource: reading {self._path!r}")
        with open(self._path, "r", encoding="utf-8") as f:
            raw_events = json.load(f)

        events: list[dict] = []
        for item in raw_events:
            title = item.get("title", "")
            if not title:
                continue
            events.append(
                make_event(
                    city=self._city_override or item.get("city", ""),
                    venue=item.get("venue_name", item.get("venue", "")),
                    title=title,
                    date=item.get("date"),
                    time=item.get("time"),
                    description=item.get("description"),
                    image_url=item.get("image_url"),
                    ticket_url=item.get("ticket_url"),
                    event_url=item.get("event_url"),
                    genre=item.get("genre"),
                    price=item.get("price"),
                    source=item.get("source", "scraper"),
                    source_id=item.get("source_id"),
                    artists=item.get("artists", []),
                    artist_links=item.get("artist_links", {}),
                    raw_data=item,
                )
            )

        log.info(f"   {len(events)} events read from scraper JSON")
        return events
