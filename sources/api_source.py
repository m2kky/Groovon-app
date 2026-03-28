"""
sources.api_source — Fetch events from live APIs (Ticketmaster, SeatGeek, Bandsintown).

Config keys:
    city       (str)  – city name to query (required)
    apis       (list) – which APIs to hit, default ["ticketmaster","seatgeek","bandsintown"]
    days_ahead (int)  – look-ahead window in days (default: 30)
    max_events (int)  – cap per API (default: 200)
    genres     (list) – optional genre filters (supports comma-separated values)
    venue      (str)  – optional venue substring filter
    artist     (str)  – optional artist substring filter
"""

from __future__ import annotations

import json
import logging
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from typing import Any

from sources.base import EventSource
from pipeline.event_model import make_event
from pipeline.config import (
    TICKETMASTER_KEY,
    SEATGEEK_CLIENT_ID,
    SSL_CTX,
)

log = logging.getLogger(__name__)


def _normalize_text(value: str | None) -> str:
    """Lowercase + collapse spaces for robust substring matching."""
    return " ".join((value or "").lower().split())


def _split_filter_values(values: Any) -> list[str]:
    """Support repeated args and comma-separated values in a single arg."""
    if values is None:
        return []
    chunks = [values] if isinstance(values, str) else list(values)
    out: list[str] = []
    for raw in chunks:
        for part in str(raw).split(","):
            norm = _normalize_text(part)
            if norm:
                out.append(norm)
    return out


def _get_json(url: str, headers: dict | None = None, timeout: int = 15) -> dict | None:
    """Simple JSON GET helper."""
    try:
        req = urllib.request.Request(url, headers=headers or {})
        with urllib.request.urlopen(req, context=SSL_CTX, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as exc:
        log.debug(f"API request failed: {url[:80]}… → {exc}")
        return None


class APISource(EventSource):
    """Fetch events from Ticketmaster, SeatGeek, Bandsintown."""

    name = "api"

    def __init__(self, *, config: dict[str, Any] | None = None):
        super().__init__(config=config)
        self._city: str = self.config.get("city", "")
        self._apis: list[str] = self.config.get(
            "apis", ["ticketmaster", "seatgeek", "bandsintown"]
        )
        self._days: int = self.config.get("days_ahead", 30)
        self._max: int = self.config.get("max_events", 200)
        self._genre_filters: list[str] = _split_filter_values(self.config.get("genres", []))
        self._venue_filter: str = _normalize_text(self.config.get("venue", ""))
        self._artist_filter: str = _normalize_text(self.config.get("artist", ""))

    def validate(self) -> bool:
        if not self._city:
            log.error("APISource: 'city' config is required")
            return False
        return True

    def fetch(self) -> list[dict]:
        events: list[dict] = []
        for api in self._apis:
            try:
                if api == "ticketmaster":
                    events.extend(self._ticketmaster())
                elif api == "seatgeek":
                    events.extend(self._seatgeek())
                elif api == "bandsintown":
                    log.info("   Bandsintown requires artist-level queries — skipping in batch mode")
                else:
                    log.warning(f"   Unknown API: {api}")
            except Exception as exc:
                log.warning(f"   {api} failed: {exc}")
        filtered = self._apply_filters(events)
        log.info(f"   {len(filtered)} total events from APIs")
        return filtered

    def _apply_filters(self, events: list[dict]) -> list[dict]:
        has_filters = bool(self._genre_filters or self._venue_filter or self._artist_filter)
        if not has_filters:
            return events

        filtered = [ev for ev in events if self._matches_filters(ev)]
        log.info(
            "   Filters applied: genre=%s venue=%s artist=%s → %s/%s kept",
            self._genre_filters or ["*"],
            self._venue_filter or "*",
            self._artist_filter or "*",
            len(filtered),
            len(events),
        )
        return filtered

    def _matches_filters(self, event: dict) -> bool:
        if self._venue_filter:
            if self._venue_filter not in _normalize_text(event.get("venue", "")):
                return False

        if self._artist_filter:
            artist_bits = [event.get("title", "")]
            artist_bits.extend(event.get("artists") or [])
            raw_data = event.get("raw_data") or {}
            attractions = (raw_data.get("_embedded", {}).get("attractions") or [])
            for a in attractions:
                name = a.get("name")
                if name:
                    artist_bits.append(name)
            if self._artist_filter not in _normalize_text(" ".join(artist_bits)):
                return False

        if self._genre_filters and not self._matches_genre(event):
            return False

        return True

    def _matches_genre(self, event: dict) -> bool:
        genre_parts: list[str] = [str(event.get("genre") or ""), str(event.get("title") or "")]
        raw_data = event.get("raw_data") or {}

        for c in (raw_data.get("classifications") or []):
            for key in ("segment", "genre", "subGenre", "subgenre", "type", "subType", "subtype"):
                value = c.get(key)
                if isinstance(value, dict):
                    name = value.get("name")
                else:
                    name = value
                if name:
                    genre_parts.append(str(name))

        for t in (raw_data.get("taxonomies") or []):
            name = t.get("name")
            if name:
                genre_parts.append(str(name))

        genre_text = _normalize_text(" ".join(genre_parts))
        return any(g in genre_text for g in self._genre_filters)

    # ── Ticketmaster ────────────────────────────────────────────────
    def _ticketmaster(self) -> list[dict]:
        if not TICKETMASTER_KEY:
            log.info("   ⏩ Ticketmaster: no API key configured")
            return []
        log.info(f"🎫 Ticketmaster: searching {self._city!r}…")
        now = datetime.utcnow()
        start = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        end = (now + timedelta(days=self._days)).strftime("%Y-%m-%dT%H:%M:%SZ")
        url = (
            f"https://app.ticketmaster.com/discovery/v2/events.json"
            f"?apikey={TICKETMASTER_KEY}"
            f"&city={urllib.parse.quote(self._city)}"
            f"&classificationName=music"
            f"&startDateTime={start}&endDateTime={end}"
            f"&size={min(self._max, 200)}&sort=date,asc"
        )
        data = _get_json(url)
        if not data:
            return []

        results: list[dict] = []
        for ev in (data.get("_embedded", {}).get("events") or []):
            venue_obj = (ev.get("_embedded", {}).get("venues") or [{}])[0]
            artists_list = [
                a.get("name")
                for a in (ev.get("_embedded", {}).get("attractions") or [])
                if a.get("name")
            ]
            results.append(
                make_event(
                    city=venue_obj.get("city", {}).get("name", self._city),
                    venue=venue_obj.get("name", ""),
                    title=ev.get("name", ""),
                    date=(ev.get("dates", {}).get("start", {}).get("localDate")),
                    time=(ev.get("dates", {}).get("start", {}).get("localTime")),
                    image_url=(ev.get("images", [{}])[0].get("url") if ev.get("images") else None),
                    ticket_url=ev.get("url"),
                    event_url=ev.get("url"),
                    genre=(ev.get("classifications", [{}])[0].get("genre", {}).get("name")),
                    price=self._tm_price(ev),
                    source="ticketmaster",
                    source_id=ev.get("id"),
                    artists=artists_list,
                    raw_data=ev,
                )
            )
        log.info(f"   Ticketmaster: {len(results)} events")
        return results

    @staticmethod
    def _tm_price(ev: dict) -> str | None:
        pr = ev.get("priceRanges", [{}])[0] if ev.get("priceRanges") else {}
        if pr.get("min"):
            return f"{pr.get('currency','')}{pr['min']}"
        return None

    # ── SeatGeek ────────────────────────────────────────────────────
    def _seatgeek(self) -> list[dict]:
        if not SEATGEEK_CLIENT_ID:
            log.info("   ⏩ SeatGeek: no client_id configured")
            return []
        log.info(f"🎟️  SeatGeek: searching {self._city!r}…")
        now = datetime.utcnow()
        dt_start = now.strftime("%Y-%m-%dT%H:%M:%S")
        dt_end = (now + timedelta(days=self._days)).strftime("%Y-%m-%dT%H:%M:%S")
        url = (
            f"https://api.seatgeek.com/2/events"
            f"?client_id={SEATGEEK_CLIENT_ID}"
            f"&venue.city={urllib.parse.quote(self._city)}"
            f"&type=concert"
            f"&datetime_utc.gte={dt_start}&datetime_utc.lte={dt_end}"
            f"&per_page={min(self._max, 100)}&sort=datetime_utc.asc"
        )
        data = _get_json(url)
        if not data:
            return []

        results: list[dict] = []
        for ev in data.get("events", []):
            venue_obj = ev.get("venue", {})
            date_str = (ev.get("datetime_local") or "")[:10] or None
            time_str = (ev.get("datetime_local") or "")[11:16] or None
            artists_list = [p["name"] for p in ev.get("performers", []) if p.get("name")]
            results.append(
                make_event(
                    city=venue_obj.get("city", self._city),
                    venue=venue_obj.get("name", ""),
                    title=ev.get("title", ev.get("short_title", "")),
                    date=date_str,
                    time=time_str,
                    ticket_url=ev.get("url"),
                    event_url=ev.get("url"),
                    source="seatgeek",
                    source_id=str(ev.get("id", "")),
                    artists=artists_list,
                    raw_data=ev,
                )
            )
        log.info(f"   SeatGeek: {len(results)} events")
        return results
