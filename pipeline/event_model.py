"""
pipeline.event_model — Canonical event dict schema for the enrichment pipeline.

Every source (Excel, scrapers, APIs) must emit events as plain dicts with
at least the REQUIRED keys.  The pipeline operates on lists of these dicts;
no ORM or dataclass dependencies.

Required keys:  city, venue, title
Optional keys:  row, date, time, description, image_url, ticket_url,
                event_url, genre, price, source, source_id, artists,
                artist_links, raw_data
"""

from __future__ import annotations
import hashlib
import re
import unicodedata

REQUIRED_KEYS = {"city", "venue", "title"}


def _norm(value: str | None) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower().strip()
    text = re.sub(r"\s+", " ", text)
    return text


def canonical_event_id(
    *,
    source: str,
    source_id: str | None,
    city: str,
    venue: str,
    title: str,
    date: str | None = None,
    time: str | None = None,
) -> str:
    """Stable canonical event id for dedupe across runs/sources."""
    src = _norm(source) or "unknown"
    sid = _norm(source_id)
    if sid:
        base = f"{src}|sid:{sid}"
    else:
        base = "|".join([
            src,
            _norm(city),
            _norm(venue),
            _norm(title),
            _norm(date),
            _norm(time),
        ])
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]
    return f"ev_{digest}"


def canonical_artist_id(name: str, *, city_hint: str | None = None) -> str:
    """Stable artist id with optional city hint for weak disambiguation."""
    base = "|".join([_norm(name), _norm(city_hint)])
    digest = hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]
    return f"ar_{digest}"


def make_event(
    *,
    city: str,
    venue: str,
    title: str,
    row: int | None = None,
    date: str | None = None,
    time: str | None = None,
    description: str | None = None,
    image_url: str | None = None,
    ticket_url: str | None = None,
    event_url: str | None = None,
    genre: str | None = None,
    price: str | None = None,
    source: str = "",
    source_id: str | None = None,
    artists: list[str] | None = None,
    artist_links: dict | None = None,
    raw_data: dict | None = None,
) -> dict:
    """Build a well-formed event dict.  Strips whitespace on text fields."""
    return {
        "city": (city or "").strip(),
        "venue": (venue or "").strip(),
        "title": (title or "").strip(),
        "row": row,
        "date": date,
        "time": time,
        "description": description,
        "image_url": image_url,
        "ticket_url": ticket_url,
        "event_url": event_url,
        "genre": genre,
        "price": price,
        "source": source,
        "source_id": source_id,
        "artists": artists or [],
        "artist_links": artist_links or {},
        "raw_data": raw_data or {},
        "canonical_event_id": canonical_event_id(
            source=source,
            source_id=source_id,
            city=city,
            venue=venue,
            title=title,
            date=date,
            time=time,
        ),
    }


def validate_event(event: dict) -> bool:
    """Return True if *event* contains the minimum required keys with values."""
    for key in REQUIRED_KEYS:
        val = event.get(key)
        if not val or not str(val).strip():
            return False
    return True
