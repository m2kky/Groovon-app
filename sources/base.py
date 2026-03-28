"""
sources.base — Abstract base class for all event sources.

Every concrete source (Excel file, web scraper, API client) subclasses
EventSource and implements `fetch()` to return a list of event dicts
conforming to the schema in `pipeline.event_model`.
"""

from __future__ import annotations

import abc
import logging
from typing import Any

log = logging.getLogger(__name__)


class EventSource(abc.ABC):
    """Pull raw events from *some* upstream (file, API, scraper, etc.)."""

    name: str = "base"  # human-friendly label, override in subclass

    def __init__(self, *, config: dict[str, Any] | None = None):
        """
        Args:
            config: source-specific settings (file path, API key, city
                    filter, date range …).  Each subclass documents which
                    keys it expects.
        """
        self.config = config or {}

    # ── public API ──────────────────────────────────────────────────
    @abc.abstractmethod
    def fetch(self) -> list[dict]:
        """Return a list of event dicts (see `pipeline.event_model`).

        Must include at minimum: city, venue, title.
        """

    # ── optional hooks ──────────────────────────────────────────────
    def validate(self) -> bool:
        """Return True if the source can be reached / file exists.

        Default implementation always returns True; subclasses may
        override to add a connectivity check.
        """
        return True

    # ── helpers ──────────────────────────────────────────────────────
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} name={self.name!r}>"
