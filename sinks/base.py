"""
sinks.base — Abstract base class for all output sinks.

A sink receives the fully enriched pipeline output and writes it somewhere:
Excel file, Supabase, JSON file, etc.
"""

from __future__ import annotations

import abc
import logging
from typing import Any

log = logging.getLogger(__name__)


class OutputSink(abc.ABC):
    """Write enriched pipeline output to *some* destination."""

    name: str = "base"

    def __init__(self, *, config: dict[str, Any] | None = None):
        """
        Args:
            config: sink-specific settings (file path, API url, etc.).
        """
        self.config = config or {}

    # ── public API ──────────────────────────────────────────────────
    @abc.abstractmethod
    def write(
        self,
        *,
        classified: list[dict],
        verify_cache: dict,
        enrichment_cache: dict,
        profile_cache: dict,
        output_rows: list[dict],
    ) -> str | None:
        """Persist the enriched data.

        Returns:
            A human-friendly status string (e.g. file path written),
            or None on failure.
        """

    # ── optional hooks ──────────────────────────────────────────────
    def validate(self) -> bool:
        """Return True if the destination is writable.  Default: True."""
        return True

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} name={self.name!r}>"
