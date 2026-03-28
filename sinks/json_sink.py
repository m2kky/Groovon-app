"""
sinks.json_sink — Write enriched profiles to a JSON file.

Useful for debugging, downstream API consumption, or piping into other tools.

Config keys:
    output_path  (str) – path to write the .json file (required)
    indent       (int) – JSON indentation (default: 2)
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from sinks.base import OutputSink

log = logging.getLogger(__name__)


class JsonSink(OutputSink):
    """Dump profile_cache to a JSON file."""

    name = "json"

    def __init__(self, *, config: dict[str, Any] | None = None):
        super().__init__(config=config)
        self._output_path: str = self.config.get("output_path", "profiles_rich.json")
        self._indent: int = self.config.get("indent", 2)

    def validate(self) -> bool:
        if not self._output_path:
            log.error("JsonSink: 'output_path' config is required")
            return False
        return True

    def write(
        self,
        *,
        classified: list[dict],
        verify_cache: dict,
        enrichment_cache: dict,
        profile_cache: dict,
        output_rows: list[dict],
    ) -> str | None:
        try:
            os.makedirs(os.path.dirname(self._output_path) or ".", exist_ok=True)
            profiles = list(profile_cache.values())
            with open(self._output_path, "w", encoding="utf-8") as f:
                json.dump(profiles, f, indent=self._indent, ensure_ascii=False)
            log.info(f"📄 JsonSink: wrote {len(profiles)} profiles → {self._output_path}")
            return self._output_path
        except Exception as exc:
            log.error(f"JsonSink: write failed → {exc}")
            return None
