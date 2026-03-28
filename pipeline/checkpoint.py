"""
Checkpoint System for the Groovon artist-enrichment pipeline.

Saves/loads intermediate state between phases so that a crash at Phase 3.5
doesn't lose 40 minutes of Phase 1–3 work.

Usage in main():
    cp = Checkpoint("practise_batch")
    state = cp.load()
    if state and state["phase"] >= 1:
        classified = state["classified"]
        ...skip Phase 1...
    ...after Phase 1...
    cp.save(phase=1, classified=classified)
"""

import json
import os
import time
import logging
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)

# Where checkpoint files live — same directory as process_david_excel.py
_CHECKPOINT_DIR = Path(__file__).resolve().parent.parent  # => d:\projects\groovon\new


class Checkpoint:
    """Phase-level checkpoint for the pipeline."""

    def __init__(self, batch_id: str = "default"):
        self.batch_id = batch_id
        self.filepath = _CHECKPOINT_DIR / f"checkpoint_{batch_id}.json"

    # ─── Save ───────────────────────────────────────────────────────
    def save(self, *, phase: int, **data) -> None:
        """
        Save pipeline state after completing a phase.

        Args:
            phase: completed phase number (1, 2, 3, 35, 4)
            **data: arbitrary dicts/lists to persist (must be JSON-serialisable)
        """
        payload = {
            "batch_id": self.batch_id,
            "phase": phase,
            "timestamp": datetime.now().isoformat(),
            "data": data,
        }
        tmp = self.filepath.with_suffix(".tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            # Atomic rename (safe on Windows too — overwrites existing)
            if self.filepath.exists():
                self.filepath.unlink()
            tmp.rename(self.filepath)
            size_kb = self.filepath.stat().st_size / 1024
            log.info(f"   💾 Checkpoint saved: phase={phase}, {size_kb:.0f} KB → {self.filepath.name}")
        except Exception as exc:
            log.warning(f"   ⚠️ Checkpoint save failed: {exc}")
            if tmp.exists():
                tmp.unlink(missing_ok=True)

    # ─── Load ───────────────────────────────────────────────────────
    def load(self) -> dict | None:
        """
        Load the most recent checkpoint for this batch.

        Returns:
            dict with keys {"batch_id", "phase", "timestamp", "data"}
            or None if no checkpoint exists / file is corrupt.
        """
        if not self.filepath.exists():
            return None
        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                payload = json.load(f)
            phase = payload.get("phase", 0)
            ts = payload.get("timestamp", "?")
            log.info(f"   📂 Checkpoint found: phase={phase}, saved at {ts}")
            return payload
        except Exception as exc:
            log.warning(f"   ⚠️ Checkpoint file corrupt, starting fresh: {exc}")
            return None

    # ─── Delete (clean exit) ────────────────────────────────────────
    def delete(self) -> None:
        """Remove checkpoint file after successful completion."""
        if self.filepath.exists():
            self.filepath.unlink()
            log.info(f"   🗑️ Checkpoint deleted (clean finish)")

    # ─── Convenience ────────────────────────────────────────────────
    def exists(self) -> bool:
        return self.filepath.exists()

    def __repr__(self) -> str:
        return f"Checkpoint(batch_id={self.batch_id!r}, path={self.filepath})"
