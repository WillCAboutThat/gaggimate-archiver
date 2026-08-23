"""Configuration, env-driven with CLI overrides.

Nothing here is secret; the compose file supplies these as ${VAR:-default},
so the stack needs no env-file (deploy.yml counts defaults as satisfied).
"""

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class Config:
    host: str = os.environ.get("GAGGIMATE_HOST", "gaggimate.local")
    archive_dir: Path = Path(os.environ.get("ARCHIVE_DIR", "./archive-dev"))
    log_level: str = os.environ.get("LOG_LEVEL", "INFO")
    # Device gentleness: the display MCU sits at ~80% heap with ~50%
    # fragmentation, so requests are sequential and spaced. Seconds.
    request_delay: float = float(os.environ.get("REQUEST_DELAY", "1.0"))
    request_timeout: float = float(os.environ.get("REQUEST_TIMEOUT", "15"))
    # Notes are typically added from the phone AFTER the shot was archived,
    # so each sync re-checks notes on shots newer than this window.
    notes_recheck_days: int = int(os.environ.get("NOTES_RECHECK_DAYS", "30"))
    # Phase 2: verified deletion from the device (see ARCHITECTURE.md).
    # Enable only after your archive has accumulated verified cycles.
    delete_enabled: bool = os.environ.get("DELETE_ENABLED", "false").lower() in ("1", "true", "yes")
    delete_grace_days: int = int(os.environ.get("DELETE_GRACE_DAYS", "14"))
    keep_recent_shots: int = int(os.environ.get("KEEP_RECENT_SHOTS", "20"))
    delete_max_per_run: int = int(os.environ.get("DELETE_MAX_PER_RUN", "10"))

    @property
    def raw_dir(self) -> Path:
        return self.archive_dir / "raw"

    @property
    def parquet_dir(self) -> Path:
        return self.archive_dir / "parquet"

    @property
    def manifest_path(self) -> Path:
        return self.archive_dir / "manifest.json"

    @property
    def last_success_path(self) -> Path:
        return self.archive_dir / "last-success"
