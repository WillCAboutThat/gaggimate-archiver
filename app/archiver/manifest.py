"""The archive manifest: a rebuildable cache of what has been archived.

The raw/ tree is the truth; this file only saves re-downloading and carries
the verification evidence phase 2 (verified deletion) will require. Schema
carries the deletion fields from day one so phase 1 runs accumulate
delete-eligibility evidence before that feature ever turns on (ARCHITECTURE.md).

Writes are temp-then-rename and happen after every archived shot, so a
crashed run strands one download at most, never the pass.
"""

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path

SCHEMA_VERSION = 1


def shot_key(epoch: int, padded: str) -> str:
    """Manifest key for a shot. The device's shot counter RESETS when its
    filesystem is recreated (measured 2026-08-23: the SPIFFS->LittleFS
    migration started the counter over), so a device id alone stops being
    unique across the archive's lifetime. Epoch 1 keeps plain padded keys
    (every record archived before epochs existed IS epoch 1 - zero churn);
    later epochs prefix, so a reborn id 000233 can never collide with the
    archived one."""
    return padded if epoch <= 1 else f"e{epoch}-{padded}"


@dataclass
class ShotRecord:
    id: int
    padded_id: str
    timestamp: int              # device epoch seconds, as reported by index.bin
    profile_id: str = ""
    profile_name: str = ""
    duration: int = 0           # units as reported by the device; see parse notes
    rating: int = 0
    slog_path: str = ""         # relative to archive root
    slog_sha256: str = ""
    slog_size: int = 0
    json_path: str | None = None
    json_sha256: str | None = None
    json_size: int | None = None
    archived_at: str = ""       # ISO 8601 UTC
    verified_at: str = ""       # NAS read-back matched the in-flight hash
    parse_ok: bool | None = None
    sample_count: int | None = None
    tombstone_seen: bool = False   # index entry carries SHOT_FLAG_DELETED
    device_deleted_at: str | None = None  # set by phase 2 only
    # Terminal state for a capture that parses degenerate but is BYTE-STABLE
    # across fetches: the device's file simply IS this (e.g. damaged by a
    # mid-write WiFi drop). We faithfully hold what the device holds; the
    # retry loop ends; parse_ok stays False as the quality record.
    accepted_degenerate: bool = False
    # Which life of the device's shot counter this id belongs to (additive,
    # default 1 = every pre-epoch record). See shot_key().
    epoch: int = 1


@dataclass
class Manifest:
    schema: int = SCHEMA_VERSION
    shots: dict[str, ShotRecord] = field(default_factory=dict)  # key: padded_id
    # Format-drift ledger: wire-format versions actually observed. The device
    # only changes format when new firmware is flashed, so a change here IS
    # the firmware-update detector (v1.8.1 exposes no version over HTTP; the
    # WS res:ota-settings version lands here once the profile-snapshot
    # session exists). Keys: index_version, slog_versions (sorted list),
    # display_version, controller_version, format_changed_at.
    device: dict = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> "Manifest":
        if not path.exists():
            return cls()
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("schema") != SCHEMA_VERSION:
            raise ValueError(f"manifest schema {data.get('schema')} != {SCHEMA_VERSION}")
        shots = {k: ShotRecord(**v) for k, v in data.get("shots", {}).items()}
        return cls(schema=data["schema"], shots=shots, device=data.get("device", {}))

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        payload = {
            "schema": self.schema,
            "device": self.device,
            "shots": {k: asdict(v) for k, v in sorted(self.shots.items())},
        }
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=1)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
