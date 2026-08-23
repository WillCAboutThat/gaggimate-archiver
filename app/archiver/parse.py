"""Parse: derive Parquet from the raw archive. Rebuildable at any time.

Two tables (ARCHITECTURE.md):
  parquet/shots.parquet            one row per shot, rebuilt wholesale each
                                   run (small; wholesale = idempotent).
  parquet/samples/<id>.parquet     one file per shot, skipped when present.

The raw/ tree is the truth: this step reads only the archive, never the
device. parse_ok + sample_count land in the manifest - they are phase 2
delete-eligibility evidence (byte-identical alone cannot catch a device that
consistently serves truncated bytes; a clean parse can).
"""

import json
import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .config import Config
from .manifest import Manifest
from .notify import notify
from .vendor.gaggimate_mcp.parsers.shot import ShotData, parse_binary_shot

log = logging.getLogger("archiver.parse")

SAMPLE_FIELDS = ["t", "tt", "ct", "tp", "cp", "fl", "tf", "pf", "vf", "v", "ev", "pr"]


def _parse_slog(cfg: Config, rec) -> ShotData:
    data = (cfg.archive_dir / rec.slog_path).read_bytes()
    return parse_binary_shot(data, rec.padded_id)


def _samples_table(shot: ShotData) -> pa.Table:
    cols: dict[str, list] = {"shot_id": [], "phase": []}
    for f in SAMPLE_FIELDS:
        cols[f] = []
    for s in shot.samples:
        cols["shot_id"].append(int(shot.id))
        cols["phase"].append(s.get("phase"))
        for f in SAMPLE_FIELDS:
            cols[f].append(s.get(f))
    schema = pa.schema(
        [("shot_id", pa.int32()), ("phase", pa.int8())]
        + [(f, pa.int32() if f == "t" else pa.float64()) for f in SAMPLE_FIELDS]
    )
    return pa.table(cols, schema=schema)


def _notes(cfg: Config, rec) -> dict:
    if not rec.json_path:
        return {}
    try:
        return json.loads((cfg.archive_dir / rec.json_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        log.warning("%s: notes unreadable: %s", rec.padded_id, e)
        return {}


def run(cfg: Config, rebuild: bool = False) -> int:
    """Parse new shots (all, when rebuild=True). Returns shots parsed."""
    manifest = Manifest.load(cfg.manifest_path)
    samples_dir = cfg.parquet_dir / "samples"
    samples_dir.mkdir(parents=True, exist_ok=True)

    parsed = 0
    shot_rows: list[dict] = []
    manifest_dirty = False

    # Iteration is by manifest KEY (== padded id for epoch 1; epoch-prefixed
    # for later device-counter lives). Sample files are named by key so a
    # reborn shot id can never overwrite an archived shot's samples.
    for key, rec in sorted(manifest.shots.items()):
        if not rec.slog_path:
            continue
        sample_path = samples_dir / f"{key}.parquet"
        need_samples = rebuild or not sample_path.exists()

        try:
            shot = _parse_slog(cfg, rec)
        except (ValueError, OSError) as e:
            log.error("%s: parse FAILED: %s", key, e)
            if rec.parse_ok is not False:
                rec.parse_ok = False
                manifest_dirty = True
            # Failed captures stay VISIBLE (2026-08-22): a minimal row from
            # manifest facts, so a corrupt shot is a queryable record with
            # parse_ok=false - never a ghost the MCP cannot see.
            shot_rows.append({
                "shot_id": rec.id, "padded_id": rec.padded_id,
                "device_epoch": rec.epoch,
                "timestamp": rec.timestamp,
                "profile_id": rec.profile_id, "profile_name": rec.profile_name,
                "duration": rec.duration, "sample_interval_ms": None,
                "sample_count": 0, "slog_version": None, "final_weight": None,
                "rating": rec.rating or None, "incomplete": True,
                "phase_count": 0, "notes_text": None, "notes_json": None,
                "parse_ok": False,
            })
            continue

        # A clean parse of the newest known format clears a standing FORMAT
        # DRIFT pause (the drift proved out in practice) - phase 2 resumes.
        if (manifest.device.get("format_changed_at")
                and shot.version == max(manifest.device.get("slog_versions") or [shot.version])):
            log.warning("FORMAT DRIFT resolved: shot %s parsed clean under the new format", key)
            manifest.device.pop("format_changed_at", None)
            manifest_dirty = True

        # First sighting of a new .slog version = firmware format change
        # (v1.8.1 writes V5; the vendored parser handles V4/V5).
        seen = manifest.device.setdefault("slog_versions", [])
        if shot.version not in seen:
            seen.append(shot.version)
            seen.sort()
            manifest_dirty = True
            if len(seen) > 1:
                msg = f"new .slog format version {shot.version} first seen on shot {key} (known: {seen})"
                log.error("FORMAT DRIFT: %s", msg)
                notify(msg)

        if need_samples:
            pq.write_table(_samples_table(shot), sample_path)
            parsed += 1

        # A 0-sample shot is a degenerate capture, not a valid archive of a
        # real shot (receipt 2026-08-19: shot 286 archived mid-write as a
        # bare 512-byte header - "verified" against its own truncated
        # download). parse_ok=False makes sync re-fetch it (plan()'s heal).
        ok = shot.sample_count > 0
        if not ok:
            log.warning("%s: degenerate capture (0 samples) - flagged for re-fetch", key)
        if rec.parse_ok is not ok or rec.sample_count != shot.sample_count:
            rec.parse_ok = ok
            rec.sample_count = shot.sample_count
            manifest_dirty = True

        notes = _notes(cfg, rec)
        shot_rows.append({
            "shot_id": rec.id,
            "padded_id": rec.padded_id,
            "device_epoch": rec.epoch,
            "timestamp": rec.timestamp,
            "profile_id": shot.profile_id or rec.profile_id,
            "profile_name": shot.profile_name or rec.profile_name,
            "duration": shot.duration,
            "sample_interval_ms": shot.sample_interval,
            "sample_count": shot.sample_count,
            "slog_version": shot.version,
            "final_weight": shot.weight,
            "rating": rec.rating or None,
            "incomplete": shot.incomplete,
            "phase_count": len(shot.phases),
            # Notes schema is firmware-owned; keep the raw JSON alongside any
            # fields we recognize so nothing is lost if it grows.
            "notes_text": notes.get("notes"),
            "notes_json": json.dumps(notes, ensure_ascii=False) if notes else None,
            "parse_ok": ok,
        })

    if shot_rows:
        shots_table = pa.Table.from_pylist(shot_rows)
        pq.write_table(shots_table, cfg.parquet_dir / "shots.parquet")

    if manifest_dirty:
        manifest.save(cfg.manifest_path)

    log.info("parse done: %d sample files written, %d shots in shots.parquet",
             parsed, len(shot_rows))
    return parsed
