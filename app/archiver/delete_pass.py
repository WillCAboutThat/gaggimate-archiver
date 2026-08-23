"""Phase 2: verified deletion from the device (see ARCHITECTURE.md).
Enable deliberately, after your archive has accumulated verified cycles
and you have spot-checked a rebuild-from-raw.

Rationale: relieve SPIFFS pressure under OUR protocol instead of the
firmware's unverified oldest-first reaper. Every deletion is preceded by
delete-time re-verification against the archive; a shot is only ever
removed from the device when the archive provably holds it.

Eligibility (ALL must hold; ARCHITECTURE.md phase 2):
1. Archived and verified in a PREVIOUS run, parse_ok true (blocks
   degenerate captures - including accepted corpses, which stay on the
   device for the firmware's own reaper).
2. Delete-time verification: fresh device .slog download AND fresh NAS
   read-back both match the manifest hash; the index's has_notes state
   matches the manifest, and when notes exist, a fresh WS fetch
   re-canonicalizes to the stored hash.
3. Older than DELETE_GRACE_DAYS and outside the KEEP_RECENT_SHOTS newest
   (the machine's own history screen stays useful).
4. At most DELETE_MAX_PER_RUN per cycle, oldest first, one gentle session.
5. Hard pause while manifest.device carries format_changed_at (FORMAT
   DRIFT unresolved - parse clears it once the new format proves out).

Execution: req:history:delete over one WebSocket session (unpadded id,
the stock-UI convention); tombstone confirmed on a fresh index fetch;
append-only deletions.log; device_deleted_at recorded in the manifest.
"""

import asyncio
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from .config import Config
from .device import DeviceClient, fetch_notes_ws
from .manifest import Manifest
from .notify import notify
from .sync import _canonical_notes
from .vendor.gaggimate_mcp.parsers.index import parse_binary_index

log = logging.getLogger("archiver.delete")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _ws_delete(host: str, ids: list[str], timeout: float = 60) -> dict[str, bool]:
    import websockets
    out: dict[str, bool] = {}

    async def _run():
        async with websockets.connect(f"ws://{host}/ws",
                                      open_timeout=10, close_timeout=5) as ws:
            for sid in ids:
                rid = f"arch-d{sid}"
                await ws.send(json.dumps(
                    {"tp": "req:history:delete", "rid": rid, "id": sid}))
                while True:
                    msg = json.loads(await ws.recv())
                    if msg.get("tp") == "res:history:delete" and msg.get("rid") == rid:
                        out[sid] = True
                        break

    asyncio.run(asyncio.wait_for(_run(), timeout))
    return out


def run(cfg: Config, dry_run: bool = False) -> dict:
    """One deletion pass. Returns a summary dict; never raises for
    per-shot verification failures (those shots are simply not deleted)."""
    manifest = Manifest.load(cfg.manifest_path)

    if manifest.device.get("format_changed_at"):
        log.warning("deletion paused: FORMAT DRIFT unresolved (format_changed_at=%s)",
                    manifest.device["format_changed_at"])
        return {"deleted": 0, "paused": "format-drift"}

    client = DeviceClient(cfg.host, timeout=cfg.request_timeout, delay=cfg.request_delay)
    try:
        index = parse_binary_index(client.fetch_index())
        live = [e for e in index.entries if not e.deleted]
        # Keep-recent window: the newest N stay on the device regardless.
        keep_ids = {e.id for e in sorted(live, key=lambda e: e.timestamp)[-cfg.keep_recent_shots:]}
        now_ts = datetime.now(timezone.utc).timestamp()

        candidates = []
        for entry in sorted(live, key=lambda e: e.timestamp):  # oldest first
            padded = DeviceClient.padded(entry.id)
            rec = manifest.shots.get(padded)
            if (rec is None or not rec.verified_at or rec.parse_ok is not True
                    or rec.device_deleted_at):
                continue
            if entry.id in keep_ids:
                continue
            if (now_ts - entry.timestamp) < cfg.delete_grace_days * 86400:
                continue
            if entry.has_notes != bool(rec.json_sha256):
                continue  # notes state diverged; sync reconciles first
            candidates.append((entry, rec))
            if len(candidates) >= cfg.delete_max_per_run:
                break

        if dry_run:
            for entry, rec in candidates:
                log.info("would delete %s (ts=%s, notes=%s) after re-verification",
                         rec.padded_id,
                         datetime.fromtimestamp(entry.timestamp, tz=timezone.utc).date(),
                         entry.has_notes)
            return {"deleted": 0, "dry_run": True, "eligible": len(candidates),
                    "kept_recent": len(keep_ids), "live_on_device": len(live)}

        # Delete-time verification, per shot; failures skip, never abort.
        verified: list[tuple] = []
        notes_needed = [rec.padded_id for entry, rec in candidates if entry.has_notes]
        notes_docs = fetch_notes_ws(cfg.host, notes_needed) if notes_needed else {}
        for entry, rec in candidates:
            try:
                fresh = client.fetch_slog(entry.id)
                if _sha256(fresh) != rec.slog_sha256:
                    log.warning("%s: device slog no longer matches manifest - re-sync first",
                                rec.padded_id)
                    continue
                nas = (cfg.archive_dir / rec.slog_path).read_bytes()
                if _sha256(nas) != rec.slog_sha256:
                    log.error("%s: NAS copy hash mismatch - NOT deleting (archive integrity!)",
                              rec.padded_id)
                    notify(f"phase2: NAS hash mismatch on {rec.padded_id}; deletion skipped")
                    continue
                if entry.has_notes:
                    doc = notes_docs.get(rec.padded_id)
                    if doc is None or _sha256(_canonical_notes(doc)) != rec.json_sha256:
                        log.warning("%s: notes verification failed - deferred", rec.padded_id)
                        continue
                verified.append((entry, rec))
            except Exception as e:
                log.warning("%s: delete-time verification error, skipped: %s",
                            rec.padded_id, e)

        if not verified:
            return {"deleted": 0, "eligible": len(candidates), "verified": 0}

        _ws_delete(cfg.host, [str(e.id) for e, _ in verified])

        # Confirm tombstones on a fresh index.
        index2 = parse_binary_index(client.fetch_index())
        gone = {e.id for e in index2.entries if e.deleted} | (
            {e.id for e in index.entries} - {e.id for e in index2.entries})

        deleted = 0
        log_path = cfg.archive_dir / "deletions.log"
        with open(log_path, "a", encoding="utf-8") as f:
            for entry, rec in verified:
                confirmed = entry.id in gone
                if confirmed:
                    rec.device_deleted_at = _now()
                    rec.tombstone_seen = True
                    deleted += 1
                f.write(json.dumps({
                    "shot_id": entry.id, "padded_id": rec.padded_id,
                    "slog_sha256": rec.slog_sha256, "json_sha256": rec.json_sha256,
                    "deleted_at": _now(), "confirmed": confirmed,
                }) + "\n")
        manifest.save(cfg.manifest_path)
        log.info("phase2: %d deleted (verified %d, eligible %d, keep-recent %d)",
                 deleted, len(verified), len(candidates), len(keep_ids))
        return {"deleted": deleted, "verified": len(verified),
                "eligible": len(candidates)}
    finally:
        client.close()
