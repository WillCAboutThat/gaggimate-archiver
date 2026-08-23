"""Sync: pull new shots off the device into the raw archive, verified.

Verification at archive time (ARCHITECTURE.md, phase 1):
  download -> hash in flight -> temp write -> rename -> read back from the
  archive filesystem -> hashes must match. Both files (.slog + .json notes)
  when the index says notes exist. Hashes land in the manifest; they are the
  evidence phase 2's verified deletion will re-check.

Never writes to the device. Tombstoned index entries (SHOT_FLAG_DELETED) are
recorded and never fetched.
"""

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from .config import Config
from .device import DeviceClient
from .manifest import Manifest, ShotRecord
from .notify import notify
from .vendor.gaggimate_mcp.parsers.index import IndexData, IndexEntry, parse_binary_index
from .vendor.gaggimate_mcp.parsers.shot import parse_binary_shot

log = logging.getLogger("archiver.sync")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _shot_dir(cfg: Config, entry: IndexEntry) -> Path:
    ts = datetime.fromtimestamp(entry.timestamp, tz=timezone.utc)
    return cfg.raw_dir / f"{ts.year:04d}" / f"{ts.month:02d}"


def _write_verified(path: Path, data: bytes, expect_sha: str) -> None:
    """Temp-then-rename, then read BACK from the target filesystem and
    compare hashes. On CIFS this is the guard against a write that claimed
    success and stored nothing (the 2026-07-28 volume-backup incident class).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)
    readback = path.read_bytes()
    if _sha256(readback) != expect_sha:
        raise IOError(f"read-back hash mismatch for {path} - archive filesystem unreliable")


def fetch_and_parse_index(client: DeviceClient) -> IndexData:
    raw = client.fetch_index()
    index = parse_binary_index(raw)
    log.info(
        "index: version=%d entries=%d next_id=%d bytes=%d",
        index.header.version, index.header.entry_count, index.header.next_id, len(raw),
    )
    return index


def plan(cfg: Config, index: IndexData, manifest: Manifest) -> tuple[list[IndexEntry], list[IndexEntry]]:
    """Split index entries into (to_archive, tombstoned). A verified shot
    whose parse flagged a degenerate capture (parse_ok False - the mid-write
    race) is re-fetched while inside the recheck window: the device usually
    still holds the complete file, so this heals truncation automatically.
    The window bounds the churn for shots that are genuinely degenerate."""
    now_ts = datetime.now(timezone.utc).timestamp()
    to_archive: list[IndexEntry] = []
    tombstoned: list[IndexEntry] = []
    for entry in index.entries:
        padded = DeviceClient.padded(entry.id)
        if entry.deleted:
            tombstoned.append(entry)
            continue
        rec = manifest.shots.get(padded)
        if rec and rec.verified_at:
            heal = (rec.parse_ok is False
                    and not rec.accepted_degenerate
                    and (now_ts - entry.timestamp) <= cfg.notes_recheck_days * 86400)
            if not heal:
                continue
            log.info("%s: re-fetching (parse flagged degenerate capture)", padded)
        to_archive.append(entry)
    return to_archive, tombstoned


def _canonical_notes(notes_doc: dict) -> bytes:
    """The archived notes artifact is OUR canonical serialization of the
    WebSocket document (sorted keys, UTF-8): notes are not HTTP-fetchable,
    so byte-identity with the device's own file is unobtainable - what we
    verify (now, and at phase-2 delete time) is re-fetch -> re-canonicalize
    -> hash match."""
    return json.dumps(notes_doc, ensure_ascii=False, sort_keys=True).encode("utf-8")


def _store_notes(cfg: Config, rec: ShotRecord, notes_doc: dict) -> bool:
    """Canonicalize + verify-write the notes for an archived shot.
    Returns True when the stored copy changed."""
    data = _canonical_notes(notes_doc)
    sha = _sha256(data)
    if sha == rec.json_sha256:
        return False
    npath = Path(cfg.archive_dir) / rec.slog_path.replace(".slog", ".json")
    _write_verified(npath, data, sha)
    rec.json_path = npath.relative_to(cfg.archive_dir).as_posix()
    rec.json_sha256 = sha
    rec.json_size = len(data)
    return True


def reconcile_archived(cfg: Config, index: IndexData, manifest: Manifest,
                       dry_run: bool = False) -> tuple[int, dict[str, ShotRecord]]:
    """Late metadata catch-up (the phone-notes window): notes are typically
    added AFTER a shot was archived, and ratings can change any time. For
    already-verified shots, apply index rating drift and compute which shots
    need a notes fetch: missing notes, or recent ones (<= notes_recheck_days)
    that may have been edited - the index carries no notes hash, so recency
    is the only cheap change signal. Device quirk (ShotHistoryPlugin):
    SHOT_FLAG_HAS_NOTES is only set when the note carries a rating > 0 -
    a text-only unrated note is invisible here (README house rule: star it).
    Returns (ratings_updated, {padded_id: rec needing notes fetch})."""
    now_ts = datetime.now(timezone.utc).timestamp()
    updated = 0
    needs_notes: dict[str, ShotRecord] = {}
    for entry in index.entries:
        if entry.deleted:
            continue
        rec = manifest.shots.get(DeviceClient.padded(entry.id))
        if not rec or not rec.verified_at:
            continue
        if entry.rating != rec.rating:
            if dry_run:
                log.info("would reconcile %s rating %s -> %s",
                         rec.padded_id, rec.rating, entry.rating)
            else:
                log.info("%s: rating %s -> %s", rec.padded_id, rec.rating, entry.rating)
                rec.rating = entry.rating
                manifest.save(cfg.manifest_path)
            updated += 1
        if entry.has_notes and (
                not rec.json_sha256
                or (now_ts - entry.timestamp) <= cfg.notes_recheck_days * 86400):
            needs_notes[rec.padded_id] = rec
    return updated, needs_notes


def _fetch_and_store_notes(cfg: Config, manifest: Manifest,
                           needs: dict[str, ShotRecord]) -> tuple[int, int]:
    """One WS session for all pending notes; per-shot tolerant. A notes
    failure NEVER fails the sync (receipt 2026-08-19: one HTML-instead-of-
    JSON response 500'd the whole cycle) - the .slog durability path is
    independent, and deferred notes are retried every cycle by design.
    Returns (stored, deferred)."""
    if not needs:
        return 0, 0
    from .device import fetch_notes_ws
    try:
        docs = fetch_notes_ws(cfg.host, sorted(needs))
    except Exception as e:
        log.warning("notes fetch deferred for %d shot(s): %s", len(needs), e)
        return 0, len(needs)
    stored = deferred = 0
    for pid, rec in sorted(needs.items()):
        doc = docs.get(pid)
        if doc is None:
            log.warning("%s: index says has_notes but WS returned no notes", pid)
            deferred += 1
            continue
        try:
            if _store_notes(cfg, rec, doc):
                log.info("%s: notes archived/updated (%d bytes)", pid, rec.json_size)
                manifest.save(cfg.manifest_path)
                stored += 1
        except Exception as e:
            log.warning("%s: notes store failed, deferred: %s", pid, e)
            deferred += 1
    return stored, deferred


def run(cfg: Config, dry_run: bool = False, limit: int | None = None) -> dict:
    """Returns a summary dict: {"archived": n, "reconciled": n, "tombstoned": n}."""
    manifest = Manifest.load(cfg.manifest_path)
    client = DeviceClient(cfg.host, timeout=cfg.request_timeout, delay=cfg.request_delay)
    try:
        index = fetch_and_parse_index(client)

        # Format-drift check: index.bin's header version only moves when new
        # firmware is flashed. Detect it the moment it happens; phase 2 must
        # refuse to delete until a clean verified cycle on the new format.
        prev = manifest.device.get("index_version")
        if prev is not None and prev != index.header.version:
            msg = (f"index.bin format version changed {prev} -> "
                   f"{index.header.version} (firmware updated?) - re-verify "
                   f"parsers before trusting derived data")
            log.error("FORMAT DRIFT: %s", msg)
            notify(msg)
            if not dry_run:
                manifest.device["format_changed_at"] = _now()
        if not dry_run and prev != index.header.version:
            manifest.device["index_version"] = index.header.version
            manifest.save(cfg.manifest_path)

        to_archive, tombstoned = plan(cfg, index, manifest)

        # Record tombstones (never fetched; a tombstone for a shot we never
        # archived is data the firmware or its cleanup reaper already took).
        tombstone_dirty = False
        for entry in tombstoned:
            padded = DeviceClient.padded(entry.id)
            rec = manifest.shots.get(padded)
            if rec:
                if not rec.tombstone_seen:
                    rec.tombstone_seen = True
                    tombstone_dirty = True
            else:
                log.warning("shot %s is deleted on device and was never archived (lost)", padded)

        oldest_first = sorted(to_archive, key=lambda e: e.timestamp)
        if limit is not None:
            oldest_first = oldest_first[:limit]

        if dry_run:
            for entry in oldest_first:
                log.info(
                    "would archive %s  ts=%s  profile=%r  notes=%s",
                    DeviceClient.padded(entry.id),
                    datetime.fromtimestamp(entry.timestamp, tz=timezone.utc).isoformat(),
                    entry.profile_name,
                    entry.has_notes,
                )
            would_reconcile, would_notes = reconcile_archived(cfg, index, manifest, dry_run=True)
            log.info(
                "dry-run: %d to archive, %d rating updates, %d notes fetches, "
                "%d already archived, %d tombstoned",
                len(oldest_first),
                would_reconcile,
                len(would_notes),
                sum(1 for r in manifest.shots.values() if r.verified_at),
                len(tombstoned),
            )
            return {"would_archive": len(oldest_first), "would_reconcile": would_reconcile,
                    "tombstoned": len(tombstoned)}

        if tombstone_dirty:
            manifest.save(cfg.manifest_path)

        archived = 0
        new_shot_notes: dict[str, ShotRecord] = {}
        for entry in oldest_first:
            padded = DeviceClient.padded(entry.id)
            shot_dir = _shot_dir(cfg, entry)
            prev = manifest.shots.get(padded)  # provisional or heal re-fetch

            slog = client.fetch_slog(entry.id)
            slog_sha = _sha256(slog)
            slog_path = shot_dir / f"{padded}.slog"
            _write_verified(slog_path, slog, slog_sha)

            # A re-fetch that CHANGED the bytes invalidates the derived
            # per-shot samples file (parse only writes missing ones, so a
            # healed slog would otherwise keep its stale 0-row parquet).
            if prev is not None and prev.slog_sha256 != slog_sha:
                stale = cfg.parquet_dir / "samples" / f"{padded}.parquet"
                stale.unlink(missing_ok=True)

            # Mid-write race guard: a shot's index entry appears while its
            # .slog may still be flushing (measured 2026-08-19: shot 286
            # fetched as a bare 512-byte header seconds after the pull -
            # the button-right-after-a-shot pattern makes this LIKELY, not
            # rare). If the bytes parse to 0 samples while the index says
            # the shot had duration, store provisionally (no verified_at):
            # plan() re-fetches un-verified shots next cycle.
            complete = True
            if entry.duration > 0:
                try:
                    complete = parse_binary_shot(slog, padded).sample_count > 0
                except ValueError:
                    complete = False
            # Terminal state (2026-08-22, the WiFi-drop corpses): a degenerate
            # capture whose bytes are IDENTICAL across two fetches is what the
            # device actually holds - archival duty is to hold it faithfully,
            # not to retry forever. Accept: verified, parse_ok stays False as
            # the quality record, the loop ends.
            accepted = False
            if not complete and prev is not None and prev.slog_sha256 == slog_sha:
                accepted = True
                complete = True
                log.warning("%s: degenerate capture is byte-stable across fetches "
                            "(%d bytes) - accepting as what the device holds",
                            padded, len(slog))
            elif not complete:
                log.warning("%s: capture looks mid-write (%d bytes, index duration %d ms) - "
                            "provisional, re-fetching next cycle", padded, len(slog), entry.duration)

            now = _now()
            rec = ShotRecord(
                id=entry.id,
                padded_id=padded,
                timestamp=entry.timestamp,
                profile_id=entry.profile_id,
                profile_name=entry.profile_name,
                duration=entry.duration,
                rating=entry.rating,
                slog_path=slog_path.relative_to(cfg.archive_dir).as_posix(),
                slog_sha256=slog_sha,
                slog_size=len(slog),
                archived_at=now,
                verified_at=now if complete else "",
                accepted_degenerate=accepted,
            )
            manifest.shots[padded] = rec
            manifest.save(cfg.manifest_path)
            archived += 1
            if entry.has_notes:
                new_shot_notes[padded] = rec
            log.info("archived %s (%d bytes)", padded, len(slog))

        reconciled, needs_notes = reconcile_archived(cfg, index, manifest)
        needs_notes.update(new_shot_notes)
        notes_stored, notes_deferred = _fetch_and_store_notes(cfg, manifest, needs_notes)

        log.info("sync done: %d archived, %d reconciled, %d notes stored (%d deferred), "
                 "%d tombstoned, %d total in manifest",
                 archived, reconciled, notes_stored, notes_deferred,
                 len(tombstoned), len(manifest.shots))
        cfg.last_success_path.parent.mkdir(parents=True, exist_ok=True)
        cfg.last_success_path.write_text(_now() + "\n", encoding="utf-8")
        return {"archived": archived, "reconciled": reconciled,
                "notes_stored": notes_stored, "notes_deferred": notes_deferred,
                "tombstoned": len(tombstoned)}
    finally:
        client.close()
