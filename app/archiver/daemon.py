"""Long-running scheduler: the container entrypoint.

House convention is in-container scheduling (volume-backup's cron env; no
ofelia in the fleet). Times are HH:MM local (TZ env; the compose file sets
set TZ in your compose). A cycle is sync -> parse -> profiles snapshot (the
snapshot self-skips when today's file exists, so it self-heals missed days).
A failed cycle logs, notifies, and waits for the next slot - the daemon
never exits on a cycle failure, because a device that was busy at 06:00 is
usually fine at 18:00. Staleness is watchable via the last-success file.
"""

import logging
import os
import threading
import time
from datetime import datetime, timedelta

from . import parse as parse_step
from . import profiles as profiles_step
from . import sync as sync_step
from .config import Config
from .notify import notify
from .sync_api import start_listener

log = logging.getLogger("archiver.daemon")

# One cycle at a time, whoever asks: the scheduled loop and the manual
# trigger (sync_api) both run cycles through this lock.
_cycle_lock = threading.Lock()


def parse_schedule(spec: str) -> list[tuple[int, int]]:
    times = []
    for part in spec.split(","):
        hh, mm = part.strip().split(":")
        times.append((int(hh), int(mm)))
    if not times:
        raise ValueError(f"empty schedule: {spec!r}")
    return sorted(times)


def next_run(now: datetime, times: list[tuple[int, int]]) -> datetime:
    for hh, mm in times:
        candidate = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if candidate > now:
            return candidate
    hh, mm = times[0]
    return (now + timedelta(days=1)).replace(hour=hh, minute=mm, second=0, microsecond=0)


def cycle(cfg: Config) -> dict:
    out: dict = {"ok": True}
    try:
        out.update(sync_step.run(cfg))
    except Exception as e:
        log.exception("sync failed")
        notify(f"sync FAILED: {e}")
        out["ok"] = False
        out["sync_error"] = str(e)
    try:
        out["parsed"] = parse_step.run(cfg)
    except Exception as e:
        log.exception("parse failed")
        notify(f"parse FAILED: {e}")
        out["ok"] = False
        out["parse_error"] = str(e)
    try:
        out["profiles_snapshotted"] = profiles_step.run(cfg) is not None
    except Exception as e:
        log.exception("profiles snapshot failed")
        notify(f"profiles snapshot FAILED: {e}")
        out["ok"] = False
        out["profiles_error"] = str(e)
    # Phase 2 (verified deletion) runs ONLY after a fully clean cycle -
    # deletion never outruns archiving - and only when enabled.
    if out["ok"] and cfg.delete_enabled:
        try:
            from . import delete_pass
            out["phase2"] = delete_pass.run(cfg)
        except Exception as e:
            log.exception("phase2 delete pass failed")
            notify(f"phase2 delete pass FAILED: {e}")
            out["phase2_error"] = str(e)
    return out


def cycle_locked(cfg: Config, wait_s: float = 60) -> dict | None:
    """Run one cycle under the lock; None when another cycle held it too long."""
    if not _cycle_lock.acquire(timeout=wait_s):
        return None
    try:
        return cycle(cfg)
    finally:
        _cycle_lock.release()


def run(cfg: Config) -> None:
    times = parse_schedule(os.environ.get("SYNC_SCHEDULE", "06:00,18:00"))
    log.info("daemon up: schedule %s (local time), archive %s",
             ",".join(f"{h:02d}:{m:02d}" for h, m in times), cfg.archive_dir)

    # Manual trigger (iPhone Shortcut / MCP sync_now). Fail-closed: no
    # token, no listener.
    sync_token = os.environ.get("SYNC_TOKEN", "")
    if sync_token:
        start_listener(int(os.environ.get("SYNC_PORT", "8092")), sync_token,
                       lambda wait_s: cycle_locked(cfg, wait_s))
    else:
        log.info("SYNC_TOKEN unset - manual sync trigger disabled")

    if os.environ.get("RUN_ON_START", "true").lower() in ("1", "true", "yes"):
        log.info("run-on-start cycle")
        cycle_locked(cfg)

    while True:
        target = next_run(datetime.now(), times)
        log.info("next cycle at %s", target.isoformat(timespec="minutes"))
        while True:
            remaining = (target - datetime.now()).total_seconds()
            if remaining <= 0:
                break
            time.sleep(min(remaining, 300))
        cycle_locked(cfg, wait_s=600)
