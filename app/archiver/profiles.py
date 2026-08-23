"""Daily profile snapshot over the WebSocket, plus firmware-version pickup.

Profiles are also lost on firmware updates, so they get the same durability
treatment as shots: one dated JSON snapshot per day under profiles/.
The same short session requests res:ota-settings - the ONLY place v1.8.1
reports its firmware versions - which lands in the manifest's device ledger
as corroborating evidence for the format-drift detector (ARCHITECTURE.md).

One connection, two requests, closed immediately: the display heap is
fragile, so the session stays as short as the API allows. The device also
pushes unsolicited evt:/res: traffic (status broadcasts) on the same socket;
responses are matched on tp, tolerating interleaved noise.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import websockets

from .config import Config
from .manifest import Manifest
from .notify import notify

log = logging.getLogger("archiver.profiles")

WS_TIMEOUT = 20  # whole-session budget, seconds


async def _request(ws, tp: str, rid: str, want_tp: str) -> dict:
    await ws.send(json.dumps({"tp": tp, "rid": rid}))
    while True:
        msg = json.loads(await ws.recv())
        if msg.get("tp") == want_tp:
            return msg


async def _snapshot(cfg: Config) -> tuple[dict, dict]:
    uri = f"ws://{cfg.host}/ws"
    async with websockets.connect(uri, open_timeout=10, close_timeout=5) as ws:
        profiles = await _request(ws, "req:profiles:list", "arch-p", "res:profiles:list")
        ota = await _request(ws, "req:ota-settings", "arch-o", "res:ota-settings")
    return profiles, ota


def run(cfg: Config) -> Path | None:
    """Snapshot profiles for today; skip if today's file exists. Returns the
    file written (or None on skip)."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out = cfg.archive_dir / "profiles" / f"{today}.json"
    if out.exists():
        log.info("profiles snapshot for %s already exists", today)
        return None

    profiles, ota = asyncio.run(asyncio.wait_for(_snapshot(cfg), WS_TIMEOUT))

    if profiles.get("error"):
        raise RuntimeError(f"profiles:list error from device: {profiles['error']}")
    count = len(profiles.get("profiles", []))

    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".tmp")
    tmp.write_text(json.dumps(profiles, indent=1, ensure_ascii=False), encoding="utf-8")
    tmp.replace(out)
    log.info("profiles snapshot: %d profiles -> %s", count, out.name)

    # Firmware versions into the device ledger; a change is worth a loud
    # line (the structural detectors in sync/parse remain the authority).
    manifest = Manifest.load(cfg.manifest_path)
    dev = manifest.device
    changed = False
    for key, field in (("displayVersion", "display_version"),
                       ("controllerVersion", "controller_version")):
        new = ota.get(key)
        if new and dev.get(field) != new:
            if dev.get(field):
                msg = f"firmware {field} changed {dev[field]} -> {new}"
                log.error("FIRMWARE CHANGE: %s", msg)
                notify(msg)
            dev[field] = new
            changed = True
    if changed:
        manifest.save(cfg.manifest_path)
    return out
