"""HTTP client for the GaggiMate shot-history API.

Written here rather than vendored: the archiver needs request pacing and
returns raw bytes for hashing/verification, which the upstream gaggimate-mcp
client does not expose. Endpoint shapes match upstream (api/http.py):
GET /api/history/index.bin, GET /api/history/<id zero-padded to 6>.slog.

The .json notes companion is NOT HTTP-fetchable: the firmware's static
handler serves .slog/.bin only, and a .json request falls through to the
web-UI catch-all - HTTP 200 with index.html (discovered live 2026-08-19,
first real note). Notes go over the WebSocket: req:history:notes:get ->
res:history:notes:get (fetch_notes_ws below).
"""

import asyncio
import json
import logging
import time

import httpx

from .vendor.gaggimate_mcp.parsers.shot import is_html_response

log = logging.getLogger("archiver.device")


class DeviceError(Exception):
    pass


class DeviceClient:
    def __init__(self, host: str, timeout: float = 15.0, delay: float = 1.0):
        self.base = f"http://{host}/api/history"
        self.delay = delay
        self._last = 0.0
        # No keep-alive: one short connection per request keeps pressure off
        # the fragmented display heap.
        self._client = httpx.Client(
            timeout=timeout,
            limits=httpx.Limits(max_keepalive_connections=0),
            headers={"Connection": "close"},
        )

    def close(self) -> None:
        self._client.close()

    def _get(self, path: str, ok_404: bool = False) -> bytes | None:
        wait = self.delay - (time.monotonic() - self._last)
        if wait > 0:
            time.sleep(wait)
        url = f"{self.base}/{path}"
        try:
            r = self._client.get(url)
        finally:
            self._last = time.monotonic()
        if ok_404 and r.status_code == 404:
            return None
        if r.status_code != 200:
            raise DeviceError(f"GET {url} -> HTTP {r.status_code}")
        data = r.content
        # Overloaded firmware serves its HTML UI instead of API bytes
        # (upstream saw this in the field; parsers/shot.py has the check).
        if is_html_response(data):
            raise DeviceError(f"GET {url} returned HTML, not binary (device busy?)")
        return data

    @staticmethod
    def padded(shot_id: int | str) -> str:
        return str(shot_id).zfill(6)

    def fetch_index(self) -> bytes:
        data = self._get("index.bin")
        assert data is not None
        return data

    def fetch_slog(self, shot_id: int | str) -> bytes:
        data = self._get(f"{self.padded(shot_id)}.slog")
        assert data is not None
        return data


def fetch_notes_ws(host: str, padded_ids: list[str], timeout: float = 60) -> dict[str, dict | None]:
    """Fetch notes documents over the WebSocket (the only channel that
    serves them). One short session, sequential requests - same gentleness
    posture as the profile snapshot.

    The id is tried UNPADDED first: the firmware opens "/h/" + id + ".json"
    verbatim with whatever id the writer sent, and the stock web UI sends
    unpadded ids (ShotNotesCard.jsx: id = shot.id = String(entry.id)) -
    despite docs/shot-notes-api.md showing padded examples. Padded is the
    fallback in case some writer followed the docs. (Side effect of the same
    firmware inconsistency: history:delete removes the PADDED name, so
    UI-written notes are orphaned by on-device deletion.)
    Returns {padded_id: notes dict | None (no/empty notes)}."""
    import websockets

    async def _run() -> dict[str, dict | None]:
        out: dict[str, dict | None] = {}
        async with websockets.connect(f"ws://{host}/ws",
                                      open_timeout=10, close_timeout=5) as ws:
            async def ask(req_id: str) -> dict | None:
                rid = f"arch-n{req_id}"
                await ws.send(json.dumps(
                    {"tp": "req:history:notes:get", "rid": rid, "id": req_id}))
                while True:
                    msg = json.loads(await ws.recv())
                    if msg.get("tp") == "res:history:notes:get" and msg.get("rid") == rid:
                        return msg.get("notes") or None

            for pid in padded_ids:
                doc = await ask(str(int(pid)))  # unpadded first (stock UI)
                if doc is None:
                    doc = await ask(pid)        # padded fallback (docs)
                out[pid] = doc
        return out

    return asyncio.run(asyncio.wait_for(_run(), timeout))
