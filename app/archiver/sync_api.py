"""Manual sync trigger: POST /sync, bearer-tokened, synchronous.

Exists for two consumers (ARCHITECTURE.md): an iPhone Shortcut button and the
archive MCP's sync_now tool - both just POST here. The handler runs a full
cycle (sync -> parse -> profiles) under the daemon's cycle lock and returns
the summary JSON, so the button literally answers "2 new shots archived".

Fail-closed: SYNC_TOKEN unset = listener never starts. A press while a
cycle is already running waits briefly for the lock, then 503s rather than
stacking cycles. Cost of a no-op press is one index.bin request to the
device - gentleness unaffected.
"""

import hmac
import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

log = logging.getLogger("archiver.sync_api")


def start_listener(port: int, token: str, run_cycle_locked) -> threading.Thread:
    """run_cycle_locked(wait_s) -> summary dict, or None when busy too long."""
    expected = f"Bearer {token}".encode()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args):  # route http.server chatter to our logger
            log.debug(fmt, *args)

        def _reply(self, status: int, payload: dict) -> None:
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            supplied = (self.headers.get("Authorization") or "").encode()
            if not hmac.compare_digest(supplied, expected):
                self._reply(401, {"error": "unauthorized"})
                return
            if self.path.rstrip("/") != "/sync":
                self._reply(404, {"error": "unknown path (POST /sync)"})
                return
            log.info("manual sync triggered")
            result = run_cycle_locked(60)
            if result is None:
                self._reply(503, {"error": "a cycle is already running; try again shortly"})
                return
            self._reply(200 if result.get("ok") else 500, result)

        def do_GET(self):
            self._reply(405, {"error": "POST /sync"})

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    thread = threading.Thread(target=server.serve_forever, name="sync-api", daemon=True)
    thread.start()
    log.info("sync trigger listening on :%d", port)
    return thread
