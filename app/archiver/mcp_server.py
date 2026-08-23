"""The archive MCP server: AI reasoning over the full shot history.

Remote (streamable HTTP) by design - it runs in a container next to the
data, so it is always current; a local stdio copy would silently serve a
stale archive (ARCHITECTURE.md). Read-only by construction except the two
append-only meta logs (beans, prep), which live under meta/ and never touch
raw/, parquet/, or the manifest - the container enforces this structurally
(archive mounted ro, meta/ mounted rw).

Auth: a static bearer token (MCP_TOKEN, required - the server refuses to
start without one). Checked by plain ASGI middleware rather than the SDK's
OAuth machinery: one user, one token, no identity provider to stand up.
The SDK's DNS-rebinding Host check is disabled - it exists to protect
localhost servers from hostile web pages, and it would reject the LAN/
tailnet hostnames this server is for; the bearer token is the gate.
"""

import logging
import os
import sys

from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings

from . import archive_read as ar
from .archive_read import append_jsonl
from .config import Config

log = logging.getLogger("archiver.mcp")

cfg = Config()

server = MCPServer(
    "gaggimate-archive",
    instructions=(
        "Espresso shot history from a GaggiMate espresso machine, archived "
        "with verified integrity. Every shot carries its covariates: bean lot (with "
        "days off roast), grind setting, and dose - use them before attributing "
        "a change to prep or profile. Weighed shots (scale connected) are real "
        "espresso; on single-boiler machines, unweighed pulls are typically "
        "boiler flushes and should be excluded from taste reasoning. Start with "
        "list_shots or get_shot; use query for anything the curated tools "
        "don't answer (schema in its description)."),
)


@server.tool()
def list_shots(days: int = 14, profile: str | None = None,
               weighed_only: bool = True, limit: int = 30) -> list[dict]:
    """Recent shots, newest first: one compact row each (time, profile,
    duration, weight, brew ratio, peak bar, grind, dose, rating, notes flag).
    `profile` is a case-insensitive substring. Set weighed_only=False to
    include boiler flushes."""
    return ar.list_shots(cfg, days=days, profile=profile,
                         weighed_only=weighed_only, limit=limit)


@server.tool()
def get_shot(shot_id: str = "latest", include_curves: bool = False) -> dict:
    """One shot's full story: headline metrics (duration, weight, ratio,
    peak pressure, extraction temp and temp error, time to first drops),
    bean lot + grind + dose in effect, tasting notes if any, and a per-phase
    table (named phases with pressure/flow/temp/puck-resistance/grams).
    include_curves adds downsampled (~80-point) pressure/flow/temp/weight
    series - only ask for curves when phase stats aren't enough.
    shot_id 'latest' = newest weighed shot."""
    return ar.get_shot(cfg, shot_id=shot_id, include_curves=include_curves)


@server.tool()
def compare_shots(shot_a: str, shot_b: str) -> dict:
    """Two shots side by side: both headlines (with their bean/grind/dose),
    headline deltas (a minus b), and both per-phase tables. Facts only -
    interpretation is the caller's job."""
    return ar.compare_shots(cfg, shot_a, shot_b)


@server.tool()
def benchmark_shot(shot_id: str = "latest", days: int = 60) -> dict:
    """One shot against its own profile's recent distribution: per metric
    (duration, weight, ratio, peak bar, extraction temp, first drops) the
    value, profile mean/sd, and percentile. The mechanical half of 'how did
    this compare to average - and why?'"""
    return ar.benchmark_shot(cfg, shot_id=shot_id, days=days)


@server.tool()
def profile_stats(profile: str, days: int = 30) -> dict:
    """A profile's recent record: per-metric distributions, a by-day shot
    list (dial-in trajectory), and the prep + beans currently in effect.
    Evidence base for 'what should we adjust?'"""
    return ar.profile_stats(cfg, profile=profile, days=days)


@server.tool()
def list_profiles() -> list[dict]:
    """All profiles on the machine (from the newest daily snapshot) with
    type, temperature, phase count, and archived shot counts."""
    return ar.list_profiles(cfg)


@server.tool()
def get_profile(name: str) -> dict:
    """A profile's full machine definition - every phase with targets,
    durations, transitions, pump mode - plus the snapshot dates where the
    definition changed. The anchor for designing a child or derived
    profile."""
    return ar.get_profile(cfg, name)


@server.tool()
def query(sql: str) -> dict:
    """Read-only DuckDB over the archive, for questions the curated tools
    don't answer. Views: shots(shot_id, padded_id, timestamp epoch-seconds,
    profile_id, profile_name, duration ms, sample_interval_ms, sample_count,
    slog_version, final_weight g, rating, incomplete, phase_count,
    notes_text, notes_json, parse_ok - false = corrupt/degenerate capture,
    curves unavailable) and samples(shot_id, phase, t ms, tt, ct target/
    current temp C, tp, cp target/current pressure bar, fl, tf, pf, vf flows
    ml/s, v, ev weight g, pr puck resistance). Weighed espresso =
    final_weight IS NOT NULL; unweighed rows are boiler flushes. One SELECT/
    WITH statement, 200-row cap."""
    return ar.run_query(cfg, sql)


@server.tool()
def sync_now() -> dict:
    """Trigger an immediate verified sync of the device (sync -> parse ->
    profiles) and return its summary. Use when the question concerns shots
    pulled since the last scheduled sync (06:00/18:00 CT) - e.g. 'how was
    this morning's shot?' before evening. Costs the device one index fetch
    plus downloads for any new shots; safe to call whenever freshness
    matters."""
    import httpx
    token = os.environ.get("SYNC_TOKEN", "")
    if not token:
        raise ValueError("sync trigger not configured on this deployment (SYNC_TOKEN unset)")
    url = os.environ.get("ARCHIVER_SYNC_URL", "http://gaggimate-archiver:8092/sync")
    r = httpx.post(url, headers={"Authorization": f"Bearer {token}"}, timeout=180)
    # A 500 carries the cycle summary WITH its error fields - that diagnosis
    # must reach the caller, not vanish into raise_for_status (receipt
    # 2026-08-19: a notes-fetch failure was undebuggable from the tool side).
    try:
        return r.json()
    except ValueError:
        raise ValueError(f"sync returned HTTP {r.status_code}: {r.text[:300]}")


@server.tool()
def log_beans(roaster: str, name: str, roast_level: str, roast_date: str,
              start_date: str | None = None, note: str | None = None) -> dict:
    """Record a new bean lot (one entry per bag). Dates are YYYY-MM-DD;
    start_date defaults to today and assigns this lot to all shots from that
    date until the next lot. Append-only, meta/ only."""
    from datetime import date, datetime as dt
    dt.strptime(roast_date, "%Y-%m-%d")
    start = start_date or date.today().isoformat()
    dt.strptime(start, "%Y-%m-%d")
    rec = {"logged_at": dt.now().astimezone().isoformat(timespec="seconds"),
           "start_date": start, "roaster": roaster, "name": name,
           "roast_level": roast_level, "roast_date": roast_date}
    if note:
        rec["note"] = note
    append_jsonl(cfg.archive_dir / "meta" / "beans.jsonl", rec)
    return {"logged": rec}


@server.tool()
def log_prep(grind: str | None = None, dose_g: float | None = None,
             effective_from: str | None = None, grinder: str | None = None,
             note: str | None = None) -> dict:
    """Record a grind and/or dose change. Settings are sticky: they apply
    from effective_from (ISO datetime; default now, may be in the past for
    retroactive capture) until the next change. Unlogged = unchanged;
    dose defaults to 18 g before any event. Append-only, meta/ only."""
    from datetime import datetime as dt
    if grind is None and dose_g is None:
        raise ValueError("log at least one of grind / dose_g")
    eff = (dt.fromisoformat(effective_from) if effective_from
           else dt.now()).astimezone()
    rec = {"logged_at": dt.now().astimezone().isoformat(timespec="seconds"),
           "effective_from": eff.isoformat(timespec="seconds")}
    if grind is not None:
        rec["grind"] = grind
    if dose_g is not None:
        rec["dose_g"] = dose_g
    if grinder:
        rec["grinder"] = grinder
    if note:
        rec["note"] = note
    append_jsonl(cfg.archive_dir / "meta" / "prep.jsonl", rec)
    return {"logged": rec}


class BearerAuth:
    """Minimal ASGI middleware: every HTTP request must carry the token."""

    def __init__(self, app, token: str):
        self.app = app
        self.expected = f"Bearer {token}".encode()

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            import hmac
            headers = dict(scope.get("headers") or [])
            supplied = headers.get(b"authorization", b"")
            if not hmac.compare_digest(supplied, self.expected):
                await send({"type": "http.response.start", "status": 401,
                            "headers": [(b"content-type", b"text/plain")]})
                await send({"type": "http.response.body", "body": b"unauthorized"})
                return
        await self.app(scope, receive, send)


def main() -> int:
    logging.basicConfig(level=cfg.log_level, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    token = os.environ.get("MCP_TOKEN", "")
    if not token:
        log.error("MCP_TOKEN is required; refusing to serve the archive unauthenticated")
        return 1
    port = int(os.environ.get("MCP_PORT", "8091"))
    host = os.environ.get("MCP_HOST", "0.0.0.0")
    app = server.streamable_http_app(
        stateless_http=True,
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=False),
    )
    import uvicorn
    log.info("archive MCP up on %s:%d (archive %s)", host, port, cfg.archive_dir)
    uvicorn.run(BearerAuth(app, token), host=host, port=port, log_level="warning")
    return 0


if __name__ == "__main__":
    sys.exit(main())
