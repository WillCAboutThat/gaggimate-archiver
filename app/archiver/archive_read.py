"""Read layer for the archive MCP: curated, token-shaped answers.

Design rule (ARCHITECTURE.md): tools deliver deterministic facts shaped for a
reasoner - summaries and per-phase tables by default, raw curves only on
explicit request, everything joined to the meta/ covariates (beans, grind,
dose) so comparisons can't misattribute. All reads; the only writes in this
module are the two append-only meta logs, which never touch raw/parquet/
manifest.

DuckDB connections are per-call: the archiver rewrites Parquet twice a day
under this server, and a fresh connection is cheap next to a stale answer.
"""

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

from .config import Config
from .vendor.gaggimate_mcp.parsers.shot import parse_binary_shot

LOCAL_TZ = ZoneInfo(os.environ.get("TZ", "UTC"))
DOSE_DEFAULT_G = float(os.environ.get("DOSE_DEFAULT_G", "18.0"))  # log_prep events override
SQL_FORBIDDEN = re.compile(
    r"\b(attach|copy|export|install|load|create|insert|update|delete|drop|alter|pragma|set)\b",
    re.IGNORECASE)


def _local(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=LOCAL_TZ).isoformat(timespec="minutes")


def _con(cfg: Config) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    shots = (cfg.parquet_dir / "shots.parquet").as_posix()
    samples = (cfg.parquet_dir / "samples" / "*.parquet").as_posix()
    con.execute(f"CREATE VIEW shots AS SELECT * FROM '{shots}'")
    con.execute(f"CREATE VIEW samples AS SELECT * FROM '{samples}'")
    return con


def _jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def append_jsonl(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def prep_at(cfg: Config, ts: int) -> dict:
    """Grind/dose effective at a shot's timestamp: the latest log_prep event
    at or before it, per field (sticky - unlogged means unchanged)."""
    when = datetime.fromtimestamp(ts, tz=timezone.utc)
    grind = None
    dose = DOSE_DEFAULT_G
    grinder = None
    for ev in sorted(_jsonl(cfg.archive_dir / "meta" / "prep.jsonl"),
                     key=lambda e: e["effective_from"]):
        if datetime.fromisoformat(ev["effective_from"]) > when:
            break
        if ev.get("grind") is not None:
            grind = ev["grind"]
        if ev.get("dose_g") is not None:
            dose = ev["dose_g"]
        if ev.get("grinder"):
            grinder = ev["grinder"]
    out = {"grind": grind, "dose_g": dose}
    if grinder:
        out["grinder"] = grinder
    return out


def beans_at(cfg: Config, ts: int) -> dict | None:
    """The bean lot active at a shot's timestamp: latest lot whose start_date
    is on or before the shot's local date."""
    shot_date = datetime.fromtimestamp(ts, tz=LOCAL_TZ).date()
    active = None
    for lot in sorted(_jsonl(cfg.archive_dir / "meta" / "beans.jsonl"),
                      key=lambda e: e["start_date"]):
        if datetime.fromisoformat(lot["start_date"]).date() <= shot_date:
            active = lot
    if not active:
        return None
    out = {k: active[k] for k in ("roaster", "name", "roast_level") if active.get(k)}
    if active.get("roast_date"):
        off = (shot_date - datetime.fromisoformat(active["roast_date"]).date()).days
        out["roast_date"] = active["roast_date"]
        out["days_off_roast"] = off
    return out


def _manifest_rec(shots: dict, padded: str) -> dict | None:
    """Find a shot's manifest record by device padded id, preferring the
    newest epoch. Epoch 1 keys by plain padded id; later device-counter
    lives prefix the key (manifest.shot_key) - MCP callers only know the
    device id, so scan for the prefixed variants."""
    best = None
    plain = shots.get(padded)
    if plain:
        best = (int(plain.get("epoch", 1)), plain)
    suffix = "-" + padded
    for k, v in shots.items():
        if k.startswith("e") and k.endswith(suffix):
            try:
                ep = int(k[1:k.index("-")])
            except ValueError:
                continue
            if best is None or ep > best[0]:
                best = (ep, v)
    return best[1] if best else None


def _notes_text(cfg: Config, padded: str) -> str | None:
    manifest = json.loads((cfg.archive_dir / "manifest.json").read_text(encoding="utf-8"))
    rec = _manifest_rec(manifest["shots"], padded)
    if not rec or not rec.get("json_path"):
        return None
    try:
        data = json.loads((cfg.archive_dir / rec["json_path"]).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data.get("notes") or (json.dumps(data, ensure_ascii=False) if data else None)


def resolve_shot_id(cfg: Config, shot_id: str) -> int:
    with _con(cfg) as con:
        if str(shot_id).lower() == "latest":
            row = con.execute(
                "SELECT shot_id FROM shots WHERE final_weight IS NOT NULL "
                "ORDER BY timestamp DESC LIMIT 1").fetchone()
            if not row:
                raise ValueError("archive holds no weighed shots")
            return row[0]
        row = con.execute("SELECT shot_id FROM shots WHERE shot_id = ?",
                          [int(shot_id)]).fetchone()
        if not row:
            raise ValueError(f"shot {shot_id} not in archive")
        return row[0]


def _headline(cfg: Config, con, shot_id: int) -> dict:
    s = con.execute("""
        SELECT s.shot_id, s.timestamp, s.profile_name, s.duration/1000.0,
               s.final_weight, s.rating, s.incomplete, s.sample_count,
               a.peak_cp, a.avg_extract_ct, a.avg_temp_err, a.first_drops_s
        FROM shots s LEFT JOIN (
            SELECT shot_id, max(cp) peak_cp,
                   avg(ct) FILTER (phase >= 2) avg_extract_ct,
                   avg(abs(ct - tt)) FILTER (phase >= 2) avg_temp_err,
                   min(t) FILTER (v > 0.3) / 1000.0 first_drops_s
            FROM samples WHERE shot_id = ? GROUP BY shot_id
        ) a USING (shot_id) WHERE s.shot_id = ?""", [shot_id, shot_id]).fetchone()
    if not s:
        raise ValueError(f"shot {shot_id} not in archive")
    prep = prep_at(cfg, s[1])
    out = {
        "shot_id": s[0],
        "time_local": _local(s[1]),
        "profile": s[2],
        "duration_s": round(s[3], 1),
        "weight_g": s[4],
        "weighed": s[4] is not None,
        "rating": s[5] or None,
        "incomplete": s[6],
        "peak_pressure_bar": round(s[8], 1) if s[8] is not None else None,
        "avg_extract_temp_c": round(s[9], 1) if s[9] is not None else None,
        "avg_temp_err_c": round(s[10], 2) if s[10] is not None else None,
        "first_drops_s": round(s[11], 1) if s[11] is not None else None,
        **prep,
        "beans": beans_at(cfg, s[1]),
    }
    if s[4] and prep.get("dose_g"):
        out["ratio"] = f"1:{s[4] / prep['dose_g']:.2f}"
    notes = _notes_text(cfg, f"{shot_id:06d}")
    if notes:
        out["tasting_notes"] = notes
    return out


def _phase_names(cfg: Config, shot_id: int) -> dict[int, str]:
    manifest = json.loads((cfg.archive_dir / "manifest.json").read_text(encoding="utf-8"))
    rec = _manifest_rec(manifest["shots"], f"{shot_id:06d}")
    if not rec:
        return {}
    shot = parse_binary_shot((cfg.archive_dir / rec["slog_path"]).read_bytes(), rec["padded_id"])
    return {p.phase_number: p.phase_name for p in shot.phases}


def _phase_table(cfg: Config, con, shot_id: int) -> list[dict]:
    names = _phase_names(cfg, shot_id)
    rows = con.execute("""
        SELECT phase, count(*) * 0.25, round(avg(cp),1), round(max(cp),1),
               round(avg(fl),1), round(avg(pf),1), round(avg(ct),1),
               round(avg(abs(ct-tt)),1), round(avg(pr),2),
               round(coalesce(max(v),0) - coalesce(min(v),0), 1)
        FROM samples WHERE shot_id = ? GROUP BY phase ORDER BY phase""",
        [shot_id]).fetchall()
    return [{
        "phase": names.get(r[0], f"phase {r[0]}"),
        "seconds": round(r[1], 1),
        "avg_bar": r[2], "peak_bar": r[3],
        "avg_pump_flow": r[4], "avg_puck_flow": r[5],
        "avg_temp_c": r[6], "avg_temp_err_c": r[7],
        "avg_puck_resistance": r[8],
        "grams_gained": r[9],
    } for r in rows]


def get_shot(cfg: Config, shot_id: str = "latest", include_curves: bool = False) -> dict:
    sid = resolve_shot_id(cfg, shot_id)
    with _con(cfg) as con:
        out = _headline(cfg, con, sid)
        out["phases"] = _phase_table(cfg, con, sid)
        if include_curves:
            rows = con.execute(
                "SELECT t, cp, fl, pf, ct, v FROM samples WHERE shot_id = ? ORDER BY t",
                [sid]).fetchall()
            step = -(-len(rows) // 80)  # ceil: cap at ~80 points per series
            ds = rows[::step]
            out["curves"] = {
                "t_s": [round(r[0] / 1000.0, 2) for r in ds],
                "pressure_bar": [round(r[1], 1) if r[1] is not None else None for r in ds],
                "pump_flow": [round(r[2], 1) if r[2] is not None else None for r in ds],
                "puck_flow": [round(r[3], 1) if r[3] is not None else None for r in ds],
                "temp_c": [round(r[4], 1) if r[4] is not None else None for r in ds],
                "weight_g": [round(r[5], 1) if r[5] is not None else None for r in ds],
            }
    return out


def list_shots(cfg: Config, days: int = 14, profile: str | None = None,
               weighed_only: bool = True, limit: int = 30) -> list[dict]:
    where = ["to_timestamp(s.timestamp) > now() - (? * INTERVAL 1 DAY)"]
    args: list = [days]
    if profile:
        where.append("s.profile_name ILIKE '%' || ? || '%'")
        args.append(profile)
    if weighed_only:
        where.append("s.final_weight IS NOT NULL")
    with _con(cfg) as con:
        rows = con.execute(f"""
            SELECT s.shot_id, s.timestamp, s.profile_name, s.duration/1000.0,
                   s.final_weight, s.rating, a.peak_cp, a.avg_extract_ct,
                   s.notes_text IS NOT NULL
            FROM shots s LEFT JOIN (
                SELECT shot_id, max(cp) peak_cp,
                       avg(ct) FILTER (phase >= 2) avg_extract_ct
                FROM samples GROUP BY shot_id) a USING (shot_id)
            WHERE {' AND '.join(where)}
            ORDER BY s.timestamp DESC LIMIT {int(limit)}""", args).fetchall()
    out = []
    for r in rows:
        prep = prep_at(cfg, r[1])
        row = {
            "shot_id": r[0], "time_local": _local(r[1]), "profile": r[2],
            "duration_s": round(r[3], 1), "weight_g": r[4], "rating": r[5] or None,
            "peak_bar": round(r[6], 1) if r[6] is not None else None,
            "extract_temp_c": round(r[7], 1) if r[7] else None,
            "grind": prep.get("grind"), "dose_g": prep.get("dose_g"),
            "has_notes": bool(r[8]),
        }
        if r[4] and prep.get("dose_g"):
            row["ratio"] = f"1:{r[4] / prep['dose_g']:.2f}"
        out.append(row)
    return out


def compare_shots(cfg: Config, shot_a: str, shot_b: str) -> dict:
    a, b = resolve_shot_id(cfg, shot_a), resolve_shot_id(cfg, shot_b)
    with _con(cfg) as con:
        ha, hb = _headline(cfg, con, a), _headline(cfg, con, b)
        pa, pb = _phase_table(cfg, con, a), _phase_table(cfg, con, b)
    deltas = {}
    for k in ("duration_s", "weight_g", "peak_pressure_bar", "avg_extract_temp_c",
              "first_drops_s"):
        if ha.get(k) is not None and hb.get(k) is not None:
            deltas[k] = round(ha[k] - hb[k], 2)
    return {"shot_a": ha, "shot_b": hb, "a_minus_b": deltas,
            "phases_a": pa, "phases_b": pb}


def _metric_rows(cfg: Config, con, profile: str, days: int) -> list[dict]:
    rows = con.execute("""
        SELECT s.shot_id, s.timestamp, s.duration/1000.0, s.final_weight,
               a.peak_cp, a.avg_extract_ct, a.first_drops_s
        FROM shots s JOIN (
            SELECT shot_id, max(cp) peak_cp,
                   avg(ct) FILTER (phase >= 2) avg_extract_ct,
                   min(t) FILTER (v > 0.3) / 1000.0 first_drops_s
            FROM samples GROUP BY shot_id) a USING (shot_id)
        WHERE s.profile_name = ? AND s.final_weight IS NOT NULL
          AND to_timestamp(s.timestamp) > now() - (? * INTERVAL 1 DAY)
        ORDER BY s.timestamp""", [profile, days]).fetchall()
    out = []
    for r in rows:
        d = {"shot_id": r[0], "ts": r[1], "duration_s": r[2], "weight_g": r[3],
             "peak_pressure_bar": r[4], "avg_extract_temp_c": r[5], "first_drops_s": r[6]}
        dose = prep_at(cfg, r[1]).get("dose_g")
        d["ratio_n"] = round(r[3] / dose, 2) if (r[3] and dose) else None
        out.append(d)
    return out


def benchmark_shot(cfg: Config, shot_id: str = "latest", days: int = 60) -> dict:
    sid = resolve_shot_id(cfg, shot_id)
    with _con(cfg) as con:
        head = _headline(cfg, con, sid)
        pool = _metric_rows(cfg, con, head["profile"], days)
    me = next((r for r in pool if r["shot_id"] == sid), None)
    if me is None:
        raise ValueError(f"shot {sid} is unweighed or outside the {days}-day window; "
                         "benchmarks compare weighed shots of the same profile")
    metrics = {}
    for k in ("duration_s", "weight_g", "peak_pressure_bar", "avg_extract_temp_c",
              "first_drops_s", "ratio_n"):
        vals = sorted(r[k] for r in pool if r[k] is not None)
        v = me.get(k)
        if v is None or len(vals) < 3:
            continue
        mean = sum(vals) / len(vals)
        sd = (sum((x - mean) ** 2 for x in vals) / (len(vals) - 1)) ** 0.5 if len(vals) > 1 else 0
        pct = round(100 * sum(1 for x in vals if x <= v) / len(vals))
        metrics[k] = {"value": round(v, 2), "profile_mean": round(mean, 2),
                      "profile_sd": round(sd, 2), "percentile": pct}
    return {"shot": head, "vs_profile_last_days": days,
            "pool_size": len(pool), "metrics": metrics}


def profile_stats(cfg: Config, profile: str, days: int = 30) -> dict:
    with _con(cfg) as con:
        exact = con.execute(
            "SELECT DISTINCT profile_name FROM shots WHERE profile_name ILIKE '%' || ? || '%'",
            [profile]).fetchall()
        if not exact:
            raise ValueError(f"no shots match profile {profile!r}")
        if len(exact) > 1:
            raise ValueError(f"ambiguous profile {profile!r}: {[e[0] for e in exact]}")
        name = exact[0][0]
        pool = _metric_rows(cfg, con, name, days)
    if not pool:
        return {"profile": name, "days": days, "weighed_shots": 0}

    def stat(k):
        vals = [r[k] for r in pool if r[k] is not None]
        if not vals:
            return None
        mean = sum(vals) / len(vals)
        sd = (sum((x - mean) ** 2 for x in vals) / (len(vals) - 1)) ** 0.5 if len(vals) > 1 else 0
        return {"mean": round(mean, 2), "sd": round(sd, 2),
                "min": round(min(vals), 2), "max": round(max(vals), 2), "n": len(vals)}

    latest_ts = pool[-1]["ts"]
    return {
        "profile": name, "days": days, "weighed_shots": len(pool),
        "metrics": {k: stat(k) for k in ("duration_s", "weight_g", "ratio_n",
                                          "peak_pressure_bar", "avg_extract_temp_c",
                                          "first_drops_s")},
        "current_prep": prep_at(cfg, latest_ts),
        "current_beans": beans_at(cfg, latest_ts),
        "by_day": [{"date": datetime.fromtimestamp(r["ts"], tz=LOCAL_TZ).date().isoformat(),
                    "shot_id": r["shot_id"], "duration_s": round(r["duration_s"], 1),
                    "weight_g": r["weight_g"], "ratio_n": r["ratio_n"]} for r in pool],
    }


def _profile_snapshots(cfg: Config) -> list[Path]:
    return sorted((cfg.archive_dir / "profiles").glob("*.json"))


def list_profiles(cfg: Config) -> list[dict]:
    snaps = _profile_snapshots(cfg)
    if not snaps:
        raise ValueError("no profile snapshots archived yet")
    latest = json.loads(snaps[-1].read_text(encoding="utf-8"))
    with _con(cfg) as con:
        counts = dict(con.execute(
            "SELECT profile_name, count(*) FROM shots GROUP BY profile_name").fetchall())
    return [{"label": p.get("label"), "type": p.get("type"),
             "temperature": p.get("temperature"), "phases": len(p.get("phases", [])),
             "selected": p.get("selected", False), "favorite": p.get("favorite", False),
             "archived_shots": counts.get(p.get("label"), 0)}
            for p in latest.get("profiles", [])]


def get_profile(cfg: Config, name: str) -> dict:
    snaps = _profile_snapshots(cfg)
    if not snaps:
        raise ValueError("no profile snapshots archived yet")
    history: list[tuple[str, str]] = []  # (snapshot date, definition hash)
    current = None
    for snap in snaps:
        data = json.loads(snap.read_text(encoding="utf-8"))
        match = [p for p in data.get("profiles", [])
                 if name.lower() in (p.get("label") or "").lower()]
        if len(match) > 1:
            raise ValueError(f"ambiguous profile {name!r}: {[p['label'] for p in match]}")
        if match:
            current = match[0]
            digest = json.dumps(match[0], sort_keys=True)
            if not history or history[-1][1] != digest:
                history.append((snap.stem, digest))
    if current is None:
        raise ValueError(f"profile {name!r} not found in any snapshot")
    return {"definition": current,
            "snapshot_dates_where_changed": [h[0] for h in history],
            "note": "definition from the newest snapshot containing it; change dates "
                    "are snapshot-granularity (daily)"}


def run_query(cfg: Config, sql: str) -> dict:
    stripped = sql.strip().rstrip(";")
    if ";" in stripped:
        raise ValueError("one statement per query")
    if not re.match(r"^\s*(select|with)\b", stripped, re.IGNORECASE):
        raise ValueError("read-only: query must start with SELECT or WITH")
    if SQL_FORBIDDEN.search(stripped):
        raise ValueError("read-only: DDL/DML/meta statements are refused")
    with _con(cfg) as con:
        cur = con.execute(stripped)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchmany(200)
        more = cur.fetchone() is not None
    return {"columns": cols,
            "rows": [[(round(v, 3) if isinstance(v, float) else
                       (str(v) if not isinstance(v, (int, str, bool, type(None))) else v))
                      for v in row] for row in rows],
            "truncated_at_200": more}
