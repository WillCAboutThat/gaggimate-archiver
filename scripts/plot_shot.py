"""Plot one shot's pressure / flow / temperature curves from the archive.

Dev-side tool (needs matplotlib + pandas, deliberately NOT container
dependencies). Reads Parquet for the curves and the raw .slog for phase NAMES,
which are not projected into Parquet.

  python scripts/plot_shot.py --archive-dir <archive root> [shot_id] [--out x.png]

Default shot: the most recent weighed one (final_weight present) - the
unweighed pulls are flushes and make dull plots.
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
from archiver.vendor.gaggimate_mcp.parsers.shot import parse_binary_shot  # noqa: E402

PHASE_COLORS = ["#dce9f5", "#f5eedc", "#dcf5e1", "#f5dcdc", "#e8dcf5", "#dcf2f5"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("shot_id", nargs="?", help="numeric id; default: latest weighed shot")
    ap.add_argument("--archive-dir", default=".", help="archive root")
    ap.add_argument("--out", default=None, help="output PNG (default shot_<id>.png)")
    args = ap.parse_args()

    root = Path(args.archive_dir)
    con = duckdb.connect()
    shots = str(root / "parquet" / "shots.parquet")

    if args.shot_id:
        shot_id = int(args.shot_id)
    else:
        row = con.execute(
            f"SELECT shot_id FROM '{shots}' WHERE final_weight IS NOT NULL "
            "ORDER BY timestamp DESC LIMIT 1").fetchone()
        if not row:
            print("no weighed shots in archive", file=sys.stderr)
            return 1
        shot_id = row[0]

    meta = con.execute(
        f"SELECT padded_id, profile_name, timestamp, duration, final_weight "
        f"FROM '{shots}' WHERE shot_id = ?", [shot_id]).fetchone()
    if not meta:
        print(f"shot {shot_id} not in archive", file=sys.stderr)
        return 1
    padded, profile, ts, duration_ms, weight = meta

    df = con.execute(
        f"SELECT * FROM '{root / 'parquet' / 'samples' / (padded + '.parquet')}' "
        "ORDER BY t").fetchdf()
    t = df["t"] / 1000.0

    # Phase names come from the raw .slog (the truth), located via manifest.
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    slog = root / manifest["shots"][padded]["slog_path"]
    phases = parse_binary_shot(slog.read_bytes(), padded).phases

    # Scale weight panel only when the BT scale was connected (v > 0
    # somewhere); ev alone is the firmware's flow-integral ESTIMATE and
    # shows up even on flushes, so it never earns a panel by itself.
    has_scale = bool((df["v"] > 0).any())
    n_axes = 4 if has_scale else 3
    fig, axes = plt.subplots(n_axes, 1, sharex=True, figsize=(11, 8 + 2 * has_scale))
    if has_scale:
        ax_p, ax_f, ax_w, ax_t = axes
    else:
        ax_p, ax_f, ax_t = axes
    when = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    title = f"Shot {shot_id} - {profile} - {when} - {duration_ms/1000:.1f}s"
    if weight:
        title += f" - {weight:g} g"
    fig.suptitle(title)

    ax_p.plot(t, df["cp"], color="#1a5fb4", label="pressure")
    ax_p.plot(t, df["tp"], color="#1a5fb4", ls="--", lw=1, label="target")
    ax_p.set_ylabel("bar")
    ax_f.plot(t, df["fl"], color="#26a269", label="pump flow")
    if df["pf"].notna().any():
        ax_f.plot(t, df["pf"], color="#e66100", lw=1, label="puck flow")
    ax_f.plot(t, df["tf"], color="#26a269", ls="--", lw=1, label="target")
    ax_f.set_ylabel("ml/s")
    if has_scale:
        # Scale-derived output rate: d(v)/dt, smoothed (the scale reports in
        # 0.1 g steps at 4 Hz, so the raw derivative is a comb). Overlaid on
        # the flow panel it is the reality check on the firmware's puck-flow
        # model; drawn on the weight panel's sibling for shared context.
        dv = (df["v"].diff() / t.diff()).rolling(9, center=True, min_periods=3).mean()
        ax_f.plot(t, dv, color="#813d9c", lw=1, ls=":", label="scale rate (dv/dt)")
        ax_w.plot(t, df["v"], color="#813d9c", label="weight (scale)")
        ax_w.plot(t, df["ev"], color="#813d9c", ls="--", lw=1, label="estimated")
        if weight:
            ax_w.axhline(weight, color="#813d9c", lw=0.5, alpha=0.5)
        ax_w.set_ylabel("g")
    ax_t.plot(t, df["ct"], color="#c01c28", label="temp")
    ax_t.plot(t, df["tt"], color="#c01c28", ls="--", lw=1, label="target")
    ax_t.set_ylabel("\N{DEGREE SIGN}C")
    ax_t.set_xlabel("seconds")

    # Shade phases across all axes; label at the top axis.
    interval = df["t"].iloc[1] - df["t"].iloc[0] if len(df) > 1 else 250
    bounds = [(p.sample_index * interval / 1000.0, p.phase_name) for p in phases]
    bounds.append((t.iloc[-1], None))
    for i, ((start, name), (end, _)) in enumerate(zip(bounds, bounds[1:])):
        for ax in axes:
            ax.axvspan(start, end, color=PHASE_COLORS[i % len(PHASE_COLORS)], zorder=0)
        if name:
            ax_p.annotate(name, ((start + end) / 2, 0.98), xycoords=("data", "axes fraction"),
                          ha="center", va="top", fontsize=8, rotation=0, alpha=0.8)
    for ax in axes:
        ax.legend(loc="center left", fontsize=8)
        ax.grid(alpha=0.25)

    out = args.out or f"shot_{shot_id}.png"
    fig.tight_layout()
    fig.savefig(out, dpi=130)
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
