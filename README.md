# gaggimate-archiver

Durable, verified, analytics-ready archiving for [GaggiMate](https://gaggimate.eu)
espresso machines - plus an optional MCP server so an AI assistant can
reason over your full shot history.

**The problem** ([jniebuhr/gaggimate#571](https://github.com/jniebuhr/gaggimate/issues/571)):
your shot history lives only on the machine's flash. Firmware updates have
wiped it; the firmware's own free-space reaper silently rotates old shots
out. This project makes the machine a *buffer* and your own storage the
*home* - every shot pulled off the device on a schedule, hash-verified end
to end, parsed into Parquet, queryable forever.

## What it does

One small container (a second, optional one for the MCP):

- **Sync** (scheduled + on demand): pulls new `.slog` shot files and their
  notes off the device, gently (the display MCU's heap is fragile).
  Every file is verified: hashed in flight, written temp-then-rename,
  **read back from storage and re-hashed** - because network filesystems
  lie about writes.
- **Parse**: derives `shots.parquet` (one row per shot) and per-shot
  sample files - typed, DuckDB/pandas/Polars-ready. Rebuildable from raw
  at any time; raw bytes are kept forever.
- **Profiles**: daily snapshot of your machine's profile list (also lost
  on firmware updates) - and profiles can be pushed back, so this is a
  real backup, not just a copy.
- **Notes**: ratings and tasting notes added later from the web app are
  picked up and reconciled (they are WebSocket-only on the device - one
  of several firmware quirks this project documents and handles; see
  [ARCHITECTURE.md](ARCHITECTURE.md)).
- **Phase 2 (opt-in)**: verified deletion from the device - relieve the
  machine's flash under *your* protocol instead of its silent reaper.
  A shot is only removed after fresh device-hash, storage-read-back, and
  notes re-verification all match the archive. Every action is logged.
- **Sync button**: `POST /sync` with a bearer token - wire it to an iOS
  Shortcut and get "2 new shots archived" as a home-screen tap.
- **MCP server** (optional): curated tools over the archive -
  `list_shots`, `get_shot` (per-phase story + curves), `compare_shots`,
  `benchmark_shot` (percentiles vs your profile's history),
  `profile_stats`, `get_profile`, a read-only SQL hatch, and append-only
  bean/grind/dose logging so every answer carries its covariates
  (bean age, grind setting, brew ratio).

## Quick start

```bash
cp docker-compose.example.yml docker-compose.yml
# edit: your archive path, your device host, your TZ
docker compose up -d
```

The first run backfills everything the device still holds. See the
example compose for every knob; the only required setting is where your
archive lives and how to reach your machine (`gaggimate.local` works if
your network resolves mDNS; a fixed IP is sturdier).

**Storage note**: any bind mount works - local disk, NAS, ZFS. If your
archive lives on CIFS/NFS, read the mount-guard pattern in the example
compose: it makes the container refuse to start when the share isn't
mounted, instead of happily archiving into an empty mountpoint.

## The data

```
<your-archive>/
  raw/YYYY/MM/<id>.slog|.json   verbatim device bytes - the truth, kept forever
  profiles/<date>.json          daily profile snapshots (restorable)
  parquet/                      derived, rebuildable: shots.parquet + samples/
  manifest.json                 bookkeeping + verification evidence (rebuildable)
  meta/                         your bean/grind/dose logs (append-only jsonl)
  deletions.log                 phase-2 audit trail
```

The schema contract (columns, semantics, and the additive-only promise)
is in [ARCHITECTURE.md](ARCHITECTURE.md). Analysis starters:
`queries.sql` (DuckDB views + example queries) and
`scripts/plot_shot.py` (a shot's pressure/flow/temp curves with named
phases).

## House rules learned the hard way

- **Star your notes.** The firmware only marks a shot as having notes
  when the note carries a rating > 0 - unrated text notes are invisible
  to any sync tool.
- **Use water mode for flushes** (single-boiler machines): water mode
  isn't recorded as a shot, so your archive stays espresso-only.
- **Sync before flashing firmware.** Updates have wiped device history;
  one sync first makes that cost zero.

## Neighbors and positioning

- [GLP](https://github.com/mxkissnr/gaggiuino-local-profiler) - shot
  recording and visualization as a Home Assistant add-on. Choose it if
  you live in HA; choose this if you want a standalone, verification-
  first archive with an analytics layer and MCP.
- [visualizer.coffee](https://visualizer.coffee) - cloud shot sharing;
  GaggiMate can push to it natively. Complementary (a bridge from this
  archive is on the roadmap).
- [gaggimate-mcp](https://github.com/julianleopold/gaggimate-mcp) - MCP
  over the *live device* (sees only what's currently on flash). This
  project vendors its excellent `.slog`/`index.bin` parsers (MIT, see
  `app/archiver/vendor/`) and archives what the device forgets.
- [MyBrewFolio](https://mybrewfolio.com) - cloud shot analysis for
  GaggiMate. Choose it for zero-setup convenience; choose this if you
  want local-first ownership - your shot data never leaves your network.

## Roadmap

- Push-to-visualizer bridge · optional duckdb-wasm static viewer ·
  test suite + CI hardening. Issues and PRs welcome.

## Honesty section

This project was built in production against one machine (GaggiMate Pro
v1.1, display firmware v1.8.1, Gaggia Classic E24) and is validated by
daily use rather than a test suite (yet). It was also largely written by
an AI assistant working with a product manager - every mechanism is
documented in ARCHITECTURE.md with the receipts. If that bothers you,
that's okay: don't use it. The shots are excellent.

MIT licensed. Vendored parsers from
[gaggimate-mcp](https://github.com/julianleopold/gaggimate-mcp) (MIT).
