# Architecture

The design in one sentence: **the device is a buffer; your storage is the
home; every byte is verified on the way in and re-verified before anything
is ever deleted.**

## Three layers

1. **`raw/` - the truth.** Verbatim device bytes (`.slog` shot files,
   canonicalized notes JSON), kept forever, never mutated. Everything
   else is derived from this layer and can be rebuilt from it
   (`python -m archiver.cli parse --rebuild`).
2. **`manifest.json` - bookkeeping.** Hashes, sizes, timestamps,
   verification state per shot. A cache: if lost, a re-sync re-fetches
   what the device still has and raw/ remains authoritative for the rest.
3. **`parquet/` - the analysis layer.** Typed, columnar, disposable.
   Chosen because Parquet is the lingua franca of the analytics world
   (DuckDB, pandas, Polars, R, Spark) with one light dependency
   (pyarrow). If you prefer another shape, regenerate from raw.

## The schema contract (additive-only)

`manifest.json` carries `schema: 1`. The promise: **columns and fields
are only ever added, never renamed, removed, or re-meaning-ed.** History
of the promise being kept: `parse_ok` and `accepted_degenerate` were
both later additions; nothing has ever changed meaning.

**`shots.parquet`** - one row per shot:

| column | meaning |
|---|---|
| shot_id / padded_id | numeric id / zero-padded string (file naming) |
| timestamp | device epoch seconds (UTC) |
| profile_id / profile_name | as self-reported by the shot file |
| duration | milliseconds |
| sample_interval_ms | sampling period (250 ms on tested firmware) |
| sample_count | samples actually parsed |
| slog_version | .slog format version (V5 on tested firmware) |
| final_weight | grams, ONLY when a BLE scale was connected (else null). Normally the .slog header's stamp; when the header reads ~0 g but the sample stream settled substantially higher (firmware v1.8.1-159+ stops the scale timer at brew:end, which on some scales zeroes the stream while the recording tail is still written; or the operator powered the scale off in the tail), the settled sample value is used instead |
| weight_rescued | true when final_weight came from the sample stream's settled value rather than the header (see above). Additive column, 2026-08-25 |
| rating / notes_text / notes_json | from the device's notes doc |
| incomplete | file ended before its declared sample count |
| phase_count | number of named phases |
| parse_ok | false = corrupt/degenerate capture; curves unavailable |
| device_epoch | which life of the device's shot counter the id belongs to (see below) |

Semantics that matter: **`final_weight IS NOT NULL` ("weighed") separates
real espresso from boiler flushes** on single-boiler machines - filter on
it before averaging temperatures (flushes read 120-145 C).

**`samples/<id>.parquet`** - one row per 250 ms sample: `shot_id, phase,
t` (ms), `tt/ct` (target/current temp C), `tp/cp` (pressure bar),
`fl/tf/pf/vf` (flows ml/s), `v/ev` (measured/estimated weight g), `pr`
(puck resistance). Phase NAMES are not projected (read them from raw via
the vendored parser - `scripts/plot_shot.py` shows how).

**`meta/`** - your covariates, append-only jsonl: `beans.jsonl` (one
entry per bag, shots assigned by date range) and `prep.jsonl`
(grind/dose events, sticky-until-changed). The MCP joins these into
every answer; bean age and grind changes stop being folklore.

## Verification, everywhere

- **Archive time**: hash in flight -> temp-then-rename -> read back from
  the destination filesystem -> hashes must match. This exists because a
  network filesystem once reported success on 25 GB written to an
  unmounted path. Trust nothing.
- **Mid-write race guard**: a shot's index entry can appear while its
  file is still being written. A capture that parses to 0 samples while
  the index claims real duration is held *provisional* and re-fetched
  next cycle.
- **Terminal state for corpses**: a degenerate capture whose bytes are
  identical across two fetches IS what the device holds (e.g. damaged by
  a WiFi drop mid-save). It is accepted as-is, marked `parse_ok: false`,
  and the retry loop ends. Failed-parse shots stay VISIBLE as minimal
  rows - never ghosts.
- **Phase 2 (deletion)**: eligibility requires verified-in-a-prior-run,
  parse_ok, grace period (default 14 d), outside the keep-recent window
  (default 20), notes-state agreement - and then delete-time
  re-verification: fresh device hash == manifest == fresh storage
  read-back, notes re-fetched and re-canonicalized. Capped per cycle,
  oldest first, only after a fully clean cycle, hard-paused whenever a
  firmware format change is detected and not yet proven out. Every
  action appends to `deletions.log`; tombstones are confirmed on a fresh
  index fetch. A storage mismatch at delete time skips the shot and
  notifies - archive integrity always outranks flash relief.

## Device facts and firmware quirks (discovered live, receipts in code comments)

- Shot files: `GET /api/history/index.bin` and `<id>.slog` over HTTP.
  **Notes are WebSocket-only** - an HTTP request for the `.json` falls
  through to the web UI catch-all and returns HTML with status 200.
  Fetched via `req:history:notes:get`, **unpadded id** (the stock UI's
  convention; the docs show padded - this project tries both). The
  archived notes artifact is a canonical serialization (sorted keys),
  since the device's literal bytes are unreachable over WS.
- The index's has-notes flag is set **only when the note carries a
  rating > 0**. Unrated text notes are invisible to any sync tool.
- On-device deletion (`req:history:delete`) tombstones the index entry
  and removes files by *padded* name - orphaning UI-written (unpadded)
  notes files. One more reason the archive is their durable home.
- The device may serve its HTML UI instead of binary when busy; every
  fetch guards against HTML-shaped "success".
- Format drift is detected structurally (index header version each sync,
  .slog version each parse), not by trusting version strings. Drift
  pauses phase 2 until a shot parses clean under the new format.
- **A missing index is not an error.** Nightly-era firmware (LittleFS)
  creates the shot index on the first recorded shot; until then
  `index.bin` 404s with a literal `Index not found`. A fresh machine or a
  just-flashed one is an EMPTY device: sync treats it as a clean no-op
  cycle and phase 2 reports nothing-to-delete.
- **The device's shot counter can reset** (measured 2026-08-23: the
  SPIFFS to LittleFS migration restarted ids at 1), so a device id alone
  is not unique across your archive's lifetime. The archiver watches
  `next_id` against a watermark; a counter moving backwards starts a new
  **epoch**. Archived records keep their identity: epoch 1 keys stay
  plain, later epochs prefix manifest keys and sample filenames
  (`e2-000233`), and `shots.parquet` carries `device_epoch` - a reborn
  id can never collide with or silently replace an archived shot.

## Services

- `archiver` - the scheduler (sync -> parse -> profiles -> optional
  phase 2), plus the `POST /sync` trigger (bearer token, fail-closed:
  no token, no listener).
- `mcp` (optional) - streamable-HTTP MCP over the same package: 7 read
  tools + `sync_now` + read-only `query` + 2 append-only meta writers.
  Read-only **by mount** (archive ro, only `meta/` rw). Static bearer
  token, required.

Both are one image, two commands. Deployment shape and every knob:
`docker-compose.example.yml`.
