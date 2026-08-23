# Vendored from gaggimate-mcp

Source: https://github.com/julianleopold/gaggimate-mcp
Commit: 0af88ad4a5f99da73245de9868a803247cdb6226 (main, 2026-06-03)
License: MIT (see LICENSE in this directory)

Vendored files, unmodified:

- `parsers/shot.py`  - .slog binary parser (V4/V5)
- `parsers/index.py` - index.bin binary parser

Only the parsers are vendored. The upstream HTTP client and models are not
used: the archiver's client needs byte hashing, read-back verification, and
request pacing that the upstream client does not do, so it is written here
instead (see `archiver/device.py`).

To refresh: re-download the two files from upstream, update the commit pin
above, and re-run the parse step over a few known shots before trusting it.
