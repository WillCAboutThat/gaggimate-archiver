"""Failure/drift notification hook. Inert unless NTFY_URL is set.

The fleet has no alerting today (beszel is metrics, not log alerts), so the
default channels are loud ERROR logs + the last-success staleness file.
This hook is the opt-in third channel: a plain HTTP POST of a one-line
message, compatible with ntfy (self-hosted or ntfy.sh topic) and with
anything else that accepts a POSTed body. No new dependency, no new infra
decision forced - set NTFY_URL when a notifier exists, delete it to opt out.
"""

import logging
import os

import httpx

log = logging.getLogger("archiver.notify")


def notify(message: str) -> None:
    url = os.environ.get("NTFY_URL", "")
    if not url:
        return
    try:
        httpx.post(url, content=message.encode("utf-8"),
                   headers={"Title": "gaggimate-archiver"}, timeout=10)
    except Exception as e:  # notification failure must never fail the run
        log.warning("notify failed: %s", e)
