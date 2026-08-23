"""CLI: python -m archiver.cli <sync|parse> [options]."""

import argparse
import logging
import sys
from pathlib import Path

from . import parse as parse_step
from . import sync as sync_step
from .config import Config


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="gaggimate-archiver")
    p.add_argument("--host", help="device host (default: $GAGGIMATE_HOST)")
    p.add_argument("--archive-dir", help="archive root (default: $ARCHIVE_DIR)")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_sync = sub.add_parser("sync", help="pull new shots off the device, verified")
    p_sync.add_argument("--dry-run", action="store_true",
                        help="list what would be downloaded; touch nothing")
    p_sync.add_argument("--limit", type=int, default=None,
                        help="archive at most N shots this run (oldest first)")

    p_parse = sub.add_parser("parse", help="derive Parquet from the raw archive")
    p_parse.add_argument("--rebuild", action="store_true",
                         help="rewrite all sample files, not just missing ones")

    sub.add_parser("profiles", help="snapshot the device's profile list (daily)")
    sub.add_parser("daemon", help="run forever on $SYNC_SCHEDULE (container entrypoint)")

    p_del = sub.add_parser("delete-pass", help="phase 2: one verified deletion pass")
    p_del.add_argument("--dry-run", action="store_true",
                       help="list eligible shots; delete nothing")

    args = p.parse_args(argv)

    cfg = Config()
    if args.host:
        cfg.host = args.host
    if args.archive_dir:
        cfg.archive_dir = Path(args.archive_dir)

    logging.basicConfig(
        level=cfg.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )

    try:
        if args.cmd == "sync":
            sync_step.run(cfg, dry_run=args.dry_run, limit=args.limit)
        elif args.cmd == "parse":
            parse_step.run(cfg, rebuild=args.rebuild)
        elif args.cmd == "profiles":
            from . import profiles as profiles_step
            profiles_step.run(cfg)
        elif args.cmd == "daemon":
            from . import daemon as daemon_step
            daemon_step.run(cfg)
        elif args.cmd == "delete-pass":
            from . import delete_pass
            delete_pass.run(cfg, dry_run=args.dry_run)
    except Exception as e:
        logging.getLogger("archiver").exception("run FAILED")
        from .notify import notify
        notify(f"{args.cmd} FAILED: {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
