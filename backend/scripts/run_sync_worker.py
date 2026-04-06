#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dedicated sync worker for keeping the work database fresh outside the API request path.
"""

import logging
import signal
import sys
import threading
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from main import ensure_pm_synced, load_and_enrich_data

logger = logging.getLogger("SYNC_WORKER")
_STOP_EVENT = threading.Event()


def _handle_signal(signum, _frame):
    logger.info("Received signal %s, stopping sync worker", signum)
    _STOP_EVENT.set()


def _start_embedding_check() -> None:
    try:
        from services.embeddings import auto_generate_embeddings_if_needed

        threading.Thread(
            target=auto_generate_embeddings_if_needed,
            kwargs={"threshold": 10},
            daemon=True,
            name="sync-worker-embedding-check",
        ).start()
        logger.info("Startup embedding check launched")
    except Exception as exc:
        logger.warning("Startup embedding check failed: %s", exc)


def main() -> None:
    logger.info("Starting dedicated sync worker")
    repairs_ok = load_and_enrich_data(force=False)
    pm_ok = ensure_pm_synced(force=False)
    logger.info("Initial sync finished (repairs=%s, pm=%s)", repairs_ok, pm_ok)
    _start_embedding_check()

    realtime_enabled = False
    try:
        from services.realtime_data_sync import setup_realtime_sync

        setup_realtime_sync()
        realtime_enabled = True
        logger.info("Realtime sync watcher enabled")
    except Exception as exc:
        logger.error("Failed to start realtime sync watcher: %s", exc)

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _handle_signal)

    try:
        _STOP_EVENT.wait()
    finally:
        if realtime_enabled:
            try:
                from services.realtime_data_sync import cleanup_realtime_sync

                cleanup_realtime_sync()
                logger.info("Realtime sync watcher cleaned up")
            except Exception as exc:
                logger.error("Failed to cleanup realtime sync watcher: %s", exc)


if __name__ == "__main__":
    main()
