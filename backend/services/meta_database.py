"""
meta_database.py — Thread-safe Meta Database manager for Docker + Windows volume mounts.

Strategy for 30 concurrent writers:
  - Single global threading.Lock (one writer at a time: SQLite is single-writer)
  - Retry loop with exponential back-off for OperationalError
  - No WAL mode (WAL uses shared memory files that may fail on Docker-for-Windows volumes)
  - busy_timeout keeps SQLite from failing immediately on contention
"""
import sqlite3
import os
import logging
import time as _time
import threading
from core.config import BASE_DIR

# ─────────────────────────────────────────────────────────────
# Database path — Located in the shared data directory.
# ─────────────────────────────────────────────────────────────
META_DB_PATH = os.path.join(BASE_DIR, "data", "meta_database.db")

logger = logging.getLogger("Meta Database")

# Single global lock for all Meta Database access (read and write) 
# to avoid I/O errors on Docker-for-Windows NTFS volumes.
_DB_LOCK = threading.Lock()
_MAX_QUEUE_WAIT = 30          # seconds a caller will wait for the lock
_MAX_DB_RETRIES = 5           # retries when SQLite throws OperationalError
_MAX_DB_RETRIES = 3           # retries when SQLite throws OperationalError


def _connect() -> sqlite3.Connection:
    """Open a connection with very conservative settings for network volumes."""
    os.makedirs(os.path.dirname(META_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(META_DB_PATH, timeout=20, check_same_thread=False)
    # TRUNCATE mode is safer than DELETE/WAL for some network mounts
    # It keeps the file handle open and just zeros the journal.
    conn.execute("PRAGMA journal_mode=TRUNCATE")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA busy_timeout=20000")   # ms
    return conn


# ─────────────────────────────────────────────────────────────
# Schema bootstrap
# ─────────────────────────────────────────────────────────────
def init_meta_db() -> None:
    """Create tables if they don't exist.  Called once at import time."""
    try:
        with _DB_LOCK:
            conn = _connect()
            # สร้างตาราง meta_knowledge
            conn.execute("""
                CREATE TABLE IF NOT EXISTS meta_knowledge (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    name      TEXT NOT NULL,
                    topic     TEXT NOT NULL,
                    answer    TEXT NOT NULL,
                    timestamp DATETIME DEFAULT (DATETIME('now', 'localtime'))
                )
            """)
            # สร้างตาราง meta_embeddings สำหรับเก็บ vector
            conn.execute("""
                CREATE TABLE IF NOT EXISTS meta_embeddings (
                    id        INTEGER PRIMARY KEY,
                    embedding BLOB NOT NULL,
                    FOREIGN KEY (id) REFERENCES meta_knowledge(id) ON DELETE CASCADE
                )
            """)
            conn.commit()
            conn.close()
        logger.info("Meta Database initialised successfully.")
    except Exception as exc:
        logger.error(f"init_meta_db failed: {exc}")


# ─────────────────────────────────────────────────────────────
# Write operations  (thread-safe)
# ─────────────────────────────────────────────────────────────
def insert_meta_knowledge(name: str, topic: str, answer: str) -> int:
    """
    Insert a new knowledge row.

    Supports 30+ concurrent callers by:
      1. Acquiring _WRITE_LOCK (queue: one writer at a time)
      2. Retrying on SQLite OperationalError up to _MAX_DB_RETRIES times

    Returns new row id, or -1 on failure.
    """
    acquired = _DB_LOCK.acquire(timeout=_MAX_QUEUE_WAIT)
    if not acquired:
        logger.error("insert_meta_knowledge: timed-out waiting for lock")
        return -1

    try:
        for attempt in range(1, _MAX_DB_RETRIES + 1):
            try:
                conn = _connect()
                cur  = conn.cursor()
                cur.execute(
                    "INSERT INTO meta_knowledge (name, topic, answer) VALUES (?, ?, ?)",
                    (name, topic, answer),
                )
                new_id = cur.lastrowid
                conn.commit()
                conn.close()
                logger.info(f"Inserted meta id={new_id} topic='{topic[:60]}' by {name}")
                return new_id
            except sqlite3.OperationalError as exc:
                logger.warning(f"insert attempt {attempt}/{_MAX_DB_RETRIES}: {exc}")
                _time.sleep(0.2 * attempt)
            except Exception as exc:
                logger.error(f"insert_meta_knowledge unexpected error: {exc}")
                return -1

        return -1
    finally:
        _DB_LOCK.release()


def delete_meta_knowledge(meta_id: int) -> bool:
    """
    Delete a knowledge row by id.  Returns True on success.
    """
    acquired = _DB_LOCK.acquire(timeout=_MAX_QUEUE_WAIT)
    if not acquired:
        logger.error(f"delete_meta_knowledge id={meta_id}: timed-out waiting for lock")
        return False

    try:
        conn = _connect()
        cur  = conn.cursor()
        cur.execute("DELETE FROM meta_knowledge WHERE id=?", (meta_id,))
        affected = cur.rowcount
        conn.commit()
        conn.close()
        if affected:
            logger.info(f"Deleted meta id={meta_id}")
            return True
        else:
            logger.warning(f"delete_meta_knowledge: id={meta_id} not found")
            return False
    except Exception as exc:
        logger.error(f"delete_meta_knowledge error: {exc}")
        return False
    finally:
        _DB_LOCK.release()


# ─────────────────────────────────────────────────────────────
# Read operations  (fully serialized to avoid I/O errors)
# ─────────────────────────────────────────────────────────────
def get_all_meta_knowledge() -> list:
    """Return all rows as a list of dicts, newest first."""
    if not os.path.exists(META_DB_PATH):
        return []
    with _DB_LOCK:
        try:
            conn = _connect()
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT id, name, topic, answer, timestamp FROM meta_knowledge ORDER BY id DESC"
            ).fetchall()
            conn.close()
            return [dict(r) for r in rows]
        except Exception as exc:
            logger.error(f"get_all_meta_knowledge error: {exc}")
            return []


def get_meta_knowledge_count() -> int:
    """Return total number of knowledge entries."""
    if not os.path.exists(META_DB_PATH):
        return 0
    with _DB_LOCK:
        try:
            conn = _connect()
            count = conn.execute("SELECT COUNT(*) FROM meta_knowledge").fetchone()[0]
            conn.close()
            return count
        except Exception:
            return 0


# ─────────────────────────────────────────────────────────────
# Auto-init on import
# ─────────────────────────────────────────────────────────────
init_meta_db()
