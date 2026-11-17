"""SQLite persistence layer (WAL, safe for two processes on one volume)."""
from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

from . import config

_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS jobs (
    job_id          TEXT PRIMARY KEY,
    lane            TEXT NOT NULL UNIQUE,
    status          TEXT NOT NULL CHECK (status IN
                        ('queued','processing','finalizing','published','failed')),
    input_hash      TEXT NOT NULL,
    barcode_table   TEXT NOT NULL,
    r1_incoming     TEXT NOT NULL,
    r2_incoming     TEXT NOT NULL,
    error           TEXT,
    instance_id     TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    heartbeat_at    TEXT,
    attempt_id      TEXT,
    cleanup_pending INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS job_samples (
    job_id      TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    sample_id   TEXT NOT NULL,
    barcode_r1  TEXT NOT NULL,
    barcode_r2  TEXT NOT NULL,
    PRIMARY KEY (job_id, sample_id)
);

CREATE TABLE IF NOT EXISTS lane_publishes (
    lane            TEXT PRIMARY KEY,
    job_id          TEXT NOT NULL,
    published_at    TEXT NOT NULL,
    seq             INTEGER NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS lane_counts (
    lane        TEXT NOT NULL REFERENCES lane_publishes(lane),
    sample_id   TEXT NOT NULL,           -- '' denotes the quarantine bucket
    r1_file     TEXT NOT NULL,
    r2_file     TEXT NOT NULL,
    read_pairs  INTEGER NOT NULL,
    PRIMARY KEY (lane, sample_id)
);

CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id   TEXT PRIMARY KEY,
    created_at    TEXT NOT NULL,
    lanes_json    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshot_lanes (
    snapshot_id TEXT NOT NULL REFERENCES snapshots(snapshot_id) ON DELETE CASCADE,
    lane        TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, lane)
);
"""

# Canonical jobs-table definition, reused by the CHECK-constraint migration.
_JOBS_TABLE_SQL = """
CREATE TABLE jobs (
    job_id          TEXT PRIMARY KEY,
    lane            TEXT NOT NULL UNIQUE,
    status          TEXT NOT NULL CHECK (status IN
                        ('queued','processing','finalizing','published','failed')),
    input_hash      TEXT NOT NULL,
    barcode_table   TEXT NOT NULL,
    r1_incoming     TEXT NOT NULL,
    r2_incoming     TEXT NOT NULL,
    error           TEXT,
    instance_id     TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    heartbeat_at    TEXT,
    attempt_id      TEXT,
    cleanup_pending INTEGER NOT NULL DEFAULT 0
)
"""

_JOB_COLUMNS = (
    "job_id, lane, status, input_hash, barcode_table, r1_incoming, "
    "r2_incoming, error, instance_id, created_at, updated_at, heartbeat_at, "
    "attempt_id, cleanup_pending"
)

_local = threading.local()
_hb_local = threading.local()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def utcnow() -> str:
    return _now()


def get_conn() -> sqlite3.Connection:
    """One connection per worker thread, configured for cross-process use."""
    conn = getattr(_local, "conn", None)
    if conn is None:
        conn = sqlite3.connect(
            config.DB_PATH, timeout=config.BUSY_TIMEOUT_MS / 1000,
            isolation_level=None, check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(f"PRAGMA busy_timeout={config.BUSY_TIMEOUT_MS}")
        conn.execute("PRAGMA foreign_keys=ON")
        _local.conn = conn
    return conn


def _heartbeat_conn() -> sqlite3.Connection:
    """Advisory-heartbeat connection with a short busy timeout.

    A locked database must not stall demultiplexing for the full busy
    timeout on every flush; heartbeat failures are tolerated by the caller.
    """
    conn = getattr(_hb_local, "conn", None)
    if conn is None:
        conn = sqlite3.connect(
            config.DB_PATH, timeout=config.HEARTBEAT_BUSY_TIMEOUT_MS / 1000,
            isolation_level=None, check_same_thread=False,
        )
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(f"PRAGMA busy_timeout={config.HEARTBEAT_BUSY_TIMEOUT_MS}")
        _hb_local.conn = conn
    return conn


@contextmanager
def tx() -> Iterator[sqlite3.Connection]:
    conn = get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def init_db() -> None:
    conn = get_conn()
    conn.executescript(_SCHEMA)
    _migrate_jobs(conn)


def _migrate_jobs(conn: sqlite3.Connection) -> None:
    """Bring pre-finalization databases up to the current jobs schema.

    Adds the attempt/cleanup columns when missing and rebuilds the table
    when the status CHECK constraint predates the 'finalizing' state.
    """
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(jobs)")}
    if "attempt_id" not in cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN attempt_id TEXT")
    if "cleanup_pending" not in cols:
        conn.execute(
            "ALTER TABLE jobs ADD COLUMN cleanup_pending "
            "INTEGER NOT NULL DEFAULT 0")
    sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='jobs'"
    ).fetchone()["sql"]
    if "'finalizing'" in sql:
        return
    # SQLite cannot alter a CHECK constraint: rebuild the table.
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute("BEGIN")
    try:
        conn.execute(_JOBS_TABLE_SQL.replace("CREATE TABLE jobs",
                                             "CREATE TABLE jobs_new"))
        conn.execute(
            f"INSERT INTO jobs_new ({_JOB_COLUMNS}) "
            f"SELECT {_JOB_COLUMNS} FROM jobs")
        conn.execute("DROP TABLE jobs")
        conn.execute("ALTER TABLE jobs_new RENAME TO jobs")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.execute("PRAGMA foreign_keys=ON")


# ---------- job helpers ----------

def create_job(row: dict[str, Any], samples: list[dict[str, str]]) -> None:
    with tx() as conn:
        conn.execute(
            """INSERT INTO jobs (job_id, lane, status, input_hash, barcode_table,
                                  r1_incoming, r2_incoming, error, instance_id,
                                  created_at, updated_at, heartbeat_at)
               VALUES (:job_id,:lane,'queued',:input_hash,:barcode_table,
                       :r1_incoming,:r2_incoming,NULL,NULL,
                       :created_at,:created_at,NULL)""",
            row,
        )
        conn.executemany(
            """INSERT INTO job_samples (job_id, sample_id, barcode_r1, barcode_r2)
               VALUES (:job_id,:sample_id,:barcode_r1,:barcode_r2)""",
            [
                {
                    "job_id": row["job_id"],
                    "sample_id": s["sample_id"],
                    "barcode_r1": s["barcode_r1"],
                    "barcode_r2": s["barcode_r2"],
                }
                for s in samples
            ],
        )


def get_job_by_lane(lane: str) -> sqlite3.Row | None:
    return get_conn().execute("SELECT * FROM jobs WHERE lane=?", (lane,)).fetchone()


def get_job(job_id: str) -> sqlite3.Row | None:
    return get_conn().execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()


def list_jobs() -> list[sqlite3.Row]:
    return get_conn().execute(
        "SELECT * FROM jobs ORDER BY created_at").fetchall()


def get_samples(job_id: str) -> list[sqlite3.Row]:
    return get_conn().execute(
        "SELECT * FROM job_samples WHERE job_id=? ORDER BY rowid", (job_id,)
    ).fetchall()


def claim_next_job(instance_id: str) -> sqlite3.Row | None:
    """Atomically take one queued job. Returns None when the queue is empty.

    Each claim mints a fresh attempt token; finalization writes for an
    older attempt are no-ops, so a late finisher can never clobber a
    retried attempt.
    """
    attempt_id = uuid.uuid4().hex
    with tx() as conn:
        row = conn.execute(
            "SELECT * FROM jobs WHERE status='queued' ORDER BY created_at LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        conn.execute(
            """UPDATE jobs SET status='processing', instance_id=?,
                               attempt_id=?, updated_at=?, heartbeat_at=?
               WHERE job_id=?""",
            (instance_id, attempt_id, _now(), _now(), row["job_id"]),
        )
    return get_job(row["job_id"])


def heartbeat(job_id: str, attempt_id: str | None) -> None:
    """Best-effort liveness touch; raises on lock timeout (caller tolerates)."""
    _heartbeat_conn().execute(
        "UPDATE jobs SET heartbeat_at=? WHERE job_id=? AND attempt_id IS ?",
        (_now(), job_id, attempt_id),
    )


def set_finalizing(job_id: str, attempt_id: str | None) -> bool:
    """Mark a still-owned job as finishing up (staging not yet clean)."""
    cur = get_conn().execute(
        """UPDATE jobs SET status='finalizing', updated_at=?
           WHERE job_id=? AND attempt_id IS ? AND status='processing'""",
        (_now(), job_id, attempt_id),
    )
    return cur.rowcount == 1


def mark_failed(job_id: str, attempt_id: str | None, error: str) -> bool:
    """Record the failure of one attempt.

    Guarded by the attempt token and the pre-failure states: returns False
    when the job has meanwhile been finalized by someone else or retried,
    in which case the caller must drop its own finalization.
    """
    cur = get_conn().execute(
        """UPDATE jobs SET status='failed', error=?, updated_at=?
           WHERE job_id=? AND attempt_id IS ?
                 AND status IN ('processing','finalizing')""",
        (error[:2000], _now(), job_id, attempt_id),
    )
    return cur.rowcount == 1


def requeue_job(job_id: str) -> bool:
    with tx() as conn:
        row = conn.execute(
            "SELECT status FROM jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        if row is None:
            return False
        if row["status"] != "failed":
            return False
        conn.execute(
            """UPDATE jobs SET status='queued', error=NULL, instance_id=NULL,
                               attempt_id=NULL, updated_at=?, heartbeat_at=NULL
               WHERE job_id=?""",
            (_now(), job_id),
        )
    return True


def publish_job(job_id: str, lane: str, counts: list[dict[str, Any]],
                attempt_id: str | None) -> None:
    """Publish metadata atomically; files are moved in beforehand.

    The job flips to published with cleanup_pending=1 in the same commit:
    if the process dies before the original inputs are reclaimed, the
    janitor (or the next startup) finishes the reclamation.
    """
    with tx() as conn:
        cur = conn.execute(
            """UPDATE jobs SET status='published', cleanup_pending=1,
                              updated_at=?
               WHERE job_id=? AND attempt_id IS ? AND status='processing'""",
            (_now(), job_id, attempt_id),
        )
        if cur.rowcount != 1:
            raise RuntimeError(
                f"job {job_id} is no longer owned by this attempt; "
                "refusing to publish")
        next_seq = conn.execute(
            "SELECT COALESCE(MAX(seq),0)+1 AS s FROM lane_publishes").fetchone()["s"]
        conn.execute(
            """INSERT INTO lane_publishes (lane, job_id, published_at, seq)
               VALUES (?,?,?,?)""",
            (lane, job_id, _now(), next_seq),
        )
        conn.executemany(
            """INSERT INTO lane_counts (lane, sample_id, r1_file, r2_file, read_pairs)
               VALUES (:lane,:sample_id,:r1_file,:r2_file,:read_pairs)""",
            [{**c, "lane": lane} for c in counts],
        )


def clear_cleanup_pending(job_id: str) -> bool:
    """Flag a published job's original-input reclamation as complete."""
    cur = get_conn().execute(
        """UPDATE jobs SET cleanup_pending=0, updated_at=?
           WHERE job_id=? AND status='published'""",
        (_now(), job_id),
    )
    return cur.rowcount == 1


def jobs_for_reap(cutoff_iso: str) -> list[sqlite3.Row]:
    """Processing/finalizing jobs whose heartbeat is older than the cutoff."""
    return get_conn().execute(
        """SELECT * FROM jobs
           WHERE status IN ('processing','finalizing')
                 AND COALESCE(heartbeat_at, updated_at) < ?""",
        (cutoff_iso,),
    ).fetchall()


def published_pending_cleanup() -> list[sqlite3.Row]:
    return get_conn().execute(
        "SELECT * FROM jobs WHERE status='published' AND cleanup_pending=1"
    ).fetchall()


# ---------- published lanes / snapshots ----------

def published_lanes() -> list[sqlite3.Row]:
    return get_conn().execute(
        "SELECT * FROM lane_publishes ORDER BY seq").fetchall()


def lane_counts(lane: str) -> list[sqlite3.Row]:
    return get_conn().execute(
        "SELECT * FROM lane_counts WHERE lane=? ORDER BY sample_id", (lane,)
    ).fetchall()


def create_snapshot(snapshot_id: str, lanes: list[str]) -> dict[str, Any]:
    now = _now()
    with tx() as conn:
        rows = conn.execute(
            "SELECT lane, seq FROM lane_publishes WHERE lane IN (%s) ORDER BY seq"
            % ",".join("?" * len(lanes)),
            lanes,
        ).fetchall()
        fixed = [r["lane"] for r in rows]
        conn.execute(
            "INSERT INTO snapshots (snapshot_id, created_at, lanes_json) VALUES (?,?,?)",
            (snapshot_id, now, json.dumps(fixed)),
        )
        conn.executemany(
            "INSERT INTO snapshot_lanes (snapshot_id, lane, seq) VALUES (?,?,?)",
            [(snapshot_id, r["lane"], r["seq"]) for r in rows],
        )
    return {"snapshot_id": snapshot_id, "created_at": now, "lanes": fixed}


def get_snapshot(snapshot_id: str) -> dict[str, Any] | None:
    row = get_conn().execute(
        "SELECT * FROM snapshots WHERE snapshot_id=?", (snapshot_id,)
    ).fetchone()
    if row is None:
        return None
    return {
        "snapshot_id": row["snapshot_id"],
        "created_at": row["created_at"],
        "lanes": json.loads(row["lanes_json"]),
    }


def list_snapshots() -> list[dict[str, Any]]:
    return [
        {
            "snapshot_id": r["snapshot_id"],
            "created_at": r["created_at"],
            "lanes": json.loads(r["lanes_json"]),
        }
        for r in get_conn().execute(
            "SELECT * FROM snapshots ORDER BY created_at").fetchall()
    ]
