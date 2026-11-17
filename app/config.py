"""Runtime configuration driven by environment variables.

All state lives under one shared data directory so two container instances
mounting the same volume can cooperate.
"""
from __future__ import annotations

import os
from pathlib import Path


def _default_data_dir() -> Path:
    candidate = Path(os.environ.get("DEMUX_DATA_DIR", "/data"))
    try:
        candidate.mkdir(parents=True, exist_ok=True)
        probe = candidate / ".write_test"
        probe.touch()
        probe.unlink()
        return candidate.resolve()
    except OSError:
        # Fallback for non-container local runs/tests.
        fallback = Path(os.environ.get("TMPDIR", "/tmp")) / "demux-data"
        fallback.mkdir(parents=True, exist_ok=True)
        return fallback.resolve()


DATA_DIR = _default_data_dir()
DB_PATH = DATA_DIR / "demux.db"
INCOMING_DIR = DATA_DIR / "incoming"
WORK_DIR = DATA_DIR / "work"
PUBLISHED_DIR = DATA_DIR / "published"

# Hard limits from the task specification.
MAX_SAMPLES = 8
BARCODE_LEN = 8
MAX_TOTAL_UNCOMPRESSED = 512 * 1024 * 1024  # 512 MiB across both mates
HEARTBEAT_FLUSH_EVERY = 256  # records between heartbeat writes
POLL_INTERVAL = float(os.environ.get("DEMUX_POLL_INTERVAL", "1.0"))

# SQLite waits. These are never *extended* to ride out storage faults: the
# janitor retries finalization instead of blocking longer.
BUSY_TIMEOUT_MS = int(os.environ.get("DEMUX_SQLITE_BUSY_TIMEOUT_MS", "30000"))
# Heartbeats are advisory; they fail fast on a dedicated connection so a
# locked database slows the lane by at most this much per flush.
HEARTBEAT_BUSY_TIMEOUT_MS = int(
    os.environ.get("DEMUX_HEARTBEAT_BUSY_TIMEOUT_MS", "1000"))

# Finalization (janitor) cadence and orphan reaping.
JANITOR_INTERVAL = float(os.environ.get("DEMUX_JANITOR_INTERVAL", "2.0"))
# A processing/finalizing job whose flock is free AND whose heartbeat is
# older than this is considered abandoned and gets finalized by the reaper.
# The freshness check protects the brief claim->flock-acquire window.
REAP_AFTER_SECONDS = float(os.environ.get("DEMUX_REAP_AFTER_SECONDS", "60"))

for _d in (DATA_DIR, INCOMING_DIR, WORK_DIR, PUBLISHED_DIR):
    _d.mkdir(parents=True, exist_ok=True)
