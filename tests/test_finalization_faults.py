"""Regression tests for the storage-fault finalization handoff.

Covers the incident modes from the field:
  * SQLite write lock makes heartbeat/failure registration time out; after
    the lock is released the job must still reach a retryable failed state
    (no restart, no stuck 'processing').
  * Deleting the original inputs fails after the publish committed; the
    job must stay published with counts/downloads intact and the cleanup
    must continue automatically.
  * Unfinalized jobs must not be retryable; a late finalization must not
    clobber a retry or a published product.
"""
import gzip
import importlib
import json
import sqlite3
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

BARCODES = [
    {"sample_id": "s1", "barcode_r1": "AAAAAAAA", "barcode_r2": "CCCCCCCC"},
]


@pytest.fixture()
def fault_env(tmp_path, monkeypatch):
    """Data dir with short SQLite waits and a fast janitor (no 30s stalls)."""
    d = tmp_path / "data"
    d.mkdir()
    monkeypatch.setenv("DEMUX_DATA_DIR", str(d))
    monkeypatch.setenv("DEMUX_SQLITE_BUSY_TIMEOUT_MS", "200")
    monkeypatch.setenv("DEMUX_HEARTBEAT_BUSY_TIMEOUT_MS", "100")
    monkeypatch.setenv("DEMUX_JANITOR_INTERVAL", "0.1")
    monkeypatch.setenv("DEMUX_REAP_AFTER_SECONDS", "0.5")
    monkeypatch.setenv("DEMUX_POLL_INTERVAL", "0.1")
    from app import config as cfg
    importlib.reload(cfg)
    for sub in (cfg.INCOMING_DIR, cfg.WORK_DIR, cfg.PUBLISHED_DIR):
        sub.mkdir(parents=True, exist_ok=True)
    from app import db as dbmod
    importlib.reload(dbmod)
    dbmod.init_db()
    yield d


@pytest.fixture()
def fault_client(fault_env):
    from app import main as mainmod
    with TestClient(mainmod.app) as c:
        yield c


def _post(client, lane, r1_path, r2_path, barcodes=BARCODES):
    with open(r1_path, "rb") as f1, open(r2_path, "rb") as f2:
        return client.post(
            "/jobs",
            data={"lane": lane, "barcodes": json.dumps(barcodes)},
            files={"r1": ("R1.fastq.gz", f1, "application/gzip"),
                   "r2": ("R2.fastq.gz", f2, "application/gzip")},
        )


def _wait_status(client, job_id, want, timeout=15):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        row = client.get(f"/jobs/{job_id}").json()
        last = row["status"]
        if row["status"] == want:
            return row
        time.sleep(0.05)
    raise AssertionError(f"job {job_id} stuck at {last}, wanted {want}")


def _wait_terminal(client, job_id, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = client.get(f"/jobs/{job_id}").json()
        if row["status"] in ("published", "failed"):
            return row
        time.sleep(0.05)
    raise AssertionError("job did not finish in time")


def _good_pair(tmp: Path, name1="R1.fastq.gz", name2="R2.fastq.gz"):
    r1 = tmp / name1
    r2 = tmp / name2
    with gzip.open(r1, "wb") as g:
        g.write(b"@x\nAAAAAAAATT\n+\nIIIIIIIIII\n")
    with gzip.open(r2, "wb") as g:
        g.write(b"@x\nCCCCCCCCGG\n+\nJJJJJJJJJJ\n")
    return r1, r2


def _make_processing_job(dbmod, data_dir, job_id, lane, attempt, tmp_path,
                         heartbeat="2000-01-01T00:00:00+00:00"):
    """Directly plant a processing job with real retained uploads."""
    r1, r2 = _good_pair(tmp_path)
    upload = data_dir / "incoming" / job_id
    upload.mkdir(parents=True, exist_ok=True)
    up1, up2 = upload / "R1.fastq.gz", upload / "R2.fastq.gz"
    up1.write_bytes(r1.read_bytes())
    up2.write_bytes(r2.read_bytes())
    (upload / "COMPLETE").touch()
    dbmod.create_job(
        {"job_id": job_id, "lane": lane, "input_hash": "x",
         "barcode_table": "[]", "r1_incoming": str(up1),
         "r2_incoming": str(up2), "created_at": dbmod.utcnow()},
        [{"sample_id": "s1", "barcode_r1": "AAAAAAAA",
          "barcode_r2": "CCCCCCCC"}],
    )
    dbmod.get_conn().execute(
        "UPDATE jobs SET status='processing', instance_id='ghost', "
        "attempt_id=?, heartbeat_at=? WHERE job_id=?",
        (attempt, heartbeat, job_id))
    return up1, up2


# ---------------------------------------------------------------- SQLite lock

def test_finalize_blocked_by_lock_completes_after_release(fault_env, tmp_path):
    """A real SQLite write lock defers finalization; releasing it lets the
    handoff land without any restart."""
    from app import config, db as dbmod, processing

    up1, _ = _make_processing_job(
        dbmod, fault_env, "lockjob", "lockLane", "att1", tmp_path)
    work = fault_env / "work" / "lockjob" / "output"
    work.mkdir(parents=True)  # staging debris to be cleaned

    lock_conn = sqlite3.connect(str(config.DB_PATH), isolation_level=None)
    lock_conn.execute("BEGIN IMMEDIATE")
    try:
        assert processing.finalize_failed_job("lockjob", "att1", "boom") is False
        row = dbmod.get_job("lockjob")
        assert row["status"] in ("processing", "finalizing")  # not terminal
    finally:
        lock_conn.rollback()
        lock_conn.close()

    assert processing.finalize_failed_job("lockjob", "att1", "boom") is True
    row = dbmod.get_job("lockjob")
    assert row["status"] == "failed"
    assert "boom" in row["error"]
    assert up1.is_file()          # both mates retained for the retry
    assert not work.exists()      # staging cleaned before the failure landed


def test_janitor_retries_failure_registration(fault_client, monkeypatch,
                                              tmp_path):
    """mark_failed keeps failing (lock storm); the janitor retries until the
    failure is durable, then the job is retryable."""
    from app import db as dbmod

    real = dbmod.mark_failed
    calls = {"n": 0}

    def flaky(job_id, attempt_id, error):
        calls["n"] += 1
        if calls["n"] <= 3:
            raise sqlite3.OperationalError("database is locked")
        return real(job_id, attempt_id, error)

    monkeypatch.setattr(dbmod, "mark_failed", flaky)

    r1, r2 = _good_pair(tmp_path)
    r1.write_bytes(r1.read_bytes()[:-6])  # corrupt gzip -> processing fails
    resp = _post(fault_client, "lockStorm", r1, r2)
    job_id = resp.json()["job"]["job_id"]

    job = _wait_status(fault_client, job_id, "failed")
    assert calls["n"] >= 3  # needed retries to get through the storm
    assert "format" in job["error"].lower() or "gzip" in job["error"].lower()
    assert job["uploads_retained"] is True

    # Retry now works and fails again on the still-corrupt input.
    assert fault_client.post(f"/jobs/{job_id}/retry").status_code == 200
    assert _wait_terminal(fault_client, job_id)["status"] == "failed"


def test_heartbeat_failure_does_not_kill_lane(fault_client, monkeypatch,
                                              tmp_path):
    from app import db as dbmod

    def boom(job_id, attempt_id):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(dbmod, "heartbeat", boom)
    r1, r2 = _good_pair(tmp_path)
    resp = _post(fault_client, "hbLane", r1, r2)
    job = _wait_terminal(fault_client, resp.json()["job"]["job_id"])
    assert job["status"] == "published"
    assert fault_client.get("/lanes/hbLane").status_code == 200


def test_queries_and_downloads_survive_write_lock(fault_client, tmp_path):
    """Readers (queries, existing downloads) are not blocked by a writer."""
    from app import config

    r1, r2 = _good_pair(tmp_path)
    resp = _post(fault_client, "readLane", r1, r2)
    job = _wait_terminal(fault_client, resp.json()["job"]["job_id"])
    assert job["status"] == "published"

    lock_conn = sqlite3.connect(str(config.DB_PATH), isolation_level=None)
    lock_conn.execute("BEGIN IMMEDIATE")
    try:
        t0 = time.time()
        assert fault_client.get("/jobs").status_code == 200
        assert fault_client.get("/lanes/readLane").status_code == 200
        dl = fault_client.get("/lanes/readLane/samples/s1/r1")
        assert dl.status_code == 200 and len(dl.content) > 0
        assert fault_client.get("/health").status_code == 200
        assert time.time() - t0 < 5  # no busy-timeout stalls for readers
    finally:
        lock_conn.rollback()
        lock_conn.close()


# ------------------------------------------------------- unfinalized != retry

def test_retry_refused_until_finalized(fault_client, monkeypatch, tmp_path):
    """While failure registration cannot land, the job sits in 'finalizing'
    and the retry endpoint refuses; once it lands, retry is accepted."""
    from app import db as dbmod

    def locked(job_id, attempt_id, error):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(dbmod, "mark_failed", locked)
    r1, r2 = _good_pair(tmp_path)
    r1.write_bytes(r1.read_bytes()[:-6])
    resp = _post(fault_client, "finLane", r1, r2)
    job_id = resp.json()["job"]["job_id"]

    job = _wait_status(fault_client, job_id, "finalizing")
    assert job["uploads_retained"] is True
    resp = fault_client.post(f"/jobs/{job_id}/retry")
    assert resp.status_code == 409
    assert "finalizing" in resp.json()["detail"]

    monkeypatch.undo()  # "lock released": real mark_failed again
    _wait_status(fault_client, job_id, "failed")
    assert fault_client.post(f"/jobs/{job_id}/retry").status_code == 200


# ------------------------------------------------------- post-publish faults

def test_publish_cleanup_failure_keeps_published(fault_client, monkeypatch,
                                                 tmp_path):
    """Input deletion fails after the publish commit: the job stays
    published with counts and downloads intact, and the janitor finishes
    the reclamation once the storage fault clears."""
    from app import db as dbmod

    real_unlink = Path.unlink

    def flaky_unlink(self, *args, **kwargs):
        if "incoming" in str(self):
            raise OSError(5, "simulated storage fault")
        return real_unlink(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", flaky_unlink)
    r1, r2 = _good_pair(tmp_path)
    resp = _post(fault_client, "pubLane", r1, r2)
    job_id = resp.json()["job"]["job_id"]
    job = _wait_terminal(fault_client, job_id)
    assert job["status"] == "published"

    # The historical bug flipped this job to failed; it must stay published.
    deadline = time.time() + 3
    while time.time() < deadline:
        row = dbmod.get_job(job_id)
        assert row["status"] == "published"
        assert row["cleanup_pending"] == 1  # reclamation still owed
        time.sleep(0.1)

    lane = fault_client.get("/lanes/pubLane").json()
    assert lane["total_read_pairs"] == 1
    by_sample = {c["sample_id"]: c["read_pairs"] for c in lane["counts"]}
    assert by_sample["s1"] == 1
    dl = fault_client.get("/lanes/pubLane/samples/s1/r1")
    assert dl.status_code == 200 and len(dl.content) > 0
    assert fault_client.post(f"/jobs/{job_id}/retry").status_code == 409

    # Fault clears: reclamation continues automatically, uploads disappear.
    monkeypatch.setattr(Path, "unlink", real_unlink)
    deadline = time.time() + 10
    while time.time() < deadline:
        row = dbmod.get_job(job_id)
        if row["cleanup_pending"] == 0:
            break
        time.sleep(0.1)
    assert row["cleanup_pending"] == 0
    assert row["status"] == "published"
    assert not Path(row["r1_incoming"]).exists()
    assert not Path(row["r2_incoming"]).exists()
    # Downloads still fine after the originals are gone.
    assert fault_client.get("/lanes/pubLane/samples/s1/r2").status_code == 200


def test_recovery_continues_pending_publish_cleanup(fault_env, tmp_path):
    """Crash between publish commit and reclamation: startup finishes it."""
    from app import db as dbmod, processing

    up1, up2 = _make_processing_job(
        dbmod, fault_env, "pubjob", "pubCrashLane", "att9", tmp_path)
    work = fault_env / "work" / "pubjob"
    work.mkdir(parents=True, exist_ok=True)
    conn = dbmod.get_conn()
    conn.execute(
        "UPDATE jobs SET status='published', cleanup_pending=1 "
        "WHERE job_id='pubjob'")
    conn.execute(
        "INSERT INTO lane_publishes (lane, job_id, published_at, seq) "
        "VALUES ('pubCrashLane','pubjob',?,1)", (dbmod.utcnow(),))

    notes = processing.recover_on_startup()
    assert any("pubjob" in n for n in notes)
    row = dbmod.get_job("pubjob")
    assert row["status"] == "published"
    assert row["cleanup_pending"] == 0
    assert not up1.exists() and not up2.exists()
    assert not work.exists()
    assert processing.recover_on_startup() == []  # idempotent


# --------------------------------------------------- late/duplicated finishers

def test_late_finalize_does_not_clobber_new_attempt(fault_env, tmp_path):
    """A stale finisher from attempt 1 must not touch attempt 2's state or
    staging, and must not touch a published lane's products."""
    from app import db as dbmod, processing

    # Attempt 2 is processing; attempt 1's finisher wakes up late.
    _make_processing_job(dbmod, fault_env, "racejob", "raceLane", "att2",
                         tmp_path)
    staging = fault_env / "work" / "racejob" / "output"
    staging.mkdir(parents=True)
    marker = staging / "attempt2-data"
    marker.write_text("keep")

    assert processing.finalize_failed_job("racejob", "att1", "late") is True
    row = dbmod.get_job("racejob")
    assert row["status"] == "processing"      # untouched
    assert row["attempt_id"] == "att2"
    assert marker.exists()                    # attempt 2's staging intact

    # A published job's products are untouchable by stray finalization.
    conn = dbmod.get_conn()
    conn.execute(
        "UPDATE jobs SET status='published', cleanup_pending=0 "
        "WHERE job_id='racejob'")
    conn.execute(
        "INSERT INTO lane_publishes (lane, job_id, published_at, seq) "
        "VALUES ('raceLane','racejob',?,1)", (dbmod.utcnow(),))
    final = fault_env / "published" / "lanes" / "raceLane"
    final.mkdir(parents=True)
    (final / "product").write_text("durable")
    assert processing.finalize_failed_job("racejob", "att2", "late") is True
    assert (final / "product").exists()
    assert dbmod.get_job("racejob")["status"] == "published"


def test_reaper_finalizes_abandoned_job_without_restart(fault_client,
                                                        fault_env, tmp_path):
    """Owner died mid-lane (no restart anywhere): the janitor's reaper
    finalizes the job so it becomes retryable, inputs retained."""
    from app import db as dbmod

    up1, _ = _make_processing_job(
        dbmod, fault_env, "ghostjob", "ghostLane", "attG", tmp_path)
    (fault_env / "work" / "ghostjob" / "output").mkdir(parents=True)

    job = _wait_status(fault_client, "ghostjob", "failed")
    assert "interrupted" in job["error"]
    assert job["uploads_retained"] is True
    assert not (fault_env / "work" / "ghostjob" / "output").exists()

    # The retained inputs make the retry succeed (this lane is healthy).
    assert fault_client.post("/jobs/ghostjob/retry").status_code == 200
    assert _wait_terminal(fault_client, "ghostjob")["status"] == "published"
    assert fault_client.get("/lanes/ghostLane").status_code == 200


def test_recovery_finalizes_finalizing_job(fault_env, tmp_path):
    """A job that died mid-finalization is finished at startup."""
    from app import db as dbmod, processing

    up1, _ = _make_processing_job(
        dbmod, fault_env, "finjob", "finLane", "attF", tmp_path)
    dbmod.get_conn().execute(
        "UPDATE jobs SET status='finalizing' WHERE job_id='finjob'")
    staging = fault_env / "work" / "finjob" / "output"
    staging.mkdir(parents=True)

    notes = processing.recover_on_startup()
    assert any("finjob" in n for n in notes)
    row = dbmod.get_job("finjob")
    assert row["status"] == "failed"
    assert "interrupted" in row["error"]
    assert up1.is_file()
    assert not staging.exists()


# ------------------------------------------------------------------ migration

_OLD_SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE jobs (
    job_id          TEXT PRIMARY KEY,
    lane            TEXT NOT NULL UNIQUE,
    status          TEXT NOT NULL CHECK (status IN
                        ('queued','processing','published','failed')),
    input_hash      TEXT NOT NULL,
    barcode_table   TEXT NOT NULL,
    r1_incoming     TEXT NOT NULL,
    r2_incoming     TEXT NOT NULL,
    error           TEXT,
    instance_id     TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    heartbeat_at    TEXT
);
CREATE TABLE job_samples (
    job_id      TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    sample_id   TEXT NOT NULL,
    barcode_r1  TEXT NOT NULL,
    barcode_r2  TEXT NOT NULL,
    PRIMARY KEY (job_id, sample_id)
);
"""


def test_old_schema_database_is_migrated(tmp_path, monkeypatch):
    d = tmp_path / "olddata"
    d.mkdir()
    monkeypatch.setenv("DEMUX_DATA_DIR", str(d))
    raw = sqlite3.connect(str(d / "demux.db"), isolation_level=None)
    raw.executescript(_OLD_SCHEMA)
    raw.execute(
        "INSERT INTO jobs (job_id, lane, status, input_hash, barcode_table,"
        " r1_incoming, r2_incoming, created_at, updated_at) VALUES "
        "('oldjob','oldLane','processing','h','[]','/a','/b',"
        "'2000-01-01T00:00:00+00:00','2000-01-01T00:00:00+00:00')")
    raw.close()

    from app import config as cfg
    importlib.reload(cfg)
    from app import db as dbmod
    importlib.reload(dbmod)
    dbmod.init_db()

    cols = {r["name"] for r in dbmod.get_conn().execute(
        "PRAGMA table_info(jobs)")}
    assert {"attempt_id", "cleanup_pending"} <= cols
    row = dbmod.get_job("oldjob")
    assert row["status"] == "processing"      # data preserved
    assert row["cleanup_pending"] == 0
    # The widened CHECK constraint accepts the finalizing state.
    dbmod.get_conn().execute(
        "UPDATE jobs SET status='finalizing' WHERE job_id='oldjob'")
    assert dbmod.get_job("oldjob")["status"] == "finalizing"
    # And the reaper query works on the migrated schema.
    assert dbmod.jobs_for_reap("2999-01-01T00:00:00+00:00")
