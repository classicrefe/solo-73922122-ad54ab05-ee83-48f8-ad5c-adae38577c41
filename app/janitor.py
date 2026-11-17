"""Janitor: finishes task-end <-> file-reclamation handoffs in the background.

The worker thread hands finalizations over instead of blocking on them, so
a locked SQLite or a sick filesystem never stalls lane processing:

* failure finalization - clean a failed attempt's staging, then record the
  failure (retried until the write lands; the job shows ``finalizing``
  meanwhile and cannot be retried yet);
* publish cleanup - reclaim a published job's original uploads and scratch
  (the job stays published with its counts and downloads untouched);
* reaping - finalize processing/finalizing jobs whose owner vanished
  (free flock + stale heartbeat), so a crashed instance's lanes become
  retryable without waiting for a restart;
* pending publish cleanups left behind by a crash (cleanup_pending=1).

All file work is serialized on the per-job flock and all state transitions
are guarded by the attempt token, so a late finalization can never break a
retry or a published product.
"""
from __future__ import annotations

import logging
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone

from . import config, db, processing

log = logging.getLogger("demux.janitor")


class Janitor(threading.Thread):
    def __init__(self, instance_id: str):
        super().__init__(name=f"demux-janitor-{instance_id[:8]}", daemon=True)
        self.instance_id = instance_id
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._lock = threading.Lock()
        # (not_before_monotonic, kind, args); kind in {"fail", "clean"}
        self._pending: deque[tuple[float, str, tuple]] = deque()

    # ---------------- enqueued by the worker ----------------

    def enqueue_failure(self, job_id: str, attempt_id: str | None,
                        error: str) -> None:
        self._add("fail", (job_id, attempt_id, error))

    def enqueue_publish_cleanup(self, job_id: str) -> None:
        self._add("clean", (job_id,))

    def _add(self, kind: str, args: tuple, delay: float = 0.0) -> None:
        with self._lock:
            self._pending.append((time.monotonic() + delay, kind, args))
        self._wake.set()

    @property
    def pending(self) -> int:
        with self._lock:
            return len(self._pending)

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    # ---------------- main loop ----------------

    def run(self) -> None:
        log.info("janitor %s started", self.instance_id)
        while not self._stop.is_set():
            try:
                self._process_due()
                self._sweep()
            except Exception:
                log.exception("janitor iteration failed")
            self._wake.wait(config.JANITOR_INTERVAL)
            self._wake.clear()

    def _process_due(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                index = next(
                    (i for i, (not_before, _, _) in enumerate(self._pending)
                     if not_before <= now),
                    None,
                )
                if index is None:
                    return
                _, kind, args = self._pending[index]
                del self._pending[index]
            try:
                done = self._dispatch(kind, args)
            except Exception:
                log.exception("janitor task %s %s crashed", kind, args)
                done = False
            if not done:
                # Storage/SQLite still faulting: back off and retry. The
                # periodic sweep also re-derives this work from the DB, so
                # nothing is lost if this entry is dropped on shutdown.
                self._add(kind, args, delay=config.JANITOR_INTERVAL)

    @staticmethod
    def _dispatch(kind: str, args: tuple) -> bool:
        if kind == "fail":
            job_id, attempt_id, error = args
            return processing.finalize_failed_job(job_id, attempt_id, error)
        if kind == "clean":
            return processing.cleanup_published_job(args[0])
        raise ValueError(f"unknown janitor task {kind!r}")

    def _sweep(self) -> None:
        # Reap jobs whose owner disappeared without finishing. The flock is
        # the authority; the staleness check only protects the brief
        # claim->flock-acquire window of a live claimant.
        cutoff = (datetime.now(timezone.utc)
                  - timedelta(seconds=config.REAP_AFTER_SECONDS)).isoformat()
        for row in db.jobs_for_reap(cutoff):
            try:
                if not processing._lock_is_free(
                        processing._lock_path(row["job_id"])):
                    continue  # a live instance holds this job
                if processing.finalize_failed_job(
                        row["job_id"], row["attempt_id"],
                        "interrupted (owner process disappeared)"):
                    log.warning("reaped abandoned job %s", row["job_id"])
            except Exception:
                log.exception("reaper failed on job %s", row["job_id"])
        # Continue published-job reclamation interrupted by a crash or a
        # storage fault.
        for row in db.published_pending_cleanup():
            try:
                processing.cleanup_published_job(row["job_id"])
            except Exception:
                log.exception("publish cleanup failed for job %s",
                              row["job_id"])
