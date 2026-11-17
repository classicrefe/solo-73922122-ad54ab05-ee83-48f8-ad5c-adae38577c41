# Sequencing Lane Demultiplexing API

FastAPI + SQLite service that splits mixed paired-end gzip FASTQ uploads into
per-sample files. Designed to run as **two container instances on one shared
volume** with a **128 MiB memory limit**.

## What it does

* Accepts a lane number, a dual-index barcode table (1–8 samples, two
  8-base barcodes each) and paired `R1`/`R2` gzip FASTQ files.
  Decompressed input across both mates is capped at **512 MiB**.
* Validates both mates: valid gzip, well-formed FASTQ, equal record counts,
  identical read names in the same order, matching sequence/quality lengths.
  Any corruption fails the **whole lane**.
* Assigns a pair to a sample when the combined R1+R2 barcode matches with
  **at most one substitution** and the candidate sample is **unique**.
  Everything else (no hit, >1 substitution, or a tie) goes to quarantine.
* Assigned reads have both 8-base barcodes and their corresponding quality
  characters trimmed; quarantined reads are kept intact.
* A lane becomes visible only when **all** per-sample paired files, the
  quarantine paired files and the count table exist (atomic publish:
  files staged, renamed, then a single DB transaction).
* Same lane + identical input (hash of barcode table and both files) reuses
  the existing task; same lane with different input is rejected with `409`.
* Failed tasks pass through a **`finalizing`** state: staging is cleaned
  first, then the failure is recorded; only then can the task be retried
  (`POST /jobs/{id}/retry`). Both original uploads are always retained for
  a failed task. Retrying a task that has not finished finalizing is
  refused with `409`.
* A committed publish is **never** converted into a failure by reclamation
  faults: the task stays `published` with its counts and downloads, and
  deleting the original uploads/scratch is retried in the background
  (`cleanup_pending` on the task shows it is still owed).
* Snapshots freeze the published lane list at creation time. Sample/
  quarantine downloads through a snapshot only ever concatenate files from
  that fixed list — lanes published later never mix in.

## Running (Docker Compose, two instances on a shared volume)

```bash
docker compose up --build
# instance A: http://localhost:8001   instance B: http://localhost:8002
# OpenAPI demo UI:  http://localhost:8001/docs  (also on 8002)
```

Both containers mount the named volume `demux-shared` at `/data`, are capped
at 128 MiB (`mem_limit`), and automatically recover on restart.

### Quick demo

```bash
python scripts/make_demo_fastq.py /tmp/demo          # makes R1/R2.fastq.gz
# In http://localhost:8001/docs -> POST /jobs:
#   lane: demoLane
#   barcodes: [{"sample_id":"sampleA","barcode_r1":"AAAAAAAA","barcode_r2":"CCCCCCCC"},
#              {"sample_id":"sampleB","barcode_r1":"GGGGGGGG","barcode_r2":"TTTTTTTT"}]
#   r1/r2: the generated gzip files
curl http://localhost:8001/lanes/demoLane             # counts
curl -OJ http://localhost:8001/lanes/demoLane/samples/sampleA/r1
curl -OJ http://localhost:8001/lanes/demoLane/quarantine/r1
```

### Retry/finalization demo via /docs

1. `POST /jobs` with a truncated/corrupt `r1` (e.g. cut a few bytes off the
   gzip). Poll `GET /jobs/{id}`: the task goes `processing` → (`finalizing`
   if the storage layer is faulting) → `failed`, uploads retained.
2. `POST /jobs/{id}/retry` — refused with `409` while `finalizing`,
   accepted once `failed`. The retry fails again on the same corrupt input.
3. Resubmitting the same lane with identical input reuses the task; with
   different input it is rejected (`409`).
4. `GET /health` shows `pending_finalizations` — finalizations the janitor
   is still retrying (e.g. while the SQLite volume is faulting).

## API surface

| Method | Path | Purpose |
|---|---|---|
| POST | `/jobs` | Submit a lane (multipart: `lane`, `barcodes` JSON, `r1`, `r2`) |
| GET | `/jobs`, `/jobs/{id}` | Task status (`queued`/`processing`/`finalizing`/`published`/`failed`) |
| POST | `/jobs/{id}/retry` | Requeue a finalized failed task (409 while `finalizing`) |
| GET | `/lanes`, `/lanes/{lane}` | Published lanes and per-bucket counts |
| GET | `/lanes/{lane}/samples/{sample_id}/{r1\|r2}` | Download a sample mate |
| GET | `/lanes/{lane}/quarantine/{r1\|r2}` | Download a quarantine mate |
| POST | `/snapshots` | Freeze the current published lane list |
| GET | `/snapshots`, `/snapshots/{id}` | List/inspect snapshots |
| GET | `/snapshots/{id}/samples/{sample_id}/{mate}` | Snapshot-scoped concatenated download |
| GET | `/snapshots/{id}/quarantine/{mate}` | Snapshot-scoped quarantine download |
| GET | `/health` | Liveness, instance id, current job, queue depth, pending finalizations |

## Concurrency and crash safety

* **SQLite WAL** with `BEGIN IMMEDIATE` job claiming: exactly one instance
  ever picks up a given job; the other keeps serving requests. Readers
  (queries, downloads) never wait for the writer, so a locked database or
  a slow cleanup never blocks them.
* Each instance processes one lane at a time in a background thread, so a
  slow lane never blocks uploads, queries or downloads on the event loop.
* A job runs under an exclusive `flock` on its work directory. Startup
  recovery removes scratch/staging debris only when no lock is held, and
  never deletes directories that have a published-lane DB row — so another
  instance's in-flight work and all published data are preserved.
* Publish order: write/verify outputs in `work/`, move to
  `published/lanes/.tmp.<job>`, atomic `rename` to the final lane directory,
  then insert metadata in one DB transaction. Inputs are reclaimed only
  after the commit, so a crash at any point leaves no half-visible lane.

### Task end ⇄ file reclamation (finalization)

* Every claim mints an **attempt token**; all finalization writes are
  guarded by it and serialized on the job flock, so a late finisher can
  never clobber a retried attempt or a published product.
* **Failure path** (unpublished): the worker hands the failure to a
  per-instance **janitor** thread, which cleans the staging and *then*
  records the failure — retrying through SQLite lock storms and storage
  faults instead of blocking longer. The task shows `finalizing` until the
  record lands and cannot be retried before that; both original uploads
  are kept for the retry.
* **Publish path** (committed): the task flips to `published` with
  `cleanup_pending=1` in the same transaction. Reclaiming the original
  uploads/scratch happens in the janitor; faults leave the task published
  (counts and downloads intact) and the cleanup is retried until done.
* **Reaper**: jobs left in `processing`/`finalizing` by a vanished owner
  (free flock + stale heartbeat) are finalized by the janitor without
  waiting for a restart; startup recovery does the same after restarts and
  also finishes interrupted publish cleanups. Heartbeats are advisory and
  fail fast on a dedicated connection — a locked database never kills a
  healthy lane.
* SQLite waits are **not** extended to ride out faults
  (`DEMUX_SQLITE_BUSY_TIMEOUT_MS` stays at 30 s by default; heartbeats use
  a 1 s budget): the janitor retries instead of the database waiting.

Tunable env vars: `DEMUX_SQLITE_BUSY_TIMEOUT_MS`,
`DEMUX_HEARTBEAT_BUSY_TIMEOUT_MS`, `DEMUX_JANITOR_INTERVAL`,
`DEMUX_REAP_AFTER_SECONDS`, `DEMUX_POLL_INTERVAL`.

## Memory

All I/O is streamed record-by-record (1 MiB network chunks, line-based gzip
reads, one open gzip writer per bucket). No file is ever buffered whole.
The automated streaming test processes hundreds of MiB of decompressed
data with RSS growth below 64 MiB.

## Layout

```
app/            FastAPI app, SQLite layer, streaming splitter, worker, janitor
scripts/        demo FASTQ generator, two-instance integration check
tests/          pytest suite (happy path, corruption, retry, recovery,
                barcode rules, size budget, streaming memory,
                storage-fault finalization regressions)
Dockerfile      python:3.11-slim, non-root user, /data volume, healthcheck
docker-compose.yml  two 128 MiB instances sharing one named volume
```

## Tests

```bash
python -m pytest tests/ -q
python scripts/integration_two_instances.py   # real two-process check
```

`tests/test_finalization_faults.py` replays the storage-fault incident:
SQLite write-lock storms during heartbeat/failure registration, input
reclamation failing after a committed publish, retry refused until
finalized, late finishers vs. retries, abandoned-owner reaping, restart
recovery of pending cleanups, and old-schema migration. The integration
script adds a live write-lock drill against two real instances.
