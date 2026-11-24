# durable-jobs

A small Python background-job queue that keeps job state in SQLite and
recovers abandoned work after a worker's lease expires. Standard library only,
Python3.10+. This is a working first slice, not a completed distributed task
platform or a replacement for Celery/Temporal.

## Why this project

Backend services often accept work that outlives one request or worker.
A crash should not silently lose the job, and duplicate submissions should
not enqueue duplicate jobs. That is the problem explored here.

The public GetCrux backend JD lists Python, Temporal, Celery, Redis and
RabbitMQ, and values debugging race conditions and service failures. Checked
23 November2026: https://www.ycombinator.com/companies/getcrux/jobs/Q7UBlgM-founding-engineer-backend-analytics-getcrux

This project explores the underlying reliability concepts with SQLite. It
does **not** establish experience operating Temporal, Celery or those brokers,
and copies no employer code or private design.

## Run

```sh
python -m unittest discover -s tests -v
python -m durable_jobs --db demo.sqlite3 submit --key example 10 20 30
# Copy the printed job ID, then:
python -m durable_jobs --db demo.sqlite3 work-once
python -m durable_jobs --db demo.sqlite3 status JOB_ID
```

Expected result is `{"total":60}` and state `succeeded`. Resubmitting the same
key/task/canonical JSON/max-attempts returns the same ID. Reusing that key for
a different request raises `Conflict`. No external services, paid APIs or
credentials are needed to run it. Database files contain inputs/results;
keep private data out of demo payloads and Git.

## Implemented

- Persisted states: `queued -> running -> succeeded`, or bounded retry back
to queued, or terminal `dead`.
- Idempotency-key uniqueness enforced inside the database transaction.
- Atomic claim using `BEGIN IMMEDIATE`, short-lived connections and a bounded
busy timeout. Claim and attempt increment commit together.
- Per-attempt lease tokens. Expired or replaced owners cannot acknowledge or
fail a newer attempt, even before another worker reclaims it.
- Expired leases are recovered during the next claim, not by a hidden daemon.
Attempts consume a bounded budget; exhausted jobs become dead on recovery.
- Retry delay persisted as an availability timestamp. The demo worker chooses
bounded exponential delays (1,2,4... up to60seconds); the store accepts an
explicit positive delay.
- `renew` extends a live lease from now; stale owners cannot renew. Long tasks
can heartbeat instead of sizing one big lease.
- Cancellation: a queued job cancels at once (terminal `canceled`). A running
job only gets a `cancel_requested` flag. The owner sees it as `CancelRequested`
on its next `renew` and calls `acknowledge_cancel`. If the owner finishes
first, `complete` wins and the job stays `succeeded`. If the owner dies, the
expired lease goes to `canceled`, not back to the queue. Nothing is killed
forcibly. `python -m durable_jobs --db PATH cancel ID` is the CLI form.
- Day-1 databases upgrade in place (new state and column, tested).
- Allowlisted deterministic `sum_numbers` task only. No arbitrary import,
shell, network or user-supplied code execution. `work-once` runs one attempt.
- Tests for concurrency, retries, stale owners, deduplication, persistence and
an actual subprocess exit after claiming, followed by recovery on restart.

## Guarantees and non-guarantees

Submission deduplication is **not exactly-once execution**. Attempts are
at-least-once after recovery: a worker may do its external side effect and die
before acknowledgement. Lease tokens fence this database's acknowledgements,
not a payment/email/other external system. Such effects need their own
idempotency protocol or transactional outbox. No external side effects are
implemented here.

SQLite serializes writers. This is a local single-database prototype, not a
broker distributed over hosts. Concurrency tests show correctness for their
specific contenders, not throughput, scalability or full fault tolerance.
No power-loss, disk corruption, network filesystem or clock-jump test is
claimed. Wall-clock changes affect leases; jobs longer than their lease can
be retried unless the worker renews. Cancellation is cooperative: a worker
that never renews learns of it only when its lease expires.

No API, dead-letter replay, automatic heartbeat thread, metrics, scheduler,
priority queue, retention or production deployment yet. Dead jobs can be
inspected with status. A stuck process is recovered after its lease expires
and another claim runs. Only the one Day-1 to Day-2 migration exists.

## Evidence

`python -m unittest discover -s tests -v` runs 35 tests without dependencies.
The subprocess recovery test uses an actual child process that exits with
code23 after claiming. A deterministic injected clock moves past expiry
without a flaky sleep. Concurrent claims/submissions use independent SQLite
connections across threads, plus a separate eight-process claim check. These are correctness checks, not benchmarks.

See `docs/day-01.md` for tradeoffs and interview questions. No forecast
performance numbers or unimplemented capabilities are presented as results.

## Technical sources

- SQLite transaction/locking semantics: https://www.sqlite.org/lang_transaction.html
- Celery task/idempotency background: https://docs.celeryq.dev/en/stable/userguide/tasks.html

Project code is MIT licensed. SQLite/Python licenses are separate.
