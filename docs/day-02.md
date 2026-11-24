# Day 2: lease renewal and cancellation

## Decisions

- `renew` is rejected for expired or replaced owners, same as `complete`.
- Cancel of a queued job is terminal and immediate. Cancel of a running job is
  a request, because the queue cannot stop a process it does not own.
- Race rule: if `complete` reaches the database before `acknowledge_cancel`,
  the job succeeded and stays succeeded. The effect already happened.
- Crash rule: an expired lease with a pending cancel request becomes
  `canceled`, so a cancelled job is never retried.
- Schema: `canceled` state and `cancel_requested` column, with an in-place
  rebuild for Day-1 databases.

## Tests added (13)

Renewal from now, expiry after renewal, stale owner after reclaim, input
validation; cancel while queued, cooperative running cancel, complete winning,
crashed owner, idempotent cancel, stale acknowledgement, 25 rounds of a
thread race between claim and cancel, and a real Day-1 database migration.

## Limits

No forced kill, no automatic heartbeat thread, no cancel reason or audit log,
still at-least-once attempts with no exactly-once external effects.

## Questions to be able to answer

- Why does complete win over a cancel request?
- What happens to a worker that never renews and is cancelled?
- Why is cancel of a running job not terminal?
