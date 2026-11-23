# Day1: persistent claims and crash recovery

23 November2026. Working first slice, not a complete background-job platform.

Implemented a SQLite state machine, deduplicated submissions, atomic claims,
per-attempt lease fencing, delayed retries and terminal dead state, plus a
small CLI and a pure allowlisted task.22 tests include actual process exit
and recovery, concurrent contenders and stale acknowledgement rejection.

The useful learning gap is reliability under failure, not another thin API
wrapper. The public GetCrux backend stack names workflow/task frameworks;
this implementation teaches some underlying mechanics without claiming those
frameworks were used.

Decisions:
- `BEGIN IMMEDIATE` makes claim selection/update one serialized write
transaction. Correctness before throughput; no throughput benchmark yet.
- Canonical JSON makes key conflicts deterministic for JSON dictionaries.
- Tokens change on each claim, so an expired owner cannot overwrite success
from the current owner. They do not stop external side effects.
- Retry timestamps survive process restart. Recovery runs during claim.
- Standard library only; no infrastructure or paid service required.

Next useful slices, not implemented claims: add cancellation semantics and
lease renewal with race tests, a typed API. Decide the bounded completion contract before expanding into
brokers, DAGs or deployment. No claim to clone Celery/Temporal.

Interview questions:
1. Where does a duplicate side effect happen even with an idempotent submit?
2. Why must a lease token change on every retry, not just every worker?
3. Which clock failure makes a lease expire too early or too late?
4. What can one SQLite writer transaction guarantee, and what does it cost?
5. Why is a terminal dead state useful without automatically replaying it?
