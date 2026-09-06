# Foundation adapter closure at f238b0f

All four unchanged parent regressions pass on the fixed archive at `f238b0fe0c13a2399d8b30e4abfe453254d2de99`, integrated as `d1f8f7c`. The same tests failed on `b5fc422`. Root rebuilt and copied the executable before running each case with a 12-second process bound. [Fixed receipt](../implementation/reliable-log/verification/foundation-adapter-f238b0f.json), [baseline findings](foundation-adapter-b5fc422.md), [unchanged test source](../implementation/reliable-log/verification/foundation-adapter-parent.rs), [fixed logs](../implementation/reliable-log/verification/foundation-adapter-f238b0f.txt).

## Closed findings

- One failed release no longer aborts a healthy sibling. Close reports completed, failed and uncertain logs after attempting independent releases through one deadline. A retained registry slot skips only its own release.
- Failed and cancelled opens release unused registry capacity. The last request drops its Arc under the registry lock before checking whether the empty slot has another waiter. This prevents replacing a slot while a waiter can still initialize it. Initialized sessions stay registered, preserving Lost state.
- EOF gives admitted requests 500 ms to finish, then 250 ms to cancel and drain, followed by up to three seconds for session releases inside the four-second total. The parent test stalls only chunk upload while keeping release I/O healthy; cleanup clears the durable lease.
- Steady output writes have a separate 30-second bound. The unchanged parent test resumes a blocked reader after five seconds and receives its response.

The independent source closure review found no new blocker. A forced termination or unavailable backend can leave a lease until its TTL. EOF does not promise that a slow remote append completes inside the short grace; clients should await responses before closing stdin. If an append result is uncertain, retry the same key and bytes. A diagnostic race that may report an input-ended error instead of the underlying transport error is deferred; either path fails closed.

Shared Store, Log, catalog and session production is unchanged by this follow-up. Final immutable backend capture and fresh Foundation replay are recorded separately when complete.
