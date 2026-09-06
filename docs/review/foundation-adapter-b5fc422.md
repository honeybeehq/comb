# Foundation adapter review at b5fc422

The implemented adapter passes its existing workspace tests, lint, build and CI, but four independent parent regressions fail. Root rebuilt a fixed archive and copied the test executable before executing each case with a 12-second process bound. [Receipt](../implementation/reliable-log/verification/foundation-adapter-b5fc422.json), [test source](../implementation/reliable-log/verification/foundation-adapter-parent.rs), [logs](../implementation/reliable-log/verification/foundation-adapter-b5fc422.txt).

## Executed findings

1. One unavailable release aborts a healthy sibling. The probe fails log A's final release CAS and delays log B's otherwise healthy release by 250 ms. `Bridge::close` returns on A's error and drops the JoinSet. B's durable lease remains present.
2. Failed opens exhaust the registry. After 256 distinct opens return BackendUnavailable, a healthy new log gets Busy even though no feed initialized. Failed and cancelled initialization must not retain capacity, and cleanup must preserve one shared slot while concurrent initializers still use it.
3. EOF skips a possible release. A broker already owns a feed; its next chunk upload stalls, while ref reads and release writes remain healthy. EOF terminates the process path within four seconds but skips session close, leaving the lease. A short admitted-request grace followed by cancellation and bounded independent cleanup can release it. An unavailable release backend can still leave a lease until TTL; no implementation can guarantee release after forced termination.
4. A five-second steady-state output pause terminates the transport. The four-second shutdown constant also governs every normal frame write. Keep a separate finite output budget and apply the short total shutdown budget on EOF.

## Source review dispositions

The independent source review agrees on release aggregation and failed-open slots. Root also requested diagnostics for every release outcome, including deadline uncertainty, and continuation past any unwrappable registry slot. A strict early return must not skip unrelated sessions.

Immediate EOF cancellation was rejected because it can discard an admitted hello or append. Preserve the passing short-request EOF cases. The constructor returns only InvalidLeasePolicy; the suggestion that this arm can silently swallow other lease errors does not follow from its actual type.

Wire bounds, typed errors, one lazy publisher per feed, exact stable-key bytes, zero-wait follow and accepted child-task/ID handling remain intact. Shared R2 production is unchanged. The authoritative local/MinIO/S3 immutable process capture and fresh-Loro gate waits for the focused follow-up.
