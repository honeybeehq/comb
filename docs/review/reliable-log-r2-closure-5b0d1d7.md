# R2 closure review at 5b0d1d7

R2 remains unaccepted. All 15 existing parent regressions pass, including 12 additional pre-cancelled calls and 12 expired calls. Six new cases reproduce incomplete closure of two requirements: binding a ref to its committed target, and enforcing lease expiry across awaited work.

Root reviewed `5b0d1d71f3754cb63b8f8873907638009e7117ec`, extracted it with `git archive`, and rebuilt in a private target. Each test ran against a copied immutable executable with an external eight-second limit. No worker files or shared targets were changed. [Receipt and binary hashes](../implementation/reliable-log/verification/r2-closure-5b0d1d7.json), [test source](../implementation/reliable-log/verification/r2-closure-5b0d1d7.rs), [execution logs](../implementation/reliable-log/verification/r2-closure-5b0d1d7.txt).

## Closed cases

The original cancellation, acquisition-mutex deadline, takeover replay, idle expiry, missing immediate append target, catalog transient, and head/catalog cases pass. The earlier namespace, malformed resource, overflow, bounded-read, and close cases still pass. Source inspection confirms that legacy v2 refs and history use the original unbounded read path while v3 reads remain bounded. The same-snapshot stable lookup now runs again after acquisition and before `commit_at_snapshot`.

These are material improvements. The remaining tests exercise the same invariants after a different valid preceding operation or across suspended I/O.

## F1: a generic lease commit is not proof of an empty feed

`feed.rs:552`, `published_or_empty`, returns an empty feed for any current commit with `target_follows_commit == false`. Generic claim and release commits also use that representation when they retain an existing log target. The persisted `ref_state.target` distinguishes these cases, but the helper does not inspect it.

Executed traces:

| Test | Observed result |
| --- | --- |
| `review_missing_target_after_release_is_not_empty` | Append, close, clear only the ref target. Read succeeds with no events and `at_head=true`. |
| `review_missing_target_after_release_cannot_reset_key` | The same trace followed by a fresh writer accepts different bytes for the committed key, at range 1..1 and generation 5. |
| `review_old_target_cannot_hide_committed_key` | After two appends, replace only the target with the valid first manifest. Different bytes for the second key succeed at range 2..2 and generation 4. |

The last case follows the target-present path. `load_manifest` validates the manifest internally, but no shared check binds that target to the current ref's `head_commit`. A valid older manifest can therefore hide committed stable-key evidence.

These tests deliberately inject inconsistent ref metadata. They do not show that ordinary append clears or rolls back the target. The complete-feed contract requires this inconsistency to fail closed, and the retained commit already contains the evidence needed to detect it.

The correction must validate one paired ref/commit/target snapshot for open, head, read, and append. Reconstruct the target from the current commit's persisted state, including manifest self-target reconstruction, and compare it with the live target. Check resource and generation binding. Genuine lease-only initialization has an empty persisted target. A generic commit alone is insufficient. Preserve lease fields that can legitimately change through renewal without a new commit.

## F2: lease checks do not cover awaited work

`publish.rs:1350` samples the clock before `renew_owned_lease` awaits `read_head`. It uses that stale time to check expiry and construct the replacement. `feed.rs:1180` awaits the renewal without a timeout. Checking slack before the call does not bound the call itself.

New append has the same timing gap. `commit_at_snapshot` samples `now` at `publish.rs:954`, then awaits skip reads, preparation, and uploads. `CompleteAppendPlan::prepare` validates the lease against that old time. The final ref CAS at `publish.rs:1073` has no fresh expiry check.

Executed traces:

| Test | Observed result |
| --- | --- |
| `review_renewal_rechecks_clock_after_delayed_read` | Pause the renewal read, advance the Store clock past TTL, resume. An extra ref CAS rewrites the expired lease. |
| `review_hung_idle_renewal_loses_by_lease_deadline` | Block renewal I/O with a one-second lease. After 1.3 seconds in the blocked call, the session still reports Active. |
| `review_append_cannot_publish_after_lease_expires_during_upload` | Pause an object upload, advance the Store clock past TTL, resume. A new append succeeds at range 1..1 and generation 2. |

The paused operations are deterministic backend barriers. The third case pauses before final publication, so it is not a claim that cancelling an already-issued CAS can undo it.

Renewal and new publication need the confirmed lease deadline in addition to caller cancellation and deadlines. Refresh the clock after awaited reads and before final CAS. Expiry or uncertain renewal must move the session to Lost, and suspended preparation must not issue a new publication after ownership is lost. A committed stable-key retry must retain its ability to return the original receipt without reacquiring ownership.

## Disposition

Root sent the executed cases to owner `3311b020` and replaced `/tmp/comb-r2-current-task.md` with these two requirements. Foundation capabilities remain false. Adapter wiring and live backend acceptance still wait for corrected R2. Existing R1 and Pheromone A1 acceptance remains unchanged.
