# R2 closure review at 1698ecd

All 24 preceding parent regressions pass on fixed `1698ecd47824c69df8d47e849404c7092e937577`. One bounded-termination case remains open. Capabilities stay false until the outstanding append observes loss while backend I/O is suspended.

Root rebuilt a git archive in a private target and ran 25 exact test processes against a copied immutable executable. [Receipt and hashes](../implementation/reliable-log/verification/r2-closure-1698ecd.json), [source](../implementation/reliable-log/verification/r2-closure-1698ecd.rs), [logs](../implementation/reliable-log/verification/r2-closure-1698ecd.txt).

## Closed since a53d49b

`PreparedMutation.live_lease` makes live ownership an explicit requirement of the complete-feed operation. Generic target, claim, and release plans leave the guard absent. Both shared CAS paths preserve the already-persisted publication timestamp and use a fresh clock only for validation.

The real-clock claim retry, legacy unfenced target update after lease expiry, and resumed append after Lost cases now pass. The earlier 21 cases remain green. Source inspection also confirms typed `LeaseExpired` and `LeaseHeld` handling instead of lifecycle decisions based on `Rejected` message text.

## Outstanding append does not terminate when loss occurs

`review_lost_session_returns_while_upload_stays_blocked` uses the existing Lost regression with one change: it never resumes the backend upload. After renewal fails and the session reports Lost, the pending append still has not returned after 250 milliseconds.

The final guard prevents publication when the upload eventually resumes, so this is a bounded-termination defect, not a new duplicate-publication result. `timed_cas` watches caller cancellation and deadline. It does not watch the session loss token or the confirmed lease deadline during preparation and upload. A short lease can therefore end while the operation continues waiting for the caller's longer deadline.

Select the new-publication await on loss or close and the confirmed lease deadline, as well as caller cancellation and deadline. Retain the final CAS guard. Committed-key lookup remains before this ownership-bound phase so a Lost session can recover an existing receipt. Cancellation after a CAS was issued can still leave an unknown outcome; the stable-key retry must resolve it.

Root sent the exact test and narrowed `/tmp/comb-r2-current-task.md` to this remaining case. The worker acknowledged the missing await selection. No catalog, namespace, R1, or timestamp redesign remains.

## Integration preparation

A read-only `git merge-tree` check against the current integration branch reports conflicts in `publish.rs` and `store.rs`. Root will resolve these using the checked R2 versions after closure, preserving accepted R1 and Foundation content, then verify the merged tree before handing the APIs to Foundation. No worktree merge or adapter wiring occurred during this review.
