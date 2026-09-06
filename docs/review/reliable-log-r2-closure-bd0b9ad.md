# R2 closure review at bd0b9ad

All 25 unchanged parent cases pass on `bd0b9adb30a911c542b6cbee35deaab297ef4e68`. The no-resume Lost case is closed by execution. One source-scope correction remains before integration: the initial history read occurs outside the ownership-bound future.

Root extracted a fixed git archive, rebuilt in a private target, and ran each test against a copied immutable executable. [Receipt and hashes](../implementation/reliable-log/verification/r2-closure-bd0b9ad.json), [unchanged test source](../implementation/reliable-log/verification/r2-closure-1698ecd.rs), [logs](../implementation/reliable-log/verification/r2-closure-bd0b9ad.txt).

## Verified correction

`publication_io` selects on the opt-in guard's loss token and remaining lease lifetime. The complete-feed plan supplies the guard before preparation. Preparation, object uploads, and the final ref CAS use it. Generic plans keep the unguarded path. The final `enforce_live_lease` remains in place, and no persisted timestamps change.

This closes the blocked-upload case without weakening the previously verified receipt, legacy, target-binding, or final-publication behavior.

## Remaining source scope

`commit_at_snapshot` awaits `compute_skip` before constructing the guard and calling `publication_io`. A delayed history read there therefore remains governed only by the caller's longer timeout. This is the same outstanding-I/O requirement at an earlier await, not a newly reproduced publication or integrity failure.

The correction is an outer guard around the whole new-publication future, beginning before `compute_skip`, with the final pre-CAS guard retained. Committed-key lookup remains outside that ownership-bound phase. An outer wrapper avoids maintaining a list of individually guarded awaits.

Root sent this one source correction and rewrote the current-only worker task. No new runtime cases were requested. Foundation capabilities remain false pending the fixed source scope, merged-tree verification, and actual bridge acceptance.
