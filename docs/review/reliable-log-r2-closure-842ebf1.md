# R2 closure at 842ebf1

R2 is accepted for bridge integration at `842ebf1d7620b2aa7397782e31b326d538c22d32`. All 25 unchanged parent regressions pass on a rebuilt immutable executable. [Receipt and hashes](../implementation/reliable-log/verification/r2-closure-842ebf1.json), [test source](../implementation/reliable-log/verification/r2-closure-1698ecd.rs), [logs](../implementation/reliable-log/verification/r2-closure-842ebf1.txt).

## Final source correction

`commit_at_snapshot` constructs the optional lease guard before awaiting `commit_at_snapshot_inner`. One `publication_io` wrapper covers initial `compute_skip`, preparation, uploads and the final ref CAS. The inner method no longer maintains separate wrappers around selected awaits. `enforce_live_lease` still checks immediately before CAS.

Generic plans supply no guard and retain R1 behavior. Publication timestamps remain those persisted in the commit. Stable-key lookup remains outside the ownership-bound phase, allowing a Lost session to recover an existing receipt. This closes the remaining source-scope finding from bd0b9ad without reopening the catalog, target binding or namespace design.

## Integration

Root merged the fixed R2 branch into the integration checkout. The two conflicts in `publish.rs` and `store.rs` were resolved to the exact reviewed R2 versions; they already contain the accepted R1 follow-ups. Core, object, Store, publication, Log and HAMT production files match the reviewed R2 source. Foundation transport files are unchanged.

Combined-workspace all-target tests, clippy and build pass. Eight JavaScript tests and four syntax checks pass. The merged immutable backend binaries pass bounded-get conformance and legacy lost-CAS recovery on S3 and MinIO. [Merge receipt](../implementation/reliable-log/verification/r2-merge-842ebf1.json). This acceptance permits Foundation adapter work. It does not establish successful JSONL storage transport, live backend replay or fresh Foundation reconstruction. Storage capabilities remain false until the adapter is implemented and verified. Destructive collection remains disabled.
