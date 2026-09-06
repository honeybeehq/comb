# R1 final review — closure verification against 52c1963

Read-only. No edits, builds, tests, or children. Closure is verified by **source inspection at the
fixed SHA**, not by execution.

- Repo `comb-reliable-log-r1`, branch `feat/reliable-log-r1`, HEAD
  `52c1963aa8ab52deceaa6b5cae04b26c9b24970f`.
- Dirty and excluded as instructed: `crates/combctl/src/bin/comb-bridge.rs`,
  `crates/combctl/tests/bridge_protocol.rs`, `crates/combctl/tests/bridge_transport.rs`.
- Old `feat/reliable-log` / `29d271e` not consulted.
- Not reopened: R2 bounds/leases, stable physical batching.

## Verdict

**All ten items closed.** F1–F6 and S1–S3 plus the depth-52 boundary are fixed at this SHA, several
with a stronger correction than I proposed. **No new correctness blocker.** One bounded-liveness
regression introduced by the F2/F3 fix is recorded below as N1 — it is a cost, not a defect, and it
is in scope because it is a direct consequence of the fix rather than deferred work.

## Generic path

| | Status | Evidence |
|---|---|---|
| **F1** companion ranges fabricated from leader outcome | **Closed** | `Published` carries `admitted: Vec<Admission>` (`publish.rs:33-41`). `commit_group` now looks each pending identity up in `published.admitted` by `canonical()` and acks that entry's `first`/`last`; an identity absent from the list is error-acked with "grouped identity {id} was not admitted in this publication" (`log.rs:1443-1466`). No arithmetic walk remains. The interlock holds on every exit: `admitted: prepared.admitted` on the attempt path (`publish.rs:848`) and `commit_at_snapshot` (`1022`), and `published_from_view` sets `admitted: view.admitted.clone()` (`1507`) so a leader resolving from a prior commit returns *that* commit's admissions. Test: `retry_drills.rs:421 grouped_companion_not_acked_from_stale_leader_range`. |
| **F2** `sync_companions` moved an unresolved base | **Closed, both parts** | (a) `if intent.base_generation != base { return Ok(false); }` (`publish.rs:873-875`) — the base is never written unverified. (b) the livelock-preventing half is present: `ensure_pending_intent` on `Pending` with `head > base` seeks `base+1` (`publish.rs:415-427`), returns `IntentAdmission::Applied` when `identity_in_commit` (`428-443`), and only otherwise advances the base by CAS (`444-457`). Test: `retry_drills.rs:552 racing_groups_admit_shared_producer_once`. |
| **F3** `Applied` companion skipped, payload retained | **Closed** | `if matches!(intent.state, IntentState::Applied { .. }) { return Ok(false); }` (`publish.rs:870-872`) — aborts the attempt instead of `continue`. `sync_companions` is now a pure verification pass; it writes only `proposed` (`876`). No dedicated named test found (see N2). |
| **F4** cached generation mixed with live epoch/value | **Closed, stronger than proposed** | `try_cached_applied` (`publish.rs:1315-1356`) seeks the cached commit and verifies digest identity (`1335`), generation and resource (`1338`), request against header or admissions (`1341-1348`), and `identity_in_commit` (`1349`); any mismatch returns `None` and the caller falls through to `recover_without_intent` / re-resolution (`357-368`). `published_from_view` rebuilds the record entirely from the commit — `generation`/`epoch` from `view.header`, `value` from a newly persisted `ref_state` with tenant/name cross-check (`1477-1499`). No live-head field survives. Tests: `retry_drills.rs:397`, `retry_drills.rs:661 forged_applied_cache_is_not_publication`. |
| **F5** foreign commit accepted as recovery proof | **Closed, all three gaps** | `seek_generation` takes `resource` and calls `check_view_resource` on start (`publish.rs:1058`), skip (`1083`), and parent (`1118`). Strict decrease enforced: `skip_view.header.generation >= current.header.generation` → error (`1084-1089`), replacing the old `>`. The empty `if` is now an error: `skip_view.header.generation != claimed` → `RecoveryFailed` (`1090-1097`). `load_commit_view` requires an explicit `schema` and accepts only `COMMIT_SCHEMA` or `LOG_MANIFEST_SCHEMA`, rejecting anything else (`1138-1166`) — "has a header field" is gone. Test: `publish.rs:1532 seek_rejects_bad_skips_and_foreign_resource`. |
| **F6** `outcome_from_view` foreign-result fallback | **Closed** | The third branch is deleted; only header-identity match or admissions match, else `RecoveryFailed("commit does not record {id}")` (`publish.rs:1443-1455`). Test: `publish.rs:1601 outcome_from_view_does_not_return_foreign_result`. |

Also observed closed from the earlier boundary-identity notes: `load_intent` now rejects a stored
identity that differs from the requested one (`publish.rs:537-544`).

## Stable path

| | Status | Evidence |
|---|---|---|
| **S1** transient read reported as corruption | **Closed, stronger than proposed** | `map_node_read_error` (`hamt.rs:512-530`) maps `CoreError::Io` → `BackendUnavailable` (`520-522`), preserves `BackendUnavailable` (`517-519`), keeps `NotFound` → Integrity, and the catch-all now fails loudly: `Ok(other)` → `RecoveryFailed("unclassified stable index node error …")` (`524-527`) instead of silently calling an unknown variant corruption. Test: `hamt.rs:954 io_on_node_read_is_unavailable`. |
| **S2** branch nodes had no position binding | **Closed with full prefix binding** | Branch carries `depth: u8` and `prefix: Vec<u8>`. `load_node` verifies `node_depth == depth` (`hamt.rs:446-451`), `depth <= MAX_DEPTH` (`452-457`), `prefix.len() == depth` (`458-464`), and every prefix nibble against the walked path (`465-473`). This catches horizontal misplacement as well as vertical — the complete fix, not the narrow depth-only one I offered. |
| **S3** insert path skipped leaf-range validation | **Closed** | `insert`/`insert_at` take `head: IndexHead` and pass `Some(head)` to `load_node` (`hamt.rs:192, 224`), so every leaf touched during a path-copy is validated. `insert` additionally validates the new entry's own range up front (`182-188`) and checks root shape before descending (`193`). |
| **depth-52 boundary** | **Closed as dispositioned** | `MAX_LEAF_DEPTH = 52` is now separate from `MAX_DEPTH = 51` (`hamt.rs:22-23`). `lookup` bounds the walk at `MAX_LEAF_DEPTH` (`119-124`) and rejects a *branch* at `depth > MAX_DEPTH` (`155-160`); `insert_at` bounds at `MAX_LEAF_DEPTH` (`220-222`). A leaf produced by `split` at depth 51 is therefore readable, and a branch past nibble 51 is refused — the write/read asymmetry is gone. Constructed-path test present: `hamt.rs:1052 fully_prefixed_leaf_at_depth_52_is_accepted`. |

Also closed from the earlier metadata gap: `StableIndexRoot.entries` is no longer decorative.
`lookup` and `insert` call `check_root_shape` at depth 0 (`hamt.rs:127, 193`), which rejects
`entries == 0` unless the root is genuinely an empty depth-0 branch, and rejects `entries > 0` with
no children (`562-587`). The `entries == 0 → Absent` short-circuit (`128-130`) is safe because
`check_root_shape` runs first — a forged zero count cannot manufacture a false negative.

## N1 — Bounded-liveness regression from the F2/F3 fix (not a blocker)

`sync_companions` returning `Ok(false)` maps to `AttemptResult::Retry` (`publish.rs:803-806`), which
`continue`s the `publish` loop (`254-261`, `271-278`). But `GroupPlan.items` is fixed for the
duration of that call, so a companion that is `Applied` or base-mismatched rejects **every**
iteration up to `MAX_PUBLISH_ATTEMPTS = 128`, each of which first runs `plan.prepare` — building a
chunk and manifest, and on the stable path uploading up to 52 HAMT nodes — before
`sync_companions` aborts. The call then returns `"publication did not converge"` and `commit_group`
error-acks the whole batch, including the leader, which never committed.

Reachable whenever a companion's intent is finalized concurrently between `ensure_pending_intent`
and `attempt` — the same race that previously produced the F3 duplicate. The fix converted a silent
duplicate into 128 wasted attempt cycles plus a batch-wide error. Correct, terminating, and
self-healing (the next batch's `ensure_pending_intent` resolves the companion via the F2(b) path),
so no operation is lost or duplicated.

Minimal correction if it is worth taking: have `sync_companions` return the offending identity
rather than a bare `bool`, so `commit_group` can drop that companion and re-form the batch in one
pass; or bound companion-abort retries separately from `MAX_PUBLISH_ATTEMPTS`.

## N2 — Test-coverage observation

F1, F2, F4, F5, F6, S1 and the depth-52 boundary each have a directly named test (locations above).
I found no test named for the **F3** race specifically — a companion transitioning to `Applied`
between `ensure_pending_intent` and `sync_companions`. `retry_drills.rs:552
racing_groups_admit_shared_producer_once` may cover it incidentally; worth confirming it drives the
`Applied`-companion abort arm at `publish.rs:870-872` rather than only the base-mismatch arm at
`873-875`, since those are the two branches whose confusion caused the original duplicate.

## Out of scope, untouched

R2 bounded manifests/reads/leases and stable physical batching were not re-examined. GC remains
disabled and unresolved; nothing here changes that.


## Root disposition

Review is source inspection of fixed commit52c1963, not execution. Root separately ran33 bridge tests against the API transition:22protocol,7transport,3real-process,1binary, all passed. The reserved Store constructor and exact head-overflow fixture changes are committed052ca11. The R1 owner started a separate retained-target recovery followup during the build; do not attribute that combined worktree run to a pristine52c1963 binary.

N1 remains a recorded bounded cost, not a reason to add physical stable grouping to the first gate. Generic GroupPlan does not admit stable submissions in this slice, so the generic companion retry cost does not include stable HAMT path copying. N2 remains a focused test opportunity.

The opt-in backend_drills test at52c1963 made clean duplicate calls despite its lost-reply name. Root requested a separate followup using the existing typed final-ref-CAS failpoint for Core C4, Log L2 and stable append on each selected backend. Live backend recovery is not yet verified by that test.

R2 worker3311b020-3aa4-45b6-bae4-9acc0fbcedf1 starts bounded backend/envelope work in comb-reliable-log-r2 from052ca11. It must receive the R1 followup before editing log/publication code. Foundation storage capabilities remain false pending bounded reads, renewable sessions and actual process acceptance.
