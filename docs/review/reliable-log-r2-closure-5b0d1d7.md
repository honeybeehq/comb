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

## Independent shared-code review disposition

The independent source reviewer found no additional blocker beyond the six executed failures. The review below confirms the legacy v2, carrier-list, transient-error, and catalog-write-validation corrections. Root accepts S1 as a mapper consistency fix and S2 as part of the lease correction. Ownership loss must use a typed signal rather than `Rejected` message matching. S3 and S4 are later hardening and cleanup, not additional first-gate blockers. Any future cap on generic `attempt` must apply only to v3.

Two qualifications keep the source report precise. The current feed error conversion already maps a catalog `NotFound` to `ReadError::Integrity`, so S1 is not a demonstrated empty-read success. Also, renewal copies the returned `lease_until` into session state. The demonstrated clock defect is the expired CAS and stale Active state, not a separate discrepancy between returned and stored deadline values.

### Independent report, source inspection only

# R2 final shared-code review — 5b0d1d7 (seal)

Source review only, pinned to `5b0d1d71f3754cb63b8f8873907638009e7117ec` in `comb-reliable-log-r2`
(extracted with `git archive` to `/tmp/r2fin`; diff base `8171fea`). No edits, builds, children,
push. Scope: shared publication/store/catalog. Feed snapshots/session and all execution are root's;
I did not re-execute anything and do not duplicate root's six failing cases or the closed original
catalog design.

## Closed since 8171fea (verified in source)

| Item | Evidence |
|---|---|
| N1 legacy v2 read cap | Layout split: `load_commit_view` uses unbounded `get_blob` under `KeyLayout::V2`, bounded `get_blob_limited` under V3 (`publish.rs:1199-1213`). Same split in `read_head` (`184-192`), `load_intent` (`560-570`), `reject_existing_log_manifest` (`store.rs:474-497`). Test `v2_manifest_above_v3_cap_stays_readable` covers both directions. |
| N2 carrier drift | `LOG_MANIFEST_CARRIERS` is one `pub(crate)` const (`publish.rs:29`), consumed by `load_commit_view` (`1234`), `encoded_object_cap` (`1632`) and `reject_existing_log_manifest` (`store.rs:481, 505`). |
| N3 transient → `Rejected` | `reject_existing_log_manifest` now routes both branches through `map_commit_read_error` (`store.rs:477, 494`), with test `set_target_transient_manifest_read_is_not_rejected`. |
| N5 validate-on-put | `put_node` validates leaf/branch before writing (`catalog.rs:148-155`), test `put_node_rejects_structurally_invalid_leaf`. |
| Catalog transient mapping (task 5) | `map_catalog_read_error` preserves `BackendUnavailable`/`Io` (`catalog.rs:148-159`). Partial — see S1. |
| Owner-aware release (task 3) | `release_owned` + `ReleasePlan.writer` checks `lease.writer` and refuses a missing lease (`store.rs:377-397, 683-695`); legacy `release` passes `writer: None`, so R1 behavior is isolated. |
| Cached-decode quarantine | `get_blob` now quarantines on every decode failure (`store.rs:167`). `get_blob_limited` correctly keeps the one terminal exception, guarded by `cache_plaintext_cap_is_authoritative` so it returns only when the cached bytes are provably the requested object (`store.rs:~194-201`). This is the right distinction, not a gap. |

## Shared-code root causes of root's six failures

Stated so the fixes land in shared code rather than at a feed entry point. Not new findings —
root has the runtime evidence — but the defects are in my slice.

**Lease-await family (3 failures).** `renew_owned_lease` samples the clock *before* the awaited read
(`publish.rs:1345-1350`):

```rust
let now = self.clock().now();
let snapshot = self.read_head(name).await?;      // may block arbitrarily
...
if !snapshot.value.lease_live(now) { return Err(Rejected("lease expired")) }
next.lease = Some(Lease { lease_until: now + ttl_secs, .. });
```

Two consequences, both matching observed failures: `lease_live(now)` is evaluated against a
timestamp that predates the read, so a lease that expired *during* the read is renewed anyway
(root's "stale clock around ref read → expired renew CAS"); and `lease_until` is computed from the
same stale `now`, so the durable deadline is shorter than the session believes, which is the wrong
direction for any slack calculation. Legacy `renew_lease` has the identical shape
(`publish.rs:~1400`); sampling after the read is a strict improvement in both, so fixing the shared
shape does not change v2 semantics.

Separately, `renew_owned_lease(name, writer, epoch, ttl_secs)` takes no deadline or `CallContext`
and loops up to 16 times over unbounded `read_head` + `put_update`. It cannot be bounded by its
caller except by wrapping the whole future, which discards the per-iteration recheck — root's "hung
renew stays Active beyond TTL". The parameter has to reach the function.

Third: `commit_at_snapshot` validates the lease inside `plan.prepare` and then performs uploads and
the ref CAS with no revalidation and no deadline. If nobody took over, an expired lease still
publishes — root's "pause object upload + clock past TTL → new append publishes gen 2". The ref CAS
guards against a *competing* writer, not against one's own expiry. A confirmed-lease deadline has to
be carried into the publication path and rechecked immediately before the CAS.

**Target-binding family (3 failures).** `read_head` enforces `generation > 0 ⇒ head_commit.is_some()`
(`publish.rs:200-206`) but nothing pairs `RefValue.target` with the commit evidence, so a ref with a
valid `head_commit`, a live lease and `target: None` passes cleanly. The evidence to detect that
exists and is simply not consulted: for a manifest carrier `target` must equal `head_commit`
(`target_follows_commit`, `publish.rs:1269`), and for a `comb.commit/v1` carrier the commit's own
`ref_state.target` records what the target was. Root's note that a generic release commit sets
`target_follows_commit: false` and lets `published_or_empty` accept `None` is the same gap seen from
the read side. Deriving the expected target from the head commit in shared code satisfies "not merely
another optional check at one entry point".

## Additional shared-code findings (not among root's six)

### S1 — `map_catalog_read_error` passes `NotFound` through; its two siblings map it to `IntegrityError`

`catalog.rs:148-159` ends with `Ok(other) => other.into()`, so a confirmed-missing catalog node
surfaces as `CoreError::NotFound`. Both sibling mappers do the opposite:
`map_commit_read_error` → `IntegrityError("commit … is missing")` (`publish.rs:1651-1653`) and
`map_node_read_error` → `IntegrityError("stable index node … is missing")` (`hamt.rs:530-532`).

A catalog node is referenced by a published root, so its absence is retained-state corruption, never
benign absence. This is the same class as root's target-binding family — missing required evidence
presenting as something a caller may treat as "nothing there" — in the one mapper that was changed
this round without picking up the `NotFound` arm. One match arm.

### S2 — `Rejected` is overloaded across ownership loss and unrelated policy failures

`renew_owned_lease` returns `Rejected("lease owner changed")` and `Rejected("lease expired")`
(`publish.rs:1362-1372`); `ReleasePlan` returns `Rejected("release by non-owner")` and
`Rejected("release requires the held lease")` (`store.rs:685-694`). The same variant carries
`"catalog height exceeded"`, `"stable key path hash collision"` and other unrelated policy failures.

A session that must transition to `Lost` on ownership loss cannot distinguish these without matching
on error text — the exact pattern flagged and removed in the R1 review. Give ownership loss its own
typed signal (a dedicated variant, or reuse `Fenced` where the epoch is known) before the session
logic depends on it.

### S3 — `attempt` still has no encoded-object cap; the fix must be layout-conditional

`encoded_object_cap` is applied only in `commit_at_snapshot` (`publish.rs:988, 1033`). The other
publication path, `attempt`, encodes and `put_create`s with no check. Under V3 the reachable uploads
on that path are small synthesized commits, so there is **no currently reachable trigger** — this is
a latent write/read cap mismatch, not a blocker. Recording it because the obvious fix is wrong:
`attempt` is shared, so adding the cap unconditionally would newly reject large v2 manifests at write
time and contradict "do not silently reject old committed data". Gate it on `KeyLayout::V3`.

### S4 — Dead v3 size check in `load_commit_view` (cosmetic)

`publish.rs:1240-1250` re-checks `payload.len() > MAX_MANIFEST_OBJECT_BYTES` for a v3 manifest under
V3 layout, but `get_blob_limited` already enforced `max_plaintext_bytes` at the same value, so the
branch is unreachable. `v2_manifest_above_v3_cap_stays_readable` passes through the read cap, not
this check. Harmless; remove it or it will read as defense in depth that is not there.

## Seal

No additional blocker beyond root's six. S1 is the one finding I would fix before sealing — it is a
one-arm change in the same failure class root is already correcting, and leaving it means the catalog
mapper alone can report missing retained state as absence. S2 should land before the session's `Lost`
transition is written against it. S3 and S4 are latent/cosmetic and can ride a later commit.

Limitations of this report: source inspection only, no execution; feed snapshots, session state and
read semantics were out of scope and are root's; the original catalog design and the absent-v3 legacy
guard were not reopened.
