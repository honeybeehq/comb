# R2 closure review at a53d49b

R2 remains unaccepted. All 21 preceding parent regressions pass. Three further tests fail: exact original-ref recovery, legacy target updates after lease expiry, and an outstanding append that publishes after the session becomes Lost.

The first two are regressions introduced by this change. The same tests pass on the previous `5b0d1d7` archive. The third is incomplete closure of the requirement that Lost sessions cannot issue new publications.

Root extracted fixed `a53d49b769020720195dc19b98d50a3a1f831f2c`, rebuilt in a private target, and copied the executable before testing. The final 24 cases ran individually with an external eight-second bound. [Receipt and hashes](../implementation/reliable-log/verification/r2-closure-a53d49b.json), [source](../implementation/reliable-log/verification/r2-closure-a53d49b.rs), [logs](../implementation/reliable-log/verification/r2-closure-a53d49b.txt).

## Closed at this checkpoint

The six extended cases from `5b0d1d7` now pass. The feed checks the current commit's persisted target, so missing targets after release and older valid targets fail closed. Renewal samples the clock after its read. Hung renewal loses ownership at the outer timeout, and a clock advance past lease expiry during upload prevents final publication.

The older 15 cases continue to pass. There is no reason to reopen catalog splits, phase 1 bounds, namespace isolation, or prior cancellation fixes.

## A1: final timestamp mutation breaks exact receipts

Both shared publication paths serialize `prepared.next` into immutable `ref_state`, upload that evidence, then overwrite the live ref's `updated_at` with a fresh `cas_now`. Recovery returns the persisted timestamp while first delivery returns the newer timestamp.

`review_real_clock_claim_retry_returns_exact_original_ref` fails on a normal claim followed by the same operation retry. All returned fields agree except `updated_at`. This test passes on `5b0d1d7` and fails on `a53d49b`.

The existing frozen-clock tests cannot expose this difference because both clock reads return the same value. Retain the real-clock test. Fresh time is needed to validate lease expiry, but it must not mutate fields whose immutable recovery evidence has already been built.

## A2: a retained expired lease is not a live-lease requirement

The new shared guard rejects any `next` containing an expired lease. Legacy `SetTargetPlan` retains lease metadata and permits an unfenced update after that lease expires. The guard changes this previously accepted behavior.

`review_v2_set_target_after_expired_lease_still_works` creates a two-second lease, advances the Store clock by three seconds, then sets a target without a fence. It succeeds on `5b0d1d7` and returns `Rejected("lease expired")` on `a53d49b`.

A live lease must be an explicit requirement of the publication operation. An arbitrary retained `next.lease` is insufficient. Preserve the semantics of legacy target, claim, and release plans while the v3 feed opts into its stronger ownership guard.

## A3: suspended append ignores the Lost transition

`review_lost_session_cannot_finish_suspended_append` uses this order:

1. Acquire a three-second lease and pause a new append during an object upload.
2. Inject a transient failure into renewal's ref read.
3. Wait until the session reports `Lost` from uncertain renewal.
4. Resume the upload before the lease timestamp expires.

The append returns success at range 1..1, generation 2. The session has declared ownership uncertain, but the publication path checks only the retained lease timestamp. No final CAS was issued before the Lost transition in this test.

Tie outstanding new publication to the session's loss or close signal and the confirmed lease deadline, as well as the caller's deadline. Check the operation's guard before final CAS. Keep committed-key lookup before ownership acquisition so a Lost session can still recover an existing receipt.

The first version of this test used an acquire budget smaller than its TTL and stopped at policy validation. Root corrected the budget to three seconds. Only the corrected runtime trace and final binary appear in the receipt.

## Typed errors and source-review disposition

S2 remains open. Expiry still uses `Rejected` message text, and the feed adds another `contains("expired")` lifecycle branch. Use a typed ownership or expiry signal.

The independent source review confirms A1 and A2. Root does not adopt its A3 claim that an outer timeout is inherently insufficient. The hung-renew test passes, and dropping the whole future does not bypass its per-iteration checks. The concrete outstanding issue is the Lost append trace above. An explicit publication guard can solve that without spreading `CallContext` through unrelated generic APIs.

Root also narrows the timestamp recommendation. Keep a single logical timestamp already included in the immutable commit. Read a fresh clock for validation only. Do not replace the timestamp after upload or patch recovery to conceal the mismatch.

Owner `3311b020` has the three tests and a current-only task at `/tmp/comb-r2-current-task.md`. Capabilities remain false. R1 and Pheromone acceptance is unchanged on the integration branch.

## Independent shared-code report, source inspection only

# R2 shared-publish review — a53d49b (diff 5b0d1d7)

Source only, pinned to `a53d49b769020720195dc19b98d50a3a1f831f2c` in `comb-reliable-log-r2`,
extracted with `git archive` to `/tmp/r2a53`. No builds, edits, children, push, no catalog redesign.
Scope: shared `publish.rs` changes only — R1 regression and lease/Lost deadline behavior. Feed
`bound_target` and the 21 parent cases are root's; I did not execute anything.

The whole shared diff is four hunks in `publish.rs`. Both of root's suspicions are confirmed.

## A1 — `cas_now` diverges from the persisted `ref_state`: confirmed R1 receipt regression

`ref_state: durable_ref_state(&prepared.next)` is serialized into the commit at `publish.rs:782`
(`attempt`) and `publish.rs:1031` (`commit_at_snapshot`). The commit object is then encoded and
uploaded, and `commit_digest` is resolved at `836` / `1063`. Only afterwards, at `857-864` /
`1076-1083`, does the engine sample `cas_now` and overwrite `next.updated_at`.

So the durable evidence records `updated_at = now` (the plan's pre-upload timestamp — `SetTargetPlan`
sets it explicitly), while the ref actually written by the CAS carries `updated_at = cas_now`.

**Consequence.** `published_from_view` rebuilds the caller-visible `RefValue` from `ref_state` and
patches `generation`, `epoch`, `head_commit` and `target`, but not `updated_at`. Therefore:

- first delivery returns `value.updated_at == cas_now` (from `attempt`'s own `next`);
- a lost-reply retry resolving through the chain returns `value.updated_at == now`.

The same operation now yields two different `RefValue`s depending only on whether the reply was
lost. That is the R1 F4 contract — "the caller-visible result must be the exact original" — broken in
exactly one field.

**Why it will show up now and not before.** Under `FrozenClock` `now == cas_now`, so the divergence
is invisible. Under a real clock the two differ whenever the object uploads take any measurable time,
which is essentially always. Root's plan to execute a real-clock claim retry is the right probe; a
frozen-clock test cannot fail here.

**Severity.** `updated_at` is not load-bearing: liveness reads `lease.lease_until`, fencing reads
`epoch`, ordering reads `generation`. So this is receipt fidelity, not state correctness — but it is
the fidelity property R1 F4 was closed on.

**Minimal correction.** Sample the CAS timestamp once, *before* the commit/manifest is built, and
thread it through `PrepareCtx.now` so the embedded `ref_state`, the `CommitHeader.at` and the CAS'd
ref all carry one value. Patching `published_from_view` to also restore `updated_at` would hide the
divergence rather than remove it, and would still leave the third skew below.

**Related skew worth one line.** `CommitHeader.at` is still built from `ctx.now`, so
`commit.at < ref.updated_at` now holds for every publication. Anything reconciling history against
live ref state sees a systematic offset. The single-timestamp fix closes this too.

## A2 — Universal expired-lease rejection changes closed R1 `set_target` semantics

The new guard fires in both publication paths for **any** plan whose `next` carries a lease,
irrespective of layout (`publish.rs:858-863`, `1077-1082`):

```rust
if let Some(lease) = next.lease.as_ref() {
    if lease.lease_until <= cas_now { return Err(Rejected("lease expired")) }
    next.updated_at = cas_now;
}
```

`SetTargetPlan::prepare` builds `next` as `current.clone()` and never clears the lease, so an expired
lease is carried straight into the guard. Three concrete behavior changes against closed R1:

1. **Unfenced `set_target` over an expired lease now fails.** R1 rejects with `LeaseHeld` only while
   `current.lease_live(now)`; an expired lease is explicitly permitted. That path now returns
   `Rejected("lease expired")`. This is the documented R1 semantics that the C4/lease drills closed.
2. **Fenced `set_target` over an expired lease now fails too, and this is the more surprising one.**
   With a matching fence R1 skips the liveness check entirely — presenting the epoch is the
   documented way to write under a lease. The new guard ignores the fence and rejects anyway.
3. **`claim` can self-reject.** `ClaimPlan` sets `lease_until = ctx.now + ttl_secs`, and the guard
   compares against the strictly later `cas_now`. If uploads take longer than `ttl_secs`, the freshly
   minted lease is already expired at CAS time and the claim fails with "lease expired" on the lease
   it is creating. Short TTL plus a slow backend is enough; no fault injection required.

**Assessment.** The intent — never publish under an expired lease — is correct for v3 feed appends,
and it is what closes root's "pause upload + clock past TTL → new append publishes gen 2". Applying
it in the shared engine to every plan is what breaks R1. The narrow correction is to make it a
property the plan declares (a `PreparedMutation` flag, defaulting to off) so the feed's append plan
opts in while `SetTargetPlan` and `ClaimPlan` opt out; gating on `KeyLayout::V3` would also work but
is coarser. For `ClaimPlan` specifically the right behavior is not rejection but recomputation —
derive `lease_until` from `cas_now`, exactly as `renew_owned_lease` now does.

This keeps "legacy R1 behavior isolated", which every prior handoff has required.

## A3 — Lease/Lost deadline: the stale-clock half is closed, the unbounded-await half is not

Closed: `renew_owned_lease` now samples the clock **after** the awaited `read_head`
(`publish.rs:1363-1370`), takes a second `cas_now` immediately before building `next` with a second
`lease_live(cas_now)` check (`1393-1396`), and derives both `lease_until` and `updated_at` from
`cas_now` (`1399-1402`). That removes the renew-an-already-expired-lease window and the
shorter-than-believed deadline. Legacy `renew_lease` got the same `cas_now` treatment (`1442-1448`)
without gaining a liveness check, so R1 semantics stay isolated — correct.

Not closed: the signature is still `renew_owned_lease(name, writer, epoch, ttl_secs)` — no deadline,
no `CallContext` — and it still loops up to 16 times over unbounded `read_head` + `put_update`. The
"hung renew stays Active beyond TTL" case therefore cannot be fixed in shared code as it stands, and
wrapping the call in a timeout at the feed level discards the per-iteration liveness recheck that
makes the fix above meaningful. If root's feed-side bound is a `select` around the whole future, the
session can still observe a renewal that was checked against a stale snapshot at the moment it was
cancelled. The deadline needs to reach the loop.

## A4 — S2 partially addressed

`renew_owned_lease` now returns typed `LeaseHeld { holder, until }` for a changed owner
(`publish.rs:1384-1389`) instead of `Rejected("lease owner changed")` — that half is fixed and the
session can branch on it. Expiry is still `Rejected("lease expired")` in three places
(`858`, `1077`, `1397`), sharing the variant with `"catalog height exceeded"`,
`"stable key path hash collision"` and the release-owner refusals. Ownership loss by expiry still
requires string matching to distinguish. Acknowledged as known; recording the exact sites so the
remaining half is a mechanical change.

## Summary

| | Finding | Class |
|---|---|---|
| A1 | `ref_state.updated_at` (pre-upload) vs CAS'd `updated_at` (`cas_now`) — retry returns a different `RefValue` than first delivery | R1 regression, receipt fidelity |
| A2 | Universal expired-lease guard breaks unfenced *and* fenced `set_target` over an expired lease, and lets `claim` self-reject | R1 regression, semantics |
| A3 | Renew stale-clock closed; unbounded await not closable at the call site | Incomplete |
| A4 | Owner-change typed; expiry still `Rejected(String)` | Known, sites listed |

A1 and A2 are both fixed by narrow changes in the same two hunks: one timestamp sampled before the
commit is built, and the lease guard made opt-in per plan. Neither requires touching the feed, the
catalog, or R1 call sites.

Limitations: source inspection only; no execution; feed `bound_target`, session state and the 21
parent cases are root's.
