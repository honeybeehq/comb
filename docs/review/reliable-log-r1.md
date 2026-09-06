# R1 concurrency review — timed generic operations (WIP snapshot)

Read-only review of uncommitted source in `comb-reliable-log`, taken while R1 compiles. **Not an
approval.** Every finding is stated against the source as observed today; line numbers will move.
Scope limited as instructed: group companion rebasing/finalization, immutable-history proof, lost
CAS reply, original receipts after intervening writes. Stable HAMT deliberately excluded (root is
reviewing it separately); stable-path code is touched only where the generic path shares it.

Files read: `crates/combctl/src/publish.rs` (1121), `crates/combctl/src/log.rs` (1430),
`crates/comb-core/src/commit.rs` (171), `crates/comb-core/src/operation.rs` (558).
No edits, no tests run in the shared worktree, no children.

## Summary

The single-operation lost-reply path is **correct as written** (F0). Four defects break at-most-once
or produce fabricated receipts under concurrency; two weaken the immutable-history proof. F1 and F2
are the severe ones: each can acknowledge an append that never happened, or append the same producer
operation twice.

| | Finding | Effect | Correction size |
|---|---|---|---|
| F1 | `commit_group` derives companion ranges arithmetically from the leader's outcome | Fabricated success ack; events never written | ~15 lines |
| F2 | `sync_companions` overwrites an unresolved companion base | Same producer op appended twice | ~10 lines, needs F2b |
| F3 | `sync_companions` skips an `Applied` companion but leaves its payload in the batch | Duplicate events in a complete feed | 1 line |
| F4 | `Applied` cache path mixes cached generation with live head epoch/value | Caller-visible result is not the original | ~12 lines |
| F5 | `seek_generation` skip validation is an empty `if`; no resource check | Foreign commit accepted as recovery proof | ~12 lines |
| F6 | `outcome_from_view` third branch returns another identity's result | Latent wrong receipt | 3 lines |

---

## F0 — Lost CAS reply, single operation: correct

Recorded because it is the headline R1 requirement and it holds.

`attempt` maps only `PreconditionFailed`/`AlreadyExists` to `Retry` (`publish.rs:703-705`); every
other error, including a lost reply surfacing as `BackendUnavailable`, propagates out of `publish`
(`publish.rs:706`). The retry re-enters `publish`, loads the `Pending` intent at base *g*, sees
`head_gen == g+1`, seeks *g+1*, matches its own identity, and returns the original outcome with
`first_delivery: false` (`publish.rs:453-491`). Generation does not advance twice.

One caveat on the same path: the intent-create branch swallows `BackendUnavailable` only when the
message contains `"injected"` (`publish.rs:278-280`), so the guarantee is currently reachable in
tests via error *text*. The typed failpoint work should drive this identical path so the production
branch is the one under test.

---

## F1 — `commit_group` fabricates companion ranges when `publish` resolves instead of committing

**Severity: highest. A producer is told its append succeeded when its payload was never written.**

`commit_group` acks by walking `pending` and adding lengths (`log.rs:1394-1407`):

```rust
let mut seq = published.outcome.first;
for (item, acks) in pending {
    let last = seq + item.payloads.len() as u64 - 1;
    ...  Appended { first: seq, last, ... }
    seq = last + 1;
}
```

This assumes `published.outcome` is the leader range *of a commit this call just produced, containing
exactly these items in this order*. That holds only on the `attempt` path. `publish` has two other
exits that return an outcome from an **earlier** commit: the `Applied` cache (`publish.rs:209-229`)
and `PendingResolution::Done` (`publish.rs:484-491`). Both return `first_delivery: false`, which
`commit_group` reads but never branches on.

Executable interleaving:

1. Batch B1 = `[X(2 payloads), Y(3 payloads)]` at base *g*. Leader X, companion Y.
2. `attempt` uploads chunk+manifest, `sync_companions` writes Y's base, then the ref CAS **commits
   and its reply is lost**. `publish` returns `Err` (F0), so `commit_group` errors both acks
   (`log.rs:1409-1417`). The commit is live at *g+1* with `admitted = [X@1..2, Y@3..5]`.
   Neither intent was finalized — `finalize_intent` is only reached on the `Ok` arm
   (`publish.rs:679-693`).
3. Producers retry. Y's retry lands in a new batch B2 = `[Y, Z]`. `ensure_pending_intent(Y)` still
   sees `Pending`, so Y goes to `pending`, not `cached` (`log.rs:1347`).
4. Leader is now Y. `publish` → `resolve_pending` → head *g+1* > base *g* → seek *g+1* → Y found in
   `admitted` → returns `Done` with Y's **original** range `3..5`, `first_delivery: false`.
5. `commit_group` treats `3..5` as this batch's leader range and acks Z with `first = 6`,
   `last = 6 + len(Z) - 1`.

**Z's events were never written to any chunk. Z receives a successful `Appended`.** Z's intent stays
`Pending` forever, so a later Z retry seeks *g+1*, does not find Z, and re-appends at a different
range — the producer now holds two contradictory receipts for one operation.

The same bug fires with no lost reply at all: any batch whose leader resolves from the `Applied`
cache acks every companion against a stale range.

**Missing rule.** Companion acknowledgement must come from the committed admission record, never
from arithmetic. Minimal shape:

- add `admitted: Vec<Admission>` to `Published<T>`, filled from `prepared.admitted` on the attempt
  path and from `view.admitted` on the resolve path;
- in `commit_group`, look each pending identity up in `published.admitted` by
  `identity.canonical()` and ack that entry's `first`/`last`;
- any pending identity **absent** from `published.admitted` gets an error ack so the producer
  retries. Do not synthesise a range for it.

---

## F2 — `sync_companions` moves an unresolved companion base, admitting the same operation twice

**Severity: high. Breaks at-most-once for companions.** (Confirms root's observation with a trace.)

`sync_companions` (`publish.rs:710-743`) writes `intent.base_generation = base` for every non-Applied
companion without ever reading what that companion's base was, and without resolving it.

Executable interleaving:

1. Producer Y is a companion in W1's batch at base *g*. `ensure_pending_intent` creates
   `Pending{base: g}`.
2. W1 commits at *g+1* with Y in `admitted` at range **R**. Reply lost; Y's intent stays
   `Pending{base: g}`.
3. Y retries into a later batch at base *g+1*, leader Z. `ensure_pending_intent(Y, base=g+1)` finds
   the existing intent and returns the **stored** base *g* (`publish.rs:326-330`) — correct — and Y
   is pushed to `pending` (`log.rs:1347`).
4. Z's `resolve_pending` sees `head_gen == g+1 == Z's base` → `Attempt{base: g+1}`.
5. `prepare_events` allocates Y a **new** range **R'** (`log.rs:686-699`).
6. `sync_companions(base = g+1)` sees Y `Pending`, overwrites `g → g+1`, discards the unresolved
   base.
7. Ref CAS commits at *g+2*. Y is now in `admitted` at *g+1* with **R** and at *g+2* with **R'**.
   Y's payload is in the log twice.

Had Y been the *leader*, `resolve_pending` would have caught this at step 4. The companion path
skips resolution entirely — that is the gap.

**Missing rule (two parts; part (a) alone livelocks).**

(a) `sync_companions` may never write a base it did not verify: if
`intent.base_generation != base`, return `Ok(false)` (Retry) instead of overwriting.

(b) `ensure_pending_intent` must resolve a `Pending` intent whose stored base is behind live head —
the same seek-and-check `resolve_pending` performs — and return `IntentAdmission::Applied` when the
identity appears in the commit at `base + 1`. Without (b) the batch retries forever, because nothing
else ever advances a stale companion base.

---

## F3 — An `Applied` companion is skipped but its payload stays in the batch

**Severity: high. Duplicate events in a complete feed (§8.8).** (Confirms root's observation.)

`sync_companions` does `continue` on `IntentState::Applied` (`publish.rs:724-726`). But
`prepared` was built at `publish.rs:538`, well before this check, and already contains that
companion's frames inside the chunk and its `Admission` entry in the manifest. The `continue` skips
only the intent write; the ref CAS still publishes the payload.

Executable interleaving: `ensure_pending_intent(Y)` observes `Pending` at *t₀*; a concurrent writer
commits Y and finalizes its intent at *t₁*; this attempt reaches `sync_companions` at *t₂ > t₁*,
sees `Applied`, skips, and commits Y's events a second time. A later Y retry seeks its recorded base
and returns the *first* range, so the caller sees a consistent receipt while the log holds Y's
payload twice — a silent duplicate rather than a visible error.

**Missing rule.** `sync_companions` is a verification pass, not a repair pass: an `Applied` companion
must abort the attempt (`return Ok(false)`), never `continue`. Combined with F2(a), the whole
function reduces to one rule — *for every companion require `state == Pending && base_generation ==
base`, otherwise Retry; write only `proposed`.*

---

## F4 — `Applied` cache returns a stale generation beside a live epoch and ref value

**Severity: medium. The caller-visible result is not the exact original.** (Confirms root's
observation.)

`publish.rs:209-229` returns `generation` and `commit` from the cached intent, but reads the ref
*now* and returns `epoch: head.value.epoch` and `value: head.value` from that fresh read.

Trace: op X publishes at generation 5, epoch 3. A takeover moves the ref to generation 9, epoch 4.
X retries and receives `{generation: 5, commit: c5, epoch: 4, value: <ref at generation 9>}` — three
fields from two different points in history. `Appended.generation` (`log.rs:1361`) then reports 5
while `value.generation` reports 9.

The cache is also trusted without verification: the result is `serde_json::from_value` on stored
JSON (`publish.rs:214-217`) with no check that `commit` exists, that its header generation equals
the cached generation, or that its identity/request/resource match this call.

**Missing rule.** Reconstruct the caller-visible record from the cached *commit*, not from live head:
load `commit`, require `header.generation == generation`, `header.resource == plan.resource()`,
`header.request == request`, and identity present in `header.identity` or `admitted`; return
`epoch: header.epoch`. On any mismatch or unreadable commit, fall through to `resolve_pending`
rather than trusting the cache. A stale cache then costs one seek, which is the design's stated
tradeoff.

---

## F5 — `seek_generation` accepts a foreign commit as recovery proof

**Severity: high for the immutable-history proof.** (Confirms root's observation, with the
consequence made concrete.)

Three gaps compose:

1. The skip validation is an empty `if` (`publish.rs:925-928`):

   ```rust
   let claimed = skip_target_generation(current.header.generation);
   if skip_view.header.generation != claimed && skip_view.header.generation < target {
       // still usable if it lands at/after target
   }
   ```

   Any skip landing in `[target, current.generation]` is followed. The declared Fenwick target is
   computed and discarded.

2. The forward check is `>` (`publish.rs:920-924`), so a skip to the **same** generation is
   accepted and re-entered, looping until `MAX_SEEK_HOPS = 10_000` object reads before failing.

3. No view is ever checked against the resource being sought. `CommitHeader.resource` exists
   (`commit.rs:18`) and is never compared in `seek_generation`, `load_commit_view`, or
   `resolve_pending`.

Consequence: `resolve_pending` seeks target *t*, follows a skip to a commit belonging to a different
resource whose generation happens to equal *t*, `view.header.generation == target` passes
(`publish.rs:454-460`), `identity_in_commit` matches on the identity **string alone**
(`publish.rs:1100-1103`), and `outcome_from_view` returns that commit's range. **A producer is told
its append landed on a log where it never landed.** No adversary is required — `compute_skip`
derives skips by seeking from the parent (`publish.rs:888`), so one bad digest propagates.

Compounding: `load_commit_view` accepts *any* JSON object carrying a `header` field as a valid
commit (`publish.rs:979-999`), with no outer-schema check on the non-`comb.commit/v1` branch.

**Missing rule.** Three local checks, all inside `seek_generation`/`load_commit_view`:
require a known outer schema (`comb.commit/v1` or the manifest schema) rather than "has a header";
thread the expected resource through the seek and reject any view whose `header.resource` differs;
and make the discarded `claimed` comparison an error, with strictly decreasing generation required
on every hop.

---

## F6 — `outcome_from_view` can return another identity's result

**Severity: low today, latent.** The third branch (`publish.rs:1116-1119`) returns `view.result` for
*any* identity once the first two branches miss. Currently unreachable from `resolve_pending`,
because `identity_in_commit` gates the call and the surviving case has a null result — but the
function is a general helper and the branch is one refactor away from handing a caller a receipt
that belongs to a different operation.

**Missing rule.** Delete the third branch, or gate it on `view.header.identity == id`.

---

## Confirmed, no correction requested here

- `attempt` clears and rebuilds `intent.proposed` (`publish.rs:548`) and `sync_companions`
  overwrites the companion copy (`publish.rs:728`), so a losing twin's uploads lose their anchor.
  Collection is disabled, so this is not an active deletion path — but it should not be described as
  GC protection for twin uploads until a retention barrier exists. Matches root's note.
- `read_head` validates schema but not stored tenant/name against the addressed key
  (`publish.rs:154-176`); `load_intent` validates schema but not stored identity/resource against
  the requested pair (`publish.rs:393-413`). Both are root's boundary-identity items; F5's resource
  check is the same rule applied to commits, and the three should land together.

## Suggested test targets (not written; shared worktree untouched)

1. F1: batch `[X, Y]`, fail the ref CAS reply, retry Y in a batch with a fresh Z, assert Z is **not**
   acked success and that no chunk contains Z's payload.
2. F2: delayed group racing another group containing the same producer operation — assert the
   identity appears in `admitted` exactly once across the whole chain.
3. F3: finalize a companion's intent between `ensure_pending_intent` and `sync_companions`; assert
   the attempt retries and the payload appears once.
4. F4: publish at (generation 5, epoch 3), take over to (9, 4), retry the original op, assert every
   returned field is the original.
5. F5: hand-built chain with a same-generation skip, a Fenwick-mismatched skip, and a foreign-resource
   commit at the target generation — each must be `RecoveryFailed`, not an accepted proof.

## Root stable-index and boundary review

This is a review of unfinished code, not approval. The replacement worker must add focused failure tests and rerun verification after correcting these findings.

- HAMT lookup must validate root/node schemas, branch fanout, unique sorted slots, depth and leaf path placement before returning absence. It currently accepts a leaf with a different key as absent without proving that leaf belongs on the traversed path. Validate receipt range/generation and root metadata as well. Missing required objects must never mean a new stable key.
- StableKey hex parsing should enforce the encoded-size limit before allocating decoded bytes.
- Compare ref tenant/name and intent identity/resource to the addressed key at the read boundary. Schema validity alone does not establish identity.
- Restrict intent cleanup to expired operations. The current public unconditional delete can remove the evidence required to prevent a timely retry from being appended twice.
- Stable append must remain one key and one payload. The late batch API and read-page wrapper that first loads the whole history came from stale instructions and are excluded from the accepted contract.
- A returned page limit is not a memory bound when backend fetch, manifest decoding or chunk loading remains unbounded. R2 must enforce its object and decoded-size limits before enabling bridge capabilities.

Validation so far covers fixtures, the bridge acceptance client and the local Pheromone slice. R1 storage tests have not yet passed on the replacement worktree.

## Store runtime ownership follow-up

StoreRuntime currently lives in a process-global map keyed by backend Arc address and tenant. Two Store values sharing a backend and tenant overwrite each other's clock and policy. The registry also keeps entries after backend drop, so allocator address reuse can give an unrelated Store an old test clock or expiry policy. These settings govern lease and operation validity. Store must own its clock and policy fields. Update existing struct literals to Store::new rather than preserving them through a pointer-keyed registry. Test independent settings on two Store values that share a backend and tenant.
