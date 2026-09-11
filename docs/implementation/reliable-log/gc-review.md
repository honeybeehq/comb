# Review — collection protocol (`gc-design.md`), bounded

Read-only design review. Scope: `docs/implementation/reliable-log/gc-design.md` only, against the
current `ObjectBackend`, `sweep.rs`, spec v0.3 §7.7/§7.8/§7.9/§19, and the measured probe
`conditional-delete-backends.json`. No repo edits, no child agents. The complete-feed Foundation
gate does not depend on anything here.

## Verdict

**Implementable as a first safe collection — conditionally. Not a reject.** The freeze-based
consistent cut is the right mechanism and is a genuine advance over recheck-plus-grace, which the
document correctly refutes. But as written the protocol is **unsound**: trace T1 below produces a
committed root pointing at a deleted object, and the incarnation guard does not cover it. T1's rule
is a required correction, not a hardening. Seven further rules are missing. With them, destructive
collection is shippable behind a capability gate; without T1 it must not ship.

Recommended first destructive slice is narrower than the document proposes — see §5.

---

## 1. Backend capability: worse than the document assumes, on three of four backends

The document says S3 ETag "can repeat when the same bytes are recreated" and defers selection.
The measured and code-verified position is sharper.

**S3 — passes.** `conditional-delete-backends.json` records all five checks green, including
`incarnation_changes_token`, `old_incarnation_rejected`, `new_incarnation_survives`. This is the
capability the protocol needs, confirmed with fresh nonce-bearing verification keys.

**MinIO — fails in the most dangerous way available.** `wrong_token_rejected: false`, and the
recorded error is a subsequent `GetObject` returning `NoSuchKey`. Current MinIO **accepted a
deliberately wrong `If-Match` and deleted the object.** This is not "capability absent" — it is a
silently ignored precondition. A collector cannot distinguish it from a working guard by observing
success replies, because the delete returns success in both cases. Every safety argument in step 5
evaluates to false on this backend while appearing to hold.

**Memory and local — provably reuse incarnation tokens, today, deterministically.**

- `LocalBackend::version_of` is `blake3(bytes)` (`crates/comb-object/src/local.rs:29-31`). The
  version token *is* content identity. Recreating identical bytes always yields an identical token.
  This directly contradicts the trait's own contract comment, "Never a content identity"
  (`crates/comb-object/src/backend.rs:5`).
- `MemoryBackend` versions are per-key counters starting at 1 (`memory.rs:22-28`), and `delete`
  removes the map entry entirely (`memory.rs:66-69`). Delete-then-recreate returns `Version("1")`
  again — the same token the deleted incarnation held.

**Consequence.** The document's requirement that non-reuse "must be proven on memory, local, MinIO
and S3" cannot be met by adding an `If-Match` parameter alone. An envelope incarnation nonce fixes
local (bytes change → BLAKE3 changes) and S3 (bytes change → ETag changes) but **does not fix
memory**, whose token is positional, not content-derived. Memory needs a per-key generation counter
that survives delete; local needs the nonce (or a sidecar). Both are small, contained changes in
`comb-object`, but they are prerequisites, not follow-ups — and until they land, adversarial check
6 will report a false pass on memory, which is the primary deterministic-test backend.

**Rule C1.** Conditional deletion is a probed runtime capability, never an inferred one, and the
probe must be a **negative** test: a deliberately wrong token must be *rejected* and the object must
survive. A positive-only probe passes on a backend that ignores the header. This is §7.9's "an
S3-compatible API string alone is insufficient evidence" applied to deletion. Destructive collection
is disabled unless the negative probe passes for that backend instance at that endpoint.
Per the measured evidence: **enabled on S3, disabled on current MinIO**, disabled on memory and
local until their token semantics are fixed.

---

## 2. Traces that break the protocol as written

Each is a concrete interleaving, followed by the single missing state or ordering rule.

### T1 — A stalled plan from an earlier round deletes an object resurrected by reference

This is the sharpest hole. The incarnation guard does not cover it.

1. Round *N*. Object `O` is an old orphan: unreachable from every captured root, past grace.
   Enumeration records `O` at incarnation `i₀`. Helper H1 stalls before issuing its delete.
2. Round *N* completes by another helper, unfreezes, reopens.
3. A publisher performs a by-reference publication — `set_target(digest_of(O))`, or any commit
   naming a digest it did not upload. Per step 6 it "revalidates" `O`: it exists, at `i₀`.
   Nobody deleted it, so nobody recreated it, so **the incarnation is unchanged**. The publisher
   CASes. `O` is now live under a committed root.
4. H1 wakes and issues its recorded conditional delete of `O` at `i₀`. The token matches. **The
   delete succeeds.** A committed, visible root now points at a missing object — §22.7's exact
   prohibition and the failure this whole document exists to prevent.

Step 6's "revalidates/reuploads every object it plans to reference" cannot save this. As candidate A
already established, a `set_target` caller holds only a digest and cannot reconstruct the bytes; it
can only verify existence, and verification is defeated by a delete issued in a *prior* round.
Step 7's "an old helper must never compute a new plan" does not apply — H1 is replaying its own
round-*N* plan, exactly as permitted.

**Rule T1.** A by-reference publication must **touch** every object it will reference that it did
not create in this attempt: `put_update(object_key, expected = observed_token, bytes_with_fresh_
incarnation_nonce)`, and may CAS the ref only after every touch succeeds. A failed touch is
fail-closed `NotFound` before any ref movement.

This needs no new backend capability — `put_update` already exists — and it is the atomic guard the
document is missing. The touch and any outstanding conditional delete contend on the **same token**,
so they serialize at the backend: if the touch wins, every recorded delete for that object is now
stale and must fail; if the delete wins, the touch fails and the publisher fails closed before
publishing. It also subsumes the "revalidate" hand-wave with something enforceable.

### T2 — The registered-but-absent placeholder install has no losing branch

Step 2 and adversarial check 2 assert the collector "installs a frozen placeholder and the stale
first publication fails." Verified backend semantics defeat the stated ordering: `put_update` with
`expected = None` **creates** when the key is absent (`memory.rs:34`, `local.rs:97`), and a first
publication uses exactly that path (`store.rs:127-129, 145`).

1. Collector reads registered ref `R`: absent.
2. Publisher creates `R` for real with `put_update(expected = None)`. Wins.
3. Collector's placeholder create returns `AlreadyExists`.

The document does not say what happens next. A collector that reads `AlreadyExists` as "already
frozen by a peer helper" advances the round to `Deleting` while `R` is **live and unfrozen**. `R`'s
graph was never captured, so its objects are enumerated as garbage and deleted.

**Rule T2.** Placeholder installation is a loop, not a single attempt: on `AlreadyExists`, re-read
and freeze the observed value. The round may not enter `Deleting` until **every** catalog member is
observed `Frozen(round)` at a token the collector recorded — a count of successful writes is not
sufficient evidence.

### T3 — A frozen ref returns `PreconditionFailed`, which the accepted R1 resolve loop cannot distinguish

Freeze increments a non-logical storage revision while preserving the logical value. A publisher
holding the pre-freeze token sees `PreconditionFailed`. In the accepted candidate-A `mutate`
protocol, that routes to *resolve*, which reads head, observes `generation == base` (freeze
preserves the logical value), concludes **undecided**, and re-attempts at the same base — failing
again for the entire round. That is a hot spin against the backend for the round's duration, and it
consumes the operation's 7-day expiry window. A round that outlives the remaining window turns an
operation that never executed into `UnknownOperation`.

**Rule T3.** A frozen ref returns a distinct, non-terminal `Collecting { round }` error, and
`mutate` treats it as retry-after-round with backoff, never as a lost CAS race. Freeze must be
observable to the publisher as a *state*, not as a token mismatch.

The document should also state the corollary it currently leaves implicit: since renewal refuses
frozen refs, any round longer than the lease TTL forces leadership takeover on every partition in
the tenant. Fencing handles that correctly, but round duration is now coupled to lease TTL and that
belongs in the protocol text.

### T4 — A registered-but-absent *pin* must abort the round; a placeholder silently removes protection

The placeholder trick is safe for refs, where empty means "nothing to protect". It is unsafe for
pins, where empty means "protection silently withdrawn". Step 2 applies it to "every registered ref
or pin" without distinction.

1. Reader registers pin `P` in the catalog, then crashes before writing the pin object.
   (Or: writes it, and the object is unreadable.)
2. Round captures `P`, finds the object absent, installs a frozen empty placeholder.
3. Traversal marks nothing from `P`. The snapshot the reader was protecting is collected.
4. Reader resumes, reads `P`, finds it empty — and per §7.7 has no way to distinguish
   "my pin was neutralised" from "my pin expired".

**Rule T4.** Pin object durable **before** catalog registration; a catalog-registered pin whose
object is absent or unreadable **aborts the round** under step 3's missing-root rule. Placeholders
are valid for refs only. The reverse order is what makes this safe: a reader that crashes between
writing the object and registering it has no claim, and its object is legitimately collectible.

### T5 — "The same immutable deletion plan" has no *the*

Step 7 permits duplicate helpers to "replay only the same immutable deletion plan", but step 4 lets
any helper "record a bounded, durable deletion plan". Two live helpers in the same round compute two
different plans from stale listings and each persists one. Both plans are individually safe under
freeze, so this is not a deletion bug — but the step 7 invariant is then unenforceable, because
nothing identifies which plan is canonical, and a helper arriving in a later round has no way to
tell whether it is replaying or computing.

**Rule T5.** The `Deleting` transition carries exactly one plan digest inside the CAS'd control
record. Helpers adopt that digest by direct read and never compute their own; a helper that finds a
plan digest already bound and holds a different one aborts itself.

Related and worth one line in the document: the control record's `state` and the captured catalog
root must be **one value under one token**, or the step 1 capture is not atomic with the transition.
The text implies this ("named by one CAS-guarded control record") without stating it.

### T6 — Catalog completeness is aspirational, exactly like the capability marker was

The whole safety argument depends on "a resource is registered before its first publication."
Nothing enforces it. Any publisher that CASes a ref without registering is invisible to the
collector, and its entire object graph is enumerated as garbage. `sweep.rs:41-51` today derives roots
from `list`, so every existing ref is a root by accident; the catalog design removes that accident
and replaces it with an unenforced convention. This is the same shape as the tenant capability
marker in the prior review: a fence that the writing path never checks is not a fence.

**Rule T6.** The crate-private `RefCommitter` (graft G3 of the accepted verdict) refuses to CAS any
ref absent from the catalog, verified by a **direct read of the catalog node**, never a list. The
pre-catalog key space is sealed the same way v1 refs are — overwrite with a body the old reader
rejects — because a pre-catalog binary registers nothing and deleting the ref key is not a seal
(`put_update(expected = None)` recreates, `memory.rs:34`).

### T7 — The root-class enumeration is narrower than §19.2

The catalog holds "every resource and pin key". Spec §19.2 lists a wider root set: staging refs,
retained manifest history, active Cell heads, retained volume branches, explicit snapshots, legal
holds, cross-region replication/export roots, and GC safety roots. The document's own edge rule
already fails closed on unknown *schemas*; the equivalent rule for unknown *root classes* is missing,
and its absence is more dangerous, because an unrepresented root class is silently absent from the
mark rather than encountered during traversal.

**Rule T7.** The collector refuses to run for any tenant containing an object or ref class not
representable in the catalog schema — fail closed on unknown root classes, mirroring the
unknown-schema edge rule. For the first slice, restrict destructive collection to tenants whose
namespace contains only log and plain-ref classes, and say so.

### T8 — The R1 retention floor is not a root, so the retention-edge rule has nothing to stand on

The document says "a compact commit receipt containing a historical target digest does not
automatically promise permanent retention of that target's data", and that finite-retention mode
"needs separate commit-metadata and readable-data windows". Neither window is a catalog root, and
nobody publishes them. Under the accepted design this is not a future concern: candidate A's
`resolve` seeks the commit at `base + 1` through the chain, and unexpired `OpIntent` records anchor
`proposed` objects. Traversing from the captured head via `parent` reaches genesis, so either
nothing below head is ever collectible (collection is useless) or traversal truncates at a boundary
that is currently undefined — and if that boundary sits above the 7-day operation window, collection
deletes the evidence an in-flight retry needs to decide whether it committed.

**Rule T8.** Per-resource retention floors (commit-metadata floor and data floor) and unexpired
operation intents are catalog root classes, frozen with their ref and captured in the round.
Traversal marks from the captured head down to the floor; below the floor is collectible. A floor
may advance only while the control record is `Open`.

---

## 3. What the protocol gets right

Recording these so the corrections are not mistaken for a rewrite.

- The refutation of grace periods and of post-CAS reupload is correct and matches §22.7 and the
  prior arena finding. §19.3 step 9's "revalidate protected refs and pins" is precisely the
  insufficient recheck this document rejects; the freeze supersedes it.
- Freeze-as-consistent-cut is the right primitive. Because the freeze CAS and a publisher's CAS
  contend on the same ref key, they serialize: a publication that wins is inside the captured value
  and is marked; one that loses cannot land. That genuinely closes the publish/mark race that
  `sweep.rs` leaves open, and it is what makes T1's touch rule sufficient rather than merely
  narrowing a window.
- "Any missing root, unknown schema, corrupt object or unreadable required child aborts deletion"
  is the correct fix for `sweep.rs:62-64`, which today skips unreadable reachable objects and then
  deletes.
- Treating stale listing as a completeness concern only, with correctness resting on direct reads,
  is right and matches §7.8.
- Requiring incarnation-unique deletion rather than plain ETag is right, and the measured MinIO
  result vindicates the caution.

---

## 4. Added adversarial checks

Beyond the document's eight. Each targets a trace above.

1. Negative capability probe per backend instance: a deliberately wrong token must be **rejected**
   and the object must **survive** a subsequent GET. Run before every destructive round, not once at
   deploy. (Current MinIO must fail this; that is the regression test for C1.)
2. Delete-then-recreate on memory and local must produce a different token. Both fail today.
3. T1: helper stalls in round *N*; round completes; a by-reference publication touches and commits
   the object; the stalled delete must fail. Then the same trace **without** the touch rule, asserted
   to fail — so the rule cannot be silently dropped later.
4. T2: publisher and collector race to create an absent registered ref; collector loses; assert the
   round does not reach `Deleting` until it has re-read and frozen the publisher's value.
5. T3: publisher holds a pre-freeze token across a full round; assert it observes `Collecting`, not
   `PreconditionFailed`, and that its operation does not expire inside a bounded round.
6. T4: catalog-registered pin with an absent object aborts the round; assert no placeholder.
7. T5: two helpers in one round; assert exactly one plan digest is bound and the other aborts.
8. T6: a writer that never registered attempts a ref CAS; assert refusal, and assert a sealed
   pre-catalog binary cannot publish.

---

## 5. Decision and first slice

**Do not ship the protocol as written.** T1 alone makes it unsound.

**Ship destructive collection in this order:**

1. **Now, no new capability needed:** fenced-owner deferred deletion — the partition's fenced writer
   deletes objects on its own published `retired` list past `reader_grace`. T1 does not apply,
   because §7.5a.1 forbids anything outside the partition from referencing its WAL and segment
   objects, so no by-reference resurrection exists. This reclaims the dominant byte source and is
   independent of the catalog, the freeze, and conditional delete.
2. **Now:** global collection in dry-run only, with the traversal-poisoning fix, the catalog, T6's
   registration gate, and the seal. This is where catalog completeness gets exercised without
   consequences.
3. **Gated:** destructive global collection, enabled only when (a) the negative probe of C1 passes
   for that backend instance, and (b) rules T1–T8 are implemented and their checks pass. On the
   measured evidence that means S3 only, with MinIO explicitly disabled and memory/local disabled
   until their token semantics are fixed.

None of this blocks the complete-feed Foundation gate, and none of it changes R1's scope. The only
R1-facing items are T3 (a distinct `Collecting` error variant in the error enum) and T8 (retention
floors as a catalog root class) — both are one-field additions worth reserving now rather than
retrofitting, and neither is a format rewrite.
