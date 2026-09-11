# R1 stable-key review — HAMT + `append_stable` (WIP snapshot)

Adversarial read-only review of `comb-reliable-log-r1`. **Not a final approval.** Findings are stated
against source as observed now; line numbers will move. No edits, no builds or tests in the shared
tree, no child agents, no push.

Read: `crates/combctl/src/hamt.rs` (805), stable portions of `crates/combctl/src/log.rs`,
`crates/comb-core/src/operation.rs` (stable types), `commit_at_snapshot` in
`crates/combctl/src/publish.rs:843-974`. Contract:
`docs/implementation/reliable-log/stable-key-addendum.md`. Generic timed/group path excluded per
ownership split.

## Verdict

The core duplicate-prevention argument holds. Lookup and CAS are bound to one provider token, the
index is derived from the same snapshot the CAS consumes, there is no stable Pending registry, key
scope excludes the physical partition, and the receipt is exact. I could not construct a trace that
produces two ranges for one key.

Four defects, none of which break that argument: three are error-classification or
defence-in-depth gaps where the code is weaker than the contract's own table, one is a liveness
bound. Two further items are contract gaps rather than defects.

---

## What I verified as sound (traces attempted, no defect found)

- **CAS loss.** `append_stable` (`log.rs:215-285`) loops on `is_retryable` → `BackendUnavailable`
  (`log.rs:529-534`) and on `CasResult::Conflict`. Both re-read the head and re-run the lookup, so a
  lost final reply resolves from the committed index and returns the exact stored range
  (`log.rs:242-251`). No finalize write is needed, matching the addendum's step 5.
- **Same-head race.** Both callers pass the same `snapshot` into `commit_at_snapshot`, which CASes on
  `snapshot.version` (`publish.rs:951-958`). At most one wins; the loser gets `Conflict`, re-reads,
  and finds the winner's leaf. The lookup at `log.rs:227-239` and the CAS consume the *same* token,
  so there is no window between proving absence and publishing.
- **Restart.** No in-process state participates. `commit_at_snapshot` writes no intent, so a crashed
  attempt reserves nothing and a later payload may win — exactly what the addendum specifies
  (line 164).
- **Duplicate insert is asserted, not assumed.** `insert_at` returns `RecoveryFailed` if it reaches a
  leaf already holding the key (`hamt.rs:211-216`). Given lookup and insert walk the same root with
  the same path, this is unreachable in a healthy index — it is a real assertion, and worth keeping.
- **Key scope.** `LogStore` sets `logical = <log name>` and `name = log/<name>/p0`
  (`log.rs:132-148`); `path_digest` takes `logical`, never the resource
  (`operation.rs:246-252`). The domain is `(tenant, logical_log, key)` as required, and survives
  repartitioning.
- **Object digests are consistent.** `Envelope::new` digests the payload only
  (`envelope.rs:51`), so `store.key.digest(bytes)` in `build_chunk` (`log.rs:600`) and
  `CompactPlan` (`log.rs:1040`) equals the key that `commit_at_snapshot` uploads under. No dangling
  segment or chunk digest. (I chased this specifically; it is clean.)
- **Complete-feed retention.** Trim rejects Complete (`log.rs:1107`); `load_domain` refuses to open a
  Complete feed as Trimmable (`log.rs:448`); `prepare_events` never downgrades
  (`log.rs:714-720`); a Trimmable log cannot be upgraded, because the missing-index guard fires
  (`log.rs:722-730`). Nothing removes index keys.
- **Empty-index regeneration is closed.** `index_from_snapshot` errors on a missing root
  (`log.rs:472-476`) and `prepare_events` guards it (`log.rs:722-730`), so
  `hamt::empty_root` at `log.rs:769` is reachable only for a genuinely new log. This was the
  highest-risk hole in the contract (line 150) and it is handled.

---

## S1 — A transient read error is reported as index corruption

**Concrete defect. Contract deviation against the addendum's own table (line 178).**

`map_node_read_error` (`hamt.rs:475-484`) handles `NotFound`, `BackendUnavailable`, `IntegrityError`
and `InvalidFormat`, then sends **everything else** to `IntegrityError`:

```rust
_ => CoreError::IntegrityError(format!("stable index node {digest}: {e:#}")).into(),
```

`CoreError::Io(#[from] std::io::Error)` exists (`error.rs:54`), and `LocalBackend::get` returns it
for every non-`NotFound` io failure. So EIO, EMFILE/ENFILE, EINTR, or an ETIMEDOUT on a network
mount becomes "the stable index is corrupt".

Trace: a complete feed on the local backend hits the process fd limit during a lookup at depth 3.
`get_blob` → `CoreError::Io` → `map_node_read_error` → `IntegrityError`. `append_stable` does not
retry Integrity (`is_retryable` matches only `BackendUnavailable`, `log.rs:529-534`), so the caller
receives a permanent-looking corruption verdict for a completely healthy index. Per addendum line
181, a density gap "blocks new publication until repair" — an operator or verifier acting on this
starts repairing an index that was never damaged.

**Minimal correction.** Classify explicitly rather than by catch-all: map `Io` (and any future
transient variant) to `BackendUnavailable`, and reserve `IntegrityError` for `NotFound`, digest
mismatch, and decode failures. Make the catch-all arm fail loudly for unclassified variants rather
than silently calling them corruption.

---

## S2 — Branch nodes have no position binding, so a misplaced subtree yields `Absent`, not `Integrity`

**Defence-in-depth gap the contract explicitly promises to close (addendum line 177: "A node digest,
schema, size, **path**, or count check fails → the index is corrupt").**

`load_node` (`hamt.rs:429-471`) checks branch nodes for schema, encoded size, and slot ordering, and
checks **leaves** against the walked path for nibbles `0..depth` (`hamt.rs:453-466`). Branch nodes get
no path check and no depth check. Nothing binds a branch to the position it was loaded from.

Trace: suppose any path-copy defect, format change, or partial-write recovery installs subtree `B`
(the subtree for slot `s'`) at slot `s`. A lookup for a key whose nibble at that depth is `s`
descends into `B`, finds no child at its next nibble, and returns `Lookup::Absent`
(`hamt.rs:155`). `append_stable` then commits a **second** range for a key whose leaf still exists,
unreachable, under the correct subtree. The false negative is silent — it is the one failure mode
duplicate prevention cannot survive.

I found no current code path that produces this: `insert_at` and `split` both compute the slot from
`nibble(path, depth)` consistently (`hamt.rs:231, 255, 281-303`). So this is a latent class, not a
live bug — but it is precisely the class the contract's table says must be detected, and leaves are
already protected this way while branches are not.

**Minimal correction.** Add `depth: u8` to the branch variant and require it to equal the walk depth
in `load_node`. One additive field inside the existing node schema; no format rewrite. This catches
vertical misplacement. Binding the accumulated path prefix would also catch same-depth horizontal
misplacement — worth stating as the complete fix, but the depth field is the narrow one.

---

## S3 — The insert path skips leaf-range validation and copies invalid leaves into the new root

**Concrete defect. Contract deviation (addendum line 179: a leaf naming an impossible range →
`Integrity`, no CAS).**

`insert_at` calls `load_node(store, d, logical_log, path, depth, None)` (`hamt.rs:196`) — `head` is
`None`, so `validate_leaf_range` never runs (`hamt.rs:467-469`). The lookup in `append_stable` does
pass a real `IndexHead` (`log.rs:234-237`), but it only walks **our key's** path.

Trace: a leaf on an unrelated path carries `last` beyond `head_seq` (a partial-write or an earlier
format defect). Our key's insert descends a shared prefix, reaches that leaf, `existing != entry.key`,
and `split` re-emits it verbatim via `leaf_node(&old)` (`hamt.rs:220-228, 284-285`). The invalid leaf
is path-copied into the newly published root and the CAS proceeds. The contract requires no CAS while
known-invalid evidence is in play; here the commit both proceeds and carries the corruption forward
into the authoritative root.

**Minimal correction.** Thread the same `IndexHead` through `insert`/`insert_at`/`split` and pass
`Some(head)` to `load_node`, so any leaf touched during a path-copy is validated and a bad one aborts
before upload. `append_stable` already has the head at the call site.

---

## S4 — Bounded-liveness: 128 attempts, full path-copy per attempt, no stable batching

**Liveness defect, not a correctness defect. No duplicate risk.**

Every stable append re-runs the whole cycle on conflict: read head, load manifest twice
(`index_from_snapshot` and `load_domain`, `log.rs:227-228`), walk the index, then in `prepare` upload
up to 52 fresh HAMT nodes before CASing (`hamt::put_node` → `put_blob` runs inside `prepare`, so
uploads happen *per attempt*). Losers discard all of it.

The addendum's group clause (line 166 — "`GroupWriter` may combine calls physically… one CAS plan
contains at most one candidate for each key") is **not implemented**: `Submission` carries only
`OpIdentity::Generic` and `GroupPlan` always passes `stables: &[]`. So N concurrent stable appends to
one log fully serialize on the ref CAS, each round wasting N−1 path-copies. Under sustained
concurrency a caller can exhaust the 128-attempt loop and get `"stable append did not converge"`
(`log.rs:285`) for an operation that never committed.

Correctness survives — the caller retries the same key and payload and converges to the same range.
Worth recording because the failure surfaces as an opaque error under exactly the load Foundation
will generate, and because the unimplemented group clause is the intended remedy.

---

## Contract gaps (not defects in the code as scoped)

- **`Fenced` is unreachable for stable appends.** The addendum declares
  `StableAppendError::Fenced` (line 99) and gates `CompleteFeedWriter` on a writer fence.
  `append_stable` takes no fence; `next_writer` (`log.rs:536+`) grants leadership opportunistically
  when no live lease is held, so a stable appender silently takes the lease and bumps the epoch. Not
  a duplicate-prevention hole — the ref CAS still serialises — but the declared error cannot occur
  and the declared fence is not carried.
- **`StableIndexRoot.entries` is never verified.** `lookup` calls `root.validate()`, which checks the
  schema only (`hamt.rs:41-49`); `entries` is written by `insert` (`hamt.rs:178-181`) and read by
  nobody. It is the cheapest available detector for a silently regressed root and is currently
  decorative. (Matches root's earlier note; recorded here as confirmed.)
- **Compaction clears `stable_admissions`** (`log.rs:1048`). Correct — it is a per-commit list — but
  the addendum's integrity verifier (line 181) cross-checks admissions against commit generations,
  and after compaction that evidence exists only in the retained commit chain, not the head manifest.
  The verifier must walk the chain; worth stating so it is not written against the head manifest.

## Explicitly out of scope / correctly classified

- **Hash collisions are theoretical only.** `nibble(path, 51)` reads bits 255..259 with 256+ padded to
  zero (`hamt.rs:338-352`), so depth 51 is a 2-way level and nibbles 0..51 cover the full 256-bit
  path. A full-depth collision is therefore a full digest collision. The `Rejected("stable key path
  hash collision")` arms (`hamt.rs:193, 218, 279, 299`) are correct and publish nothing. No action.
- **Object-memory bounds are deferred to R2.** `load_domain` materialises whole manifests, `read`
  returns a `Vec`, and compaction loads every chunk into memory (`log.rs:1020-1027`). Out of scope
  here and already owned by the bounded-manifest slice.
- **Orphans from failed CAS attempts** are accepted by the addendum (line 189) and GC is off. Not a
  finding.

## Suggested test targets (not written)

1. S1: inject `CoreError::Io` on a node read; assert `Unavailable`/retry, not `Integrity`.
2. S2: hand-build an index with a branch subtree grafted at the wrong slot; assert `Integrity`, not
   `Absent`, and assert no second range is committed for the shadowed key.
3. S3: place an out-of-range leaf on a sibling path, then insert a key sharing its prefix; assert the
   CAS is refused and the bad leaf is not path-copied into a new root.
4. S4: release 64 distinct keys concurrently against one log; assert convergence within the attempt
   budget, or record the observed conflict count as the reason to implement the group clause.
