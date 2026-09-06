# R2 catalog and feed review

Commit `2587de1953d1acdc866e0df6e5f2f5db522da1c0` is not accepted. This review is pinned to that commit; the owner is correcting a later branch state.

Root executed eight feed cases and three catalog cases against copied, hashed binaries. Ten assertions failed. Closing an unacquired writer session hung until the external eight-second timeout stopped that private test child. The original worker worktree and its compiler were not touched.

[Receipt](../implementation/reliable-log/verification/r2-parent-review-2587de1.json), [logs](../implementation/reliable-log/verification/r2-parent-review-2587de1.txt), [feed test source](../implementation/reliable-log/verification/r2-parent-review-2587de1.rs), [catalog test source](../implementation/reliable-log/verification/r2-catalog-parent-review-2587de1.rs).

## Executed findings

| Case | Observed result |
| --- | --- |
| Pre-cancelled page read | Returned success instead of Cancelled |
| Expired page deadline | Returned success instead of DeadlineExceeded |
| Unbounded Get failpoint on ref | Fired during bounded page read |
| Transient manifest read | Reported Integrity, without recovery |
| Foreign manifest identity | Accepted and replayed |
| Manifest head at u64::MAX | Panicked on cursor addition |
| v1 ref created after v3 feed | Blocked the existing v3 feed |
| Close unacquired session | Hung until external timeout |
| Append 2,100 catalog refs | Child height 2 under a parent requiring height 1 |
| Ten acyclic catalog branch hops | Accepted despite depth bound |
| Parent span routes to noncovering leaf | Returned the leaf instead of Integrity |

The catalog test constructs a valid-envelope acyclic chain, not an artificial content-digest cycle. It proves the missing depth/parent-child validation without requiring a hash collision.

## Review dispositions

The independent catalog review below flags the absent-v3 legacy guard as a seam. CompleteFeed::open already checks v1/v2 absence there, so that item is closed by source inspection. The actual namespace failure is different: shared read_head rejects a v1 ref even after v3 already exists, reproduced above.

The statement in the independent review that legacy Envelope::decode still ignores flags/codecs is stale. Those checks landed in 8f0f5cb. Its unbounded-read finding remains valid, and root found other unbounded v3 paths in ref and recovery reads.

Idle renewal, owner-aware renewal, lease state transitions, exact chunk/head validation, bounded upload checks and strict v3 recovery fields also remain required. Root reviewed these in source below. The real .fdnc bridge acceptance cannot start until the production fixes and their tests pass. Foundation capabilities stay false.

## Root feed review

# Root R2 feed review, fixed 2587de1

Source inspection against 2587de1953d1acdc866e0df6e5f2f5db522da1c0. Independent executable regressions were built and run in a private fixed-source archive. Ten assertions failed and closing an unused session exceeded the external eight-second timeout. No edits/builds in the owner worktree. Catalog/recovery review is separate.

Not accepted yet. Known idle renewal and owner-aware renewal omissions remain blockers, but the following gaps also need correction.

1. **Cancellation/deadline ignored inside reads and publication.** read_page_once explicitly discards CallContext and awaits read_head, manifest, catalog and chunks directly. A pre-cancelled or expired read can return success; a stalled backend can wait forever. head/open wrap only read_head, not load_manifest. acquire checks before claim but does not bound the claim await. append likewise checks loop entry but not I/O. Follow uses the original call deadline for subsequent reads, so backend work can exceed the follow wait deadline. Apply deadline/cancellation to complete operations and their I/O; preserve typed errors. Bound close release and propagate failure.

2. **The v3 path still uses unbounded shared publication reads.** read_head invokes backend.get for refs, load_commit_view invokes get_blob, generic claim uses unbounded intent/history reads. Adding get_limited is insufficient while reader/session paths reach these calls. Select bounded classes in the v3 layout, including refs, manifests/commit carriers and HAMT nodes. Reject an oversized append payload before hashing/cloning it into the plan. Enforce encoded upload caps in the actual commit_at_snapshot upload path; it currently encodes and uploads without checking the 4 MiB chunk / 512 KiB manifest caps.

3. **Transient object reads become corruption.** load_manifest maps BackendUnavailable/Io to Integrity; catalog and chunk read wrappers map every error to Integrity; append HAMT wrapper does likewise. The read retry loop handles only ReadError::Unavailable, so it cannot recover those transient failures. Keep missing required objects, malformed contents and digest mismatch distinct from transient failures, and retry transient failures with bounded attempts/deadline. Do not hide typed Fenced/Rejected/Integrity behind catch-all Unavailable at publication.

4. **Chunk/head integrity is not checked before replay.** load_chunk only checks an upper plaintext bound, schema, and event count <=2048. It never checks exact catalog first/last/count/raw bytes, contiguous frame sequence, or nonempty frames. A valid-envelope empty chunk leaves seq unchanged and read_page_once loops forever. A chunk with extra frames can return events above snapshot_head because its inner loop does not stop at head_seq or catalog last_seq. Validate the whole bounded chunk against catalog evidence before returning anything; missing/gapped evidence fails closed, never a partial successful page.

5. **Unchecked cursor/sequence arithmetic and manifest binding.** head_seq+1, frame.seq+1, trim_before_seq+1 and append allocation use unchecked additions. A head at u64::MAX panics in debug or wraps in release. Validate manifest resource/log/retention/epoch/head consistency and supported complete-feed shape. Empty target on an existing ref must be distinguished from legitimate lease-only initialization, not treated as an empty published feed. A manifest from another logical log must not replay as this log.

6. **Session state does not track ownership loss, expiry or concurrent acquisition.** acquire returns an Active epoch without checking expiry; it uses wall-clock Utc::now for state instead of the Store clock and durable returned lease. No serialization prevents two concurrent ready/append calls minting separate claims and bumping the same instance epoch. prepare returns LeaseHeld for wrong owner/epoch/expired lease; append loops 32 times and returns Unavailable while state remains Active. This does not satisfy a fenced, non-reactivating session. Serialize acquisition, return exact durable lease state, check owner+epoch on fresh snapshot, transition to Lost on expiry/fence/uncertain renewal, and require a fresh session for new writes. Stable committed-key lookup remains available without reacquiring ownership.

7. **Closing an unacquired/lost session can deadlock.** close matches on &*self.state.borrow(); its non-Active branch calls self.state.send while the watch read guard is still held. Clone/drop the state guard before any write. The release result is discarded and close returns success even on timeout/cancellation/backend failure. Return the actual bounded close outcome. Renewal task must be owned and cancelled on drop without retaining the public session forever.

8. **Earlier phase corrections and R1 followups are still outstanding at this SHA.** 8f0f5cb checks declared plaintext size before clone, but still clones the actual suffix before comparing its actual length. Cache UnsupportedEnvelopeFormat still returns without refetch. Remaining accepted picks db41bce, e0c911e, 05d3816, aa94980 and e61d872 are not in ancestry/patch history shown so far. a675d38 is already 069824a. Integrate missing production fixes while preserving R2 changes, especially Core log namespace reservation and HAMT entry/head/split validation. No adoption of the old R1 tree.

A v1 ref appearing after v3 creation also blocks v3 via read_head.reject_v1. Existing v3 must stay authoritative; the legacy absence check belongs only to new-v3 open. The physical namespace already supplies writer isolation.

Known missing tests from owner remain required: 0xff expansion/encoded upload cap, first-event-too-large with unchanged cursor, idle renewal and fresh instance takeover. Add the failure cases above through actual production methods. No bridge capability changes yet.


## Independent catalog review

# R2 catalog / v3 layout / recovery-schema review

Read-only, pinned to `2587de1953d1acdc866e0df6e5f2f5db522da1c0` in `comb-reliable-log-r2` (extracted
with `git archive` to `/tmp/r2cat`; worktree was clean at extraction but the owner continues, so all
line numbers are SHA-pinned). No builds, edits, children, push.

Slice: `catalog.rs`, v3 layout, and recovery/schema integration in `store.rs` / `publish.rs` /
`hamt.rs`. `feed.rs`, session, and read semantics are root's. Idle renewal and owner-aware renew are
known owner omissions outside this slice.

## Blockers

### C1 — The leaf-split guard is pinned to `h == 1`, so any split above the first internal level produces a ragged tree

`append_at`'s split-handling arm (`catalog.rs:374-433`) only merges a bubbled split when
`child_height == 1 && h == 1 && sh == 1 && split_children.len() == 2`. Note `h` is the **node's own**
height, destructured at `catalog.rs:342-346`, which shadows the `height` parameter.

Trace, with the concrete threshold:

1. 1025 chunks: the root height-1 branch overflows to 33 children, splits into two height-1 branches
   under a new height-2 parent, returns `(digest, 2, true)` (`catalog.rs:398-431`). This return lands
   in `append`, which rebuilds the root correctly. This is the existing test
   (`catalog.rs:573-597`) and it passes.
2. The right height-1 branch then refills: `split_off(32)` left it with 1 child, so it takes ~1024
   more chunks to reach 33 children.
3. At roughly **chunk 2050** that nested height-1 branch overflows and returns `(digest, 2, true)` to
   the root.
4. The root has `h == 2`, so the guard fails on `h == 1`. Control falls to the `other` arm
   (`catalog.rs:434-452`), which replaces the root's **last child with a pointer to a height-2 node**
   and writes the root back as `Branch { height: 2, … }`.

The root now declares height 2 while one of its children is height 2. Accounting survives —
`child_from_branch` uses `child_span`, so span and `chunk_count` remain correct, and `seek_leaf`
still terminates because the branch arm reads each node's own height rather than the passed
parameter. What breaks is the structural invariant and, with it, the depth bound: actual depth grows
while `ChunkCatalogRoot.height` does not, and the only `MAX_CATALOG_HEIGHT` check
(`catalog.rs:425-429`) sits inside the guarded path, so it can never fire on the ragged path.

Both existing tests stop at 1025 chunks, one chunk past the *first* internal split and ~1025 short of
the second. The failing region is untested.

Secondary defect in that same check: `h + 1 > MAX_CATALOG_HEIGHT` is evaluated **after** both
`put_node` calls for the left and right branches (`catalog.rs:399-429`), so a rejected height still
uploads two objects. Harmless while collection is off; wrong order regardless.

### C2 — `seek_leaf` returns a leaf without proving the leaf covers the requested sequence

`catalog.rs:473`: `CatalogNode::Leaf { refs, .. } => return Ok(Some(refs))`. The descent trusts
`CatalogChild.first_seq`/`last_seq` to choose the branch (`catalog.rs:475-483`) and then returns
whatever leaf it lands on. Nothing checks that any ref in that leaf covers `seq`.

Trace: a `CatalogChild` whose declared `last_seq` is one too high — from the C1 rebuild path, a
partial write, or any future split bug — routes a seek for that sequence into the sibling leaf. The
caller receives `Ok(Some(refs))` containing a well-formed, internally valid leaf with no ref covering
`seq`. Depending on read semantics (root's slice) that surfaces as a gap or a missing event rather
than `IntegrityError`. `validate_leaf_refs` cannot catch it: the leaf is valid, it is simply the
wrong one.

This is the same rule `hamt.rs` already enforces for leaves — a leaf reached on the wrong path must
not become a negative membership proof. The catalog needs the equivalent: after landing, require
`refs.first().first_seq <= seq <= refs.last().last_seq`, else `IntegrityError`.

### C3 — `seek_leaf` and `walk_right_spine_digests` have no depth bound and no cycle guard

Both `loop` until they reach a `Leaf` (`catalog.rs:471-488`, `508-517`), one `load_node` — one
backend read — per iteration. Neither carries a depth counter nor a visited set, and
`validate_children` cannot detect a cycle because children carry no height or parent evidence. A
branch child digest pointing at an ancestor spins forever issuing reads. C1 makes this reachable
without corruption, since actual depth is no longer bounded by `MAX_CATALOG_HEIGHT`.

Minimal fix: carry the descent depth and fail with `IntegrityError` past `MAX_CATALOG_HEIGHT + 1`.
That bounds both loops regardless of whether C1 is fixed first.

### V1 — Nothing in this slice detects an existing **v2** resource before a v3 store initializes

Root decision (handoff line 146): "Before creating an absent v3 resource, detect an existing matching
v1/v2 resource and return unsupported rather than silently presenting it as a new empty feed."

`Store::reject_v1` (`publish.rs:161-170`) checks `self.v1_ref_key(name)` only — v1, never v2 — and is
called from `read_head` (`publish.rs:173`). So a v3-layout store opening `log/foo/p0` where an R1
**v2** ref exists finds no v1 ref, reads the absent v3 ref, and proceeds as a new empty feed.

If that guard is intended to live in `feed.rs::CompleteFeed::open`, it is root's slice — but it
cannot be built on `reject_v1` as written, and no other v1/v2 detection exists in `store.rs` or
`publish.rs`. Flagging the seam rather than the placement.

Related, minor but in exactly this diagnostic: `reject_v1`'s message hardcodes `NS_V2` — "this
process uses comb/v2 only" — even when the store is V3 (`publish.rs:164`).

### V2 — The v3 log-target guard is the one remaining unbounded read on a v3 path

`reject_existing_log_manifest` (`store.rs:410-430`) fetches the current target with
`store.get_blob(digest)`, i.e. the legacy unbounded path: `cache_read` via `std::fs::read`
(`store.rs:~215`) and `Envelope::decode`, which still ignores flags and codecs. It runs inside
`SetTargetPlan::prepare` (`store.rs:463`), so every Core `set_target` against a v3 ref fully
materializes a manifest of arbitrary size through the unchecked decoder — the precise thing phase 1
was built to stop.

The fix is now cheap: `EnvelopeReadSpec` already carries `allowed_schemas` (`envelope.rs:49-55`), so
this can read through `get_blob_limited` with a manifest class listing both carrier schemas.

## Medium

### V3 — `load_commit_view` applies v2-permissive defaults to the v3 carrier

`publish.rs:1168` accepts both `comb.log.partition-manifest/v2` and `…/v3`, then decodes with
defaults that are correct for v2 and wrong for v3: `admitted` falls back to an empty vec when the
field is absent (`publish.rs:1180-1184`), `result` to `Null`, `ref_state` to `None`
(`publish.rs:1188-1195`).

Handoff lines 117 and 148 require the v3 manifest to **retain** `log`, `admitted`,
`stable_admissions`, `result` and `ref_state`. With the permissive defaults, a v3 manifest missing
`ref_state` decodes cleanly and fails later in `published_from_view` (`publish.rs:1487-1492`,
"commit … is missing the original ref state"); one missing `admitted` decodes to an empty list, which
by the R1 F1 rule makes an admitted identity look un-admitted at ack time. Missing evidence should be
an integrity error at decode for the v3 arm, while the v2 arm keeps its permissive defaults for R1
compatibility.

### C4 — `encoded_node_bytes` measures a re-encode, and the node-size assertions are vacuous

`catalog.rs:490-500` reads the payload, then rebuilds an envelope with `Envelope::new` and returns
that encoding's length. `Envelope::new` stamps `created_at: Utc::now().to_rfc3339()`, whose length
varies with fractional-second precision, so the number can differ from the stored object's by several
bytes in either direction.

More importantly the read itself is capped at `MAX_CATALOG_NODE_OBJECT_BYTES` via `spec()`
(`catalog.rs:75-83`), so an over-cap node can never be measured — it fails the read first. The two
tests asserting `encoded_node_bytes(…) <= MAX_CATALOG_NODE_OBJECT_BYTES` (`catalog.rs:566-570`,
`592-596`) therefore assert something the read already guaranteed; a real violation would surface as
an `unwrap` panic on `ObjectTooLarge`, not a failed assertion. The bound *is* enforced — correctly,
at write time in `put_object` (`store.rs:134-141`) against the true encoding — so the recommendation
is to assert there and drop this helper rather than to add another check.

## Harmless sketch differences (no action needed)

- `catalog.rs` `spec()` sets `max_plaintext_bytes == max_encoded_bytes == 16 KiB`
  (`catalog.rs:80-81`). Non-binding, since the payload is a suffix of the encoded bytes. Contrast
  `hamt.rs:428-429`, which correctly separates 4 KiB plaintext from 8 KiB encoded.
- Dead code marking the unfinished split logic: `if height != 0 { /* caller rebuilds parent */ }`
  (`catalog.rs:330-332`), unused `_height`/`_split` (`catalog.rs:262`), and the stale reasoning
  comment at `catalog.rs:370-383`. Not defects, but they sit exactly on C1.
- `append` requires the first chunk to start at sequence 1 (`catalog.rs:232-237`). Correct for a
  complete feed that never trims; would need revisiting only if a feed is ever initialized at a
  non-1 base.
- Each append performs a second `load_node` per level to rebuild the parent entry
  (`catalog.rs:264`, `catalog.rs:351`), roughly doubling reads per append. A cost, not a defect; the
  span and count are already available in the recursive return if the signature carried them.

## Verified correct (checked, no defect)

- **R1 compatibility of the HAMT envelope carrier is preserved.** `hamt::put_node` still writes via
  `store.put_blob` (`hamt.rs:609`), so nodes keep envelope schema `comb.object/v1`, and
  `HAMT_ENVELOPE_SCHEMAS` is exactly `["comb.object/v1"]` (`hamt.rs:19`) — matching handoff line 70.
  Existing v2 stable indexes stay readable through the new bounded path. This was the most likely
  place for a silent break and it is right.
- v3 layout is applied through `object_key`, `ref_key`, `intent_key` (`publish.rs:121-157`) and
  `cache_path` (`store.rs:223-228`), with a test asserting all four differ from v2
  (`store.rs:762-778`). R1 constructors still default to `KeyLayout::V2` (`store.rs:71`).
- `load_commit_view` recognizes both manifest carriers (`publish.rs:1168`) and
  `reject_existing_log_manifest` recognizes v3 (`store.rs:423-426`).
- `put_object` enforces the encoded cap on the **real** envelope encoding, before upload
  (`store.rs:132-141`) — root decision 3 satisfied, and `AlreadyExists` is treated as success so the
  write stays idempotent.
- `EnvelopeReadSpec` now carries `tenant` and `allowed_schemas` (`envelope.rs:49-55`); the phase-1
  items on both are resolved, and `catalog.rs` and `hamt.rs` both use the slice form.
- Catalog node validation is thorough where it exists: `deny_unknown_fields` on every node and ref
  type, leaf and branch width bounds, height range, per-ref `event_count`-versus-span agreement, and
  contiguity across siblings (`catalog.rs:85-146`). The gaps are C2 (leaf-covers-seq) and the absence
  of any child-to-subtree cross-check, not the per-node rules.

## Suggested tests (not written)

1. C1: append 2100 chunks; assert every branch child's node height equals the parent's height minus
   one, and that `ChunkCatalogRoot.height` equals the true descent depth.
2. C2: hand-build a catalog with one `CatalogChild.last_seq` off by one; assert `seek_leaf` returns
   `IntegrityError`, not a non-covering leaf.
3. C3: hand-build a branch whose child digest points at its own ancestor; assert a bounded
   `IntegrityError` rather than a hang.
4. V1: one MemoryBackend, an R1 v2 `log/foo/p0` present, then open v3 `foo`; assert unsupported, not
   a new empty feed.
5. V3: a v3 manifest missing `ref_state`, and one missing `admitted`; assert `IntegrityError` at
   decode rather than a late recovery failure or an empty admissions list.
