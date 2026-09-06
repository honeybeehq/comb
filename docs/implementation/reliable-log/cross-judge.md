# Cross-judge — Comb reliable Log arena (A: ledgered refs / B: forward helping)

Date: 2026-09-06. Read-only judgement. No repo files changed, no agents launched.

Inputs: `/tmp/comb-design-a.md` (296 lines), `/tmp/comb-design-b.md` (467 lines),
rubric `comb-reliable-log/docs/implementation/reliable-log/design-rubric.md`.
Code verified in `comb-design-a` working tree; spec is `comb-specification-v0_3.md`.

---

## 0. Code facts established (not taken from either proposal)

| Fact | Evidence |
| --- | --- |
| `RefValue` has **no** `#[serde(deny_unknown_fields)]`; `schema` is a plain `String` | `crates/comb-core/src/refs.rs:17-28` |
| `read_ref` never validates `schema`; it deserializes and propagates serde errors with `?` | `crates/combctl/src/store.rs:105-113` |
| `put_update(expected = None)` **creates** when the key is absent, returns `AlreadyExists` when present | `crates/comb-object/src/memory.rs:31-52` |
| `FaultBackend` is probability-only (`random_bool`), no instruction-boundary targeting | `crates/comb-object/src/fault.rs:20-46` |
| Sweeper skips unreadable reachable objects, then deletes | `crates/combctl/src/sweep.rs:62-64, 90-96` |
| Journal head is a single contended ref with a 16-attempt cap; Log ref writes bypass it | `crates/combctl/src/store.rs:231-262`; `crates/combctl/src/log.rs:89-167, 231-313` |
| C4 drill asserts generation 2 after a lost-reply retry | `crates/combctl/tests/drills.rs:89-115` |
| Spec guarantees strong read-after-successful-write per key; range reads required; object-lock / versioning **optional** | `comb-specification-v0_3.md:887-911` (§7.9) |
| §7.8's conceptual trait already carries `get(key, range)`; the implemented trait does not | `comb-specification-v0_3.md:857-886`; `crates/comb-object/src/backend.rs:18-36` |
| §18.1: 7-day default window, `UnknownOperation` after expiry, MUST NOT silently re-execute | `comb-specification-v0_3.md:2663-2684` |

Both proposals' grounding citations check out. B's line-level citations are more precise;
A's C4 characterisation ("the spec forbids") is slightly overstated — the current test passes
no operation ID at all, so it is *absence of the contract*, not a violation of it.

---

## 1. Scores

Scale 1–5 against the six rubric criteria.

| # | Criterion | A | B | Basis |
| --- | --- | --- | --- | --- |
| 1 | Atomic retry outcome | **4** | **4** | Both close lost-reply, later-writes, concurrent same-ID, mismatch, expiry. A carries a real proof (decidability lemma + at-most-one-commit theorem, A:149-153). B carries better contract detail: expiry-beats-conflict, explicit hash domain, first-use allowance, `UnknownOperation` even after key deletion (B:247-254). Each is missing what the other has. |
| 2 | Bounded active state | **4** | **2** | A: one digest in the ref; capped `wal`/`l1`; paged catalog; one index object per compaction cycle (A:163-202). B: one link in the ref, but **two durable create-only keys per operation** for 7 days, cleaned by listing 256 shards with no size limit (B:222-231), plus a ≤512 KiB bundle fetched and verified on every logical commit. B also defers bounded manifests and paged reads entirely (B:16, 401-425). A's `admitted: Vec<(OperationId,u64)>` is uncapped — a real but local defect. |
| 3 | Recoverable ownership | **4** | **3** | Both use instance-ID fencing over display names and idle renewal. A is structurally immune to renewal damaging retry evidence (no receipt in the ref). B's renewal path may *clear* `pending_receipts` on a marker it declares non-correctness (B:279, holes B3/B4). A's cost: takeover needs an admission-index rebuild (hole A4). |
| 4 | Safe lifecycle | **3.5** | **3** | Neither claims GC is solved; both explicitly reject publish-then-repair. A fixes `sweep.rs:62` poisoning, adds pins/roots/quarantine, and reclaims the dominant byte source inside a defensible fenced-owner boundary with stated limits (A:204-217). B ships no reclaim and leaves the unsafe sweep untouched, but its seven GC preconditions (B:437-447) are the better acceptance gate. |
| 5 | Small consumer interface | **3** | **4** | B's crate-private `RefCommitter` + `RefMutationPlan` (B:199-218, 336-359) *structurally* prevents bypass and closes the three raw Log ref-write sites; `TrailLog` with `ReadLimit`/`ReadPage`/`seek_ts`/`trim_before` (B:405-423) is the cleaner consumer boundary. A leaks `WriterSession`, `compact_if_due`, `Retention` to the consumer and models mutation as a closed `Mutation` enum that cannot express compaction/trim plans without growing into every view. |
| 6 | Verifiable delivery | **4** | **4** | A has four independently landable slices with stated dependencies and a genuinely small R1 (A:241-246). B has one deep slice, a stronger crash-at-every-step matrix, a four-backend acceptance run, and a measured overhead gate (B:374-399), but defers more and correctly notes the probability-only fault backend cannot target the decisive boundary. |
| | **Total** | **22.5** | **20** | |

---

## 2. B's comparative case against the chain does not survive contact with A

B rejects the immutable-history alternative on three grounds (B:322-334). All three
are aimed at a chain-only design that A explicitly rejects as its own alternative 3 (A:284).

1. *"Tenant-wide cross-request conflict still needs the separate intent reservation. A chain
   local to resource A cannot detect the same ID used on resource B."* — **Factually wrong about A.**
   A's `OpIntent` lives at `ops/<shard2>/<op_id>.json`, keyed by operation ID alone within the
   tenant, and carries `resource` + `request` (A:108-119). Cross-resource reuse produces a
   different `RequestHash` and returns `IdempotencyConflict` at step 2 (A:136). A and B have the
   *same* tenant-wide reservation key; they differ only in what the key stores.
2. *"Recovery after many intervening writes is linear in commit count."* — **Answered.** A's
   `skip: ancestor at generation − 2^k` (A:76) gives O(log n) seeks; A asserts the fetch-count
   bound as a test (A:259). ~20 GETs at a million commits.
3. *"Retaining chain nodes can retain old manifest identities and complicate physical retention."*
   — **Mostly answered.** A decouples `op_window` (headers, KB-scale) from `reader_grace` (data
   bytes) (A:202). Residual concern is real but small; it is not a structural defeat.

B's one genuine advantage is O(1) receipt lookup by ID alone. A also gives O(1) on the healthy
path (the `Applied` intent caches the result, A:136) and degrades to one O(log n) seek only when
the finalize write was lost. That is a constant factor on a rare path — not worth the coupling
B pays for it.

---

## 3. Correctness holes

### A — immutable-history recovery

- **A1 — `proposed` is overwritten, not unioned.** Step 3 CASes the intent to
  `Pending{base, proposed}` (A:137). A losing twin's uploaded objects lose their only GC anchor
  when the winning twin rewrites the field. Fix: bounded union, or derive upload digests
  deterministically so twins produce identical objects.
- **A2 — no future-time bound and no first-use allowance.** The age gate is one-sided
  (`created_at < now − window + slack`, A:135). A client with a fast clock mints an ID that stays
  admissible for as long as its skew, so the intent can never be safely deleted at the window
  edge. Graft B:249.
- **A3 — chain density is an unenforced invariant.** `resolve` seeks "the commit at base+1"
  (A:140), which presumes every generation bump wrote a commit. Any writer that bumps
  `generation` without a commit — a v1 binary (§4 below), or a bug — permanently breaks resolution
  for every in-flight op on that ref. Needs an explicit density check with a fail-closed error.
- **A4 — producer dedup is a second, weaker mechanism.** The ref path uses durable intents; the
  log producer path uses an in-memory `AdmissionIndex` rebuilt from a checkpoint plus a tail scan
  (A:197). If any manifest between the checkpoint and head is unreadable, the index has a silent
  hole and a producer retry re-appends — a duplicate event, the exact §8.8 failure. Rebuild must
  fail closed (refuse leadership) on any unreadable tail manifest. Separately,
  `producer_dedup_window < op_window` (A:197, A:290) is a §18.1 deviation A itself flags; it must
  yield `UnknownOperation`, never silent re-execution, or be dropped.
- **A5 — sweeper vs in-flight CAS.** The ops sweeper deletes an expired `Pending` intent (A:155,
  case C-e) while a CAS from that op may still be in flight; the commit then lands with no intent.
  The outcome is spec-legal (`UnknownOperation`) but should be stated, and the sweeper should
  refuse intents whose recorded base is at or adjacent to live head.
- **A6 — group duplicates under-specified.** `admitted` has no cap (criterion 2), and A does not
  say what happens when one batch carries the same producer op twice. B does (B:316-318).

### B — forward helping

- **B1 — predecessor finalization is an availability coupling on the write path, with no bound
  and no break-glass.** "Do not proceed if any receipt cannot be made durable" (B:269) plus
  invariant 3 (B:296) means: if the bundle object is unreadable, or one receipt key cannot be
  written, **no writer can ever advance that ref again.** B calls this "a brief per-resource
  stall" (B:452); nothing bounds it. A ref whose bundle is lost is bricked. A has no analogue —
  a fresh op reads head, builds a commit, and CASes; a missing ancestor never blocks a write.
- **B2 — expiry cleanup contradicts invariant 4.** "No path removes an unexpired intent or
  receipt" (B:298) coexists with cleanup keyed on `expires_at` (B:231). A ref idle past seven days
  links a bundle whose receipts have been deleted. The next writer must finalize it, so it
  *resurrects expired receipt keys*; a concurrent cleaner deleting them makes the helper's verify
  fail, which under B1 refuses the write. Needs an explicit rule: **helping skips entries whose ID
  has expired**, and a fully expired bundle link may be cleared unconditionally.
- **B3 — the finalized marker is load-bearing but declared non-correctness.** Renewal may clear
  `pending_receipts` "unless its finalized marker is known durable" (B:279), while the storage
  table calls the marker "create-only acceleration, not correctness" (B:228). Clearing on the
  marker is safe *only* if the marker is written strictly after every entry in the bundle is
  durable and verified, by a helper that did the verification. Either forbid clearing in the first
  slice, or promote the marker to an invariant with that write-order rule stated.
- **B4 — renewal must re-derive the link from a fresh read.** "Preserve the link byte for byte"
  (B:279) is safe only if the renewal writes a value derived from the ref version it is CASing
  against. Any renewal built from a cached `RefValue`, or one that retries after a precondition
  failure without re-reading, silently reverts a newer bundle link. State it as an invariant;
  B's own test list already has the race (B:389).
- **B5 — helping-before-completeness-check is an implicit invariant.** A same-ID retry landing on
  a *new* leader after takeover is resolved correctly only because step 2 (help predecessor)
  precedes step 3 (check completeness) (B:269-270). B never states this as a rule, and B:277
  offers latitude on finalization timing that must **not** extend to the predecessor. Make it
  explicit: predecessor finalization is mandatory and ordered; only one's *own* bundle may be
  finalized lazily.
- **B6 — registry key volume vs criterion 2.** Two create-only keys per operation, held seven
  days, cleaned by listing 256 shards (B:222-231). At log-ingest rates this is O(events) durable
  keys in the window with no explicit maintenance size limit. Fix: time-bucketed prefixes so
  cleanup drops a closed bucket instead of scanning, and fold intent+receipt into one CAS'd key.
- **B7 — up to 2,048 create-only PUTs between consecutive logical commits** (B:157, 320), which
  must complete before the next CAS. B honestly gates the slice on a benchmark. Note the
  asymmetry: A reaches the same producer-dedup guarantee at O(1) writes per batch via checkpoints
  (A:197). This is the sharpest reason to prefer A's shape on the log path specifically.

### Both — migration (the flagged issue)

- **M1 — the tenant capability barrier is not a fence.** B's migration step 4 — "Flip a tenant
  writer-capability barrier. After this point, v1 binaries cannot mutate refs" (B:366) — has **no
  enforcement mechanism.** Nothing in the v1 code path reads any marker. Verified: `RefValue` has
  no `deny_unknown_fields` (`refs.rs:17`) and `read_ref` never checks `schema` (`store.rs:105`),
  so a v1 binary reads a v2 ref, silently drops the new field, and writes back a v1 body.
  B *names* this hazard at step 3 and then relies on the unenforceable barrier to prevent it.
  A never mentions it at all.
  **Failure modes differ sharply.** For B, one v1 write destroys `pending_receipts` — the only
  crash-recovery evidence — and a duplicate execution becomes possible: the exact failure the
  design exists to prevent. For A, one v1 write breaks chain density and `resolve` fails closed
  (A3). **A's migration failure mode is strictly safer.**
- **M2 — the enforceable seal available today.** Overwrite the v1 ref key with a body that v1
  `RefValue` deserialization *rejects* (e.g. a required non-`Option` field with a wrong type).
  v1's `read_ref` propagates the serde error through `?`, so `set_target`, `claim`, and `append`
  all fail hard. This uses only existing primitives and is restartable and idempotent.
  **Deleting the key is not a seal:** `put_update(expected = None)` on an absent key *creates*
  (`memory.rs:34`), so a v1 writer would recreate the ref at generation 1 and resume writing.
- **M3 — ordering.** Deploy v2-aware readers everywhere → seal → migrate → require `OperationId`.
  Add `deny_unknown_fields` plus a schema floor to v2 readers so the *next* format bump is
  self-fencing; that protects v2→v3 only, which is precisely why the seal is needed for v1→v2.
  Rollback after sealing means rolling forward. B states this (B:372); A does not.

---

## 4. Verdict

**Base: A. First slice: immutable-history recovery, not forward helping.**

Forward helping puts a mandatory, cross-object, cross-writer protocol between every pair of
logical commits. It is load-bearing for correctness (B5), on the critical path (B7), coupled to
registry availability with no bound (B1), and it interacts badly with expiry cleanup (B2),
renewal (B3, B4), and old binaries (M1). Immutable-history recovery puts the evidence inside the
object the CAS already publishes. No writer depends on another operation's finalization; recovery
is a read-only seek; every failure mode is read-side and fail-closed.

The three arguments B raises against the chain are, respectively, false about A, answered by skip
pointers, and mostly answered by A's decoupled retention windows (§2).

### Grafts from B into A, in slice order

| # | Graft | Source | Why it is not optional |
| --- | --- | --- | --- |
| G1 | Request-hash domain: include tenant, kind, resource, portable condition, payload; **exclude** provider version token, route, timeout, writer instance, lease epoch, sequence allocation | B:253 | A says only "canonicalized request body". Without this, a retry through a different route or after re-reading the generation hashes differently → spurious `IdempotencyConflict`. |
| G2 | Future-time bound + configured first-use allowance on `OperationId`; expiry beats conflict | B:249-251 | Closes A2. |
| G3 | Crate-private `RefCommitter` + `RefMutationPlan` (prepare is re-runnable, cannot issue the CAS; only the committer calls `put_update` on a ref) as the internal shape of `Store::mutate` | B:199-218, 359 | A's closed `Mutation` enum cannot express compaction/trim/manifest plans without growing into every view. This is the mechanism that actually satisfies criterion 5 and closes `log.rs:89, 231, 313`. |
| G4 | `BoundedReceipts` caps (max entries, max encoded bytes, sorted, duplicate-ID rejection at decode) applied to A's manifest `admitted` list | B:157, 395 | Closes A6; without it criterion 2 fails on a large batch. |
| G5 | Scripted failpoints at instruction boundaries + the crash-at-every-step matrix, run on memory / local / MinIO / S3 | B:376-397 | Verified: `FaultBackend` is probability-only. A's `fail_nth` is close; B's step list is the better matrix. |
| G6 | The seven GC preconditions as the acceptance gate for the later GC slice | B:437-447 | Strictly better than A's prose. **GC remains unresolved in both; nothing here changes that.** |
| G7 | `TrailLog` consumer trait shape (`ReadLimit` / `ReadPage` / `seek_ts` / `trim_before`) for A's R3 | B:405-423 | Keeps Pheromone off manifest layout and recovery steps. |
| G8 | Migration ordering + explicit rollback-forward statement, with the **seal replacing the capability barrier** (M2) | B:361-372, corrected | The barrier as written is unenforceable. |

### Keep from A, do not graft

The commit chain with skip pointers; the intent-as-cache demotion; `HeadSnapshot` (value and
version token from one read — this makes the `log.rs:295` pairing bug unrepresentable); the
`op_window` / `reader_grace` split; admission checkpoints instead of per-producer receipt keys;
the four-slice delivery split.

### R1 acceptance (unchanged in scope, plus grafts)

`comb-core` types; `Store::mutate` over memory + deterministic-fault backends; intents; commit
chain for plain refs; sealed v1→v2 migration; rewritten C4 (same-op retry returns generation *n*;
a different op returns *n+1*). Add from the grafts: G1, G2, G4, G5, and the chain-density
fail-closed check (A3). Sweeper contact stays the one additive root change A specifies, manual
and dry-run.

### Not resolved by this verdict

Safe general GC. Neither proposal solves the check/delete race, both say so, and choosing A does
not change that. G6 sets the bar the eventual design must clear. Physical reclaim in the interim
is limited to A's fenced-owner boundary with its three stated limits (A:217), which is a bounded
step and not a proof.
