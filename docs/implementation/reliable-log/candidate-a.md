# Comb reliable Log — candidate design A (ledgered refs)

Candidate design package for the retry-safe mutation / reliable Log program (reality check 2026-09-06, ranked moves 1–3; spec v0.3 §7.5, §7.5a, §8, §18.1, §19). Produced per the architect runner prompt; rationale follows `rationale-template.md`. No repository files were changed.

## Problem

Comb's linearization point is a conditional ref write (CAS) over an object backend. Today (`combctl/src/store.rs:123`, `log.rs:89`) no mutation carries an operation identity: a caller whose CAS reply is lost cannot learn whether it committed, and a retry re-executes (`drills.rs:93` even asserts generation 2 after a lost-reply retry — the behavior spec drill C4 and §18.1 forbid). The journal is written after the CAS through a contended shared head ref, and Log appends bypass it (`store.rs:231`, claim 9). The sweeper deletes while traversal reads fail and has no pins or barrier (`sweep.rs:62`, claim 8). Reads return the whole backlog as one `Vec` (claim 7). Constraints honored: the ref CAS stays the sole linearization point (§7.5); one guarded write manages target+lease atomically (§7.5 v0.3); 7-day operation records, explicit `UnknownOperation` after expiry, `IdempotencyConflict` on request mismatch (§18.1); resolution survives many intervening writes, concurrent same-ID calls, crashed writers; no unbounded per-ref receipt lists; no global write lock; object-before-publication ordering is preserved.

## Usage (caller's view)

```rust
use comb_core::{OperationId, Mutation};
use comb_store::Store;
use comb_log::{EventLog, Record, Position};

// 1. Core ref mutation, exactly-once. The caller persists only `op`.
let op = OperationId::new(clock.now());          // ULID: 48-bit ms timestamp + 80 random bits
let committed = store.mutate(op, Mutation::set_target("volume/vol_01/branches/main", digest)
                                     .with_fence(session.fence())).await?;
// Lost reply? Call again with the same `op`:
let again = store.mutate(op, /* same mutation */).await?;
assert_eq!(again.generation, committed.generation);   // same result, no second generation
assert!(!again.first_delivery);

// 2. Log producer with idempotent admission (Pheromone ingest path).
let appended = writer.append(vec![Record { op, payload }]).await?;
// Retry after ambiguous ack, possibly to a *new* leader after takeover:
let dup = writer2.append(vec![Record { op, payload }]).await?;
assert_eq!(dup.first, appended.first);                // original sequence, no duplicate event

// 3. Bounded replay (follower / matcher).
let mut pos = Position { partition: 0, seq: 1 };
loop {
    let page = log.read(pos, 4096).await?;            // flat memory, O(max) frames
    for (p, frame) in &page.frames { handle(p, frame); }
    if page.frames.is_empty() { break }
    pos = page.next;
}

// 4. History is a seek, not a scan (jj-op-log style).
let entries = store.history("volume/vol_01/branches/main", Some(from_gen), 100).await?;
```

Error surface the caller programs against: `UnknownOperation` (op ID older than the window — never silently re-executed), `IdempotencyConflict` (same ID, different request hash), `Fenced`, `LeaseHeld`, `Trimmed { resume_at }`.

## Shape

### Load-bearing decision: the ref carries its history head

Every logical ref update publishes, in the **same single CAS**, both the new state and the digest of an immutable **commit** recording who did it. The chain of commits is the per-resource journal, the retry ledger, and the audit trail at once. State and history cannot diverge because they are one write.

```rust
// ---------- comb-core (pure types, no IO) ----------

/// 128-bit ULID. The embedded timestamp is load-bearing: admission rejects
/// IDs older than `op_window` with UnknownOperation — expiry needs no tombstones.
pub struct OperationId(u128);
impl OperationId {
    pub fn new(now: DateTime<Utc>) -> Self;
    pub fn created_at(&self) -> DateTime<Utc>;
}

/// Tenant-keyed BLAKE3 of the canonicalized request body.
pub struct RequestHash(pub Digest);

/// Chain contract. Every object a ref's `head_commit` names embeds this
/// header — a bare Commit for plain refs, the partition manifest itself for
/// logs (no extra object on the log data path).
pub struct CommitHeader {
    pub resource: String,
    pub generation: u64,          // dense: parent is at generation-1
    pub epoch: u64,
    pub op: OperationId,
    pub request: RequestHash,
    pub parent: Option<Digest>,   // commit at generation-1
    pub skip: Option<Digest>,     // ancestor at generation - 2^k (largest 2^k | generation)
    pub at: DateTime<Utc>,
}

pub struct Commit {                       // "comb.commit/v1"
    pub header: CommitHeader,
    pub change: Change,                   // SetTarget{..} | Claim{..} | Release | Genesis{imported}
    pub result: OpResult,                 // what the caller was/will be told
}

pub struct RefValue {                     // "comb.ref/v2"
    /* v1 fields: tenant, name, generation, epoch, target, lease, updated_at */
    pub head_commit: Option<Digest>,      // None only pre-migration / journal refs
}

/// A ref read: value and provider token from the SAME read. CAS consumes the
/// snapshot whole — the value/token pairing bug class (log.rs:295 comment)
/// becomes unrepresentable.
pub struct HeadSnapshot { pub value: RefValue, pub version: Version }

pub enum CoreError { /* existing… */
    UnknownOperation(OperationId),
    IdempotencyConflict { op: OperationId },
}

/// Injected clock (Store and tests; deterministic expiry tests need it).
pub trait Clock: Send + Sync { fn now(&self) -> DateTime<Utc>; }
```

### Operation intent record — thin, mutable, time-swept

```rust
/// ops/<shard2>/<op_id>.json — the only new mutable key class. CAS-updated.
/// Never the source of truth for results (the chain is); it anchors crash
/// recovery, serializes same-ID callers, and caches the outcome.
pub struct OpIntent {                     // "comb.op-intent/v1"
    pub op: OperationId,
    pub resource: String,
    pub request: RequestHash,
    pub base_generation: u64,             // head generation this attempt CASes from
    pub proposed: Vec<Digest>,            // uploaded-but-unpublished objects (GC roots)
    pub state: IntentState,               // Pending | Applied { generation, commit, result }
    pub expires_at: DateTime<Utc>,
}
```

Bounded by construction: one record per operation, sharded by hash prefix, deleted after the window. No per-ref lists anywhere; no lock wider than one op record and one ref.

### The mutate protocol (exactly-once engine)

```rust
impl Store {
    /// Single entry point for every logical ref mutation.
    pub async fn mutate(&self, op: OperationId, m: Mutation) -> Result<Committed>;
}
pub struct Committed { pub generation: u64, pub epoch: u64, pub commit: Digest,
                       pub value: RefValue, pub first_delivery: bool }
```

1. **Age gate.** `op.created_at() < now − op_window + slack` → `UnknownOperation`. O(1), no storage, distinguishes "expired" from "fresh" without tombstones.
2. **Intent load.** Absent → read head, `put_create` intent `Pending{base = head.generation}`. Present with different `request` → `IdempotencyConflict`. Present `Applied` → return cached result (`first_delivery: false`). Present `Pending{base}` → **resolve** (step 4).
3. **Attempt.** Read `HeadSnapshot`; check fence/lease (`Fenced`/`LeaseHeld`); build the commit (+ view objects) against head; CAS the intent to `Pending{base = head.generation, proposed}`; upload proposed objects create-only; CAS the ref to the new value with `head_commit = commit`. Success → best-effort finalize intent to `Applied` (a lost finalize costs one seek on the next retry, never correctness) and return `first_delivery: true`. `PreconditionFailed` → step 4. Reply lost → backoff, then step 4.
4. **Resolve** from the intent's current `base`:
   - `head.generation == base`: undecided. Re-attempt **at the same base** with a freshly read token (an in-flight twin holds the same expected token; the backend admits at most one; both twins carry the same op, so either winning is the op committing once).
   - `head.generation > base`: seek the commit at `base + 1` — skip pointers give O(log n) hops from head. `op == ours` → committed; finalize and return. Foreign → the token at `base` is consumed, so no CAS of ours at that base can ever land; advance the base (intent CAS `base := head.generation`) and go to step 3.

**Serialization invariants.**
- **I1** — every ref CAS is issued at the intent's *current* base, using the version token read at that base, after a successful intent CAS recording that base.
- **I2** — the base advances b → h only after seeking b+1 and finding a *foreign* commit (which also witnesses that the token at b is consumed).
- **I3** — the intent record is CAS-guarded; a twin holding a stale intent version fails its intent CAS, reloads, and re-resolves before any ref CAS.

### Crash recovery proofs

**Lemma (decidability).** An attempt at base *b* CASes the token observed at generation *b*; if it commits, its commit's parent is the head-at-*b*, so its generation is exactly *b+1*. Backends guarantee strong read-after-write per key (§7.9), so a later read shows `generation ≥ b+1` iff it landed. "Did my attempt commit?" is decided by one head read and one O(log n) seek to *b+1*. Intervening writes lengthen the skip walk logarithmically; they never blur the verdict.

**Theorem (at most one commit per OperationId).** A commit with op X can only be created by a ref CAS issued under I1, i.e. at some base recorded in X's intent, landing at that base + 1 (lemma). The recorded bases form a strictly increasing sequence serialized by the intent's CAS (I3). By I2, the base advances past b only after generation b+1 is attributed to a *foreign* commit. Suppose two commits Cₓ at g₁+1 and Cᵧ at g₂+1 (g₁ < g₂) both carry X. Cᵧ requires recorded base g₂ ≥ g₁+1 > g₁; the advancement that moved the base past g₁ can only have advanced *from* g₁ (bases are exactly the generations at which attempts ran, and g₁ is one), and its I2 check attributed g₁+1 = Cₓ, found `op == X`, and returned "committed" instead of advancing. Contradiction. A stale twin firing at an old base's token cannot land either: once the base advanced past b, the token at b was witnessed consumed (I2), and tokens are never reissued. ∎

Resolution cost is therefore **O(log n) uniformly** — resolve never scans intervening commits; it seeks exactly one generation per recorded base, and I3 makes the live base unique.

**Crash matrix.** C-a after intent create, before uploads: record pending ≤ window; retry resolves "undecided" and proceeds. C-b after uploads, before ref CAS: objects are orphans *named by the intent* → GC-protected until expiry, then sweepable. C-c CAS sent, reply lost, process dies: any later same-op call (any client) resolves via the lemma; the result is served from the chain. C-d CAS committed, finalize lost: same as C-c; finalize is a cache fill. C-e crashed forever, no retry: intent expires; the ops sweeper resolves it against the chain, records the verdict, then deletes it; proposed orphans become sweepable. C-f concurrent same-ID twins: serialized by I3; twins at the same base race the same token — the backend admits one; both callers converge on the same `Committed`.

### Comb Log on the same engine

The partition manifest *is* the commit (embeds `CommitHeader`; parent = previous manifest): no extra object on the data path.

```rust
// ---------- comb-log ----------
pub struct LogManifest {                  // "comb.log.partition-manifest/v2"
    pub header: CommitHeader,
    pub head_seq: u64,
    pub admitted: Vec<(OperationId, u64)>,// this batch only — bounded by batch size
    pub wal: Vec<ChunkRef>,               // ≤ max_wal_chunks (overflow forces compaction)
    pub l1: Vec<SegmentRef>,              // recent segments inline, ≤ l1_max
    pub catalog: Option<Digest>,          // paged immutable SegmentCatalog for the deep past
    pub admission_checkpoint: Option<Digest>, // durable op→seq index up to some seq
    pub retired: Vec<Retired>,            // superseded/trimmed objects awaiting deferred deletion
    pub trim_before_seq: u64,
}
pub struct SegmentCatalog { pub segments: Vec<SegmentRef>, pub older: Option<Digest>,
                            pub span: (u64, u64) }        // each page ≤ ~1k entries
pub struct Retired { pub digest: Digest, pub retired_at: DateTime<Utc>, pub bytes: u64 }

pub struct WriterSession { /* log, WriterId, fence: Epoch, renewal task */ }
impl Log {
    /// Claim/steal leadership: one guarded write, epoch+1. Spawns the idle
    /// renewal loop (renew_interval), which rewrites lease_until only —
    /// no generation, no commit, not journaled (§7.5).
    pub async fn claim(&self, writer: WriterId, cfg: LeaseCfg) -> Result<WriterSession>;
}
impl WriterSession {
    pub async fn append(&self, batch: Vec<Record>) -> Result<Appended>;   // dedup + group commit
    pub async fn compact_if_due(&self) -> Result<CompactReport>;          // auto, post-commit
    pub async fn trim(&self, retention: &Retention) -> Result<u64>;
}
pub struct Record   { pub op: OperationId, pub payload: Bytes }
pub struct Appended { pub first: u64, pub last: u64, pub duplicates: Vec<(OperationId, u64)> }
```

Two idempotency layers, matching §8.8:

- **Writer-level (manifest CAS):** each group commit is one `mutate` with a writer-generated batch op. A lost final CAS reply resolves by the lemma; producers blocked on that batch are acked from the resolved manifest's `admitted` ranges.
- **Producer-level (admission):** frames carry the producer `OperationId`; the leader consults an in-memory `AdmissionIndex` (op → seq) before assigning sequences. **Rebuild is bounded by checkpoints, not the window:** compaction periodically writes an immutable op→seq index object and stamps `admission_checkpoint`; on takeover the new leader loads the checkpoint and scans only the tail since its seq. Rebuild cost = O(tail since last checkpoint), independent of partition age or heat. The producer dedup window is its own knob (`producer_dedup_window`, default = `op_window` per §18.1; operators may shorten it for hot partitions — a *documented deviation* from the 7-day default, traded against checkpoint size).
- **Fencing:** `WriterId = node/pid/nonce` (instance identity — a restarted process is a new writer; today's name-string comparison at `log.rs:107` would let a zombie twin write). Every manifest CAS carries the session fence; epoch mismatch → `Fenced`; the session poisons itself and stops (§7.6, drills L3/L7). Crashed-writer-mid-batch: producers see a channel error, retry to the new leader, the index answers.

**Bounded reads.** `read(Position, max)` locates the covering chunk/segment via inline `l1` spans, then catalog pages (each names its span — O(pages touched)); fetches only the needed objects; returns `Page { frames, next, trimmed_to }`. `follow` keys on `head_seq` (never the provider token, §8.10); compaction commits advance generation but not `head_seq`, so followers skip them for free. Positions ≤ `trim_before_seq` → `Trimmed { resume_at }`. The backend gains `get_range` (already in the spec §7.8 trait) so segment reads stop materializing whole objects — the L8 flat-memory path.

**Auto compaction and physical retention (§8.11, §19.5) — two decoupled windows.** The 7-day `op_window` governs *resolution metadata* only: intents, commit headers, and manifest objects (KB-scale). It does **not** pin data bytes. Data bytes are governed by `reader_grace` (default 1 h, configurable): when compaction supersedes chunks or trim drops a segment's span wholly below the floor, the writer moves their references to the manifest's `retired` list; after `reader_grace` it deletes them and drops the entries. A stale reader within `reader_grace` still resolves every reference (drill L5); an older reader may get `NotFound` on retired data and must refresh — `reader_grace` is the *documented staleness bound*, and readers needing more hold a pin (§7.7). Storage growth is therefore `retention floor + reader_grace` of data plus `op_window` of small headers — not seven days of superseded segments. Triggers: `wal.len() > max_wal_chunks || wal_bytes > threshold || oldest_wal_age > compact_after` → fold WAL into L1; `l1.len() > l1_max` → merge into catalog pages (today's code never merges segments — claim 4). Compaction publishes through `mutate`; races with appends resolve by CAS retry (drill L4).

### GC: safe initial boundary — and what is explicitly not solved

The check/delete race on a plain object store is not fully closed by this design, and two mechanisms from an earlier draft are **rejected as guarantees**:

- *Rejected:* post-CAS existence verification with republish. Between the ref CAS and the verify, the ref is visible while its object is missing; and a `SetTarget` caller holds only a digest — it cannot reconstruct the bytes at all. A guarantee that requires publishing first and repairing after violates "no visible ref target with missing referenced data" (§22.7) by construction.
- *Rejected:* late-intent reconciliation (sweeper re-reads intents after deleting and restores from quarantine) as a guarantee. It shrinks the window and heals most interleavings, but a reader can still observe `NotFound` between delete and restore, and the restore itself races the quarantine reaper. It is defense in depth, not a proof.

What this design does claim, as the **safe initial boundary**:

1. **Fenced-owner deferred deletion (automatic, safe).** The only unattended physical deletion is by the resource's fenced writer over objects it exclusively owns: log chunks/segments on its own `retired` list past `reader_grace`, and finalized op intents past expiry. Safety argument: the fence serializes all writers of the partition (I-epoch); retirement is published in the manifest *before* any deletion, so the set of deletable objects is itself crash-recoverable (a new leader resumes the list); nothing outside the partition may reference partition-owned WAL/segment objects (namespace rule §7.5a.1), so global reachability is not in question. This delivers physical reclaim for the dominant byte source (log data) without touching the open race.
2. **Global mark-and-sweep is advisory.** Sweeper v2 keeps: barrier-stamped run records, roots = refs + head commits + pins + all intents' `proposed` + legacy journal heads, and the rule that **any traversal read failure poisons the run — report, never delete** (fixes `sweep.rs:62`, drill G2/G5). Its output is a candidate report and optional *quarantine* (copy to `quarantine/<digest>`, then delete primary; digest-verified restore is possible), gated behind explicit operator invocation, dry-run by default — as today, but with the poisoning and root fixes. Hard deletion of quarantined objects requires a *second* full, healthy mark plus operator ack.
3. **By-reference mutations fail closed, before publishing.** `mutate` orders intent-CAS (naming `proposed`) → uploads → ref CAS. A mutation referencing an out-of-grace digest it did not upload must establish existence *pre-CAS*: `exists()` → else digest-verified restore from quarantine → else fail with `NotFound` before any ref movement. No ref ever becomes visible pointing at data the caller couldn't produce.

**Stated limits.** (a) A hard delete (operator-gated) can still race a concurrent by-reference publication in the exists()→CAS gap; the initial boundary avoids unattended hard deletes of by-referenceable objects precisely because this window is open. Closing it needs a capability the reference backends don't uniformly offer (conditional delete / object-lock / versioned buckets) or an epoch fence over the objects prefix — listed as an open question, not designed around. (b) Fenced-owner deletion trusts the config invariant `reader_grace > max reader page-fetch latency`; violated readers see `NotFound` and refresh. (c) Orphans from crashed mutations survive up to `op_window` + grace before reclaim — bounded, not immediate.

### Journal conventions (§7.5a.3, claim 9)

- **Per-resource journal = the commit chain.** Written atomically with the CAS — gap-free by generation density, and it includes Log publications, which bypass the journal today. `history(ref, from, max)` seeks the chain: the §7.5a.7 API without the 16-retry shared-head contention of `store.rs:231`.
- **Tenant shared scope** stays a derived Comb Log fed best-effort after the CAS (as today), now *repairable*: a follower detecting a per-ref generation gap backfills from that ref's chain. Cross-ref order remains a recording order, not causal (§7.5a.3).
- Renewals: no generation, no commit, not journaled. Journal refs: `head_commit = None`, never journaled — no recursion.

### Module ownership

```
comb-core    types only, no IO: OperationId, RequestHash, CommitHeader, Commit,
             RefValue v2, OpIntent, HeadSnapshot, errors, Clock
comb-object  ObjectBackend (+ get_range), memory / local / s3 / fault (+ deterministic
             fail_nth(key-pattern) triggers for drills)
comb-store   NEW crate: blobs, refs, the mutate() engine, chain walk/seek, intents,
             journal, pins, sweeper v2                      (moved out of combctl)
comb-log     NEW crate: EventLog trait, Position, WriterSession, GroupWriter,
             AdmissionIndex + checkpoints, compaction/retention planner
combctl      CLI shell only: parse → call → print
```

Call chain: consumer → comb-log → comb-store → comb-object (three files). Pheromone's `TrailLog` (§8.2) maps 1:1 onto `EventLog` + `WriterSession`; `SqliteLog`/`FsLog` implement the same trait later — the reusable consumer interface of reality-check move 3.

### Delivery slices (each lands alone)

- **R1 — idempotent mutations** (the first slice, separately implementable): `comb-core` types, `Store::mutate`, intents, commit chain for plain refs, lazy v1→v2 ref migration, rewritten C4 drill. GC contact is one additive change: today's sweeper adds intents' `proposed` to its roots (a few lines in `sweep.rs`), and it remains manual/dry-run — no dependency on quarantine, catalogs, or writer sessions. Closes claim 5 on Core refs.
- **R2 — log engine on mutate**: manifest v2 as commit, batch ops, producer admission + checkpoints, `WriterSession` fencing/renewal, group commit rebased on `mutate`. Closes claims 5 (appends) and 6. Depends on R1 types only.
- **R3 — bounded storage and reads**: catalogs, paged `read`, `get_range`, auto-compaction, `retired`-list deferred deletion, `reader_grace`. Closes claims 4 and 7. Depends on R2.
- **R4 — GC v2 (advisory)**: poisoned runs, pins, quarantine, by-reference fail-closed. Improves claim 8 within the stated boundary. Depends on R1; independent of R2/R3.

### Migration (§7.13: restartable, idempotent)

1. Additive schemas: `comb.ref/v2` adds optional `head_commit`; manifest v2 adds its new fields behind serde defaults (the `segments` field took this path already). Readers accept v1 and v2.
2. Lazy per-ref upgrade: the first `mutate` on a v1 ref writes a `Genesis { imported }` commit in the same CAS, preserving `generation`. No bulk rewrite; re-running is a no-op per ref.
3. `history` reads the chain first and falls back to the legacy hash-chained journal for pre-genesis generations; legacy journal objects retire under normal retention.
4. The ops admission window starts at deploy (the ULID age gate handles it naturally). Format-floor bump only after the fleet upgrades.

### Tests

- **Retry drills (replace `drills.rs:93`):** deterministic fault triggers (nth mutation on key pattern) — C4: lost ref-CAS reply, same-op retry returns generation *n*, not *n+1*; L2: fault exactly after manifest publication, producer retry returns the original range; a different op still gets *n+1*.
- **Protocol properties (chaos.rs extension):** seeded runs asserting "≤ 1 commit per OperationId" by chain scan, contiguous per-partition sequences, no ack without a committed manifest.
- **Concurrency:** two tasks, same op, barrier-released → one commit, identical `Committed`; 500 intervening foreign writes → correct verdict both ways with O(log n) fetch count asserted; stale-twin intent interleavings (I3).
- **Expiry:** mock `Clock`; day-6 retry → original result; day-8 → `UnknownOperation`; same-op-different-payload → `IdempotencyConflict`.
- **Writers:** L3 fencing via instance identity (not name equality); idle renewal across silent minutes; L7 skew; takeover loads the admission checkpoint and answers a dead writer's producer retries; rebuild cost bounded by tail length.
- **Storage:** auto-compaction at thresholds; L4 compactor/appender race; L9 `Trimmed`; catalog paging with 10k segments and flat read memory (scaled-down L8); `retired` deletion after `reader_grace`; a reader inside `reader_grace` never faults, one outside observes `NotFound` then recovers by refresh.
- **GC:** poisoned traversal refuses deletion; intents protect `proposed`; by-reference mutation to a missing digest fails closed pre-CAS; quarantine restore verifies digest; rerun idempotence (G3).
- **Conformance:** extend `comb-object/src/conformance.rs` with `get_range` and CAS-token semantics; memory/local in CI, MinIO/S3 nightly (§22.8).

## Synthesis decision

*(left for the orchestrator)*

## Tradeoffs accepted

- One small commit object per Core-ref write, **in exchange for** atomic gap-free history — net zero object count, since it replaces today's separately CAS'd journal entry; log batches embed the header and add no object.
- One intent create + best-effort finalize (two small writes) per operation, **in exchange for** O(log n) crash-anchored resolution, same-ID serialization, and GC anchoring of proposed objects; the log amortizes this per batch, not per event.
- ULID op-IDs trusting client clocks ± slack, **in exchange for** tombstone-free expiry and an O(1) `UnknownOperation` gate; a badly skewed client gets an explicit error, never silent re-execution.
- The `Applied` intent duplicates the result **as a cache** of the chain (derivable, reconcilable), **in exchange for** zero-seek hot retries — a deliberate exception to derive-don't-sync; losing the cache costs one seek.
- Readers staler than `reader_grace` may fault on retired data and must refresh (or pin), **in exchange for** data-byte retention decoupled from the 7-day resolution window — storage growth tracks the retention floor, not op history.
- Admission checkpoints add one index object per compaction cycle, **in exchange for** takeover rebuild bounded by the tail, not the window.
- Unattended physical deletion is limited to fenced-owner retired lists and expired intents, **in exchange for** never publishing-then-repairing; global GC stays advisory until the check/delete race has a real capability-backed solution.

## Alternatives considered

1. **Indexed operation registry as the mechanism (required comparison).** `ops/<shard>/<op_id>` records with a Pending→Applied state machine and results stored in the record; no chain. Wins on lookup (O(1) always, even for a client that lost all state) and on conceptual surface. Loses decisively on the hard case: after a lost CAS reply, *nothing can truthfully finalize the record* — the outcome is knowable only from history, and "re-read the ref and compare" collapses under intervening writes. Either the registry write becomes a second linearization point (the split-authority partial-failure zoo §7.5 v0.3 prohibits) or the registry silently grows a history dependency. It also duplicates results as truth rather than cache, and delivers no audit trail — §7.5a.3's journal would remain the racy post-CAS side channel it is today, leaving claim 9 open. Interface depth: same public surface, strictly less hidden capability. Verdict: the registry survives demoted to the thin intent/receipt cache **on top of** the chain — exactly where this design places it.
2. **Receipt window inside the ref value** (last K ops embedded in `RefValue`): O(1) and no extra keys, but K must cover the window on a hot ref (unbounded — prohibited), and every reader pays the fat-ref cost. Rejected.
3. **Pure chain, client-remembered base, no intent record:** zero extra writes; but a caller that crashes loses its base and must scan the window for its op, orphaned uploads have no GC anchor, concurrent same-ID callers have no serialization point (I3 evaporates), and cleanup has nothing to expire. Rejected as the whole story; retained as the underlying lemma.

## Open questions and risks

- Which certified backends offer conditional delete, object-lock, or versioned buckets that could actually close the global GC check/delete race — and is an objects-prefix epoch fence worth designing if none do uniformly?
- Is `reader_grace = 1 h` the right default staleness bound for followers, and should pins be required (rather than recommended) for analytical readers that lag further?
- `producer_dedup_window` shorter than the §18.1 7-day default on hot partitions: acceptable documented deviation, or must checkpoints be sized to hold the full window everywhere?
- Should maintenance commits (compaction, trim) share the data chain (proposed: yes — followers key on `head_seq` and skip them) or live on a parallel maintenance chain to keep logical history pure?
- Tenant shared-scope journal: keep the best-effort derived stream with gap repair, or push Phase C consumers to per-resource chains only?

## Next implementation step

Slice R1: add `OperationId`, `CommitHeader`, `RefValue` v2 and the new error variants to `comb-core`, implement `Store::mutate` against the memory+fault backend, and turn the rewritten C4 drill (same-op retry returns the original generation) red→green.
