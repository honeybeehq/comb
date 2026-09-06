# Comb reliable Log, candidate B

Date: 2026-09-06

Status: bounded architecture proposal, not an implementation plan for every later phase

## Recommendation

Build the first reliability slice around two mechanisms:

1. A tenant-scoped operation registry uses one create-only intent key and one create-only receipt key per operation ID. The intent reserves the ID across every operation kind and resource. It binds the ID to a canonical request hash.
2. Every logical ref CAS carries one digest-sized `pending_receipts` link. The linked immutable bundle contains the exact receipt or receipts produced by that CAS. A later logical writer must copy every receipt in the preceding bundle into the registry and verify them before it may advance the ref.

The ref CAS remains the only linearization point. Forward helping closes the crash window between that CAS and registry finalization. The ref never contains an accumulating receipt list. Different refs do not share a lock. A grouped Log commit carries one bounded bundle, not one field per producer.

This is the first slice. Bounded manifests, range reads, compaction, physical retention, writer renewal, journal cleanup, and the consumer crate are follow-on slices. Their interfaces are sketched below so this design does not block them. Automatic deletion remains disabled until a separate GC design closes the publication-versus-delete race.

## Grounded runtime flow

The current code has a sound publication kernel and no retry result recovery.

For an ordinary ref mutation, `Store::set_target` reads the ref and provider version, checks the fence, increments `generation`, conditionally writes the ref, then appends the journal (`crates/combctl/src/store.rs:123-149`). A lost reply from the conditional write returns an error even when the new generation is live. The retry reads that live generation and applies the request again. The existing C4 test explicitly expects generation 2 (`crates/combctl/tests/drills.rs:89-113`), contrary to specification section 18.1.

For a Log append, `LogStore::append` reads the ref and manifest, assigns the next sequence range, uploads a chunk and manifest, then conditionally publishes the manifest digest in the ref (`crates/combctl/src/log.rs:89-167`). Uploaded but unpublished objects are harmless orphans. Once the CAS succeeds, the append is visible. If its reply is lost, no durable operation identity says that the caller owns that sequence range. `GroupWriter` has the same gap and keeps the only per-submission mapping in oneshot senders in process memory (`log.rs:354-438`).

Compaction and trimming also write the Log ref directly (`log.rs:231-313`). Log ref writes bypass `Store::journal_append`. Reads fetch every selected object and return the whole backlog in a `Vec` (`log.rs:203-225`). The sweeper lists refs, treats them as its root snapshot, skips failed reachable-object reads, and then deletes (`crates/combctl/src/sweep.rs:37-97`). The skip at lines 62-64 and the lack of a publication barrier make destructive GC unsafe.

The backend gives us the needed first-slice operations: create-only PUT, conditional PUT, strong direct GET, and deterministic before/after fault injection (`crates/comb-object/src/backend.rs`, `fault.rs`). We do not need a transaction service or a global lock.

## Scope of the first slice

In scope:

- exact retry semantics for `set_target`, lease claim/release, maintenance ref publications, and Log append;
- seven-day operation records;
- `UnknownOperation` after expiry, including after registry cleanup;
- tenant-wide detection of one ID reused for a different operation or request;
- recovery after a lost final ref-CAS reply, arbitrary later writes, concurrent same-ID calls, and writer process death;
- grouped append receipt bundles with hard entry and byte caps;
- one internal ref mutation path so Log cannot bypass the protocol;
- targeted deterministic fault and concurrency tests.

Out of scope:

- multi-ref atomicity;
- automatic physical deletion;
- full manifest and segment rewrite;
- Pheromone's local `TrailLog` extraction, which is already underway;
- a hosted operation service or database.

## Usage from the caller

The caller creates an operation ID immediately before the first attempt and retries the identical typed request. Routing and writer-fence data are execution metadata, not part of the material request hash.

```rust
let request = SetTargetRequest {
    operation: OperationId::new_now(&mut rng, clock.now()),
    name: RefName::parse("refs/volume/vol_01/branches/main")?,
    condition: RefCondition::Generation(41),
    target: new_manifest,
};

let committed = refs.set_target(request.clone()).await?;
// A timeout or BackendUnavailable is retried with request.clone().
// It returns the original generation and target, even after later writes.
```

```rust
let request = AppendRequest {
    operation: OperationId::new_now(&mut rng, clock.now()),
    records: NonEmptyRecords::try_from(events)?,
};

let receipt = writer.append(request.clone()).await?;
assert_eq!(receipt.range.count(), request.records.len() as u64);
// A retry through another writer instance returns this exact range.
```

```rust
match writer.append(expired_request).await {
    Err(LogError::UnknownOperation { id, expired_at }) => {
        // Mint a new ID only after the caller decides whether a new append is wanted.
    }
    other => consume(other?),
}
```

The library must never mint a replacement ID during retry.

## Public and stored types

```rust
/// Opaque to callers. Encoding contains issued_at_ms plus 128 random bits.
/// Text form is `op_` plus canonical base32.
pub struct OperationId([u8; 24]);

impl OperationId {
    pub fn new_now(rng: &mut (impl RngCore + CryptoRng), now: Timestamp) -> Self;
    pub fn issued_at(&self) -> Timestamp;
    pub fn expires_at(&self, window: OperationWindow) -> Timestamp;
}

pub struct RequestHash([u8; 32]);

pub struct OperationIntent {
    pub schema: SchemaId,                 // comb.operation-intent/v1
    pub id: OperationId,
    pub tenant: TenantId,
    pub kind: OperationKind,
    pub resource: ResourceKey,
    pub request_hash: RequestHash,
    pub issued_at: Timestamp,
    pub expires_at: Timestamp,
}

pub struct OperationReceipt {
    pub schema: SchemaId,                 // comb.operation-receipt/v1
    pub id: OperationId,
    pub request_hash: RequestHash,
    pub resource: ResourceKey,
    pub committed_at: Timestamp,
    pub outcome: ReceiptBody,             // private tagged canonical bytes
}

pub struct PendingReceiptRef {
    pub bundle: Digest,
    pub entries: u16,
    pub encoded_bytes: u32,
}

pub struct RefValueV2 {
    pub schema: SchemaId,                 // comb.ref/v2
    pub tenant: TenantId,
    pub name: RefName,
    pub generation: Generation,
    pub epoch: Epoch,
    pub target: Option<Digest>,
    pub symref: Option<RefName>,
    pub lease: Option<Lease>,
    pub pending_receipts: Option<PendingReceiptRef>,
    pub updated_at: Timestamp,
}

pub struct ReceiptBundle {
    pub schema: SchemaId,                 // comb.receipt-bundle/v1
    pub resource: ResourceKey,
    pub physical_commit: CommitId,
    pub entries: BoundedReceipts,         // sorted by OperationId
}

pub struct ReceiptCandidate {
    pub intent: OperationIntent,
    pub receipt: OperationReceipt,
}
```

`BoundedReceipts` has a private constructor and enforces both `MAX_RECEIPTS_PER_COMMIT` and `MAX_RECEIPT_BUNDLE_BYTES`. Initial values should match the existing group writer ceiling, at most 2,048 receipts and 512 KiB encoded. A producer batch containing many events still has one operation and one receipt. The group writer closes a physical batch before either cap.

New portable errors:

```rust
pub enum CoreError {
    UnknownOperation { id: OperationId, expired_at: Timestamp },
    IdempotencyConflict {
        id: OperationId,
        original: RequestHash,
        supplied: RequestHash,
    },
    OperationRegistryUnavailable(String),
    // existing variants remain
}
```

Public operations stay small:

```rust
#[async_trait]
pub trait RefStore: Send + Sync {
    async fn get(&self, name: &RefName) -> Result<Option<RefSnapshot>>;
    async fn set_target(&self, request: SetTargetRequest) -> Result<RefCommit>;
    async fn claim(&self, request: ClaimRequest) -> Result<LeaseGrant>;
    async fn release(&self, request: ReleaseRequest) -> Result<RefCommit>;
}

#[async_trait]
pub trait OperationRegistry: Send + Sync {
    async fn admit(&self, intent: &OperationIntent) -> Result<Admission>;
    async fn receipt(&self, id: OperationId) -> Result<Option<OperationReceipt>>;
    async fn finalize(&self, pending: &PendingReceiptRef) -> Result<()>;
}

pub enum Admission {
    New,
    Pending(OperationIntent),
    Complete(OperationReceipt),
}
```

Only the internal `RefCommitter` may call `ObjectBackend::put_update` for a ref:

```rust
impl RefCommitter {
    pub(crate) async fn execute<P: RefMutationPlan>(
        &self,
        intent: AdmittedIntent,
        plan: &P,
    ) -> Result<P::Receipt>;

    async fn help_predecessor(&self, snapshot: &RefSnapshot) -> Result<()>;
    async fn recover<P: RefMutationPlan>(
        &self,
        intent: &AdmittedIntent,
        plan: &P,
    ) -> Result<Recovery<P::Receipt>>;
}
```

`RefMutationPlan::prepare` receives one observed ref and produces immutable uploads, the next domain value, and typed receipt candidates. It may be run more than once. It cannot issue the ref CAS itself.

## Storage state

```text
comb/v2/tenants/<tenant>/
  refs/<view>/<resource>/<name>.json       RefValueV2, mutable only by CAS
  operations/by-id/<00..ff>/<op>.intent   create-only, canonical and authenticated
  operations/by-id/<00..ff>/<op>.receipt  create-only, canonical and authenticated
  operations/finalized/<bundle>.marker    create-only helping-completion certificate
  objects/b3k/<prefix>/<digest>            immutable objects and receipt bundles
```

The operation shard is selected from the operation ID hash. It affects maintenance listing only. Admission and lookup use direct known-key GETs, so stale listing cannot change correctness. Intent and receipt cleanup uses `expires_at`; a stale listing delays cleanup.

The logical state machine is derived from durable facts:

```text
Fresh       no intent, ID still admissible
Intent      intent exists, receipt absent, no matching current pending bundle
Published   current ref points to a matching pending bundle
Complete    direct receipt exists and matches the intent
Expired     now >= ID expiry, always returns UnknownOperation
```

`Published` is already committed. Registry finalization changes lookup speed, not the operation outcome.

## Exact expiry semantics

A bounded implementation cannot both accept arbitrary timeless IDs and remember every expired ID forever. A Bloom filter or rotating tombstone can forget an old ID and silently execute it again. That is disallowed.

The first slice therefore uses time-bearing, 192-bit operation IDs. The timestamp is internal to `OperationId`; callers still treat the value as opaque. The library mints it immediately before first use. A new ID must be admitted within a small configured first-use allowance. Existing IDs remain retryable until `issued_at + 7 days`. At or after that instant, every mutation and status API returns `UnknownOperation` before admission or request comparison. Registry records may then be deleted without making the ID reusable. IDs too far in the future are invalid.

Expiry wins over conflict. Reusing an expired ID with different bytes returns `UnknownOperation`, not `IdempotencyConflict`. Before expiry, the original intent is retained and a different tenant-local operation kind, resource, condition, or canonical payload returns `IdempotencyConflict`.

The canonical request hash includes semantic input only. For Log append it includes tenant, log, partition, ordered event IDs and bytes. It excludes sequence allocation, commit timestamp, current writer instance, lease epoch, route, and timeout. For ref mutation it includes the portable logical condition and requested change. It excludes the provider version token.

## Forward-helping protocol

### Admission

1. Validate ID time and typed request boundaries.
2. Compute the domain-separated canonical request hash.
3. Create the intent at its deterministic key.
4. On `AlreadyExists` or an ambiguous create reply, read that exact key. Return the stored receipt if present. Continue if the intent matches. Return `IdempotencyConflict` if it does not.

The intent key is tenant-wide, so the conflict rule crosses operation kinds and resources without a tenant-wide CAS.

### Commit

1. Read the resource ref and provider version together.
2. If `pending_receipts` is present, fetch and verify the bundle. Write each missing deterministic receipt key using create-only PUT. Existing receipts must match byte for byte. Write the finalized marker last. Do not proceed if any unexpired receipt cannot be made durable.
3. Check whether this operation is now complete. If so, return its stored result.
4. Check the portable ref condition and writer fence. Build the next immutable target and a bounded receipt bundle from the observed state.
5. Upload every referenced immutable object and verify availability. The final ref body increments the logical generation, applies the target and lease fields atomically, and replaces `pending_receipts` with the new bundle link.
6. Conditionally write the ref with the observed provider version.
7. On known precondition failure, reload and repeat.
8. On any ambiguous write error, enter recovery. Never issue another ref CAS until recovery performs direct reads.

The implementation may acknowledge once the ref CAS is known committed because the current ref durably roots the exact receipt bundle. It should finalize the new bundle in parallel with acknowledgements and must finish finalization before allowing its next logical ref CAS. A simpler first implementation may finalize before acknowledgement. Benchmark both choices.

Lease renewals and optional clearing of an already finalized pending link are non-logical CAS operations. They preserve the link byte for byte unless its finalized marker is known durable. They do not advance `generation` and are not journaled. A finalized marker remains while its bundle may still be the live pending link. Once the ref advances, the old marker is only cleanup metadata because the direct receipts are already durable.

An idle ref may still point to a pending bundle when its operations expire. Expiry does not let a successor skip helping. The helper verifies the bundle, confirms every entry is expired, writes an expiry-finalized marker, and only then advances. It does not recreate a caller-visible receipt. Calls using those IDs still return `UnknownOperation` because the time check runs first.

### Recovery

Recovery reads the direct receipt, then the current ref, then the direct receipt once more before deciding an operation is still pending.

- A matching receipt returns its exact typed outcome.
- A current pending bundle containing the ID proves the ref CAS committed. Recovery finalizes the bundle and returns its receipt.
- If the current pending bundle is different and the second direct receipt read is absent, the operation was not committed before the observed ref version. It may race with a live same-ID attempt, but the next ref CAS either loses to that attempt or wins once. Reload handles both.
- If a required direct read is unavailable, return `OperationRegistryUnavailable` or `BackendUnavailable`. Do not guess and do not mint a new ID.

## Why the crash cases close

The proof rests on four invariants.

1. One create-only intent key permanently binds an unexpired ID to one material request.
2. A logical ref CAS publishes the domain change and its receipt bundle link in the same atomic value.
3. Every logical successor finalizes and verifies its predecessor before replacing that link.
4. No path removes an unexpired intent or receipt.

From those invariants:

- A crash before intent creation leaves no operation.
- A crash after intent creation but before ref CAS leaves `Intent`; retry may rebuild immutable orphans and attempt the CAS.
- A crash after chunk or manifest uploads leaves unreachable objects if the CAS did not happen.
- A request lost before CAS leaves no matching pending bundle or receipt, so retry may apply once.
- A CAS applied with its reply lost leaves the matching bundle in the ref. Recovery returns its recorded generation or Log range.
- Partial receipt finalization is harmless. Helpers use create-only writes and leave the pending link in place until every receipt is durable.
- If many logical writes followed, the first successor had to finalize the receipt. Direct lookup therefore finds it.
- Concurrent equal requests with the same ID may upload competing immutable candidates, but only one ref CAS wins. Losers reload and return the winner's receipt.
- Concurrent different requests with the same ID stop at the intent comparison. At most the request that created the intent can reach the ref CAS.
- A writer that wakes after takeover has an old provider version and epoch. Its CAS fails; recovery returns `Fenced` unless the operation had committed before takeover.

This gives at-most-one logical mutation and a stable result inside the window. It does not claim exactly-once external side effects.

## Grouped Log append

`GroupWriter` admits each submission before batching. It resolves completed IDs immediately, coalesces duplicate same-ID submissions onto one acknowledgement, rejects duplicates with a different hash, then groups only new intents.

One physical chunk and manifest assign disjoint ranges to all new submissions. One immutable receipt bundle records each operation ID and its assigned range. The ref CAS publishes the manifest and bundle together. A dead group writer loses only in-memory acknowledgements; any process can recover each result.

Forward helping adds up to one create-only receipt write per producer operation after a physical commit. Writes can run with bounded concurrency, and the finalized marker makes the next predecessor check one GET in the healthy path. This cost is real. The slice is accepted only if a sustained benchmark shows it meets the agreed operation rate or establishes that callers must batch more events per operation. Skipping finalization, overwriting the pending link, or silently falling back to event payload comparison are not acceptable optimizations.

## Immutable commit-history alternative

The main alternative makes each ref point to an immutable commit chain. Each node contains operation ID, request hash, old and new logical state, receipt, and parent digest. The ref CAS publishes state and chain head atomically.

That shape handles the immediate crash window, and it could become the journal source. It loses here for three reasons:

- Tenant-wide cross-request conflict still needs the separate intent reservation. A chain local to resource A cannot detect the same ID used on resource B.
- Recovery after many intervening writes is linear in commit count unless the design adds an index. A hot Log can have millions of commits inside seven days.
- Retaining chain nodes can retain old manifest identities and complicate physical retention. Adding bounded time buckets and a persistent lookup index recreates a second substantial subsystem.

Forward helping keeps only one bundle link in the live ref and gives O(1) direct receipt lookup after the next commit. It exposes one cost instead: receipt finalization sits between logical commits. That is easier to model, fault-inject, and measure in the first slice.

A `last_operation` field without forward helping also loses. Once a later write overwrites it before the registry receipt is durable, exact recovery is impossible. A seven-day array in each ref is rejected because its serialized size grows with write rate.

## Module ownership

```text
comb-core
  OperationId, RequestHash, timestamps, RefValueV2, PendingReceiptRef,
  typed portable errors, fence and writer-instance types

comb-object
  ObjectBackend and certified semantics, deterministic fault schedules;
  later range and conditional reads

comb-store                 new library crate
  object envelopes, OperationRegistry, RefStore, RefCommitter,
  canonical request/receipt codecs, journal consequence dispatcher

comb-log                   new library crate
  Log request and receipt types, manifest/chunk/segment formats,
  grouped writer, reader, follower, maintenance policy

combctl
  argument parsing and display only; no raw ref CAS
```

`comb-core` stays backend-free. Registry wire records remain private to `comb-store`. `comb-log` supplies a mutation plan to the crate-private committer, so it cannot bypass admission, helping, fencing, or journaling. This replaces the three current raw Log ref-write sites.

## Migration

1. Add deterministic clock and fault schedules, then land the new tests against the old implementation as expected failures.
2. Add v2 readers and registry record codecs. Keep v1 emission at first.
3. Deploy every writer with v2 preservation support. An old writer would deserialize an unknown pending field and write it back without that field, so mixed writes are unsafe.
4. Flip a tenant writer-capability barrier. After this point, v1 binaries cannot mutate refs.
5. CAS-migrate each live ref to `comb.ref/v2` with no pending bundle. Register existing refs in the future authoritative root catalog. Migration is restartable and does not change the target.
6. Require `OperationId` on external ref mutations and Log append. `combctl` accepts `--operation` and otherwise generates and prints an ID before sending the request. No pre-cutover request gains retroactive idempotency.
7. Route `set_target`, claim/release, Log append, compaction publication, and trim publication through `RefCommitter`. Lease renewal preserves pending state.
8. Convert a v1 Log manifest to v2 only when the later bounded-manifest slice is ready. The idempotency slice can read and publish the current format.

No repository format is rewritten in place. Rollback after the capability barrier means rolling back to a v2-aware binary, not to the current code.

## Tests and acceptance

Use a deterministic clock and scripted failpoints. The current probability-only `FaultBackend` cannot target the decisive instruction boundary.

First-slice model and property tests:

- same ID and same ref request, CAS reply lost, exact original generation returned;
- same ID and same append, manifest-ref CAS reply lost, exact original sequence range returned;
- 100,000 intervening logical writes, then retry the first ID;
- 100 concurrent calls with the same ID and bytes produce one generation or one sequence range;
- concurrent same ID with two payloads produces one winner and only `IdempotencyConflict` for the other request;
- same ID reused across a ref mutation and a Log append conflicts;
- crash at every step: intent create, each immutable PUT, predecessor finalization, ref CAS, each receipt PUT, finalized marker, acknowledgement;
- partial grouped-bundle finalization resumes without a second append;
- predecessor registry outage prevents the next logical CAS;
- lease renewal racing helping preserves the bundle;
- writer A pauses, writer B takes a higher epoch, A resumes and cannot publish;
- two processes with the same display name but different `WriterInstanceId` cannot co-lead;
- at `expiry - 1 tick` the result resolves; at `expiry` and later it is `UnknownOperation` even after intent and receipt keys are deleted;
- an idle ref whose pending bundle crosses expiry is expiry-finalized before its next logical write, without making the old IDs resolvable again;
- a never-used expired ID is also `UnknownOperation`; no probabilistic tombstone participates;
- stale operation-key listing delays cleanup but cannot affect admit, conflict, resolve, or expiry;
- bundle decoding rejects excess entries, excess bytes, duplicate IDs, mismatched resource, or mismatched hash.

Run the same C4 and L2 cases on memory, local filesystem, MinIO, and S3. The two live backends already pass the existing conformance suite, so this slice adds targeted ambiguous-response and direct-read checks rather than reopening backend selection.

Acceptance is zero duplicate logical operations, exact receipt equality on every retry inside seven days, constant serialized ref size, no tenant-wide CAS, and measured group-commit overhead. Existing `cargo check --workspace`, tests, and formatting remain required gates.

## Follow-on Log shape

These signatures reserve the consumer boundary without pulling later work into this slice:

```rust
pub struct Position { pub partition: u32, pub seq: u64 }
pub struct ReadLimit { pub max_records: NonZeroU32, pub max_bytes: NonZeroU32 }
pub struct ReadPage {
    pub records: Vec<(Position, Envelope)>,
    pub next: Position,
    pub head_at_read: Position,
}

#[async_trait]
pub trait TrailLog: Send + Sync {
    async fn append(&self, request: AppendRequest) -> Result<AppendReceipt>;
    async fn head(&self) -> Result<Vec<Position>>;
    async fn read(&self, from: Position, limit: ReadLimit) -> Result<ReadPage>;
    async fn seek_ts(&self, partition: u32, at: Timestamp) -> Result<Position>;
    fn follow(&self, from: Position) -> BoxStream<'static, Result<FollowEvent>>;
    async fn trim_before(&self, request: TrimRequest) -> Result<TrimReceipt>;
}
```

The object-backed manifest should cap live WAL descriptors at 256 and point to an immutable paged segment catalog. Catalog nodes and segment indexes have encoded byte caps. `read` fetches at most the requested records/bytes plus one bounded compressed block. This requires `ObjectBackend::get_range` and seekable independently compressed segment blocks. Soft compaction starts by age or WAL count; the hard cap applies backpressure rather than growing the manifest. Leveled merging updates the segment catalog through the same ref committer.

Writer sessions use random `WriterInstanceId`, not a user label. `WriterFence` contains instance ID and epoch. A writer handle renews at one third of its TTL even while idle, stops intake when its safe local deadline passes, and maps an epoch change to `Fenced`. Inject the clock for L3 and L7.

Ref journal entries include the source operation ID and receipt digest. They append after the source CAS with a deterministic journal event ID. Lease renewals and journal refs remain excluded. Per-resource scope is the default; a shared namespace scope is an explicit serialization choice, never a tenant-wide default. Journal failure cannot cause a second source mutation. A stronger gap-free audit mode needs its own durable outbox decision and is not smuggled into this slice.

## GC and physical retention are separately unresolved

This proposal makes no claim that rechecking refs immediately before deletion is sufficient. There is a check/delete race: a writer can publish an old object after the check and before delete. Reuploading or verifying an object after the ref CAS does not fix that race and is rejected as a proof.

The idempotency slice does not enable automatic physical deletion. Receipt bundles are ordinary immutable objects rooted by the live ref until registry finalization. The existing destructive sweep path should not be broadened.

Before physical retention ships, a separate design must prove all of the following:

- GC captures refs and pins through an authoritative, directly readable catalog, not a possibly stale list.
- A reader pins an old manifest before relying on it beyond the unpinned grace interval.
- Any publication that can make an old, previously unreachable graph live acquires a pin or passes a write barrier before its ref CAS.
- The collector has a sealing handshake with publishers, so no qualifying pin or publication can enter between final mark and delete. A two-phase `Marking` then `Sealed` epoch with fixed pin shards is one candidate, not an accepted design.
- New objects remain protected by a conservative age grace, but grace is supplementary rather than the publication proof.
- Any required root, manifest, bundle, pin, or child read failure aborts deletion.
- Deletion plans are immutable, tombstoned, bounded, and replayable after partial failure.

Only after L5 and G1 through G4 pass under deterministic races should trim remove physical descriptors and automatic GC delete their objects.

## Tradeoffs accepted

- We accept one intent write per external operation and one bounded receipt-finalization wave per physical commit in exchange for exact recovery without a global lock or growing ref.
- We accept a brief per-resource stall when its predecessor cannot be finalized in exchange for never overwriting the only crash-recovery evidence.
- We accept time-bearing operation IDs and a first-use allowance in exchange for exact bounded expiry without permanent tombstones.
- We accept a v2 writer barrier in exchange for preventing old binaries from dropping the pending link.
- We accept that this slice does not solve automatic deletion, bounded replay, or Pheromone integration in exchange for making the next implementation small enough to model exhaustively.

## Candidate status

This is one architect candidate, so it makes no synthesis decision. The caller should cross-judge it against the other structurally distinct design. Its distinguishing bet is that bounded forward helping is cheaper to reason about than a retained per-resource commit index, while its extra registry writes remain acceptable after batching and bounded parallelism.

## Open questions for implementation review

- Should acknowledgement wait for registry finalization, or should it return after the ref CAS while the writer gates only its next CAS? Both are correct; measure latency and recovery load.
- Are 2,048 receipts and 512 KiB the right initial bundle caps for the live MinIO and S3 request envelope?
- Is seven-day expiry anchored to locally minted `issued_at` acceptable for every SDK, or should a service issue operation IDs?
- Which terminal validation failures, if any, deserve stored negative receipts in the first slice?
- Does journal completeness remain best-effort per v0.3, or will a later milestone require a durable per-resource outbox?

## Next implementation step

Build the deterministic operation ID, intent/receipt codec, and a model-only `RefCommitter` state machine with the C4 and L2 lost-reply tests before changing the current Store or Log paths.
