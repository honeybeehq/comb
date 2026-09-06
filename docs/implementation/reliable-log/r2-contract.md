# R2 complete-feed integration contract

## Scope

R2 follows the accepted R1 publication engine. It adds the smallest set of capabilities that unblocks the Foundation bridge: bounded `head`, `read`, and long-poll `follow`; an ordered immutable chunk catalog; and renewable instance-owned writer sessions.

R2 does not add physical compaction, destructive GC, partitions, Comb Trees, Volumes, hosted tenancy, or bridge wire fields. Complete feeds remain untrimmed. A separate finite-feed test keeps `Trimmed` behavior honest.

R1 must first provide checked names for `StableKey`, `StableAppendReceipt`, `HeadSnapshot`, `RefMutationPlan`, and the persisted `Complete` mode. The sketches below use those names but do not create aliases around them.

## Caller usage

```rust
use combctl::log::{
    CallContext, CompleteFeed, Cursor, FollowWait, LeasePolicy, ReadLimits,
    WriterLabel,
};

let call = CallContext::new(deadline, cancellation.clone());
let feed = CompleteFeed::open(store.clone(), log_id, &call).await?;

// This creates a fresh random instance identity. The label is diagnostic only.
let writer = feed.writer_session(
    WriterLabel::try_from("foundation-bridge")?,
    LeasePolicy::bridge_default(),
)?;

// The method resolves an existing stable key before it asks for ownership.
// A fresh process can therefore resolve a committed retry while another
// instance still holds, or has taken over, the publication lease.
let receipt = writer.append_stable(stable_key, payload, &call).await?;

let limits = ReadLimits::try_new(128, 256 * 1024)?;
let page = feed
    .read_page(Cursor::first(0), limits, &call)
    .await?;

let followed = feed
    .follow_page(page.next, limits, FollowWait::try_new(Duration::from_secs(5))?, &call)
    .await?;
```

Foundation stays behind its JSONL v1 process boundary. The bridge maps hex bytes and decimal-string cursors to these types. The bridge team owns all wire field names.

## Public Rust contract

```rust
pub const MAX_PAGE_EVENTS: u32 = 1_024;
pub const MAX_PAGE_RAW_BYTES: u64 = 512 * 1024;
pub const MAX_FOLLOW_WAIT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct CallContext {
    pub deadline: tokio::time::Instant,
    pub cancellation: tokio_util::sync::CancellationToken,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Cursor {
    pub partition: u32,
    pub next_seq: u64,
}

impl Cursor {
    pub fn first(partition: u32) -> Self;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Position {
    pub partition: u32,
    pub seq: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct ReadLimits {
    max_events: NonZeroU32,
    max_raw_bytes: NonZeroU64,
}

impl ReadLimits {
    pub fn try_new(max_events: u32, max_raw_bytes: u64)
        -> Result<Self, InvalidReadLimit>;
}

#[derive(Clone, Debug)]
pub struct LogEvent {
    pub position: Position,
    pub committed_at: DateTime<Utc>,
    pub payload: Bytes,
}

#[derive(Clone, Debug)]
pub struct LogHead {
    pub generation: u64,
    pub head_seq: u64,
    pub next: Cursor,
    pub trim_before_seq: u64,
}

#[derive(Clone, Debug)]
pub struct ReadPage {
    pub events: Vec<LogEvent>,
    pub next: Cursor,
    pub snapshot_head: u64,
    pub raw_payload_bytes: u64,
    pub at_head: bool,
}

#[derive(Clone, Debug)]
pub struct FollowPage {
    pub page: ReadPage,
    pub timed_out: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct FollowWait(Duration);

impl FollowWait {
    pub fn try_new(wait: Duration) -> Result<Self, InvalidReadLimit>;
}

#[async_trait]
pub trait LogReader: Send + Sync {
    async fn head(&self, call: &CallContext) -> Result<LogHead, ReadError>;

    async fn read_page(
        &self,
        cursor: Cursor,
        limits: ReadLimits,
        call: &CallContext,
    ) -> Result<ReadPage, ReadError>;

    async fn follow_page(
        &self,
        cursor: Cursor,
        limits: ReadLimits,
        wait: FollowWait,
        call: &CallContext,
    ) -> Result<FollowPage, ReadError>;
}

impl CompleteFeed {
    pub async fn open(
        store: Arc<Store>,
        log: LogId,
        call: &CallContext,
    ) -> Result<Self, OpenLogError>;
}

pub enum OpenLogError {
    UnsupportedManifestSchema {
        found: String,
        required: &'static str,
    },
    Integrity(LogIntegrityError),
    Unavailable,
    DeadlineExceeded,
    Cancelled,
}

pub enum ReadError {
    Trimmed { requested: Cursor, resume_at: Cursor },
    InvalidCursor { requested: Cursor, next_at_head: Cursor },
    EventTooLarge {
        cursor: Cursor,
        position: Position,
        event_bytes: u64,
        max_bytes: u64,
    },
    Integrity(LogIntegrityError),
    Unavailable { operation: ReadOperation },
    DeadlineExceeded,
    Cancelled,
    InvalidLimit(InvalidReadLimit),
}
```

A cursor names the next event. Sequence 1 is the first event. An empty page leaves the cursor unchanged. If the first unread event exceeds `max_raw_bytes`, `read_page` returns `EventTooLarge` and leaves the cursor unchanged. After at least one event fits, the page stops before an event that would exceed either limit.

`cursor.next_seq == head_seq + 1` is the empty-at-head case. Zero or a value above `head_seq + 1` returns `InvalidCursor`; it never becomes a silent empty page.

`max_raw_bytes` counts payload bytes only. The bridge separately enforces its encoded JSON frame budget. Hex needs two encoded bytes per raw payload byte. The bridge advances only through events that fit its emitted frame, even if it uses fewer events than the returned Comb page.

## Ordered chunk catalog

R2 replaces the unbounded `LogManifest.chunks` and `segments` vectors with one immutable append-oriented B+tree root. This is an internal Log catalog, unrelated to the Comb Tree product.

```rust
pub const MAX_CATALOG_NODE_OBJECT_BYTES: u64 = 16 * 1024;
pub const MAX_CATALOG_ITEMS: usize = 32;
pub const MAX_CATALOG_HEIGHT: u8 = 8;

pub const MAX_CHUNK_EVENTS: u32 = 2_048;
pub const MAX_CHUNK_RAW_BYTES: u64 = 512 * 1024;
pub const MAX_FRAME_JSON_OVERHEAD_BYTES: u64 = 256;
pub const MAX_CHUNK_PLAINTEXT_BYTES: u64 = 3 * 1024 * 1024;
pub const MAX_CHUNK_OBJECT_BYTES: u64 = 4 * 1024 * 1024;

pub const COMPLETE_MANIFEST_SCHEMA: &str = "comb.log.partition-manifest/v3";
pub const CATALOG_NODE_SCHEMA: &str = "comb.log.chunk-catalog-node/v1";
pub const CHUNK_SCHEMA: &str = "comb.log.chunk/v1";

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum CompleteManifestSchemaV3 {
    #[serde(rename = "comb.log.partition-manifest/v3")]
    V3,
}

pub struct ChunkRef {
    pub digest: Digest,
    pub first_seq: u64,
    pub last_seq: u64,
    pub event_count: u32,
    pub raw_payload_bytes: u64,
    pub plaintext_bytes: u64,
    pub object_bytes: u64,
}

pub struct ChunkCatalogRoot {
    pub schema: CatalogSchemaV1,
    pub digest: Digest,
    pub height: u8,
    pub first_seq: u64,
    pub last_seq: u64,
    pub chunk_count: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum CatalogSchemaV1 {
    #[serde(rename = "comb.log.chunk-catalog-node/v1")]
    V1,
}

enum CatalogNode {
    Leaf {
        refs: BoundedVec<ChunkRef, MAX_CATALOG_ITEMS>,
    },
    Branch {
        height: u8,
        children: BoundedVec<CatalogChild, MAX_CATALOG_ITEMS>,
    },
}

pub struct CatalogChild {
    pub first_seq: u64,
    pub last_seq: u64,
    pub chunk_count: u64,
    pub digest: Digest,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum CatalogState {
    Empty,
    Root { root: ChunkCatalogRoot },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CompleteLogManifest {
    /// A required exact-literal field; only COMPLETE_MANIFEST_SCHEMA decodes.
    pub schema: CompleteManifestSchemaV3,
    pub header: CommitHeader,
    pub epoch: u64,
    pub head_seq: u64,
    pub retention: RetentionMode,
    pub trim_before_seq: u64,
    /// Required even for an empty feed; there is no missing-field default.
    pub catalog: CatalogState,
    pub stable_index: StableIndexRoot,
    pub stable_admissions: BoundedStableAdmissions,
    // R1 commit header and result fields remain unchanged.
}
```

`CompleteManifestSchemaV3` has a custom exact-literal decoder and `CatalogState` is either `Empty` or `Root(ChunkCatalogRoot)`. R2 rejects R1's `comb.log.partition-manifest/v2` vector manifest with `OpenLogError::UnsupportedManifestSchema`; it never derives a catalog from `chunks` or `segments`. Conversely, v3 omits the required v2 vectors and adds required catalog state, so an R1 decoder cannot write it. There is no `serde(default)`, compatibility fallback, automatic migration, or mixed-version writer mode at this boundary.

Leaf ranges and child ranges are sorted, non-overlapping, and contiguous. For a nonempty complete feed, the root covers `1..=head_seq`. An append path-copies the rightmost leaf and its ancestors. A full node creates a right sibling and propagates one bounded split upward. The manifest CAS publishes the new chunk-catalog root, the R1 stable-key root, the stable receipt, and the new `head_seq` together.

Catalog lookup descends by range. Iteration keeps at most one node per level and one chunk in memory. A missing node, an oversized node, a digest mismatch, a bad height, an unordered span, an overlap, or a gap above the retention floor returns `Integrity`. The reader never skips to the next valid-looking range.

The first complete-feed gate writes and reads chunks only. It rejects the existing whole-history `compact()` path for this manifest schema. General segments need authenticated block indexes before partial reads can be safe.

## Bounded object reads

The current `ObjectBackend::get` allocates the full object on memory, local, and S3 backends. Checking `payload.len()` after `get` does not enforce a memory limit. R2 adds this required method:

```rust
pub struct LimitedObject {
    pub bytes: Bytes,
    pub version: Version,
}

#[async_trait]
pub trait ObjectBackend: Send + Sync {
    async fn get_limited(
        &self,
        key: &str,
        max_encoded_bytes: NonZeroU64,
    ) -> Result<LimitedObject>;
}

pub enum EnvelopeFormatField {
    Version,
    Flags,
    Compression,
    Encryption,
    ObjectKind,
    Schema,
}

pub enum CoreError {
    ObjectTooLarge {
        key: String,
        limit: u64,
        actual: Option<u64>,
    },
    UnsupportedEnvelopeFormat {
        field: EnvelopeFormatField,
        value: String,
    },
    // Existing variants remain.
}
```

Each backend enforces the limit before an unbounded allocation:

- Memory checks the stored length before cloning.
- Local checks file metadata, then reads through `take(limit + 1)` and rechecks the result.
- S3 checks `content_length()`, then reads `ByteStream::into_async_read()` through `take(limit + 1)`. It never calls unbounded `collect()` on this path.
- `FaultBackend`, `FailpointBackend`, and `CountingBackend` implement and forward `get_limited`. Failpoints distinguish transient read failure from confirmed `NotFound`.

`Store::get_blob_limited` applies the same bound to its disk cache, decodes the full envelope, verifies the whole plaintext digest, and checks the object class's plaintext limit. Log ref, manifest, R1 HAMT, catalog-node, and chunk reads all use class limits. No R2 read path calls unbounded `get_blob`.

Limited envelope decoding accepts only the existing envelope version `1`, fixed-header `flags == 0`, metadata `compression == "none"`, metadata `encryption == "none"`, and the object kind and schema requested by the caller. An unknown version, nonzero flag, codec, kind, or schema returns typed `UnsupportedEnvelopeFormat`. Decode order is: enforce the encoded-object limit; parse the fixed header and at most 64 KiB of metadata; validate all format tags; verify the untransformed payload length and whole digest; only then deserialize the bounded manifest, catalog, or chunk payload. Unsupported payloads are never deserialized, decompressed, or decrypted.

The complete-feed reader fetches each chunk in full because the current envelope authenticates one digest over the whole payload. A backend byte range alone cannot authenticate a partial payload. A later segment format may add independently hashed blocks or a Merkle block map. R2 does not claim authenticated range reads.

Additional encoded-object limits are:

```rust
pub const MAX_REF_OBJECT_BYTES: u64 = 64 * 1024;
pub const MAX_MANIFEST_OBJECT_BYTES: u64 = 512 * 1024;
pub const MAX_STABLE_INDEX_NODE_OBJECT_BYTES: u64 = 8 * 1024;
```

Writers check count, raw bytes, serialized chunk bytes, and encoded envelope bytes before upload. Readers check the same fields against `ChunkRef`.

The chunk limits account for serde JSON's worst case for a byte vector, not an average or ASCII payload. At most four encoded bytes per raw byte, plus `256` bytes of fixed-schema framing per event and 1 KiB of body framing, gives `4 * 512 KiB + 2,048 * 256 + 1 KiB < 3 MiB`. Envelope v1 then adds at most 64 KiB of metadata and 12 header bytes, below the 4 MiB object cap. The current hex adapter is smaller, but the bound does not depend on it. The writer still measures the actual serialization and rejects an over-cap chunk before upload.

Peak reader memory is

```text
O(page raw-byte budget
  + MAX_CHUNK_OBJECT_BYTES
  + MAX_CATALOG_HEIGHT * MAX_CATALOG_NODE_OBJECT_BYTES
  + MAX_MANIFEST_OBJECT_BYTES)
```

The bound permits one encoded chunk and its decoded frames at once. The reader releases both before loading the next chunk. A bounded custom `ChunkBody` deserializer stops at `MAX_CHUNK_EVENTS + 1` and tracks aggregate decoded payload bytes during decode.

## Read and follow flow

`read_page` reads a bounded ref and manifest snapshot, checks the trim floor, then seeks the first catalog span whose `last_seq >= cursor.next_seq`. It loads and verifies one chunk at a time. Every emitted sequence must equal the cursor's expected sequence.

The page reports the head from its manifest snapshot. Concurrent later appends appear on the next call. If a transient backend error occurs, the reader retries at most eight times with jittered exponential backoff from 25 ms to 400 ms. Every backend await also races the absolute deadline and cancellation token. Exhausted retries return `Unavailable`; an elapsed deadline returns `DeadlineExceeded`. Neither result advances the cursor.

`follow_page` first calls `read_page`. If the page is empty at head, it waits for an in-process hint or polls the ref every 250 ms. Hints only wake the loop. The ref and manifest remain authoritative. Renewal changes the provider token but not the target or `head_seq`, so it does not produce an event. A normal wait timeout returns an empty page with the original cursor and `timed_out = true`. Backend failure never masquerades as a timeout.

These methods hold no consumer `State` lock across object I/O. Dropping the future is safe, and explicit cancellation returns `Cancelled`. A pending follow never blocks an append through the same process.

## Instance-owned writer sessions

```rust
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct WriterInstanceId([u8; 16]);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriterLabel(BoundedString<64>);

pub struct LeaseOwner {
    pub instance: WriterInstanceId,
    pub label: WriterLabel,
}

pub struct Lease {
    pub owner: LeaseOwner,
    pub lease_until: DateTime<Utc>,
}

pub struct LeasePolicy {
    pub ttl: Duration,
    pub renew_every: Duration,
    pub clock_slack: Duration,
    pub initial_acquire_budget: Duration,
}

impl LeasePolicy {
    /// 30 s TTL, 10 s renewal, 5 s clock slack, 45 s acquisition budget.
    pub fn bridge_default() -> Self;
}

pub enum WriterState {
    Unacquired,
    Acquiring { held_until: Option<DateTime<Utc>> },
    Active { epoch: u64, lease_until: DateTime<Utc> },
    Lost { epoch: Option<u64>, cause: SessionLoss },
    Closed,
}

pub enum SessionLoss {
    Fenced { live_epoch: u64 },
    OwnerChanged,
    RenewalUncertain,
    LeaseExpired,
}

pub enum LeaseError {
    LeaseHeld { owner: WriterInstanceId, until: DateTime<Utc> },
    Fenced { session_epoch: u64, live_epoch: u64 },
    ReacquireRequired { cause: SessionLoss },
    Unavailable,
    DeadlineExceeded,
    Cancelled,
    InvalidPolicy(InvalidLeasePolicy),
}

pub enum StableAppendError {
    StableKeyConflict { existing: Digest, supplied: Digest },
    Lease(LeaseError),
    Integrity(LogIntegrityError),
    Unavailable,
    DeadlineExceeded,
    Cancelled,
    InvalidInput,
}

pub struct WriterSession { /* feed, fresh instance, policy, watch state, task */ }

impl CompleteFeed {
    /// Generates `WriterInstanceId` from the OS RNG. Callers cannot supply it.
    pub fn writer_session(
        &self,
        label: WriterLabel,
        policy: LeasePolicy,
    ) -> Result<WriterSession, InvalidLeasePolicy>;
}

impl WriterSession {
    pub fn state(&self) -> WriterState;
    pub async fn ready(&self, call: &CallContext) -> Result<u64, LeaseError>;
    pub async fn append_stable(
        &self,
        key: StableKey,
        payload: Bytes,
        call: &CallContext,
    ) -> Result<StableAppendReceipt, StableAppendError>;
    pub async fn close(self, call: &CallContext) -> Result<(), LeaseError>;
}
```

The display label never establishes ownership. Each process start and each explicit reacquisition creates a new random `WriterInstanceId`. A live lease belongs only to the pair `(instance, epoch)`. This `Lease` replaces equality on the current `Lease.writer: String`.

The session starts in `Unacquired` and performs no storage I/O. `ready`, or an append whose key is absent, moves it to `Acquiring`. A bridge supervisor may call `ready` at process start. It may wait for an unrelated lease to expire within its 45-second lifecycle budget, then acquire through a fresh-head CAS that increments the epoch. It never steals a live unrelated lease. If the caller's deadline ends first, new appends return typed `LeaseHeld` or `DeadlineExceeded`. An administrative live takeover remains a separate CLI operation.

Policy validation requires `renew_every + clock_slack < ttl` and caps all four durations. The default 30-second TTL, 10-second interval, and 5-second slack match spec section 7.6.

After acquisition, a background task renews during idle periods. Every renewal reads a fresh `HeadSnapshot`, verifies both the instance and epoch, and CASes a value derived from that snapshot. It changes only `lease_until` and `updated_at`. Generation, target, `head_commit`, and both manifest roots remain unchanged. Ref-CAS conflicts trigger a fresh read and bounded retry.

If renewal cannot be confirmed before `lease_until - clock_slack`, the session enters `Lost` and stops publishing. A fence or owner mismatch enters `Lost` immediately. A lost session never silently reacquires. The caller creates a new `WriterSession`, which gets a new instance ID.

`close` stops the renewal task and attempts R1's guarded generic release within the call deadline. A failed close leaves the lease to expire. `Drop` only stops renewal and never blocks.

`append_stable` reads the R1 stable index before it checks session state. A committed retry returns its original receipt from `Unacquired`, `Acquiring`, or `Lost`. A successful lookup never starts acquisition. For an absent key, the method requires `Active`, then re-reads the head and verifies the live instance and epoch before it prepares the CAS. A renewal or another append may consume the snapshot token; the method then replans from the new head. It permits at most 32 replans and never runs beyond `CallContext.deadline`.

Foundation's stable key and payload hash exclude the session instance, label, epoch, and lease times. Foundation authorship is unrelated to the bridge process that owns publication.

## Implementation order

1. Land `ObjectBackend::get_limited`, limited cache reads, typed size errors, and conformance tests on memory, local, MinIO, S3, fault, failpoint, and counting wrappers.
2. Add the bounded chunk decoder and immutable chunk catalog with pure node tests. Use `32` items per node so `34` tiny chunks force a leaf split in live-backend tests.
3. Introduce the required v3 complete-feed manifest on a fresh, isolated prefix. Reject v2 vector manifests rather than converting them. Make append publish the catalog root and R1 stable root in one existing `RefMutationPlan` CAS. Reject complete-feed compaction and trim.
4. Add `head` and `read_page`. Remove the unbounded `read(from) -> Vec<Frame>` from the bridge-capable path. Make the CLI paginate internally if it keeps an all-output command.
5. Add `WriterSession`, idle renewal, fence loss, and bounded reacquisition. Replace writer-string equality in Log append with instance-and-epoch checks.
6. Add `follow_page` as bounded pull with hints and polling. Then enable the existing Foundation bridge capabilities without changing its wire contract.

Namespace isolation replaces a migration seal only when the R2 prefix is fresh and legacy binaries cannot address it. Otherwise R2 remains disabled until the tested seal is present. Manifest schema checks are a fail-closed backstop, not permission to mix R1 and R2 writers.

Module ownership stays narrow. `comb-object` owns `get_limited` and backend conformance. `comb-core` owns limited envelope decoding, durable lease-owner types, and portable errors. A new private `combctl::catalog` module owns the chunk tree. `combctl::log` owns `CompleteFeed`, the page types, and `WriterSession`; `publish.rs` remains the only ref-CAS path. Crate extraction is not part of R2.

## Focused tests

- Backend conformance reads an object at the exact limit and rejects one byte above it before cloning or collecting. Assert that every wrapper preserves the limit and error class.
- Catalog tests append `70` one-event chunks to force leaf splits and root creation; that fixture does not claim an internal branch split. A separate `MemoryBackend` test appends `1,025` one-event chunk refs, exceeding the `32 * 32` two-level capacity and forcing the first internal branch split. Seek every boundary and assert that every encoded node is at most `16 KiB`.
- Serialize both one `512 KiB` all-`0xff` event and `2,048` all-`0xff` events totaling `512 KiB`. Include maximum sequence, timestamp, separators, and field-name overhead; assert plaintext below `3 MiB` and envelope below `4 MiB`. Assert one extra raw byte, event, plaintext byte, or object byte is rejected before upload.
- Mutate envelope version, flags, compression, encryption, object kind, and schema independently. Assert `UnsupportedEnvelopeFormat` and prove the chunk/catalog payload decoder and any codec path were not invoked.
- Open an R1 v2 vector manifest through R2 and a v3 catalog manifest through R1. Both must fail as unsupported without a ref CAS. Also reject a v3 document missing catalog state or carrying legacy `chunks`/`segments` fields.
- Replay `257` events over many pages with count `7` and small byte budgets. Concatenated pages must equal `1..=257` without gaps or duplicates.
- Put an oversized event first. Assert `EventTooLarge` and the unchanged cursor. Put it after one fitting event and assert that the page stops before it.
- Read at zero and beyond `head_seq + 1`. Assert `InvalidCursor`, not an empty page.
- Corrupt a catalog digest, remove a node, oversize a node and a chunk, break a span, and reorder chunk frames. Each case returns `Integrity`, never a partial success.
- Inject one transient read failure and then recover. Exhaust eight attempts and assert `Unavailable`. Expire the deadline and cancel a follow; both leave the cursor unchanged.
- Keep a follow pending while another task appends. Assert that append completes and follow returns the event. Test a normal follow timeout separately.
- Start two sessions with the same label. Assert different instance IDs and `LeaseHeld` for the second while the first lease is live.
- Run idle renewal for three intervals with a mock clock. Assert a later lease deadline and byte-for-byte preservation of generation, target, and `head_commit`.
- Take over administratively. Assert that the old session becomes `Lost(Fenced)` and a new append requires reacquisition. Retry an already committed stable key through the lost session and assert the original receipt.
- Simulate process restart. The new instance waits for the old 30-second TTL, acquires within the 45-second budget, and never performs a live steal.
- Drop the final append-CAS reply, restart the bridge process, add intervening writes, and retry the same stable key. Assert one range and no requirement to own the current lease.
- On a disposable finite feed, advance the logical floor and assert `Trimmed { resume_at }`. Complete feeds reject the operation.
- Run append, one catalog split, paged replay, idle renewal, takeover, and lost-ack retry with `34` tiny chunks on both MinIO and S3 under fresh prefixes.
- In bridge acceptance, cross-check the Log page's event count and raw-byte total against the bridge's hex expansion and encoded-frame cap. The bridge advances only through emitted events.

## Deferred work and trade-offs

Fetching one whole bounded chunk repeats work when adjacent pages split a chunk, but it preserves full-envelope authentication. A cache can hide most repeats. Large authenticated segments and byte-range reads belong to a later format.

Path copying adds at most eight catalog-node uploads per chunk publication. Group commit amortizes that cost later. R2 does not need group commit for Foundation's single-append gate.

Failed CAS attempts may leave chunks and catalog nodes. R2 keeps destructive GC disabled, so these are a storage leak rather than a safety risk. A later GC design still needs a proof that closes the publication check-to-delete race.

The first gate remains one partition and one publication ref per logical Log. The neutral `Cursor`, `Position`, `LogEvent`, `ReadLimits`, and `LogReader` types can support a later Pheromone adapter without importing Foundation document types into Comb.
