# R2 checked implementation handoff

Checked against `52c1963aa8ab52deceaa6b5cae04b26c9b24970f` on `feat/reliable-log-r1`. The R2 worktree starts at `052ca112c65077c9eca150e4ba285108b1cfb541`, which contains `52c1963` plus bridge and regression-test changes. The accepted `/tmp/comb-r2-contract.md` matches the committed `docs/implementation/reliable-log/r2-contract.md` byte for byte.

The R2 design fits the R1 publication engine. Several sketch names do not match the code, and the v3 manifest must retain four R1 recovery fields that the sketch abbreviates. Phase 1 can proceed without resolving the later lease and namespace choices.

## Immediate phase 1

Do not edit `crates/combctl/src/log.rs` or `crates/combctl/src/publish.rs` until the retained-target follow-up is cherry-picked. Phase 1 owns these exact seams.

### `comb-object`

Extend `comb_object::backend::ObjectBackend`. Keep the existing tuple and `Vec<u8>` convention instead of adding the sketch-only `LimitedObject` type.

```rust
async fn get_limited(
    &self,
    key: &str,
    max_encoded_bytes: NonZeroU64,
) -> comb_core::error::Result<(Vec<u8>, Version)>;
```

Implement it in `MemoryBackend`, `LocalBackend`, `S3Backend`, `FaultBackend`, `FailpointBackend`, and `CountingBackend`.

- `MemoryBackend` checks the stored length while holding `state`, before `Vec::clone`.
- `LocalBackend` checks file metadata, then reads through `take(limit + 1)` to catch growth and stale metadata.
- `S3Backend` rejects a declared `content_length` above the limit, then reads `body.into_async_read()` through `take(limit + 1)`. Missing or false length metadata cannot bypass the stream limit.
- Add `FailMethod::GetLimited`. `CountingBackend::gets` counts both read methods unless a separate counter is useful to a focused test.
- Add exact-limit and one-byte-over tests to `comb_object::conformance::run`. Keep `get`; R1 callers still use it.

Add these portable errors in `comb_core::error::CoreError`:

```rust
ObjectTooLarge {
    key: String,
    limit: u64,
    actual: Option<u64>,
},
UnsupportedEnvelopeFormat {
    field: EnvelopeFormatField,
    value: String,
},
```

### `comb-core`

Add a checked policy to `comb_core::envelope`, with public re-exports from `comb_core::lib`:

```rust
#[derive(Clone, Copy)]
pub struct EnvelopeReadSpec<'a> {
    pub tenant: &'a str,
    pub kind: ObjectKind,
    pub allowed_schemas: &'a [&'a str],
    pub max_encoded_bytes: NonZeroU64,
    pub max_plaintext_bytes: NonZeroU64,
}

impl Envelope {
    pub fn decode_limited(
        bytes: &[u8],
        key: &DigestKey,
        spec: EnvelopeReadSpec<'_>,
    ) -> Result<Self>;
}
```

`decode_limited` checks the encoded length before parsing. It then accepts only magic `COMB`, version `1`, flags `0`, at most 64 KiB of metadata, the requested tenant and `ObjectKind`, an allowlisted schema, `compression == "none"`, and `encryption == "none"`. It checks the declared plaintext length and digest before a caller deserializes the payload. Unsupported flags or codecs never reach a payload decoder or codec path. Keep `Envelope::decode` for R1 compatibility, but make it reject nonzero flags and unsupported compression or encryption too.

R1 writes Log chunks and manifests as `ObjectKind::Blob`, not `ObjectKind::Manifest`. Their envelope schemas are `comb.log.chunk/v1` and `comb.log.partition-manifest/v2`. `Store::put_blob`, including current HAMT nodes, uses `ObjectKind::Blob` with envelope schema `comb.object/v1`; the node's own schema remains inside its JSON payload.

### `combctl::store`

Add the bounded sibling to the current `Store::get_blob`:

```rust
pub(crate) async fn get_blob_limited(
    &self,
    digest: &Digest,
    spec: EnvelopeReadSpec<'_>,
) -> anyhow::Result<(Vec<u8>, GetSource)>;
```

Add a private `cache_read_limited`. It must open and read at most `max_encoded_bytes + 1`; `std::fs::read` is not bounded. Both cache and backend paths call `Envelope::decode_limited` and require `env.meta.digest == digest`. Quarantine malformed cache entries as `get_blob` does. Do not replace broad R1 call sites during phase 1.

## Later Log and publication work

Cherry-pick the retained-target follow-up first. Then use these existing R1 hooks rather than creating a second publication engine:

- `crate::publish::{HeadSnapshot, RefMutationPlan, PrepareCtx, PreparedMutation, Upload, CasResult}`. The plan types are crate-private by design.
- `Store::read_head` and `Store::commit_at_snapshot(OpIdentity::Stable(key), plan, snapshot)` for stable publication.
- `crate::hamt::{empty_root, lookup, insert, admissions_size, IndexHead, StableIndexEntry, StableIndexRoot}` for the authoritative stable-key map.
- Existing `combctl::log::{AppendRange, StableAppendReceipt, Frame, RetentionMode}`. `StableAppendReceipt` already stores `Digest`, `AppendRange { first, last }`, and `generation`.

Add `pub(crate) mod catalog;` in `crates/combctl/src/lib.rs`. Use `catalog::CatalogChunkRef`, `ChunkCatalogRoot`, `CatalogState`, `CatalogNode`, and `CatalogChild`. `combctl::log::ChunkRef` already names the v2 vector entry, so the private catalog entry needs a distinct name.

Keep `LogStore<'a>` and its v2 methods intact. Add the owned R2 facade in `combctl::log`:

```rust
pub struct CompleteFeed {
    store: Arc<Store>,
    logical: String,
    resource: String, // exactly format!("log/{logical}/p0")
}

impl CompleteFeed {
    pub async fn open(
        store: Arc<Store>,
        logical: String,
        call: &CallContext,
    ) -> Result<Self, OpenLogError>;
}
```

R1 has no `LogId`; use the logical `String`. `bytes` is already a workspace dependency but must be added to `combctl`. `tokio-util` is not in the workspace and must be added if `CallContext` keeps `CancellationToken`. Use `std::time::Duration` in the new API because `log.rs` currently imports `chrono::Duration`.

The v3 `CompleteLogManifest` must retain the R1 embedded-commit contract. In addition to the catalog and required `StableIndexRoot`, it needs `log`, `admitted: Vec<Admission>`, `stable_admissions: Vec<StableIndexEntry>`, `result: serde_json::Value`, and `ref_state: Option<RefValue>`. Use `hamt::admissions_size` for the existing 1,024-entry and 256-KiB caps. `BoundedStableAdmissions` is only a sketch name.

Teach `publish.rs::load_commit_view` that both `comb.log.partition-manifest/v2` and the exact v3 schema are known embedded commits. This is read-side history support, not permission to open a v2 feed through `CompleteFeed`. Extend `store.rs::reject_existing_log_manifest` to protect v3 targets as well as v2 targets.

Do not call `LogStore::append_stable` for a new R2 append. Its private `next_writer` can acquire after expiry and checks only `Lease.writer`. The R2 plan must verify a live lease against both the session instance and `RefValue.epoch`, preserve that lease in the new ref value, build the catalog root and HAMT root from the same `HeadSnapshot`, and use one `commit_at_snapshot` CAS. A committed-key lookup still runs before any session-state or lease check.

Session acquisition can reuse `Store::claim` with one retry-stable `OperationId`, the instance identity as the writer value, and `steal = false`. Add an owner-aware sibling of `publish.rs::renew_lease`; the current method checks only `epoch`. Renewal must check the expected writer and epoch on the same fresh `HeadSnapshot`, then preserve generation, target, `head_commit`, and both manifest roots.

## Contract choices to close before Log edits

1. **Durable lease encoding.** R1 persists `comb_core::Lease { writer: String, lease_until }` inside `comb.ref/v2`. The R2 sketch replaces it with structured `LeaseOwner`. The narrow option is to store `WriterInstanceId`'s canonical text in `Lease.writer` and keep `WriterLabel` process-local. A structured owner requires a ref schema bump and broader R1 changes. Do not choose silently.
2. **Public head name.** `combctl::log::LogHead` already exists with `head_seq`, `next_seq`, `epoch`, and `generation`; the R2 sketch declares another `LogHead` with `Cursor` and `trim_before_seq`. Either extend the existing type or call the new result `CompleteFeedHead`. The private `ChunkRef` collision is already resolved above.
3. **Persisted object length.** `RefMutationPlan::prepare` creates plaintext `Upload`s, but `commit_at_snapshot` creates envelopes afterwards. A plan cannot record the exact envelope length in `ChunkRef`. The narrow option is to omit persisted `object_bytes`; enforce the 4-MiB cap in `get_limited` and immediately after envelope encoding. Extending `Upload` to carry pre-encoded bytes is a larger publication-engine change.
4. **Fresh namespace gate.** `Store::ref_key` hard-codes `comb/v2/tenants/...`, and no R1 API records an R2 capability seal. Confirm that deployment supplies an isolated backend prefix that legacy writers cannot address. Otherwise define the seal or a new namespace before enabling `CompleteFeed`; manifest rejection alone does not prevent an R1 writer from winning initialization of an empty ref.

The current `Frame` codec hex-encodes payload bytes, so the accepted 3-MiB plaintext and 4-MiB object caps are conservative. Keep the all-`0xff`, maximum-count, and framing-overhead tests. R2 still performs no compaction, grouping, trim, destructive GC, crate extraction, or bridge protocol work.


## Root decisions, September 6

These choices resolve the four questions above. They supersede the corresponding abbreviated sketches in the R2 contract. No bridge wire fields change.

1. Keep `comb.ref/v2` and `Lease { writer, lease_until }`. Store the canonical random `WriterInstanceId` text in `writer`; keep `WriterLabel` process-local. The R2 lease owner is the pair of that instance ID and `RefValue.epoch`. Every new append, renewal and release checks both on a fresh paired snapshot. Labels never confer ownership. No new ref serialization is needed.
2. Name the new reader result `CompleteFeedHead`. Keep R1 `LogHead` intact. Use `CatalogChunkRef` for the private catalog entry. These are Rust names, not wire changes.
3. Omit persisted `object_bytes` from catalog entries. Keep the digest, sequence span, event count and raw-byte information needed to validate the catalog and chunk. Enforce encoded caps immediately after envelope encoding and before any upload, and enforce them before read collection through `get_limited`. Do not add pre-encoded uploads or a second publication path.
4. Use a distinct physical key layout, `comb/v3/tenants/...`, for all R2 refs, immutable objects and generic operation intents. Add one private Store-owned layout selection, defaulting to the existing v2 layout. `CompleteFeed::open` derives an owned Store view using the v3 layout while preserving backend, tenant, digest key, clock and policy. Route every key builder and cache path through that layout. The selection is not a mutable global or a backend decorator. R1 public constructors and LogStore behavior remain v2.

The logical log resource remains `log/{logical}/p0`; physical layout is not a Foundation key component or an authorship identity. R2 uses fresh isolated backend prefixes for acceptance as an additional operational precaution, not as its writer exclusion mechanism. A legacy R1 binary retains its hard-coded v2 keys and cannot initialize or mutate an R2 v3 ref through the supported API on the same backend.

Opening an existing v3 resource reads v3 state only and rejects unsupported or malformed schema. It never falls back to a v1/v2 log. Before creating an absent v3 resource, detect an existing matching v1/v2 resource and return unsupported rather than silently presenting it as a new empty feed. This check is a migration guard, not the publication barrier: the distinct key namespace prevents a racing R1 initialization from touching v3. Existing v3 state remains authoritative if a separate legacy ref later appears. No live migration or cross-layout feed equivalence is claimed.

Keep shared recovery decoding aware of both exact embedded manifest schemas and preserve `log`, `admitted`, `stable_admissions`, `result`, and `ref_state`. The v3 facade rejects v2 vector manifests even though generic history decoding recognizes their carrier. Core log-target protection recognizes v3 too. Destructive sweep stays refused and must not acquire an accidental v3 deletion path.

Required isolation tests use one MemoryBackend, the same tenant and logical log, and an R1 Store next to the R2 facade. Assert distinct object/ref/intent/cache keys, no cross-layout CAS or history traversal, no v2 fallback, and unchanged v3 receipts after legacy writes. Exercise absent-ref initialization racing in both layouts. A pre-existing legacy ref with no v3 ref must produce unsupported, not a new-feed receipt.

Phase1 remains bounded backend/envelope/cache work. The R2 owner must integrate the retained-target R1 followup before editing log/publication code. Stable physical grouping and collection remain deferred.
