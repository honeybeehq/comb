# Stable-key append addendum

## Decision

Keep design A's intent and immutable-history recovery for generic timed `OperationId` mutations. Add a separate Log-only path for stable committed-key idempotence. The stable path uses an immutable bounded-node key index whose root is published in the same Log manifest CAS as the events. It has no stable Pending intent registry.

Foundation's stable caller key is the canonical length-delimited pair `(docId, original portable envelope SHA256)`. A document can therefore produce many immutable changes. A new portable envelope digest naturally produces a new key and a new change. Comb treats the complete key as opaque bytes and independently fingerprints the exact payload that it receives. Reusing one supplied key with different payload bytes returns a conflict.

```text
stable_key = u32be(len(doc_id)) || doc_id
           || u32be(32) || original_portable_envelope_sha256

key_path = H_tenant(
    "comb.log.stable-key-path/v1",
    tenant_id,
    logical_log_id,
    stable_key,
)

payload_hash = H_tenant(
    "comb.log.stable-payload/v1",
    exact_payload_bytes,
)

request_hash = H_tenant(
    "comb.log.append-stable/v1",
    tenant_id,
    logical_log_id,
    stable_key,
    payload_hash,
)
```

Each hash field uses canonical length-prefix encoding. `request_hash` excludes the physical partition, provider token, route, timeout, writer identity, fence epoch, allocated sequence, commit generation, and timestamps.

The generic timed path keeps the seven-day rule unchanged. Expiry still wins over conflict, and an expired `OperationId` returns `UnknownOperation`. `StableKey` has no timestamp and is never converted to an `OperationId`.

## Caller boundary

Foundation uses the JSONL v1 process bridge. The Foundation bridge team owns its wire field names. Positions and sequences are decimal strings for JavaScript safety; opaque keys and payloads use hex initially. stdout contains protocol frames only. The transport enforces hard frame, event-count and byte limits. Foundation does not link Comb Rust crates.

Foundation constructs its canonical key from docId bytes and the original portable envelope SHA256 before calling the bridge. Comb treats that key as opaque bounded bytes and independently hashes the received payload. The following internal Rust shape is a design sketch; the implementation owner will publish checked signatures.

The internal adapter uses these Comb-owned types:

```rust
// comb-log/src/stable.rs

pub const MAX_STABLE_KEY_BYTES: usize = 512;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StableKey(Box<[u8]>);

impl StableKey {
    pub fn try_from_canonical(
        bytes: impl Into<Box<[u8]>>,
    ) -> Result<Self, StableKeyError>;

    pub fn as_bytes(&self) -> &[u8];
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PayloadHash(Digest);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendRange {
    pub start: u64,
    pub end: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StableAppendReceipt {
    pub payload_hash: PayloadHash,
    pub range: AppendRange,
    pub generation: u64,
}

impl CompleteFeedWriter {
    pub async fn append_stable(
        &self,
        key: StableKey,
        payload: Bytes,
    ) -> Result<StableAppendReceipt, StableAppendError>;
}

#[derive(Debug, Error)]
pub enum StableAppendError {
    #[error("stable key is committed for different payload bytes")]
    StableKeyConflict {
        existing: PayloadHash,
        supplied: PayloadHash,
    },
    #[error(transparent)]
    Integrity(#[from] StableIndexIntegrityError),
    #[error("stable-key index is temporarily unavailable")]
    Unavailable,
    #[error("stable append requires a retained complete feed")]
    NotCompleteFeed,
    #[error("writer fence is stale")]
    Fenced,
}
```

The stable method has no clock, caller time, retry time, `OperationId`, route hint, or sequence hint. `CompleteFeedWriter` is available only when durable Log metadata declares `RetentionMode::Complete`.

## Manifest-rooted index

The complete-feed manifest carries one fixed-size root descriptor and one bounded list for the physical commit:

```rust
pub const MAX_STABLE_INDEX_NODE_BYTES: usize = 4 * 1024;
pub const MAX_STABLE_ADMISSIONS: usize = 1_024;
pub const MAX_STABLE_ADMISSION_BYTES: usize = 256 * 1024;

pub struct StableIndexRoot {
    pub schema: StableIndexSchema,
    pub digest: Digest,
    pub entries: u64,
}

pub struct StableIndexEntry {
    pub key: StableKey,
    pub payload_hash: PayloadHash,
    pub range: AppendRange,
    pub generation: u64,
}

pub struct CompleteLogManifest {
    // Design A fields remain.
    pub stable_index: StableIndexRoot,
    pub stable_admissions: BoundedVec<
        StableIndexEntry,
        MAX_STABLE_ADMISSIONS,
        MAX_STABLE_ADMISSION_BYTES,
    >,
}
```

`stable_index.digest` names a create-only persistent HAMT. `key_path` supplies 5-bit path components. A branch has at most 32 child digests, every encoded node is at most 4 KiB, and the path depth is at most 52. A leaf stores the full `StableKey`, which detects a path-hash collision. A full-depth collision returns `StableKeyHashCollision` and publishes nothing.

Insertion path-copies at most 52 nodes. Old roots share unchanged nodes. Fixed depth makes total index metadata O(retained keys), with a fixed maximum per key. No manifest contains an all-keys vector. Active ref state remains one manifest digest.

Every complete-feed mutation preserves a monotonic index:

- An append derives its new root from the `HeadSnapshot` used by the ref CAS and only inserts absent keys.
- Compaction and maintenance preserve the current root byte for byte.
- Lease renewal re-reads the ref and preserves the root from the version that it CASes.
- After `PreconditionFailed`, the caller discards the plan and rebuilds from a fresh snapshot.

`RefMutationPlan::prepare` creates the data objects, path-copy nodes, manifest, and bounded admissions. Only the crate-private `RefCommitter` writes the ref. An enforced v2 namespace or tested v1-to-v2 seal must isolate stable append before it is enabled. An old writer that drops the index root would invalidate every later negative lookup.

## Runtime and proof

`append_stable` runs one retry loop:

1. Compute `payload_hash` from the exact received bytes. Read one `HeadSnapshot`, load its manifest, and traverse `stable_index`.
2. If a leaf exists with the same hash, return its exact range and generation. If the hash differs, return `StableKeyConflict`.
3. If a valid traversal proves that the leaf is absent, allocate the next range from that manifest. Path-copy the index with one new entry and prepare the data objects and next manifest.
4. Upload every immutable object, then CAS the ref against the snapshot's provider token.
5. On success, return the inserted receipt. On `PreconditionFailed`, reread and restart at step 1. On an ambiguous final reply, also reread and restart at step 1.

The ref CAS publishes both key membership and event visibility. If two callers use the same key from one head, both CAS the same provider token. At most one succeeds. The loser reads the winner's leaf and returns the same receipt or `StableKeyConflict`. Every later manifest preserves that leaf, so many intervening writes do not change the result.

An attempt that crashes before the CAS has no committed effect and does not reserve the key. A later payload may win. The user requires committed-key idempotence, not permanent reservation before commit, so a stable Pending intent adds no required guarantee. A crash after the CAS, including a lost final reply, resolves from the current authoritative index without a finalize write.

`GroupWriter` may combine calls physically. One CAS plan contains at most one candidate for each key. Equal payload hashes share one range. If payload hashes differ, the group selects one candidate deterministically, publishes at most that candidate, and resolves the other against the resulting head. Both the entry count and the encoded admission bytes must fit the manifest limits.

## New key, unavailable index, and corruption

The stable path has no intent lookup. A missing intent therefore neither proves that a key is new nor signals corruption. The authoritative index makes the distinction:

| Observation | Meaning | Result |
| --- | --- | --- |
| A valid traversal ends at an absent child or a nonmatching leaf | No commit has claimed the key | Plan an insert against that snapshot |
| A leaf exists and its hash and range decode correctly | The key committed | Return its receipt or conflict |
| A referenced index node returns confirmed `NotFound` | Retained authoritative state is missing | `Integrity(MissingIndexNode)`; no CAS |
| A node digest, schema, size, path, or count check fails | The index is corrupt | `Integrity`; no CAS |
| A required read has a transient backend failure | Absence is not established | `Unavailable`; no CAS |
| A leaf names an impossible range or generation | Commit metadata is inconsistent | `Integrity`; no CAS |

Compact commit headers remain retained for audit, chain density, and generic design A recovery. Missing committed history is corruption, never proof of a new stable key. An integrity verifier cross-checks stable admissions against their commit generations. Any density gap blocks new publication until repair. The current manifest-rooted index remains the direct lookup authority for stable retries.

## Permanent retention assumptions

For the lifetime of a retained complete feed, the system retains the current index root, every node reachable from it, every logical event range named by a leaf, and the compact commit headers required by the format. Backup, replication, and disaster recovery copy them as one feed.

Complete-feed mode permits no trim, retention downgrade, index-key removal, or stable-metadata cleanup. Physical compaction may change object layout, but it preserves logical ranges and every index entry. A deleted logical `LogId` cannot be reused unless a permanent tombstone preserves its stable-key domain.

The first gate performs no destructive GC over Log data, index nodes, commit headers, or pre-publication upload namespaces. Failed CAS attempts can leave orphans. Later GC must add a publication barrier, a pin, or a backend capability that closes the check-to-delete race. Rechecking the ref before deletion is not a proof and remains rejected.

The first gate supports one publication ref per logical log. A future partitioned form must preserve the `(tenant, logical_log, key)` domain and provide one authoritative log-wide index. Adding a physical partition to key scope would weaken retry behavior after rerouting.

## Focused acceptance tests

- Append two different portable-envelope keys for one `docId`. Assert two ranges.
- Retry one key and payload from a fresh peer after eight days and many unrelated writes. Assert the original range.
- Hold one key fixed and change the payload bytes. Assert `StableKeyConflict` without a ref CAS.
- Release two same-key calls at a barrier, first with equal payloads and then with different payloads. Assert that only one exact range is admitted.
- Lose the final CAS reply, crash the writer, and retry. Assert resolution from the current index with no second range.
- Crash before CAS, then submit another payload for the key. Assert that the later request may commit because no key was reserved.
- Remove a required node, inject a transient node-read failure, and corrupt a leaf range. Assert `Integrity`, `Unavailable`, and `Integrity`, respectively, with no CAS.
- Race append with compaction and lease renewal. Assert root preservation and replanning from a fresh `HeadSnapshot`.
- Exceed each key, node, admission-count, and encoded-byte limit. Assert rejection before publication.
- Advance the mock clock by years. Assert stable retry still resolves, while an expired generic `OperationId` returns `UnknownOperation`.

## Trade-off

The index adds at most 52 bounded immutable nodes per inserted key before structural sharing. In return, stable Log append has one authority and one linearization point. It needs no permanent per-key intent, proves negative membership from current retained state, resolves lost replies after arbitrary intervening writes, and keeps active ref state bounded. Generic timed mutations keep design A's intent and commit-history recovery unchanged.
