# Comb: Durable State Substrate for Honeybee, Apiary, Pheromone, and Nectar

**Status:** Draft specification v0.2 (review pass over v0.1)  
**Date:** 2026-08-22  
**Working name:** Comb  
**Primary systems:** Honeybee, Apiary, Cells, Pheromone, Nectar  
**Audience:** implementers, operators, reviewers, and product owners

---

## Table of contents

- [0. Document status and normative language](#0-document-status-and-normative-language)
- [1. Executive summary](#1-executive-summary)
- [2. Motivation and concrete workloads](#2-motivation-and-concrete-workloads)
- [3. Product definition and boundaries](#3-product-definition-and-boundaries)
- [4. Terminology](#4-terminology)
- [5. High-level architecture](#5-high-level-architecture)
- [6. Correctness model](#6-correctness-model)
- [7. Comb Core](#7-comb-core)
- [8. Comb Log](#8-comb-log)
- [9. Comb Tree](#9-comb-tree)
- [10. Comb Volume](#10-comb-volume)
- [11. Apiary Cell integration](#11-apiary-cell-integration)
- [12. Nectar integration](#12-nectar-integration)
- [13. Pheromone integration summary](#13-pheromone-integration-summary)
- [14. Durability and materialization profiles](#14-durability-and-materialization-profiles)
- [15. Comb Cache and materialization](#15-comb-cache-and-materialization)
- [16. Security, tenancy, and trust](#16-security-tenancy-and-trust)
- [17. Hosted service and self-hosting](#17-hosted-service-and-self-hosting)
- [18. API conventions and error model](#18-api-conventions-and-error-model)
- [19. Compaction, retention, and garbage collection](#19-compaction-retention-and-garbage-collection)
- [20. Storage accounting, quota, and billing](#20-storage-accounting-quota-and-billing)
- [21. Observability and operations](#21-observability-and-operations)
- [22. Failure drills and property testing](#22-failure-drills-and-property-testing)
- [23. Implementation plan](#23-implementation-plan)
- [24. Milestone acceptance criteria](#24-milestone-acceptance-criteria)
- [25. Decided design points](#25-decided-design-points)
- [26. Open questions](#26-open-questions)
- [27. Recommended repository layout](#27-recommended-repository-layout)
- [28. Configuration examples](#28-configuration-examples)
- [29. Example end-to-end flows](#29-example-end-to-end-flows)
- [30. Source basis and provenance](#30-source-basis-and-provenance)
- [31. Final architectural statement](#31-final-architectural-statement)

---

## 0. Document status and normative language

This document consolidates the current design direction for a reusable storage substrate across Honeybee and Apiary. It combines:

- the existing Pheromone object-storage log design;
- the requirements discovered while optimizing Apiary/Hive Cells with copy-on-write storage;
- the storage requirements of Nectar hosted previews;
- the broader product ambition of a self-hosted and managed durable-state service.

The document is intentionally implementation-oriented. It distinguishes between decisions that are already made, a proposed initial implementation, and explicitly open questions.

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** are normative.

Unless a section says otherwise, the requirements describe the intended architecture rather than already implemented behavior.

### 0.1 Changes in v0.2

This revision is a review pass over v0.1. It does not change the core commit mechanism (immutable objects, one conditionally updated ref, fenced single writer, disposable cache). It:

- states the Core boundary test for new consumers and lists the planned views beyond the three initial workloads (§3.5);
- replaces plaintext content digests with tenant-keyed digests, removing the within-bucket existence oracle (§7.2, §7.4);
- expands the ref model into a set of Core conventions — namespaces, symbolic refs, a ref journal, a reserved signature field, release manifests, change hints — that every view builds on (§7.5a);
- fixes follower change detection to key on logical state rather than provider version tokens (§8.10);
- makes Comb Volume a separately gated track and answers the question of the initial cloud Cell driver (§10.0);
- adds "what this workload needs from Comb v1" mappings for Nectar and Pheromone, tying the spec back to its two originating requirements (§12.0, §13.0);
- names the sovereign production backend explicitly (§7.10);
- re-sequences the implementation plan so the hosted beta ships before the portable block overlay (§23.1);
- closes open questions 26.1.1, 26.1.4, 26.1.6, 26.4.6 and converts 26.3.3 into a measurement gate.

---

## 1. Executive summary

Comb is a tenant-scoped, object-backed durable-state substrate for ordered logs, immutable trees, branchable volumes, snapshots, artifacts, and lazy local materialization.

Comb is not merely a WAL library and is not initially a general distributed POSIX filesystem. It is a small correctness kernel with multiple purpose-built views:

- **Comb Log** provides ordered append, replay, follow, retention, and partitioning. Pheromone is the first consumer.
- **Comb Tree** provides immutable file trees, repository images, artifacts, evidence, and selective materialization.
- **Comb Volume** provides block-addressed snapshots, metadata-instant clones, writable overlays, checkpointing, and portable Cell disks.
- **Comb Snapshot** is the common immutable-root and lifecycle model used by Tree and Volume, rather than necessarily a separate storage engine.
- **Comb Cache** is the node-local materialization and eviction subsystem shared by all views.

The common correctness mechanism is:

1. Write immutable data objects.
2. Build an immutable manifest that references only complete, verifiable objects.
3. Atomically advance one named ref using compare-and-swap.
4. Acknowledge according to the selected durability profile only after the required durable state is visible through that ref.

The authoritative long-term state is stored in S3-compatible object storage. The initial certified remote backends are S3 and MinIO. R2 is a planned adapter that must be independently certified rather than assumed equivalent.

Comb supports three deployment forms:

1. local or self-hosted;
2. customer-owned bucket and optional customer-managed encryption key;
3. fully managed Apiary-hosted service.

The most important design constraints are:

- immutable data, small atomic refs;
- one writable owner per ordered log head or writable volume;
- stale-writer fencing through monotonically increasing epochs;
- disposable caches that affect performance but never correctness;
- tenant-keyed content digests, so the deduplication domain and the key domain coincide and no existence oracle exists;
- refs as the extension point, with namespaces, symbolic refs, a journal, release manifests, and an optional signature defined in Core;
- tenant-scoped deduplication only;
- reachability-based garbage collection rather than distributed mutable reference counts;
- explicit durability and materialization policies rather than one misleading global promise;
- no acknowledgement before the selected durability condition has been met.

The recommended implementation order is:

1. preserve Pheromone's local SQLite mode while splitting ingest from matching;
2. implement Comb Core — including the ref conventions of §7.5a — and Comb Log over local files, MinIO, and S3;
3. implement Comb Tree and unify repository-image lifecycle across workstations and satellites;
4. expose one Comb Volume contract over native snapshot/reflink and cloud-provider snapshot backends;
5. add the hosted control plane, managed caches, and customer-owned-bucket mode, shipping Nectar and cloud Cells on the native drivers;
6. implement the portable object-backed block overlay only if the gates in §10.0 require it;
7. add a replicated NVMe journal only after the object-durable path is correct.

---

## 2. Motivation and concrete workloads

### 2.1 Pheromone

The current local Pheromone shape is appropriate for a single daemon: one process, one SQLite database, one critical section, microsecond-scale acknowledgements, and no external infrastructure. It is not sufficient for a cloud trail that must retain billions of events, survive machine loss, support many followers, replay arbitrarily far back, and permit safe writer takeover.

The existing Scale design chooses the correct cloud pattern:

- object storage contains the durable data;
- a small per-partition manifest is the linearization point;
- the manifest is advanced using compare-and-swap;
- servers are replaceable writers, readers, compactors, and caches;
- nothing is acknowledged before immutable WAL data and the new manifest are durable.

Comb generalizes the underlying correctness machinery without weakening Pheromone's existing semantics.

### 2.2 Apiary and Hive Cells

Cells need fast, cheap fan-out from a shared repository or prepared environment.

Using the current Apiary repository as a representative example:

- packed Git image: approximately 300–350 MB;
- checked-out tracked tree: approximately 70 MB;
- one Cell Git-image clone: approximately 300 MB logically, but initially almost no unique physical data under block-level copy-on-write;
- ten Cells: approximately 4 GB logical, but close to 1 GB of unique physical data before dependency installation and agent changes;
- without copy-on-write, the same ten Cells consume approximately 4 GB physically.

Warm dependency trees and build trees amplify the difference. A prepared TypeScript environment may occupy several gigabytes logically, yet the base should be stored once per organization, product, and setup epoch. Cells should initially consume only metadata and later consume physical bytes as blocks diverge.

Current local storage capabilities vary:

| Placement | Native capability | Intended behavior |
|---|---|---|
| Workstation on APFS | reflink / clone | one repository image; Cell images share unchanged blocks |
| Satellite on btrfs, XFS reflink, APFS, ZFS, or LVM-thin | snapshot or reflink | same model with a node-local base |
| Satellite on ext4 | no native block clone | OverlayFS tree overlay where safe; portable Comb block overlay later; physical copy only as final fallback |
| Cloud | provider snapshot or Comb block overlay | shared post-setup snapshot plus per-Cell writable overlay |

The local APFS implementation and the cloud snapshot implementation MUST converge on one logical lifecycle and one `CombVolume` contract rather than evolve as unrelated systems.

### 2.3 Nectar hosted previews

Nectar needs hosted previews that can start from a repository or uploaded bundle, perform setup once, and fan out many isolated preview instances.

The desired flow is:

```text
Git host or uploaded bundle
          ↓ once
organization/product repository image
          ↓ setup once per setup identity
post-setup snapshot
          ↓ metadata-only clone
preview Cell writable overlay
```

Nectar benefits from the same properties as Cells:

- immutable source and setup generations;
- instant logical forks;
- lazy local materialization;
- durable artifacts separate from opaque machine state;
- cleanup by reachability;
- tenant-scoped encryption and deduplication;
- the ability to reconstruct a preview on a different node.

### 2.4 Why one substrate

These workloads differ at the API level but share the same lower-level requirements:

- immutable durable objects;
- content identity and verification;
- atomic publication of a new logical state;
- stale-writer exclusion;
- snapshots and forks;
- hot local storage in front of low-cost object storage;
- compaction, retention, and garbage collection;
- repeatable failure testing;
- tenant isolation and accounting.

Comb therefore shares the correctness machinery but preserves distinct higher-level interfaces. A log is not exposed as a filesystem, and a filesystem is not forced through an append-only event API.

---

## 3. Product definition and boundaries

### 3.1 Product statement

> **Comb is the object-backed snapshot, log, and materialization substrate for Apiary. It gives Cells and services instant, branchable, durable state that can be attached on any authorized node.**

### 3.2 Product family

The working product names are:

- **Comb Core** — immutable objects, refs, fencing, pins, integrity, and backend adapters;
- **Comb Log** — ordered append and replay;
- **Comb Tree** — immutable file trees and artifacts;
- **Comb Volume** — block snapshots and writable overlays (a separately gated track; §10.0);
- **Comb Cache** — local materialization, prefetch, and eviction;
- **Comb Queue** — possible future consumer-group semantics above Comb Log, not part of the initial scope.

Recommended executable and package names:

```text
comb-core
comb-object
comb-log
comb-tree
comb-volume
comb-cache
combd
combctl
```

### 3.3 Goals

Comb MUST support:

1. Durable state larger than any individual host disk.
2. Exact reconstruction of committed state from the authoritative backend.
3. Atomic publication of a log head, tree ref, or volume branch.
4. Safe writer takeover without stale-writer commits.
5. Metadata-instant snapshot and fork operations.
6. Lazy materialization onto local NVMe or SSD.
7. Linear read scaling through independent caches and readers.
8. Explicit durability profiles with honest acknowledgement semantics.
9. Tenant-scoped encryption and deduplication.
10. Self-hosting, customer-owned buckets, and a managed service.
11. One conformance and failure-drill suite across local files, MinIO, and S3.
12. An open, versioned storage format with standalone verification and export tooling.
13. Native adapters for local copy-on-write facilities where available.
14. A portable fallback that does not require the host filesystem itself to support reflinks.
15. Correct retention and garbage collection in the presence of long-lived Cells and old base generations.

### 3.4 Non-goals for the initial implementation

Comb v1 MUST NOT claim or require:

- multi-region active-active writes to one resource;
- concurrent multi-writer mounts of one writable volume;
- globally total ordering across log partitions;
- atomic transactions across multiple independent refs;
- exactly-once external side effects;
- cross-tenant deduplication of private data;
- a fully general distributed POSIX filesystem;
- synchronous object-store persistence for every ephemeral Cell filesystem write;
- transparent execution of arbitrary unmodified databases with strong remote `fsync` guarantees;
- infinite capacity in the literal sense;
- zero-cost distribution of all bytes to all nodes.

“Infinite” means elastic retention at object-storage economics, subject to provider limits, quotas, retention policy, and budget.

“Immediately distributed” means that any authorized node can resolve the state and begin lazy materialization. It does not mean every byte is pre-replicated to every node.

### 3.5 Views and the Core boundary test

Log, Tree, and Volume are *views*: purpose-built data structures over Comb Core. Comb Core is objects, refs, pins, and leases — the same kernel as Git's objects and refs, plus the two things Git lacks for a multi-node world (GC roots and writer leases). Git's entire feature surface — branches, tags, notes, worktrees, submodules — is conventions over refs that never touch the object model. That is the shape Comb preserves.

The test for every proposed consumer is:

> **Can it be built as a view using only Core's public API?**

If a new consumer requires a change to Core, that is a design smell to be resolved by reconsidering the consumer, not by widening Core.

Planned consumers beyond the three initial workloads, each expressible as a view:

| Consumer | View | Notes |
|---|---|---|
| Agent session logs | Log | one trail per agent, subject = session; single writer, append-heavy, read-rarely, tiers cold; nothing new required |
| vis (agent identity and memory) | Tree + Log | memory is a Tree ref per agent: diffable, auditable through the ref journal, forkable into a new agent by metadata-only clone; identity is a signing key over ref updates (§7.5a.4); memory events are a Log |
| bod (distributed virtual filesystem) | Tree | read-only materialized tree plus an overlay whose commits become new tree generations; Git-shaped, not POSIX-with-`fsync` (§3.4) |
| gull (gold-standard code storage) | Tree | signed tree publication (§7.5a.4), release manifests (§7.5a.5), retention and legal-hold pins; also the natural object store for a Git forge |
| Satellites | Cache + compute | a satellite is a Comb Cache node with compute attached; "any authorized node can resolve state and begin lazy materialization" is the satellite model |

Apiary's distribution story therefore decomposes as: **Comb for state, Pheromone for signal, Council for arbitration.** Comb MUST NOT depend on Pheromone for correctness; Pheromone MAY carry Comb change hints for latency (§7.5a.6).

---

## 4. Terminology

| Term | Definition |
|---|---|
| **Tenant** | Security, encryption, deduplication, accounting, and retention boundary. Normally one Apiary organization. |
| **Namespace** | Product or project grouping inside a tenant. |
| **Resource** | A log, tree, volume, artifact collection, or other independently managed Comb object. |
| **Blob** | Immutable byte sequence addressed by a tagged content digest. |
| **Manifest** | Immutable structured object that references blobs or other manifests and describes a complete logical state. |
| **Ref** | Small named mutable pointer to one immutable target. Updated atomically by compare-and-swap. |
| **Symbolic ref** | A ref whose target is another ref name rather than an object digest (§7.5a.2). |
| **Ref journal** | Per-resource Comb Log recording every successful ref update; the operation log and audit trail (§7.5a.3). |
| **Release manifest** | Immutable object recording a set of ref names and the targets observed at one moment, published through one ref; a consistent cut without a multi-ref transaction (§7.5a.5). |
| **View** | A data structure and protocol built on Comb Core's public API (Log, Tree, Volume, and future consumers). Views never modify Core (§3.5). |
| **Tenant-keyed digest** | `BLAKE3_keyed(tenant_digest_key, plaintext)`. The content identity used for storage keys. Unrelated across tenants for identical content (§7.2, §7.4). |
| **Head** | The ref naming the current writable state of a resource or branch. |
| **Snapshot** | Immutable state reachable from a root manifest. |
| **Generation** | A published repository or setup snapshot intended for new provisioning. |
| **Staging generation** | Candidate generation being built and validated before atomic publication. |
| **Overlay** | Cell- or branch-specific writable state layered over an immutable base. |
| **Materialization** | Fetching immutable data into a node-local cache or native filesystem representation. |
| **Pin** | Temporary or durable root that prevents reachable objects from garbage collection. |
| **Lease** | Time-bounded claim used to coordinate a writer or background worker. |
| **Epoch** | Monotonically increasing fencing number. A stale writer with an older epoch cannot commit. |
| **Provider version token** | Backend value used for conditional replacement, such as an object version or ETag-like token. It is not a content digest. |
| **Content-addressed storage** | Immutable objects identified by their content digest. |
| **Compare-and-swap** | Conditional update of a ref only if the observed provider version is still current. |
| **Durability profile** | The condition that must be met before a write or checkpoint is acknowledged. |
| **Hot set** | Data predicted or measured to be required early by a workload. |
| **Cell** | An isolated Apiary/Hive sandbox with one immutable base and one writable overlay. |
| **Template** | Published repository or post-setup snapshot used to create Cells. |

The acronym **CAS** MUST NOT be used unqualified in user-facing APIs or documentation because it can mean both content-addressed storage and compare-and-swap. Use **content digest** or **conditional ref update** instead.

---

## 5. High-level architecture

```text
                              Apiary control plane
                 provisioning · policy · billing · discovery
                                      │
             ┌────────────────────────┼────────────────────────┐
             │                        │                        │
        Pheromone                 Apiary Cells              Nectar
        Comb Log              Comb Tree + Volume       Tree + Volume
             │                        │                        │
             └────────────────────────┼────────────────────────┘
                                      │
                                Comb Core
       ┌──────────────────────────────────────────────────────────────┐
       │ immutable blobs and manifests                               │
       │ conditional refs and per-ref linearization                  │
       │ leases and fencing epochs                                   │
       │ pins, tombstones, reachability, retention, and GC           │
       │ encryption, digests, checksums, and schema versions         │
       │ cache, materialization, prefetch, and backend conformance    │
       └──────────────────────────────────────────────────────────────┘
                         │                              │
                  node-local NVMe                authoritative backend
                native snapshots/cache          S3 · MinIO · later R2
```

### 5.1 Planes

Comb consists of three planes.

#### Data plane

The data plane performs:

- immutable object upload and download;
- conditional ref updates;
- append, read, follow, and replay;
- volume attach, read, write, checkpoint, and fork;
- tree materialization;
- cache lookup, fill, verification, and eviction;
- optional replicated journal writes in a later phase.

#### Control plane

The hosted control plane performs:

- tenant and resource provisioning;
- policy and quota management;
- credentials and encryption-key orchestration;
- placement and scheduling hints;
- discovery, UI, billing, and audit search;
- asynchronous lifecycle workflows.

The hosted control-plane database MAY be PostgreSQL. It is not the authoritative source of the stored resource contents. Enough state MUST remain in the tenant's bucket to verify and recover logs, trees, volumes, and snapshots without the hosted control-plane database.

#### Compute integration plane

Compute integration connects Cells or services to storage through:

- native filesystem snapshots or reflinks;
- OverlayFS for tree-level sharing on Linux;
- a portable userspace block-device driver for Comb Volume;
- direct SDK access for logs, trees, artifacts, and prefetch;
- optional FUSE or kernel-mode interfaces later.

### 5.2 Deployment modes

#### Local

- local filesystem backend;
- SQLite for Pheromone local mode;
- native APFS, btrfs, XFS, ZFS, or LVM-thin adapters where available;
- no external service required.

#### Self-hosted or customer-owned bucket

- `combd` runs in the customer's environment or Apiary compute plane;
- authoritative objects live in customer S3 or MinIO;
- customer grants scoped access through a role or credentials;
- customer-managed encryption keys are optional and separate from bucket ownership.

#### Managed

- Apiary operates the regional data plane, caches, compaction, GC, and optional journal;
- customer chooses Apiary-managed storage or customer-owned storage;
- managed service supplies observability, verification, repair workflows, and billing.

---

## 6. Correctness model

### 6.1 Core invariants

Every implementation MUST preserve these invariants.

1. **Acknowledged means durable enough.** A successful acknowledgement means the selected durability profile has been satisfied.
2. **Acknowledged means reachable.** Durable immutable data is not acknowledged as committed until a committed ref or log manifest makes it logically reachable.
3. **Reachable means complete.** A visible manifest MUST reference only complete, verifiable objects.
4. **Immutable means immutable.** A content-addressed object is never overwritten in place.
5. **Stale writers cannot commit.** Every writable head carries an epoch; conditional update failure or epoch mismatch fences the writer.
6. **Caches are disposable.** Destroying every cache MAY reduce performance but MUST NOT destroy committed state.
7. **Snapshots are stable.** Resolving one immutable snapshot root yields the same logical contents for its lifetime.
8. **Garbage collection respects reachability.** No object reachable from an active ref, retained root, valid pin, or protected history root may be deleted.
9. **Tenant boundaries are preserved.** Private bytes, digests, keys, caches, and deduplication domains do not cross tenants.
10. **Retries are idempotent where promised.** Client operation IDs prevent an ambiguous acknowledgement from silently creating an unintended second logical operation.
11. **Corruption is detected, not hidden.** Digest, checksum, decompression, or authenticated-decryption failure produces an integrity error and never returns unverified content.

### 6.2 Atomicity and consistency scope

Comb provides linearizability at the granularity of one ref.

Examples:

- one log partition manifest;
- one volume branch head;
- one tree ref;
- one template publication ref.

Comb does not initially provide an atomic transaction across two unrelated refs. Multi-resource workflows MUST be implemented as idempotent state machines with pins, intent records, and cleanup.

A client that resolves a ref may read either the old complete state or the new complete state during a concurrent update. It MUST NOT observe a partially published state.

### 6.3 Ordering

- Comb Log provides total order within one partition.
- It does not provide total order across partitions.
- Comb Tree and Comb Volume provide ordered ref generations, not a total order over every internal object write.
- External side effects remain at-least-once unless the consumer implements idempotency.

### 6.4 Availability during backend failure

Comb prefers correctness over false durability.

If a durability profile requires the authoritative object store and that backend is unavailable:

- new object-durable acknowledgements MUST block or fail according to policy;
- no success may be returned merely because local memory or disk accepted the write;
- readers MAY continue serving already verified cached immutable data;
- active Cells MAY continue local work under `local` or `checkpointed` policies, but the potential loss window MUST be exposed;
- recovery MUST not invent committed state that was never reachable through a successful ref update.

### 6.5 Guarantee matrix

| Property | Initial guarantee |
|---|---|
| Blob write | idempotent create; immutable after success |
| Ref update | atomic and linearizable per ref |
| Log append | atomic batch per partition; durable before return according to log policy |
| Log replay | exact from a retained position or timestamp-derived position |
| Tree snapshot | immutable complete namespace |
| Volume checkpoint | atomic branch-head movement to one immutable block-map root |
| Writable volume | one writer; any number of read-only consumers |
| Fork | metadata-only logical operation before materialization |
| Distribution | attachable from any authorized node; bytes may materialize lazily |
| Cache | non-authoritative, verified, disposable |
| Exactly-once effects | not provided; use stable IDs and idempotent consumers |
| Multi-ref transaction | not provided in v1 |


---

## 7. Comb Core

### 7.1 Responsibilities

Comb Core owns only the primitives that are genuinely shared:

- tenant-scoped object keys;
- immutable object creation and retrieval;
- tagged content digests;
- conditional ref updates;
- leases and fencing epochs;
- pins and tombstones;
- schema versioning;
- integrity verification;
- encryption envelopes;
- backend capability detection and certification;
- cache identity and materialization contracts;
- reachability traversal hooks.

Comb Core MUST NOT know Pheromone envelope semantics, Git object semantics, filesystem directory semantics, or VM block semantics.

### 7.2 Object identity

Every immutable object has a tagged content identity:

```text
<algorithm>:<lowercase-hex-digest>
```

The on-disk schema MUST permit more than one digest algorithm. The initial implementation uses **BLAKE3-256 in keyed mode** for internal objects, keyed with the tenant's digest key (§7.4). BLAKE3 is chosen for software throughput; keyed mode is a native feature of the algorithm and costs nothing over unkeyed hashing. Externally exchanged artifacts MAY additionally record an unkeyed SHA-256 or BLAKE3 digest in object metadata for interoperability; an unkeyed digest is never used as a storage key.

Algorithm tags: `b3k` is tenant-keyed BLAKE3-256 (`b3k:<hex>`), `b3` is unkeyed BLAKE3-256, `sha256` is unkeyed SHA-256. Tools MUST refuse to compare a `b3k` digest across tenants. This closes open question 26.1.1.

An object-store provider version token or ETag MUST NOT be used as the content identity.

The digest is computed over a canonical plaintext representation before provider-specific multipart encoding. Compression and encryption metadata are stored in the object envelope.

### 7.3 Immutable object envelope

A generic object consists of:

```text
fixed header
canonical metadata
compressed or raw payload
integrity trailer where required
```

Conceptual metadata:

```json
{
  "schema": "comb.object/v1",
  "tenant": "org_01...",
  "kind": "blob|tree-node|volume-node|log-chunk|log-segment|index|manifest",
  "digest": "b3k:...",
  "plaintext_bytes": 4194304,
  "stored_bytes": 1038811,
  "compression": "zstd|none",
  "encryption": {
    "scheme": "aes-256-gcm|none",
    "key_version": "tenant-key-v7",
    "nonce": "..."
  },
  "created_at": "2026-08-22T00:00:00Z"
}
```

The exact binary envelope remains an implementation decision, but the following are mandatory:

- object kind and schema version are unambiguous;
- plaintext length is known before allocation;
- decompression bombs are rejected through configured limits;
- authenticated decryption or equivalent detects tampering;
- plaintext digest is verified after decode;
- object creation is create-only;
- duplicate upload of the same tenant-scoped digest is safe.

### 7.4 Tenant-scoped keys and deduplication

Content keys MUST include the tenant boundary:

```text
comb/v1/tenants/<tenant-id>/objects/<algorithm>/<prefix>/<digest>
```

Consequences:

- identical private content in two tenants is stored and encrypted independently;
- within one tenant, identical content MAY deduplicate;
- existence checks do not reveal cross-tenant content;
- keys and billing can be separated cleanly;
- cryptographic erasure can be applied per tenant or key version.

Object keys are derived from a **tenant-keyed digest**: `BLAKE3_keyed(tenant_digest_key, plaintext)`, where the digest key is derived from the tenant root key alongside the data-encryption key (§16.3). Consequences beyond prefix scoping:

- identical plaintext in two tenants produces unrelated digests, so cross-tenant deduplication is structurally impossible rather than merely prohibited by policy;
- a party that can list a bucket — including the hosted operator in managed-bucket mode — cannot confirm whether a tenant holds a specific known file by computing that file's digest; there is no existence oracle. With plaintext digests as keys, "does this customer have this exact leaked credential / this exact library version" would be answerable by anyone with list permission;
- the key domain and the deduplication domain are the same object and rotate together; a digest-key rotation is a format migration under §7.13 (new objects, advanced refs), never an in-place rewrite;
- within one tenant, deduplication works exactly as with plaintext digests: the first writer stores the encrypted object with a random nonce, later writers that lose the create-only race reference the existing verified object.

The keyed digest is computed over canonical plaintext; the nonce and ciphertext do not participate in identity. A reader needs the tenant digest key to verify identity. It is distributed through the same scoped grant as decryption keys (§13.4, §16.3): a principal authorized to read a tenant's content is by definition permitted to compute its digests, and no weaker grant exists.

### 7.5 Named refs

A ref is the only generally mutable object in Comb.

Conceptual ref schema:

```json
{
  "schema": "comb.ref/v1",
  "tenant": "org_01...",
  "resource": "volume/vol_01...",
  "name": "branches/main",
  "generation": 42,
  "epoch": 17,
  "target": "b3k:manifest-digest",
  "writer": "node-eu-north-1-a-7",
  "lease_until": "2026-08-22T10:15:00Z",
  "updated_at": "2026-08-22T10:14:58Z",
  "metadata": {}
}
```

A read returns both the decoded ref and an opaque provider version token:

```rust
pub struct RefSnapshot {
    pub value: RefValue,
    pub version: RefVersion,
}
```

An update succeeds only if the version observed by the caller is still current:

```rust
async fn compare_and_set_ref(
    &self,
    name: &RefName,
    expected: &RefVersion,
    next: &RefValue,
) -> Result<RefSnapshot>;
```

The provider version token is an implementation mechanism. The logical `generation` and `epoch` are part of the portable format and are independently checked.

The ref schema additionally carries two optional fields defined in §7.5a: `symref` (§7.5a.2) and `signature` (§7.5a.4).

### 7.5a Ref conventions

Refs are the extension point of Comb. Every view is a set of conventions over refs, in the same way that Git branches, tags, notes, and worktrees are conventions over Git refs. The following conventions are part of Core from the first format version so that consumers never invent them independently and incompatibly.

#### 7.5a.1 Namespaces

Ref names are hierarchical and view-owned:

```text
refs/<view>/<resource-id>/<name>
```

Examples:

```text
refs/log/trail_01.../p/0/manifest
refs/tree/repo_01.../generations/42
refs/tree/repo_01.../current
refs/volume/vol_01.../branches/main
refs/vis/agent_01.../memory
refs/gull/pkg_01.../releases/1.4.0
```

A view MUST NOT write outside its own namespace. Core reserves `refs/core/` for leases, release manifests, and journals. The namespace layout in §7.12 (`refs/<resource-kind>/<resource-id>/<ref-name>.json`) is the physical encoding of this scheme.

#### 7.5a.2 Symbolic refs

A ref MAY be symbolic: its `symref` field names another ref instead of `target` naming an object.

```json
{ "schema": "comb.ref/v1", "name": "current", "symref": "generations/42", "generation": 7, "epoch": 3 }
```

Current and staging generation pointers (§9.7, §11.15) are symbolic refs. Resolving a symref costs one additional read per hop and MUST NOT chain deeper than a configured limit (default 4). Updating a symref is an ordinary conditional ref update; it does not touch the target ref.

#### 7.5a.3 Ref journal

Every successful ref update appends an entry to a per-resource **ref journal**, which is itself a Comb Log:

```text
refs/core/journal/<resource-kind>/<resource-id>
```

An entry records the previous and new `generation`, `epoch`, `target` or `symref`, `writer`, operation ID, and timestamp. This is an operation log in the sense of jujutsu's `jj op log`, and for agents it is the audit trail: "what did this agent's memory ref point to at 14:02" is a journal seek.

The journal is written *after* the conditional update succeeds. The conditional update remains the sole linearization point; the journal is a consequence, never a prerequisite. A lost journal write therefore degrades auditability but cannot produce an inconsistent ref, and a journal gap is detectable because consecutive entries carry consecutive generations. Journal entries are retained under the resource's retention policy and are members of the GC root set while retained (§19.2). `combctl ref history` reads the journal. This closes open question 26.1.6.

#### 7.5a.4 Signatures

The ref schema reserves an optional `signature` field from the first format version:

```json
"signature": { "scheme": "ed25519", "key_id": "...", "sig": "..." }
```

Verification is not required in v1 and unsigned refs are valid. The field exists so that views with provenance requirements — vis agent identity, gull release publication — can sign ref updates without a format migration. When present, the signature covers the canonical ref body excluding `lease_until`, `updated_at`, and the provider version token, so lease renewals do not invalidate it. Key management for signing is the view's concern, not Core's. This closes open question 26.1.4.

#### 7.5a.5 Release manifests: consistent cuts without transactions

Comb does not provide multi-ref transactions (§6.2). The blessed pattern for a consistent snapshot across several refs is a **release manifest**: an immutable object recording a set of ref names and the exact targets observed, published through one ref under `refs/core/releases/` or the view's own namespace.

```json
{
  "schema": "comb.release/v1",
  "members": {
    "refs/tree/repo_01.../current": "b3k:...",
    "refs/volume/vol_01.../branches/main": "b3k:...",
    "refs/log/trail_01.../p/0/manifest": "b3k:..."
  },
  "created_by": "operation-id",
  "created_at": "..."
}
```

A reader that resolves the release ref sees one consistent cut. Members are not locked: a release records what was true at publication, it does not freeze the member refs afterwards. A release ref is a GC root, so the recorded targets stay reachable for as long as the release is retained. Setup templates (§11.4), Nectar preview publication (§12.1), and gull releases use this pattern.

#### 7.5a.6 Change hints

Followers discover ref changes by reading the ref (§8.10). To reduce tail latency, `combd` MAY emit a ref-change hint on each successful update. Within the Honeybee ecosystem the hint channel is Pheromone. Hints are non-authoritative: a consumer that receives a hint re-reads the ref; a consumer that receives no hint still polls. Comb MUST NOT depend on the hint channel for correctness, and a Pheromone outage MUST NOT affect Comb acknowledgement. The dependency is one-directional: Pheromone is built on Comb Log; Comb uses Pheromone only for latency.

#### 7.5a.7 Ref API

```rust
trait RefStore {
    async fn get(&self, name: &RefName) -> Result<RefSnapshot>;
    /// Follows symrefs up to the configured depth.
    async fn resolve(&self, name: &RefName) -> Result<RefSnapshot>;
    async fn compare_and_set(&self, name: &RefName, expected: &RefVersion,
                             next: &RefValue) -> Result<RefSnapshot>;
    async fn list(&self, prefix: &RefPrefix, cursor: Option<ListCursor>)
        -> Result<RefPage>;
    async fn history(&self, name: &RefName, from: Option<JournalPos>, max: usize)
        -> Result<Vec<RefJournalEntry>>;
}
```

`list` over refs follows the same rule as object listing (§7.8): discovery only, never linearization.

### 7.6 Leases and fencing

Leases coordinate ownership; epochs provide correctness.

To acquire a writable head:

1. Read the current ref.
2. Determine whether the current lease is expired, already owned by the caller, or administratively revocable.
3. Construct a new ref with `epoch + 1`, the new writer identity, and a new lease deadline.
4. Attempt a conditional ref update.
5. Success grants leadership at the new epoch; failure means another writer won.

A writer MUST include its observed epoch in every ref update. If the ref's epoch changed, the writer is fenced and MUST stop committing.

Clock time is used only to decide when takeover may be attempted. Correctness comes from conditional update and epoch comparison. Clock skew may delay or accelerate an attempt but MUST NOT permit two epochs to commit to the same ref.

Default provisional values:

```text
lease duration: 30 s
renew interval: 10 s
clock slack: 5 s
```

These values are configurable and not part of the storage format.

### 7.7 Pins

Pins protect roots during operations and retention.

Pin types:

- **provisioning pin** — short-lived protection while a Cell is being created;
- **active Cell root** — durable root while a Cell exists;
- **retention pin** — user or policy retention;
- **staging pin** — protects a generation during build and validation;
- **legal-hold pin** — administrative retention that does not expire automatically;
- **GC traversal pin** — prevents a concurrent collection pass from invalidating its own view.

A pin contains:

```json
{
  "schema": "comb.pin/v1",
  "root": "b3k:...",
  "kind": "provisioning|active|retention|staging|legal-hold|gc",
  "owner": "operation-or-resource-id",
  "created_at": "...",
  "expires_at": "... or null"
}
```

Expired pins are not roots. Legal-hold and retained pins require explicit removal.

### 7.8 Core backend interface

Comb SHOULD wrap the Rust `object_store` crate behind a narrower capability-checked interface.

Conceptual trait:

```rust
#[async_trait]
pub trait ObjectBackend: Send + Sync {
    async fn put_create(&self, key: &ObjectKey, body: ByteStream, opts: PutOpts)
        -> Result<ObjectVersion>;

    async fn put_update(&self, key: &ObjectKey, expected: &ObjectVersion,
                        body: ByteStream, opts: PutOpts)
        -> Result<ObjectVersion>;

    async fn get(&self, key: &ObjectKey, range: Option<ByteRange>)
        -> Result<GetResult>;

    async fn head(&self, key: &ObjectKey) -> Result<ObjectMeta>;
    async fn delete(&self, key: &ObjectKey) -> Result<()>;
    async fn list(&self, prefix: &ObjectPrefix, cursor: Option<ListCursor>)
        -> Result<ListPage>;

    fn capabilities(&self) -> BackendCapabilities;
}
```

`list` MUST NOT be used as the linearization mechanism. Correctness is based on direct reads of known refs and immutable objects. Listing is for discovery, orphan scanning, audit, and garbage collection.

### 7.9 Required backend capabilities

A certified authoritative backend MUST provide:

- create-only object writes;
- conditional replacement using an observed version token;
- atomic visibility of one object;
- strong read-after-successful-write behavior for one key;
- range reads;
- multipart or streaming upload for large immutable objects;
- object metadata or an equivalent version token;
- deletion;
- predictable authorization and error behavior.

Optional capabilities include:

- provider checksums;
- object versioning;
- object lock or retention mode;
- lifecycle policies;
- replication;
- inventory exports.

Comb MUST certify each provider against the conformance suite. An S3-compatible API string alone is insufficient evidence of equivalent semantics.

### 7.10 Initial backend matrix

| Backend | Role | Status target |
|---|---|---|
| in-memory fault-injected backend | model and property tests | required in unit tests |
| local filesystem | development, CI, single-machine mode | required |
| MinIO | self-hosting and conditional-write integration | first-class from day one |
| S3 Standard | conformance reference; expected customer-owned-bucket target | first-class from day one |
| MinIO on operator-owned hardware (Hetzner, Dvergatal fleet) | **sovereign production baseline** for the Apiary deployment | first-class from day one; same conformance suite as S3, no relaxation |
| Hetzner Object Storage | candidate sovereign managed backend | certified separately; conditional-write and read-after-write behavior MUST be verified, not inferred from the S3-compatible API |
| S3 Express One Zone | lower-latency zonal profile | certified separately; durability described accurately |
| R2 | pluggable remote alternative | post-initial certification |
| GCS / Azure | possible adapters | not initial commitments |

For the Apiary deployment the sovereign baseline is the production path; S3 Standard is the reference against which conformance is defined. The conformance suite is the main deliverable of Phase B precisely because the production backend is not the reference backend.

### 7.11 Local filesystem semantics

The local backend MUST emulate the same logical contracts:

- create-only objects through exclusive create;
- conditional ref update through an atomic replace guarded by an observed version and local lock;
- file and parent-directory `fsync` where durable acknowledgement is promised;
- atomic rename on one filesystem;
- no reliance on directory listing for head state.

A local backend that cannot guarantee these semantics MUST advertise a weaker durability capability and MUST NOT be used to claim object-equivalent durability.

### 7.12 Namespace layout

The common tenant layout is:

```text
comb/v1/tenants/<tenant>/
  objects/<algorithm>/<prefix>/<digest>
  refs/<resource-kind>/<resource-id>/<ref-name>.json
  pins/<pin-id>.json
  tombstones/<resource-kind>/<resource-id>/<tombstone-id>.json
  resources/<resource-kind>/<resource-id>/...
  audit/...
```

Resource-specific mutable refs are deliberately separated. Comb MUST NOT create one global tenant manifest whose conditional update serializes unrelated logs, volumes, or templates.

### 7.13 Format evolution

- Every object and ref has an explicit schema identifier.
- Readers SHOULD support the current format and at least one previous compatible format.
- Immutable objects are migrated by writing new objects and advancing refs, never by rewriting in place.
- A format migration MUST be restartable and idempotent.
- `combctl export` and `combctl verify` MUST understand every format still considered supported.
- A future writer MUST NOT publish a schema that the configured minimum reader version cannot understand.

---

## 8. Comb Log

### 8.1 Purpose

Comb Log provides:

- atomic batch append;
- monotonically increasing sequence positions within a partition;
- read and seek;
- follow;
- exact retained replay;
- compaction;
- retention;
- safe writer takeover;
- independent read scaling;
- optional partitioning.

Pheromone is the first implementation and conformance workload.

### 8.2 Pheromone interface

The Pheromone adapter preserves the `TrailLog` boundary:

```rust
pub struct Pos {
    pub partition: u32,
    pub seq: u64,
}

pub trait TrailLog: Send + Sync {
    /// Atomically append one batch. Return only after the configured
    /// durability condition is satisfied.
    fn append(&self, batch: &[Envelope]) -> Result<Pos>;

    fn head(&self) -> Result<Vec<Pos>>;
    fn read(&self, from: Pos, max: usize) -> Result<Vec<(Pos, Envelope)>>;
    fn seek_ts(&self, partition: u32, ts: &str) -> Result<Pos>;
    fn follow(&self, from: Pos) -> Result<Box<dyn Stream<Item = (Pos, Envelope)>>>;
    fn trim_before(&self, ts: &str) -> Result<()>;
}
```

Implementations:

- `SqliteLog` — existing local path;
- `FsLog` — Comb Log over a local directory;
- `ObjectLog` — Comb Log over MinIO, S3, or another certified backend.

`event_by_id` is a side-index operation rather than part of the append/read trait.

### 8.3 Pheromone structural split

Before changing storage, Pheromone MUST split:

```text
ingest → append to TrailLog → matcher follows from cursor → cascade → delivery
```

This preserves per-trail order and at-least-once delivery while decoupling ingest latency from matcher cost. Matching workers can scale independently and replay becomes starting a follower at a retained position.

### 8.4 Resource layout

Pheromone-compatible layout:

```text
<tenant>/<trail>/
  p/<partition>/manifest.json
  p/<partition>/wal/<epoch>/<first-seq:020>.chunk
  p/<partition>/seg/<first-seq:020>-<last-seq:020>.seg
  p/<partition>/seg/<first-seq:020>-<last-seq:020>.idx
  control/manifest.json
  control/deliveries/<partition>/...
```

In a fully normalized Comb namespace, these keys MAY be rooted beneath the tenant and resource prefix, but the logical separation remains the same.

### 8.5 Partition manifest

Conceptual schema:

```json
{
  "schema": "comb.log.partition-manifest/v1",
  "version": 1,
  "epoch": 17,
  "leader": {
    "node": "hub-a",
    "lease_until": "2026-08-21T10:15:00Z"
  },
  "head_seq": 48213994,
  "wal": [
    {
      "key": "wal/17/00000000000048210000.chunk",
      "first": 48210000,
      "last": 48213994,
      "ts_first": "...",
      "ts_last": "...",
      "bytes": 917304,
      "digest": "b3k:..."
    }
  ],
  "segments": [
    {
      "key": "seg/000...-000....seg",
      "idx": "seg/000...-000....idx",
      "first": 1,
      "last": 48209999,
      "ts_first": "...",
      "ts_last": "...",
      "bytes": 4193941021,
      "digest": "b3k:..."
    }
  ],
  "trim_before_seq": 0,
  "updated": "2026-08-21T10:14:58Z"
}
```

The WAL list is bounded by compaction. Readers poll or receive hints about this small manifest rather than list the entire object prefix.

### 8.6 Chunk and segment format

Initial Pheromone frame format:

```text
u32 length
u64 sequence
u64 timestamp_ms
u16 flags
bytes canonical-envelope-json
```

Chunks are zstd-compressed and have a CRC32C trailer. Segments concatenate and recompress chunks. The index provides:

- sparse sequence-to-offset mapping;
- timestamp-to-sequence mapping;
- event-ID-to-sequence mapping.

Parquet MAY be emitted later for analytics but is not the primary replay format.

### 8.7 Append protocol

For one partition leader:

1. Fronts route appends to the current leader.
2. The leader group-commits until `commit_window` or `commit_bytes` is reached.
3. It assigns positions `head + 1 ... head + n`.
4. It writes a new immutable WAL chunk using create-only semantics.
5. It constructs a manifest that references the new chunk and advances `head_seq`.
6. It conditionally replaces the manifest using the observed provider version.
7. If the epoch changed, the leader is fenced and stops.
8. If another same-epoch maintenance update won, it reloads, merges, and retries where safe.
9. It acknowledges the batch only after the manifest update succeeds.

Default provisional batching:

```text
commit_window = 10 ms
commit_bytes  = 4 MiB
```

A chunk uploaded but not referenced by a committed manifest is logically nonexistent. It is later removed by an orphan sweeper.

### 8.8 Ambiguous acknowledgement and idempotency

If a leader commits the manifest and dies before replying, a client may retry. The client MUST provide stable event IDs or a stable operation ID. Admission deduplication MUST ensure that the retry does not create a second logical event.

Comb Log itself guarantees a stable append result only when the caller uses the idempotency contract. It does not infer external equivalence between arbitrary payloads.

### 8.9 Leadership

The partition manifest acts as both lock and head.

- Acquisition increments the epoch through a conditional update.
- Renewal conditionally updates the same epoch.
- Every append carries the epoch.
- WAL keys include the epoch to prevent key collision.
- Fronts cache the current leader and refresh on `Fenced`.
- A stale leader may upload orphan chunks but cannot make them visible.

### 8.10 Read and follow

Follower behavior:

1. Read the manifest ref. A conditional read on the provider version token MAY be used to save bandwidth.
2. Determine change by comparing logical state — `head_seq` for a log manifest, `generation` for any other ref — with the last observed value. A changed provider token is not by itself a change: lease renewals (§7.6) rewrite the ref every `renew` interval without changing logical state, and a token-keyed follower would wake and refetch on every renewal. If unchanged, wait for the poll interval or a hint (§7.5a.6).
3. If changed, fetch newly referenced chunks or segments.
4. Verify and place them in the local immutable cache.
5. Serve sequential reads from cache.

Default poll interval:

```text
poll = 250 ms
```

A WebSocket, gossip, or in-process notification MAY reduce tail latency. Hints are not authoritative; the manifest remains the truth.

Read-scoped direct bucket following MAY be supported. Write access remains leader-mediated.

### 8.11 Compaction and retention

The leader or a separately fenced compactor:

- merges old WAL chunks into immutable segments;
- builds the side index;
- conditionally replaces the manifest entries;
- retains the old objects for a grace period;
- advances `trim_before_seq` for retention;
- deletes fully unreachable segments only after grace and reachability checks.

Provisional triggers:

```text
compact_after  = 5 min
max_wal_chunks = 256
orphan_grace   = 1 h
```

A compaction and append race MUST resolve through conditional-update retry. No committed WAL entry may disappear unless its records are present in a referenced segment.

### 8.12 Partitions

A trail begins with one partition. Partitioning is an explicit operator action.

Example:

```text
pher trail partition <trail> --by subject --n 8
```

Routing:

```text
hash(subject) % partition_count
```

Ordering is per partition. Cursors become vectors of positions. Cross-region movement is implemented as a bridge, not active-active mutation of one partition.

### 8.13 Pheromone control-plane boundary

Pheromone-specific subscriptions, named cursors, grants, bridges, shard leases, and delivery history remain above Comb Log.

A low-write-rate `control/manifest.json` MAY initially use the same conditional-update discipline. If that object becomes hot, it MAY move behind PostgreSQL through an internal trait. Comb Core MUST NOT force all tenants or resources through one global control manifest.

### 8.14 Queue semantics

A general queue is not part of Comb Log v1.

A future Comb Queue would add:

- consumer groups;
- claims and visibility deadlines;
- acknowledgements;
- retries;
- dead-letter policy;
- attempt counts;
- queue-specific idempotency.

It MUST be built as a higher-level view over logs rather than weakening the log contract.

### 8.15 Log performance targets

These are engineering targets, not contractual guarantees:

- publish append p50 and p99 by backend at 1, 10, and 100 producers;
- publish follower-tail latency;
- publish manifest conditional-update ceiling;
- replay at least 200 MB/s per follower in the 1-billion-event drill with flat memory usage;
- preserve the existing local SQLite performance envelope after ingest/match separation.

---

## 9. Comb Tree

### 9.1 Purpose

Comb Tree stores immutable hierarchical namespaces and directly retrievable artifacts.

Primary uses:

- canonical repository images;
- checked-out source trees;
- uploaded Git bundles;
- ChangeRefs;
- evidence and screenshots;
- transcripts;
- selected build outputs;
- user-downloadable artifacts;
- setup metadata;
- other content where file-level identity and selective materialization are useful.

Comb Tree is not a concurrent writable network filesystem. Mutations occur in a local materialized workspace or volume and are published as a new immutable tree.

### 9.2 Merkle tree model

A tree node contains sorted entries:

```json
{
  "schema": "comb.tree-node/v1",
  "entries": [
    {
      "name": "package.json",
      "kind": "file",
      "target": "b3k:...",
      "size": 2211,
      "mode": 33188
    },
    {
      "name": "src",
      "kind": "tree",
      "target": "b3k:...",
      "mode": 16877
    },
    {
      "name": "current",
      "kind": "symlink",
      "target_text": "releases/42"
    }
  ]
}
```

Requirements:

- entry names are encoded and compared canonically;
- entries are sorted so identity is deterministic;
- file content is stored as one blob or a chunk-list manifest depending on size;
- executable mode and symlink semantics are preserved;
- unsupported special files are rejected or explicitly represented;
- timestamps SHOULD be treated as metadata rather than identity unless a use case requires exact preservation;
- ownership and extended attributes are opt-in because host semantics differ.

### 9.3 Large files

Large files SHOULD be represented by a chunk-list manifest rather than one monolithic object:

```json
{
  "schema": "comb.file/v1",
  "size": 9126805504,
  "chunks": [
    { "offset": 0, "length": 4194304, "digest": "b3k:..." }
  ]
}
```

Chunking MAY initially be fixed-size for simplicity. Content-defined chunking MAY be evaluated for large artifacts and Git packs if measurements show meaningful reuse.

### 9.4 Tree refs and publication

A tree ref points to one immutable root node and optional metadata:

```json
{
  "target": "b3k:tree-root",
  "source_commit": "...",
  "source_bundle": "b3k:...",
  "created_by": "operation-id",
  "created_at": "..."
}
```

Publishing a repository or artifact tree is:

1. upload missing file blobs;
2. upload bottom-up tree nodes;
3. verify the root;
4. conditionally advance the intended ref.

### 9.5 Repository image model

A repository image is tenant-scoped and product-scoped. It includes enough data to provision an independent mutable Git workspace without Git alternates or hardlinks.

Recommended logical contents:

```text
repository image
├── packed .git object database and refs
├── canonical checked-out tracked tree or checkout metadata
├── source identity and synchronization metadata
└── optional hot-file/materialization profile
```

Git alternates and hardlinks MUST NOT be used to fake isolation because an agent can mutate `.git`. Native reflinks, snapshots, OverlayFS copy-up, or a Comb Volume overlay provide safe separation.

Git packfiles SHOULD be treated as immutable:

- append small packs for incremental synchronization;
- avoid frequent full repacks;
- compact only after pack-count or reclaim thresholds;
- build compaction output as a staging generation;
- publish atomically;
- keep old bytes while reachable from existing Cells.

### 9.6 Tree materialization modes

#### Full materialization

Fetch and write every file before use. Appropriate when a workload requires all bytes locally or the tree is small.

#### Selective materialization

Fetch requested paths on demand through an SDK, FUSE-style adapter, or application integration.

#### Native clone

Materialize one node-local base, then clone it using APFS clone, XFS/btrfs reflink, ZFS clone, or another native facility.

#### Linux tree overlay

On ext4 or another filesystem without block reflinks, OverlayFS MAY share one read-only lower directory across many Cells. Each Cell gets an independent upper and work directory.

Important limitation: OverlayFS copies up an entire file when that file is modified. It does not provide block-level copy-on-write. This is acceptable for mostly immutable Git packfiles and source trees but is not a universal replacement for Comb Volume.

#### Physical copy

Correct final fallback for low concurrency. The scheduler MUST reserve the complete source size and SHOULD avoid high-density placement.

### 9.7 Current and staging generations

For each repository image:

- at most one generation is `current` for new provisioning;
- at most one generation is `staging` under construction;
- publication is one conditional ref update;
- a failed staging build cannot corrupt the current generation;
- the previous current generation is unpublished after successful promotion;
- unpublished does not mean immediately deleted;
- long-lived Cells continue to pin all reachable old objects;
- physical deletion occurs only after reachability and grace checks.

A full repository compaction requires temporary space approximately equal to one additional packed repository. For the representative Apiary repository, the scheduler and local cache manager should expect roughly 300–350 MB of temporary headroom.

### 9.8 Local repository-image cache

Each node maintains an LRU-governed set of inactive repository images.

The cache manager MUST:

- preserve a configured free-space floor;
- account for staging and compaction headroom;
- avoid evicting bases used by active Cells;
- expose cached-template hints to the scheduler;
- prefer evicting inactive and expensive-to-retain generations;
- treat remote authoritative state as recoverable and local cache state as disposable.

### 9.9 Artifacts and evidence

ChangeRefs, evidence, transcripts, screenshots, test reports, and selected outputs SHOULD be stored as first-class Comb blobs or trees rather than only inside a Cell disk.

Benefits:

- retain a small result after deleting a large Cell;
- search and inspect without mounting a volume;
- assign separate retention and access policies;
- export directly;
- bill actual retained bytes;
- attach evidence to a Cell or run manifest by digest.

A Cell manifest may reference these objects:

```json
{
  "volume_head": "b3k:...",
  "change_ref": "b3k:...",
  "transcript": "b3k:...",
  "evidence": ["b3k:...", "b3k:..."]
}
```

### 9.10 Tree API

Conceptual operations:

```rust
trait TreeStore {
    async fn put_blob(&self, tenant: TenantId, bytes: ByteStream) -> Result<ObjectId>;
    async fn put_tree(&self, entries: Vec<TreeEntry>) -> Result<TreeId>;
    async fn resolve_ref(&self, resource: TreeResource, name: RefName) -> Result<TreeSnapshot>;
    async fn publish_ref(&self, resource: TreeResource, name: RefName,
                         expected: RefVersion, root: TreeId,
                         metadata: TreeMetadata) -> Result<TreeSnapshot>;
    async fn diff(&self, from: TreeId, to: TreeId) -> Result<TreeDiff>;
    async fn materialize(&self, root: TreeId, destination: Path,
                         policy: MaterializationPolicy) -> Result<Materialization>;
}
```

---

## 10. Comb Volume

### 10.0 Scope and gating (v0.2)

Comb Volume is a **separately gated track**, not a v1 deliverable in its portable form. The contract in §10.2 and the lifecycle operations in §10.8 are stable and are implemented in Phase E over native facilities — APFS clone, btrfs/XFS reflink, ZFS, LVM-thin, and cloud-provider block snapshots. The portable object-backed block overlay (§10.4–10.6, the `ublk` driver) is Phase F and is gated on two decisions:

1. **Cloud Cells and Nectar previews run on provider-native snapshots first.** The provider-native driver is a complete implementation of the `CombVolume` contract, not a stopgap. This closes open question 26.4.6.
2. **Prepared dependency trees are measured as Comb Tree before Volume is assumed for them.** This converts open question 26.3.3 into a gate: if a lazily materialized Tree of `node_modules` and build output meets Cell cold-start and density targets on ext4 satellites through OverlayFS, the portable block overlay is deferred further. Block-level storage is required where arbitrary disk state must be snapshotted — databases inside Cells, process-memory images — and not, by default, for source and dependency trees.

The rest of this section remains normative for whichever driver is in use.

### 10.1 Purpose

Comb Volume provides ordinary block-device semantics to one writer while storing committed snapshots as immutable content-addressed block maps.

Primary uses:

- post-setup Cell root filesystems;
- cloud sandbox disks;
- dependency and build environments;
- portable snapshots on hosts without native reflinks;
- pause, retain, fork, migrate, and restore;
- optional process- or VM-state association.

### 10.2 Core contract

A volume consists of:

```text
immutable base snapshot
        +
one writable local overlay
        +
zero or more committed snapshot roots over time
```

The v1 volume contract is single-writer:

- exactly one writable owner at one epoch;
- any number of read-only consumers of immutable snapshots;
- no concurrent multi-writer mount;
- metadata-only fork from a committed snapshot;
- atomic checkpoint publication through one branch ref.

### 10.3 Logical geometry

Provisional defaults:

```text
logical sector size: 4 KiB
content chunk size: 4 MiB
volume size: fixed logical capacity per volume version
sparse zero regions: represented implicitly
```

The chunk size MUST be configurable and format-tagged. The initial implementation SHOULD favor fixed-size aligned chunks because they simplify random access, range mapping, caching, and block-device implementation.

A small write may cause one full changed content chunk to be emitted at checkpoint. Sub-chunk delta encoding or variable chunking is a later optimization and MUST NOT complicate initial correctness.

### 10.4 Persistent block map

A volume root points to an immutable persistent radix or Merkle tree mapping logical chunk indexes to content objects.

Conceptual leaf:

```json
{
  "schema": "comb.volume-leaf/v1",
  "start_chunk": 1024,
  "entries": [
    { "chunk": 1024, "digest": "b3k:..." },
    { "chunk": 1025, "digest": null },
    { "chunk": 1026, "digest": "b3k:..." }
  ]
}
```

A null digest means a sparse zero chunk or inheritance according to the node format. The mapping structure SHOULD share unchanged internal nodes across snapshots so that a checkpoint rewrites only the paths covering changed chunks.

The root manifest records:

```json
{
  "schema": "comb.volume-manifest/v1",
  "volume_id": "vol_01...",
  "logical_bytes": 85899345920,
  "sector_bytes": 4096,
  "chunk_bytes": 4194304,
  "root_map": "b3k:...",
  "parent": "b3k:... or null",
  "filesystem": "ext4|xfs|unknown",
  "consistency": "crash-consistent|application-consistent",
  "created_at": "...",
  "metadata": {}
}
```

### 10.5 Writable overlay

The active node maintains:

- a sparse local data file or native writable snapshot;
- a dirty bitmap at sector or sub-chunk granularity;
- a local write journal sufficient for the selected durability mode;
- the base snapshot ID;
- the current writer epoch;
- checkpoint operation state.

Read resolution:

```text
1. dirty local overlay
2. node-local verified chunk cache
3. immutable base snapshot in object storage
4. implicit zero
```

Write resolution:

```text
write to local overlay
→ update dirty metadata
→ satisfy local or replicated acknowledgement policy
→ checkpoint later, or object-commit synchronously when configured
```

### 10.6 Checkpoint protocol

A checkpoint operation MUST be idempotent and fenced.

1. Verify ownership and epoch.
2. Establish the requested consistency boundary.
3. Freeze or quiesce writes, or take a native snapshot of the writable layer.
4. Enumerate dirty chunks.
5. Reconstruct complete changed chunks from base plus dirty sectors.
6. Compute content digests.
7. Upload missing chunks create-only.
8. Write new immutable block-map nodes.
9. Write the new immutable volume manifest.
10. Conditionally advance the branch head using the expected ref version and epoch.
11. Release the consistency boundary.
12. Mark old local dirty state reclaimable only after the commit result is known.
13. Return the new snapshot ID.

Nothing is object-durably checkpointed until step 10 succeeds.

If the process dies after immutable uploads but before head publication, those objects are unreachable orphans and are collected later.

If the head update succeeds but the reply is lost, retrying with the same operation ID MUST return the already committed snapshot rather than publish an unintended duplicate state.

### 10.7 Consistency classes

#### Crash-consistent

Equivalent to machine power loss after acknowledged underlying writes. The filesystem may replay its journal on attach.

#### Filesystem-consistent

The filesystem is frozen or otherwise flushed so on-disk metadata is internally consistent.

#### Application-consistent

The guest or application runs a workload-specific quiescence hook before snapshot and resumes afterward.

Cells SHOULD default to filesystem-consistent checkpoints when a safe guest integration exists, and otherwise document crash-consistent behavior.

Process-memory snapshotting is a separate operation and MUST NOT be implied by a disk checkpoint.

### 10.8 Attach drivers

All drivers implement the same logical operations:

```text
publish
clone
attach
checkpoint
fork
retain
release
verify
```

#### Native snapshot driver

Uses btrfs, ZFS, LVM-thin, cloud-provider block snapshots, or another native snapshot facility. This is the preferred fast path where operationally available.

#### Native reflink driver

Uses APFS clone or XFS/btrfs reflink to clone image files or directories. It remains subject to the host filesystem's snapshot and durability properties.

#### Tree-overlay driver

Uses OverlayFS for source and repository trees. It is not a general block-volume implementation and copies up whole modified files.

#### Comb block-overlay driver

Uses a userspace virtual block device, likely Linux `ublk` or an equivalent mechanism, backed by:

- immutable object-store chunks;
- shared node-local cache;
- sparse per-Cell dirty overlay;
- checkpoint logic.

This is the portable high-density fallback for ext4 and the intended cloud implementation when provider-native snapshots are unsuitable.

#### Copy driver

Copies the complete base. It is correct but low density and SHOULD be restricted by policy.

### 10.9 Fork

Forking a committed snapshot is metadata-only:

1. resolve the source snapshot;
2. create a pin for the new branch or Cell;
3. create a new branch ref pointing at the same immutable root;
4. attach an empty writable overlay.

The first changed chunk consumes new physical storage at checkpoint. Unchanged chunks remain shared within the tenant.

### 10.10 Migration and takeover

To move a writable Cell:

1. checkpoint under the current writer where possible;
2. acquire a new writer epoch on the destination;
3. fence the previous writer;
4. resolve the latest committed head;
5. attach lazily on the destination;
6. resume writes in a new overlay.

If the source node disappears before checkpoint:

- `ephemeral` and `local` policies may lose uncommitted state;
- `checkpointed` loses at most the declared checkpoint window;
- `replicated` recovers from the journal according to its quorum guarantee;
- `object` recovers all acknowledged object-durable writes.

### 10.11 Process and VM state

Process or VM snapshots MAY be associated with a volume snapshot but are stored as separate immutable objects with stricter security and retention.

They may contain:

- credentials;
- tokens;
- decrypted user data;
- environment variables;
- memory-resident secrets.

Therefore:

- they MUST be encrypted;
- access MUST be narrower than ordinary artifacts;
- retention SHOULD default shorter;
- export SHOULD require explicit authorization;
- template publication MUST scrub or prohibit secrets.

### 10.12 Volume API

Conceptual operations:

```rust
trait VolumeStore {
    async fn create(&self, spec: VolumeSpec, op: OperationId) -> Result<Volume>;
    async fn resolve(&self, volume: VolumeId, branch: RefName) -> Result<VolumeHead>;
    async fn acquire_writer(&self, volume: VolumeId, branch: RefName,
                            node: NodeId) -> Result<WriterLease>;
    async fn attach(&self, head: VolumeHead, policy: AttachPolicy) -> Result<Attachment>;
    async fn checkpoint(&self, attachment: AttachmentId,
                        consistency: ConsistencyClass,
                        op: OperationId) -> Result<SnapshotId>;
    async fn fork(&self, snapshot: SnapshotId, destination: BranchSpec,
                  op: OperationId) -> Result<VolumeHead>;
    async fn release(&self, attachment: AttachmentId) -> Result<()>;
    async fn verify(&self, snapshot: SnapshotId, mode: VerifyMode) -> Result<VerifyReport>;
}
```


---

## 11. Apiary Cell integration

### 11.1 Resource hierarchy

The recommended hierarchy is:

```text
tenant / organization
└── product
    ├── repository resource
    │   ├── current repository generation
    │   └── staging repository generation
    ├── setup template resource
    │   ├── current setup generation
    │   └── staging setup generation
    └── Cells
        ├── Cell head and volume branch
        ├── active attachment
        ├── artifacts and evidence
        └── retention policy
```

Identifiers MUST be opaque, stable, and globally unique within their type. Human-readable product names are metadata, not storage keys.

### 11.2 Repository synchronization

The two-layer satellite plan remains:

1. `pro sync` or origin provisioning maintains the satellite's canonical checkout.
2. Honeybee builds one node-local repository image from that checkout.
3. Every Cell on that satellite materializes from the local image.

No repository image is retransmitted for every Cell. Unpushed commits continue to move through the existing Git-bundle synchronization lane.

For cloud provisioning:

- Git-host lane fetches the repository once into the organization/product image;
- snapshot-push lane uploads a content-addressed bundle once;
- both produce a repository generation with an immutable identity;
- setup runs from that generation and publishes a post-setup volume snapshot.

### 11.3 Setup identity

A setup generation SHOULD be content-derived from all non-secret inputs that determine the result.

Conceptual identity:

```text
setup_identity = hash(
    repository_generation,
    source_commit_or_bundle,
    runtime_base_image_digest,
    architecture,
    operating_system,
    setup_command_and_version,
    relevant_lockfile_digests,
    toolchain_version,
    non_secret_setup_configuration
)
```

A change to any material input creates a new setup identity.

Secrets MUST NOT become reusable template inputs or published template contents. Setup logic MUST use temporary secret injection and scrub secret-bearing files, sockets, environment state, cloud metadata, SSH material, and runtime identifiers before publication.

### 11.4 Setup build and publication

```text
repository generation
        ↓ materialize
isolated setup builder Cell
        ↓ run deterministic setup where possible
quiesce and scrub
        ↓
immutable post-setup snapshot
        ↓ validate
conditional publish as current setup generation
```

Protocol:

1. Resolve and pin the repository generation.
2. Create an isolated builder Cell.
3. Materialize according to the setup policy.
4. Run setup.
5. Record build logs and selected outputs as Comb artifacts.
6. Scrub secrets and machine-specific state.
7. Establish a filesystem-consistent snapshot.
8. Validate boot, health checks, and expected files.
9. Publish the staging setup ref.
10. Conditionally promote staging to current.
11. Unpublish the previous current generation for new provisioning.
12. Release temporary pins and builder state.

A failed build MUST leave the current generation unchanged.

### 11.5 Cell provisioning protocol

```text
1. resolve current TemplateRef
2. create short provisioning pin
3. choose node and storage driver
4. ensure required base metadata is available
5. attach base snapshot
6. prefetch boot-critical or predicted-hot data
7. create empty writable overlay
8. acquire Cell writer epoch
9. start compute
10. convert provisioning pin into active Cell root
```

The operation MUST be idempotent by `provisioning_operation_id`.

A retry after partial failure must either:

- resume the same Cell creation;
- return the already created Cell;
- or cleanly abandon the incomplete resource and release its expiring pin.

It MUST NOT create unbounded duplicate Cells because an acknowledgement was lost.

### 11.6 Cell head

Conceptual schema:

```json
{
  "schema": "apiary.cell-head/v1",
  "cell_id": "cell_01...",
  "tenant": "org_01...",
  "product": "product_01...",
  "template_snapshot": "b3k:...",
  "volume_snapshot": "b3k:...",
  "branch_ref": "volumes/vol_01/branches/cell_01",
  "writer_epoch": 17,
  "durability": "checkpointed",
  "materialization": "lazy",
  "logical_disk_bytes": 85899345920,
  "artifacts": [],
  "created_at": "...",
  "updated_at": "..."
}
```

The Cell record in the hosted database may contain additional scheduling and UI state. The durable storage identity and snapshot reachability MUST remain recoverable from Comb data.

### 11.7 Read path

```text
Cell read
   ↓
per-Cell dirty overlay
   ↓ miss
shared node-local verified cache
   ↓ miss
Comb immutable object backend
```

The node MAY prefetch:

- boot and init ranges;
- package manifests and lockfiles;
- known hot directories;
- data observed during the setup-build trace;
- runtime binaries;
- working-tree metadata.

A Cell may start before the complete base is resident when its driver and policy support lazy materialization.

### 11.8 Write path

```text
Cell write
   ↓
local writable overlay
   ↓
local journal / replicated journal / object checkpoint according to policy
   ↓
immutable changed chunks
   ↓
new volume root
   ↓
conditional Cell branch-head update
```

The ordinary Cell write path MUST NOT synchronously send every transient build-cache write to S3 unless the user explicitly selected object-durable writes.

### 11.9 Cell data classes

Cell files SHOULD be assigned to one of five treatment classes.

| Class | Examples | Default treatment |
|---|---|---|
| Shared immutable base | source generation, runtime, prepared dependencies | tenant-scoped snapshot and cache |
| Persistent workspace | agent edits, uncommitted code, generated configuration | checkpointed or replicated |
| Rebuildable cache | compiler cache, package cache, transient build tree | local or optionally checkpointed |
| Durable evidence | ChangeRefs, transcripts, screenshots, reports, selected outputs | first-class Comb Tree/Blob objects |
| Ephemeral runtime | `/tmp`, sockets, disposable logs, transient process state | local only unless explicitly retained |

Apiary SHOULD support path- or mount-level policy so rebuildable caches do not dominate durable overlay storage.

### 11.10 Cell durability modes

User-facing modes:

#### `scratch`

- ephemeral or local durability;
- fastest and cheapest;
- node loss may lose uncheckpointed work;
- appropriate for disposable tests.

#### `checkpointed`

- local writes during execution;
- automatic object-durable checkpoints by time, dirty-byte threshold, lifecycle event, or explicit command;
- pause, retain, migrate, and graceful stop wait for a successful checkpoint;
- node loss may lose changes since the last committed checkpoint.

#### `durable`

- acknowledged persistent writes use a replicated regional journal when available, or object durability when explicitly configured;
- checkpoints compact journaled state into object-backed snapshots;
- recovery behavior is published as a concrete quorum and failure-domain guarantee.

Comb internal profiles are described in Section 14.

### 11.11 Lifecycle operations

#### Pause

- quiesce the Cell;
- checkpoint according to policy;
- optionally snapshot process state;
- release compute;
- retain the volume root and selected artifacts.

#### Resume

- resolve retained root;
- schedule a node;
- attach lazily;
- acquire a new writer epoch;
- restore process state when compatible and requested;
- otherwise boot from the disk snapshot.

#### Fork

- checkpoint or choose an existing snapshot;
- create a new Cell head referencing the same snapshot;
- create an empty overlay;
- acquire an independent writer epoch.

#### Stop and discard

- stop compute;
- do not create a final checkpoint unless policy requires it;
- remove active roots after a grace period;
- retain first-class evidence according to its own policy.

#### Retain

- ensure the selected checkpoint is object durable;
- create a retention pin;
- record retention duration or legal hold;
- release active compute and local overlay after verification.

### 11.12 Storage capability advertisement

A satellite or cloud node advertises more than one coarse string.

```text
storage.mode =
  native_snapshot
  native_reflink
  tree_overlay
  comb_block_overlay
  copy

storage.free_bytes
storage.cache_capacity_bytes
storage.cache_used_bytes
storage.minimum_free_floor_bytes
storage.maximum_active_overlays
storage.supports_lazy_materialization
storage.supports_durable_journal
storage.checkpoint_bandwidth_bytes_per_s
storage.object_read_bandwidth_bytes_per_s
storage.object_write_bandwidth_bytes_per_s
storage.cached_templates[]
storage.native_snapshot_backend
storage.filesystem
```

The scheduler MUST treat capabilities as observed facts, not operator assumptions. Startup self-tests SHOULD verify advertised clone, snapshot, sparse-file, freeze, and direct-I/O behavior.

### 11.13 Scheduling

Estimated immediate reservation:

```text
reservation =
    expected_dirty_overlay
  + missing_hot_working_set
  + checkpoint_or_compaction_headroom
  + safety_margin
```

Driver-specific rules:

- `copy`: reserve the full source image plus expected changes;
- `native_reflink` or `native_snapshot`: reserve expected divergence plus metadata and safety margin;
- `tree_overlay`: reserve expected copied-up files and upperdir growth;
- `comb_block_overlay`: reserve expected divergence, dirty journal, and missing hot set;
- all modes: preserve the node's free-space floor.

A node below its free-space floor MUST reject new provisioning and SHOULD trigger eviction of inactive cache entries. It MUST NOT silently consume emergency headroom needed for checkpoint or compaction.

### 11.14 Density example

For the representative Apiary repository:

- a copy-only satellite provisioning 100 Cells may consume roughly 30 GB for Git-image data alone, before working trees, dependencies, or changes;
- a native or Comb copy-on-write backend stores the base once per node and charges Cells primarily for divergence;
- dependencies and build outputs are likely to dominate the 300–350 MB Git image in cloud operation;
- an advertised 20–80 GB Cell disk is a logical capacity limit, not an indication that the full amount is physically allocated.

### 11.15 Generation lifecycle

Repository and setup generations use:

```text
building → staging → validated → current → unpublished → unreachable → deleted
```

Rules:

- only current is eligible for new ordinary provisioning;
- staging is protected by a staging pin;
- publication is atomic;
- previous current becomes unpublished after promotion;
- existing Cells continue using old generations;
- old objects remain until no active or retained root reaches them;
- physical deletion follows a grace period;
- a generation may be restored to discoverable status only through an explicit new ref update.

### 11.16 Cloud tenant isolation

- templates are organization-scoped;
- private data is never deduplicated across organizations;
- local decrypted cache entries are keyed by tenant and digest;
- nodes require tenant-scoped authorization before attach;
- process-state snapshots receive stricter handling;
- provider-side and application-level encryption policies are recorded with the resource.

### 11.17 Cell API surface

Conceptual service operations:

```text
CreateRepositoryGeneration
BuildSetupGeneration
PublishSetupGeneration
CreateCell
AttachCell
CheckpointCell
PauseCell
ResumeCell
ForkCell
RetainCell
ReleaseCell
ListCellArtifacts
ExportCell
VerifyCell
```

Every mutating operation MUST accept an idempotency key.

---

## 12. Nectar integration

### 12.0 What Nectar needs from Comb v1

Nectar's originating requirement was *cloud distribution*: start a hosted preview of any app on any node in any region without re-fetching or re-building per preview, and reconstruct it elsewhere if the node dies. Against Comb v1 that is:

| Nectar need | Comb mechanism | Phase |
|---|---|---|
| fetch a repository once, fan out many previews | repository image as a Tree generation (§9.5, §9.7) with packs as immutable objects; built once per generation, pulled by every node | D |
| run setup once per source and setup identity | setup template published as a release manifest (§7.5a.5) over a provider-native snapshot (§10.0) | E |
| start a preview on any node | any authorized `combd` resolves the refs and lazily materializes from the tenant bucket through the node cache (§15) | D/E |
| start fast on a cold node | hot-set prefetch (§15.4–15.5) over Tree; block-level lazy attach only once Phase F exists | D, later F |
| survive node loss | preview state lives in refs and objects; recovery is re-attach from the last committed snapshot under a new epoch (§10.10) | E |
| keep valuable work out of an opaque disk | ChangeRefs, build outputs, and evidence are first-class Trees and Blobs (§9.9, §12.3) | D |
| regional placement and EU residency | tenant bucket per region (§16.8); routing stays in the Nectar control plane | G |

Nothing on this list requires the portable block overlay. Nectar's hosted beta therefore ships on Tree plus native snapshots, which is why Phase G precedes Phase F in the v0.2 plan (§23.1).

### 12.1 Preview creation

Nectar uses the same repository and setup-generation pipeline as Apiary Cells.

```text
source ref or bundle
→ repository generation
→ setup generation
→ preview Cell fork
→ start web process
→ publish routing metadata
```

Preview routing metadata belongs in the Nectar control plane. Filesystem and artifact state belongs in Comb.

### 12.2 Preview persistence

Nectar preview policies may include:

- disposable preview: no final checkpoint;
- workspace preview: checkpoint source edits and selected state;
- retained preview: retain complete volume root for a configured period;
- artifact-only preview: delete volume after extracting build output and evidence.

### 12.3 Deployment artifacts

Static build outputs or deployment bundles SHOULD be published as Comb Trees or Blobs. They SHOULD NOT require a running Cell or mounted volume for serving or export.

### 12.4 Branching

A preview branch MAY be represented by:

- a Git/source tree ref;
- a Cell volume branch;
- or both.

Source changes intended for review SHOULD be exported as a ChangeRef or Git bundle even when the complete workspace volume is retained. This prevents the volume from becoming the only representation of valuable work.

### 12.5 POSIX boundary

Nectar applications see an ordinary local filesystem through the Cell. Comb does not initially expose a shared remote POSIX mount to multiple writers. The local filesystem view is backed by native snapshots or the Comb block-overlay driver.

---

## 13. Pheromone integration summary

### 13.0 What Pheromone needs from Comb v1

Pheromone's originating requirement was *trail handling at scale*: a cloud trail that retains billions of events, survives machine loss, supports many followers, replays from any retained position, and permits safe writer takeover — without regressing the local single-daemon mode. Against Comb v1 that is Comb Core plus Comb Log and nothing else:

| Pheromone need | Comb mechanism | Phase |
|---|---|---|
| durable append with honest acknowledgement | WAL chunk uploaded create-only, then manifest conditionally advanced; acknowledge only after the ref update (§8.7) | C |
| per-trail order | one partition manifest is the linearization point (§6.2, §8.5) | C |
| safe writer takeover | epoch fencing on the partition manifest (§7.6, §8.9) | B/C |
| many followers, read scaling | followers read the manifest and immutable chunks through cache with no leader involvement (§8.10) | C |
| exact replay from any retained position | immutable chunks and segments with sparse indexes (§8.6) | C |
| retention at object-storage cost | compaction into segments, `trim_before_seq`, reachability GC (§8.11, §19) | C |
| durable `why <delivery-id>` | deliveries appended to a Comb Log (§13.5) | C |
| local mode unchanged | `SqliteLog` behind the same `TrailLog` trait (§8.2, §13.1) | A |

Pheromone is also Comb's change-hint channel (§7.5a.6). That dependency is one-directional: Pheromone is built on Comb Log; Comb uses Pheromone only for latency and never for correctness.

### 13.1 Local mode remains

Pheromone local mode keeps SQLite and its low-latency, no-infrastructure behavior.

```toml
[store]
kind = "sqlite"
```

The ingest-to-log-to-follower refactor applies locally and should not regress semantics or performance.

### 13.2 Cloud mode

```toml
[store]
kind = "object"
url = "s3://pher-prod/acme"
credentials = "hem:project/pheromone/s3"
commit_window = "10ms"
commit_bytes = "4MiB"
lease = "30s"
poll = "250ms"
```

MinIO uses the same logical path with endpoint configuration.

### 13.3 Preserved semantics

- per-partition order;
- at-least-once delivery;
- stable `deliveryId` deduplication;
- exact retained replay;
- bridge semantics;
- grants enforced at emit and listen boundaries;
- no direct bucket writes by clients;
- local daemon and mesh behavior remain available.

### 13.4 Direct follow

A read grant MAY materialize as a short-lived scoped credential limited to one tenant and trail prefix. This permits direct bucket following without a continuously running cloud Pheromone daemon. Encryption-key access, when application encryption is enabled, MUST be scoped with the same or narrower lifetime and permissions.

### 13.5 Pheromone-specific state

Judge caches, forwarded-dedup windows, and other cost-only state MAY remain local and rebuildable. Durable deliveries are appended to a Comb Log so explanations such as `why <delivery-id>` survive process and machine loss.

---

## 14. Durability and materialization profiles

### 14.1 Internal durability profiles

| Profile | Acknowledgement condition | Survives | Does not necessarily survive |
|---|---|---|---|
| `ephemeral` | memory or disposable overlay accepts write | process lifetime as configured | process, node, or disk loss |
| `local` | local durable storage has flushed according to driver contract | process restart; often local host restart | host or local-disk loss |
| `checkpointed` | ordinary writes use local profile; explicit/automatic checkpoint waits for object ref commit | object-durable committed checkpoints | writes after latest checkpoint |
| `replicated` | configured quorum of independent journal replicas acknowledges | failures within the published quorum/failure-domain guarantee | larger correlated failure; untiered state beyond guarantee |
| `object` | immutable objects and authoritative ref/manifest commit | any compute-node loss; backend durability domain | backend-wide failure beyond provider guarantee |

The exact meaning of `replicated` MUST name:

- replica count;
- write quorum;
- zones or failure domains;
- flush semantics;
- recovery-point objective;
- behavior during degraded quorum.

“Replicated” without these details is not a sufficient guarantee.

### 14.2 Initial defaults by workload

| Workload | Default |
|---|---|
| Pheromone local trail | SQLite local durability |
| Pheromone cloud trail | `object` before append acknowledgement |
| disposable Cell | `ephemeral` or `local` |
| normal Apiary Cell | `checkpointed` |
| retained or migration-critical Cell | `checkpointed` with lifecycle barrier |
| future high-durability Cell | `replicated`, then object-tiered |
| immutable artifact upload | `object` before publish acknowledgement |
| template publication | `object` before current-ref update acknowledgement |

### 14.3 Materialization profiles

#### `lazy`

Start after metadata and minimum boot-critical content are available. Fetch remaining chunks on demand.

#### `hotset`

Fetch a predicted or recorded hot set before start, then materialize lazily.

#### `full`

Do not start the workload until the full snapshot is verified locally.

#### `cache-only`

Require that required content is already resident on an approved local cache. Used only for tightly controlled latency-sensitive placement.

### 14.4 Policy composition

A resource policy is a combination, not one opaque tier:

```json
{
  "write_durability": "checkpointed",
  "checkpoint_interval_seconds": 30,
  "checkpoint_dirty_bytes": 536870912,
  "lifecycle_checkpoint": true,
  "read_materialization": "hotset",
  "retention": "7d",
  "logical_capacity_bytes": 85899345920
}
```

### 14.5 Object-store outage behavior

For `object`:

- append/checkpoint/publish blocks or returns an explicit unavailable error;
- no success is returned;
- clients may retry with the same operation ID.

For `checkpointed`:

- local work MAY continue until a configured dirty-byte or time safety limit;
- the UI and scheduler MUST expose degraded durability;
- lifecycle operations requiring persistence fail rather than pretend completion;
- policy MAY pause the Cell when the uncheckpointed window exceeds a limit.

For `replicated`:

- writes MAY continue while the configured quorum is healthy;
- tiering backlog is monitored and bounded;
- exhaustion or loss of quorum follows explicit backpressure or fail-stop policy.

---

## 15. Comb Cache and materialization

### 15.1 Cache properties

Comb Cache stores verified immutable objects or native materializations derived from them.

- cache entries are keyed by tenant, object ID, format, and encryption/key context;
- immutable data needs no content invalidation;
- mutable refs are cached separately with short TTLs or conditional reads;
- a cache miss never changes logical semantics;
- corrupt cache entries are evicted and refetched;
- cache loss is a performance event, not data loss.

### 15.2 Cache layers

Possible layers:

1. process memory metadata cache;
2. node-local NVMe immutable object cache;
3. node-local fully or partially materialized template;
4. optional regional shared cache;
5. authoritative object store.

The initial implementation SHOULD prefer node-local cache and avoid adding a shared stateful cache until measurements justify it.

### 15.3 Cache object state

```text
downloading → verified → active → evictable → deleted
```

An object is visible to readers only after complete verification. Partial downloads use temporary keys and are never treated as valid cache entries.

### 15.4 Prefetch

Prefetch requests may identify:

- object IDs;
- tree paths;
- volume chunk ranges;
- a named hot-set profile;
- a boot/setup trace;
- priority and deadline class.

Prefetch is advisory. Failure or cancellation MUST NOT affect correctness.

### 15.5 Hot-set learning

A setup build or representative Cell MAY record early read ranges and path accesses. A template may publish a versioned hot-set profile:

```json
{
  "schema": "comb.hotset/v1",
  "template": "b3k:...",
  "objects": ["b3k:..."],
  "volume_ranges": [
    { "offset": 0, "length": 134217728, "priority": 0 }
  ]
}
```

Hot-set data is a performance hint and may be discarded or relearned.

### 15.6 Eviction

Eviction considers:

- active attachment count;
- current and staging template status;
- expected near-term demand;
- fetch cost and size;
- last access;
- tenant fairness;
- free-space floor;
- checkpoint and compaction headroom.

Active writable overlays are not ordinary cache and MUST NOT be evicted. Committed immutable content can be evicted after verifying that it is reachable from the authoritative backend.

### 15.7 Cache security

- host disks SHOULD use full-disk encryption;
- decrypted tenant data MUST be isolated by process and filesystem permissions;
- cache keys include tenant identity even when digests match;
- cache reuse across tenants is prohibited for private data;
- eviction of sensitive process snapshots SHOULD support immediate secure key destruction where feasible.

---

## 16. Security, tenancy, and trust

### 16.1 Trust boundaries

Comb assumes:

- object-store conditional writes behave according to the certified backend contract;
- authorized Comb writers are trusted to submit data for their tenant;
- tenant isolation is enforced by credentials, prefixes, encryption keys, and service authorization;
- untrusted Cell workloads cannot directly obtain broad bucket-write credentials.

### 16.2 Authentication and authorization

- hosted API access uses short-lived service tokens or workload identity;
- data-plane calls are scoped to tenant, resource, operation, and action;
- direct object-store credentials are read-only unless a trusted Comb component requires write access;
- Pheromone direct follow receives prefix-scoped read access;
- Cell workloads access their attached filesystem, not arbitrary tenant objects;
- administrative export, legal hold, key rotation, and deletion are separately authorized.

### 16.3 Encryption

Comb supports:

- provider-managed server-side encryption;
- tenant-scoped envelope encryption;
- optional customer-managed KMS keys;
- key-version metadata on encrypted objects;
- key rotation through new writes and background re-encryption where required.

**Bring your own bucket** and **bring your own key** are independent choices and MUST be described separately.

### 16.4 Integrity

Integrity layers may include:

- provider transport checksums;
- authenticated encryption;
- plaintext content digest;
- CRC32C on log frames/chunks;
- manifest schema validation;
- referential verification from root to leaves.

A read MUST fail closed on integrity errors. Silent zero-fill or stale substitution is forbidden except where the volume manifest explicitly represents a sparse zero region.

### 16.5 Secret scrubbing for templates

Before a setup snapshot becomes reusable, the builder MUST remove or invalidate:

- temporary cloud credentials;
- SSH keys and agent sockets;
- package-registry tokens;
- authentication cookies;
- `/run` and temporary socket state;
- host-specific machine IDs where inappropriate;
- ephemeral certificates;
- environment dumps;
- shell history created during setup;
- runtime metadata endpoints or cached responses;
- application secrets not explicitly intended for the template.

Validation SHOULD scan for known secret patterns but cannot replace correct setup design.

### 16.6 Audit

Comb records immutable or append-only audit events for:

- resource creation and deletion;
- ref publication;
- writer acquisition and fencing;
- key and policy changes;
- export;
- legal hold;
- administrative verification and repair;
- direct credential issuance;
- cross-region copy or migration.

Audit retention is configured separately from ordinary workload retention.

### 16.7 Deletion

Logical deletion:

1. removes discoverability and new-use refs;
2. creates a tombstone;
3. waits for retention, pins, and grace;
4. removes unreachable encrypted objects;
5. records deletion completion.

Cryptographic erasure MAY be used to make tenant data inaccessible before provider lifecycle deletion completes, provided shared within-tenant objects and retention requirements are handled correctly.

### 16.8 Data residency

Each resource records its authoritative region and backend. v1 uses one writable region per resource. Cross-region copies are explicit replicas, exports, or bridges and do not create active-active mutation.

---

## 17. Hosted service and self-hosting

### 17.1 Components

#### `combd`

Data-plane daemon providing:

- backend access;
- object and ref operations;
- log leadership and following;
- volume attach and checkpoint;
- tree materialization;
- cache management;
- verification;
- background compaction and GC workers;
- optional journal integration.

#### `combctl`

Administrative CLI providing:

```text
combctl tenant inspect
combctl backend test
combctl object verify
combctl ref get
combctl ref history
combctl log inspect
combctl log replay
combctl volume inspect
combctl volume checkpoint
combctl snapshot verify
combctl gc plan
combctl gc run
combctl export
combctl import
combctl fsck
```

#### SDKs

Initial SDK priority:

1. Rust;
2. internal TypeScript client for Apiary control-plane use;
3. CLI and HTTP/gRPC surface;
4. additional languages based on customer demand.

### 17.2 Hosted topology

A managed regional deployment may contain:

```text
API / control plane
      │
regional request routers
      │
┌─────┼──────────────────────────────┐
│ writer/leader processes            │
│ cache and materialization nodes    │
│ compaction and GC workers          │
│ optional replicated journal nodes  │
└─────┼──────────────────────────────┘
      │
managed or customer-owned object store
```

Compute and cache nodes are replaceable. Journal nodes, if introduced, are stateful and require a separately proven quorum protocol and operational model.

### 17.3 Customer-owned bucket

The customer supplies:

- bucket and prefix;
- region;
- scoped role or credentials;
- optional KMS key;
- lifecycle constraints;
- desired verification mode.

Comb performs a backend conformance check before marking the backend authoritative.

The service MUST fail setup when required conditional-write or consistency behavior cannot be verified.

### 17.4 Managed bucket

Apiary supplies:

- tenant prefix;
- encryption defaults;
- lifecycle and backup settings;
- regional durability profile;
- quotas and billing;
- support and repair processes.

### 17.5 Open format and exit path

The durable format SHOULD be documented sufficiently for independent recovery.

The customer MUST be able to:

- enumerate resource refs;
- verify root-to-object reachability;
- decrypt with their authorized keys;
- export logs, trees, artifacts, and volume snapshots;
- run a standalone verifier;
- migrate to another certified backend without dependence on the hosted control-plane database.

### 17.6 Replicated journal: later managed tier

A replicated NVMe journal is not required for the first correct implementation.

When added, it provides lower-latency durable acknowledgement before object-store tiering.

It MUST specify and test:

- replica placement;
- quorum;
- write ordering;
- flush semantics;
- leader change;
- split-brain prevention;
- recovery and replay into object snapshots;
- bounded untiered backlog;
- correlated-failure behavior;
- backup and upgrade procedures.

Comb MUST NOT market “multi-AZ durable” based solely on running multiple processes without a demonstrated storage and quorum model.


---

## 18. API conventions and error model

### 18.1 Operation identifiers

Every externally initiated mutating operation MUST accept a stable idempotency key:

```text
OperationId = opaque 128-bit or larger identifier
```

The operation record SHOULD include:

- tenant;
- operation type;
- resource target;
- normalized request hash;
- current state;
- result identity when complete;
- expiry or retention policy.

Reusing an operation ID with a materially different request MUST return an idempotency-conflict error.

### 18.2 Resource identifiers

Identifiers SHOULD use typed opaque forms:

```text
org_...
product_...
log_...
tree_...
vol_...
snap_...
cell_...
op_...
pin_...
```

Object digests are not resource IDs. A resource may move its refs over time while immutable snapshot IDs remain content identities.

### 18.3 Error classes

Minimum portable error classes:

| Error | Meaning | Retry guidance |
|---|---|---|
| `NotFound` | requested resource, ref, or retained position does not exist | retry only if eventual provisioning is expected |
| `AlreadyExists` | create-only resource already exists | resolve or use idempotent result |
| `PreconditionFailed` | expected ref version is stale | reload, merge where valid, or surface conflict |
| `Fenced` | writer epoch is stale | stop writes and reacquire through scheduler |
| `LeaseHeld` | another valid writer owns the resource | route to owner or wait |
| `BackendUnavailable` | authoritative backend cannot satisfy request | retry with backoff and same operation ID |
| `DurabilityUnavailable` | requested durability profile cannot be met | do not downgrade silently |
| `IntegrityError` | digest, checksum, decryption, or manifest validation failed | quarantine; retry alternate copy only if verified |
| `Trimmed` | requested log position is below retention floor | resume at supplied retained position |
| `QuotaExceeded` | tenant or resource quota prevents operation | delete, raise quota, or change policy |
| `InsufficientLocalCapacity` | node cannot preserve safety floor/headroom | reschedule or evict cache |
| `UnsupportedCapability` | selected driver/backend lacks required semantics | choose compatible placement |
| `IdempotencyConflict` | same operation ID used for different input | caller bug; do not retry unchanged |
| `ConsistencyUnavailable` | requested checkpoint consistency could not be established | retry or explicitly choose weaker class |
| `RetentionConflict` | deletion conflicts with pin or hold | remove/expire protection first |

### 18.4 Retry rules

- create-only immutable uploads MAY be retried safely;
- conditional ref updates require a reload after precondition failure;
- a fenced writer MUST NOT blindly retry the same commit;
- client retries after ambiguous network failure MUST use the same operation ID;
- backoff SHOULD include jitter;
- background workflows MUST persist enough state to resume after process restart.

### 18.5 API versioning

- service APIs use explicit major versions;
- unknown fields are ignored only where schema rules permit;
- required semantic changes use a new version;
- clients advertise supported storage and API versions during attach;
- control plane MUST prevent attaching a snapshot to an incompatible driver.

---

## 19. Compaction, retention, and garbage collection

### 19.1 Principles

- compaction changes representation, not logical contents;
- publication of compacted state is atomic through a ref;
- old representation remains readable during a grace period;
- retention removes roots or advances retention floors;
- garbage collection deletes only unreachable immutable objects;
- mutable distributed per-object reference counts are not the source of truth.

### 19.2 Root set

The tenant root set includes:

- current refs;
- staging refs;
- active log manifests and retained manifest history;
- active Cell heads;
- retained volume branches;
- explicit snapshots;
- non-expired pins;
- legal holds;
- artifact refs;
- cross-region replication/export roots;
- GC safety roots.

### 19.3 Mark-and-sweep

Conceptual algorithm:

1. Establish a GC run identity and root-set view.
2. Record a GC traversal pin or generation barrier.
3. Traverse all reachable manifests and object references.
4. Produce a marked set or partitioned mark index.
5. List candidate immutable objects by prefix/inventory.
6. Exclude objects newer than the orphan grace cutoff.
7. Create a deletion plan and summary.
8. Optionally run in dry-run mode.
9. Revalidate protected refs and pins.
10. Tombstone and delete unreachable candidates in bounded batches.
11. Record completion and metrics.

For large tenants, mark state SHOULD be partitioned and incremental. Provider inventory MAY reduce listing cost but MUST NOT become a correctness prerequisite.

### 19.4 Grace periods

Different object classes may use different grace periods:

```text
unreferenced log WAL chunk: 1 h provisional
superseded log segment: configurable, typically hours to days
unpublished template generation: configurable, typically days
ordinary unreachable snapshot chunks: configurable, typically days
legal hold: no automatic deletion
```

Long grace periods are preferred during early operation.

### 19.5 Log compaction

Covered by Comb Log, with additional rule:

- the old chunks and new segment may coexist;
- only the manifest decides which representation is authoritative;
- deletion waits until readers with stale manifests can recover from the new segment or refresh cleanly.

### 19.6 Repository compaction

Repository pack compaction:

1. create staging image;
2. repack without mutating current image;
3. verify object reachability and refs;
4. publish staging;
5. promote atomically;
6. unpublish previous current;
7. preserve old generation while Cells reference it.

Triggers SHOULD be based on:

- pack count;
- reclaimable ratio;
- fetch/read degradation;
- explicit maintenance;
- available headroom.

### 19.7 Volume metadata compaction

A persistent block-map tree naturally shares nodes without requiring a long linear delta chain. Implementations that use layered delta manifests MUST enforce a maximum depth and periodically collapse them into a new root.

Compaction MUST NOT rewrite unchanged content chunks solely to flatten metadata.

### 19.8 Retention policy

Policy may be attached to:

- log resource;
- log partition;
- template generation;
- Cell;
- snapshot;
- artifact class;
- process-state snapshot;
- audit stream.

Examples:

```text
retain forever
retain 30 days
retain last 10 checkpoints
retain while Cell exists
retain artifact after Cell deletion
legal hold
```

A policy update is itself audited and conditionally applied.

### 19.9 Ref history

Comb SHOULD retain bounded ref history separately from the live ref so operators can diagnose publication and recovery events.

Ref history is append-only and MAY be compacted. It does not supersede the live ref as the linearization point.

### 19.10 Repair

Automated repair MAY:

- evict and refetch corrupt cache content;
- reconstruct a missing side index from retained log data;
- rebuild control-plane discovery records from bucket roots;
- copy a verified object from an authorized replica;
- resume interrupted compaction;
- complete or abandon stale operations.

Automated repair MUST NOT synthesize missing authoritative content or move a ref to an unverified target.

---

## 20. Storage accounting, quota, and billing

### 20.1 Definitions

#### Logical capacity

The maximum addressable size exposed to a Cell or resource.

#### Logical referenced bytes

The sum of file or block sizes visible through a snapshot, counting shared data once per logical view.

#### Tenant-unique durable bytes

The bytes of unique immutable objects reachable within one tenant after within-tenant deduplication.

#### Retained overlay bytes

Tenant-unique chunks introduced by Cell or branch changes and still reachable.

#### Cache bytes

Non-authoritative local copies. A single durable object may be cached on many nodes.

#### Journal bytes

Untiered or replicated write data held in the managed journal.

### 20.2 Customer-facing durable formula

```text
customer durable storage =
    reachable tenant-unique base objects
  + reachable tenant-unique Cell overlay objects
  + retained artifacts, evidence, and transcripts
  + retained process-state objects
  + retained log chunks, segments, and indexes
  + metadata overhead
```

A base shared by 100 Cells in one organization is billed once for its durable tenant-unique bytes, subject to product policy.

### 20.3 Infrastructure physical formula

```text
infrastructure physical storage =
    authoritative durable object bytes
  + node-local cache copies
  + active uncheckpointed overlays
  + journal replica copies
  + staging generations
  + compaction/checkpoint temporary headroom
  + provider replication and metadata overhead
```

This formula is required for Apiary capacity planning even when customer billing excludes cache copies.

### 20.4 Cell quota

A nominal 20–80 GB Cell disk is a logical capacity and abuse-control limit. It SHOULD NOT be billed as fully allocated physical storage when the backing implementation is sparse or copy-on-write.

Quota enforcement considers:

- logical filesystem capacity;
- actual dirty overlay growth;
- tenant durable-storage quota;
- local node safety floor;
- artifact retention quota;
- journal backlog quota.

### 20.5 Billing principles

A managed plan may bill separately for:

- tenant-unique durable storage per month;
- active Cell compute;
- high-performance cache or reserved warm capacity;
- object-store operations and egress where material;
- replicated-journal durability tier;
- retained process snapshots;
- cross-region replication;
- unusually high checkpoint or replay traffic.

Billing metrics MUST be derivable, auditable, and versioned. Provider-estimated bytes and Comb logical bytes must not be conflated.

### 20.6 Compaction and staging headroom

Admission control MUST account for temporary duplication during:

- Git repack;
- log compaction;
- volume checkpoint;
- encryption-key migration;
- backend migration;
- export.

A system that can store current state but cannot complete a checkpoint or compaction safely is already overcommitted.

---

## 21. Observability and operations

### 21.1 Metrics

#### Core and backend

- immutable object PUT/GET rate and bytes;
- create-only collision rate;
- conditional ref update latency and failure rate;
- backend error rate by class;
- range-read latency;
- integrity failures;
- orphan bytes and age;
- ref-generation rate;
- lease acquisition, renewal, and fencing events.

#### Comb Log

- append p50/p95/p99;
- batch size and commit-window utilization;
- manifest updates per second;
- follower lag by sequence and time;
- replay throughput;
- WAL chunk count;
- compaction backlog;
- retention floor;
- dedup hits;
- delivery lag.

#### Comb Tree

- repository generation build duration;
- upload reuse ratio;
- materialized bytes;
- pack count and reclaim estimate;
- current/staging generation status;
- tree diff and export throughput;
- artifact retention bytes.

#### Comb Volume and Cells

- attach latency;
- time to first runnable state;
- lazy-read miss latency;
- cache hit ratio;
- hot-set hit ratio;
- active overlay bytes;
- dirty bytes since checkpoint;
- checkpoint duration and bytes uploaded;
- checkpoint failure rate;
- snapshot fork rate;
- writer-epoch changes;
- uncheckpointed age;
- journal replication and tiering lag;
- free-space floor margin;
- Cells by driver mode.

#### GC and lifecycle

- marked and deleted bytes;
- protected-by-pin bytes;
- oldest unreachable candidate;
- stale operations;
- tombstone age;
- unpublished generation count;
- GC dry-run discrepancy;
- deletion failures.

### 21.2 Structured logs

Every mutating operation SHOULD log:

- operation ID;
- tenant and resource ID;
- ref name;
- expected and resulting generation;
- epoch;
- writer node;
- durability profile;
- object counts and bytes;
- backend request IDs where available;
- result and error class.

Sensitive paths, payloads, tokens, and plaintext content MUST NOT be logged by default.

### 21.3 Tracing

Distributed traces SHOULD connect:

```text
Apiary provision request
→ scheduler placement
→ template resolution
→ cache/materialization
→ writer acquisition
→ Cell startup
→ checkpoint
→ object uploads
→ ref publication
```

Trace sampling must preserve error and high-latency cases.

### 21.4 Health and readiness

`combd` readiness is capability-specific:

- backend credentials valid;
- conditional-write conformance recently checked;
- cache directory writable;
- free-space floor preserved;
- required kernel driver available;
- clock within operational bounds;
- journal quorum healthy when serving replicated writes;
- schema versions supported.

A daemon may remain ready for cached reads while not ready for durable writes. Health reporting MUST distinguish these states.

### 21.5 Verification

Verification modes:

- **metadata** — schemas, refs, and known object existence;
- **sampled** — random reachable content verification;
- **full** — traverse and verify every reachable object;
- **scrub** — scheduled verification with repair from authorized replicas where possible.

`combctl fsck` produces a durable report containing:

- roots examined;
- missing objects;
- corrupt objects;
- orphan estimates;
- invalid refs;
- stale pins;
- unsupported schema versions;
- repair actions taken or recommended.

### 21.6 Disaster recovery

Recovery from loss of the hosted control-plane database:

1. enumerate tenant roots or configured resource catalog;
2. read live refs and retained ref history;
3. reconstruct resource inventory;
4. validate pins, tombstones, and manifests;
5. rebuild search, billing, and UI metadata;
6. require administrative review for ambiguous stale operations.

Recovery from complete cache loss:

- start clean nodes;
- resolve refs from object storage;
- lazily materialize;
- rebuild hot sets and side caches.

Recovery from authoritative backend loss is bounded by the backend's durability, backups, replicas, and customer configuration. Comb MUST state that boundary explicitly.

### 21.7 Upgrades

Rolling upgrades MUST preserve:

- read compatibility with currently referenced formats;
- writer fencing across versions;
- lease interoperability;
- deterministic canonical encodings;
- checkpoint resumability;
- no simultaneous publication of incompatible refs.

A new writer version may require an upgrade barrier before emitting a new schema.

---

## 22. Failure drills and property testing

### 22.1 Release gate

Comb MUST NOT claim a durability or consistency property until the corresponding drill passes against every backend and driver advertised for that property.

### 22.2 Comb Log drills

| # | Drill | Expected result |
|---|---|---|
| L1 | kill leader between WAL chunk upload and manifest update | orphan chunk; no acknowledgement; no visibility; sweeper removes it |
| L2 | kill leader after manifest update but before reply | retry with same event IDs returns one logical append; no duplicate sequence |
| L3 | pause leader A beyond lease, start B, resume A | A's next update fails by epoch/version; zero interleaving |
| L4 | compactor update races append update | one retries; no WAL entry is dropped without equivalent segment data |
| L5 | reader holds stale cache after compacted chunk deletion | reader refreshes and falls back to referenced segment; no gap |
| L6 | object backend unavailable for 60 seconds | no object-durable acknowledgement; cached reads continue where possible; recovery loses no committed data |
| L7 | one node clock skewed by +5 minutes | takeover timing may change; epoch update prevents double commit |
| L8 | replay one billion events from sequence 1 | sequential segment reads; at least 200 MB/s target per follower; flat memory |
| L9 | trim while follower reads below retention floor | explicit `Trimmed{resume_at}` result; no silent gap |
| L10 | MinIO single-node disk loss | behavior documented as dependent on MinIO storage configuration, not misrepresented as Comb replication |

### 22.3 Core ref and object drills

| # | Drill | Expected result |
|---|---|---|
| C1 | two writers create same immutable digest | one create succeeds; the other verifies and reuses it |
| C2 | corrupt object in local cache | digest failure; cache entry quarantined; verified refetch succeeds |
| C3 | corrupt authoritative object | integrity error; no content returned; repair only from verified replica |
| C4 | conditional ref response lost after success | same operation ID resolves committed result |
| C5 | provider returns stale listing | no correctness impact because live state uses direct ref reads |
| C6 | schema unsupported by old node | node refuses attach/write; no incompatible publication |
| C7 | tenant A guesses tenant B digest | authorization and tenant key prevent access or dedup leakage |
| C8 | key rotation during upload | object records one valid key version; retry is deterministic and verifiable |

### 22.4 Tree and generation drills

| # | Drill | Expected result |
|---|---|---|
| T1 | setup builder dies before staging publish | current remains unchanged; temporary pin expires; orphans collect later |
| T2 | validation fails | staging is not promoted; logs/artifacts retained by policy |
| T3 | promotion reply is lost | idempotent retry returns promoted generation |
| T4 | old generation unpublished while Cells use it | Cells continue; GC sees active roots |
| T5 | Git repack races new provisioning | provisioning resolves one complete generation; never partial pack state |
| T6 | OverlayFS Cell mutates `.git` | mutation copy-ups into that Cell; canonical lower image remains unchanged |
| T7 | cache evicts inactive current image metadata incorrectly | authoritative ref rematerializes; no data loss; eviction policy is corrected |

### 22.5 Volume and Cell drills

| # | Drill | Expected result |
|---|---|---|
| V1 | kill checkpoint after changed chunks upload but before head update | old head remains; uploads are orphans |
| V2 | kill checkpoint after head update before reply | retry with operation ID returns same snapshot |
| V3 | stale source node checkpoints after destination takeover | source receives `Fenced`; destination remains sole writer |
| V4 | lose all node-local cache | committed Cell attaches from object store and resumes lazily |
| V5 | lose node with `scratch` Cell | documented loss; no false recovery claim |
| V6 | lose node with `checkpointed` Cell | recover latest committed checkpoint; expose measured loss window |
| V7 | lose one journal replica | writes continue only if published quorum remains healthy |
| V8 | object store unavailable during checkpoint | checkpoint does not succeed; local work follows policy; no head movement |
| V9 | GC runs while Cell provisions from old generation | provisioning pin keeps all required objects reachable |
| V10 | local disk reaches free-space floor | scheduler stops new Cells; cache eviction runs; active overlays are not deleted |
| V11 | changed cached chunk contains bit flip | digest detects; refetch; no silent filesystem corruption |
| V12 | checkpoint while guest writes metadata | requested consistency boundary is enforced or operation fails explicitly |
| V13 | lazy fetch fails halfway | partial object never becomes verified cache content; read retries or fails cleanly |
| V14 | fork 10,000 Cells from one snapshot | metadata scales without 10,000 complete base copies |
| V15 | long-lived Cell pins old base through multiple promotions | old base retained until Cell release; new Cells use current |
| V16 | process snapshot contains injected secret | security scan/test confirms encrypted restricted handling and retention policy |
| V17 | copy-only satellite provisions beyond capacity estimate | admission rejects before violating free-space floor |

### 22.6 GC drills

| # | Drill | Expected result |
|---|---|---|
| G1 | object becomes reachable after GC root capture | generation barrier or revalidation prevents deletion |
| G2 | pin expires during traversal | behavior follows captured root-set semantics and grace; no unsafe deletion |
| G3 | deletion batch partially fails | rerun is idempotent; tombstones and metrics show remaining work |
| G4 | object listing omits recent object | age grace prevents deletion; list is not visibility source |
| G5 | distributed refcount is deliberately corrupted in test metadata | reachability traversal still preserves live data because refcount is not authoritative |

### 22.7 Property and model tests

An in-memory backend with fault injection MUST model:

- arbitrary object PUT failure;
- ambiguous success responses;
- conditional-update conflicts;
- delayed reads;
- stale listings;
- process crashes at every protocol step;
- lease expiry and clock skew;
- concurrent compaction, append, checkpoint, fork, and GC.

Properties:

```text
no acknowledgement without selected durability
no visible ref target with missing referenced data
no stale epoch advances a writable ref
snapshot contents are immutable
per-partition log sequence is contiguous and ordered
GC never deletes reachable content
cache deletion never changes committed logical contents
idempotent retry yields at most one logical operation
cross-tenant object reuse never occurs
```

`loom`, `turmoil`, deterministic simulation, or an equivalent framework SHOULD be used for concurrency and network scheduling.

### 22.8 Test matrix

| Environment | Purpose |
|---|---|
| in-memory backend with injected faults | protocol and property correctness |
| local filesystem backend | fast CI and single-machine semantics |
| MinIO in CI | real conditional writes and self-hosting path |
| S3 Standard nightly | latency, cost, and correctness baseline |
| S3 Express One Zone nightly | zonal low-latency profile |
| macOS APFS CI host | native clone and local Cell lifecycle |
| Linux ext4 + OverlayFS | tree-overlay fallback |
| Linux XFS reflink | native reflink adapter |
| Linux btrfs | native snapshot adapter |
| Linux `ublk` capable host | portable block-overlay driver |
| R2 certification suite | future backend adapter |

---

## 23. Implementation plan

### 23.1 Guiding rule

Do not pause Pheromone to build an abstract universal platform before a real workload works. Extract the shared kernel while implementing concrete Pheromone and Cell paths.

**v0.2 sequencing.** Phase order is **A → B → C → D → E → G → F → H**. The hosted beta (G) ships Nectar and cloud Cells on the provider-native volume driver from Phase E; the portable object-backed overlay (F) is gated as described in §10.0. Phase labels are preserved from v0.1 so that drill and milestone references remain stable.

### 23.2 Phase A — Pheromone structural split

Scope:

- ingest appends only to `TrailLog`;
- matcher follows from a cursor;
- existing SQLite semantics preserved;
- benchmarks and tests remain green.

Exit criteria:

- matching cost no longer blocks ingest;
- replay can start a follower at a position;
- local mode remains kill-safe and low latency;
- at-least-once plus `deliveryId` dedup is preserved.

### 23.3 Phase B — Comb Core foundation

Scope:

- object IDs and canonical envelopes;
- local, memory, MinIO, and S3 adapters;
- create-only immutable upload;
- refs and conditional updates;
- ref conventions: namespaces, symbolic refs, ref journal, reserved signature field, release manifests (§7.5a);
- tenant-keyed digests (§7.4);
- leases and epochs;
- operation IDs;
- basic pins;
- verifier;
- backend conformance command.

Exit criteria:

- core invariants pass fault-injected tests;
- local, MinIO (including on operator-owned hardware), and S3 pass conditional-write certification;
- standalone object and ref inspection works;
- every ref update produces a journal entry and `combctl ref history` replays it;
- no global manifest exists.

### 23.4 Phase C — Comb Log / Pheromone ObjectLog

Scope:

- `SqliteLog`, `FsLog`, and `ObjectLog`;
- single partition;
- group commit;
- manifest ref;
- reader/follower;
- leader takeover;
- orphan sweep;
- failure drills L1–L3 and L6.

Then:

- compaction;
- indexes;
- retention;
- drills L4, L5, L8, L9;
- Pheromone control manifest and delivery log;
- bucket-direct follow;
- tenant provisioning;
- optional partitions.

Exit criteria:

- Pheromone cloud mode runs end to end on MinIO and S3;
- no acknowledged event is lost in the required drills;
- one-billion-event replay target is demonstrated;
- local mode remains available.

### 23.5 Phase D — Comb Tree and repository generations

Scope:

- blob and tree schemas;
- tree publication and diff;
- repository image metadata;
- one current and one staging generation;
- local cache with LRU and free-space floor;
- APFS/native reflink materializer;
- Linux OverlayFS materializer;
- ChangeRef and artifact storage.

Exit criteria:

- one repository base provisions many isolated Cells without Git alternates or hardlinks;
- generation promotion is atomic;
- old generation remains available to active Cells;
- ext4 can use safe tree-overlay mode where supported;
- artifacts survive Cell deletion independently.

### 23.6 Phase E — Unified Comb Volume contract over native backends

Scope:

- volume, branch, snapshot, attach, checkpoint, fork, retain, and release interfaces;
- APFS, btrfs/XFS/ZFS/LVM-thin or selected native adapters;
- Cell head and writer epoch;
- setup generation pipeline;
- scheduler capability advertisement;
- accounting for logical versus physical bytes;
- cloud-provider block-snapshot adapter (§10.0 gate 1);
- measurement of prepared dependency trees as Comb Tree versus Volume on ext4 satellites (§10.0 gate 2).

Exit criteria:

- local workstation and high-density satellite share one logical lifecycle;
- the Tree-versus-Volume measurement for dependency trees is published and the Phase F decision is recorded;
- metadata-only fork works;
- stale writers are fenced;
- current/staging setup publication works;
- lifecycle checkpoints are reliable.

### 23.7 Phase F — Portable object-backed Comb Volume

Scope:

- fixed-size chunk objects;
- persistent block-map tree;
- sparse writable overlay;
- dirty tracking;
- object checkpoint protocol;
- node-local immutable chunk cache;
- lazy reads and prefetch;
- Linux userspace block-device driver;
- ext4 high-density mode.

Exit criteria:

- a cloud or plain-ext4 node creates thousands of logical clones without full base copies;
- cache destruction is recoverable;
- checkpoints reconstruct on another node;
- volume drills V1–V15 pass;
- materialization metrics and scheduler estimates are usable.

### 23.8 Phase G — Hosted beta

Scope:

- `combd` regional deployment;
- managed control plane;
- customer-owned bucket onboarding;
- managed bucket mode;
- quotas and billing metrics;
- audit;
- dashboards;
- verification and export;
- Nectar preview integration on Tree plus provider-native snapshots (§12.0).

Exit criteria:

- customer can provision, attach, checkpoint, export, and delete resources;
- Nectar previews start on any node in a region from refs alone and recover on node loss;
- bucket remains independently recoverable;
- no cross-tenant deduplication;
- billing reconciles against storage state;
- operational runbooks and incident drills exist.

### 23.9 Phase H — Replicated journal and advanced tiers

Scope only after prior phases are stable:

- replicated NVMe journal;
- published quorum profiles;
- journal replay into volume/log snapshots;
- faster durable Cell writes;
- bounded tiering backlog;
- rolling upgrades and disaster drills.

Later optional work:

- R2 certification;
- cross-region asynchronous replication;
- Comb Queue;
- FUSE or kernel filesystem interface;
- S3-compatible API for tree/blob resources;
- content-defined chunking;
- generalized prefetch API;
- multi-reader analytic fan-out;
- remote POSIX semantics only when a concrete product requirement justifies the complexity.

---

## 24. Milestone acceptance criteria

### 24.1 Comb Core alpha

- immutable object create and verify works on local files, MinIO, and S3;
- conditional ref update passes race tests;
- epochs fence stale writers;
- operation IDs resolve ambiguous results;
- pin and root traversal formats are defined;
- `combctl backend test`, `ref get`, and `object verify` work;
- storage schemas are documented.

### 24.2 Pheromone cloud alpha

- local SQLite path unchanged in availability;
- object-backed append follows exact acknowledgement protocol;
- leader takeover automatic;
- followers replay and follow from cache/object storage;
- compaction and retention correct;
- failure drills pass;
- performance benchmarks published.

### 24.3 Cell storage alpha

- repository generation current/staging lifecycle works;
- one native snapshot/reflink adapter and OverlayFS tree mode work;
- Git mutation isolation demonstrated;
- setup snapshot can provision multiple Cells;
- logical and physical storage accounting visible;
- free-space floor enforced.

### 24.4 Cloud volume beta

- portable block overlay on ext4-capable Linux;
- lazy attach and prefetch;
- checkpoint/fork/restore on a different node;
- current and old generations coexist safely;
- object-store outage behavior matches policy;
- tenant-scoped encryption and deduplication tested;
- artifact extraction independent of volume retention.

### 24.5 Hosted beta

- managed and customer-owned bucket paths;
- standalone export and verify;
- audit and quotas;
- recover resource inventory from object storage;
- incident response and GC dry-run procedures;
- no durability claim lacks a passing drill.

### 24.6 General availability

GA requires:

- documented SLOs based on measured production behavior;
- completed security review;
- backup and disaster-recovery review;
- rolling upgrade demonstration;
- backend conformance automation;
- billing reconciliation;
- capacity models including cache, overlay, and compaction headroom;
- customer-visible durability and retention semantics;
- tested migration and exit path;
- operational ownership and on-call runbooks.

---

## 25. Decided design points

The following are current decisions unless explicitly revisited:

1. Working product name: **Comb**.
2. Comb is a branchable durable-state substrate, not merely a WAL library.
3. Higher-level views remain separate: Log, Tree, and Volume.
4. Immutable objects plus conditionally updated refs are the fundamental commit mechanism.
5. S3 and MinIO are first-class remote backends from the beginning.
6. R2 is pluggable later but requires independent certification.
7. Pheromone local SQLite mode remains.
8. Pheromone cloud mode acknowledges only after object data and manifest publication.
9. One writer per log partition or writable volume head.
10. Epoch fencing is mandatory.
11. Ordering is per log partition.
12. General queue semantics are above the log and not v1.
13. Cells use immutable bases and writable overlays.
14. Local native reflinks/snapshots are fast-path adapters to one common `CombVolume` lifecycle.
15. OverlayFS is an immediate tree-level ext4 fallback where safe.
16. A portable object-backed block overlay is the long-term high-density ext4 and cloud fallback.
17. Git alternates and hardlinks are not used for mutable Cell isolation.
18. Repository and setup images have one current and one staging generation.
19. Previous generations are unpublished before they are physically deleted.
20. Active and retained Cells pin old generations.
21. Deduplication is allowed within a tenant and prohibited across private tenants.
22. Artifacts and evidence are stored independently from opaque volume state.
23. Mutable per-object reference counts are not the authoritative GC mechanism.
24. Logical disk capacity is distinct from physical and billable storage.
25. Customer-owned bucket and customer-managed key are independent options.
26. The durable format is open, versioned, verifiable, and exportable.
27. A replicated NVMe journal is later work, not a shortcut around proving the object-durable path.
28. A full distributed POSIX filesystem is not an initial requirement.

Added in v0.2:

29. Content digests are tenant-keyed (BLAKE3 keyed mode); there is no within-bucket existence oracle.
30. Ref namespaces, symbolic refs, the ref journal, the reserved signature field, release manifests, and change hints are Core conventions from the first format version.
31. New consumers must be expressible as views over Core's public API; a consumer that needs a Core change is redesigned, not accommodated.
32. Comb Volume is a separately gated track; cloud Cells and Nectar ship on provider-native snapshots first.
33. Prepared dependency trees are measured as Comb Tree before block-level storage is assumed for them.
34. MinIO on operator-owned hardware is the sovereign production baseline; S3 Standard is the conformance reference and the customer-owned-bucket target.
35. Followers detect change by logical state (`head_seq`, `generation`), never by provider version token alone.
36. Phase order is A → B → C → D → E → G → F → H.

---

## 26. Open questions

### 26.1 Core format

1. ~~Final default internal digest: BLAKE3-256 versus SHA-256.~~ **Closed in v0.2:** BLAKE3-256 keyed mode (§7.2).
2. Canonical object-envelope binary format.
3. Compression defaults by object kind.
4. ~~Whether refs should include an authenticated signature in addition to object-store authorization and encrypted object integrity.~~ **Closed in v0.2:** field reserved, verification optional (§7.5a.4).
5. Maximum supported object and manifest sizes.
6. ~~Exact retained ref-history format.~~ **Closed in v0.2:** ref journal as a Comb Log (§7.5a.3).

### 26.2 Comb Log

1. Canonical JSON versus binary envelope encoding for Pheromone frames. Current direction: JSON plus zstd until replay CPU proves problematic.
2. Commit-window defaults by backend.
3. Placement of cloud semantic/vector side indexes.
4. Whether to offer a hosted-log adapter behind `TrailLog` for specific customers later.
5. Exact direct-follow encryption-key distribution. (v0.2: the tenant digest key rides on the same scoped grant; §7.4, §13.4.)

### 26.3 Comb Tree

1. Fixed versus content-defined chunking for large files.
2. Exact metadata preservation policy for ownership, timestamps, and extended attributes.
3. Whether prepared dependency trees are best stored as Tree, Volume, or both in measured workloads. **Converted in v0.2 to a Phase E measurement gate** (§10.0).
4. Git pack compaction thresholds.
5. Whether selective materialization needs FUSE in the first public release.

### 26.4 Comb Volume

1. Initial chunk size and cache page size.
2. Persistent radix-tree fanout and encoding.
3. `ublk` versus another block-device integration.
4. Direct I/O and page-cache strategy.
5. Checkpoint freeze mechanism across VM/container runtimes.
6. ~~Whether initial cloud Cells use a provider-native snapshot driver before portable Comb Volume is complete.~~ **Closed in v0.2:** yes (§10.0).
7. Process-state implementation: Firecracker snapshot, CRIU, hypervisor-specific integration, or deferred.
8. Maximum uncheckpointed dirty bytes and default checkpoint interval.
9. Whether sub-chunk delta encoding is worth the complexity.
10. How to expose path-level durability classes inside one Cell.

### 26.5 Hosted service

1. Regional topology and initial deployment provider.
2. Hosted control-plane schema and recovery catalog.
3. Customer-owned bucket credential model.
4. Application encryption by default versus provider encryption only in the first beta.
5. Replicated journal protocol and provider.
6. Pricing for warm cache reservation versus ordinary active compute.
7. Cross-region asynchronous replica format and lag guarantees.
8. Formal data-deletion completion SLA.

Open questions MUST be resolved by benchmarks, failure tests, security review, or a concrete product requirement—not by prematurely widening the abstraction.

---

## 27. Recommended repository layout

```text
comb/
  Cargo.toml
  crates/
    comb-core/
      src/
        object.rs
        digest.rs
        refs.rs
        lease.rs
        pin.rs
        operation.rs
        schema.rs
        error.rs
    comb-object/
      src/
        backend.rs
        memory.rs
        local.rs
        s3.rs
        minio.rs
        conformance.rs
        encryption.rs
    comb-cache/
      src/
        cache.rs
        eviction.rs
        materialize.rs
        prefetch.rs
    comb-log/
      src/
        manifest.rs
        writer.rs
        follower.rs
        segment.rs
        index.rs
        compaction.rs
        retention.rs
    comb-tree/
      src/
        blob.rs
        tree.rs
        diff.rs
        publish.rs
        materialize.rs
        repository.rs
    comb-volume/
      src/
        geometry.rs
        map.rs
        overlay.rs
        checkpoint.rs
        attach.rs
        drivers/
          native.rs
          reflink.rs
          ublk.rs
          copy.rs
    comb-gc/
      src/
        roots.rs
        mark.rs
        sweep.rs
        report.rs
    comb-api/
      src/
        grpc.rs
        http.rs
        auth.rs
        idempotency.rs
    combd/
      src/
    combctl/
      src/
  schemas/
  docs/
    specification.md
    formats/
    operations/
    failure-drills/
  tests/
    conformance/
    fault-injection/
    minio/
    s3/
    filesystems/
```

Pheromone MAY initially host some implementation inside its repository, but shared protocol and format code SHOULD move into Comb crates before a second consumer duplicates it.

---

## 28. Configuration examples

### 28.1 Self-hosted Comb backend

```toml
[comb]
tenant = "org_acme"
region = "eu-north-1"

[backend]
kind = "s3"
url = "s3://acme-comb-prod/root"
credentials = "hem:project/comb/s3"

[backend.encryption]
mode = "tenant-envelope"
kms_key = "arn:..."

[cache]
path = "/var/lib/comb/cache"
capacity = "2TiB"
minimum_free_floor = "200GiB"
verify_on_read = true

[leases]
duration = "30s"
renew = "10s"
clock_slack = "5s"

[gc]
orphan_grace = "24h"
delete_grace = "7d"
dry_run_by_default = true
```

### 28.2 MinIO development backend

```toml
[backend]
kind = "s3"
url = "s3://comb-dev/root?endpoint=http://minio.tail:9000"
credentials = "env:COMB_MINIO_CREDENTIALS"
allow_insecure_http = true
```

### 28.3 Pheromone cloud log

```toml
[store]
kind = "object"
url = "s3://pher-prod/acme"
credentials = "hem:project/pheromone/s3"
commit_window = "10ms"
commit_bytes = "4MiB"
lease = "30s"
poll = "250ms"
compact_after = "5m"
max_wal_chunks = 256
```

### 28.4 Normal Cell policy

```toml
[cell.storage]
logical_capacity = "80GiB"
durability = "checkpointed"
checkpoint_interval = "30s"
checkpoint_dirty_bytes = "512MiB"
checkpoint_on_pause = true
checkpoint_on_retain = true
materialization = "hotset"
retention = "7d"

[cell.storage.paths]
"/workspace" = "persistent"
"/workspace/.next/cache" = "rebuildable"
"/workspace/node_modules/.cache" = "rebuildable"
"/tmp" = "ephemeral"
"/apiary/evidence" = "artifact"
```

### 28.5 Satellite capability report

```json
{
  "node": "satellite-oslo-3",
  "storage": {
    "mode": "comb_block_overlay",
    "filesystem": "ext4",
    "free_bytes": 3848290697216,
    "cache_capacity_bytes": 2199023255552,
    "cache_used_bytes": 824633720832,
    "minimum_free_floor_bytes": 214748364800,
    "maximum_active_overlays": 250,
    "supports_lazy_materialization": true,
    "supports_durable_journal": false,
    "cached_templates": ["b3k:..."]
  }
}
```

---

## 29. Example end-to-end flows

### 29.1 Pheromone append

```text
client emits envelope with stable event ID
→ front resolves partition and leader
→ leader batches events
→ immutable WAL chunk uploaded create-only
→ partition manifest conditionally advanced
→ client receives final position
→ matcher follower observes manifest
→ chunk served from cache or object store
→ cascade evaluates
→ durable delivery event appended
```

### 29.2 Build and publish a setup template

```text
new source commit arrives
→ repository generation built as staging
→ repository generation verified and promoted
→ setup identity computed
→ isolated setup builder Cell created
→ dependencies installed and setup executed
→ secret scrub and filesystem quiescence
→ volume checkpoint uploads changed chunks
→ setup snapshot validated
→ staging setup ref published
→ current setup ref atomically promoted
→ previous generation unpublished
→ builder released; old data retained by reachability
```

### 29.3 Create ten Cells

```text
resolve one current setup snapshot
→ create ten provisioning operations and pins
→ scheduler prefers nodes with cached template
→ each Cell creates a branch ref to the same snapshot
→ each attaches an empty writable overlay
→ no full base copy
→ blocks materialize from shared cache on demand
→ only divergent blocks and selected artifacts add tenant-unique storage
```

### 29.4 Recover a Cell after node loss

```text
scheduler marks old attachment lost
→ acquire new writer epoch
→ old writer is fenced by ref epoch
→ resolve latest committed Cell snapshot
→ attach on new node
→ fetch boot/hot chunks
→ start Cell
→ remaining chunks materialize lazily
```

Recovery point depends on the Cell's durability profile.

### 29.5 Delete a Cell but retain evidence

```text
stop Cell
→ optionally checkpoint final source state
→ publish ChangeRef, transcript, screenshots, and report as first-class objects
→ remove active Cell root
→ preserve artifact refs under their retention policy
→ volume chunks become GC candidates when no other root reaches them
```

### 29.6 Customer exits hosted Comb

```text
customer freezes mutations or records final heads
→ combctl export resource catalog and refs
→ verify all reachable objects
→ copy objects and refs to destination backend
→ run destination conformance and verification
→ switch configured backend or consume exported standard formats
→ revoke hosted access
```

---

## 30. Source basis and provenance

This specification consolidates three inputs:

1. **Scale — the trail on object storage**, dated 2026-08-21. This supplies the Pheromone ingest/log split, `TrailLog` contract, object-backed WAL layout, manifest compare-and-swap protocol, group commit, fencing, follower behavior, compaction, partitions, failure drills, test matrix, and original phasing.
2. **The attached Archil design blog.** This supplies the workload framing of compute, high-performance storage, and object storage; lazy materialization; durability as a configurable write-path parameter; and the observation that the product value includes the managed cache, acknowledgement layer, and compute integration rather than only a file format.
3. **The Apiary/Honeybee design discussion.** This supplies Comb's name and product boundary; the separation into Log, Tree, and Volume; the concrete Cell storage figures; repository/setup generation lifecycle; native CoW and ext4 fallback strategy; tenant-scoped deduplication; artifact separation; durability profiles; scheduler capability reporting; accounting; and implementation order.
4. **Review pass, 2026-08-22 (v0.2).** This supplies the Core boundary test and the planned views (session logs, vis, bod, gull, satellites); tenant-keyed digests; the ref conventions; follower change detection by logical state; the Volume gating; the sovereign backend naming; the Nectar and Pheromone v1 mappings; and the re-sequenced plan. Prior art consulted: Git's object/ref split, jujutsu's operation log, Perkeep's blob-and-permanode substrate (as a caution against abstraction drift).

No external vendor claim in this specification should be treated as certified until the corresponding backend or driver passes Comb's own conformance and failure suite.

---

## 31. Final architectural statement

Comb's defining abstraction is not “a WAL on S3” and not “a remote disk.” It is:

> **Immutable durable data plus a small atomically advanced root, combined with fenced single-writer ownership and disposable high-performance materialization.**

That abstraction is specialized into:

- an ordered log for Pheromone;
- immutable source and artifact trees;
- branchable volumes for Apiary Cells and Nectar previews.

The implementation should remain narrow enough that its guarantees can be tested exhaustively, while the product around it supplies the operational capabilities customers actually pay for: warm NVMe, instant attach, safe checkpointing, verification, retention, managed tenancy, and simple deployment.

