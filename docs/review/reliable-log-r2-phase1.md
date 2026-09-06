# R2 phase 1 review

Reviewed commit `b5fc5d2a4e2de798f9c07e1955fff740ced1d5f3`. Source inspection only. The active worktree has subsequent edits and is not the review snapshot. No R2 build or test was run by root in that working tree.

## Root disposition

Phase 1 is not accepted yet. Backend and cache reads enforce the encoded byte cap before collecting the complete body. The envelope boundary needs four corrections:

- Validate expected tenant as well as kind and schema.
- Enforce the plaintext cap against borrowed bytes before cloning or invoking a payload decoder. The encoded cap already supplies a larger finite bound; this is not an unbounded allocation.
- Reject unsupported flags and codecs in the legacy decoder too. Subsequent worker edits already address this.
- Quarantine invalid cached envelopes and refetch the backend once. Unsupported cache metadata must not make a valid authoritative object unreadable.

Add an independently armed GetLimited failpoint test proving that bounded reads never fall back to unbounded get. The worker already has the separate variant in progress.

The review below identifies several contract differences that need interpretation. R1 commit and log carrier schema tags are in the payload; current Store::put_blob wraps both with envelope schema comb.object/v1. A schema allowlist may help future object classes, but history traversal does not inherently require guessing among those payload schema tags before envelope decoding. Unknown metadata fields do not activate codecs, so deny_unknown_fields is optional strict-schema enforcement. No digest-format change is requested. LimitedObject versus tuple is a representation choice; the worker's tuple conversion can remain.

The current task file supplies the final R1 followups and authorizes catalog, v3 manifest, pages, follow and instance-owned session work after these fixes. Bridge capabilities remain false until that implementation and actual process acceptance pass.

## Independent fixed-commit review

# R2 phase 1 review — bounded gets, wrappers, cache, decode preconditions

Read-only. No edits, builds, tests, or children. Reviewed against
`b5fc5d2a4e2de798f9c07e1955fff740ced1d5f3` on `feat/reliable-log-r2` in `comb-reliable-log-r2`,
contract `docs/implementation/reliable-log/r2-checked-handoff.md` (r1 root). R2 log/catalog/session
excluded as not implemented.

## Worktree drift during this review — read first

The worktree was **clean** when I started (`git status --porcelain` empty) and is **now dirty across
12 files**: `comb-core/src/{envelope,error,lib}.rs`, `comb-object/{Cargo.toml,src/backend.rs,
src/conformance.rs,src/failpoint.rs,src/fault.rs,src/lib.rs,src/local.rs,src/memory.rs,src/s3.rs}`.
The in-flight edit converts `get_limited` from `Result<LimitedObject>` to the
`Result<(Vec<u8>, Version)>` tuple and adds `FailMethod::GetLimited` — i.e. it is already correcting
two of the items below.

Everything reported here is pinned to the SHA (extracted via `git archive` to `/tmp/r2sha`).
`crates/combctl/src/store.rs` is **not** dirty, so its findings apply to both. Flagging because
focused tests running against the working tree are testing different content than this SHA.

## Root's four, confirmed at the SHA

Locations only, no re-derivation: missing tenant check — `EnvelopeExpectation` has `kind` and
`schema` only (`envelope.rs:49-52`), `meta.tenant` never compared. Plaintext cap after payload clone
— `payload = bytes[12 + meta_len..].to_vec()` inside `decode_limited`, cap applied later in
`store.rs:145-152` and `184-191`. Legacy `decode` ignores flags and codecs — never reads
`bytes[6..8]`, never checks `compression`/`encryption` (`envelope.rs:147-183`).
`UnsupportedEnvelopeFormat` from the cache returns without quarantine or refetch (`store.rs:161`).

---

## A1 — `FailMethod::GetLimited` was never added; the bounded read is gated on `FailMethod::Get`

**Concrete, and it removes a test the handoff asked for.**

`enum FailMethod { PutUpdate, PutCreate, Get }` (`failpoint.rs:15-19`) — no `GetLimited` variant, and
`FailpointBackend::get_limited` dispatches on `self.decide(FailMethod::Get, key)`
(`failpoint.rs:234-240`). Handoff line 28 requires the variant.

Consequence: the two read paths cannot be armed independently. The test that matters —
drop only `get_limited` while leaving `get` healthy, to prove `get_blob_limited` never silently
falls back to unbounded `get`/`cache_read` — cannot be written. The `Store` still exposes both
`get_blob` (unbounded, `cache_read` via `std::fs::read`) and `get_blob_limited`, so that fallback is
exactly the regression worth pinning. Conversely, an R1 retry drill arming `Get` now also perturbs
every R2 bounded read in the same process, which will make focused failures ambiguous.

Fix: add the variant and dispatch on it; keep `CountingBackend::gets` counting both
(`failpoint.rs:302-305` already does, correctly per handoff line 28).

## A2 — `EnvelopeExpectation` takes one `schema`, not `allowed_schemas`

**Concrete downstream blocker, not a naming preference.**

Handoff line 53 specifies `allowed_schemas: &'a [&'a str]`. The SHA has a single
`schema: &'a str` (`envelope.rs:49-52`), matched by equality (`decode_limited`, schema arm).

Consequence: handoff line 119 requires `publish.rs::load_commit_view` to accept **both**
`comb.commit/v1` and `comb.log.partition-manifest/v2`, and the v3 schema later, for a digest whose
class is not known before the read — R1's `load_commit_view` decides *after* decoding, by inspecting
the payload's schema. With a single-schema expectation the caller must guess before reading, and a
wrong guess yields `UnsupportedEnvelopeFormat`: on the cache path that returns without quarantine or
refetch (root's item 4), on the backend path it is a hard error. So migrating history traversal to
`get_blob_limited` in phase 2 is blocked, or forced into speculative double reads with a guaranteed
wasted round trip for one of the two carrier types.

Fix while the type is still unused by R1 callers: make it a slice and match by membership.
`ObjectClass::expectation()` and `ObjectClass::blob()` follow mechanically.

## A3 — `decode_limited_with` runs the caller's payload decoder with no plaintext bound

`decode_limited` takes `max_encoded_bytes` but no plaintext cap (`envelope.rs:189-195`); the cap
lives only in `ObjectClass` and is applied by `get_blob_limited` after decode returns. Root's item 2
covers the ordering. The additional point is `decode_limited_with` (`envelope.rs:291`), whose stated
purpose is "the payload decoder runs only after every format tag is accepted and the untransformed
digest verifies" — the plaintext cap is not among those checks and the function does not accept one.
Any caller using it directly gets only the encoded cap.

Stated accurately: the payload is a suffix of the encoded bytes, so `max_encoded_bytes` **does**
bound the plaintext. This is a looser-than-specified bound, not an unbounded allocation. The gap is
that the handoff's `EnvelopeReadSpec` carried `max_plaintext_bytes` into the decode signature
precisely so both entry points enforce it before the clone; the SHA moved it out to the one caller
that happens to check it.

Fix: put `max_plaintext_bytes` back in the decode signature and check it against
`meta.plaintext_bytes` before slicing the payload — that closes root's item 2 and this one together.

## A4 — Envelope metadata is unauthenticated, and `EnvelopeMeta` allows unknown fields

`Envelope::new` computes `digest = key.digest(&payload)` (`envelope.rs:112`) — payload only. So
`schema`, `tenant`, `kind`, `plaintext_bytes`, `compression`, `encryption` and `created_at` are
outside the integrity boundary, and `EnvelopeMeta` has no `#[serde(deny_unknown_fields)]`
(`envelope.rs:81-91`).

Two consequences that bear directly on the phase-1 error classification:

1. **It is why root's item 4 is a real bug rather than a symmetry nit.** `decode_limited` checks
   format tags *before* computing the digest, so a cache entry corrupted in the metadata region
   fails with `UnsupportedEnvelopeFormat` before anything verifies whether the payload is intact.
   That is indistinguishable from a genuine class mismatch, and it is exactly the corruption a local
   disk cache produces. The fix must therefore be quarantine-and-refetch-once, not a bare error.
2. Without `deny_unknown_fields`, extra metadata fields pass silently through both `decode` and
   `decode_limited`, weakening "unsupported flags or codecs never reach a codec path" to "only the
   codecs we thought to name". A future `compression_v2`-style tag would be ignored rather than
   rejected.

Fix: `deny_unknown_fields` on `EnvelopeMeta` (narrow). I am **not** recommending a digest-format
change to cover metadata — that is a format rewrite and out of phase-1 scope; recording the property
so the error-classification fix is chosen with it in mind.

## A5 — The cache error arm is a catch-all, so the fix should be an allowlist

`store.rs:155-167`:

```
Ok(env) if env.meta.digest == *digest => { …plaintext check…; return Ok(…) }
Ok(_) | Err(IntegrityError) | Err(InvalidFormat) => { quarantine; }   // falls through to backend
Err(e) => return Err(e.into()),                                        // no quarantine, no refetch
```

Root's item 4 is the `UnsupportedEnvelopeFormat` instance. The shape is the problem: the quarantine
set is an explicit allowlist of three cases and everything else falls into a terminal catch-all, so
each new `CoreError` variant reachable from `decode_limited` silently repeats the bug. Invert it —
treat every decode failure on *cached* bytes as "quarantine and refetch once", and reserve terminal
errors for the backend path.

Two things on that path that are **correct** and should not be changed while fixing it: the
`ObjectTooLarge` arm from `cache_read_limited` already quarantines and falls through
(`store.rs:164-166`); and the in-arm plaintext-cap failure at `store.rs:145-152` correctly returns
without quarantine, because the digest already matched, so the cached bytes are the real object and
the backend copy would be identical.

## A6 — `LimitedObject` was added against an explicit handoff instruction

Handoff line 13: "Keep the existing tuple and `Vec<u8>` convention instead of adding the sketch-only
`LimitedObject` type." The SHA defines it (`backend.rs:20-25`) and returns it from the trait
(`backend.rs:72`) and every implementation. Recording as a SHA-state deviation; the dirty worktree is
already converting it back, so this is likely resolved by the next commit.

---

## Verified correct at the SHA (checked, no defect)

- `MemoryBackend::get_limited` compares the stored length while holding `state`, before
  `Bytes::copy_from_slice` (`memory.rs:69-82`) — handoff line 25 satisfied.
- `LocalBackend::get_limited` checks `fs::metadata`, then reads through `take(limit + 1)` and
  re-checks, so stale metadata and growth are both caught (`local.rs:147-172`,
  `backend.rs:34-47`) — handoff line 26 satisfied.
- `S3Backend::get_limited` rejects a declared `content_length` over the limit, then still reads
  through `take(limit + 1)` and re-checks, so missing or understated length cannot bypass the bound
  (`s3.rs:152-181`) — handoff line 27 satisfied.
- `saturating_add(1)` on the take limit is safe at `u64::MAX` (the post-check then trivially passes,
  which is the correct meaning of an unbounded limit).
- `meta_len` is `u32` bounded by `MAX_META_BYTES` before `12 + meta_len as usize`, so no index
  overflow.
- `decode_limited` checks encoded size, magic, version, flags, meta bound, compression, encryption,
  kind and schema **before** deserializing `EnvelopeMeta` and before the digest — the ordering the
  handoff requires (subject to A3's missing plaintext cap).
- `cache_read_limited` is genuinely bounded: metadata check then `take(limit + 1)` with a re-check
  (`store.rs:204-244`); `std::fs::read` is not used on this path. Read errors degrade to a cache
  miss and self-heal via `cache_write`, which is right for a cache.
- `CountingBackend::gets` counts both read methods (`failpoint.rs:302-305`).
- `FaultBackend::get_limited` rolls only `fail_before`, matching `get` (`fault.rs:93-105`) — correct,
  a lost response on a read has no durable effect.
- Conformance checks 10 and 11 assert the exact-cap accept, the over-cap reject as
  `ObjectTooLarge { limit, actual }`, that `get` still returns the full body, and that a missing key
  through `get_limited` stays `NotFound` (`conformance.rs:179-217`), and are run through all three
  wrappers (`conformance.rs:250-273`).

## Suggested focused tests (not written)

1. A1: arm `GetLimited` only; assert `get_blob_limited` fails and does **not** fall back to `get` or
   the unbounded cache path.
2. A2: read one digest whose envelope schema is `comb.log.partition-manifest/v2` with a class
   expecting `comb.commit/v1`; assert the error, then assert a multi-schema expectation accepts both
   carriers in one read.
3. A4/A5: corrupt only the metadata region of a cached entry (flip a byte in `compression`); assert
   quarantine plus a successful backend refetch, not a terminal error.
4. A3: call `decode_limited_with` directly with a payload above `max_plaintext_bytes` but below
   `max_encoded_bytes`; assert rejection before the decoder runs.
