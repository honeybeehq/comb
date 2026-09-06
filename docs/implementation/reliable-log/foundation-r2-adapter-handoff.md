# Foundation adapter handoff

The checked production source is R2 `842ebf1d7620b2aa7397782e31b326d538c22d32`. Root's integration merge preserves the accepted transport and contains that production source. Start adapter work from the integration merge supplied by root, in a new isolated branch. Do not continue from rejected transport commit `cd9e0d4`.

Foundation owns `crates/combctl/src/bridge/`, `crates/combctl/src/bin/comb-bridge.rs`, `crates/combctl/tests/bridge_*.rs`, and its existing acceptance scripts, fixtures and review notes. Root retains Store, Log, publication, catalog, HAMT and backend ownership. Use a private Cargo target. Report API gaps to root before changing shared production.

## Checked calls

Import the feed types and `LogReader` from `combctl::log`. Build an `Arc<Store>` using `Store::new`. `CompleteFeed::open(store, logical_log, &call)` internally selects the isolated v3 layout. Use this feed for head, read, follow and append. The old `LogStore` API remains the legacy v2 path.

```rust
CompleteFeed::open(Arc<Store>, String, &CallContext)
    -> Result<CompleteFeed, OpenLogError>
feed.writer_session(WriterLabel, LeasePolicy)
    -> Result<WriterSession, InvalidLeasePolicy>

// LogReader methods
feed.head(&CallContext) -> Result<CompleteFeedHead, ReadError>
feed.read_page(Cursor, ReadLimits, &CallContext) -> Result<ReadPage, ReadError>
feed.follow_page(Cursor, ReadLimits, FollowWait, &CallContext)
    -> Result<FollowPage, ReadError>

session.append_stable(StableKey, bytes::Bytes, &CallContext)
    -> Result<StableAppendReceipt, StableAppendError>
session.close(&CallContext) -> Result<(), LeaseError> // consumes session
```

`CallContext::new` takes an absolute Tokio `Instant` deadline and a `CancellationToken`. Connect it to process/request cancellation and give each operation a finite budget. Keep response writing and child-task supervision bounded during shutdown.

Maintain one feed and one session per logical log in the process. Serialize creation so parallel requests cannot create competing local publishers. Session creation does not acquire ownership. Do not call `ready` for hello, head or reads. Stable append checks durable committed keys before acquiring a lease. Comb owns instance identity, epoch, renewal and loss handling; the bridge must not store or renew a fence.

`WriterLabel::try_from(&str)` accepts 1..64 bytes. It is a diagnostic label, not publication ownership. `LeasePolicy::bridge_default()` uses a 30-second TTL, 10-second renewal interval, 5-second slack and 45-second acquisition budget. If retaining `--lease`, derive and validate the full policy and ensure the call budget permits acquisition. A Lost session must not be silently reactivated by retrying new publication. Return the typed loss result; committed-key retries remain available. Closing consumes the session and returns the release result. Dropping it aborts renewal.

## Wire mapping

- Decode the required top-level `idempotency_key` hex into `StableKey::try_from_canonical(Vec<u8>)`, 1..512 opaque bytes. Pass exactly one decoded `payload_hex` as `Bytes`. Comb hashes payload bytes independently and returns the original receipt on retry. Do not add timestamps, parse Foundation document IDs, or create a local alias map.
- A cursor is inclusive `Cursor { partition: 0, next_seq }`. `Cursor::first(0)` is sequence 1. Keep all wire positions decimal strings.
- `CompleteFeedHead` has `head_seq`, `next`, `trim_before_seq` and `generation`. Use its checked `next`, not unchecked `head + 1`.
- `ReadPage` has `events`, `next`, `snapshot_head`, `raw_payload_bytes` and `at_head`. Each event has `position.seq`, `committed_at` and exact `payload` bytes. Use the page's snapshot head rather than a second head read.
- `ReadLimits::try_new(u32, u64)` validates event and raw-byte budgets. Retain the narrower bridge limits, including 480 KiB raw bytes within the 1 MiB JSON frame. The final encoded response must also fit, including IDs and per-event overhead.
- `FollowWait::try_new(Duration)` accepts a positive wait up to 30 seconds. The existing wire contract accepts `timeout_ms=0`; implement that as one immediate bounded `read_page`, then the follow response shape. Do not pass zero to `FollowWait`. Keep positive follow wait separate from the overall call deadline.
- `StableAppendReceipt` contains `payload_hash`, `range.first`, `range.last` and `generation`. Single-payload append has `first == last`. Preserve the original values on retries.

Map typed errors explicitly. Preserve `Trimmed.resume_at`, `EventTooLarge.position/event_bytes/max_bytes`, stable-key conflict, integrity, transient unavailable, deadline and cancellation distinctions. Ownership errors are `LeaseHeld`, `Fenced` or `ReacquireRequired` with a typed `SessionLoss`. Do not match error text or turn integrity failures into empty reads or fresh append permission.

## Verification and acceptance

Retain the accepted JoinSet supervision, exact request-ID parsing and bounded fallback, multiline oversized-frame draining, and closed-stdout termination. Update provisional Unsupported assertions only when their operations actually use the checked storage API. Add adapter tests for concurrent first access, original receipts/conflicts, bounded replay, zero-wait follow, shutdown and typed errors.

Capability flags may become true in the completed adapter once those operations are implemented and verified. This is not a Foundation readiness claim. Root must still run the immutable binary on local, MinIO and S3, capture the exact real `.fdnc` bytes through actual requests, restart with a fresh cache, and verify fresh Loro reconstruction in both submission orders. Existing acceptance receipts are written only after clean process close and complete stdout drain. Destructive collection remains disabled.
