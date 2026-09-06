# Foundation bridge acceptance

The bridge is Comb's long-lived machine interface for Foundation and similar clients.
Foundation owns its documents and merge semantics. The bridge transports opaque bytes
and delegates all durable publication, deduplication, and fencing to the shared Log.

Implementation branch: `feat/foundation-r2-adapter`, based on `bff2edb`. The first transport is stdio JSONL,
brokered by the Foundation host. A separate socket server is outside this slice.
Shared Core and Log implementation proceeds on `feat/reliable-log-r1`.

## Required contract

- Each request declares protocol version 1 and a request ID. Responses correlate by
  request ID. Unsupported versions fail explicitly.
- A capability handshake describes supported operations and hard limits. An adapter
  awaiting a shared Log capability returns `Unsupported`; it never silently weakens
  a requested guarantee.
- Append carries one stable `idempotency_key` and one opaque binary `payload_hex`.
  The key is independent of the transport request ID. Foundation uses document ID
  plus envelope hash. Both key and payload use hex. The key bytes are
  `u32be(docId UTF-8 byte length) || docId UTF-8 || u32be(32) || raw envelope SHA256`.
  Comb does not parse the Foundation document format. Batch append is deferred.
- The same key and bytes return the original logical append result for the lifetime
  of the retained complete feed. This includes process restart, a fresh client, and
  more than seven days. Changed bytes under that key fail with a conflict.
- Append success means the shared Log has durably published its manifest. An upload
  or a process-local buffer is insufficient. Retry after an ambiguous result retains
  the same stable key.
- Sequence positions are decimal strings. A cursor names the next sequence to read.
  Pages have event-count and byte limits. An oversized first event produces an
  explicit error without advancing the cursor.
- Read and bounded long-poll follow share resumable page semantics. A pending follow
  must not prevent another client from appending through the same process.
- Trimmed cursors return an explicit resume position. The caller must choose how to
  recover; neither bridge nor client silently skips history.
- Backend errors, fenced writers, conflicts, invalid input, and unsupported operations
  have stable machine-readable codes. Diagnostics stay on stderr; stdout is protocol
  frames only.
- Input and output frames, outstanding requests, queued output, payloads, response pages,
  and follow waits all have hard limits. Underlying Log reads must also be bounded.
  Raw payload byte budgets are separate from encoded JSON frame budgets; hex uses
  two wire bytes per raw byte before JSON metadata.

## JSONL v1 transport

A host keeps one `comb-bridge --dir <config-directory>` process alive and brokers
its stdio among clients. Each line contains one JSON object. Responses may arrive
out of request order; the host correlates them with `id`.

```json
{"v":1,"id":"hello-1","op":"hello","require":["durable_idempotency","bounded_memory_read"]}
{"v":1,"id":"append-1","op":"append","log":"document-feed","idempotency_key":"0102","payload_hex":"00ff"}
{"v":1,"id":"head-1","op":"head","log":"document-feed"}
{"v":1,"id":"read-1","op":"read","log":"document-feed","cursor":"1","max_events":16,"max_bytes":65536}
{"v":1,"id":"follow-1","op":"follow","log":"document-feed","cursor":"1","max_events":16,"max_bytes":65536,"timeout_ms":5000}
```

The append example uses an arbitrary two-byte opaque key. A Foundation client uses
the canonical document/envelope key described above. The bridge accepts one payload
per append request. It does not reinterpret an array of separately keyed events as
one contiguous append receipt.

Successful append returns `first`, `last`, and `cursor`, each a decimal string.
A single event has equal `first` and `last`; `cursor` is the following sequence.
A committed retry returns those original positions even after intervening writes.
Head returns `head`, `cursor`, and `trim_before`. Read returns `events` with `seq`,
`at`, and `payload_hex`, plus `next_cursor` and `at_head`. Follow uses the same page
and adds `timed_out` when its bounded wait expires. An empty page leaves its input
cursor unchanged. Request cursors use canonical positive decimal strings starting
at `"1"`, without leading zeroes. `timeout_ms: 0` or an omitted timeout requests a
non-blocking follow poll; page count and byte limits must be positive.

Success frames contain `v`, `id`, `ok: true`, `op`, and the operation's fields.
Error frames contain `v`, `id`, `ok: false`, and an `error` object with a stable
`code` and a diagnostic `message`. Clients branch on the code rather than parsing
messages. `trimmed` carries a decimal-string `resume_at`. `event_too_large` carries
`seq`, `event_bytes`, and `max_bytes` as decimal strings and does not skip the event.
Invalid JSON or a frame too large to parse safely can have an empty response ID.

`max_bytes` counts decoded payload bytes. The hello limits distinguish this from
the encoded line limit, which includes JSON fields and hex expansion. Both request
and response lines must fit the wire limit. Page construction must respect the wire
budget before emitting a cursor that advances past events.
The default wire limit is 1 MiB. Raw append and read limits are 480 KiB, leaving
64 KiB after hex expansion for JSON fields and up to 256 event records. The output
transport also checks the final encoded frame.

`idempotency_key` contains 1 to 512 opaque bytes, encoded as 2 to 1024 hex characters.
Uppercase hex is accepted and normalized to lowercase. `max_idempotency_key_len`
counts wire characters. Strings such as `doc:hash` are invalid keys.

Unknown required capabilities and protocol versions return `unsupported`. The V3
adapter uses `CompleteFeed`, `LogReader`, and `WriterSession` for every storage
operation and advertises durable idempotency and bounded-memory reads. The Foundation
host must require both. These flags describe adapter support; deployment readiness
still requires the immutable-binary backend capture and fresh-Loro gates below.

The process caches at most 256 logical feeds and creates one lazy writer session per
feed, on its first append. Parallel first requests share initialization. Reads and
hello never acquire publication ownership. The process does not evict Lost sessions
or silently acquire new ownership; exceeding the feed limit returns `busy`.

`--writer` is a diagnostic label of 1 to 64 bytes. Comb generates each session's
actual instance identity. `--lease` defaults to 30 seconds and accepts 3 to 600;
renewal runs every third of the TTL, slack is one sixth, and the acquisition budget
is 1.5 times the TTL. Comb owns all renewal and fencing. Each call gets the acquisition
budget plus 15 seconds as its finite deadline. Positive follow waits remain bounded
separately at 30 seconds; zero and omitted waits call `read_page` immediately.

The adapter uses only Comb's V3 physical layout. Existing legacy feeds return
`unsupported` with capability `v3_complete_feed`; there is no fallback or migration.
Read responses use the page's single snapshot and checked next cursor.

Typed errors additionally distinguish `integrity`, `deadline_exceeded`, `cancelled`,
`lease_held`, and `reacquire_required`. A lost session stays lost for new publication;
committed-key retries remain available. Error messages describe the loss cause, but
clients branch on codes. An unavailable, cancelled, or timed-out append may already
be committed. Retry with the identical key and bytes; do not mint another identity.

EOF has a four-second total shutdown budget. Admitted requests first get a 500 ms
grace period without cancellation. Remaining requests are cancelled and get up to
250 ms to drain, leaving a three-second budget for independent session closes.
One failed release or a slot still held by a caller does not skip other sessions.
Cleanup errors identify logs whose closes completed, failed, or remain uncertain.
A failed backend or forced termination can leave a lease until its TTL expires;
the bridge does not guarantee release in those cases.

During normal operation, each output frame has a separate 30-second write budget.
EOF still bounds output draining by the total shutdown deadline. Failed shutdown
exits unsuccessfully so the acceptance client cannot write a success receipt.
Cancellation drops the owning JoinSets and cached sessions, which stops Comb's
renewal tasks.

Failed or cancelled feed initialization does not permanently consume one of the
256 registry slots. The last request removes an empty slot under the registry lock.
Concurrent waiters retain the same slot. Initialized feeds and sessions remain
registered so a lost session is never silently replaced.

## Foundation fixture

`fixtures/foundation-bridge.json` contains three real `.fdnc` blobs generated by the
Foundation engine: one shared genesis and one edit from each of two offline peers.
Each peer changes a different node and adds a comment. Explicit distinct comment IDs
isolate storage verification from Foundation's known annotation-minter collision.

The fixture records the expected canonical projection hash and both comments. Comb
tests can use the opaque bytes without depending on the Foundation source or runtime.

To regenerate it, run from a Foundation checkout with its dependencies installed:

```sh
pnpm exec tsx /path/to/comb/scripts/foundation-bridge-fixture.mjs /path/to/foundation
```

The generator checks both peers converge before writing the fixture. Its temporary
mailbox is local to the invocation.

To independently verify a recovered feed with the actual Foundation engine, capture
an object containing `changes`, where each item has `idempotency_key` and `payload_hex`.
Run from the Foundation checkout:

```sh
pnpm exec tsx /path/to/comb/scripts/foundation-bridge-verify.mjs \
  /path/to/foundation /path/to/captured-feed.json
```

The verifier checks exact bytes, duplicate keys, both comments, and the expected
projection hash after replay into a fresh empty Loro document.

## Integration gate

1. Start one bridge process against an isolated local backend and negotiate limits.
2. Pipeline requests from multiple logical clients through that process. Keep a follow
   pending while another request appends; prove both can complete.
3. Append the fixture in different arrival orders. Retry with new request IDs and the
   original stable keys, including after intervening appends. Require one logical entry
   per key and exact original result ranges.
4. Send changed bytes under a committed key. Require a conflict without moving the head.
5. Read in small pages bounded by both count and bytes. Persist each next cursor.
6. Stop the bridge, start a new process, and replay the retained feed from a fresh
   client. Verify the capture with `foundation-bridge-verify.mjs`.
7. Exercise malformed frames, invalid hex, invalid names and cursors, oversized input,
   full output queues, disconnected clients, and follow timeouts.
8. Use the shared Log's injectable clock to prove retained stable keys survive its
   generic seven-day operation window. Exercise explicit trimming separately, in a
   disposable feed that is not the complete-history recovery fixture.
9. Repeat append, retry, paged replay, and process recovery under unique prefixes on
   MinIO and S3. Never modify existing feeds or restart shared services.

The executable acceptance runner uses a fresh random log name on each invocation:

```sh
node scripts/foundation-bridge-acceptance.mjs \
  /path/to/comb-bridge /path/to/isolated-config /path/to/results reverse
```

Run once with `forward` and once with `reverse` to vary fixture submission order.
The runner records the binary SHA256, handshake, and passed checks in `receipt.json`,
and writes `captured-feed.json` for the Foundation verifier. It writes these artifacts
only after the storage checks pass and the process closes cleanly after draining stdout.
Every invocation requires a new output directory; an existing directory is refused
before starting the bridge. A failed run cannot leave a prior receipt looking current.
The client retains terminal errors even when no request is pending.

Successful append calls explicitly retry only typed `backend_unavailable` responses.
Each logical append keeps the exact original log, key hex, and payload hex. Transient
request IDs change between attempts. The caller allows at most six attempts within
one 60-second overall deadline, including requests and backoff. Backoff starts at
100 ms, doubles, and is capped at 2 seconds. Each request receives only the remaining
deadline budget. The bridge process is not restarted or reacquired by this retry loop.
Conflicts, cancellation, deadline errors, session loss, malformed protocol output,
and transport failures end the run. Exhaustion also fails, since an unavailable
append might already have committed.

`receipt.json` includes `append_retry_policy` and `append_attempts`, so a recovery
cannot be mistaken for a first-attempt success. `append-attempts.json` is written
before a success receipt or during failure cleanup. It records each response outcome,
request ID, elapsed time, scheduled backoff, log, and SHA256 fingerprints of the exact
key and payload hex strings. Failed runs retain this evidence without a success
receipt. Remaining concurrent append loops are cancelled and settled before failure
cleanup finishes. The earlier failed S3 run remains a separate artifact.

Run `node --test scripts/foundation-bridge-client.test.mjs` for deterministic client
lifecycle checks, including late invalid output and stale receipt refusal. These
checks simulate process I/O only and provide no storage acceptance evidence.

Keep config directories private; they contain
the tenant digest key. Use a new backend prefix for each live verification run.

The runner was previously checked against the provisional bridge: missing durable
idempotency caused a nonzero exit and no success receipt. Actual immutable-binary
captures and fresh-Loro reconstruction remain root's integration gate.

Core failure drills must target the interval after manifest publication and before
the reply. The bridge tests do not introduce their own storage fault protocol.

Passing transport tests while the Log adapter returns `Unsupported` is an intermediate
result. Foundation integration is unblocked only after actual append and replay pass
through the bridge and the retained-key guarantee passes the shared storage drills.
