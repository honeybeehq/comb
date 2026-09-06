# Foundation bridge acceptance

The bridge is Comb's long-lived machine interface for Foundation and similar clients.
Foundation owns its documents and merge semantics. The bridge transports opaque bytes
and delegates all durable publication, deduplication, and fencing to the shared Log.

Implementation branch: `feat/foundation-bridge`. The first transport is stdio JSONL,
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

Unknown required capabilities and protocol versions return `unsupported`. Until
R1 stable publication and R2 bounded reads are wired, hello advertises those
capabilities as false and the dependent operations return `unsupported`. The
Foundation host must require both before offering distributed document sync.

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

Run `node --test scripts/foundation-bridge-client.test.mjs` for deterministic client
lifecycle checks, including late invalid output and stale receipt refusal. These
checks simulate process I/O only and provide no storage acceptance evidence.

Keep config directories private; they contain
the tenant digest key. Use a new backend prefix for each live verification run.

The runner has been checked against the provisional bridge: missing durable
idempotency causes a nonzero exit and no success receipt. A successful end-to-end
capture remains pending R1 and R2 integration.

Core failure drills must target the interval after manifest publication and before
the reply. The bridge tests do not introduce their own storage fault protocol.

Passing transport tests while the Log adapter returns `Unsupported` is an intermediate
result. Foundation integration is unblocked only after actual append and replay pass
through the bridge and the retained-key guarantee passes the shared storage drills.
