# Foundation V3 adapter review and verification

Base: `bff2edb`. Branch: `feat/foundation-r2-adapter` in a new isolated worktree.
The rejected `cd9e0d4` transport is not an ancestor. This change touches only the
bridge, its tests, and Foundation bridge documentation. Shared Store, Log, catalog,
publication, HAMT and backend production code is unchanged.

## Behavior and ownership

All storage operations use `CompleteFeed` and `LogReader`. The bridge never calls
legacy `LogStore` and does not derive production storage paths. A per-log OnceCell
serializes feed initialization; another creates one lazy WriterSession on first
append. Registry entries are bounded at 256 and are not evicted or reactivated after
session loss. Comb owns random instance identity, renewal, lease checks and fencing.

Append passes one opaque StableKey and one exact decoded payload. Stable errors,
including ambiguous post-commit failures, are returned to the host. Retries retain
the original key and bytes. No bridge receipt format, alias map or timed-ID minting
is introduced. Read uses the checked page cursor and snapshot result. A zero-wait
follow uses one bounded read; positive waits use `follow_page`.

Typed error mapping distinguishes integrity, unavailable, deadline, cancellation,
conflict, held leases, fencing and reacquisition requirements. Error bodies preserve
decimal-string retention and event-size information. Successful wire field names
and the 480 KiB raw / 1 MiB encoded budgets remain unchanged.

## Shutdown review

The accepted owning JoinSets, exact ID recovery, bounded fallback and frame draining
remain in place. A process guard cancels the bridge and drops cached sessions if
transport fails or its future is cancelled. Requests have finite deadlines and
child cancellation tokens. Output writes and EOF draining are bounded. Successful
EOF drains admitted requests before closing sessions and propagates release errors.

Root identified immediate EOF cancellation in an intermediate edit. That call was
removed before final verification. The real-process oversized-line-plus-hello-plus-EOF
assertion is unchanged and passes 20 consecutive runs. It still requires hello
success, not a Cancelled response.

## Evidence

- Adapter target: 9 passing tests, including the path-included drain unit test.
- Protocol target: 22 passing tests.
- Transport target: 7 passing tests.
- Foundation process target: 5 passing tests, including an admitted append followed
  immediately by EOF and unsuccessful bounded shutdown of a pending long follow.
- Binary unit target: 1 passing test.
- Node client/key tests: 8 passing tests; acceptance script syntax passes.
- Targeted clippy passes. It reports existing shared-code and protocol-style
  warnings, plus unused items from path-included test modules.

Adapter cases include concurrent first publication through one epoch, original
receipts after intervening writes, conflict without head movement, retained retry
from a fresh broker after eight days, Lost sessions refusing new keys, count and
byte limits, zero-wait follow, no lease on read-only close, and publisher release.

The lost-reply case warms a session, then drops the next successful final ref CAS
reply using the shared failpoint backend. It asserts the injection fired, requires
`backend_unavailable`, and resolves the original receipt through a fresh broker.
The bounded-read probe rejects every unbounded backend get, roundtrips a 480 KiB
payload, and verifies cancellation while a real adapter read is pending.

The existing process acceptance runner passes both forward and reverse fixture
orders against a disposable local backend. Each run includes process restart with
a fresh cache, exact `.fdnc` replay and clean closure before receipt creation.
Development binary SHA256:
`01917c8c68106e1083e19303e28436d9c9f2c30450349af3d6f58d9a82471396`.
Local receipts and captures are under
`/var/folders/y2/lgjk786x2qz6s_gt20x091vc0000gn/T/foundation-r2-adapter-local-l1t__z4o/`.
Its config is private and is not committed.

## Remaining integration gate

The adapter can advertise both storage capabilities after these checks. This is
not a Foundation deployment-readiness claim. Root still owns immutable-binary
capture on local, MinIO and S3 and fresh Loro reconstruction in both orders.
No live backend or shared daemon was changed by this adapter work.

## Lifecycle follow-up after fixed-source review

Root review `/tmp/comb-foundation-adapter-review.md` and dispositions in
`/tmp/comb-foundation-adapter-current-task.md` identified the following bridge
issues on `b5fc422`. Shared R2 production code is unchanged.

- B2 and M3: close no longer returns on the first failed release or retained slot.
  It runs independent closes until the shared deadline, records completed outcomes,
  and reports each failed or uncertain log. Deadline expiry can abort unresolved
  releases. A successful sibling close is retained in the diagnostic report.
- M1: a request guard removes an uninitialized slot only after the last caller
  releases it. Reference decrement and registry removal share one mutex. This
  covers failed opens, cancelled opens, and simultaneous final caller drops without
  allowing a waiting caller to initialize an orphan beside a replacement slot.
  Initialized feed identities remain registered.
- B1: orderly EOF gives admitted requests 500 ms without cancellation, then reserves
  250 ms for cancellation drain and three seconds for session closes within the
  existing four-second total. A stalled data upload no longer prevents an otherwise
  healthy release. Backend failure or forced termination can still leave a lease
  until expiry. Such a shutdown does not produce a success receipt.
- M2: steady-state output uses a separate finite 30-second per-frame timeout.
  Observed EOF still applies the four-second total shutdown budget.
- L1: no change. `writer_session` returns `InvalidLeasePolicy`; its current mapping
  is specific to that actual return type.

Before the fix, six added runtime regressions failed against the prior production
code: failed and cancelled opens exhausted capacity, a failed release aborted a
healthy sibling, deadline errors omitted log outcomes, EOF skipped a healthy release
after stalled publication, and 4.3 seconds of output backpressure killed the bridge.
The failing output is `/tmp/bridge-followup-red.log`.

After the fix, all 58 owned Rust test executions pass: 16 adapter, 24 protocol,
10 transport, 5 real-process Foundation, and 3 binary unit executions. These counts
include repeated path-included unit tests. Both existing EOF success assertions
remain unchanged and each passes 20 repeated real-process runs. Additional unit
coverage checks retained-slot cleanup and concurrent final guard drops.

Root's four independent tests from `/tmp/comb-foundation-parent-regressions.rs`
were copied unchanged to an ephemeral test target in this worktree. All four pass,
including a five-second output pause and a stall limited to actual chunk uploads.
The temporary target is not part of the change. Root's fixed archive was not touched.

The eight Node client/key tests, acceptance-script syntax, targeted clippy, rustfmt,
and diff whitespace checks pass. Clippy still reports existing shared-code and
protocol-style warnings plus unused path-included test items. Evidence logs are
`/tmp/bridge-followup-all.log`, `/tmp/bridge-followup-eof-repeats.log`,
`/tmp/bridge-followup-node.log`, and `/tmp/bridge-followup-clippy.log`.

Root must rerun its checks and immutable process captures against the integrated
follow-up. Earlier development-binary capture evidence above belongs to `b5fc422`
and is not evidence for this changed executable.

## Acceptance caller recovery follow-up

Root's immutable S3 forward capture returned a typed `backend_unavailable` append
response with explicit same-key retry guidance. The runner used `Bridge.ok`, which
asserted success immediately. Root independently confirmed that an identical retry
recovered the committed range. This caller change does not modify Rust or the
immutable production binary.

Successful acceptance appends now use an explicit retry helper. It freezes the
original log, key hex and payload hex, allows six attempts within one 60-second
monotonic deadline, and uses exponential backoff from 100 ms capped at 2 seconds.
Each request gets the remaining deadline, not a fresh timeout. Only an explicit
`ok:false` with `backend_unavailable` is retried. Conflict, cancellation, deadline,
loss and protocol or transport errors remain terminal. No loop restarts a broker.

Attempt evidence includes response IDs, outcomes, elapsed time, backoff, and exact
wire-string fingerprints. Successful receipts contain the attempt history. Failed
runs write `append-attempts.json` after cancelling and settling sibling appends,
without a receipt or feed capture. Success evidence is persisted before attempting
receipt creation; the existing clean-close and fresh-directory rules remain intact.
The original failed S3 artifact was not modified.

Verification: all 24 Node key/client tests pass, including deterministic identity
preservation, attempt exhaustion, shrinking deadline budgets, pending-request timeout,
cancellation during backoff and requests, terminal errors, and late protocol junk.
A failure-only child drives the actual acceptance runner to exhaustion and confirms
that attempt evidence exists while both success artifacts are absent. Existing
success receipt and stale-output tests remain. JavaScript syntax and diff whitespace
checks pass. Evidence: `/tmp/bridge-caller-retry-tests.log`.

The failure-only child is a client test, not storage acceptance. Root owns repeating
all six backend captures and fresh Loro reconstruction with the unchanged immutable
production executable and this checked runner.
