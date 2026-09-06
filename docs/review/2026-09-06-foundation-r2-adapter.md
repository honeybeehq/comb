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
