# Foundation process bridge review

Scope: `209199add77242c4cd69dcbe58e1afc31c4cfbf8`, the JSONL transport,
Foundation acceptance runner, and focused transport corrections. This review does
not establish shared Log correctness or Foundation integration readiness.

## Findings

| Finding | Effect | Resolution |
|---|---|---|
| An invalid request ID near the wire limit was echoed in an even larger error | Stdout violated its advertised frame limit | `209199a` bounds fallback IDs and checks the fallback frame itself |
| A failed stdout writer was awaited only after stdin ended | A disconnected host could leave the process alive with stdin open | `209199a` supervises input and output concurrently and cancels admitted handlers on failure |
| Oversized-line draining consumed bytes after the newline | The next valid request disappeared | `209199a` consumes only through the delimiter |
| Transport test teardown waited for EOF without explicitly shutting down the write half | A test could hang while holding Cargo's artifact lock | `209199a` explicitly shuts down the write half and bounds teardown waits |
| The acceptance client forgot terminal errors when no request was pending | Late invalid stdout could still produce a success receipt | `d261280` retains terminal errors and requires clean final process closure |
| A reused result directory could retain an older success receipt | A failed run could appear successful | `d261280` refuses an existing output directory before spawning |

`ec4fbd9` adds opaque hex key validation with a 512-byte bound and a unique default
process identity. The focused follow-up reserves 64 KiB of the encoded frame for
metadata, checks head cursor arithmetic, and accepts zero as the non-blocking follow
timeout. Count and byte page limits remain positive. Tests serialize maximum-size
requests and pages, and exercise an overflowing head through the actual handler.

## Verification

The two real-process regressions in `bridge_foundation.rs` reproduce the oversized
error and closed-stdout failures against the earlier binary. Transport tests cover
frame resynchronization and explicit EOF teardown. The client/key suite passes all
eight checks, including late junk with no pending request and stale output refusal.
Both process regressions pass after the fixes in `209199a`, using the bridge's
private Cargo target. The acceptance runner also passes `node --check`.
A third passing process test sends a line larger than 1 MiB across multiple input
buffers, followed by a valid hello. Both replies arrive with their request IDs;
the bridge preserves the next request while draining the oversized line.

Final private-target verification passes: binary unit target 1, protocol target 21,
transport target 5, and Foundation process target 3. The path-included test modules
produce unused-code warnings; there are no build errors or failing tests.

Transport success is separate from storage acceptance. The bridge advertises
`durable_idempotency: false` and `bounded_memory_read: false`; append, read, and
follow fail with `unsupported`. There is no unkeyed append path, local receipt map,
or unbounded read followed by truncation.

The next gate requires actual shared stable append and bounded read APIs, then
captured feeds from local, MinIO, and S3 runs replayed through a fresh Foundation
engine. Fixture-only convergence is insufficient. Retained-key lifetime beyond
seven days and missing-evidence failures remain shared Log verification duties.
