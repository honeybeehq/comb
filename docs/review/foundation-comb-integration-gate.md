# Foundation can start Comb integration

The first integration gate passes on local storage, MinIO and S3, in both forward and reverse submission order. Root used one copied executable for all six process captures, then reconstructed each capture in a fresh Loro document through the real Foundation engine. Every run recovered three immutable changes and both explicit-ID comments with projection SHA256 `b23b2bdba1887d615f5a3d11add3bc4203cd72eed33e35d4582f6713d8c1424a`.

[Complete receipt](../implementation/reliable-log/verification/foundation-process-gate.json), [reconstruction logs](../implementation/reliable-log/verification/foundation-process-gate.txt), [captured feeds](../implementation/reliable-log/verification/foundation-process-captures), [gate helper](../implementation/reliable-log/verification/foundation-process-gate.py).

## Fixed inputs and checks

- Comb production is `f238b0fe0c13a2399d8b30e4abfe453254d2de99`, integrated as `d1f8f7c`. Immutable binary SHA256 is `a0489c7e4414a1c524c71d0a3ffc4c7b9d968483f8510ad1640c784784c072f1`.
- The acceptance caller is `316f321b2290aeb327f807775bc74d85dfe4be9e`, integrated as `b1597eb`. It changes only scripts, tests and docs. The production binary was not rebuilt or changed for caller recovery.
- Foundation engine and fixture are at `1fa1b10a1ffe61596b0898ebd3fa5962f155bf85`. The receipt records the runner and fixture hashes separately from the executable.
- The fixed production archive passed workspace all-target tests, clippy and build. The four unchanged parent shutdown/capacity tests fail on b5fc422 and pass on f238b0f. Production integration CI34040437301 passed. The caller's 24 Node tests and syntax checks pass, including a real failure-only runner that writes attempt evidence without success artifacts.

Each run verifies concurrent logical producers while follow is pending, three unique original ranges, identical-key retry receipts, changed-byte conflict, process restart with a new cache directory, exact binary replay with count and raw-byte limits, output frame limits and EventTooLarge. Final receipts require clean process exit after stdout drains. Captured feed order can differ from submission order; all recovered documents converge.

## Transient S3 recovery

The [first process attempt](../implementation/reliable-log/verification/foundation-process-first-attempt.json) stopped at an explicit backend_unavailable append response on S3. It produced no S3 or overall success receipt. A separate [same-binary probe](../implementation/reliable-log/verification/foundation-s3-retry-probe.json) recovered with the same key and bytes, retaining three unique ranges and head 3. No production fix was needed for that permitted transient outcome.

The corrected caller retries only explicit unavailable append responses. It freezes the exact log, key-hex and payload-hex strings, uses at most six attempts under one 60-second monotonic deadline, and records each request ID, outcome and backoff. Conflict, cancellation, deadline, session loss and protocol/client errors remain terminal. The final S3 forward run recovered two first-attempt unavailable responses; the reverse run recovered one. Existing duplicate, conflict, replay and clean-close assertions all remain.

## Boundary of acceptance

Foundation host integration can begin against the JSONL v1 stdio boundary. Each append carries one opaque stable key and one exact binary payload. Comb owns durable formats, index publication and publisher sessions. The first gate retains the entire feed and uses one fenced publisher per log with multiple logical producers. Authored changes remain independent of publication ownership.

Destructive collection remains disabled. Physical stable batching, Pheromone ObjectLog, deterministic delivery IDs, broader scale work and Foundation host wiring are later work. This receipt is not evidence for those features. Direct backend drills separately cover asserted final-CAS reply loss and stable-key recovery with an injected clock beyond eight days; the six process captures do not simulate eight elapsed days themselves.

Shutdown is finite: EOF offers a short grace before cancelling unfinished requests and attempting independent releases. A failed backend or forced termination can leave a lease until TTL. Await responses before closing stdin and retry uncertain appends with the same key and bytes.
