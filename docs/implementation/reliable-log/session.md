# Execution state

As of 2026-09-06T08:44:25Z. This is a coordination reference, not an acceptance report.

## Active ownership

- Root Apiary session `09572752-9d27-4fe5-a222-2f34ea27d7a8` owns coordination, `docs/implementation/reliable-log`, CI, root review records and live backend acceptance.
- Current integration checkout is `/Users/trmd/Projects/honeybee/comb/repos/comb-reliable-log-r1`, branch `feat/reliable-log-r1`, based on `c948306`.
- Sole R1 production owner is `30db6b86-e87b-4213-9797-87909e1a3c29`, Grok grok-4.6/xhigh. Current instructions are `/tmp/comb-r1-replacement-task.md`. Review notes are `/tmp/comb-r1-review-notes.md` and [the concurrency review](r1-concurrency-review.md).
- Old worker `b9ca1d3d-1a6f-4186-94eb-b71d34537dda` is retired. Its checkout `comb-reliable-log` preserves old WIP. It has no write ownership. Later messages and signatures from that worker are obsolete.
- Foundation lead `2efd79a9-ded6-40a3-9251-30a291369cb0` owns `feat/foundation-bridge` in `comb-foundation-bridge`. Its paths are `crates/combctl/src/bin/comb-bridge.rs`, `src/bridge`, `tests/bridge_*`, bridge fixtures, scripts and review/testing docs. Worker `5711135c-4831-4bf7-8947-64bf35bba4ab` owns transport implementation there. Shared storage coordination stays through root.
- Pheromone worker `135f7971-4592-4f8d-815a-4829bd1cf469` owns `feat/comb-log` in `/Users/trmd/Projects/honeybee/pheromone/repos/pher-comb-log`. Current instructions are `/tmp/comb-pher-current-task.md`.
- Design A bee `ba27fa95-1ace-43cb-a82e-fea26a6a5eef`, R2 designer `82708653-24ed-48bd-82d8-2e6a1927f90c` and independent reviewer `25c989a5-a1ca-44e0-b6ad-692eae99826b` have completed their current assignments.

## Verification and remaining gates

- Foundation transport production remains at `1619f08`. Safe test-only follow-up `50dbaa0` is integrated as `c347f47`. Commit `cd9e0d4` is rejected and not integrated. Source picks `209199a`, `290c609`, `ec4fbd9`, `22c14d6`, `df98595` became `35bdd5c`, `e3d4019`, `cbd99c1`, `e93f37c`, `1619f08`.
- [CI run 34020281297](https://github.com/honeybeehq/comb/actions/runs/34020281297) passed workspace tests, clippy, eight Node client/key tests and script syntax at `1619f08`. This commit excludes uncommitted R1 production changes. Foundation also reports 21 protocol, five transport, three real-process and one binary test passing in its private target.
- [CI run 34022453016](https://github.com/honeybeehq/comb/actions/runs/34022453016) passed workspace tests, clippy and Node checks at `c347f47`. New tests cover cancellation ownership and exact request-ID correlation. Root has uncommitted compatibility edits in the bridge binary and protocol/transport tests for `Store::new` and the current-schema overflow fixture. These await R1 verification.
- Bridge append, read and follow still return Unsupported. Both storage capabilities remain false. No successful storage capture or Foundation readiness claim exists.
- Retired-branch commit `4706b79` is not accepted. Its `read_page` loads an unbounded feed before applying limits and cannot satisfy R2.
- Active R1 is uncommitted. The worker reports core and object crates typecheck and is checking combctl and tests. Publication proof, exact original retry results, group admission races and HAMT validation remain subject to final review.
- [R2 contract](r2-contract.md) is finalized. Implementation follows verified R1. It requires bounded backend reads, catalog manifests, bounded replay and renewable instance-owned writer sessions. R1 unbounded reads do not satisfy the bridge gate.
- Stable append uses one opaque key of 1 through 512 bytes and one payload. No stable Pending intent exists in the accepted design. A persistent HAMT is published with the event in the same manifest CAS. [The stable-key addendum](stable-key-addendum.md) overrides early batch and ASCII-key sketches.
- Three real Foundation .fdnc changes from two offline peers independently replay to two comments. Expected projection SHA256 is `b23b2bdba1887d615f5a3d11add3bc4203cd72eed33e35d4582f6713d8c1424a`. This verifies the fixture, not bridge storage.
- Final backend acceptance must use an immutable copied bridge binary, forward and reverse submission, restart with a fresh cache, exact captured bytes and fresh Loro replay. Prepared isolated configurations are in `/tmp/comb-foundation-gate-cj4oungg`. No successful run has occurred there.
- `verification/bridge-load.mjs` is syntax-checked and refuses the unsupported provisional bridge without a receipt. Throughput, latency and sampled RSS measurements await actual storage support.
- Pheromone is committed through `7260fe7`, with 60 worker tests reported passing. Root independently passed 55 tests at `618de39` and the isolated daemon smoke at `01dbe34`. Those results do not approve later changes.
- Pheromone startup fix `de71087` independently passed 40 process runs across 960 fresh database paths. Evidence is in `verification/pher-firstopen-fixed.json`. Follow-up `5bda039` is not approved: its speculative startup lock and text matching are rejected, and timer disarm rollback/follower checkpoint handling remain under review. Root is compiling isolated regressions against an archive of that commit.
- Earlier Pheromone fixes addressed held-live expect timer ordering and concurrent SQLite first-open failure. Root reproduced SQLite code 5 on the first isolated attempt. Evidence is committed in `verification/pher-firstopen-failure.txt` and `.json`. Repeated startup verification and a rebuilt daemon smoke remain pending.
- ObjectLog integration, deterministic delivery identity and the broader retention work remain unfinished.

## Build and backend isolation

- R1 uses only `/Users/trmd/Projects/honeybee/comb/repos/comb-reliable-log-r1/target` for Cargo output.
- Foundation uses only `/Users/trmd/Projects/honeybee/comb/repos/comb-foundation-bridge/target-bridge`.
- Pheromone uses `/Users/trmd/Projects/honeybee/pheromone/repos/pheromone/target`.
- The original Comb shared target and the unused `comb/target-r1` clone are not current build targets. Do not interrupt healthy compiles to move artifact locks.
- AWS profile `th`, bucket `comb-dev-th`, region `eu-north-1` and MinIO profile `minio`, bucket `comb-dev`, endpoint `http://trmd-metal-1:9000` passed nine baseline conformance checks on isolated prefixes. Configuration contents contain private keys and must not be printed.
- MinIO is an existing systemd user service. No shared daemon, backend or sccache restart is authorized for these drills.
- Physical collection remains disabled. S3 passed the negative conditional-delete probe. MinIO ignored a wrong If-Match and deleted the probe object. [GC review](gc-review.md) records additional unresolved publication races. Complete-feed Foundation acceptance does not require collection.

## Coordination

Current task files override stale queued sketches. A sent message is not evidence of delivery. Check the actual source and checked signatures before wiring a consumer. Spawn children through authenticated Apiary agent_spawn. No worker has permission to spawn nested agents or push.
