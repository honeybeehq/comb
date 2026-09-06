# Execution state

This document records orchestration facts for session continuation. It is not an acceptance report.

- Parent Apiary session: `09572752-9d27-4fe5-a222-2f34ea27d7a8`.
- Comb implementation worktree: `/Users/trmd/Projects/honeybee/comb/repos/comb-reliable-log`, branch `feat/reliable-log`, base `b77b64e`.
- Pheromone integration worktree: `/Users/trmd/Projects/honeybee/pheromone/repos/pher-comb-log`, branch `feat/comb-log`, base `9291518`.
- Design A: Claude fable/max, bee `ba27fa95-1ace-43cb-a82e-fea26a6a5eef`, detached checkout `comb-design-a`, expected artifact `/tmp/comb-design-a.md`.
- Design B: Codex gpt-5.6-sol/max, bee `82708653-24ed-48bd-82d8-2e6a1927f90c`, detached checkout `comb-design-b`, expected artifact `/tmp/comb-design-b.md`.
- Consumer map: Grok grok-4.6/xhigh, bee `135f7971-4592-4f8d-815a-4829bd1cf469`, expected artifact `/tmp/comb-pher-integration-map.md`.
- Design A and B completed; independent judge Claude opus/max selected A with grafts from B. Pheromone worker is implementing the local TrailLog slice, owns its branch production files, and must not spawn children. Spawn all children through authenticated Apiary agent_spawn.
- Comb baseline compiled in `/Users/trmd/Projects/honeybee/comb/repos/comb/target`. Set `CARGO_TARGET_DIR` to that path for sequential Comb checks to reuse native dependencies.
- Pheromone baseline test started with `CARGO_TARGET_DIR=/Users/trmd/Projects/honeybee/pheromone/repos/pheromone/target cargo test -p pher --no-fail-fast`.
- AWS test bucket `comb-dev-th`, profile `th`, region `eu-north-1` is reachable. MinIO bucket `comb-dev`, profile `minio`, endpoint `http://trmd-metal-1:9000` is reachable. Both passed all nine baseline conformance checks.
- Backend baseline configurations are isolated under `/var/folders/y2/lgjk786x2qz6s_gt20x091vc0000gn/T/comb-backend-baseline-rhm9bgn4`. Each has a unique verification prefix. Do not print credential or tenant-key contents.
- MinIO is an existing systemd user service, not Docker. Do not modify or restart it for failure tests. Inject client faults or use a separate local test instance.
- Root owns docs/implementation and backend verification. The shared Core/Log implementation follows synthesis.md and cross-judge.md.
- Foundation agent 2efd79a9-ded6-40a3-9251-30a291369cb0 owns feat/foundation-bridge at comb-foundation-bridge, based on b77b64e. Exclusive paths: crates/combctl/src/bin/comb-bridge.rs, crates/combctl/src/bridge/, crates/combctl/tests/bridge_*.rs, docs/testing/foundation-bridge.md and acceptance fixtures/scripts. Coordinate any shared Cargo/lib.rs edit. First gate stdio-only, stable keys retained for complete-feed lifetime.

- Core/Log R1 owner: b9ca1d3d-1a6f-4186-94eb-b71d34537dda, Grok grok-4.6/xhigh, exclusive production writer in comb-reliable-log. Generic timed operations use immutable-history recovery; stable keys use the manifest-rooted HAMT.
- Foundation bridge worker: 5711135c-4831-4bf7-8947-64bf35bba4ab. The default-run compatibility line is integrated as436658c.

- Root commits on feat/reliable-log: b7966bc design/baselines, 436658c default binary compatibility, dbe683a CI, c2a3914 Foundation fixtures. Feature branch pushed through dbe683a; CI run34016077878 passed tests and clippy. Later commits need pushing after verification.
- Stable path FINAL correction: no Pending/permanent intent, opaque bounded key bytes, one key per change, HAMT rooted in same Log manifest CAS, durable Complete retention mode. See stable-key-addendum.md. R1 early sk1 string signature is obsolete. Urgency-now messages2383 and follow-up deliver this before further stable code; await corrected API.
- Pheromone interim review findings delivered with urgency now2385: timestamp filter, bounded replay queues, SQL trim boundary, atomic seed, read snapshots, corrupt JSON errors, checked i64 allocation, durable SQLite sync. Worker retains ownership.
- Live conditional-delete probe: S3 passes; MinIO ignores mismatched If-Match and deletes. Results in conditional-delete-backends.json. GC design review running on Claude judge25c989a5, expected /tmp/comb-gc-review.md. Complete-feed integration does not wait for GC.
- Foundation fixture verifier independently passes with3 changes,2 annotations and expected hash. Actual bridge transport acceptance remains pending.

## Continuation update

- Integrated Foundation fixture/key/acceptance/client work through4b55c99. CI follow-up e6b1d2b is pushed. GitHub run34017850514 passed Rust tests, clippy and eight Node client/key tests. This excludes the still-uncommitted R1 code.
- Current R1 instruction file is /tmp/comb-r1-current-task.md; review observations are /tmp/comb-r1-review-notes.md. The stable-key addendum overrides every early ASCII-key or permanent-intent sketch.
- R2 design reviewer82708653 prepared /tmp/comb-r2-contract.md. Bounded object fetch, catalog and instance-owned writer sessions are the immediate next implementation slice. Contract review is still in progress.
- Pheromone branch now includes01dbe34 after3fb9782 and51fcfbb, with50 worker tests passing. Root found further silent catch-up error handling and disconnect cleanup issues; /tmp/comb-pher-current-task.md records them. Independent daemon smoke is pending the current build.
- Grok message delivery can remain queued while the runtime is busy. Do not assume sent means read. Consolidated task files preserve current decisions. Stopping only an owned child runtime previously caused Hive to revive it and deliver a queued message; no shared services were restarted.
- GC review and root disposition are committed in gc-review.md and gc-design.md. Physical collection stays disabled, including namespace-only deferred deletion.
