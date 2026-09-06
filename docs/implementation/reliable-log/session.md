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

- Core/Log R1 owner: b9ca1d3d-1a6f-4186-94eb-b71d34537dda, Grok grok-4.6/xhigh, exclusive production writer in comb-reliable-log. Generic timed operations and retained stable keys use immutable-history recovery.
- Foundation bridge worker: 5711135c-4831-4bf7-8947-64bf35bba4ab. Foundation lead reserves the Cargo.toml default-run line for a small compatibility commit.
