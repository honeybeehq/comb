# Reliable Comb Log implementation

User authorized execution of the September 6 readiness plan. Finish retry safety, bounded storage with safe collection, bounded replay and recovery, and a Pheromone integration verified on MinIO and S3. Do not treat a completed intermediate slice as completion of the program.

## Engineering workflow

- [x] Read astack principles and the feature playbook.
- [x] Ground the current object, ref, Log, GC, and test paths in the readiness report.
- [x] Compare independent architecture candidates and record synthesis.
- [ ] Blocking first steps. Baseline 23 workspace tests pass. Formatting has pre-existing failures. Set storage protocol before implementation.
- [ ] Independent workstreams. Design candidates and consumer/environment discovery are read-only and independent. Storage mutations and Log state share one implementation owner.
- [ ] Shared mutable state. Implementation runs in a dedicated worktree. Keep one owner per file set and sequence protocol changes.
- [ ] Smallest safe decomposition. One owner implements coupled Core and Log behavior. Separate consumer adapter and verification work only after interfaces settle.
- [ ] Delegate implementation with an explicit storage-state model and acceptance tests.
- [ ] Verify unit behavior, actual CLI behavior, sustained workload, and backend failure behavior.
- [ ] Commit verified slices in dependency order.
- [ ] Run independent review and resolve findings.
- [ ] Land the verified branch and seal results.

## Architecture

- [x] Ground. CLI constructs Store over ObjectBackend. Immutable chunk PUT and manifest PUT precede one ref CAS. GroupWriter separately implements that protocol with a cached ref. Read collects all selected chunks. Maintenance changes the same ref. GC traverses refs without a stable root barrier. Ref journal is an independent tenant chain.
- [x] Sketch. Arena stages are frame, fan out, cross-judge, pick, graft, verify.
- [x] Agree. No human checkpoint requested. Proceed with the synthesized design.
- [ ] Implement against the chosen contracts.
- [ ] Scrap if repeated implementation friction disproves the chosen architecture.

## Delivery slices

- [ ] Retry-safe Core mutations and Log append, including conflict, expiry, ambiguous CAS, concurrency, and takeover tests.
- [ ] Bounded manifests, automatic compaction, segment merge, physical retention, pins, and fail-closed GC.
- [ ] Bounded replay, explicit positions, follower recovery, renewable fenced writer sessions, and backend retry behavior.
- [ ] Complete Core journal/ref contracts needed by the consumer and expose reusable Log code.
- [ ] Integrate the current Pheromone storage boundary on one partition while preserving its local mode.
- [ ] Verify live MinIO, S3, sustained load, failure drills, and CI. Document remaining external limitations precisely if a backend cannot be exercised.
- [ ] Refresh README and readiness scorecard with measured results.

The multi-phase-plan playbook is not applied: the requested deliverable is implementation, and the user has already authorized the plan. The feature playbook governs the work.

- [ ] Foundation complete-feed gate: stable keys beyond seven days, bounded bridge replay, reconnect, explicit Trimmed, fresh peer, and fenced publication independent of authorship.
