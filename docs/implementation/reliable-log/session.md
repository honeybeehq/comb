# Execution state

Coordination reference updated 2026-09-06T13:48:23.110065+00:00. Acceptance evidence is linked below.

## Ownership and current work

- Root09572752 owns integration branchfeat/reliable-log-r1 in comb-reliable-log-r1, contracts, CI, review and live backend acceptance.
- R1owner30db6b86: production accepted through aa94980. Final lost-CAS drill e61d872 passed memory/local with the owner and immutable S3/MinIO runs with root. Current task is accepted/idle.
- R2 owner 3311b020: production accepted for bridge integration at 842ebf1. All 25 immutable parent cases pass; the outer guard covers the initial history read through final CAS. Owner is idle. Root owns the integration merge and live acceptance. [Adapter handoff](foundation-r2-adapter-handoff.md).
- Foundation lead 2efd79a9 owns bridge, fixtures and acceptance paths. Adapter work starts from the root integration merge using the checked CompleteFeed and WriterSession APIs. Storage caps remain false until implementation and verification.
- Pheromoneowner135f7971: A1 accepted at16b3a2e in pher-comb-log/feat/comb-log; now idle. Current-only task `/tmp/comb-pher-current-task.md`.
- RetiredR1b9ca has no ownership. Its branchfeat/reliable-log and commits4706b79/29d271e are excluded.

## Accepted evidence

- R1base52c1963 final source review closedF1-F6,S1-S3/depth52 with no new correctness blocker. [Final review](../../review/reliable-log-r1-final.md).
- Retained-target recoverya675d38 and independent clocks/policye0c911e passed worker targeted/workspace checks. Root verified CI34025362547 ata2d9810 and CI34025750563 at918e27a green, including workspace all-target tests/lint and Node checks.
- Foundation transport remains1619f08 with safe regression additionc347f47 and Store compatibility052ca11. Root33tests passed:22protocol,7transport,3realprocess,1binary. cd9e0d4 is rejected.
- PheromoneA1: owner76tests; root7regressions, final build and immutable daemon smoke passed at16b3a2e. [Final receipt](verification/pher-a1-final-16b3a2e.json). Startupde71087 independently passed40processes/960paths; no repeat run needed. Singlepartition/process-local listenerprogress/at-least-once crashreplay limits remain.

- R1 after-success CAS reply-loss drills passed on S3 and MinIO with immutable binary f2d6a4e0bfa154b3d2b603c36455a950dad930274016bf5c6189e4681ffab27a. [Receipt](verification/r1-live-lost-cas-e61d872.json). CI 34026375339 at aa94980 passed.
- [R2 phase 1 review](../../review/reliable-log-r2-phase1.md) requires tenant validation, plaintext bounds before cloning/callbacks, strict legacy codecs and cache recovery.

- [R2 catalog/feed review](../../review/reliable-log-r2-catalog-feed.md) records fixed-2587de1 source findings and eleven executed failure cases. [Receipt](verification/r2-parent-review-2587de1.json). Later owner edits require a fresh fixed-commit run.

- [R2 closure review at 8171fea](../../review/reliable-log-r2-closure-8171fea.md) and [executed evidence](verification/r2-closure-8171fea.json) supersede the older failure list. Catalog regressions pass; feed/lease/integrity and legacy v2 read compatibility remain open.

- [R2 closure review at 5b0d1d7](../../review/reliable-log-r2-closure-5b0d1d7.md) narrows the remaining work to validated ref/commit/target snapshots and lease deadlines across awaited work. [Executed evidence](verification/r2-closure-5b0d1d7.json).

- [R2 closure review at a53d49b](../../review/reliable-log-r2-closure-a53d49b.md) records 21 closed cases and three executed remaining failures, including two verified regressions against 5b0d1d7. [Receipt](verification/r2-closure-a53d49b.json).

- [R2 closure review at 1698ecd](../../review/reliable-log-r2-closure-1698ecd.md) closes all preceding 24 cases and records one remaining outstanding-I/O termination failure. [Receipt](verification/r2-closure-1698ecd.json).

- [R2 closure review at bd0b9ad](../../review/reliable-log-r2-closure-bd0b9ad.md): all 25 parent cases pass. One outer-guard scope correction remains before integration. [Receipt](verification/r2-closure-bd0b9ad.json).

- [R2 final closure at 842ebf1](../../review/reliable-log-r2-closure-842ebf1.md): all 25 parent cases pass and the outer guard source correction is closed. [Receipt](verification/r2-closure-842ebf1.json). Combined-tree tests, lint, build and eight JavaScript tests pass. Merged backend conformance and legacy recovery pass on S3/MinIO. [Merge receipt](verification/r2-merge-842ebf1.json).

## Remaining gates

- R2 [contract](r2-contract.md) and [checked handoff](r2-checked-handoff.md): boundedbackendreads, strict envelopes, immutable32-way chunkcatalog, v3manifest, boundedpages/follow, lazy renewable instance-owned publisher. Private Store layoutcomb/v3 separates refs/objects/intents/cache from R1comb/v2. Labels are local; Lease.writer stores instanceID with RefValue.epoch.
- Stable append remains oneopaque1..512byte key +onepayload; no stablePending. HAMT and event publish in one manifestCAS. No physical stablegrouping for this gate.
- Foundation append/read/follow stillUnsupported. Final immutablebinary backend acceptance must pass local,MinIO,S3 with forward/reverse submission, lostreply/retry, restart/freshcache, exactbytes and freshLoro replay. Preparedconfigs `/tmp/comb-foundation-gate-cj4oungg`; never print contents.
- Fixture3real.fdnc changes/2offlinepeers yields2comments, projectionSHA b23b2bdba1887d615f5a3d11add3bc4203cd72eed33e35d4582f6713d8c1424a. Fixture verified; storage capture not yet successful. Loadscript `verification/bridge-load.mjs` awaits storage support.
- ObjectLog, deterministicdeliveryId and broader retention work remain later. Destructive collection disabled; MinIO ignored wrong-tokenDELETE, S3negativeprobe passed but publication races remain unresolved.

## Isolation

R1private targetcomb-reliable-log-r1/target; R2private targetcomb-reliable-log-r2/target-r2; Foundationtarget-bridge; Pheromoneworker uses pheromone/target. Parent Pheromone verification uses `/tmp/comb-pher-review-path.txt` to find its private archive/target. Never interrupt healthy compiles or restart sharedsccache/MinIO/daemons. Root uses immutable copied binaries for acceptance. Workers remain isolated, no nestedagents or push.

Current task files override queued stale messages. A sent message is not evidence of delivery. Check actual branch/API before wiring. Authenticated Apiary agent_spawn is the child control path.
