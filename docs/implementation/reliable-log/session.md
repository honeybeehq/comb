# Execution state

Coordination reference updated 2026-09-06T09:58:55.442884+00:00. Acceptance evidence is linked below.

## Ownership and current work

- Root09572752 owns integration branchfeat/reliable-log-r1 in comb-reliable-log-r1, contracts, CI, review and live backend acceptance.
- R1owner30db6b86: production accepted through aa94980. Final lost-CAS drill e61d872 passed memory/local with the owner and immutable S3/MinIO runs with root. Current task is accepted/idle.
- R2owner3311b020-3aa4-45b6-bae4-9acc0fbcedf1: comb-reliable-log-r2, branchfeat/reliable-log-r2, private target-r2. R2 catalog/feed checkpoint 2587de1 failed root failure-path verification. Ten assertions failed and one close test hung. Owner is integrating remaining R1 followups and correcting feed/catalog/session behavior. Instructions `/tmp/comb-r2-current-task.md` provide R1 picks a675d38, db41bce, e0c911e, 05d3816, aa94980 and e61d872, then authorize catalog/pages/sessions without another signature wait.
- Foundationlead2efd79a9 owns bridge/fixture/acceptance paths. Parent adapter wiring waits for root-checked R2 APIs. Storage caps remainfalse.
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

## Remaining gates

- R2 [contract](r2-contract.md) and [checked handoff](r2-checked-handoff.md): boundedbackendreads, strict envelopes, immutable32-way chunkcatalog, v3manifest, boundedpages/follow, lazy renewable instance-owned publisher. Private Store layoutcomb/v3 separates refs/objects/intents/cache from R1comb/v2. Labels are local; Lease.writer stores instanceID with RefValue.epoch.
- Stable append remains oneopaque1..512byte key +onepayload; no stablePending. HAMT and event publish in one manifestCAS. No physical stablegrouping for this gate.
- Foundation append/read/follow stillUnsupported. Final immutablebinary backend acceptance must pass local,MinIO,S3 with forward/reverse submission, lostreply/retry, restart/freshcache, exactbytes and freshLoro replay. Preparedconfigs `/tmp/comb-foundation-gate-cj4oungg`; never print contents.
- Fixture3real.fdnc changes/2offlinepeers yields2comments, projectionSHA b23b2bdba1887d615f5a3d11add3bc4203cd72eed33e35d4582f6713d8c1424a. Fixture verified; storage capture not yet successful. Loadscript `verification/bridge-load.mjs` awaits storage support.
- ObjectLog, deterministicdeliveryId and broader retention work remain later. Destructive collection disabled; MinIO ignored wrong-tokenDELETE, S3negativeprobe passed but publication races remain unresolved.

## Isolation

R1private targetcomb-reliable-log-r1/target; R2private targetcomb-reliable-log-r2/target-r2; Foundationtarget-bridge; Pheromoneworker uses pheromone/target. Parent Pheromone verification uses `/tmp/comb-pher-review-path.txt` to find its private archive/target. Never interrupt healthy compiles or restart sharedsccache/MinIO/daemons. Root uses immutable copied binaries for acceptance. Workers remain isolated, no nestedagents or push.

Current task files override queued stale messages. A sent message is not evidence of delivery. Check actual branch/API before wiring. Authenticated Apiary agent_spawn is the child control path.
