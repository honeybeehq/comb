# Pheromone A1 parent review at 5bda039

Not approved. Source is the immutable archive of 5bda039. The active implementation worktree was not edited for this review. Both isolated parent regressions failed on 5bda039. Compilation passed with the two existing connector dead-code warnings.

## Follower consumes a failed timer write

`State::follow_step` logs `process_event` errors and still sets `matched_through` to the failed event before persisting the follower cursor. `arm_expect_timer` now propagates its error and rolls back the newly added timer, but the follower then skips that origin permanently. Stop before the failed sequence and preserve the successfully completed prefix. Cover failure, retry and restart, including the behavior of listeners that completed before another listener failed.

Regression: `review_failed_timer_arm_does_not_advance_follower`. Register a live expect listener, inject a timer persistence failure, append an origin and run one follower page. The cursor must remain before the origin.

## Failed disarm leaves disk and memory inconsistent

`disarm_listener_timers` removes matching timers before calling `persist_timers`. A persistence error leaves memory changed while disk retains the old timers. A later retry sees no in-memory timer, succeeds without writing the file, and a restart restores a cancelled timer. Stage or roll back the removed timers on persistence failure.

Regression: `review_failed_disarm_retry_clears_durable_timer`. Persist one origin timer, fail the completion write once, then retry successfully. The durable timer file must be empty after the retry.

## Startup rewrite is not accepted

`de71087` already passed independent startup verification across 40 processes and 960 fresh database paths. `5bda039` adds a directory lock whose destructor cannot run after a crash. After bounded waiting, `acquire_init_lock` returns success with `held=false`, so the implementation also cannot claim that it always serializes startup. Nested retry loops enlarge the retry budget, and string matching weakens the typed error classification. Restore the verified typed retry implementation unless a new failure supplies evidence for further changes.

The original failure artifact identifies an `open_path(...).unwrap()` call, not the failing internal SQLite call. The pinned rusqlite 0.32.1 `inner_connection.rs:119` installs a 5000 ms timeout during `Connection::open`. The claim that this artifact proves a failure before timeout installation is unsupported.

## Verification

The two parent regressions run in a private archive and build directory. They use the implementation's existing timer persistence failpoint. No shared daemon or user Pheromone home is used. Test command: `cargo test -p pher --offline review_ -- --test-threads=1`.

Both failures reproduced in 0.50 seconds after compilation. The follower cursor advanced from 1 to 2 after injected timer persistence failure. A disarm retry returned success while the cancelled timer remained on disk. Full output is in [the captured output](../implementation/reliable-log/verification/pher-timer-regressions.txt).

## Disposition at 535e574

Startup implementation now matches verified `de71087`, apart from corrected comments. That correction is accepted. `daemon.rs` is byte-identical to `5bda039`, so neither reproduced timer defect was changed. The two timer regressions remain acceptance blockers. The owner has the executable test snippet and captured failures.

## Disposition at cd1cb26

Both original parent regressions now pass in the isolated source archive. The follower stops before a failed event, and failed disarm restores the in-memory timer list. Startup code is unchanged.

A third parent regression fails: `review_timer_retry_does_not_repeat_successful_listener`. Register a plain `job.started` stream listener with limit 2, then an expect listener for the same subject. The matcher visits the plain listener first. Inject timer persistence failure. The plain listener delivers, the expect listener fails, and the follower correctly retains its old cursor. Clear the failure and retry. The plain listener delivers the same origin again and consumes its second limit. The test asserts the matcher order before injecting failure, so this is a deterministic partial-cascade case.

`process_event` restarts its listener loop with no record of earlier successful effects. Fix the retry boundary by staging fallible timer changes before irreversible delivery effects, or by retaining completed listener progress. Cover multiple expect listeners as well: an earlier successful arm must not be repeated when a later arm fails. Do not infer exactly-once delivery from the global follower cursor alone.

Two original parent tests passed and the new test failed. The test binary hash and captured output are in `verification/pher-timer-cd1cb26.json` and `verification/pher-timer-cd1cb26.txt`. The owner has the executable snippet. No source files in the active Pheromone worktree were edited.

The isolated daemon rebuilt successfully from `cd1cb26`. Its immutable copy passed the real-process smoke test: 520 historical events, two live events, crash/restart, durable cursor recovery, and increasing sequence numbers. Evidence and the binary hash are in `verification/pher-daemon-smoke-cd1cb26.json`. The build emitted four warnings: an unused connector import, two unused connector fields, and unused test helper methods on SqliteLog. This happy-path result does not cover the failing partial cascade.

`cda1f2b` corrects startup comments and review prose only. The pinned rusqlite default is 5000 ms. The earlier 40-process, 960-path startup result remains applicable; no repeat startup stress was run.

## Disposition at c70770c

The diff adds same-origin timer deduplication and disk assertions. It leaves `process_event`, stream delivery and limit accounting unchanged, so it does not address the remaining two-listener retry failure. This disposition is based on the diff; the executable failure was measured at `cd1cb26`. The consolidated owner task now names that single remaining blocker and marks the startup and original timer issues resolved.

The new test helper `persisted_timer_origin_ids` treats any read error or malformed JSON as an empty list. Make unexpected I/O and decode errors fail the test; otherwise assertions of an empty durable timer list can pass on unreadable or corrupt evidence.

## Disposition at 9bb108a

The three previous parent regressions pass. `process_event` now writes timers for its immediate listener list before delivery, limits or shaping. The timer-file assertion helper rejects malformed JSON and unexpected I/O.

A held listener still changes state before those writes. `hold_listen_catchup` pushes an event into `held_live` while constructing the list to process. A sibling timer write then fails. On retry, the same event is pushed again. The new `review_timer_retry_does_not_duplicate_held_live` test reproduces two deliveries of one origin when catch-up releases the queue, consuming a limit of 2. Three earlier tests passed and this one failed in 0.48 seconds. See `verification/pher-timer-9bb108a.json`, its captured output and `pher-held-retry-regression.rs`.

Make held admission part of the retry-safe boundary. Deduplicating only an existing queue is insufficient if catch-up releases the first copy between the failed attempt and retry. Verify that handoff interleaving too.

A separate source observation: the pending tier 3/4 loop still calls fallible `arm_expect_timer` after immediate listener delivery. The claim that every matching timer write precedes delivery is therefore too broad. This path was not exercised by the held-listener test.

No new daemon smoke was run at this commit because the retry boundary still fails. The last measured daemon smoke remains `cd1cb26`; startup verification remains `de71087`.

## Disposition at 3691c2a

All six timer and cascade retry checks pass in the isolated source archive, including two expect listeners, held-event deduplication and catch-up release between a failed attempt and retry. Per-listener progress replaces the earlier staging approach. It is process-local: a crash after delivery but before the durable follower cursor can replay that event, consistent with the existing at-least-once contract. This is not a durable exactly-once delivery claim.

One cleanup regression fails. A limit-1 listener delivers and removes itself, clearing its progress entry through `remove_sub`. `apply_to_listener` then unconditionally inserts that entry again. Repeated short-lived listeners therefore grow `applied_seqs` with retired IDs. `review_retired_listener_drops_applied_progress` proves the entry survives after its matcher is gone. Record progress only while the listener still exists after delivery; this also covers retirement on lag. The owner has the exact test snippet.

The seven focused tests ran in0.74seconds: six passed and the cleanup check failed. Compilation passed with the two existing connector dead-code warnings. Binary hash and output are in `verification/pher-timer-3691c2a.json` and its text artifact. Final daemon rebuild waits for this cleanup; startup code and its earlier verification remain unchanged.

## Requested rerun at ca09777

Rebuilt the immutable `ca09777` archive at the owner's request. All six retry tests pass again, including the exact parent plain-listener cascade case. The newly added timer-file helper test also passes when run from the same binary. The cleanup test still fails: a retired listener remains in `applied_seqs`. No cascade failure is being attributed to this commit. Evidence is in `verification/pher-timer-ca09777.json` and the captured output.

The active owner task files were shortened to current work only; historical imperative checklists were archived outside them. Pheromone's sole outstanding source fix is progress cleanup after retirement, followed by the final daemon smoke.

## Final acceptance at 16b3a2e

Accepted for A1 TrailLog/SqliteLog and the reviewed listener handoff scope. All seven parent regressions pass in an immutable16b3a2e source archive. The final daemon rebuilt successfully and an immutable copy passed the process smoke:520historical events,2live events, crash/restart, retained cursor and increasing sequence numbers. The source and executable hashes are in `verification/pher-a1-final-16b3a2e.json`. Focused test output is in `verification/pher-timer-16b3a2e.txt`.

The final cleanup inserts applied progress only while the matcher still contains the subscription, covering retirement during delivery on either limit or lag. Startup code was unchanged, so the existing40-process/960-path verification atde71087 stands without another stress run. The owner reports76workspace tests passed. Parent verification independently covered the seven regressions, binary build and real-process smoke.

Limits remain explicit: partition0; State held across localSQLite append; live and catch-up hold queues256each; slow consumers disconnect; process-local listener progress with at-least-once replay after a crash; catch-up tiers1-2 plus expect. Meaning/judge processing retains its documented follower behavior. ObjectLog/Comb integration and deterministic delivery IDs are later work. This acceptance does not enable Foundation storage capabilities or destructive collection.
