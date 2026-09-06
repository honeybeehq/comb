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
