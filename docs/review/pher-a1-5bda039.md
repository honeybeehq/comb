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
