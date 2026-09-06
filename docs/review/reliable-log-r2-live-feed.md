# R2 live feed acceptance

The direct CompleteFeed API passes the same drill on local storage, S3 and MinIO. Production is fixed at `842ebf1`, integrated as `bff2edb`. Root rebuilt the test in a private target and copied the executable before running it. [Receipt and hashes](../implementation/reliable-log/verification/r2-live-feed-842ebf1.json), [test source](../implementation/reliable-log/verification/r2-live-feed-842ebf1.rs), [logs](../implementation/reliable-log/verification/r2-live-feed-842ebf1.txt).

The drill first acquires the session, then arms one after-success DropResponse on the exact v3 ref CAS. The append returns typed Unavailable while the durable head confirms sequence 1. Explicit retry with the same opaque key and binary payload recovers that receipt. Another retry is identical, different bytes conflict, and the next distinct key receives sequence 2.

A second Store uses an empty cache and an injected clock eight days ahead. Its fresh session retrieves the original committed receipt while remaining Unacquired. Different bytes still conflict. This checks stable evidence beyond the generic seven-day operation window. A one-byte read budget reports EventTooLarge. Single-event pages then recover both exact payloads at cursors 1, 2 and 3, with the snapshot head preserved.

The first version of the test incorrectly expected the lost-reply call to retry internally and return success. All three backends instead returned the permitted transient error. Root corrected the test to assert that error and perform the required explicit retry. No production change was made for that test assumption.

This is a direct API test with two Stores in one process. It does not prove JSONL transport, process restart, Foundation reconstruction or safe collection. Those remain separate gates. The merged workspace CI passed in run 34037372727.
