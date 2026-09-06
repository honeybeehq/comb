# Retry drills

Workspace tests cover lost ref-CAS replies (C4, L2), concurrent same-id calls, intervening writes with a counted seek, expiry, complete-feed stable keys, group companion acknowledgement, skip-pointer recovery, original results after takeover, corrupt stable-index nodes, and empty/oversized input.

R1 read is still unbounded (`LogStore::read`). Bounded pages and writer sessions are R2.

Live MinIO/S3 (no secrets in the repo):

```
COMB_DRILL_BACKEND=s3 COMB_S3_BUCKET=... COMB_S3_REGION=... COMB_S3_ENDPOINT=... \
  cargo test -p combctl --test backend_drills -- --nocapture
```

`COMB_DRILL_BACKEND=memory` or `local` also work. Unset, the test skips.

`combctl log bench` remains the append overhead runner.
