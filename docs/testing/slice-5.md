# Slice 5 — human test script: group commit, compaction, retention, sweeper

**What this slice is:** the Log grows up (spec §8.7, §8.11, Phase C core) —
group commit batches many producers into one CAS per commit window,
compaction merges WAL chunks into segments, retention trims with explicit
`Trimmed{resume_at}`, and the reachability sweeper collects the orphans
that kill-tests and compaction leave behind.

Playgrounds: `/tmp/comb-minio` (MinIO on trmd-metal-1) as before.

## 1. Throughput bench

```sh
cd /tmp/comb-minio
combctl log bench bench1 --events 2000 --producers 16 --payload-bytes 64
```

Read the numbers, not just the total: `commits` vs `events acked` shows
batching (16 producers → ~16 events per CAS). **The events/sec from your
Mac is a measure of your tailnet round-trip to Hetzner** (3 sequential
backend calls per commit round), not of the engine — the honest number for
the production shape comes from running the same bench *on* trmd-metal-1
next to MinIO (see the published numbers at the bottom).

While a bench runs, `combctl log follow bench1` in another terminal shows
followers keeping up with batched commits.

## 2. Compaction under a live reader

```sh
combctl log status chat            # note chunks N
combctl log compact chat           # merges WAL chunks into one segment
combctl log status chat            # chunks 0, same head_seq, same epoch
combctl log read chat --from 1     # identical contents
```

Run it while a `follow` is attached — the follower never skips or repeats.
The superseded chunk objects are now orphans; step 4 collects them.

*Proves: compaction changes representation, never contents; races with
appends resolve by conditional-update retry (§8.11, drills L4/L5).*

## 3. Retention floor

```sh
combctl log trim chat --before 3
combctl log read chat --from 1
# -> error: trimmed: position is below the retention floor; resume at 4
combctl log read chat --from 4     # works
```

*Proves: trimming is explicit, never a silent gap (drill L9).*

## 4. The sweeper — reachability GC

Your kill demos and the compaction above left orphaned objects in the
bucket. Watch them go:

```sh
combctl sweep --grace-mins 60      # dry-run: fresh orphans still in grace
combctl sweep --grace-mins 0       # dry-run: lists them as candidates
combctl sweep --grace-mins 0 --yes # deletes them
combctl log read chat --from 4     # everything still reads perfectly
combctl sweep --grace-mins 0       # nothing left to collect
```

The sweeper walks every ref, BFS-expands every digest reachable from them
(manifests → chunks/segments, journal chains), and deletes only
unreachable objects older than the grace window.

**Caveat to see for yourself:** a blob you `combctl put` but never
reference from any ref is *unreachable by definition* and will be listed
as a candidate. That's the spec's reachability model working — durable
means reachable from a root. Pins (a later slice) are how loose objects
get protected.

*Proves: GC by reachability with grace; deletion never touches reachable
data (§19, drills G1/G4).*

## CI drills

`cargo test -p combctl --test phase_c_drills`: group-commit correctness
(200 events, 10 producers, disjoint contiguous ranges, fewer commits than
events), L4 compact/append race without loss, L5 identical read after
compaction, L9 trim floor, sweeper deletes orphans-and-only-orphans with
grace respected.

## Published bench numbers (2026-08-24)

| Topology | events/sec | avg batch | ack p50 | note |
|---|---|---|---|---|
| Mac → MinIO over tailnet | 39 | 15.7 | 376ms | RTT-bound: ~130ms × 3 calls/commit |
| Mac → local disk | 148 | 63.6 | 438ms | macOS F_FULLFSYNC-bound (~70ms × 6/commit) |
| trmd-metal-1 → its own MinIO, 64 producers | 3,064 | 64.0 | 21ms | sovereign shape (release build) |
| trmd-metal-1 → its own MinIO, 256 producers | **12,153** | 256.0 | 21ms | same box, bigger batches |

Interpretation: commit cadence = backend round-trip × 3 (~21ms on-box);
events/sec = batch size / cadence, so throughput scales linearly with
producer count while ack latency stays flat — 64 → 256 producers
quadrupled throughput at identical p50. Zero CAS conflicts in every run.
The engine adds no measurable overhead; the backend and topology decide
everything, which is exactly what the spec predicts for an
object-store-backed log. Reproduce it yourself:

```sh
ssh trmd-metal-1
export AWS_ACCESS_KEY_ID=combadmin
export AWS_SECRET_ACCESS_KEY=$(grep PASSWORD ~/minio/credentials | cut -d= -f2)
cd /tmp/comb-sovereign
~/comb-bench/target/release/combctl log bench sov3 --events 50000 --producers 256
```
