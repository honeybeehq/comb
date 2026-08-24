# Slice 4 — human test script: the minimal Log

**What this slice is:** the minimal Comb Log (spec §8, Phase B scope) —
atomic batch append, ordered read, live follow, and fenced leader takeover
over one partition. The partition manifest ref is the linearization point:
chunks upload create-only and exist logically only once the manifest ref
advances. This is a durable, multi-reader tail -f over your own MinIO or
S3 — and the mechanism Pheromone's cloud mode and Flight's journals build on.

## Setup (once, if you don't have the playground from slice 2)

```sh
alias combctl=~/Projects/honeybee/comb/repos/comb/target/debug/combctl
mkdir -p /tmp/comb-minio && cd /tmp/comb-minio
combctl init --tenant org_trmd --backend s3 \
  --bucket comb-dev --region eu-north-1 \
  --profile minio --endpoint http://trmd-metal-1:9000
```

The S3 playground from slice 1 works identically — the log doesn't care
which certified backend is underneath.

## 1. Durable tail -f across processes

Terminal A (the follower):

```sh
cd /tmp/comb-minio
combctl log follow chat
```

Terminal B (the writer):

```sh
cd /tmp/comb-minio
combctl log append chat "hello from studio" --writer studio
combctl log append chat "two events" "at once" --writer studio
combctl log status chat
```

A prints each event within ~0.5 s, in sequence order, with no daemon and no
connection between the terminals — coordination happens entirely through
the object store. Run the follower on a *different machine* pointed at the
same config for the full effect.

*Proves: append acknowledged only after manifest publication; followers
scale without touching the writer (§8.7, §8.10).*

## 2. Leader takeover, live

With the follower still running:

```sh
# terminal C — take the log away from studio:
combctl log steal chat --writer metal

# terminal B — the deposed leader tries to keep writing:
combctl log append chat "stale write" --writer studio
# -> error: lease held by metal until ...

# terminal C:
combctl log append chat "metal speaking now" --writer metal
```

The follower's stream shows studio's events, then metal's — the stale
write **never appears**, and sequences stay contiguous. `log status` shows
the epoch incremented.

*Proves: one leader per partition, epoch fencing, zero interleaving (§8.9,
drill L3).*

## 3. Kill an append mid-flight

Appends are usually too fast to kill by hand, so hold the dangerous window
open deliberately — the pause sits exactly between chunk upload and
manifest publication:

```sh
combctl log status chat            # note head_seq
COMB_PAUSE_BEFORE_PUBLISH_MS=3000 combctl log append chat "doomed" --writer metal &
sleep 1; kill -9 %1
combctl log status chat            # head_seq UNCHANGED — no ack was given
combctl log read chat --from 1     # "doomed" is nowhere; sequences contiguous
```

The chunk was already uploaded when you killed it, but it is logically
nonexistent: nothing references it (a later GC slice sweeps such orphans).
Without the env var the append usually wins the race and simply commits —
also correct: no gap, no torn state, no false ack, ever.

*Proves: a chunk not referenced by a committed manifest is logically
nonexistent (§8.7, drill L1).*

## 4. Replay

```sh
combctl log read chat --from 1     # everything, in order, forever
combctl log read chat --from 3     # from any retained position
```

## CI drills

`cargo test -p combctl --test log_drills`: append/read roundtrip,
takeover fences the old leader (L3), crash-before-publication invisible
with gapless retry (L1), follower sees live appends by logical state.

## Not in this slice

Group commit / batching windows, compaction into segments, indexes,
retention (`trim_before`), partitions, and the journal's migration onto
this Log — those are Phase C, driven by the Pheromone workload.
