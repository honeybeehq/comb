# Slice 1 — human test script

**What this slice is:** the Comb correctness kernel, physically demonstrable
through `combctl` over your real S3 bucket (`comb-dev-th`, eu-north-1, profile
`th`) and over local files. Objects, refs with conditional updates, leases
with fencing, and the tenant journal. No daemon, no Tree/Volume yet.

Each step below proves a named spec invariant (§6.1). If any step behaves
differently than described, that is a finding — please note exactly what you
saw.

## Setup (once)

```sh
cd ~/Projects/honeybee/comb/repos/comb
cargo build
alias combctl=$PWD/target/debug/combctl
mkdir -p /tmp/comb-play && cd /tmp/comb-play
combctl init --tenant org_trmd --backend s3 \
  --bucket comb-dev-th --region eu-north-1 --profile th
```

This writes `.comb/config.toml` containing your tenant digest key. The cache
lives in `.comb/cache/`.

## 1. Content addressing and the open format

```sh
echo "hello comb" > note.txt
combctl put note.txt            # prints b3k:<digest>
combctl get b3k:<digest>        # prints the content back
combctl put note.txt            # same digest, "deduplicated"
```

Then open the S3 console (bucket `comb-dev-th`): you can see the object under
`comb/comb/v1/tenants/org_trmd/objects/b3k/...`. The format on disk is the
documented envelope — nothing proprietary hiding in a database.

*Proves: immutable content-addressed objects; within-tenant dedup; open
format (§7.2–7.4, §17.5).*

## 2. Refs and the race — two terminals

Terminal A and Terminal B, both in `/tmp/comb-play`:

```sh
# A:
combctl ref set demo/main b3k:<digest>     # succeeds, generation 1
# A and B, as close to simultaneously as you can:
combctl ref set demo/main b3k:<digest>
```

One terminal succeeds (generation advances by exactly one); the other gets
`precondition failed`. Run it ten times — you will never see both succeed for
the same generation, and never a corrupted in-between state.

*Proves: atomic, linearizable per-ref publication (§6.2, invariant "reachable
means complete").*

## 3. Fencing — the heart of the system

```sh
# A:
combctl claim demo/main --writer laptop-a --ttl 300
# prints: fence 1
combctl ref set demo/main b3k:<digest> --fence 1     # works

# B (simulating takeover after A is presumed dead):
combctl claim demo/main --writer laptop-b --steal
# prints: fence 2

# A again (the stale writer, still running):
combctl ref set demo/main b3k:<digest> --fence 1
# -> error: fenced: writer epoch 1 is stale (live epoch 2)
```

A can still compute, but it can no longer commit. Also try `ref set` with no
fence while a lease is live → `lease held by laptop-b`.

*Proves: stale writers cannot commit (§6.1.5, §7.6). This is the property
everything else in the estate — claims, checkpoints, landings — will rest on.*

## 4. The journal

```sh
combctl ref history demo/main
```

Every generation you created above is there — writer, epoch, timestamp,
target. Note what is *not* there: lease renewals (`combctl renew`) change no
logical state and leave no entry.

*Proves: auditable history as a consequence of commits, never a prerequisite
(§7.5a.3).*

## 5. Corruption is detected, never hidden

```sh
combctl get b3k:<digest> -o /dev/null      # "from cache"
# flip one byte in the cached copy:
printf 'X' | dd of=.comb/cache/<digest-hex> bs=1 seek=100 conv=notrunc
combctl get b3k:<digest>
# -> warning: cache entry failed verification — quarantined, refetching
# and the correct content is returned from S3
```

*Proves: caches are disposable; corruption fails closed (§6.1.6, §6.1.11).*

## 6. Kill it mid-flight

```sh
# a bigger file so the upload takes a moment:
mkfile -n 200m big.bin   # or: dd if=/dev/urandom of=big.bin bs=1m count=200
combctl put big.bin & sleep 0.5; kill -9 %1
combctl ref get demo/main        # unchanged, still resolves cleanly
```

The interrupted upload left at most an unreachable orphan in the bucket —
no ref moved, nothing acknowledged, nothing corrupted. Re-running the put
completes it.

*Proves: acknowledged means reachable; partial work is invisible (§6.1.2).*

## 7. Same semantics, no cloud

```sh
mkdir /tmp/comb-local && cd /tmp/comb-local
combctl init --tenant org_local --backend local --root /tmp/comb-local-store
# repeat any of the above — identical behavior, no network
```

*Proves: one contract across backends (§7.11); the local mode that later
gives a laptop in airplane mode identical semantics.*

## What is deliberately NOT in this slice

Encryption envelopes, MinIO backend + conformance suite, pins/GC, symrefs,
release manifests, the full Log (batching, segments, compaction), Tree,
Volume, `combd`. Each arrives in later slices with its own script like this.
