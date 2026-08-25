# Comb as Pheromone's scale substrate — deep analysis and scale envelope

Date: 2026-08-25. Comb @ `8428831` (Slice 5), Pheromone @ `9291518` (ingest/follower split).
Sources: spec v0.3 (§6–8, §13, §23), full read of both code bases, and a measurement
session on an M4 Max (16 cores, 128 GB, APFS) plus the published `trmd-metal-1` MinIO numbers
in `docs/testing/slice-5.md`. Nothing in either repo was modified for the measurements.

## 1. Verdict

**Comb's design is the right substrate; Comb's implementation is not yet one.** The
correctness kernel (immutable objects, CAS'd ref as the per-partition linearization point,
epoch fencing, group commit) is real, tested on memory/local, and manually certified on S3
and MinIO. Everything that turns that kernel into a *log that survives sustained load with
many followers* is missing or manual: bounded manifests, automatic compaction, streaming
reads, idempotent appends, partitions, a follower that survives errors, and a leader that
renews its lease.

**Pheromone is two structural changes away from being able to use it** — the `TrailLog`
trait does not exist yet and the daemon still serializes everything behind one
`Mutex<State>` — and its current ceiling (~2–3k events/s, single process, single node) is
set by per-event SQLite autocommit under that lock, not by matching.

**Scale we can handle today vs. after the work below:**

| Dimension | Pheromone today (local SQLite) | Comb Log today | Comb Log after §5 fixes |
|---|---|---|---|
| Ingest, one trail | 2–3k ev/s (8 pipelined producers); 600 ev/s one strict connection; 25 ev/s via CLI spawn | 12k ev/s @ 256 in-flight producers on-box MinIO (p50 ack 21 ms); 4.4k ev/s on local APFS (p50 54 ms) | 10k–100k ev/s per partition, bounded by chunk upload bandwidth; linear in partitions |
| Ack latency | p50 0.5 ms, p99 13–29 ms (checkpoint spikes) | ≈ commit_window + 3 backend RTTs: ~21 ms on-box MinIO, ~50 ms local (6 × F_FULLFSYNC), ~380 ms over a tailnet | same floor; 2 RTTs if chunk+manifest PUTs are issued concurrently |
| Sustained load | fine; ~350 B/event on disk, linear | **degrades within minutes**: manifest re-uploaded whole per commit, O(n²) bytes until a manual `compact` (measured 1.6 GB of manifests for 1.3 MB of frames) | flat |
| Followers per trail | ~10 measured with no ingest drop; cost = daemon CPU (JSON clone per tail) | unbounded in principle (writer-independent) but N × 1–2 GETs per poll on one hot key, no conditional GET | ~1,000 per partition at 250 ms poll before S3 per-prefix GET limits; more with a hint/fan-out layer |
| Replay from far back | materializes whole backlog in RAM under the lock: 2 s first line and 1.35 GB RSS at 450k events | `read(from)` downloads *everything* from cursor into a `Vec` — no `max`, no range reads | streaming, flat memory, target 200 MB/s per follower (L8) |
| Multi-node | none (one daemon owns `pher.db`; `next_seq` in memory) | any process can append (lease) or follow; takeover works but leadership is nominal (see §4) | fenced leader + fronts; bucket-direct follow via scoped credential |
| Durability | SQLite WAL `synchronous=NORMAL`: survives kill -9, not power loss | object-durable before ack (correct) | same |

## 2. What Comb actually is right now

~3.6k lines of Rust in three crates. Real and correct:

- Tenant-keyed BLAKE3 object envelope; create-only PUT; verified read-through cache with
  quarantine (`crates/combctl/src/store.rs:44-79`).
- Refs with `generation`/`epoch`/lease in one CAS'd value; `claim`/`renew`/`release`/`steal`
  (`store.rs:104-224`). Linearization is the backend's conditional PUT
  (`If-None-Match: *` / `If-Match: <etag>` on S3, `flock`+rename on local).
- Log: `append`, `read`, `follow`, `compact`, `trim_before`, `GroupWriter` group commit,
  reachability sweeper (`crates/combctl/src/log.rs`, `sweep.rs`).
- 9-check backend conformance suite incl. an 8-racer CAS test; fault-injected memory
  backend; drills C1/C3/C6/C7, L1/L3/L4/L5/L9 analogues; a seeded chaos loop over one ref.

Not real yet, in spec order: compression/encryption (literal `"none"`), range reads,
`head()`, multipart, `capabilities()`, op-IDs/idempotency (§8.8 — drill C4's test asserts
the *opposite* of the spec), pins, symbolic refs, change hints, `TrailLog`/`Pos`,
partitions (`p0` is a literal), `seek_ts`, binary frames/zstd/CRC/indexes, automatic
compaction triggers, `commit_bytes`, lease renewal loop, `Fenced` on append, fronts,
control manifest, delivery log, metrics. README still says "no implementation yet".

## 3. Measured numbers

### Pheromone (isolated `PHEROMONE_HOME`, `PHER_EMBED=off`, unix socket, 64 B payloads)

| Path | ev/s | Latency |
|---|---|---|
| `pher emit` CLI per event | 25 | ~40 ms (process spawn; `pher --version` = 29 ms) |
| 1 conn strict req/resp, 10k | 592 | p50 474 µs, p99 13.5 ms |
| 1 conn pipelined, 20k | 2,744 | — |
| 8 conns strict, 20k | 1,659 | p50 2.3 ms, p99 29 ms |
| 8 conns pipelined (5 runs, 20k–150k) | 1,676 – 3,261 | ±50 % run variance |
| + 100 never-matching `where` subs | 2,535 / 3,316 | matcher lag ≤ 202 events, 0 after run |
| + 10 subs matching 1 % with `then emit` | 2,533 | 2k deliveries + 2k re-ingests ≈ −25 % |
| + 10 concurrent `pher tail` followers | 2,038 – 2,215 | no ingest drop; daemon CPU 19 % → 63 % |

Daemon CPU at the ceiling: **19 %**. Hot frames are `fcntl`, `pwrite`, `fsync`,
`__psynch_mutexwait`, `sqlite3Prepare` — one autocommit INSERT per event under the global
lock, statement re-parsed each time (`db.rs:155`, `daemon.rs:713`). Tier-1/2 matching of
100 predicates per event is invisible against that floor; the in-process matcher bench
(49k–290k ev/s with 10k subs, `README.md:149`) does not transfer to the daemon.

Trail growth: 350 B/event; `status`/`ls`/`why` stay at 0.03–0.07 ms at 454k events.
`tail --after 0` at 454k events: first line after 2.0 s, RSS 1.35 GB (not returned), 1.69 GB
with 10 concurrent full tails.

### Comb (local FS backend; on-box MinIO figures from `slice-5.md`)

| Measurement | Result |
|---|---|
| Sequential CLI appends | 14.7/s (68 ms each; 6 F_FULLFSYNCs per commit ≈ 45–55 ms floor) |
| 1,000 events in one append | 6,900 ev/s (one commit) |
| GroupWriter, 10 ms window, 1 / 8 / 64 / 256 producers | 15 / 135 / 1,145 / 4,440 ev/s; p50 ack 53–57 ms flat |
| Same on trmd-metal-1 → own MinIO, 64 / 256 producers | 3,064 / 12,153 ev/s; p50 21 ms |
| Mac → MinIO over tailnet | 39 ev/s, p50 376 ms (RTT-bound) |
| Read 20k events in 2,500 chunks / after compaction | 18k frames/s / 82k frames/s |
| Follow latency, poll 10 ms / 500 ms (default) | p50 ≈ 0 / 261 ms from ack |
| Storage after 46k events, 3,300 commits, no compaction | **2.83 GB, 21,329 objects** |

Throughput is purely `in-flight producers ÷ commit cadence`; ack cost is per-commit and
flat, exactly as designed. The last row is the structural defect (see §5.1).

## 4. Scale model (first principles, per partition)

```
commit cadence     = 3 sequential backend RTTs            (chunk PUT → manifest PUT → ref CAS)
                     ≈ 21 ms on-box MinIO | ~50 ms local APFS | 60–120 ms S3 Standard | ~380 ms WAN
commits/s ceiling  = 1 / cadence                           ≈ 47 | 20 | 8–16 | 2.6
events/s           = commits/s × batch                     batch ≤ commit_bytes (4 MiB ≈ 12k events @ 350 B)
                     → 47 × 12k ≈ 560k ev/s theoretical; realistically upload-bandwidth-bound at 10k–100k
ack latency        ≈ commit_window (10 ms) + cadence      → 30 ms on-box, 70–130 ms S3
follower cost      = 1 GET (ref) per poll, +1 GET (manifest) on change
                     100 followers @ 250 ms = 400 GET/s + 400 × manifest bytes
                     bounded manifest (256 chunks ≈ 40 KB) → 16 MB/s; unbounded → catastrophic
follower ceiling   ≈ 5,500 GET/s per S3 prefix ÷ 4/s      ≈ 1,000 per partition without hints/fan-out
tail latency       ≈ poll/2                                → 125 ms at 250 ms poll; ms with a hint channel
```

Three consequences:

1. **Per-partition throughput is not the problem; per-append latency is.** A single
   partition carries tens of thousands of events/s once group commit batches by bytes, but
   every producer waits ≥ 30–130 ms for an object-durable ack. Interactive `pher emit`
   users will feel it; taps and bridges will not. Pheromone should ack local mode as
   today and expose the cloud ack latency honestly (§6.4).
2. **Partitions are the only horizontal lever for commit rate**, and they don't exist.
   Trails that need > ~40 commits/s of *independent* producers (not batchable through one
   leader) need `hash(subject) % N`.
3. **Follower count scales independently of the writer only if the manifest is small and
   polls are cheap.** Both need work (§5.1, §5.4). A Pheromone change-hint channel
   (§7.5a.6) turns 250 ms tail latency into single-digit ms without touching correctness.

## 5. Blocking gaps in Comb, ranked

1. **Unbounded manifest, rewritten whole per commit; compaction manual; segments never
   merged** (`log.rs:131-139, 255-264`). At 47 commits/s the manifest grows ~7 KB/s and every
   commit re-uploads it; every follower poll downloads it. Fix: bound `chunks` to
   `max_wal_chunks`, run a fenced compactor automatically (`compact_after`,
   `max_wal_chunks`), merge segments in levels, keep a small manifest.
2. **No idempotency contract.** No op-IDs, no event-ID admission dedup; an ambiguous CAS
   ack returns `Err` to producers while the batch *is* committed → retry duplicates.
   Drill C4 currently codifies double-apply (`drills.rs:109-113`). L2 unimplemented.
3. **Reads materialize everything** (`log.rs:203-225`): no `max`, no streaming, no range
   reads, no index. L8 (1B-event replay, flat memory) is impossible by construction.
   `follow` dies on `Trimmed`/`NotFound`/transient errors (`log.rs:333,337`).
4. **Follower polling is naive**: 1–2 unconditional GETs per poll per follower on one key;
   no `If-None-Match`, no hints, no fan-out layer. CLI default poll is 500 ms (spec 250).
5. **Leadership is nominal.** Writer identity is a free-form string — two processes with
   the same name co-lead and interleave (`log.rs:107-112`); no renewal loop (30 s lease
   refreshed only by commits, `log.rs:510`); `steal` ignores liveness; non-leader gets
   `LeaseHeld` not `Fenced`; no clock slack; `Utc::now()` everywhere so L3/L7 are
   untestable deterministically.
6. **S3 error mapping**: `409 ConditionalRequestConflict` and `503 SlowDown` →
   `BackendUnavailable`, never retried; no backoff, jitter or timeouts (`s3.rs:54-74`).
   Under real contention on AWS this is spurious append failure.
7. **Partitions absent** (§8.12) — `p0` literal; no `Pos`, no routing, no control manifest.
8. **Sweeper is O(all objects)** per pass with mtime grace, no pins, no tombstones
   (`sweep.rs:42-76`); stale-manifest readers get `NotFound` (L5 real case).
9. **Tenant-wide journal head is a global CAS point** for every non-Log ref update
   (`store.rs:39,233`), while Log refs bypass the journal entirely — takeovers and
   compactions leave no audit trail (violates §23.3 exit criterion).
10. **Format/runtime debt**: JSON chunks with `String` payloads (no binary), no
    zstd/CRC/index, blocking `std::fs` inside `async fn` (will stall a tokio `combd`),
    local version token = unkeyed hash of bytes (ABA-prone), no `commit_bytes`.
11. **Test gaps that matter for us**: no two-distinct-writer race, no lease-expiry
    takeover, no multi-follower, no follower-under-compaction, no ambiguous-manifest-CAS,
    no S3/MinIO in CI, chaos never touches the Log or runs concurrent tasks.

Items 1–4 are required before any sustained Pheromone load; 5–7 before multi-node; 8–11
before hosted.

## 6. What Pheromone owes (in order of leverage)

Measured ceiling is the lock + per-event SQLite write, so this ordering is by throughput
payoff, not by Comb dependency.

1. **`TrailLog` trait + `SqliteLog`** (spec §8.2; `SCALE.md` "Remaining for Phase A").
   Today the log is the concrete `Db` with one `!Sync` `Connection` inside `State`
   (`db.rs:21`, `daemon.rs:107`), and the caller picks `seq`. Comb assigns `Pos`; `next_seq`
   must leave `State`.
2. **Group commit in `SqliteLog`**: batch appends in one transaction on a 1–10 ms window,
   `prepare_cached` the INSERT, emitters park on a oneshot. This alone should move the
   daemon from ~2.5k to tens of thousands of ev/s (daemon is at 19 % CPU); it also mirrors
   `ObjectLog`'s semantics so cloud mode is not a behavioural surprise.
3. **Break `Mutex<State>`**: log writer behind `Arc<dyn TrailLog>`; matcher as an RCU'd
   `Arc<Matcher>` snapshot swapped on registration; small control-state locks. The
   follower currently holds the global lock across ONNX inference (`daemon.rs:819`),
   process spawns for `cmd`/`buz` sinks (`:1430-1441, 1480-1485`), and whole-file JSON
   rewrites (`subs.json` per delivery `:1408`, `pending.json`, `cursors.json`,
   `outbox.json`, `timers.json`, `judge_budgets.json`).
4. **Tails/listeners become log followers** with their own cursor, paginated via
   `read(Pos, max)`, releasing the lock between pages. Kills the 1.35 GB backlog
   materialization and the unbounded per-tail mpsc.
5. **Deterministic `deliveryId = hash(sub, event.id, seq)`** so follower replays are
   idempotent (today replays mint fresh counters; `deliveryId` dedup cannot catch them).
   Prerequisite for at-least-once over Comb, where ambiguous acks are normal.
6. **Two-stage follower**: tiers 1–2 produce jobs; a worker pool does embed/judge/sinks
   off-lock; redundant work removed (`match_ids` + `pending_ids` + `evaluate` = three trie
   walks/evaluations per event, O(n) `Matcher::get`, `Subscription::clone` per match).
7. **Control state to Comb refs / delivery log** (§8.13, §13.5) and cursor commits
   batched every N events / T ms instead of one RPC + JSON rewrite per delivery
   (`main.rs:609-616`, `bridge.rs:142-151`).

Items 1–4 are Phase A work and can start now with no Comb dependency; 5 is a semantic
change worth making before cloud mode; 6–7 follow.

## 7. Recommended sequence

```
Pheromone A1  TrailLog + SqliteLog + group commit + prepare_cached        (unblocks 10× locally)
Pheromone A2  split State; followers paginate; deterministic deliveryId
Comb     C1  bounded manifest + automatic fenced compaction + segment merge
Comb     C2  op-ID idempotency on append; fix C4; L2 drill
Comb     C3  streaming read(Pos, max) with range reads; follow survives Trimmed/NotFound/transient
Comb     C4  conditional GET on manifest; change-hint hook; renewal loop; instance nonce in writer id;
             Fenced on non-leader append; S3 409/503 retry with backoff
Comb     C5  TrailLog/Pos + partitions; control manifest
Pheromone C6  ObjectLog adapter; cloud mode e2e on MinIO + S3; drills L1–L6, L9; sustained-load bench
             with manifest size vs. time, 1/10/100-producer p50/p99 by backend, N-follower GET load
```

Exit test for "Comb is the substrate": one trail on the sovereign MinIO box, 3 partitions,
50k ev/s sustained for 1 h from 8 fronts with a leader kill every 10 min, 100 followers,
one replay from seq 1 at flat memory, zero acknowledged-event loss and zero duplicates
under retry. Nothing in this document suggests that is far away; the kernel is sound and
each gap above is bounded work.

## Appendix — details behind the numbers

- Pheromone measurement harness: raw unix-socket JSON lines (`{"op":"emit",...}`), strict
  vs. 256-line pipelined, 1–8 processes; lag from `status.nextSeq - matchedThrough` every
  100 ms. Subscriptions were `on bench.** where payload.k == "nomatch" && payload.n > i`
  (tier-1 hit, tier-2 reject) and `payload.k == "k7"` (1 % match, `then emit`).
- Comb measurement harness: `combctl init --backend local`, `combctl log bench` with
  1–256 producers and 0/10 ms windows (each producer submits one event and awaits, so
  batch = producers by construction), `combctl log read`/`compact`/`follow --poll-ms`.
- macOS `File::sync_all` is `F_FULLFSYNC`; the local backend does two per object
  (`local.rs:40-47,67,113`) → 6 per commit. Linux/MinIO replace this with network RTT.
- Runs are single-shot on one machine, no warm-up discarded; Pheromone shows ±50 %
  run-to-run variance at the same config, so treat its numbers as an order of magnitude.
