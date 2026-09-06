# Comb / Pheromone log integration map

**Question.** What is the smallest TrailLog cut that keeps local SQLite behavior and later hosts async Comb ObjectLog behind today's sync pherd.

**Decision this feeds.** Phase A extract in `pher-comb-log` (SqliteLog now, ObjectLog later). Do not take a Comb crate dependency until Comb grows `read(from, max)` and a library crate.

**Throughput checkpoint:** n/a, read-only investigation.

Sources: `docs/{DIRECTION,LANGUAGE,ARCHITECTURE,ROADMAP,SCALE,ECOSYSTEM_AUDIT}.md`; `crates/pher/src/{protocol,daemon,db,store,http,main}.rs`; Comb `comb-specification-v0_3.md` §8.2; `comb-reliable-log/crates/combctl/src/log.rs`; analysis `docs/analysis/comb-as-pheromone-scale-substrate-2026-08-25.md`; reality-check `2026-09-06.md`.

## Known vs inferred

**Known (code).** No `TrailLog` type exists. Ingest/follower split is done. Events live in SQLite `events` with caller-chosen `seq`. Comb Log is async `LogStore` inside `combctl`, payloads are `String`, `read` is unbounded, `trim_before` is by seq, no `seek_ts`.

**Inferred.** Spec `follow() -> Stream` is the wrong first cut for a `Mutex<State>` daemon. Pull `read` plus the existing nudge thread is enough. ObjectLog should wait on Comb C3.

## Overview

pherd is a sync unix-socket daemon. Emit assigns `PH.*` id and `seq`, `INSERT`s, acks `{id,seq}`, then a follower matches. Tails see the event at ingest, not after match. Control state (subs, named cursors, `matched_through`) stays Pheromone-owned. Comb owns durable event bytes in the cloud. Local mode must keep µs acks, 7d ts retention, and today's cursor/listen numbers.

## Key concepts

- **Envelope** (`pher-core`). Identity is `id`. `seq` is log metadata, not a field.
- **TrailLog** (spec only). `Pos { partition, seq }`, append/head/read/seek_ts/follow/trim_before.
- **matched_through**. Matcher cursor in `meta`, not the log.
- **deliveryId**. `{sub}:{event.id}:{n}` with per-sub counter `n`.
- **Admission**. `forwarded_seen` keys on **event id**, not deliveryId (comment at `ingest_forwarded` is wrong).

## How it works (protocol → db)

```
Emit / /emit / tap
  protocol::Request::Emit (protocol.rs:9)
  handle_rpc (daemon.rs:2112) → State::ingest (684)
    id = ids::short_id("PH")  (693)
    ingest_envelope (713): seq = next_seq; Db::append_event (db.rs:155);
      next_seq += 1; notify_tails (1708); follower_tx nudge
    ack { id, seq }  (707). No deliveries on ack.
Follower thread (daemon.rs:1871)
  follow_step(32) (730): Db::events_next(matched_through, max) (db.rs:214)
  process_event → deliver (1368): deliveryId, Db::append_delivery (277)
  set_matched_through (db.rs:242)
Tail (protocol.rs:25; daemon.rs:2089, 2499; http.rs:126)
  attach_tail under lock: register mpsc, events_after backlog, then live notify_tails
  HTTP /tail defaults last=100; unix tail is unbounded
Listen (protocol.rs:33; attach_listener 2580)
  after beats named cursor; else since lookback via events_since_ts
  replay into listener channel under same lock as register (no gap)
  ack seq = next_seq-1, replayed, resumedFrom, gapExpired
CursorCommit (protocol.rs:51; 1686; 2205)
  max(seq) into cursors.json. Pheromone control, not Comb.
Replay since (LANGUAGE.md:162; register 658; listen 2622)
  events_since_ts (db.rs:252). Tiers 3–4 catch-up is tiers 1–2 only (warning 2648).
Why-not (2401): event_by_id (db.rs:263), side index.
GC (1720; load 483; every 600s 2038)
  DELETE events/deliveries WHERE ts < cutoff; verdicts/vectors by unix; forwarded window 50k
  PHER_RETENTION default 7d (18, 297)
Status head: nextSeq / matchedThrough (2138)
```

Comb today (`log.rs`): `append(writer, &[String]) -> (first,last)` (89); `read(from)` unbounded (203); `trim_before(seq)` (276); `follow` poll on `head_seq` (319), dies on Trimmed (337); `GroupWriter` (354). No TrailLog, no max, no seek_ts. Layout `log/{name}/p0`.

## File:line map

| Path | Where | Lines |
|---|---|---|
| Protocol emit/tail/listen/cursor | `crates/pher/src/protocol.rs` | 9–57, 107–118 |
| Seq assign, append, tails, follower | `daemon.rs` | 111–116, 323–330, 684–753, 1700–1732, 1871–1882 |
| Listen/tail attach | `daemon.rs` | 2499–2698 |
| deliveryId / sinks | `daemon.rs` | 1368–1401 |
| Admission by event id | `daemon.rs` | 1022–1058 |
| RPC status/why/cursors | `daemon.rs` | 2110–2224 |
| Follower tests | `daemon.rs` | 2816–2936 |
| Events/meta/GC SQL | `crates/pher/src/db.rs` | 34–65, 155–273, 377–396, 471–493 |
| HTTP /tail /listen | `http.rs` | 126–138, 233–286 |
| Cursor commit on print | `main.rs` | 581–616 |
| Envelope / ids | `pher-core/src/envelope.rs`, `ids.rs` | 9–26 / 8–19 |
| Spec trait | Comb spec §8.2 | 991–1006 |
| SCALE remaining A | `docs/SCALE.md` | 43–45, 62–74 |
| Comb LogStore | `combctl/src/log.rs` | 63–345 |
| Comb not ready | reality-check 2026-09-06 | claims 5–7 |

## Hazards

1. **Caller-chosen seq.** `State.next_seq` then INSERT. Comb assigns seq. `next_seq` must leave State. Acks and listen `ack.seq` become `log.head()`.
2. **Inclusive vs exclusive.** Pheromone reads `seq > after`. Comb `read` is `seq >= from`. Map `from = after+1`.
3. **`follow() -> Stream` vs daemon.** Follower is pull `events_next` under `Mutex<State>`. A Stream trait object does not fit. Keep pull `read`. Drive live with today's nudge (SqliteLog) or a Comb poll thread that only nudges.
4. **Do not hold State across Comb I/O.** Local INSERT is µs under the lock. Object append is 10ms+ group commit. Mint envelope under lock, `log.append` outside, re-lock for tails/nudge.
5. **rusqlite `Connection` is `!Sync`.** `TrailLog: Send + Sync` needs `Mutex<Connection>` or WAL second connection. Two writers to `pher.db` need one writer mutex.
6. **Unbounded catch-up.** `events_after(..., None)` and unix tail load the whole log. Comb `read` does the same (C3 missing). Paginate with `max` at the trait.
7. **GC vs follower.** Ts delete can drop rows with `seq > matched_through` (esp. forwarded envelopes that keep origin ts). Silent skip. Comb `Trimmed{resume_at}` is explicit. SqliteLog should bump `matched_through` or return Trimmed.
8. **trim units.** Pheromone GC is ts. Comb trim is seq. Adapter: `seek_ts` then `trim_before(seq)`. SqliteLog can DELETE by ts and return floor seq.
9. **deliveryId replay.** Follower crash remints `{sub}:{id}:{n+1}`. SCALE.md already says this. Keep format this slice. Hash(sub,id,seq) is a later semantic change.
10. **event id vs deliveryId.** Cross-node dedup is event id (`admit_remote` 1042). Preserve both.
11. **Tails are not log followers.** Live lines come from `notify_tails` at ingest. Keep that for local. Do not route unix tail through Comb follow in A1.
12. **`event_by_id` is not TrailLog.** Side index. SqliteLog writes it in the same INSERT. ObjectLog keeps a local SQLite id→seq index.
13. **Comb crate shape.** Log is `combctl` lib, not `comb-core`. Pheromone must not depend on the CLI crate. Parent should extract `comb-log`.
14. **Comb correctness gaps** (do not paper over): no append op-id (lost ack duplicates); follow stops on Trimmed/NotFound; unbounded replay RAM.
15. **Lock still covers cascade.** Follower holds State across match/ONNX/sinks (`follow_step` 730, analysis). Out of scope for A1. Do not make it worse with Comb I/O inside that lock.

## Suggested interface (concrete)

New `crates/pher/src/traillog.rs`. Stay in `pher`, not `pher-core` (storage is daemon-only).

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pos { pub partition: u32, pub seq: u64 } // local partition always 0; seq 1-based

pub enum TrailError {
    Trimmed { resume_at: Pos },
    Other(anyhow::Error),
}

/// Sync pull API. No follow/Stream in v1.
pub trait TrailLog: Send + Sync {
    /// Durable append. Returns last Pos of the batch. Log assigns seq.
    fn append(&self, batch: &[Envelope]) -> Result<Pos, TrailError>;
    fn head(&self) -> Result<Vec<Pos>, TrailError>;
    /// Inclusive `from`, bounded `max`. Empty at head. Trimmed if from <= floor.
    fn read(&self, from: Pos, max: usize) -> Result<Vec<(Pos, Envelope)>, TrailError>;
    fn seek_ts(&self, partition: u32, ts: &str) -> Result<Pos, TrailError>;
    /// Retention by RFC3339 ts. Returns new floor Pos (last deleted, or 0).
    fn trim_before(&self, ts: &str) -> Result<Pos, TrailError>;
}
```

**SqliteLog.** Wrap today's event SQL. `append` `BEGIN IMMEDIATE; MAX(seq)+1; INSERT; COMMIT`. `head` = `MAX(seq)`. `read` = `events_next` with `from.seq.saturating_sub(1)` as exclusive after, or rewrite `seq >= ? LIMIT ?`. `seek_ts` = `MIN(seq) WHERE ts >= ?` else head+1. `trim_before` = `DELETE FROM events WHERE ts < ?` returning max deleted seq. Keep `event_by_id` / deliveries / verdicts / vectors / forwarded / `matched_through` on `Db`.

**Daemon rewire (small).**

- `State.log: Arc<dyn TrailLog>`; drop `next_seq` as allocator; cache `head_seq` from last append/head.
- `ingest_envelope`: append `[event]`, `notify_tails(pos.seq)`, nudge. Local may keep append under State lock.
- `follow_step`: `log.read(Pos{0, matched_through+1}, max)`.
- `read_events` / listen / tail backlog: paginated `read`, not one giant Vec.
- `read_events_since_ts`: `seek_ts` + `read`.
- `gc`: `log.trim_before(cutoff)` then existing non-event `Db::gc`.
- Status / listen ack / CursorLs `head`: `log.head()`.

**Async Comb behind sync pherd (later, no dep now).** Dedicated std thread, tokio runtime, `LogStore` + optional `GroupWriter`. Sync `TrailLog` impl uses `std::sync::mpsc` request/oneshot (do not `block_on` on a worker that already has a runtime). Payload = `serde_json::to_string(envelope)`. `read(from,max)` truncates Comb's Vec until Comb C3. `seek_ts` scans Frame.at or waits for Comb. `trim_before(ts)` → seek then seq trim. Live: Comb `follow` thread, on frame notify daemon nudge only. Writer id = node name. Single partition `p0`.

**Preserve.** `PH.*` ids at ingest; envelope identity on admit; deliveryId string; 7d ts retention locally; listen after/cursor/`gapExpired`; emit ack `{id,seq}`; pre-split `matched_through` catch-up; tails still fire at durable append.

**Do not do in A1.** Group commit, breaking `Mutex<State>`, tails-as-log-followers, hashed deliveryId, Comb dependency, partitions, moving deliveries onto Comb.

## Tests and build (do not run full workspace)

From `pher-comb-log`. `--offline` if deps cached. `--test-threads=1` avoids tmpdir races.

```
cargo test -p pher --lib db::tests -- --test-threads=1
cargo test -p pher --lib follower_tests -- --test-threads=1
cargo test -p pher-core --lib -- --test-threads=1
```

Exact names: `events_roundtrip_ranges_and_gc`, `vectors_and_forwarded_windows`, `jsonl_migration_imports_and_renames`, `ingest_appends_only_and_follower_runs_the_cascade`, `follower_position_survives_restart_and_replays_the_gap`, `pre_split_database_starts_caught_up`.

After extract, add `traillog` tests (append assigns seq, read exclusive map, seek_ts, trim Trimmed, restart head). Keep the three follower tests green with `s.db` replaced by `s.log`.

**Do not run:** `cargo test` (workspace pulls pher-node napi / pher-wasm / embed), `cargo build --release`, `pher daemon`, anything against `~/.pheromone` or live MinIO/S3.

Comb (parent repo, memory backend only, if needed later):

```
cargo test -p combctl --test log_drills --test phase_c_drills -- --test-threads=1
```

Names: `append_read_roundtrip`, `follow_sees_new_appends`, group-commit drill in `phase_c_drills.rs`.

## Work estimate

| Slice | Effort | Notes |
|---|---|---|
| TrailLog + SqliteLog + daemon seq/head/read/gc rewire | 1.5–2.5 d | ~400–700 LOC, tests above |
| Paginate tail/listen `max` | 0.5 d | stops RAM bomb, still local |
| Blocking ObjectLog adapter | 2–3 d | **blocked** on Comb `read(max)` + extract crate + seek_ts |
| Group commit / State split / hashed deliveryId | later | throughput, not the interface |

A1 is 2–3 days. ObjectLog is a second PR after Comb C3. Local behavior stays the default `[store] kind = "sqlite"` (config not wired yet; `SCALE.md:62`).

## Next action

Implement TrailLog + SqliteLog in this repo with no Comb dependency. Parent Comb work should extract `comb-log` and add bounded `read(from, max)` plus `Trimmed` survival before Pheromone takes ObjectLog.
