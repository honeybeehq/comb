//! Retry protocol drills: lost CAS replies, concurrent same-ID, expiry, stable keys.

use chrono::{Duration, TimeZone, Utc};
use comb_core::error::CoreError;
use comb_core::operation::{Material, OpIdentity};
use comb_core::{
    DigestKey, FrozenClock, IntentState, OpIntent, OperationId, OperationPolicy, RefValue,
    StableKey, COMMIT_SCHEMA, HEADER_SCHEMA, INTENT_SCHEMA, MAX_STABLE_KEY_BYTES,
};
use comb_object::failpoint::{CountingBackend, FailpointBackend};
use comb_object::memory::MemoryBackend;
use comb_object::ObjectBackend;
use combctl::log::{LogStore, MAX_APPEND_BYTES, MAX_APPEND_EVENTS};
use combctl::store::Store;
use std::sync::Arc;

fn store_on(backend: Arc<dyn ObjectBackend>) -> Store {
    Store::new(backend, "org_t", DigestKey::from_bytes([9u8; 32]), None)
}

fn evs(v: &[&str]) -> Vec<Vec<u8>> {
    v.iter().map(|s| s.as_bytes().to_vec()).collect()
}

fn sk(label: &str) -> StableKey {
    StableKey::try_from_canonical(label.as_bytes().to_vec()).unwrap()
}

fn intent_key(op: OperationId) -> String {
    let id = OpIdentity::Generic(op);
    format!(
        "comb/v2/tenants/org_t/ops/{}/{}.json",
        id.shard(),
        id.canonical()
    )
}

async fn wipe_intent(backend: &dyn ObjectBackend, op: OperationId) {
    backend.delete(&intent_key(op)).await.unwrap();
}

#[tokio::test]
async fn l2_lost_manifest_cas_reply_retries_original_range() {
    let mem: Arc<MemoryBackend> = Arc::new(MemoryBackend::new());
    let healthy = store_on(mem.clone());
    let log = LogStore::new(&healthy, "demo");
    let op = healthy.mint_operation();
    let payload = evs(&["alpha", "beta"]);

    let faulty = Arc::new(FailpointBackend::drop_next_put_update_response(
        mem.clone(),
        "/refs/",
    ));
    let unlucky = store_on(faulty);
    let flog = LogStore::new(&unlucky, "demo");
    let err = flog.append(op, "w", &payload, 60).await.unwrap_err();
    assert!(format!("{err:#}").contains("injected"));

    let again = log.append(op, "w", &payload, 60).await.unwrap();
    assert_eq!((again.first, again.last), (1, 2));
    assert!(!again.first_delivery);
    let frames = log.read(1).await.unwrap();
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].payload, b"alpha");
    assert_eq!(frames[1].payload, b"beta");
}

#[tokio::test]
async fn intervening_writes_resolve_via_seek_with_counted_reads() {
    let mem: Arc<MemoryBackend> = Arc::new(MemoryBackend::new());
    let counting = Arc::new(CountingBackend::new(mem.clone()));
    let store = store_on(counting.clone());
    let (digest, _) = store.put_blob(b"t".to_vec()).await.unwrap();
    let op = store.mint_operation();

    let faulty = Arc::new(FailpointBackend::drop_next_put_update_response(
        mem.clone(),
        "/refs/",
    ));
    let unlucky = store_on(faulty);
    unlucky
        .set_target_op(op, "r", digest.clone(), None)
        .await
        .unwrap_err();

    let n = 64u64;
    for _ in 0..n {
        store
            .set_target_op(store.mint_operation(), "r", digest.clone(), None)
            .await
            .unwrap();
    }
    let before = counting.get_count();
    let again = store.set_target_op(op, "r", digest, None).await.unwrap();
    assert_eq!(again.generation, 1);
    let reads = counting.get_count() - before;
    // Linear scan of 64 commits would be >= 64 object reads. Seek must be cheaper.
    assert!(
        reads < n / 2,
        "seek used {reads} gets after {n} intervening writes; expected well below linear"
    );
}

#[tokio::test]
async fn hundred_concurrent_same_id_one_commit() {
    let store = store_on(Arc::new(MemoryBackend::new()));
    let (digest, _) = store.put_blob(b"x".to_vec()).await.unwrap();
    let op = store.mint_operation();
    let mut tasks = Vec::new();
    for _ in 0..100 {
        let store = store.clone();
        let digest = digest.clone();
        tasks.push(tokio::spawn(async move {
            store.set_target_op(op, "r", digest, None).await
        }));
    }
    let mut gens = Vec::new();
    for t in tasks {
        gens.push(t.await.unwrap().unwrap().generation);
    }
    assert!(gens.iter().all(|g| *g == 1), "{gens:?}");
    let (value, _) = store.read_ref("r").await.unwrap().unwrap();
    assert_eq!(value.generation, 1);
}

#[tokio::test]
async fn cross_kind_and_resource_conflict() {
    let store = store_on(Arc::new(MemoryBackend::new()));
    let (digest, _) = store.put_blob(b"x".to_vec()).await.unwrap();
    let op = store.mint_operation();
    store
        .set_target_op(op, "a", digest.clone(), None)
        .await
        .unwrap();
    let err = store
        .set_target_op(op, "b", digest, None)
        .await
        .unwrap_err();
    assert!(matches!(
        err.downcast_ref::<CoreError>(),
        Some(CoreError::IdempotencyConflict { .. })
    ));
    let log = LogStore::new(&store, "demo");
    let err = log.append(op, "w", &evs(&["z"]), 60).await.unwrap_err();
    assert!(matches!(
        err.downcast_ref::<CoreError>(),
        Some(CoreError::IdempotencyConflict { .. })
    ));
}

#[tokio::test]
async fn takeover_fences_then_completed_append_still_resolves() {
    let store = store_on(Arc::new(MemoryBackend::new()));
    let log = LogStore::new(&store, "demo");
    let op = store.mint_operation();
    let first = log.append(op, "a", &evs(&["one"]), 300).await.unwrap();
    log.steal(store.mint_operation(), "b", 300).await.unwrap();
    let again = log.append(op, "a", &evs(&["one"]), 300).await.unwrap();
    assert_eq!(again.first, first.first);
    let err = log
        .append(store.mint_operation(), "a", &evs(&["two"]), 300)
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("lease held"));
}

#[tokio::test]
async fn renewal_preserves_generation_and_commit() {
    let store = store_on(Arc::new(MemoryBackend::new()));
    let claimed = store
        .claim(store.mint_operation(), "r", "w", 60, false)
        .await
        .unwrap();
    let commit = claimed.value.head_commit.clone();
    let gen = claimed.generation;
    let renewed = store.renew("r", claimed.epoch, 120).await.unwrap();
    assert_eq!(renewed.generation, gen);
    assert_eq!(renewed.head_commit, commit);
}

#[tokio::test]
async fn missing_history_fails_closed() {
    let mem: Arc<MemoryBackend> = Arc::new(MemoryBackend::new());
    let store = store_on(mem.clone());
    let (digest, _) = store.put_blob(b"x".to_vec()).await.unwrap();
    let op = store.mint_operation();
    let committed = store
        .set_target_op(op, "r", digest.clone(), None)
        .await
        .unwrap();
    let key = format!(
        "comb/v2/tenants/org_t/objects/b3k/{}/{}",
        committed.commit.key_prefix(),
        committed.commit.hex()
    );
    mem.delete(&key).await.unwrap();
    wipe_intent(mem.as_ref(), op).await;
    let err = store
        .set_target_op(op, "r", digest, None)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<CoreError>(),
            Some(CoreError::RecoveryFailed(_))
        ) || format!("{err:#}").contains("recovery")
            || format!("{err:#}").contains("unreadable"),
        "{err:#}"
    );
    let (value, _) = store.read_ref("r").await.unwrap().unwrap();
    assert_eq!(value.generation, 1);
}

#[tokio::test]
async fn lost_intent_finalization_still_resolves() {
    let store = store_on(Arc::new(MemoryBackend::new()));
    let (digest, _) = store.put_blob(b"x".to_vec()).await.unwrap();
    let op = store.mint_operation();
    let first = store
        .set_target_op(op, "r", digest.clone(), None)
        .await
        .unwrap();
    wipe_intent(store.backend.as_ref(), op).await;
    let again = store.set_target_op(op, "r", digest, None).await.unwrap();
    assert_eq!(again.generation, first.generation);
}

#[tokio::test]
async fn expiry_cleanup_and_future_clock() {
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let clock = FrozenClock::new(now);
    let mem: Arc<MemoryBackend> = Arc::new(MemoryBackend::new());
    let store = store_on(mem.clone()).with_clock(Arc::new(clock.clone()));
    let (digest, _) = store.put_blob(b"x".to_vec()).await.unwrap();
    let op = OperationId::mint(&clock);
    store
        .set_target_op(op, "r", digest.clone(), None)
        .await
        .unwrap();
    clock.add(Duration::days(8));
    let err = store
        .set_target_op(op, "r", digest.clone(), None)
        .await
        .unwrap_err();
    assert!(matches!(
        err.downcast_ref::<CoreError>(),
        Some(CoreError::UnknownOperation { .. })
    ));
    wipe_intent(mem.as_ref(), op).await;
    let err = store
        .set_target_op(op, "r", digest.clone(), None)
        .await
        .unwrap_err();
    assert!(matches!(
        err.downcast_ref::<CoreError>(),
        Some(CoreError::UnknownOperation { .. })
    ));
    let future = OperationId::from_millis_and_entropy(
        (now + Duration::days(8) + Duration::hours(1)).timestamp_millis() as u64,
        [3u8; 18],
    );
    let err = store
        .set_target_op(future, "r", digest, None)
        .await
        .unwrap_err();
    assert!(matches!(
        err.downcast_ref::<CoreError>(),
        Some(CoreError::InvalidFormat(_))
    ));
}

#[tokio::test]
async fn stable_key_survives_eight_days_and_fresh_store() {
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let clock = FrozenClock::new(now);
    let mem: Arc<MemoryBackend> = Arc::new(MemoryBackend::new());
    let store = store_on(mem.clone()).with_clock(Arc::new(clock.clone()));
    let log = LogStore::complete_feed(&store, "feed");
    let key = sk("doc-a");
    let rec = log
        .append_stable(key.clone(), "w", b"bytes-one", 60)
        .await
        .unwrap();
    assert_eq!((rec.range.first, rec.range.last), (1, 1));

    clock.add(Duration::days(8));
    let store2 = store_on(mem).with_clock(Arc::new(clock));
    let log2 = LogStore::complete_feed(&store2, "feed");
    let again = log2
        .append_stable(key.clone(), "w", b"bytes-one", 60)
        .await
        .unwrap();
    assert_eq!(again.range.first, rec.range.first);
    assert_eq!(again.generation, rec.generation);
    let frames = log2.read(1).await.unwrap();
    assert_eq!(frames[0].payload, b"bytes-one");

    let err = log2
        .append_stable(key, "w", b"bytes-TWO", 60)
        .await
        .unwrap_err();
    assert!(matches!(
        err.downcast_ref::<CoreError>(),
        Some(CoreError::StableKeyConflict { .. })
    ));
}

#[tokio::test]
async fn complete_feed_rejects_trim() {
    let store = store_on(Arc::new(MemoryBackend::new()));
    let log = LogStore::complete_feed(&store, "feed");
    log.append(store.mint_operation(), "w", &evs(&["a"]), 60)
        .await
        .unwrap();
    let err = log
        .trim_before(store.mint_operation(), 1)
        .await
        .unwrap_err();
    assert!(matches!(
        err.downcast_ref::<CoreError>(),
        Some(CoreError::Rejected(_))
    ));
}

#[tokio::test]
async fn empty_and_oversized_append_rejected() {
    let store = store_on(Arc::new(MemoryBackend::new()));
    let log = LogStore::new(&store, "demo");
    let err = log
        .append(store.mint_operation(), "w", &[], 60)
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("empty"));
    let too_many = vec![b"x".to_vec(); MAX_APPEND_EVENTS + 1];
    let err = log
        .append(store.mint_operation(), "w", &too_many, 60)
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("exceeds"));
    let huge = vec![vec![0u8; MAX_APPEND_BYTES + 1]];
    let err = log
        .append(store.mint_operation(), "w", &huge, 60)
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("bytes"));
    let err = StableKey::try_from_canonical(vec![1u8; MAX_STABLE_KEY_BYTES + 1]).unwrap_err();
    assert!(matches!(err, CoreError::Rejected(_)));
}

#[tokio::test]
async fn grouped_duplicate_conflict_and_lost_ack() {
    let mem: Arc<MemoryBackend> = Arc::new(MemoryBackend::new());
    let store = store_on(mem.clone());
    let (writer, task) = combctl::log::GroupWriter::spawn(store.clone(), "g", "w".into(), 30, 100);
    let op = store.mint_operation();
    let a = writer.submit(op, evs(&["same"])).await.unwrap();
    let b = writer.submit(op, evs(&["same"])).await.unwrap();
    assert_eq!((a.first, a.last), (b.first, b.last));
    let err = writer.submit(op, evs(&["other"])).await.unwrap_err();
    assert!(format!("{err:#}").contains("idempotency") || format!("{err:#}").contains("conflict"));
    drop(writer);
    let _ = task.await;

    let faulty = Arc::new(FailpointBackend::drop_next_put_update_response(
        mem.clone(),
        "/refs/",
    ));
    let unlucky = store_on(faulty);
    let (w2, t2) = combctl::log::GroupWriter::spawn(unlucky.clone(), "g2", "w".into(), 5, 10);
    let op2 = unlucky.mint_operation();
    let res = w2.submit(op2, evs(&["z"])).await;
    assert!(res.is_err() || res.as_ref().map(|r| r.first == 1).unwrap_or(false));
    drop(w2);
    let _ = t2.await;
    let healthy = store_on(mem);
    let log = LogStore::new(&healthy, "g2");
    let again = log.append(op2, "w", &evs(&["z"]), 60).await.unwrap();
    assert_eq!(again.first, 1);
}

#[tokio::test]
async fn v1_ref_is_a_hard_error() {
    let mem: Arc<MemoryBackend> = Arc::new(MemoryBackend::new());
    mem.put_update("comb/v1/tenants/org_t/refs/old.json", None, b"{}")
        .await
        .unwrap();
    let store = store_on(mem);
    let (digest, _) = store.put_blob(b"x".to_vec()).await.unwrap();
    let err = store
        .set_target_op(store.mint_operation(), "old", digest, None)
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("comb/v1"));
}

#[tokio::test]
async fn applied_retry_keeps_original_generation_and_epoch() {
    let store = store_on(Arc::new(MemoryBackend::new()));
    let (digest, _) = store.put_blob(b"x".to_vec()).await.unwrap();
    let op = store.mint_operation();
    let first = store
        .set_target_op(op, "r", digest.clone(), None)
        .await
        .unwrap();
    assert_eq!(first.generation, 1);
    assert_eq!(first.epoch, 0);
    store
        .claim(store.mint_operation(), "r", "w", 60, true)
        .await
        .unwrap();
    let again = store.set_target_op(op, "r", digest, None).await.unwrap();
    assert_eq!(again.generation, first.generation);
    assert_eq!(again.epoch, first.epoch);
    assert_eq!(again.commit, first.commit);
    assert_eq!(again.value, first.value);
    assert!(!again.first_delivery);
}

#[tokio::test]
async fn grouped_companion_not_acked_from_stale_leader_range() {
    let mem: Arc<MemoryBackend> = Arc::new(MemoryBackend::new());
    let faulty = Arc::new(FailpointBackend::drop_next_put_update_response(
        mem.clone(),
        "/refs/",
    ));
    let unlucky = store_on(faulty);
    let (writer, task) =
        combctl::log::GroupWriter::spawn(unlucky.clone(), "race", "w".into(), 250, 100);
    let writer = std::sync::Arc::new(writer);
    let op_x = unlucky.mint_operation();
    let op_y = unlucky.mint_operation();
    let wx = writer.clone();
    let wy = writer.clone();
    let hx = tokio::spawn(async move { wx.submit(op_x, evs(&["x1", "x2"])).await });
    let hy = tokio::spawn(async move { wy.submit(op_y, evs(&["y1", "y2", "y3"])).await });
    let rx = hx.await.unwrap();
    let ry = hy.await.unwrap();
    assert!(
        rx.is_err() && ry.is_err(),
        "lost CAS reply should error both acks"
    );
    drop(writer);
    let _ = task.await;

    let healthy = store_on(mem.clone());
    let (w2, t2) = combctl::log::GroupWriter::spawn(healthy.clone(), "race", "w".into(), 250, 100);
    let w2 = std::sync::Arc::new(w2);
    let op_z = healthy.mint_operation();
    let wy = w2.clone();
    let wz = w2.clone();
    let hy = tokio::spawn(async move { wy.submit(op_y, evs(&["y1", "y2", "y3"])).await });
    let hz = tokio::spawn(async move { wz.submit(op_z, evs(&["z"])).await });
    let y_ack = hy.await.unwrap();
    let z_ack = hz.await.unwrap();
    drop(w2);
    let _ = t2.await;

    let log = LogStore::new(&healthy, "race");
    let frames = log.read(1).await.unwrap();
    let y_count = frames.iter().filter(|f| f.payload == b"y1").count();
    assert_eq!(y_count, 1, "Y must appear once, got {y_count}");
    if let Ok(z) = &z_ack {
        let got = frames.iter().find(|f| f.seq == z.first);
        assert!(
            got.is_some_and(|f| f.payload == b"z"),
            "Z ack {z:?} must name a written payload, frames {:?}",
            frames
                .iter()
                .map(|f| (f.seq, String::from_utf8_lossy(&f.payload).into_owned()))
                .collect::<Vec<_>>()
        );
    }
    if let Ok(y) = &y_ack {
        assert_eq!((y.first, y.last), (3, 5));
    }
}

#[tokio::test]
async fn complete_feed_trim_rejected_from_fresh_handle() {
    let store = store_on(Arc::new(MemoryBackend::new()));
    let log = LogStore::complete_feed(&store, "feed");
    log.append(store.mint_operation(), "w", &evs(&["a"]), 60)
        .await
        .unwrap();
    let other = LogStore::new(&store, "feed");
    let err = other
        .trim_before(store.mint_operation(), 1)
        .await
        .unwrap_err();
    assert!(matches!(
        err.downcast_ref::<CoreError>(),
        Some(CoreError::Rejected(_))
    ));
}

#[tokio::test]
async fn core_cannot_overwrite_a_log_ref() {
    let store = store_on(Arc::new(MemoryBackend::new()));
    let log = LogStore::new(&store, "demo");
    log.append(store.mint_operation(), "w", &evs(&["a"]), 60)
        .await
        .unwrap();
    let (digest, _) = store.put_blob(b"x".to_vec()).await.unwrap();
    let err = store
        .set_target_op(store.mint_operation(), "log/demo/p0", digest, None)
        .await
        .unwrap_err();
    assert!(matches!(
        err.downcast_ref::<CoreError>(),
        Some(CoreError::Rejected(_))
    ));
}

#[tokio::test]
async fn empty_stable_payload_rejected() {
    let store = store_on(Arc::new(MemoryBackend::new()));
    let log = LogStore::complete_feed(&store, "feed");
    let err = log.append_stable(sk("k"), "w", b"", 60).await.unwrap_err();
    assert!(format!("{err:#}").contains("empty"));
}

#[tokio::test]
async fn corrupt_stable_node_fails_closed() {
    let mem: Arc<MemoryBackend> = Arc::new(MemoryBackend::new());
    let store = store_on(mem.clone());
    let log = LogStore::complete_feed(&store, "feed");
    let rec = log.append_stable(sk("k"), "w", b"one", 60).await.unwrap();
    let (_, manifest) = log.status().await.unwrap().unwrap();
    let root = manifest.stable_index.unwrap();
    let obj = format!(
        "comb/v2/tenants/org_t/objects/b3k/{}/{}",
        root.digest.key_prefix(),
        root.digest.hex()
    );
    mem.delete(&obj).await.unwrap();
    let err = log
        .append_stable(sk("k"), "w", b"one", 60)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<CoreError>(),
            Some(CoreError::IntegrityError(_))
        ),
        "{err:#}"
    );
    assert_eq!(rec.range.first, 1);
}

#[tokio::test]
async fn racing_groups_admit_shared_producer_once() {
    let store = store_on(Arc::new(MemoryBackend::new()));
    let (w1, t1) = combctl::log::GroupWriter::spawn(store.clone(), "race2", "w".into(), 40, 100);
    let (w2, t2) = combctl::log::GroupWriter::spawn(store.clone(), "race2", "w".into(), 40, 100);
    let w1 = std::sync::Arc::new(w1);
    let w2 = std::sync::Arc::new(w2);
    let op_y = store.mint_operation();
    let op_a = store.mint_operation();
    let op_b = store.mint_operation();
    let h1 = {
        let w = w1.clone();
        tokio::spawn(async move {
            let _ = w.submit(op_a, evs(&["a"])).await;
            w.submit(op_y, evs(&["y-once"])).await
        })
    };
    let h2 = {
        let w = w2.clone();
        tokio::spawn(async move {
            let y = w.submit(op_y, evs(&["y-once"])).await;
            let _ = w.submit(op_b, evs(&["b"])).await;
            y
        })
    };
    let r1 = h1.await.unwrap();
    let r2 = h2.await.unwrap();
    drop(w1);
    drop(w2);
    let _ = t1.await;
    let _ = t2.await;

    let log = LogStore::new(&store, "race2");
    let frames = log.read(1).await.unwrap();
    let y_count = frames.iter().filter(|f| f.payload == b"y-once").count();
    assert_eq!(y_count, 1, "Y must appear once, frames={frames:?}");
    let y_ok = r1.ok().or(r2.ok());
    if let Some(y) = y_ok {
        let got = frames.iter().find(|f| f.seq == y.first);
        assert!(
            got.is_some_and(|f| f.payload == b"y-once"),
            "Y receipt {y:?} must name the written payload"
        );
    }
}

#[tokio::test]
async fn independent_clocks_and_policies_on_shared_backend() {
    let t0 = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
    let t1 = Utc.with_ymd_and_hms(2021, 6, 1, 12, 0, 0).unwrap();
    let mem: Arc<MemoryBackend> = Arc::new(MemoryBackend::new());
    let clock_a = FrozenClock::new(t0);
    let clock_b = FrozenClock::new(t1);
    let short = OperationPolicy {
        window: Duration::days(1),
        first_use: Duration::minutes(5),
        max_future_skew: Duration::minutes(2),
    };
    let a = store_on(mem.clone())
        .with_clock(Arc::new(clock_a.clone()))
        .with_policy(short);
    let b = store_on(mem).with_clock(Arc::new(clock_b.clone()));
    let oa = a.mint_operation();
    let ob = b.mint_operation();
    assert_eq!(oa.issued_at(), t0);
    assert_eq!(ob.issued_at(), t1);

    let (digest, _) = a.put_blob(b"x".to_vec()).await.unwrap();
    a.set_target_op(oa, "ra", digest.clone(), None)
        .await
        .unwrap();
    b.set_target_op(ob, "rb", digest.clone(), None)
        .await
        .unwrap();

    clock_a.add(Duration::days(2));
    clock_b.add(Duration::days(2));
    let err = a
        .set_target_op(oa, "ra", digest.clone(), None)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<CoreError>(),
            Some(CoreError::UnknownOperation { .. })
        ),
        "{err:#}"
    );
    let again = b.set_target_op(ob, "rb", digest, None).await.unwrap();
    assert_eq!(again.generation, 1);
    assert!(!again.first_delivery);
}

#[tokio::test]
async fn original_claim_set_target_and_release_survive_later_mutations() {
    let now = Utc.with_ymd_and_hms(2026, 3, 1, 0, 0, 0).unwrap();
    let clock = FrozenClock::new(now);
    let store = store_on(Arc::new(MemoryBackend::new())).with_clock(Arc::new(clock));
    let (d1, _) = store.put_blob(b"one".to_vec()).await.unwrap();
    let (d2, _) = store.put_blob(b"two".to_vec()).await.unwrap();

    let set_op = store.mint_operation();
    let set = store
        .set_target_op(set_op, "r", d1.clone(), None)
        .await
        .unwrap();
    assert_eq!(set.value.target.as_ref(), Some(&d1));
    assert!(set.value.lease.is_none());

    let claim_op = store.mint_operation();
    let claimed = store.claim(claim_op, "r", "w", 60, false).await.unwrap();
    assert!(
        claimed.value.lease.is_some(),
        "fresh claim must carry a lease"
    );
    assert_eq!(
        claimed.value.target.as_ref(),
        Some(&d1),
        "fresh claim must retain the existing target"
    );

    let release_op = store.mint_operation();
    let released = store.release(release_op, "r", claimed.epoch).await.unwrap();
    assert!(released.value.lease.is_none());
    assert_eq!(released.value.target.as_ref(), Some(&d1));

    let thief = store
        .claim(store.mint_operation(), "r", "thief", 90, true)
        .await
        .unwrap();
    store
        .set_target_op(store.mint_operation(), "r", d2, Some(thief.epoch))
        .await
        .unwrap();

    let set_again = store
        .set_target_op(set_op, "r", d1.clone(), None)
        .await
        .unwrap();
    assert!(!set_again.first_delivery);
    assert_eq!(set_again.generation, set.generation);
    assert_eq!(set_again.epoch, set.epoch);
    assert_eq!(set_again.commit, set.commit);
    assert_eq!(set_again.value, set.value);

    let claim_again = store.claim(claim_op, "r", "w", 60, false).await.unwrap();
    assert!(!claim_again.first_delivery);
    assert_eq!(claim_again.generation, claimed.generation);
    assert_eq!(claim_again.epoch, claimed.epoch);
    assert_eq!(claim_again.commit, claimed.commit);
    assert_eq!(claim_again.value, claimed.value);
    assert_eq!(claim_again.value.lease, claimed.value.lease);
    assert_eq!(claim_again.value.target.as_ref(), Some(&d1));

    let release_again = store.release(release_op, "r", claimed.epoch).await.unwrap();
    assert!(!release_again.first_delivery);
    assert_eq!(release_again.generation, released.generation);
    assert_eq!(release_again.commit, released.commit);
    assert_eq!(release_again.value, released.value);
    assert!(release_again.value.lease.is_none());
    assert_eq!(release_again.value.target.as_ref(), Some(&d1));
}

#[tokio::test]
async fn forged_applied_cache_is_not_publication() {
    let mem: Arc<MemoryBackend> = Arc::new(MemoryBackend::new());
    let store = store_on(mem.clone());
    let (digest, _) = store.put_blob(b"x".to_vec()).await.unwrap();
    let op = store.mint_operation();
    let request = Material {
        kind: "set-target".into(),
        preconditions: vec![("target".into(), digest.to_string().into_bytes())],
        payload: Vec::new(),
    }
    .hash(&store.key, &store.tenant, "r");
    let mut ref_state = RefValue::new("org_t", "r");
    ref_state.generation = 1;
    ref_state.target = Some(digest.clone());
    let fake_commit = comb_core::Commit {
        schema: COMMIT_SCHEMA.into(),
        header: comb_core::CommitHeader {
            schema: HEADER_SCHEMA.into(),
            resource: "r".into(),
            generation: 1,
            epoch: 0,
            identity: OpIdentity::Generic(op).canonical(),
            request: request.clone(),
            parent: None,
            skip: None,
            at: Utc::now(),
        },
        change: serde_json::json!({ "kind": "set-target", "target": digest }),
        result: serde_json::json!({ "generation": 1, "epoch": 0 }),
        ref_state,
    };
    let (fake, _) = store
        .put_blob(serde_json::to_vec(&fake_commit).unwrap())
        .await
        .unwrap();
    let intent = OpIntent {
        schema: INTENT_SCHEMA.into(),
        identity: OpIdentity::Generic(op).canonical(),
        resource: "r".into(),
        request,
        base_generation: 0,
        proposed: Vec::new(),
        state: IntentState::Applied {
            generation: 1,
            commit: fake.clone(),
            result: serde_json::json!({ "generation": 1, "epoch": 0 }),
        },
        expires_at: Some(op.expires_at(&OperationPolicy::default())),
    };
    mem.put_update(
        &intent_key(op),
        None,
        &serde_json::to_vec_pretty(&intent).unwrap(),
    )
    .await
    .unwrap();

    let published = store.set_target_op(op, "r", digest, None).await.unwrap();
    assert_ne!(published.commit, fake, "forged digest must not be returned");
    assert_eq!(published.generation, 1);
    let (head, _) = store.read_ref("r").await.unwrap().unwrap();
    assert_eq!(head.head_commit.as_ref(), Some(&published.commit));
    assert_eq!(head.generation, 1);
}

#[tokio::test]
async fn applied_cache_candidate_is_not_canonical_publication() {
    let mem: Arc<MemoryBackend> = Arc::new(MemoryBackend::new());
    let store = store_on(mem.clone());
    let (digest, _) = store.put_blob(b"x".to_vec()).await.unwrap();
    let op = store.mint_operation();
    let first = store
        .set_target_op(op, "r", digest.clone(), None)
        .await
        .unwrap();

    let request = Material {
        kind: "set-target".into(),
        preconditions: vec![("target".into(), digest.to_string().into_bytes())],
        payload: Vec::new(),
    }
    .hash(&store.key, &store.tenant, "r");
    let mut ref_state = first.value.clone();
    ref_state.head_commit = None;
    ref_state.lease = None;
    let candidate = comb_core::Commit {
        schema: COMMIT_SCHEMA.into(),
        header: comb_core::CommitHeader {
            schema: HEADER_SCHEMA.into(),
            resource: "r".into(),
            generation: first.generation,
            epoch: first.epoch,
            identity: OpIdentity::Generic(op).canonical(),
            request: request.clone(),
            parent: None,
            skip: None,
            at: Utc::now(),
        },
        change: serde_json::json!({ "kind": "set-target", "target": digest }),
        result: serde_json::json!({ "generation": first.generation, "epoch": first.epoch }),
        ref_state,
    };
    let (fake, _) = store
        .put_blob(serde_json::to_vec(&candidate).unwrap())
        .await
        .unwrap();
    assert_ne!(fake, first.commit);

    let intent = OpIntent {
        schema: INTENT_SCHEMA.into(),
        identity: OpIdentity::Generic(op).canonical(),
        resource: "r".into(),
        request,
        base_generation: 0,
        proposed: Vec::new(),
        state: IntentState::Applied {
            generation: first.generation,
            commit: fake.clone(),
            result: serde_json::json!({ "generation": first.generation, "epoch": first.epoch }),
        },
        expires_at: Some(op.expires_at(&OperationPolicy::default())),
    };
    mem.delete(&intent_key(op)).await.unwrap();
    mem.put_update(
        &intent_key(op),
        None,
        &serde_json::to_vec_pretty(&intent).unwrap(),
    )
    .await
    .unwrap();

    store
        .claim(store.mint_operation(), "r", "thief", 60, true)
        .await
        .unwrap();

    let again = store.set_target_op(op, "r", digest, None).await.unwrap();
    assert_eq!(again.commit, first.commit);
    assert_ne!(again.commit, fake);
    assert_eq!(again.value, first.value);
    assert_eq!(again.generation, first.generation);
}

#[tokio::test]
async fn missing_log_manifest_is_not_permission_to_overwrite() {
    let mem: Arc<MemoryBackend> = Arc::new(MemoryBackend::new());
    let store = store_on(mem.clone());
    let log = LogStore::new(&store, "demo");
    log.append(store.mint_operation(), "w", &evs(&["a"]), 60)
        .await
        .unwrap();
    let (head, _) = store.read_ref("log/demo/p0").await.unwrap().unwrap();
    let target = head.target.expect("log target");
    let obj = format!(
        "comb/v2/tenants/org_t/objects/b3k/{}/{}",
        target.key_prefix(),
        target.hex()
    );
    mem.delete(&obj).await.unwrap();
    let (digest, _) = store.put_blob(b"x".to_vec()).await.unwrap();
    let err = store
        .set_target_op(store.mint_operation(), "log/demo/p0", digest, None)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<CoreError>(),
            Some(CoreError::Rejected(_) | CoreError::RecoveryFailed(_))
        ),
        "{err:#}"
    );
    let (still, _) = store.read_ref("log/demo/p0").await.unwrap().unwrap();
    assert_eq!(still.generation, head.generation);
    assert_eq!(still.target.as_ref(), Some(&target));
}
