//! Retry protocol drills: lost CAS replies, concurrent same-ID, expiry, stable keys.

use chrono::{Duration, TimeZone, Utc};
use comb_core::error::CoreError;
use comb_core::operation::OpIdentity;
use comb_core::{
    Commit, DigestKey, FrozenClock, IntentState, OpIntent, OperationId, StableKey,
    MAX_STABLE_KEY_BYTES,
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
    let first = flog.append(op, "w", &payload, 60).await;
    match first {
        Ok(again) => {
            assert_eq!((again.first, again.last), (1, 2));
            assert!(!again.first_delivery);
        }
        Err(err) => {
            assert!(
                matches!(
                    err.downcast_ref::<CoreError>(),
                    Some(CoreError::BackendUnavailable(_))
                ),
                "{err:#}"
            );
            let again = log.append(op, "w", &payload, 60).await.unwrap();
            assert_eq!((again.first, again.last), (1, 2));
            assert!(!again.first_delivery);
        }
    }
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
    let lost = unlucky.set_target(op, "r", digest.clone(), None).await;
    assert!(
        lost.is_err()
            || lost
                .as_ref()
                .map(|p| p.generation == 1 && !p.first_delivery)
                .unwrap_or(false),
        "{lost:?}"
    );
    let (value, _) = store.read_ref("r").await.unwrap().expect("ref committed");
    assert_eq!(value.generation, 1);

    let n = 64u64;
    for _ in 0..n {
        store
            .set_target(store.mint_operation(), "r", digest.clone(), None)
            .await
            .unwrap();
    }
    let before = counting.get_count();
    let again = store.set_target(op, "r", digest, None).await.unwrap();
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
            store.set_target(op, "r", digest, None).await
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
        .set_target(op, "a", digest.clone(), None)
        .await
        .unwrap();
    let err = store.set_target(op, "b", digest, None).await.unwrap_err();
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
        .set_target(op, "r", digest.clone(), None)
        .await
        .unwrap();
    let key = format!(
        "comb/v2/tenants/org_t/objects/b3k/{}/{}",
        committed.commit.key_prefix(),
        committed.commit.hex()
    );
    mem.delete(&key).await.unwrap();
    mem.delete(&store.intent_key(&OpIdentity::Generic(op)))
        .await
        .unwrap();
    let err = store.set_target(op, "r", digest, None).await.unwrap_err();
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
        .set_target(op, "r", digest.clone(), None)
        .await
        .unwrap();
    let key = store.intent_key(&OpIdentity::Generic(op));
    let (bytes, version) = store.backend.get(&key).await.unwrap();
    let mut intent: OpIntent = serde_json::from_slice(&bytes).unwrap();
    intent.state = IntentState::Pending;
    store
        .backend
        .put_update(
            &key,
            Some(&version),
            &serde_json::to_vec_pretty(&intent).unwrap(),
        )
        .await
        .unwrap();
    let again = store.set_target(op, "r", digest, None).await.unwrap();
    assert_eq!(again.generation, first.generation);
    assert!(!again.first_delivery);
}

#[tokio::test]
async fn expiry_cleanup_and_future_clock() {
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let clock = FrozenClock::new(now);
    let store = store_on(Arc::new(MemoryBackend::new())).with_clock(Arc::new(clock.clone()));
    let (digest, _) = store.put_blob(b"x".to_vec()).await.unwrap();
    let op = OperationId::mint(&clock);
    store
        .set_target(op, "r", digest.clone(), None)
        .await
        .unwrap();
    clock.add(Duration::days(8));
    let err = store
        .set_target(op, "r", digest.clone(), None)
        .await
        .unwrap_err();
    assert!(matches!(
        err.downcast_ref::<CoreError>(),
        Some(CoreError::UnknownOperation { .. })
    ));
    store
        .backend
        .delete(&store.intent_key(&OpIdentity::Generic(op)))
        .await
        .unwrap();
    let err = store
        .set_target(op, "r", digest.clone(), None)
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
        .set_target(future, "r", digest, None)
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
async fn foundation_length_prefixed_key_is_opaque_bytes() {
    let mut key = Vec::new();
    key.extend_from_slice(&36u32.to_be_bytes());
    key.extend(vec![b'd'; 36]);
    key.extend_from_slice(&32u32.to_be_bytes());
    key.extend([0xab; 32]);
    assert_eq!(key.len(), 76);
    let sk = StableKey::new(key.clone()).unwrap();
    assert_eq!(sk.as_bytes().len(), 76);
    assert_eq!(sk.to_hex().len(), 152);

    let store = store_on(Arc::new(MemoryBackend::new()));
    let log = LogStore::complete_feed(&store, "feed");
    let rec = log
        .append_stable(sk.clone(), "w", b"fdnc-bytes", 60)
        .await
        .unwrap();
    assert_eq!(rec.range.first, rec.range.last);
    let again = log.append_stable(sk, "w", b"fdnc-bytes", 60).await.unwrap();
    assert_eq!(again.range.first, rec.range.first);
}

#[tokio::test]
async fn bounded_read_page_and_first_event_too_large() {
    let store = store_on(Arc::new(MemoryBackend::new()));
    let log = LogStore::new(&store, "demo");
    log.append(store.mint_operation(), "w", &evs(&["aa", "bbbb"]), 60)
        .await
        .unwrap();
    let page = log.read_page(1, 1, 100).await.unwrap();
    assert_eq!(page.frames.len(), 1);
    assert_eq!(page.head_seq, 2);
    assert_eq!(page.next, 2);
    assert!(page.hit_limit);
    let err = log.read_page(1, 10, 1).await.unwrap_err();
    assert!(format!("{err:#}").contains("first event"));
}

#[tokio::test]
async fn publisher_session_renews_without_bridge_fence() {
    let store = store_on(Arc::new(MemoryBackend::new()));
    let log = LogStore::complete_feed(&store, "feed");
    let session = log.claim_publisher("bridge-1", 60).await.unwrap();
    let key = sk("k1");
    session.append_stable(key.clone(), b"p").await.unwrap();
    session.renew().await.unwrap();
    let again = session.append_stable(key, b"p").await.unwrap();
    assert_eq!(again.range.first, 1);
}

#[tokio::test]
async fn new_constructor_cannot_trim_persisted_complete_feed() {
    let store = store_on(Arc::new(MemoryBackend::new()));
    {
        let log = LogStore::complete_feed(&store, "feed");
        log.append_stable(sk("k"), "w", b"p", 60).await.unwrap();
    }
    let log = LogStore::new(&store, "feed");
    let err = log
        .trim_before(store.mint_operation(), 1)
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("complete"));
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
        .set_target(store.mint_operation(), "old", digest, None)
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("comb/v1"));
}

#[tokio::test]
async fn applied_cache_returns_original_not_current_head() {
    let store = store_on(Arc::new(MemoryBackend::new()));
    let (a, _) = store.put_blob(b"a".to_vec()).await.unwrap();
    let (b, _) = store.put_blob(b"b".to_vec()).await.unwrap();
    let op = store.mint_operation();
    let first = store.set_target(op, "r", a.clone(), None).await.unwrap();
    store
        .set_target(store.mint_operation(), "r", b.clone(), None)
        .await
        .unwrap();
    let again = store.set_target(op, "r", a.clone(), None).await.unwrap();
    assert_eq!(again.generation, first.generation);
    assert_eq!(again.epoch, first.epoch);
    assert_eq!(again.commit, first.commit);
    assert_eq!(again.value.generation, first.generation);
    assert_eq!(again.value.target, first.value.target);
    assert_eq!(again.value.target, Some(a));
    let (live, _) = store.read_ref("r").await.unwrap().unwrap();
    assert_eq!(live.generation, 2);
    assert_eq!(live.target, Some(b));
    assert_ne!(again.value.generation, live.generation);
}

#[tokio::test]
async fn delayed_group_race_same_producer_one_range() {
    let store = store_on(Arc::new(MemoryBackend::new()));
    let (w1, t1) = combctl::log::GroupWriter::spawn(store.clone(), "g", "w".into(), 5, 100);
    let (w2, t2) = combctl::log::GroupWriter::spawn(store.clone(), "g", "w".into(), 5, 100);
    let op = store.mint_operation();
    let payload = evs(&["only-once"]);
    let a = w1.submit(op, payload.clone());
    let b = w2.submit(op, payload);
    let (ra, rb) = tokio::join!(a, b);
    let ra = ra.expect("group 1");
    let rb = rb.expect("group 2");
    assert_eq!((ra.first, ra.last), (rb.first, rb.last));
    drop(w1);
    drop(w2);
    let _ = t1.await;
    let _ = t2.await;
    let log = LogStore::new(&store, "g");
    let frames = log.read(1).await.unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].payload, b"only-once");
}

#[tokio::test]
async fn corrupt_stable_node_fails_closed() {
    let mem: Arc<MemoryBackend> = Arc::new(MemoryBackend::new());
    let store = store_on(mem.clone());
    let log = LogStore::complete_feed(&store, "feed");
    let rec = log.append_stable(sk("k"), "w", b"p", 60).await.unwrap();
    let (_, manifest) = log.status().await.unwrap().unwrap();
    let index = manifest
        .stable_index
        .as_ref()
        .expect("complete feed has a stable index")
        .digest
        .clone();
    mem.delete(&store.object_key(&index)).await.unwrap();
    let err = log.append_stable(sk("k"), "w", b"p", 60).await.unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<CoreError>(),
            Some(CoreError::RecoveryFailed(_)) | Some(CoreError::IntegrityError(_))
        ) || format!("{err:#}").contains("unreadable")
            || format!("{err:#}").contains("integrity")
            || format!("{err:#}").contains("missing"),
        "{err:#}"
    );
    let _ = rec;
}

#[tokio::test]
async fn forged_applied_cache_is_not_canonical() {
    let mem: Arc<MemoryBackend> = Arc::new(MemoryBackend::new());
    let store = store_on(mem.clone());
    let (digest, _) = store.put_blob(b"x".to_vec()).await.unwrap();
    let op = store.mint_operation();
    let first = store
        .set_target(op, "r", digest.clone(), None)
        .await
        .unwrap();
    let (payload, _) = store.get_blob(&first.commit).await.unwrap();
    let mut commit: Commit = serde_json::from_slice(&payload).unwrap();
    commit.change = serde_json::json!({
        "kind": "set-target",
        "target": digest,
        "forged": true
    });
    let (fake, _) = store
        .put_blob(serde_json::to_vec(&commit).unwrap())
        .await
        .unwrap();
    assert_ne!(fake, first.commit);
    let key = store.intent_key(&OpIdentity::Generic(op));
    let (bytes, version) = store.backend.get(&key).await.unwrap();
    let mut intent: OpIntent = serde_json::from_slice(&bytes).unwrap();
    if let IntentState::Applied { commit, .. } = &mut intent.state {
        *commit = fake;
    }
    store
        .backend
        .put_update(
            &key,
            Some(&version),
            &serde_json::to_vec_pretty(&intent).unwrap(),
        )
        .await
        .unwrap();
    let err = store.set_target(op, "r", digest, None).await.unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<CoreError>(),
            Some(CoreError::RecoveryFailed(_))
        ),
        "{err:#}"
    );
}

#[tokio::test]
async fn applied_retry_restores_original_ref_value() {
    let store = store_on(Arc::new(MemoryBackend::new()));
    let (a, _) = store.put_blob(b"a".to_vec()).await.unwrap();
    let (b, _) = store.put_blob(b"b".to_vec()).await.unwrap();
    let claim_op = store.mint_operation();
    let claimed = store
        .claim(claim_op, "r", "writer", 60, false)
        .await
        .unwrap();
    let lease = claimed.value.lease.clone();
    store
        .set_target(store.mint_operation(), "r", a.clone(), Some(claimed.epoch))
        .await
        .unwrap();
    let stolen = store
        .claim(store.mint_operation(), "r", "thief", 60, true)
        .await
        .unwrap();
    let again = store
        .claim(claim_op, "r", "writer", 60, false)
        .await
        .unwrap();
    assert_eq!(again.generation, claimed.generation);
    assert_eq!(again.value.lease, lease);
    assert_eq!(again.value.target, claimed.value.target);
    assert_eq!(again.value.epoch, claimed.epoch);

    let set_op = store.mint_operation();
    let set = store
        .set_target(set_op, "r", a.clone(), Some(stolen.epoch))
        .await
        .unwrap();
    store
        .set_target(store.mint_operation(), "r", b.clone(), Some(stolen.epoch))
        .await
        .unwrap();
    let set_again = store
        .set_target(set_op, "r", a.clone(), None)
        .await
        .unwrap();
    assert_eq!(set_again.value.target, Some(a.clone()));
    assert_eq!(set_again.generation, set.generation);

    let rel_op = store.mint_operation();
    let live = store.read_ref("r").await.unwrap().unwrap().0;
    let released = store.release(rel_op, "r", live.epoch).await.unwrap();
    store
        .claim(store.mint_operation(), "r", "later", 60, false)
        .await
        .unwrap();
    let rel_again = store.release(rel_op, "r", live.epoch).await.unwrap();
    assert_eq!(rel_again.generation, released.generation);
    assert!(rel_again.value.lease.is_none());
    assert_eq!(rel_again.value.target, released.value.target);
}

#[tokio::test]
async fn stores_sharing_backend_keep_independent_clocks() {
    let mem: Arc<MemoryBackend> = Arc::new(MemoryBackend::new());
    let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let s1 = store_on(mem.clone()).with_clock(Arc::new(FrozenClock::new(now)));
    let s2 = store_on(mem).with_clock(Arc::new(FrozenClock::new(now + Duration::days(8))));
    let a = s1.mint_operation();
    let b = s2.mint_operation();
    assert!(a.issued_at() < b.issued_at());
}

#[tokio::test]
async fn core_cannot_overwrite_log_owned_ref() {
    let store = store_on(Arc::new(MemoryBackend::new()));
    let log = LogStore::complete_feed(&store, "feed");
    log.append_stable(sk("k"), "w", b"p", 60).await.unwrap();
    let (digest, _) = store.put_blob(b"x".to_vec()).await.unwrap();
    let err = store
        .set_target(store.mint_operation(), "log/feed/p0", digest, None)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<CoreError>(),
            Some(CoreError::Rejected(_))
        ) || format!("{err:#}").contains("log-owned")
            || format!("{err:#}").contains("log manifest"),
        "{err:#}"
    );
}

#[tokio::test]
async fn unreadable_ref_target_is_not_permission_to_overwrite() {
    let mem: Arc<MemoryBackend> = Arc::new(MemoryBackend::new());
    let store = store_on(mem.clone());
    let (digest, _) = store.put_blob(b"x".to_vec()).await.unwrap();
    store
        .set_target(store.mint_operation(), "r", digest.clone(), None)
        .await
        .unwrap();
    mem.delete(&store.object_key(&digest)).await.unwrap();
    let (other, _) = store.put_blob(b"y".to_vec()).await.unwrap();
    let err = store
        .set_target(store.mint_operation(), "r", other, None)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err.downcast_ref::<CoreError>(),
            Some(CoreError::RecoveryFailed(_))
        ),
        "{err:#}"
    );
}
