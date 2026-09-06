//! Minimal-Log drills (analogues of spec §22.2 L1–L3 for the Phase B log).

use comb_core::{DigestKey, OperationId};
use comb_object::backend::ObjectBackend;
use comb_object::failpoint::FailpointBackend;
use comb_object::memory::MemoryBackend;
use combctl::log::LogStore;
use combctl::store::Store;
use std::sync::Arc;

fn store_on(backend: Arc<dyn ObjectBackend>) -> Store {
    Store::new(backend, "org_t", DigestKey::from_bytes([5u8; 32]), None)
}

fn evs(v: &[&str]) -> Vec<Vec<u8>> {
    v.iter().map(|s| s.as_bytes().to_vec()).collect()
}

fn op(store: &Store) -> OperationId {
    store.mint_operation()
}

/// Append assigns contiguous sequences; read returns them in order.
#[tokio::test]
async fn append_read_roundtrip() {
    let store = store_on(Arc::new(MemoryBackend::new()));
    let log = LogStore::new(&store, "demo");
    let a1 = log
        .append(op(&store), "a", &evs(&["one", "two"]), 60)
        .await
        .unwrap();
    let a2 = log
        .append(op(&store), "a", &evs(&["three"]), 60)
        .await
        .unwrap();
    assert_eq!((a1.first, a1.last, a2.first, a2.last), (1, 2, 3, 3));
    let frames = log.read(1).await.unwrap();
    assert_eq!(
        frames
            .iter()
            .map(|f| (f.seq, f.payload.as_slice()))
            .collect::<Vec<_>>(),
        vec![
            (1, b"one".as_slice()),
            (2, b"two".as_slice()),
            (3, b"three".as_slice())
        ]
    );
    let tail = log.read(3).await.unwrap();
    assert_eq!(tail.len(), 1);
    assert_eq!(tail[0].payload, b"three");
}

/// L3 analogue: after takeover, the old leader cannot append; the new
/// leader's epoch is higher; no interleaving.
#[tokio::test]
async fn takeover_fences_old_leader() {
    let store = store_on(Arc::new(MemoryBackend::new()));
    let log = LogStore::new(&store, "demo");
    log.append(op(&store), "leader-a", &evs(&["a1"]), 300)
        .await
        .unwrap();

    let epoch = log.steal(op(&store), "leader-b", 300).await.unwrap();
    assert_eq!(epoch, 2);

    let err = log
        .append(op(&store), "leader-a", &evs(&["a2-stale"]), 300)
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("lease held"), "{err:#}");

    log.append(op(&store), "leader-b", &evs(&["b1"]), 300)
        .await
        .unwrap();
    let frames = log.read(1).await.unwrap();
    assert_eq!(
        frames
            .iter()
            .map(|f| f.payload.as_slice())
            .collect::<Vec<_>>(),
        vec![b"a1".as_slice(), b"b1".as_slice()],
        "stale leader's data must never appear"
    );
}

/// L1 analogue: crash between chunk upload and manifest publication — the
/// chunk is an invisible orphan, the head is unchanged, no ack was given,
/// and the retry produces one clean append with no gap or duplicate.
#[tokio::test]
async fn crash_before_manifest_publication_is_invisible() {
    let mem: Arc<MemoryBackend> = Arc::new(MemoryBackend::new());
    let healthy = store_on(mem.clone());
    let log = LogStore::new(&healthy, "demo");
    log.append(op(&healthy), "a", &evs(&["committed"]), 300)
        .await
        .unwrap();

    // Drop the ref CAS request: chunk/manifest may upload, the ref does not move.
    let faulty_backend = Arc::new(FailpointBackend::drop_next_put_update_request(
        mem.clone(),
        "/refs/",
    ));
    let unlucky = store_on(faulty_backend);
    let flog = LogStore::new(&unlucky, "demo");
    let err = flog
        .append(op(&unlucky), "a", &evs(&["maybe-lost"]), 300)
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("injected"));

    let before = log.read(1).await.unwrap();
    let seqs: Vec<u64> = before.iter().map(|f| f.seq).collect();
    assert_eq!(
        seqs,
        (1..=seqs.len() as u64).collect::<Vec<_>>(),
        "no gaps ever"
    );

    log.append(op(&healthy), "a", &evs(&["retry"]), 300)
        .await
        .unwrap();
    let after = log.read(1).await.unwrap();
    let seqs: Vec<u64> = after.iter().map(|f| f.seq).collect();
    assert_eq!(seqs, (1..=seqs.len() as u64).collect::<Vec<_>>());
    assert_eq!(after.last().unwrap().payload, b"retry");
}

/// Follower change detection is by head_seq (logical state), and follow
/// delivers frames appended after it started.
#[tokio::test]
async fn follow_sees_new_appends() {
    let store = store_on(Arc::new(MemoryBackend::new()));
    let log = LogStore::new(&store, "demo");
    log.append(op(&store), "a", &evs(&["early"]), 300)
        .await
        .unwrap();

    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen2 = seen.clone();
    let stop_after = std::time::Instant::now() + std::time::Duration::from_millis(900);

    let store2 = store_on(store.backend.clone());
    let appender = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let log = LogStore::new(&store2, "demo");
        log.append(op(&store2), "a", &evs(&["live-1", "live-2"]), 300)
            .await
            .unwrap();
    });

    log.follow(
        1,
        50,
        |f| seen2.lock().unwrap().push(f.payload.clone()),
        || std::time::Instant::now() > stop_after,
    )
    .await
    .unwrap();
    appender.await.unwrap();

    let seen = seen.lock().unwrap().clone();
    assert_eq!(
        seen,
        vec![b"early".to_vec(), b"live-1".to_vec(), b"live-2".to_vec()]
    );
}
