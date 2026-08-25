//! Phase C drills: group commit, compaction/append race (L4), read after
//! compaction (L5), retention floor (L9), and sweeper safety (G-series).

use comb_core::error::CoreError;
use comb_core::DigestKey;
use combctl::log::{GroupWriter, LogStore};
use combctl::store::Store;
use comb_object::backend::ObjectBackend;
use comb_object::memory::MemoryBackend;
use std::sync::Arc;

fn store() -> Store {
    Store {
        backend: Arc::new(MemoryBackend::new()),
        tenant: "org_t".into(),
        key: DigestKey::from_bytes([5u8; 32]),
        cache_dir: None,
    }
}

fn evs(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

/// Group commit: many concurrent producers, all acked with disjoint
/// contiguous ranges, far fewer CAS commits than events.
#[tokio::test]
async fn group_commit_batches_and_acks_correctly() {
    let store = store();
    let (writer, task) = GroupWriter::spawn(store.clone(), "g", "w".into(), 20, 1000);
    let writer = Arc::new(writer);

    let mut tasks = Vec::new();
    for p in 0..10 {
        let writer = writer.clone();
        tasks.push(tokio::spawn(async move {
            let mut ranges = Vec::new();
            for i in 0..20 {
                let range = writer.submit(vec![format!("p{p}-e{i}")]).await.unwrap();
                ranges.push(range);
            }
            ranges
        }));
    }
    let mut all_seqs = Vec::new();
    for t in tasks {
        for (first, last) in t.await.unwrap() {
            for s in first..=last {
                all_seqs.push(s);
            }
        }
    }
    drop(writer);
    let stats = task.await.unwrap();

    all_seqs.sort();
    assert_eq!(all_seqs, (1..=200).collect::<Vec<u64>>(), "contiguous, disjoint ranges");
    assert_eq!(stats.events, 200);
    assert!(stats.commits < 200, "batching must occur: {} commits", stats.commits);

    let log = LogStore::new(&store, "g");
    let frames = log.read(1).await.unwrap();
    assert_eq!(frames.len(), 200);
    assert_eq!(frames.iter().map(|f| f.seq).collect::<Vec<_>>(), (1..=200).collect::<Vec<u64>>());
}

/// L4: compaction racing appends — conditional-update retries resolve it
/// and no entry is lost or duplicated.
#[tokio::test]
async fn l4_compaction_races_appends_without_loss() {
    let store = store();
    let log = LogStore::new(&store, "race");
    for i in 0..10 {
        log.append("w", &evs(&[&format!("pre-{i}")]), 300).await.unwrap();
    }

    let store2 = store.clone();
    let appender = tokio::spawn(async move {
        let log = LogStore::new(&store2, "race");
        for i in 0..10 {
            log.append("w", &evs(&[&format!("mid-{i}")]), 300).await.unwrap();
        }
    });
    let store3 = store.clone();
    let compactor = tokio::spawn(async move {
        let log = LogStore::new(&store3, "race");
        for _ in 0..3 {
            log.compact().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    });
    appender.await.unwrap();
    compactor.await.unwrap();

    let frames = log.read(1).await.unwrap();
    assert_eq!(frames.len(), 20, "no entry lost or duplicated");
    assert_eq!(frames.iter().map(|f| f.seq).collect::<Vec<_>>(), (1..=20).collect::<Vec<u64>>());
}

/// L5 analogue: after compaction the representation changed (segments, not
/// chunks) but the contents are identical.
#[tokio::test]
async fn l5_read_after_compaction_is_identical() {
    let store = store();
    let log = LogStore::new(&store, "c");
    for i in 0..5 {
        log.append("w", &evs(&[&format!("e{i}")]), 300).await.unwrap();
    }
    let before = log.read(1).await.unwrap();
    let merged = log.compact().await.unwrap();
    assert_eq!(merged, 5);
    let after = log.read(1).await.unwrap();
    assert_eq!(
        before.iter().map(|f| (f.seq, f.payload.clone())).collect::<Vec<_>>(),
        after.iter().map(|f| (f.seq, f.payload.clone())).collect::<Vec<_>>()
    );
    let (_, manifest) = log.status().await.unwrap().unwrap();
    assert_eq!(manifest.chunks.len(), 0);
    assert_eq!(manifest.segments.len(), 1);
}

/// L9: reading below the retention floor is an explicit Trimmed{resume_at},
/// never a silent gap; reading at the floor works.
#[tokio::test]
async fn l9_trim_refuses_below_floor_with_resume_position() {
    let store = store();
    let log = LogStore::new(&store, "t");
    for i in 1..=10 {
        log.append("w", &evs(&[&format!("e{i}")]), 300).await.unwrap();
    }
    let floor = log.trim_before(4).await.unwrap();
    assert_eq!(floor, 4);

    let err = log.read(1).await.unwrap_err();
    match err.downcast_ref::<CoreError>() {
        Some(CoreError::Trimmed { resume_at }) => assert_eq!(*resume_at, 5),
        other => panic!("expected Trimmed, got {other:?}"),
    }
    let frames = log.read(5).await.unwrap();
    assert_eq!(frames.iter().map(|f| f.seq).collect::<Vec<_>>(), (5..=10).collect::<Vec<u64>>());
}

/// Sweeper: deletes only unreachable objects past grace; everything
/// reachable from refs (manifests, chunks, segments, journal) survives;
/// grace protects fresh orphans (G4/L1 window).
#[tokio::test]
async fn sweeper_removes_orphans_and_only_orphans() {
    let store = store();
    let log = LogStore::new(&store, "s");
    for i in 0..6 {
        log.append("w", &evs(&[&format!("e{i}")]), 300).await.unwrap();
    }
    log.compact().await.unwrap(); // orphans the 6 chunk objects
    let before = log.read(1).await.unwrap();

    // A fresh orphan is protected by grace...
    let report = combctl::sweep::sweep(&store, 60, true).await.unwrap();
    assert_eq!(report.deleted, 0);
    assert!(report.in_grace >= 6, "compacted chunks are in grace: {}", report.in_grace);

    // ...and collected once grace expires (grace 0 for the test).
    let report = combctl::sweep::sweep(&store, 0, true).await.unwrap();
    assert!(report.deleted >= 6, "orphaned chunks deleted: {report:?}");

    // Nothing observable changed.
    let after = log.read(1).await.unwrap();
    assert_eq!(
        before.iter().map(|f| (f.seq, f.payload.clone())).collect::<Vec<_>>(),
        after.iter().map(|f| (f.seq, f.payload.clone())).collect::<Vec<_>>()
    );
    // And a second sweep finds nothing.
    let report = combctl::sweep::sweep(&store, 0, true).await.unwrap();
    assert_eq!(report.deleted, 0);
}
