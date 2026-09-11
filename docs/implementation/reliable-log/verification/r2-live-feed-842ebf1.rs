//! Root live-backend acceptance for CompleteFeed. Configuration is supplied by env.
use bytes::Bytes;
use comb_core::{DigestKey, StableKey};
use comb_object::{
    failpoint::{FailAction, FailMethod, FailRule, FailpointBackend},
    local::LocalBackend,
    s3::S3Backend,
    ObjectBackend,
};
use combctl::{
    log::{
        CallContext, CompleteFeed, Cursor, LeasePolicy, LogReader, ReadError, ReadLimits,
        StableAppendError, WriterLabel, WriterState,
    },
    store::Store,
};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio_util::sync::CancellationToken;
fn call() -> CallContext {
    CallContext::new(
        tokio::time::Instant::now() + Duration::from_secs(60),
        CancellationToken::new(),
    )
}
#[tokio::test]
async fn live_v3_lost_cas_and_fresh_reader() {
    let kind = std::env::var("COMB_DRILL_BACKEND").expect("explicit backend required");
    let local = tempfile::tempdir().unwrap();
    let backend: Arc<dyn ObjectBackend> = match kind.as_str() {
        "local" => Arc::new(LocalBackend::new(local.path())),
        "s3" => Arc::new(
            S3Backend::connect(
                std::env::var("AWS_PROFILE").ok().as_deref(),
                Some(&std::env::var("COMB_S3_REGION").unwrap()),
                &std::env::var("COMB_S3_BUCKET").unwrap(),
                &std::env::var("COMB_S3_PREFIX").unwrap(),
                std::env::var("COMB_S3_ENDPOINT").ok().as_deref(),
            )
            .await,
        ),
        _ => panic!("unsupported backend"),
    };
    let fp = Arc::new(FailpointBackend::new(backend.clone()));
    let tenant = format!("live_{}", chrono::Utc::now().timestamp_millis());
    let cache = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::new(
        fp.clone(),
        tenant.clone(),
        DigestKey::from_bytes([61; 32]),
        Some(cache.path().join("initial")),
    ));
    let feed = CompleteFeed::open(store, "live".into(), &call())
        .await
        .unwrap();
    let writer = feed
        .writer_session(
            WriterLabel::try_from("live-test").unwrap(),
            LeasePolicy::bridge_default(),
        )
        .unwrap();
    writer.ready(&call()).await.unwrap();
    fp.arm(FailRule {
        method: FailMethod::PutUpdate,
        key_contains: format!("comb/v3/tenants/{tenant}/refs/log/live/p0.json"),
        successes_before_fire: 0,
        fires: 1,
        action: FailAction::DropResponse,
    });
    let key = StableKey::try_from_canonical(vec![0, 255, 1, 0, 42]).unwrap();
    let payload = Bytes::from(vec![255; 16 * 1024]);
    let lost = writer
        .append_stable(key.clone(), payload.clone(), &call())
        .await
        .unwrap_err();
    assert!(matches!(lost, StableAppendError::Unavailable));
    assert_eq!(
        feed.head(&call()).await.unwrap().head_seq,
        1,
        "the lost reply follows a committed append"
    );
    let first = writer
        .append_stable(key.clone(), payload.clone(), &call())
        .await
        .unwrap();
    assert_eq!(
        fp.injected.load(Ordering::Relaxed),
        1,
        "lost final CAS response must fire"
    );
    assert_eq!((first.range.first, first.range.last), (1, 1));
    let receipt = serde_json::to_value(&first).unwrap();
    let again = writer
        .append_stable(key.clone(), payload.clone(), &call())
        .await
        .unwrap();
    assert_eq!(serde_json::to_value(again).unwrap(), receipt);
    assert!(matches!(
        writer
            .append_stable(key.clone(), Bytes::from_static(b"different"), &call())
            .await,
        Err(StableAppendError::StableKeyConflict { .. })
    ));
    let second = writer
        .append_stable(
            StableKey::try_from_canonical(b"second".to_vec()).unwrap(),
            Bytes::from_static(b"second payload"),
            &call(),
        )
        .await
        .unwrap();
    assert_eq!((second.range.first, second.range.last), (2, 2));
    let fresh = Arc::new(
        Store::new(
            backend,
            tenant,
            DigestKey::from_bytes([61; 32]),
            Some(cache.path().join("fresh")),
        )
        .with_clock(Arc::new(comb_core::operation::FrozenClock::new(
            chrono::Utc::now() + chrono::Duration::days(8),
        ))),
    );
    let reader = CompleteFeed::open(fresh, "live".into(), &call())
        .await
        .unwrap();
    let peer = reader
        .writer_session(
            WriterLabel::try_from("fresh-peer").unwrap(),
            LeasePolicy::bridge_default(),
        )
        .unwrap();
    let retry = peer
        .append_stable(key.clone(), payload.clone(), &call())
        .await
        .unwrap();
    assert_eq!(serde_json::to_value(retry).unwrap(), receipt);
    assert!(
        matches!(peer.state(), WriterState::Unacquired),
        "committed key lookup must not acquire a competing lease"
    );
    assert!(
        matches!(
            peer.append_stable(key, Bytes::from_static(b"late conflict"), &call())
                .await,
            Err(StableAppendError::StableKeyConflict { .. })
        ),
        "stable conflict evidence must outlive the generic seven-day window"
    );
    let too_small = reader
        .read_page(
            Cursor::first(0),
            ReadLimits::try_new(1, 1).unwrap(),
            &call(),
        )
        .await;
    assert!(matches!(
        too_small,
        Err(ReadError::EventTooLarge {
            event_bytes: 16384,
            ..
        })
    ));
    let page = reader
        .read_page(
            Cursor::first(0),
            ReadLimits::try_new(1, 16384).unwrap(),
            &call(),
        )
        .await
        .unwrap();
    assert_eq!(page.snapshot_head, 2);
    assert_eq!(page.events.len(), 1);
    assert_eq!(page.events[0].payload, payload);
    assert_eq!(page.next.next_seq, 2);
    assert!(!page.at_head);
    let tail = reader
        .read_page(page.next, ReadLimits::try_new(1, 1024).unwrap(), &call())
        .await
        .unwrap();
    assert_eq!(tail.events.len(), 1);
    assert_eq!(tail.events[0].payload.as_ref(), b"second payload");
    assert_eq!(tail.next.next_seq, 3);
    assert!(tail.at_head);
    peer.close(&call()).await.unwrap();
    writer.close(&call()).await.unwrap();
}
