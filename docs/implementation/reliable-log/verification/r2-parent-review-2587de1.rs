use bytes::Bytes;
use comb_core::{DigestKey, StableKey};
use comb_object::{
    failpoint::{FailAction, FailMethod, FailRule, FailpointBackend},
    memory::MemoryBackend,
    ObjectBackend,
};
use combctl::{
    log::{
        CallContext, CompleteFeed, Cursor, LeasePolicy, LogReader, ReadError, ReadLimits,
        WriterLabel,
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
        tokio::time::Instant::now() + Duration::from_secs(5),
        CancellationToken::new(),
    )
}
async fn setup() -> (CompleteFeed, Arc<FailpointBackend>) {
    let backend = Arc::new(FailpointBackend::new(Arc::new(MemoryBackend::new())));
    let store = Arc::new(Store::new(
        backend.clone(),
        "review",
        DigestKey::from_bytes([9; 32]),
        None,
    ));
    (
        CompleteFeed::open(store, "parent".into(), &call())
            .await
            .unwrap(),
        backend,
    )
}
fn limits() -> ReadLimits {
    ReadLimits::try_new(8, 1024).unwrap()
}
#[tokio::test]
async fn review_cancelled_read_is_cancelled() {
    let (feed, _) = setup().await;
    let c = call();
    c.cancellation.cancel();
    assert!(matches!(
        feed.read_page(Cursor::first(0), limits(), &c).await,
        Err(ReadError::Cancelled)
    ));
}
#[tokio::test]
async fn review_expired_read_is_deadline_exceeded() {
    let (feed, _) = setup().await;
    let c = CallContext::new(
        tokio::time::Instant::now() - Duration::from_millis(1),
        CancellationToken::new(),
    );
    assert!(matches!(
        feed.read_page(Cursor::first(0), limits(), &c).await,
        Err(ReadError::DeadlineExceeded)
    ));
}
#[tokio::test]
async fn review_read_never_uses_unbounded_get() {
    let (feed, backend) = setup().await;
    backend.arm(FailRule {
        method: FailMethod::Get,
        key_contains: "refs/log/parent/p0.json".into(),
        successes_before_fire: 0,
        fires: 1,
        action: FailAction::DropRequest,
    });
    let page = feed
        .read_page(Cursor::first(0), limits(), &call())
        .await
        .unwrap();
    assert!(page.at_head);
    assert_eq!(
        backend.injected.load(Ordering::Relaxed),
        0,
        "bounded read called unbounded ref get"
    );
}
#[tokio::test]
async fn review_transient_manifest_read_recovers() {
    let (feed, backend) = setup().await;
    let writer = feed
        .writer_session(
            WriterLabel::try_from("review").unwrap(),
            LeasePolicy::bridge_default(),
        )
        .unwrap();
    writer
        .append_stable(
            StableKey::try_from_canonical(b"one".to_vec()).unwrap(),
            Bytes::from_static(b"payload"),
            &call(),
        )
        .await
        .unwrap();
    backend.arm(FailRule {
        method: FailMethod::GetLimited,
        key_contains: "/objects/".into(),
        successes_before_fire: 0,
        fires: 1,
        action: FailAction::DropRequest,
    });
    let page = feed
        .read_page(Cursor::first(0), limits(), &call())
        .await
        .expect("transient manifest read should recover");
    assert_eq!(page.events.len(), 1);
    assert_eq!(backend.injected.load(Ordering::Relaxed), 1);
}
#[tokio::test]
async fn review_unacquired_close_completes() {
    let (feed, _) = setup().await;
    let writer = feed
        .writer_session(
            WriterLabel::try_from("review").unwrap(),
            LeasePolicy::bridge_default(),
        )
        .unwrap();
    writer.close(&call()).await.unwrap();
}
#[tokio::test]
async fn review_existing_v3_survives_later_v1_ref() {
    let (feed, backend) = setup().await;
    let writer = feed
        .writer_session(
            WriterLabel::try_from("review").unwrap(),
            LeasePolicy::bridge_default(),
        )
        .unwrap();
    writer
        .append_stable(
            StableKey::try_from_canonical(b"one".to_vec()).unwrap(),
            Bytes::from_static(b"payload"),
            &call(),
        )
        .await
        .unwrap();
    backend
        .put_create("comb/v1/tenants/review/refs/log/parent/p0.json", b"legacy")
        .await
        .unwrap();
    let page = feed
        .read_page(Cursor::first(0), limits(), &call())
        .await
        .expect("existing v3 must remain authoritative");
    assert_eq!(page.events.len(), 1);
}

async fn replace_manifest(backend: &FailpointBackend, mutate: impl FnOnce(&mut serde_json::Value)) {
    let ref_key = "comb/v3/tenants/review/refs/log/parent/p0.json";
    let (bytes, version) = backend.get(ref_key).await.unwrap();
    let mut reference: comb_core::RefValue = serde_json::from_slice(&bytes).unwrap();
    let digest = reference.target.as_ref().unwrap();
    let object_key = |d: &comb_core::Digest| {
        format!(
            "comb/v3/tenants/review/objects/b3k/{}/{}",
            d.key_prefix(),
            d.hex()
        )
    };
    let (object, _) = backend.get(&object_key(digest)).await.unwrap();
    let key = DigestKey::from_bytes([9; 32]);
    let envelope = comb_core::Envelope::decode(&object, &key).unwrap();
    let mut body: serde_json::Value = serde_json::from_slice(&envelope.payload).unwrap();
    mutate(&mut body);
    let replacement = comb_core::Envelope::new(
        "review",
        comb_core::ObjectKind::Blob,
        "comb.log.partition-manifest/v3",
        serde_json::to_vec(&body).unwrap(),
        &key,
    );
    backend
        .put_create(
            &object_key(&replacement.meta.digest),
            &replacement.encode().unwrap(),
        )
        .await
        .unwrap();
    reference.target = Some(replacement.meta.digest);
    backend
        .put_update(
            ref_key,
            Some(&version),
            &serde_json::to_vec(&reference).unwrap(),
        )
        .await
        .unwrap();
}
#[tokio::test]
async fn review_max_head_is_integrity_not_overflow() {
    let (feed, backend) = setup().await;
    let writer = feed
        .writer_session(
            WriterLabel::try_from("review").unwrap(),
            LeasePolicy::bridge_default(),
        )
        .unwrap();
    writer
        .append_stable(
            StableKey::try_from_canonical(b"one".to_vec()).unwrap(),
            Bytes::from_static(b"payload"),
            &call(),
        )
        .await
        .unwrap();
    replace_manifest(&backend, |m| m["head_seq"] = serde_json::json!(u64::MAX)).await;
    assert!(matches!(
        feed.head(&call()).await,
        Err(ReadError::Integrity(_))
    ));
}
#[tokio::test]
async fn review_foreign_manifest_is_not_replayed() {
    let (feed, backend) = setup().await;
    let writer = feed
        .writer_session(
            WriterLabel::try_from("review").unwrap(),
            LeasePolicy::bridge_default(),
        )
        .unwrap();
    writer
        .append_stable(
            StableKey::try_from_canonical(b"one".to_vec()).unwrap(),
            Bytes::from_static(b"payload"),
            &call(),
        )
        .await
        .unwrap();
    replace_manifest(&backend, |m| {
        m["log"] = serde_json::json!("log/foreign/p0");
        m["header"]["resource"] = serde_json::json!("log/foreign/p0");
    })
    .await;
    assert!(matches!(
        feed.read_page(Cursor::first(0), limits(), &call()).await,
        Err(ReadError::Integrity(_))
    ));
}
