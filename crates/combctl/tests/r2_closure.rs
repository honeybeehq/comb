//! Owned copies of root parent-review regressions for R2 closure.
//! Do not build in the isolated archive named by /tmp/comb-r2-closure-path.txt.

use bytes::Bytes;
use comb_core::{DigestKey, StableKey};
use comb_object::{
    failpoint::{FailAction, FailMethod, FailRule, FailpointBackend},
    memory::MemoryBackend,
    ObjectBackend,
};
use combctl::{
    log::{
        CallContext, CompleteFeed, Cursor, LeaseError, LeasePolicy, LogReader, ReadError,
        ReadLimits, WriterLabel, WriterState,
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

#[tokio::test]
async fn review_takeover_preserves_read_before_next_append() {
    let (feed, _) = setup().await;
    let a = feed
        .writer_session(
            WriterLabel::try_from("same").unwrap(),
            LeasePolicy::bridge_default(),
        )
        .unwrap();
    a.append_stable(
        StableKey::try_from_canonical(b"one".to_vec()).unwrap(),
        Bytes::from_static(b"payload"),
        &call(),
    )
    .await
    .unwrap();
    a.close(&call()).await.unwrap();
    let b = feed
        .writer_session(
            WriterLabel::try_from("same").unwrap(),
            LeasePolicy::bridge_default(),
        )
        .unwrap();
    b.ready(&call()).await.unwrap();
    let page = feed
        .read_page(Cursor::first(0), limits(), &call())
        .await
        .expect("lease takeover must preserve replay");
    assert_eq!(page.events.len(), 1);
}

struct JumpClock(std::sync::atomic::AtomicI64);
impl comb_core::operation::Clock for JumpClock {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp(self.0.load(Ordering::SeqCst), 0).unwrap()
    }
}

#[tokio::test]
async fn review_idle_renewal_cannot_revive_expired_lease() {
    let clock = Arc::new(JumpClock(std::sync::atomic::AtomicI64::new(
        chrono::Utc::now().timestamp(),
    )));
    let store = Arc::new(
        Store::new(
            Arc::new(MemoryBackend::new()),
            "review",
            DigestKey::from_bytes([9; 32]),
            None,
        )
        .with_clock(clock.clone()),
    );
    let feed = CompleteFeed::open(store, "expiry".into(), &call())
        .await
        .unwrap();
    let policy = LeasePolicy {
        ttl: Duration::from_secs(2),
        renew_every: Duration::from_millis(100),
        clock_slack: Duration::ZERO,
        initial_acquire_budget: Duration::from_secs(2),
    };
    let writer = feed
        .writer_session(WriterLabel::try_from("same").unwrap(), policy)
        .unwrap();
    writer.ready(&call()).await.unwrap();
    tokio::task::yield_now().await;
    clock.0.fetch_add(3, Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(
        matches!(writer.state(), WriterState::Lost { .. }),
        "expired lease was renewed back to Active: {:?}",
        writer.state()
    );
}

#[tokio::test]
async fn review_acquisition_mutex_wait_honors_deadline() {
    let (feed, _) = setup().await;
    let a = feed
        .writer_session(
            WriterLabel::try_from("owner").unwrap(),
            LeasePolicy::bridge_default(),
        )
        .unwrap();
    a.ready(&call()).await.unwrap();
    let b = Arc::new(
        feed.writer_session(
            WriterLabel::try_from("contender").unwrap(),
            LeasePolicy::bridge_default(),
        )
        .unwrap(),
    );
    let first = {
        let b = b.clone();
        tokio::spawn(async move { b.ready(&call()).await })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        while !matches!(b.state(), WriterState::Acquiring { .. }) {
            tokio::task::yield_now().await
        }
    })
    .await
    .unwrap();
    let short = CallContext::new(
        tokio::time::Instant::now() + Duration::from_millis(20),
        CancellationToken::new(),
    );
    let got = tokio::time::timeout(Duration::from_millis(200), b.ready(&short)).await;
    first.abort();
    let _ = first.await;
    assert!(
        matches!(got, Ok(Err(LeaseError::DeadlineExceeded))),
        "mutex ignored caller deadline: {got:?}"
    );
}

async fn clear_committed_target(backend: &FailpointBackend) {
    let key = "comb/v3/tenants/review/refs/log/parent/p0.json";
    let (bytes, version) = backend.get(key).await.unwrap();
    let mut value: comb_core::RefValue = serde_json::from_slice(&bytes).unwrap();
    value.target = None;
    backend
        .put_update(key, Some(&version), &serde_json::to_vec(&value).unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn review_missing_committed_target_is_not_empty_feed() {
    let (feed, backend) = setup().await;
    let a = feed
        .writer_session(
            WriterLabel::try_from("same").unwrap(),
            LeasePolicy::bridge_default(),
        )
        .unwrap();
    a.append_stable(
        StableKey::try_from_canonical(b"one".to_vec()).unwrap(),
        Bytes::from_static(b"payload"),
        &call(),
    )
    .await
    .unwrap();
    clear_committed_target(&backend).await;
    assert!(
        matches!(
            feed.read_page(Cursor::first(0), limits(), &call()).await,
            Err(ReadError::Integrity(_))
        ),
        "missing target after committed append became empty success"
    );
}

#[tokio::test]
async fn review_missing_target_cannot_reset_stable_identity() {
    let (feed, backend) = setup().await;
    let a = feed
        .writer_session(
            WriterLabel::try_from("same").unwrap(),
            LeasePolicy::bridge_default(),
        )
        .unwrap();
    let key = StableKey::try_from_canonical(b"one".to_vec()).unwrap();
    a.append_stable(key.clone(), Bytes::from_static(b"payload"), &call())
        .await
        .unwrap();
    clear_committed_target(&backend).await;
    let got = a
        .append_stable(key, Bytes::from_static(b"different payload"), &call())
        .await;
    assert!(got.is_err(), "committed stable identity was reset: {got:?}");
}

#[tokio::test]
async fn review_transient_catalog_read_recovers() {
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
    let (ref_bytes, _) = backend
        .get("comb/v3/tenants/review/refs/log/parent/p0.json")
        .await
        .unwrap();
    let reference: comb_core::RefValue = serde_json::from_slice(&ref_bytes).unwrap();
    let digest = reference.target.unwrap();
    let object_key = format!(
        "comb/v3/tenants/review/objects/b3k/{}/{}",
        digest.key_prefix(),
        digest.hex()
    );
    let (object_bytes, _) = backend.get(&object_key).await.unwrap();
    let envelope =
        comb_core::Envelope::decode(&object_bytes, &DigestKey::from_bytes([9; 32])).unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(&envelope.payload).unwrap();
    let catalog_digest =
        comb_core::Digest::parse(manifest["catalog"]["root"]["digest"].as_str().unwrap()).unwrap();
    backend.arm(FailRule {
        method: FailMethod::GetLimited,
        key_contains: catalog_digest.hex().to_string(),
        successes_before_fire: 0,
        fires: 1,
        action: FailAction::DropRequest,
    });
    let page = feed
        .read_page(Cursor::first(0), limits(), &call())
        .await
        .expect("transient catalog read should recover");
    assert_eq!(page.events.len(), 1);
    assert_eq!(backend.injected.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn review_manifest_head_cannot_hide_catalog_events() {
    let (feed, backend) = setup().await;
    let a = feed
        .writer_session(
            WriterLabel::try_from("same").unwrap(),
            LeasePolicy::bridge_default(),
        )
        .unwrap();
    for k in [b"one".as_slice(), b"two".as_slice()] {
        a.append_stable(
            StableKey::try_from_canonical(k.to_vec()).unwrap(),
            Bytes::from_static(b"payload"),
            &call(),
        )
        .await
        .unwrap();
    }
    replace_manifest(&backend, |m| m["head_seq"] = serde_json::json!(1)).await;
    let got = feed.read_page(Cursor::first(0), limits(), &call()).await;
    assert!(
        matches!(got, Err(ReadError::Integrity(_))),
        "head concealed retained catalog events: {got:?}"
    );
}

#[tokio::test]
async fn review_concurrent_same_key_returns_original_receipt() {
    let (feed, _) = setup().await;
    let writer = Arc::new(
        feed.writer_session(
            WriterLabel::try_from("same").unwrap(),
            LeasePolicy::bridge_default(),
        )
        .unwrap(),
    );
    writer.ready(&call()).await.unwrap();
    let key = StableKey::try_from_canonical(b"one".to_vec()).unwrap();
    let payload = Bytes::from_static(b"payload");
    let a = {
        let writer = writer.clone();
        let key = key.clone();
        let payload = payload.clone();
        tokio::spawn(async move { writer.append_stable(key, payload, &call()).await })
    };
    let b = {
        let writer = writer.clone();
        let key = key.clone();
        let payload = payload.clone();
        tokio::spawn(async move { writer.append_stable(key, payload, &call()).await })
    };
    let ra = a.await.unwrap().expect("first concurrent same-key append");
    let rb = b.await.unwrap().expect("second concurrent same-key append");
    assert_eq!(ra.range.first, rb.range.first);
    assert_eq!(ra.range.last, rb.range.last);
    assert_eq!(ra.payload_hash, rb.payload_hash);
    let page = feed
        .read_page(Cursor::first(0), limits(), &call())
        .await
        .expect("one committed event");
    assert_eq!(page.events.len(), 1);
    assert!(page.at_head);
}

#[tokio::test]
async fn review_missing_target_after_release_is_not_empty() {
    let (feed, backend) = setup().await;
    let a = feed
        .writer_session(
            WriterLabel::try_from("same").unwrap(),
            LeasePolicy::bridge_default(),
        )
        .unwrap();
    a.append_stable(
        StableKey::try_from_canonical(b"one".to_vec()).unwrap(),
        Bytes::from_static(b"payload"),
        &call(),
    )
    .await
    .unwrap();
    a.close(&call()).await.unwrap();
    clear_committed_target(&backend).await;
    let got = feed.read_page(Cursor::first(0), limits(), &call()).await;
    assert!(
        matches!(got, Err(ReadError::Integrity(_))),
        "missing retained target after release became empty: {got:?}"
    );
}

#[tokio::test]
async fn review_missing_target_after_release_cannot_reset_key() {
    let (feed, backend) = setup().await;
    let a = feed
        .writer_session(
            WriterLabel::try_from("same").unwrap(),
            LeasePolicy::bridge_default(),
        )
        .unwrap();
    let key = StableKey::try_from_canonical(b"one".to_vec()).unwrap();
    a.append_stable(key.clone(), Bytes::from_static(b"payload"), &call())
        .await
        .unwrap();
    a.close(&call()).await.unwrap();
    clear_committed_target(&backend).await;
    let b = feed
        .writer_session(
            WriterLabel::try_from("same").unwrap(),
            LeasePolicy::bridge_default(),
        )
        .unwrap();
    let got = b
        .append_stable(key, Bytes::from_static(b"different"), &call())
        .await;
    assert!(got.is_err(), "released committed key was reset: {got:?}");
}

#[tokio::test]
async fn review_old_target_cannot_hide_committed_key() {
    let (feed, backend) = setup().await;
    let a = feed
        .writer_session(
            WriterLabel::try_from("same").unwrap(),
            LeasePolicy::bridge_default(),
        )
        .unwrap();
    a.append_stable(
        StableKey::try_from_canonical(b"one".to_vec()).unwrap(),
        Bytes::from_static(b"payload"),
        &call(),
    )
    .await
    .unwrap();
    let ref_key = "comb/v3/tenants/review/refs/log/parent/p0.json";
    let (bytes, _) = backend.get(ref_key).await.unwrap();
    let first: comb_core::RefValue = serde_json::from_slice(&bytes).unwrap();
    let second_key = StableKey::try_from_canonical(b"two".to_vec()).unwrap();
    a.append_stable(second_key.clone(), Bytes::from_static(b"payload2"), &call())
        .await
        .unwrap();
    let (bytes, version) = backend.get(ref_key).await.unwrap();
    let mut current: comb_core::RefValue = serde_json::from_slice(&bytes).unwrap();
    current.target = first.target;
    backend
        .put_update(
            ref_key,
            Some(&version),
            &serde_json::to_vec(&current).unwrap(),
        )
        .await
        .unwrap();
    let got = a
        .append_stable(second_key, Bytes::from_static(b"different"), &call())
        .await;
    assert!(got.is_err(), "old target hid committed key: {got:?}");
}

struct PausedRefRead {
    fail_read: std::sync::atomic::AtomicBool,
    pause_write: std::sync::atomic::AtomicBool,
    inner: MemoryBackend,
    pause: std::sync::atomic::AtomicBool,
    entered: std::sync::atomic::AtomicBool,
    resume: tokio::sync::Notify,
    updates: std::sync::atomic::AtomicUsize,
}

impl PausedRefRead {
    fn new() -> Self {
        Self {
            inner: MemoryBackend::new(),
            fail_read: false.into(),
            pause_write: false.into(),
            pause: false.into(),
            entered: false.into(),
            resume: tokio::sync::Notify::new(),
            updates: 0.into(),
        }
    }
}

#[async_trait::async_trait]
impl ObjectBackend for PausedRefRead {
    async fn put_create(
        &self,
        k: &str,
        b: &[u8],
    ) -> comb_core::error::Result<comb_object::Version> {
        if k.contains("/objects/") && self.pause_write.swap(false, Ordering::SeqCst) {
            self.entered.store(true, Ordering::SeqCst);
            self.resume.notified().await;
        }
        self.inner.put_create(k, b).await
    }
    async fn put_update(
        &self,
        k: &str,
        v: Option<&comb_object::Version>,
        b: &[u8],
    ) -> comb_core::error::Result<comb_object::Version> {
        let got = self.inner.put_update(k, v, b).await;
        if got.is_ok() {
            self.updates.fetch_add(1, Ordering::SeqCst);
        }
        got
    }
    async fn get(&self, k: &str) -> comb_core::error::Result<(Vec<u8>, comb_object::Version)> {
        self.inner.get(k).await
    }
    async fn get_limited(
        &self,
        k: &str,
        n: std::num::NonZeroU64,
    ) -> comb_core::error::Result<(Vec<u8>, comb_object::Version)> {
        if k.contains("/refs/") && self.fail_read.swap(false, Ordering::SeqCst) {
            return Err(comb_core::CoreError::BackendUnavailable(
                "paused-backend read failure".into(),
            ));
        }
        if k.contains("/refs/") && self.pause.swap(false, Ordering::SeqCst) {
            self.entered.store(true, Ordering::SeqCst);
            self.resume.notified().await;
        }
        self.inner.get_limited(k, n).await
    }
    async fn exists(&self, k: &str) -> comb_core::error::Result<bool> {
        self.inner.exists(k).await
    }
    async fn delete(&self, k: &str) -> comb_core::error::Result<()> {
        self.inner.delete(k).await
    }
    async fn list(&self, k: &str) -> comb_core::error::Result<Vec<comb_object::ObjectInfo>> {
        self.inner.list(k).await
    }
}

#[tokio::test]
async fn review_renewal_rechecks_clock_after_delayed_read() {
    let backend = Arc::new(PausedRefRead::new());
    let clock = Arc::new(JumpClock(std::sync::atomic::AtomicI64::new(
        chrono::Utc::now().timestamp(),
    )));
    let store = Arc::new(
        Store::new(
            backend.clone(),
            "review",
            DigestKey::from_bytes([9; 32]),
            None,
        )
        .with_clock(clock.clone()),
    );
    let feed = CompleteFeed::open(store, "paused".into(), &call())
        .await
        .unwrap();
    let policy = LeasePolicy {
        ttl: Duration::from_secs(2),
        renew_every: Duration::from_millis(100),
        clock_slack: Duration::ZERO,
        initial_acquire_budget: Duration::from_secs(2),
    };
    let writer = feed
        .writer_session(WriterLabel::try_from("paused").unwrap(), policy)
        .unwrap();
    writer.ready(&call()).await.unwrap();
    let before = backend.updates.load(Ordering::SeqCst);
    backend.pause.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !backend.entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    clock.0.fetch_add(3, Ordering::SeqCst);
    backend.resume.notify_one();
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(
        backend.updates.load(Ordering::SeqCst),
        before,
        "expired lease was rewritten using pre-read clock"
    );
    assert!(
        matches!(writer.state(), WriterState::Lost { .. }),
        "expired renewal did not lose ownership: {:?}",
        writer.state()
    );
}

#[tokio::test]
async fn review_hung_idle_renewal_loses_by_lease_deadline() {
    let backend = Arc::new(PausedRefRead::new());
    let store = Arc::new(Store::new(
        backend.clone(),
        "review",
        DigestKey::from_bytes([9; 32]),
        None,
    ));
    let feed = CompleteFeed::open(store, "paused".into(), &call())
        .await
        .unwrap();
    let policy = LeasePolicy {
        ttl: Duration::from_secs(1),
        renew_every: Duration::from_millis(100),
        clock_slack: Duration::ZERO,
        initial_acquire_budget: Duration::from_secs(2),
    };
    let writer = feed
        .writer_session(WriterLabel::try_from("paused").unwrap(), policy)
        .unwrap();
    writer.ready(&call()).await.unwrap();
    backend.pause.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !backend.entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(1300)).await;
    assert!(
        matches!(writer.state(), WriterState::Lost { .. }),
        "hung renewal stayed Active beyond lease: {:?}",
        writer.state()
    );
}

#[tokio::test]
async fn review_append_cannot_publish_after_lease_expires_during_upload() {
    let backend = Arc::new(PausedRefRead::new());
    let clock = Arc::new(JumpClock(std::sync::atomic::AtomicI64::new(
        chrono::Utc::now().timestamp(),
    )));
    let store = Arc::new(
        Store::new(
            backend.clone(),
            "review",
            DigestKey::from_bytes([9; 32]),
            None,
        )
        .with_clock(clock.clone()),
    );
    let feed = CompleteFeed::open(store, "paused".into(), &call())
        .await
        .unwrap();
    let policy = LeasePolicy {
        ttl: Duration::from_secs(2),
        renew_every: Duration::from_secs(1),
        clock_slack: Duration::ZERO,
        initial_acquire_budget: Duration::from_secs(2),
    };
    let writer = Arc::new(
        feed.writer_session(WriterLabel::try_from("paused").unwrap(), policy)
            .unwrap(),
    );
    writer.ready(&call()).await.unwrap();
    backend.pause_write.store(true, Ordering::SeqCst);
    let pending = {
        let writer = writer.clone();
        tokio::spawn(async move {
            writer
                .append_stable(
                    StableKey::try_from_canonical(b"one".to_vec()).unwrap(),
                    Bytes::from_static(b"payload"),
                    &call(),
                )
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        while !backend.entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    clock.0.fetch_add(3, Ordering::SeqCst);
    backend.resume.notify_one();
    let got = pending.await.unwrap();
    assert!(
        got.is_err(),
        "new append published after lease expired during upload: {got:?}"
    );
}

#[tokio::test]
async fn review_real_clock_claim_retry_returns_exact_original_ref() {
    let store = Store::new(
        Arc::new(MemoryBackend::new()),
        "review",
        DigestKey::from_bytes([9; 32]),
        None,
    );
    let op = store.mint_operation();
    let first = store.claim(op, "plain", "writer", 60, false).await.unwrap();
    let second = store.claim(op, "plain", "writer", 60, false).await.unwrap();
    assert_eq!(
        second.value, first.value,
        "claim retry changed original RefValue"
    );
}

#[tokio::test]
async fn review_v2_set_target_after_expired_lease_still_works() {
    let clock = Arc::new(JumpClock(std::sync::atomic::AtomicI64::new(
        chrono::Utc::now().timestamp(),
    )));
    let store = Store::new(
        Arc::new(MemoryBackend::new()),
        "review",
        DigestKey::from_bytes([9; 32]),
        None,
    )
    .with_clock(clock.clone());
    store
        .claim(store.mint_operation(), "plain", "writer", 2, false)
        .await
        .unwrap();
    clock.0.fetch_add(3, Ordering::SeqCst);
    let (digest, _) = store.put_blob(b"target".to_vec()).await.unwrap();
    let got = store
        .set_target_op(store.mint_operation(), "plain", digest, None)
        .await;
    assert!(
        got.is_ok(),
        "legacy unfenced set-target after lease expiry was rejected: {got:?}"
    );
}

#[tokio::test]
async fn review_lost_session_cannot_finish_suspended_append() {
    let backend = Arc::new(PausedRefRead::new());
    let store = Arc::new(Store::new(
        backend.clone(),
        "review",
        DigestKey::from_bytes([9; 32]),
        None,
    ));
    let feed = CompleteFeed::open(store, "paused".into(), &call())
        .await
        .unwrap();
    let policy = LeasePolicy {
        ttl: Duration::from_secs(3),
        renew_every: Duration::from_millis(100),
        clock_slack: Duration::ZERO,
        initial_acquire_budget: Duration::from_secs(3),
    };
    let writer = Arc::new(
        feed.writer_session(WriterLabel::try_from("paused").unwrap(), policy)
            .unwrap(),
    );
    writer.ready(&call()).await.unwrap();
    backend.pause_write.store(true, Ordering::SeqCst);
    let pending = {
        let writer = writer.clone();
        tokio::spawn(async move {
            writer
                .append_stable(
                    StableKey::try_from_canonical(b"one".to_vec()).unwrap(),
                    Bytes::from_static(b"payload"),
                    &call(),
                )
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        while !backend.entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    backend.fail_read.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !matches!(writer.state(), WriterState::Lost { .. }) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    backend.resume.notify_one();
    let got = pending.await.unwrap();
    assert!(
        got.is_err(),
        "Lost session issued a new publication after resuming upload: {got:?}"
    );
}

#[tokio::test]
async fn review_lost_session_terminates_blocked_append_without_resume() {
    let backend = Arc::new(PausedRefRead::new());
    let store = Arc::new(Store::new(
        backend.clone(),
        "review",
        DigestKey::from_bytes([9; 32]),
        None,
    ));
    let feed = CompleteFeed::open(store, "paused".into(), &call())
        .await
        .unwrap();
    let policy = LeasePolicy {
        ttl: Duration::from_secs(3),
        renew_every: Duration::from_millis(100),
        clock_slack: Duration::ZERO,
        initial_acquire_budget: Duration::from_secs(3),
    };
    let writer = Arc::new(
        feed.writer_session(WriterLabel::try_from("paused").unwrap(), policy)
            .unwrap(),
    );
    writer.ready(&call()).await.unwrap();
    backend.pause_write.store(true, Ordering::SeqCst);
    let pending = {
        let writer = writer.clone();
        tokio::spawn(async move {
            writer
                .append_stable(
                    StableKey::try_from_canonical(b"one".to_vec()).unwrap(),
                    Bytes::from_static(b"payload"),
                    &call(),
                )
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        while !backend.entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    backend.fail_read.store(true, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(1), async {
        while !matches!(writer.state(), WriterState::Lost { .. }) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let got = tokio::time::timeout(Duration::from_millis(400), pending).await;
    assert!(
        matches!(got, Ok(Ok(Err(_)))),
        "Lost append waited for blocked upload: {got:?}"
    );
}
