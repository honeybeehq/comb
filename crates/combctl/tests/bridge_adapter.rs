//! Adapter verification uses the real V3 feed; backend wrappers only inject faults.
#[path = "../src/bridge/mod.rs"]
mod bridge;

use bridge::errors::{append_error, lease_error, open_error, read_error};
use bridge::handler::Bridge;
use bridge::limits::Limits;
use comb_core::{operation::Clock, DigestKey};
use comb_object::{
    failpoint::{FailAction, FailMethod, FailRule, FailpointBackend},
    memory::MemoryBackend,
    ObjectBackend,
};
use combctl::{
    log::{
        CallContext, Cursor, LeaseError, LogIntegrityError, OpenLogError, Position, ReadError,
        SessionLoss, StableAppendError,
    },
    store::Store,
};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

fn store(backend: Arc<dyn ObjectBackend>) -> Store {
    Store::new(
        backend,
        "org_adapter",
        DigestKey::from_bytes([61; 32]),
        None,
    )
}
fn broker(backend: Arc<dyn ObjectBackend>) -> Bridge {
    Bridge::new(
        store(backend),
        "same-diagnostic-label".into(),
        30,
        Limits::default(),
    )
    .unwrap()
}
async fn rpc(b: &Bridge, value: Value) -> Value {
    serde_json::to_value(b.handle_frame(&serde_json::to_vec(&value).unwrap()).await).unwrap()
}
fn append(id: &str, key: &str, payload: &str) -> Value {
    json!({"v":1,"id":id,"op":"append","log":"doc","idempotency_key":key,"payload_hex":payload})
}
async fn head(b: &Bridge) -> Value {
    rpc(b, json!({"v":1,"id":"h","op":"head","log":"doc"})).await
}
fn logical(mut value: Value) -> Value {
    value.as_object_mut().unwrap().remove("id");
    value
}

#[tokio::test]
async fn concurrent_first_access_uses_one_publisher_and_original_receipts() {
    let backend = Arc::new(MemoryBackend::new());
    let b = Arc::new(broker(backend.clone()));
    let mut tasks = tokio::task::JoinSet::new();
    for i in 1..=12u8 {
        let b = b.clone();
        tasks.spawn(async move {
            let response = rpc(
                &b,
                append(
                    &format!("a{i}"),
                    &hex::encode([i]),
                    &hex::encode([0, i, 255]),
                ),
            )
            .await;
            assert_eq!(response["ok"], true, "{response}");
            (i, response)
        });
    }
    let mut positions = Vec::new();
    while let Some(result) = tasks.join_next().await {
        let (i, receipt) = result.unwrap();
        positions.push(receipt["first"].as_str().unwrap().parse::<u64>().unwrap());
        let retry = rpc(
            &b,
            append("retry", &hex::encode([i]), &hex::encode([0, i, 255])),
        )
        .await;
        assert_eq!(logical(retry), logical(receipt));
    }
    positions.sort();
    assert_eq!(positions, (1..=12).collect::<Vec<_>>());
    assert_eq!(head(&b).await["head"], "12");
    let reference = raw_head(backend.as_ref()).await;
    assert_eq!(
        reference.epoch, 1,
        "competing local sessions acquired multiple epochs"
    );
    assert_eq!(reference.lease.as_ref().unwrap().writer.len(), 32);
    let conflict = rpc(&b, append("conflict", "01", "ff")).await;
    assert_eq!(conflict["error"]["code"], "conflict");
    assert_eq!(head(&b).await["head"], "12");
    b.close().await.unwrap();
}

#[tokio::test]
async fn read_and_follow_are_bounded_and_zero_wait_is_immediate() {
    let b = broker(Arc::new(MemoryBackend::new()));
    let empty = rpc(
        &b,
        json!({"v":1,"id":"f","op":"follow","log":"doc","cursor":"1","timeout_ms":0}),
    )
    .await;
    assert_eq!(empty["ok"], true, "{empty}");
    assert_eq!(empty["timed_out"], true);
    for (key, payload) in [("01", "00ff00"), ("02", "deadbeef"), ("03", "80")] {
        assert_eq!(rpc(&b, append("a", key, payload)).await["ok"], true);
    }
    let page = rpc(
        &b,
        json!({"v":1,"id":"r","op":"read","log":"doc","cursor":"1","max_events":3,"max_bytes":3}),
    )
    .await;
    assert_eq!(page["events"].as_array().unwrap().len(), 1);
    assert_eq!(page["events"][0]["payload_hex"], "00ff00");
    assert_eq!(page["next_cursor"], "2");
    assert_eq!(page["at_head"], false);
    let count = rpc(
        &b,
        json!({"v":1,"id":"c","op":"read","log":"doc","cursor":"2","max_events":1,"max_bytes":32}),
    )
    .await;
    assert_eq!(count["events"].as_array().unwrap().len(), 1);
    assert_eq!(count["next_cursor"], "3");
    let too_large = rpc(
        &b,
        json!({"v":1,"id":"small","op":"read","log":"doc","cursor":"2","max_bytes":3}),
    )
    .await;
    assert_eq!(too_large["error"]["code"], "event_too_large");
    assert_eq!(too_large["error"]["seq"], "2");
    assert_eq!(too_large["error"]["event_bytes"], "4");
    assert_eq!(too_large["error"]["max_bytes"], "3");
    let poll = rpc(
        &b,
        json!({"v":1,"id":"poll","op":"follow","log":"doc","cursor":"2","timeout_ms":0}),
    )
    .await;
    assert_eq!(poll["events"][0]["seq"], "2");
    assert_eq!(poll["timed_out"], false);
    assert_eq!(poll["at_head"], true);
    let invalid = rpc(
        &b,
        json!({"v":1,"id":"bad","op":"read","log":"doc","cursor":"5"}),
    )
    .await;
    assert_eq!(invalid["error"]["code"], "invalid_request");
    b.close().await.unwrap();
}

#[tokio::test]
async fn lost_final_cas_reply_is_unavailable_and_retry_recovers_the_receipt() {
    let backend = Arc::new(FailpointBackend::new(Arc::new(MemoryBackend::new())));
    let b = broker(backend.clone());
    // Warm the same session so the next ref update is the append publication,
    // not initial acquisition. The failpoint drops only a successful CAS reply.
    assert_eq!(rpc(&b, append("warm", "01", "00")).await["ok"], true);
    backend.arm(FailRule {
        method: FailMethod::PutUpdate,
        key_contains: "comb/v3/tenants/org_adapter/refs/log/doc/p0.json".into(),
        successes_before_fire: 0,
        fires: 1,
        action: FailAction::DropResponse,
    });
    let ambiguous = rpc(&b, append("lost", "02", "00ff80")).await;
    assert_eq!(backend.injected.load(Ordering::Relaxed), 1);
    assert_eq!(
        ambiguous["error"]["code"], "backend_unavailable",
        "{ambiguous}"
    );
    let fresh = broker(backend.clone());
    let retry = rpc(&fresh, append("retry", "02", "00ff80")).await;
    assert_eq!(retry["ok"], true, "{retry}");
    assert_eq!(retry["first"], "2");
    assert_eq!(retry["last"], "2");
    assert_eq!(head(&fresh).await["head"], "2");
    fresh.close().await.unwrap();
    b.close().await.unwrap();
}

struct JumpClock(AtomicI64);
impl Clock for JumpClock {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::from_timestamp_millis(self.0.load(Ordering::SeqCst)).unwrap()
    }
}
#[tokio::test]
async fn retained_retry_survives_fresh_broker_after_eight_days_and_loss_stays_lost() {
    let backend = Arc::new(MemoryBackend::new());
    let clock = Arc::new(JumpClock(AtomicI64::new(
        chrono::Utc::now().timestamp_millis(),
    )));
    let make = || {
        Bridge::new(
            store(backend.clone()).with_clock(clock.clone()),
            "label".into(),
            30,
            Limits::default(),
        )
        .unwrap()
    };
    let old = make();
    let receipt = rpc(&old, append("a", "01", "ff00")).await;
    assert_eq!(receipt["ok"], true, "{receipt}");
    clock.0.fetch_add(8 * 24 * 60 * 60 * 1000, Ordering::SeqCst);
    let lost = rpc(&old, append("new", "02", "55")).await;
    assert_eq!(lost["error"]["code"], "reacquire_required", "{lost}");
    let fresh = make();
    let retry = rpc(&fresh, append("fresh", "01", "ff00")).await;
    assert_eq!(logical(receipt), logical(retry));
    let old_retry = rpc(&old, append("old-retry", "01", "ff00")).await;
    assert_eq!(old_retry["ok"], true, "{old_retry}");
    assert_eq!(
        rpc(&old, append("still-lost", "03", "56")).await["error"]["code"],
        "reacquire_required"
    );
    fresh.close().await.unwrap();
    old.close().await.unwrap();
}

#[tokio::test]
async fn deadlines_and_cancellation_preserve_ids_and_do_not_publish() {
    let b = broker(Arc::new(MemoryBackend::new()));
    let request = serde_json::to_vec(&append("deadline", "01", "ff")).unwrap();
    let call = CallContext::new(Instant::now(), CancellationToken::new());
    let response =
        serde_json::to_value(b.handle_frame_with_context(&request, &call).await).unwrap();
    assert_eq!(response["id"], "deadline");
    assert_eq!(response["error"]["code"], "deadline_exceeded");
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let call = CallContext::new(Instant::now() + Duration::from_secs(2), cancellation);
    let response =
        serde_json::to_value(b.handle_frame_with_context(&request, &call).await).unwrap();
    assert_eq!(response["error"]["code"], "cancelled");
    assert_eq!(head(&b).await["head"], "0");
    let wait = serde_json::to_vec(
        &json!({"v":1,"id":"wait","op":"follow","log":"doc","cursor":"1","timeout_ms":30000}),
    )
    .unwrap();
    let call = CallContext::new(
        Instant::now() + Duration::from_millis(50),
        CancellationToken::new(),
    );
    let response = serde_json::to_value(b.handle_frame_with_context(&wait, &call).await).unwrap();
    assert_eq!(response["id"], "wait");
    assert_eq!(response["error"]["code"], "deadline_exceeded");
    b.close().await.unwrap();
}

#[tokio::test]
async fn closing_read_only_broker_does_not_acquire_and_closing_writer_releases() {
    let backend = Arc::new(MemoryBackend::new());
    let b = broker(backend.clone());
    assert_eq!(head(&b).await["head"], "0");
    b.close().await.unwrap();
    assert!(backend.list("comb/v3/").await.unwrap().is_empty());
    let writer = broker(backend.clone());
    assert_eq!(rpc(&writer, append("a", "01", "ff")).await["ok"], true);
    writer.close().await.unwrap();
    let reference = raw_head(backend.as_ref()).await;
    assert!(reference.lease.is_none());
    let next = broker(backend);
    assert_eq!(rpc(&next, append("b", "02", "fe")).await["ok"], true);
    next.close().await.unwrap();
}

#[test]
fn typed_error_mapping_keeps_integrity_unavailable_deadline_and_cancel_distinct() {
    fn code(response: bridge::protocol::Response) -> Value {
        serde_json::to_value(response).unwrap()["error"]["code"].clone()
    }
    assert_eq!(
        code(open_error(
            "x".into(),
            OpenLogError::Integrity(LogIntegrityError("private details".into()))
        )),
        "integrity"
    );
    assert_eq!(
        code(read_error(
            "x".into(),
            ReadError::Unavailable { operation: "get" }
        )),
        "backend_unavailable"
    );
    assert_eq!(
        code(append_error(
            "x".into(),
            StableAppendError::DeadlineExceeded
        )),
        "deadline_exceeded"
    );
    assert_eq!(code(lease_error("x", LeaseError::Cancelled)), "cancelled");
    for cause in [
        SessionLoss::OwnerChanged,
        SessionLoss::LeaseExpired,
        SessionLoss::RenewalUncertain,
        SessionLoss::Fenced { live_epoch: 2 },
    ] {
        assert_eq!(
            code(lease_error("x", LeaseError::ReacquireRequired { cause })),
            "reacquire_required"
        );
    }
    assert_eq!(
        code(lease_error(
            "x",
            LeaseError::Fenced {
                session_epoch: 1,
                live_epoch: 2
            }
        )),
        "fenced"
    );
    let response = read_error(
        "trim".into(),
        ReadError::Trimmed {
            requested: Cursor::first(0),
            resume_at: Cursor {
                partition: 0,
                next_seq: 7,
            },
        },
    );
    assert_eq!(
        serde_json::to_value(response).unwrap()["error"]["resume_at"],
        "7"
    );
    let response = read_error(
        "large".into(),
        ReadError::EventTooLarge {
            cursor: Cursor::first(0),
            position: Position {
                partition: 0,
                seq: 1,
            },
            event_bytes: 99,
            max_bytes: 8,
        },
    );
    assert_eq!(
        serde_json::to_value(response).unwrap()["error"]["event_bytes"],
        "99"
    );
}

struct BoundedProbe {
    inner: MemoryBackend,
    bounded_reads: std::sync::atomic::AtomicUsize,
    stall: std::sync::atomic::AtomicBool,
    started: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl ObjectBackend for BoundedProbe {
    async fn put_create(
        &self,
        key: &str,
        body: &[u8],
    ) -> comb_core::error::Result<comb_object::Version> {
        self.inner.put_create(key, body).await
    }
    async fn put_update(
        &self,
        key: &str,
        expected: Option<&comb_object::Version>,
        body: &[u8],
    ) -> comb_core::error::Result<comb_object::Version> {
        self.inner.put_update(key, expected, body).await
    }
    async fn get(&self, _: &str) -> comb_core::error::Result<(Vec<u8>, comb_object::Version)> {
        panic!("adapter reached an unbounded backend read")
    }
    async fn get_limited(
        &self,
        key: &str,
        limit: std::num::NonZeroU64,
    ) -> comb_core::error::Result<(Vec<u8>, comb_object::Version)> {
        self.bounded_reads.fetch_add(1, Ordering::Relaxed);
        if self.stall.load(Ordering::SeqCst) {
            self.started.notify_one();
            std::future::pending::<()>().await;
        }
        self.inner.get_limited(key, limit).await
    }
    async fn exists(&self, key: &str) -> comb_core::error::Result<bool> {
        self.inner.exists(key).await
    }
    async fn delete(&self, key: &str) -> comb_core::error::Result<()> {
        self.inner.delete(key).await
    }
    async fn list(&self, prefix: &str) -> comb_core::error::Result<Vec<comb_object::ObjectInfo>> {
        self.inner.list(prefix).await
    }
}

#[tokio::test]
async fn actual_backend_reads_are_bounded_and_pending_io_is_cancellable() {
    let backend = Arc::new(BoundedProbe {
        inner: MemoryBackend::new(),
        bounded_reads: 0.into(),
        stall: false.into(),
        started: tokio::sync::Notify::new(),
    });
    let b = Arc::new(broker(backend.clone()));
    let payload = "ff".repeat(Limits::default().max_append_bytes);
    assert_eq!(rpc(&b, append("a", "01", &payload)).await["ok"], true);
    let page = rpc(
        &b,
        json!({"v":1,"id":"max-page","op":"read","log":"doc","cursor":"1"}),
    )
    .await;
    assert_eq!(page["events"][0]["payload_hex"], payload);
    assert!(serde_json::to_vec(&page).unwrap().len() <= Limits::default().max_frame_bytes);
    assert!(backend.bounded_reads.load(Ordering::Relaxed) > 0);
    backend.stall.store(true, Ordering::SeqCst);
    let cancellation = CancellationToken::new();
    let call = CallContext::new(
        Instant::now() + Duration::from_secs(10),
        cancellation.clone(),
    );
    let worker = b.clone();
    let pending = tokio::spawn(async move {
        let bytes =
            serde_json::to_vec(&json!({"v":1,"id":"blocked","op":"head","log":"doc"})).unwrap();
        serde_json::to_value(worker.handle_frame_with_context(&bytes, &call).await).unwrap()
    });
    tokio::time::timeout(Duration::from_secs(2), backend.started.notified())
        .await
        .unwrap();
    cancellation.cancel();
    let response = tokio::time::timeout(Duration::from_secs(2), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response["id"], "blocked");
    assert_eq!(response["error"]["code"], "cancelled");
    backend.stall.store(false, Ordering::SeqCst);
    b.close().await.unwrap();
}

async fn raw_head(backend: &dyn ObjectBackend) -> comb_core::RefValue {
    let (bytes, _) = backend
        .get_limited(
            "comb/v3/tenants/org_adapter/refs/log/doc/p0.json",
            std::num::NonZeroU64::new(64 * 1024).unwrap(),
        )
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}
