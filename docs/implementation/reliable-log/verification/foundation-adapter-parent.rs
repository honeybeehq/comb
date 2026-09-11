//! Root bridge lifecycle regressions. Production comes from a fixed archive.
#[path = "../src/bridge/mod.rs"]
mod bridge;
use bridge::{handler::Bridge, limits::Limits, stdio};
use comb_core::{DigestKey, Envelope, RefValue};
use comb_object::{
    failpoint::{FailAction, FailMethod, FailRule, FailpointBackend},
    memory::MemoryBackend,
    ObjectBackend, ObjectInfo, Version,
};
use combctl::store::Store;
use serde_json::{json, Value};
use std::{
    num::NonZeroU64,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

fn broker(backend: Arc<dyn ObjectBackend>) -> Arc<Bridge> {
    Arc::new(
        Bridge::new(
            Store::new(backend, "parent", DigestKey::from_bytes([77; 32]), None),
            "parent".into(),
            30,
            Limits::default(),
        )
        .unwrap(),
    )
}
async fn rpc(b: &Bridge, value: Value) -> Value {
    serde_json::to_value(b.handle_frame(&serde_json::to_vec(&value).unwrap()).await).unwrap()
}
fn append(log: &str, key: &str) -> Value {
    json!({"v":1,"id":"append","op":"append","log":log,"idempotency_key":key,"payload_hex":"c0ffee"})
}
async fn raw_ref(backend: &dyn ObjectBackend, log: &str) -> RefValue {
    let (bytes, _) = backend
        .get(&format!("comb/v3/tenants/parent/refs/log/{log}/p0.json"))
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

struct Probe {
    inner: MemoryBackend,
    release_faults: AtomicBool,
    block_chunks: AtomicBool,
    upload_started: tokio::sync::Notify,
}
impl Probe {
    fn new() -> Self {
        Self {
            inner: MemoryBackend::new(),
            release_faults: AtomicBool::new(false),
            block_chunks: AtomicBool::new(false),
            upload_started: tokio::sync::Notify::new(),
        }
    }
}
#[async_trait::async_trait]
impl ObjectBackend for Probe {
    async fn put_create(&self, key: &str, body: &[u8]) -> comb_core::error::Result<Version> {
        if self.block_chunks.load(Ordering::SeqCst)
            && Envelope::decode(body, &DigestKey::from_bytes([77; 32]))
                .is_ok_and(|e| e.meta.schema == "comb.log.chunk/v1")
        {
            self.upload_started.notify_one();
            std::future::pending::<()>().await;
        }
        self.inner.put_create(key, body).await
    }
    async fn put_update(
        &self,
        key: &str,
        expected: Option<&Version>,
        body: &[u8],
    ) -> comb_core::error::Result<Version> {
        if self.release_faults.load(Ordering::SeqCst) {
            if key.ends_with("/refs/log/a/p0.json") {
                return Err(comb_core::error::CoreError::BackendUnavailable(
                    "parent release A failure".into(),
                ));
            }
            if key.ends_with("/refs/log/b/p0.json") {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
        self.inner.put_update(key, expected, body).await
    }
    async fn get(&self, key: &str) -> comb_core::error::Result<(Vec<u8>, Version)> {
        self.inner.get(key).await
    }
    async fn get_limited(
        &self,
        key: &str,
        limit: NonZeroU64,
    ) -> comb_core::error::Result<(Vec<u8>, Version)> {
        self.inner.get_limited(key, limit).await
    }
    async fn exists(&self, key: &str) -> comb_core::error::Result<bool> {
        self.inner.exists(key).await
    }
    async fn delete(&self, key: &str) -> comb_core::error::Result<()> {
        self.inner.delete(key).await
    }
    async fn list(&self, prefix: &str) -> comb_core::error::Result<Vec<ObjectInfo>> {
        self.inner.list(prefix).await
    }
}

#[tokio::test]
async fn parent_one_failed_release_does_not_abort_a_healthy_sibling() {
    let backend = Arc::new(Probe::new());
    let b = broker(backend.clone());
    for log in ["a", "b"] {
        assert_eq!(rpc(&b, append(log, "01")).await["ok"], true);
    }
    backend.release_faults.store(true, Ordering::SeqCst);
    assert!(b.close().await.is_err(), "A failure must still be reported");
    assert!(
        raw_ref(backend.as_ref(), "b").await.lease.is_none(),
        "healthy B release was aborted by A failure"
    );
}

#[tokio::test]
async fn parent_failed_opens_do_not_exhaust_log_capacity() {
    let backend = Arc::new(FailpointBackend::new(Arc::new(MemoryBackend::new())));
    backend.arm(FailRule {
        method: FailMethod::GetLimited,
        key_contains: "/refs/".into(),
        successes_before_fire: 0,
        fires: 256,
        action: FailAction::DropRequest,
    });
    let b = broker(backend);
    for i in 0..256 {
        let r = rpc(
            &b,
            json!({"v":1,"id":"head","op":"head","log":format!("failed-{i}")}),
        )
        .await;
        assert_eq!(r["error"]["code"], "backend_unavailable", "{r}");
    }
    let r = rpc(&b, json!({"v":1,"id":"head","op":"head","log":"healthy"})).await;
    assert_eq!(
        r["ok"], true,
        "failed initialization retained registry capacity: {r}"
    );
    b.close().await.unwrap();
}

#[tokio::test]
async fn parent_eof_cancels_stalled_data_but_attempts_healthy_release() {
    let backend = Arc::new(Probe::new());
    let b = broker(backend.clone());
    assert_eq!(rpc(&b, append("doc", "01")).await["ok"], true);
    backend.block_chunks.store(true, Ordering::SeqCst);
    let (mut client_input, server_input) = tokio::io::duplex(4096);
    let (server_output, _client_output) = tokio::io::duplex(4096);
    let mut server = tokio::spawn(stdio::run(server_input, server_output, b));
    client_input
        .write_all(&(append("doc", "02").to_string() + "\n").into_bytes())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), backend.upload_started.notified())
        .await
        .unwrap();
    client_input.shutdown().await.unwrap();
    let done = tokio::time::timeout(Duration::from_secs(6), &mut server).await;
    if done.is_err() {
        server.abort();
        panic!("EOF did not terminate within its bounded budget");
    }
    done.unwrap().unwrap().ok();
    assert!(
        raw_ref(backend.as_ref(), "doc").await.lease.is_none(),
        "EOF skipped release even though only data upload was stalled"
    );
}

#[tokio::test]
async fn parent_steady_output_can_resume_after_five_seconds() {
    let b = broker(Arc::new(MemoryBackend::new()));
    let (mut client_input, server_input) = tokio::io::duplex(4096);
    let (server_output, client_output) = tokio::io::duplex(1);
    let mut server = tokio::spawn(stdio::run(server_input, server_output, b));
    client_input
        .write_all(b"{\"v\":1,\"id\":\"hello\",\"op\":\"hello\"}\n")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(
        !server.is_finished(),
        "steady-state output reused the 4-second shutdown timer"
    );
    let mut line = String::new();
    tokio::time::timeout(
        Duration::from_secs(2),
        BufReader::new(client_output).read_line(&mut line),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(serde_json::from_str::<Value>(&line).unwrap()["ok"], true);
    client_input.shutdown().await.unwrap();
    let done = tokio::time::timeout(Duration::from_secs(6), &mut server).await;
    if done.is_err() {
        server.abort();
        panic!("shutdown did not finish");
    }
    done.unwrap().unwrap().unwrap();
}
