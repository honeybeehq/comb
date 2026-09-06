//! Stdio transport: frame bounds, concurrent requests, process binary.

#[path = "../src/bridge/mod.rs"]
mod bridge;

use bridge::handler::Bridge;
use bridge::limits::Limits;
use bridge::stdio;
use comb_core::DigestKey;
use comb_object::memory::MemoryBackend;
use combctl::config::{self, BackendConfig, Config};
use combctl::store::Store;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::UnixStream;
use tokio::process::Command;

fn unix_pair() -> (OwnedReadHalf, OwnedWriteHalf, OwnedReadHalf, OwnedWriteHalf) {
    let (a, b) = std::os::unix::net::UnixStream::pair().unwrap();
    a.set_nonblocking(true).unwrap();
    b.set_nonblocking(true).unwrap();
    let client = UnixStream::from_std(a).unwrap();
    let server = UnixStream::from_std(b).unwrap();
    let (client_read, client_write) = client.into_split();
    let (server_read, server_write) = server.into_split();
    (client_read, client_write, server_read, server_write)
}

fn mem_bridge(limits: Limits) -> Arc<Bridge> {
    Arc::new(Bridge::new(
        Store::new(
            Arc::new(MemoryBackend::new()),
            "org_t",
            DigestKey::from_bytes([5u8; 32]),
            None,
        ),
        "comb-bridge".into(),
        60,
        limits,
    ))
}

async fn write_line<W: AsyncWriteExt + Unpin>(w: &mut W, v: Value) {
    w.write_all(serde_json::to_string(&v).unwrap().as_bytes())
        .await
        .unwrap();
    w.write_all(b"\n").await.unwrap();
    w.flush().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stdio_follow_does_not_block_append() {
    let bridge = mem_bridge(Limits::default());
    let (client_read, mut client_write, server_read, server_write) = unix_pair();
    let server_task =
        tokio::spawn(async move { stdio::run(server_read, server_write, bridge).await.unwrap() });
    let mut lines = BufReader::new(client_read).lines();

    write_line(
        &mut client_write,
        json!({"v":1,"id":"f","op":"follow","log":"doc1","cursor":"1","timeout_ms":2000}),
    )
    .await;
    write_line(
        &mut client_write,
        json!({
            "v":1,"id":"a","op":"append","log":"doc1",
            "idempotency_key":"cafebabe","payload_hex":"c0ffee"
        }),
    )
    .await;

    let mut got = Vec::new();
    for _ in 0..2 {
        let line = tokio::time::timeout(Duration::from_secs(3), lines.next_line())
            .await
            .expect("response timed out")
            .unwrap()
            .unwrap();
        got.push(serde_json::from_str::<Value>(&line).unwrap());
    }
    client_write.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), server_task)
        .await
        .expect("stdio server did not close after input shutdown")
        .unwrap();

    let append = got
        .iter()
        .find(|v| v["id"] == "a")
        .expect("append response");
    let follow = got
        .iter()
        .find(|v| v["id"] == "f")
        .expect("follow response");
    assert_eq!(append["ok"], false, "{append}");
    assert_eq!(append["error"]["capability"], "durable_idempotency");
    assert_eq!(follow["ok"], false, "{follow}");
    assert_eq!(follow["error"]["capability"], "bounded_memory_read");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_frame_then_valid_request_in_same_write() {
    let mut limits = Limits::default();
    // Encoded request/response budget. Keep it large enough for a hello
    // response, small enough that the padded request is rejected.
    limits.max_frame_bytes = 1024;
    let bridge = mem_bridge(limits);
    let (client_read, mut client_write, server_read, server_write) = unix_pair();
    let server_task =
        tokio::spawn(async move { stdio::run(server_read, server_write, bridge).await.unwrap() });
    let mut lines = BufReader::new(client_read).lines();

    let mut huge = Vec::from(&b"{\"v\":1,\"id\":\"big\",\"op\":\"hello\",\"pad\":\""[..]);
    huge.extend(std::iter::repeat(b'a').take(4000));
    huge.extend_from_slice(br#""}"#);
    huge.push(b'\n');
    huge.extend_from_slice(br#"{"v":1,"id":"h","op":"hello"}"#);
    huge.push(b'\n');
    client_write.write_all(&huge).await.unwrap();
    client_write.flush().await.unwrap();

    let mut got = Vec::new();
    for _ in 0..2 {
        let line = tokio::time::timeout(Duration::from_secs(2), lines.next_line())
            .await
            .expect("response timed out")
            .unwrap()
            .unwrap();
        got.push(serde_json::from_str::<Value>(&line).unwrap());
    }
    client_write.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), server_task)
        .await
        .expect("stdio server did not close after input shutdown")
        .unwrap();

    let over = got
        .iter()
        .find(|v| v["id"] == "big")
        .expect("oversized response");
    let hello = got.iter().find(|v| v["id"] == "h").expect("hello response");
    assert_eq!(over["ok"], false);
    assert_eq!(over["error"]["code"], "invalid_request");
    assert_eq!(hello["ok"], true, "{hello}");
    assert_eq!(hello["op"], "hello");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stdio_stdout_is_protocol_only() {
    let bridge = mem_bridge(Limits::default());
    let (client_read, mut client_write, server_read, server_write) = unix_pair();
    let server_task =
        tokio::spawn(async move { stdio::run(server_read, server_write, bridge).await.unwrap() });
    let mut lines = BufReader::new(client_read).lines();
    write_line(&mut client_write, json!({"v":1,"id":"h","op":"hello"})).await;
    let line = lines.next_line().await.unwrap().unwrap();
    let v: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(v["v"], 1);
    assert_eq!(v["id"], "h");
    assert!(line.len() <= Limits::default().max_frame_bytes);
    client_write.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), server_task)
        .await
        .expect("stdio server did not close after input shutdown")
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_loads_local_config_without_printing_keys() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("objects");
    let cfgdir = tmp.path().join("cfg");
    let digest_key = hex::encode([9u8; 32]);
    config::save(
        &cfgdir,
        &Config {
            tenant: "org_bridge_test".into(),
            digest_key: digest_key.clone(),
            backend: BackendConfig::Local {
                root: root.display().to_string(),
            },
        },
    )
    .unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_comb-bridge"))
        .arg("--dir")
        .arg(&cfgdir)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn comb-bridge");

    let mut stdin = child.stdin.take().unwrap();
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let mut lines = BufReader::new(stdout).lines();

    stdin
        .write_all(br#"{"v":1,"id":"h","op":"hello"}"#)
        .await
        .unwrap();
    stdin.write_all(b"\n").await.unwrap();
    stdin
        .write_all(br#"{"v":1,"id":"a","op":"append","log":"doc1","idempotency_key":"cafebabe","payload_hex":"cafebabe"}"#)
        .await
        .unwrap();
    stdin.write_all(b"\n").await.unwrap();
    stdin.flush().await.unwrap();

    let mut got = Vec::new();
    for _ in 0..2 {
        let line = tokio::time::timeout(Duration::from_secs(10), lines.next_line())
            .await
            .expect("bridge response timed out")
            .unwrap()
            .unwrap();
        got.push(serde_json::from_str::<Value>(&line).unwrap());
    }
    let hello = got.iter().find(|v| v["id"] == "h").expect("hello response");
    let append = got
        .iter()
        .find(|v| v["id"] == "a")
        .expect("append response");
    assert_eq!(hello["ok"], true, "{hello}");
    assert_eq!(append["ok"], false, "{append}");
    assert_eq!(append["error"]["capability"], "durable_idempotency");

    drop(stdin);
    let err = tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("bridge did not exit")
        .unwrap();
    assert!(err.success(), "bridge exit {err}");

    let mut err_buf = String::new();
    let mut err_lines = BufReader::new(stderr);
    tokio::io::AsyncReadExt::read_to_string(&mut err_lines, &mut err_buf)
        .await
        .unwrap();
    assert!(
        !err_buf.contains(&digest_key),
        "stderr leaked digest key: {err_buf}"
    );
    for line in err_buf.lines() {
        if line.trim().is_empty() {
            continue;
        }
        assert!(
            serde_json::from_str::<Value>(line).is_err(),
            "protocol frame on stderr: {line}"
        );
    }
}

#[tokio::test]
async fn cancelling_transport_closes_its_owned_output() {
    let (client_read, mut client_write, server_read, server_write) = unix_pair();
    let bridge = mem_bridge(Limits::default());
    let server = tokio::spawn(stdio::run(server_read, server_write, bridge));
    let mut lines = BufReader::new(client_read).lines();
    write_line(&mut client_write, json!({"v":1,"id":"ready","op":"hello"})).await;
    tokio::time::timeout(Duration::from_secs(3), lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
    let closed = tokio::time::timeout(Duration::from_secs(1), lines.next_line()).await;
    // Clean up even if the implementation detached its reader and writer.
    // A correctly cancelled peer may already have disconnected this socket.
    let _ = client_write.shutdown().await;
    assert!(
        closed.is_ok(),
        "cancelling transport left its stdout writer alive"
    );
    assert!(closed.unwrap().unwrap().is_none());
}

#[tokio::test]
async fn oversized_error_preserves_a_valid_request_id() {
    let limits = Limits {
        max_frame_bytes: 1024,
        ..Limits::default()
    };
    let request = json!({"v":1,"id":"known-request","op":"x".repeat(940)});
    assert!(serde_json::to_vec(&request).unwrap().len() <= limits.max_frame_bytes);
    let bridge = mem_bridge(limits);
    let (client_read, mut client_write, server_read, server_write) = unix_pair();
    let server = tokio::spawn(stdio::run(server_read, server_write, bridge));
    let mut lines = BufReader::new(client_read).lines();
    write_line(&mut client_write, request).await;
    let line = tokio::time::timeout(Duration::from_secs(3), lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    client_write.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(line.len() <= 1024);
    let response: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(response["error"]["code"], "invalid_request");
    assert_eq!(response["id"], "known-request");
}
