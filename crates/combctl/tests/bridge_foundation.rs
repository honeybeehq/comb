//! Foundation host contract checks against the real stdio process.

use combctl::config::{self, BackendConfig, Config};
use serde_json::json;
use std::process::Stdio;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};

fn process(stdout: Stdio) -> (tempfile::TempDir, Child) {
    let directory = tempfile::tempdir().unwrap();
    let config_dir = directory.path().join("config");
    config::save(
        &config_dir,
        &Config {
            tenant: "org_foundation_process".into(),
            digest_key: hex::encode([19; 32]),
            backend: BackendConfig::Local {
                root: directory.path().join("objects").display().to_string(),
            },
        },
    )
    .unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_comb-bridge"))
        .arg("--dir")
        .arg(config_dir)
        .stdin(Stdio::piped())
        .stdout(stdout)
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    (directory, child)
}

#[tokio::test]
async fn invalid_request_id_cannot_expand_error_past_wire_limit() {
    let (_directory, mut child) = process(Stdio::piped());
    let frame_limit = 1_048_576;
    let frame = json!({"v":1, "id":"i".repeat(frame_limit - 64), "op":"hello"});
    let mut encoded = serde_json::to_vec(&frame).unwrap();
    assert!(encoded.len() <= frame_limit);
    encoded.push(b'\n');
    let mut stdin = child.stdin.take().unwrap();
    let input = tokio::spawn(async move { stdin.write_all(&encoded).await.unwrap() });
    let output = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
        .await
        .expect("bridge did not finish oversized-ID request")
        .unwrap();
    input.await.unwrap();
    assert!(output.status.success());
    assert_eq!(output.stdout.last(), Some(&b'\n'));
    assert!(
        output.stdout.len() - 1 <= frame_limit,
        "error response is {} bytes, above the {frame_limit}-byte wire limit",
        output.stdout.len() - 1
    );
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["ok"], false);
    assert_eq!(response["error"]["code"], "invalid_request");
}

#[tokio::test]
async fn host_closing_stdout_ends_process_while_stdin_is_open() {
    let (_directory, mut child) = process(Stdio::piped());
    drop(child.stdout.take());
    let mut stdin = child.stdin.take().unwrap();
    stdin
        .write_all(b"{\"v\":1,\"id\":\"closed-output\",\"op\":\"hello\"}\n")
        .await
        .unwrap();
    stdin.flush().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .expect("bridge must exit when its output pipe closes, even with stdin open")
        .unwrap();
    drop(stdin);
}

#[tokio::test]
async fn oversized_line_across_reader_buffers_preserves_next_request() {
    let (_directory, mut child) = process(Stdio::piped());
    let mut input = b"{\"v\":1,\"id\":\"oversized\",\"op\":\"hello\",\"padding\":\"".to_vec();
    input.extend(std::iter::repeat(b'x').take(1_048_576 + 16_384));
    input.extend_from_slice(b"\"}\n{\"v\":1,\"id\":\"next\",\"op\":\"hello\"}\n");
    let mut stdin = child.stdin.take().unwrap();
    let writer = tokio::spawn(async move { stdin.write_all(&input).await.unwrap() });
    let output = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
        .await
        .expect("bridge lost a request after draining an oversized line")
        .unwrap();
    writer.await.unwrap();
    assert!(output.status.success());
    let frames: Vec<serde_json::Value> = output
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| {
            assert!(line.len() <= 1_048_576);
            serde_json::from_slice(line).unwrap()
        })
        .collect();
    assert_eq!(frames.len(), 2);
    let oversized = frames
        .iter()
        .find(|frame| frame["id"] == "oversized")
        .unwrap();
    assert_eq!(oversized["error"]["code"], "invalid_request");
    let next = frames.iter().find(|frame| frame["id"] == "next").unwrap();
    assert_eq!(next["ok"], true);
    assert_eq!(next["op"], "hello");
}
