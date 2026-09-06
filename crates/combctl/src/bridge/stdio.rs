//! Stdio JSONL transport: concurrent handlers, bounded admission, serialized stdout.

use super::handler::Bridge;
use super::handler::SHUTDOWN_BUDGET;
use super::protocol::{extract_id, parse_request, validate_id, Response};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::Notify;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio::time::{timeout, Instant};

struct AbortOnDrop(Arc<Bridge>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub async fn run<R, W>(stdin: R, stdout: W, bridge: Arc<Bridge>) -> anyhow::Result<()>
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let _abort = AbortOnDrop(bridge.clone());
    let shutdown = Arc::new(Notify::new());
    let shutdown_started = shutdown.clone();
    let limits = bridge.limits.clone();
    let max_frame_bytes = limits.max_frame_bytes;
    let max_id_len = limits.max_id_len;
    let (tx, mut rx) = mpsc::channel::<String>(limits.max_queued_output);
    let mut transport = JoinSet::new();
    transport.spawn(async move {
        let mut stdout = stdout;
        while let Some(line) = rx.recv().await {
            let line = cap_stdout_frame(line, max_frame_bytes, max_id_len)?;
            timeout(SHUTDOWN_BUDGET, async {
                stdout.write_all(line.as_bytes()).await?;
                stdout.write_all(b"\n").await?;
                stdout.flush().await
            })
            .await
            .map_err(|_| anyhow::anyhow!("bridge output stalled"))??;
        }
        Ok::<(), anyhow::Error>(())
    });
    transport.spawn(async move {
        let sem = Arc::new(Semaphore::new(limits.max_concurrent_requests));
        let mut reader = BufReader::new(stdin);
        let mut inflight = JoinSet::new();
        loop {
            while let Some(result) = inflight.try_join_next() {
                result?;
            }
            match read_jsonl_frame(&mut reader, limits.max_frame_bytes).await? {
                FrameRead::Eof => {
                    shutdown_started.notify_one();
                    break;
                }
                FrameRead::Oversized { id } => {
                    tx.send(
                        Response::invalid(id, "request frame exceeds max_frame_bytes").to_jsonl(),
                    )
                    .await
                    .map_err(|_| anyhow::anyhow!("bridge output closed"))?;
                }
                FrameRead::Line(bytes) => {
                    let id = peek_id(&bytes, &bridge.limits);
                    let permit = match sem.clone().try_acquire_owned() {
                        Ok(permit) => permit,
                        Err(_) => {
                            tx.send(Response::busy(id).to_jsonl())
                                .await
                                .map_err(|_| anyhow::anyhow!("bridge output closed"))?;
                            continue;
                        }
                    };
                    let tx = tx.clone();
                    let bridge = bridge.clone();
                    inflight.spawn(async move {
                        handle_one(bridge, bytes, permit, tx).await;
                    });
                }
            }
        }
        drop(tx);
        while let Some(result) = inflight.join_next().await {
            result?;
        }
        bridge.close().await?;
        Ok::<(), anyhow::Error>(())
    });
    // EOF drains the writer. Either task failing drops this JoinSet, cancelling
    // its peer; dropping the input task also cancels every admitted handler.
    let mut shutdown_deadline = None;
    while !transport.is_empty() {
        tokio::select! {
            result = transport.join_next() => {
                if let Some(result) = result { result??; }
            }
            _ = shutdown.notified(), if shutdown_deadline.is_none() => {
                shutdown_deadline = Some(Instant::now() + SHUTDOWN_BUDGET);
            }
            _ = async {
                if let Some(deadline) = shutdown_deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => anyhow::bail!("bridge shutdown deadline exceeded"),
        }
    }
    Ok(())
}

fn cap_stdout_frame(
    line: String,
    max_frame_bytes: usize,
    max_id_len: usize,
) -> std::io::Result<String> {
    if line.len() <= max_frame_bytes {
        return Ok(line);
    }
    let mut id = extract_id(line.as_bytes());
    if validate_id(&id, max_id_len).is_err() {
        id.clear();
    }
    let mut fallback = Response::invalid(id, "response frame exceeds max_frame_bytes").to_jsonl();
    if fallback.len() > max_frame_bytes {
        fallback = Response::invalid("", "response exceeds frame limit").to_jsonl();
    }
    if fallback.len() > max_frame_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame limit cannot contain a protocol error",
        ));
    }
    Ok(fallback)
}

async fn handle_one(
    bridge: Arc<Bridge>,
    bytes: Vec<u8>,
    _permit: OwnedSemaphorePermit,
    tx: mpsc::Sender<String>,
) {
    let response = bridge.handle_frame(&bytes).await;
    let _ = tx.send(response.to_jsonl()).await;
}

fn peek_id(bytes: &[u8], limits: &super::limits::Limits) -> String {
    match parse_request(bytes, limits) {
        Ok(req) => req.id().to_string(),
        Err(Response::Err { id, .. }) | Err(Response::Ok { id, .. }) => id,
    }
}

#[derive(Debug)]
enum FrameRead {
    Line(Vec<u8>),
    Oversized { id: String },
    Eof,
}

async fn read_jsonl_frame<R: AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    max_bytes: usize,
) -> std::io::Result<FrameRead> {
    let mut buf = Vec::new();
    loop {
        let available = reader.fill_buf().await?.to_vec();
        if available.is_empty() {
            if buf.is_empty() {
                return Ok(FrameRead::Eof);
            }
            if buf.len() > max_bytes {
                return Ok(FrameRead::Oversized {
                    id: extract_id(&buf),
                });
            }
            if buf.last() == Some(&b'\r') {
                buf.pop();
            }
            return Ok(FrameRead::Line(buf));
        }
        if let Some(i) = available.iter().position(|&b| b == b'\n') {
            reader.consume(i + 1);
            if buf.len().saturating_add(i) > max_bytes {
                buf.extend_from_slice(&available[..i]);
                return Ok(FrameRead::Oversized {
                    id: extract_id(&buf),
                });
            }
            buf.extend_from_slice(&available[..i]);
            if buf.last() == Some(&b'\r') {
                buf.pop();
            }
            return Ok(FrameRead::Line(buf));
        }
        if buf.len().saturating_add(available.len()) > max_bytes {
            buf.extend_from_slice(&available);
            reader.consume(available.len());
            drain_until_newline(reader).await?;
            return Ok(FrameRead::Oversized {
                id: extract_id(&buf),
            });
        }
        buf.extend_from_slice(&available);
        reader.consume(available.len());
    }
}

/// Discard the rest of the current line, stopping at the newline. Bytes after
/// that delimiter belong to the next request and must stay in the buffer.
async fn drain_until_newline<R: AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
) -> std::io::Result<()> {
    loop {
        let available = reader.fill_buf().await?.to_vec();
        if available.is_empty() {
            return Ok(());
        }
        if let Some(i) = available.iter().position(|&b| b == b'\n') {
            reader.consume(i + 1);
            return Ok(());
        }
        reader.consume(available.len());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn drain_stops_at_newline_and_keeps_next_frame() {
        let mut data = Vec::from(&b"{\"v\":1,\"id\":\"big\",\"op\":\"hello\",\"pad\":\""[..]);
        data.extend(std::iter::repeat(b'a').take(80));
        data.extend_from_slice(br#""}"#);
        data.push(b'\n');
        data.extend_from_slice(br#"{"v":1,"id":"h","op":"hello"}"#);
        data.push(b'\n');
        let mut reader = BufReader::new(data.as_slice());
        match read_jsonl_frame(&mut reader, 32).await.unwrap() {
            FrameRead::Oversized { id } => assert_eq!(id, "big"),
            other => panic!("expected oversized, got {other:?}"),
        }
        match read_jsonl_frame(&mut reader, 1024).await.unwrap() {
            FrameRead::Line(line) => assert_eq!(line, br#"{"v":1,"id":"h","op":"hello"}"#),
            other => panic!("expected hello line, got {other:?}"),
        }
    }
}
