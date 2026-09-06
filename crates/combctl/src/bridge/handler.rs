//! JSONL adapter over checked V3 CompleteFeed and Comb-owned WriterSession.

use super::errors::{append_error, error, lease_error, open_error, read_error};
use super::limits::Limits;
use super::protocol::ErrorCode;
use super::protocol::{
    seq_string, Capabilities, OkBody, Request, Response, WireEvent, PROTOCOL_VERSION,
};
use comb_core::operation::StableKey;
use combctl::log::{
    CallContext, CompleteFeed, Cursor, FollowWait, LeasePolicy, LogReader, ReadLimits, ReadPage,
    WriterLabel, WriterSession,
};
use combctl::store::Store;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::OnceCell;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

// Session identities must not be evicted and silently reacquired. Bound the
// registry instead; the host can start another broker for more logical logs.
const MAX_OPEN_LOGS: usize = 256;
pub const SHUTDOWN_BUDGET: Duration = Duration::from_secs(4);
const RELEASE_BUDGET: Duration = Duration::from_secs(3);

// Every request holds a guard, including while it waits for initialization.
// Only the last request may remove an empty slot. Checking under the registry
// lock prevents a new caller from retaining an orphan while a replacement opens.
struct FeedSlot<'a> {
    bridge: &'a Bridge,
    log: String,
    slot: Option<Arc<OnceCell<FeedState>>>,
}

impl Drop for FeedSlot<'_> {
    fn drop(&mut self) {
        let mut feeds = self.bridge.feeds.lock().expect("feed registry poisoned");
        let slot = self.slot.take().expect("request owns a feed slot");
        let same_slot = feeds
            .get(&self.log)
            .is_some_and(|entry| Arc::ptr_eq(entry, &slot));
        // Decrement our reference while still holding the mutex. Otherwise two
        // simultaneous drops could both observe the other's last reference.
        drop(slot);
        if same_slot
            && feeds
                .get(&self.log)
                .is_some_and(|entry| entry.get().is_none() && Arc::strong_count(entry) == 1)
        {
            feeds.remove(&self.log);
        }
    }
}

struct FeedState {
    feed: CompleteFeed,
    session: OnceCell<WriterSession>,
}

pub struct Bridge {
    store: Arc<Store>,
    label: WriterLabel,
    policy: LeasePolicy,
    feeds: Mutex<HashMap<String, Arc<OnceCell<FeedState>>>>,
    cancellation: CancellationToken,
    closed: AtomicBool,
    pub limits: Limits,
}

pub fn mint_writer() -> String {
    let mut bytes = [0u8; 16];
    rand::Rng::fill(&mut rand::rng(), &mut bytes);
    format!("comb-bridge-{}", hex::encode(bytes))
}

impl Bridge {
    pub fn new(
        store: Store,
        writer: String,
        lease_secs: i64,
        limits: Limits,
    ) -> anyhow::Result<Self> {
        let label = WriterLabel::try_from(writer.as_str())
            .map_err(|_| anyhow::anyhow!("--writer must contain 1..64 bytes (diagnostic label)"))?;
        if !(3..=600).contains(&lease_secs) {
            anyhow::bail!("--lease must be between 3 and 600 seconds");
        }
        let ttl = Duration::from_secs(lease_secs as u64);
        let policy = LeasePolicy {
            ttl,
            renew_every: ttl / 3,
            clock_slack: ttl / 6,
            initial_acquire_budget: ttl + ttl / 2,
        };
        Ok(Self {
            store: Arc::new(store),
            label,
            policy,
            feeds: Mutex::new(HashMap::new()),
            cancellation: CancellationToken::new(),
            closed: AtomicBool::new(false),
            limits,
        })
    }

    pub async fn handle_frame(&self, bytes: &[u8]) -> Response {
        let call = CallContext::new(
            Instant::now() + self.policy.initial_acquire_budget + Duration::from_secs(15),
            self.cancellation.child_token(),
        );
        self.handle_frame_with_context(bytes, &call).await
    }

    pub async fn handle_frame_with_context(&self, bytes: &[u8], call: &CallContext) -> Response {
        let request = match super::protocol::parse_request(bytes, &self.limits) {
            Ok(request) => request,
            Err(response) => return response,
        };
        let id = request.id().to_owned();
        let call = CallContext::new(call.deadline, call.cancellation.child_token());
        let _cancel_on_drop = call.cancellation.clone().drop_guard();
        tokio::select! {
            biased;
            _ = self.cancellation.cancelled() => error(id, ErrorCode::Cancelled, "bridge is closing"),
            _ = call.cancellation.cancelled() => error(id, ErrorCode::Cancelled, "request cancelled"),
            _ = tokio::time::sleep_until(call.deadline) => error(id, ErrorCode::DeadlineExceeded, "request deadline exceeded"),
            response = self.dispatch(request, &call) => response,
        }
    }

    async fn dispatch(&self, request: Request, call: &CallContext) -> Response {
        if let Request::Hello { id, require } = request {
            return self.hello(id, require);
        }
        let (id, log) = match &request {
            Request::Append { id, log, .. }
            | Request::Head { id, log }
            | Request::Read { id, log, .. }
            | Request::Follow { id, log, .. } => (id.clone(), log.clone()),
            Request::Hello { .. } => unreachable!(),
        };
        let slot = {
            let mut feeds = self.feeds.lock().expect("feed registry poisoned");
            if self.closed.load(Ordering::Acquire) {
                return error(id, ErrorCode::Cancelled, "bridge is closing");
            }
            if !feeds.contains_key(&log) && feeds.len() >= MAX_OPEN_LOGS {
                return error(id, ErrorCode::Busy, "process has reached its 256-log limit");
            }
            feeds
                .entry(log.clone())
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };
        let slot = FeedSlot {
            bridge: self,
            log: log.clone(),
            slot: Some(slot),
        };
        let state = match slot
            .slot
            .as_ref()
            .expect("request owns a feed slot")
            .get_or_try_init(|| async {
                let feed = CompleteFeed::open(self.store.clone(), log.clone(), call).await?;
                Ok::<_, combctl::log::OpenLogError>(FeedState {
                    feed,
                    session: OnceCell::new(),
                })
            })
            .await
        {
            Ok(state) => state,
            Err(err) => return open_error(id, err),
        };
        match request {
            Request::Head { .. } => match state.feed.head(call).await {
                Ok(head) => Response::ok(
                    id,
                    "head",
                    OkBody::Head {
                        log,
                        head: seq_string(head.head_seq),
                        cursor: seq_string(head.next.next_seq),
                        trim_before: seq_string(head.trim_before_seq),
                    },
                ),
                Err(err) => read_error(id, err),
            },
            Request::Append {
                idempotency_key,
                payload,
                ..
            } => {
                let key = match hex::decode(idempotency_key)
                    .ok()
                    .and_then(|key| StableKey::try_from_canonical(key).ok())
                {
                    Some(key) => key,
                    None => return Response::invalid(id, "invalid stable key"),
                };
                let session = match state
                    .session
                    .get_or_try_init(|| async {
                        state.feed.writer_session(self.label.clone(), self.policy)
                    })
                    .await
                {
                    Ok(session) => session,
                    Err(_) => return Response::invalid(id, "invalid publisher lease policy"),
                };
                match session.append_stable(key, payload.into(), call).await {
                    Ok(receipt) => match receipt.range.last.checked_add(1) {
                        Some(cursor) => Response::ok(
                            id,
                            "append",
                            OkBody::Append {
                                log,
                                first: seq_string(receipt.range.first),
                                last: seq_string(receipt.range.last),
                                cursor: seq_string(cursor),
                            },
                        ),
                        None => error(
                            id,
                            ErrorCode::Integrity,
                            "append receipt has no next cursor",
                        ),
                    },
                    Err(err) => append_error(id, err),
                }
            }
            Request::Read {
                cursor,
                max_events,
                max_bytes,
                ..
            } => {
                self.page(
                    &state.feed,
                    id,
                    log,
                    cursor,
                    max_events,
                    max_bytes,
                    None,
                    call,
                )
                .await
            }
            Request::Follow {
                cursor,
                max_events,
                max_bytes,
                timeout_ms,
                ..
            } => {
                self.page(
                    &state.feed,
                    id,
                    log,
                    cursor,
                    max_events,
                    max_bytes,
                    Some(timeout_ms.unwrap_or(0)),
                    call,
                )
                .await
            }
            Request::Hello { .. } => unreachable!(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn page(
        &self,
        feed: &CompleteFeed,
        id: String,
        log: String,
        cursor: u64,
        max_events: Option<u64>,
        max_bytes: Option<u64>,
        follow_ms: Option<u64>,
        call: &CallContext,
    ) -> Response {
        let limit = match ReadLimits::try_new(
            max_events.unwrap_or(self.limits.max_read_events as u64) as u32,
            max_bytes.unwrap_or(self.limits.max_read_bytes as u64),
        ) {
            Ok(limit) => limit,
            Err(_) => return Response::invalid(id, "invalid page limits"),
        };
        let cursor = Cursor {
            partition: 0,
            next_seq: cursor,
        };
        let result = match follow_ms {
            Some(ms) if ms > 0 => {
                let wait = match FollowWait::try_new(Duration::from_millis(ms)) {
                    Ok(wait) => wait,
                    Err(_) => return Response::invalid(id, "invalid follow wait"),
                };
                feed.follow_page(cursor, limit, wait, call)
                    .await
                    .map(|follow| (follow.page, Some(follow.timed_out)))
            }
            _ => feed.read_page(cursor, limit, call).await.map(|page| {
                let timed_out = follow_ms.map(|_| page.events.is_empty() && page.at_head);
                (page, timed_out)
            }),
        };
        match result {
            Ok((page, timed_out)) => page_response(id, log, page, timed_out),
            Err(err) => read_error(id, err),
        }
    }

    fn hello(&self, id: String, require: Vec<String>) -> Response {
        let caps = Capabilities::offered();
        for name in require {
            if caps.supports(&name) != Some(true) {
                return Response::unsupported(
                    id,
                    &name,
                    format!("capability {name} is not available"),
                );
            }
        }
        Response::ok(
            id,
            "hello",
            OkBody::Hello {
                protocol: PROTOCOL_VERSION,
                capabilities: caps,
                limits: self.limits.view(),
            },
        )
    }

    pub fn cancel_requests(&self) {
        self.closed.store(true, Ordering::Release);
        self.cancellation.cancel();
    }

    pub fn abort(&self) {
        self.cancel_requests();
        self.feeds.lock().expect("feed registry poisoned").clear();
    }

    // Independent releases run through the shared deadline even when a sibling
    // fails. Failure diagnostics retain completed and uncertain log outcomes.
    // Forced abort only drops/halts renewal; release is never guaranteed.
    pub async fn close(&self) -> anyhow::Result<()> {
        self.cancel_requests();
        let feeds = std::mem::take(&mut *self.feeds.lock().expect("feed registry poisoned"));
        let deadline = Instant::now() + RELEASE_BUDGET;
        let mut closing = JoinSet::new();
        let mut pending = HashMap::new();
        let mut outcomes = Vec::new();
        let mut failed = false;
        for (log, slot) in feeds {
            let Ok(slot) = Arc::try_unwrap(slot) else {
                outcomes.push(format!("{log}: requests remain; release uncertain"));
                failed = true;
                continue;
            };
            if let Some(state) = slot.into_inner() {
                if let Some(session) = state.session.into_inner() {
                    let task = closing.spawn(async move {
                        let call = CallContext::new(deadline, CancellationToken::new());
                        session.close(&call).await
                    });
                    pending.insert(task.id(), log);
                }
            }
        }
        loop {
            // Account for already-completed tasks even at the deadline.
            let result = match closing.try_join_next_with_id() {
                Some(result) => Some(result),
                None => {
                    match tokio::time::timeout_at(deadline, closing.join_next_with_id()).await {
                        Ok(result) => result,
                        Err(_) => break,
                    }
                }
            };
            match result {
                Some(Ok((task, result))) => {
                    let log = pending.remove(&task).expect("tracked release task");
                    match result {
                        Ok(()) => outcomes.push(format!("{log}: released")),
                        Err(err) => {
                            failed = true;
                            outcomes.push(format!(
                                "{log}: release uncertain: {}",
                                lease_error("", err).to_jsonl()
                            ));
                        }
                    }
                }
                Some(Err(err)) => {
                    let log = pending.remove(&err.id()).expect("tracked release task");
                    failed = true;
                    outcomes.push(format!("{log}: release task failed; release uncertain"));
                }
                None => break,
            }
        }
        if !pending.is_empty() {
            failed = true;
            closing.abort_all();
            for log in pending.into_values() {
                outcomes.push(format!(
                    "{log}: publisher release deadline exceeded; release uncertain"
                ));
            }
        }
        if failed {
            outcomes.sort();
            anyhow::bail!("publisher cleanup: {}", outcomes.join("; "));
        }
        Ok(())
    }
}

fn page_response(id: String, log: String, page: ReadPage, timed_out: Option<bool>) -> Response {
    Response::ok(
        id,
        if timed_out.is_some() {
            "follow"
        } else {
            "read"
        },
        OkBody::Page {
            log,
            events: page
                .events
                .into_iter()
                .map(|event| WireEvent {
                    seq: seq_string(event.position.seq),
                    at: event.committed_at.to_rfc3339(),
                    payload_hex: hex::encode(event.payload),
                })
                .collect(),
            next_cursor: seq_string(page.next.next_seq),
            at_head: page.at_head,
            timed_out,
        },
    )
}

#[cfg(test)]
mod cleanup_tests {
    use super::*;
    use comb_core::DigestKey;
    use comb_object::{memory::MemoryBackend, ObjectBackend};
    use serde_json::json;

    fn broker(backend: Arc<MemoryBackend>) -> Bridge {
        Bridge::new(
            Store::new(backend, "cleanup", DigestKey::from_bytes([43; 32]), None),
            "cleanup".into(),
            30,
            Limits::default(),
        )
        .unwrap()
    }

    #[test]
    fn empty_slot_waiters_keep_one_identity_until_the_last_guard_drops() {
        let bridge = broker(Arc::new(MemoryBackend::new()));
        for _ in 0..32 {
            let slot = Arc::new(OnceCell::new());
            bridge
                .feeds
                .lock()
                .unwrap()
                .insert("doc".into(), slot.clone());
            let first = FeedSlot {
                bridge: &bridge,
                log: "doc".into(),
                slot: Some(slot.clone()),
            };
            let waiter = FeedSlot {
                bridge: &bridge,
                log: "doc".into(),
                slot: Some(slot),
            };
            drop(first);
            let replacement = bridge.feeds.lock().unwrap().get("doc").unwrap().clone();
            assert!(Arc::ptr_eq(waiter.slot.as_ref().unwrap(), &replacement));
            let third = FeedSlot {
                bridge: &bridge,
                log: "doc".into(),
                slot: Some(replacement),
            };
            // Both final drops use the registry lock to decrement their refs.
            let barrier = &std::sync::Barrier::new(2);
            std::thread::scope(|scope| {
                scope.spawn(move || {
                    barrier.wait();
                    drop(waiter);
                });
                scope.spawn(move || {
                    barrier.wait();
                    drop(third);
                });
            });
            assert!(bridge.feeds.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn retained_slot_does_not_skip_unrelated_release() {
        let backend = Arc::new(MemoryBackend::new());
        let bridge = broker(backend.clone());
        for log in ["retained", "healthy"] {
            let response = bridge.handle_frame(&serde_json::to_vec(&json!({
                "v":1,"id":"a","op":"append","log":log,"idempotency_key":"01","payload_hex":"00"
            })).unwrap()).await;
            assert_eq!(serde_json::to_value(response).unwrap()["ok"], true);
        }
        let retained = bridge
            .feeds
            .lock()
            .unwrap()
            .get("retained")
            .unwrap()
            .clone();
        let error = bridge.close().await.unwrap_err().to_string();
        assert!(
            error.contains("retained") && error.contains("healthy"),
            "{error}"
        );
        let (bytes, _) = backend
            .get_limited(
                "comb/v3/tenants/cleanup/refs/log/healthy/p0.json",
                std::num::NonZeroU64::new(65536).unwrap(),
            )
            .await
            .unwrap();
        let reference: comb_core::RefValue = serde_json::from_slice(&bytes).unwrap();
        assert!(
            reference.lease.is_none(),
            "unrelated release skipped: {error}"
        );
        drop(retained);
    }
}
