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
        let state = match slot
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

    // Called only after admitted request tasks have finished. Closing consumes
    // sessions and propagates release errors; an abort only drops/halts renewal.
    pub async fn close(&self) -> anyhow::Result<()> {
        self.cancel_requests();
        let feeds = std::mem::take(&mut *self.feeds.lock().expect("feed registry poisoned"));
        let deadline = Instant::now() + RELEASE_BUDGET;
        let mut closing = JoinSet::new();
        for (_, slot) in feeds {
            let slot = Arc::try_unwrap(slot)
                .map_err(|_| anyhow::anyhow!("requests remain during bridge close"))?;
            if let Some(state) = slot.into_inner() {
                if let Some(session) = state.session.into_inner() {
                    closing.spawn(async move {
                        let call = CallContext::new(deadline, CancellationToken::new());
                        session.close(&call).await
                    });
                }
            }
        }
        while let Some(result) = tokio::time::timeout_at(deadline, closing.join_next())
            .await
            .map_err(|_| anyhow::anyhow!("publisher release deadline exceeded"))?
        {
            if let Err(err) = result? {
                let response = lease_error("", err);
                anyhow::bail!("publisher release failed: {}", response.to_jsonl());
            }
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
