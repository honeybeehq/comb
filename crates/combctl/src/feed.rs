//! R2 complete-feed facade: v3 catalog manifest, paged read, writer sessions.

use super::{AppendRange, Frame, RetentionMode, StableAppendReceipt, CHUNK_SCHEMA};
use crate::catalog::{self, CatalogChunkRef, CatalogState};
use crate::hamt::{self, IndexHead, Lookup, StableIndexEntry, StableIndexRoot};
use crate::publish::{
    CasResult, HeadSnapshot, PrepareCtx, PreparedMutation, RefMutationPlan, Upload,
};
use crate::store::{KeyLayout, Store};
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use comb_core::commit::{Admission, CommitHeader};
use comb_core::error::CoreError;
use comb_core::operation::{Material, OpIdentity, StableKey};
use comb_core::{Digest, EnvelopeReadSpec, ObjectKind, RefValue, MAX_MANIFEST_OBJECT_BYTES};
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Notify};
use tokio_util::sync::CancellationToken;

pub const MAX_PAGE_EVENTS: u32 = 1_024;
pub const MAX_PAGE_RAW_BYTES: u64 = 512 * 1024;
pub const MAX_FOLLOW_WAIT: Duration = Duration::from_secs(30);
pub const MAX_CHUNK_EVENTS: u32 = 2_048;
pub const MAX_CHUNK_RAW_BYTES: u64 = 512 * 1024;
pub const MAX_CHUNK_PLAINTEXT_BYTES: u64 = 3 * 1024 * 1024;
pub const MAX_CHUNK_OBJECT_BYTES: u64 = 4 * 1024 * 1024;
pub const COMPLETE_MANIFEST_SCHEMA: &str = "comb.log.partition-manifest/v3";
const MANIFEST_V2: &str = "comb.log.partition-manifest/v2";
const CHUNK_SCHEMAS: &[&str] = &[CHUNK_SCHEMA];
const MANIFEST_SCHEMAS: &[&str] = &[COMPLETE_MANIFEST_SCHEMA];

#[derive(Clone)]
pub struct CallContext {
    pub deadline: tokio::time::Instant,
    pub cancellation: CancellationToken,
}

impl CallContext {
    pub fn new(deadline: tokio::time::Instant, cancellation: CancellationToken) -> Self {
        Self {
            deadline,
            cancellation,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Cursor {
    pub partition: u32,
    pub next_seq: u64,
}

impl Cursor {
    pub fn first(partition: u32) -> Self {
        Self {
            partition,
            next_seq: 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Position {
    pub partition: u32,
    pub seq: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct ReadLimits {
    max_events: NonZeroU32,
    max_raw_bytes: NonZeroU64,
}

#[derive(Debug)]
pub struct InvalidReadLimit;

impl ReadLimits {
    pub fn try_new(max_events: u32, max_raw_bytes: u64) -> Result<Self, InvalidReadLimit> {
        if max_events == 0
            || max_events > MAX_PAGE_EVENTS
            || max_raw_bytes == 0
            || max_raw_bytes > MAX_PAGE_RAW_BYTES
        {
            return Err(InvalidReadLimit);
        }
        Ok(Self {
            max_events: NonZeroU32::new(max_events).unwrap(),
            max_raw_bytes: NonZeroU64::new(max_raw_bytes).unwrap(),
        })
    }
}

#[derive(Clone, Debug)]
pub struct LogEvent {
    pub position: Position,
    pub committed_at: DateTime<Utc>,
    pub payload: Bytes,
}

#[derive(Clone, Debug)]
pub struct CompleteFeedHead {
    pub generation: u64,
    pub head_seq: u64,
    pub next: Cursor,
    pub trim_before_seq: u64,
}

#[derive(Clone, Debug)]
pub struct ReadPage {
    pub events: Vec<LogEvent>,
    pub next: Cursor,
    pub snapshot_head: u64,
    pub raw_payload_bytes: u64,
    pub at_head: bool,
}

#[derive(Clone, Debug)]
pub struct FollowPage {
    pub page: ReadPage,
    pub timed_out: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct FollowWait(Duration);

impl FollowWait {
    pub fn try_new(wait: Duration) -> Result<Self, InvalidReadLimit> {
        if wait.is_zero() || wait > MAX_FOLLOW_WAIT {
            return Err(InvalidReadLimit);
        }
        Ok(Self(wait))
    }
}

#[derive(Debug)]
pub enum OpenLogError {
    UnsupportedManifestSchema {
        found: String,
        required: &'static str,
    },
    Integrity(LogIntegrityError),
    Unavailable,
    DeadlineExceeded,
    Cancelled,
}

#[derive(Debug)]
pub struct LogIntegrityError(pub String);

#[derive(Debug)]
pub enum ReadError {
    Trimmed {
        requested: Cursor,
        resume_at: Cursor,
    },
    InvalidCursor {
        requested: Cursor,
        next_at_head: Cursor,
    },
    EventTooLarge {
        cursor: Cursor,
        position: Position,
        event_bytes: u64,
        max_bytes: u64,
    },
    Integrity(LogIntegrityError),
    Unavailable {
        operation: &'static str,
    },
    DeadlineExceeded,
    Cancelled,
    InvalidLimit(InvalidReadLimit),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct WriterInstanceId([u8; 16]);

impl WriterInstanceId {
    fn generate() -> Self {
        let mut bytes = [0u8; 16];
        rand::RngCore::fill_bytes(&mut rand::rng(), &mut bytes);
        Self(bytes)
    }

    pub fn canonical(&self) -> String {
        hex::encode(self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriterLabel(String);

impl TryFrom<&str> for WriterLabel {
    type Error = InvalidLeasePolicy;
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        if s.is_empty() || s.len() > 64 {
            return Err(InvalidLeasePolicy);
        }
        Ok(Self(s.to_string()))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct LeasePolicy {
    pub ttl: Duration,
    pub renew_every: Duration,
    pub clock_slack: Duration,
    pub initial_acquire_budget: Duration,
}

#[derive(Debug)]
pub struct InvalidLeasePolicy;

impl LeasePolicy {
    pub fn bridge_default() -> Self {
        Self {
            ttl: Duration::from_secs(30),
            renew_every: Duration::from_secs(10),
            clock_slack: Duration::from_secs(5),
            initial_acquire_budget: Duration::from_secs(45),
        }
    }

    fn validate(self) -> Result<Self, InvalidLeasePolicy> {
        if self.renew_every + self.clock_slack >= self.ttl
            || self.ttl > Duration::from_secs(600)
            || self.initial_acquire_budget < self.ttl
        {
            return Err(InvalidLeasePolicy);
        }
        Ok(self)
    }
}

#[derive(Clone, Debug)]
pub enum WriterState {
    Unacquired,
    Acquiring {
        held_until: Option<DateTime<Utc>>,
    },
    Active {
        epoch: u64,
        lease_until: DateTime<Utc>,
    },
    Lost {
        epoch: Option<u64>,
        cause: SessionLoss,
    },
    Closed,
}

#[derive(Clone, Debug)]
pub enum SessionLoss {
    Fenced { live_epoch: u64 },
    OwnerChanged,
    RenewalUncertain,
    LeaseExpired,
}

#[derive(Debug)]
pub enum LeaseError {
    LeaseHeld {
        owner: WriterInstanceId,
        until: DateTime<Utc>,
    },
    Fenced {
        session_epoch: u64,
        live_epoch: u64,
    },
    ReacquireRequired {
        cause: SessionLoss,
    },
    Unavailable,
    DeadlineExceeded,
    Cancelled,
    InvalidPolicy(InvalidLeasePolicy),
}

#[derive(Debug)]
pub enum StableAppendError {
    StableKeyConflict { existing: Digest, supplied: Digest },
    Lease(LeaseError),
    Integrity(LogIntegrityError),
    Unavailable,
    DeadlineExceeded,
    Cancelled,
    InvalidInput,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompleteManifestSchemaV3 {
    V3,
}

impl Serialize for CompleteManifestSchemaV3 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(COMPLETE_MANIFEST_SCHEMA)
    }
}

impl<'de> Deserialize<'de> for CompleteManifestSchemaV3 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        if s == COMPLETE_MANIFEST_SCHEMA {
            Ok(Self::V3)
        } else {
            Err(de::Error::custom(format!(
                "unsupported complete-feed schema {s}"
            )))
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompleteLogManifest {
    pub schema: CompleteManifestSchemaV3,
    pub header: CommitHeader,
    pub log: String,
    pub epoch: u64,
    pub head_seq: u64,
    pub retention: RetentionMode,
    pub trim_before_seq: u64,
    pub catalog: CatalogState,
    pub stable_index: StableIndexRoot,
    pub admitted: Vec<Admission>,
    pub stable_admissions: Vec<StableIndexEntry>,
    pub result: serde_json::Value,
    pub ref_state: Option<RefValue>,
}

pub struct CompleteFeed {
    store: Arc<Store>,
    logical: String,
    resource: String,
    wake: Arc<Notify>,
}

impl CompleteFeed {
    pub async fn open(
        store: Arc<Store>,
        logical: String,
        call: &CallContext,
    ) -> Result<Self, OpenLogError> {
        let resource = format!("log/{logical}/p0");
        let v3 = Arc::new((*store).clone().with_layout(KeyLayout::V3));
        let feed = Self {
            store: v3.clone(),
            logical,
            resource: resource.clone(),
            wake: Arc::new(Notify::new()),
        };
        match timed(call, v3.read_head(&resource)).await? {
            None => {
                let v2 = (*store).clone().with_layout(KeyLayout::V2);
                if timed(call, v2.backend.exists(&v2.ref_key(&resource))).await? {
                    return Err(OpenLogError::UnsupportedManifestSchema {
                        found: MANIFEST_V2.into(),
                        required: COMPLETE_MANIFEST_SCHEMA,
                    });
                }
                if timed(call, v2.backend.exists(&v2.v1_ref_key(&resource))).await? {
                    return Err(OpenLogError::UnsupportedManifestSchema {
                        found: "comb/v1".into(),
                        required: COMPLETE_MANIFEST_SCHEMA,
                    });
                }
            }
            Some(head) => {
                if let Some(digest) = head.value.target {
                    let _ = feed.load_manifest(&digest).await?;
                }
            }
        }
        Ok(feed)
    }

    pub fn writer_session(
        &self,
        label: WriterLabel,
        policy: LeasePolicy,
    ) -> Result<WriterSession, InvalidLeasePolicy> {
        let policy = policy.validate()?;
        let instance = WriterInstanceId::generate();
        let (tx, rx) = watch::channel(WriterState::Unacquired);
        Ok(WriterSession {
            feed: CompleteFeed {
                store: self.store.clone(),
                logical: self.logical.clone(),
                resource: self.resource.clone(),
                wake: self.wake.clone(),
            },
            instance,
            label,
            policy,
            state: tx,
            _watch: rx,
            renew: None,
        })
    }

    async fn load_manifest(&self, digest: &Digest) -> Result<CompleteLogManifest, OpenLogError> {
        let spec = EnvelopeReadSpec {
            tenant: &self.store.tenant,
            kind: ObjectKind::Blob,
            allowed_schemas: MANIFEST_SCHEMAS,
            max_encoded_bytes: NonZeroU64::new(MAX_MANIFEST_OBJECT_BYTES).unwrap(),
            max_plaintext_bytes: NonZeroU64::new(MAX_MANIFEST_OBJECT_BYTES).unwrap(),
        };
        let (payload, _) = self
            .store
            .get_blob_limited(digest, spec)
            .await
            .map_err(|e| {
                if let Some(CoreError::UnsupportedEnvelopeFormat { value, .. }) =
                    e.downcast_ref::<CoreError>()
                {
                    OpenLogError::UnsupportedManifestSchema {
                        found: value.clone(),
                        required: COMPLETE_MANIFEST_SCHEMA,
                    }
                } else {
                    OpenLogError::Integrity(LogIntegrityError(format!("{e:#}")))
                }
            })?;
        let raw: serde_json::Value = serde_json::from_slice(&payload).map_err(|e| {
            OpenLogError::Integrity(LogIntegrityError(format!("manifest json: {e}")))
        })?;
        if raw.get("schema").and_then(|s| s.as_str()) == Some(MANIFEST_V2) {
            return Err(OpenLogError::UnsupportedManifestSchema {
                found: MANIFEST_V2.into(),
                required: COMPLETE_MANIFEST_SCHEMA,
            });
        }
        serde_json::from_value(raw)
            .map_err(|e| OpenLogError::Integrity(LogIntegrityError(format!("v3 manifest: {e}"))))
    }
}

#[async_trait]
pub trait LogReader: Send + Sync {
    async fn head(&self, call: &CallContext) -> Result<CompleteFeedHead, ReadError>;
    async fn read_page(
        &self,
        cursor: Cursor,
        limits: ReadLimits,
        call: &CallContext,
    ) -> Result<ReadPage, ReadError>;
    async fn follow_page(
        &self,
        cursor: Cursor,
        limits: ReadLimits,
        wait: FollowWait,
        call: &CallContext,
    ) -> Result<FollowPage, ReadError>;
}

#[async_trait]
impl LogReader for CompleteFeed {
    async fn head(&self, call: &CallContext) -> Result<CompleteFeedHead, ReadError> {
        let snapshot = timed(call, self.store.read_head(&self.resource))
            .await
            .map_err(open_to_read)?;
        Ok(match snapshot {
            None => CompleteFeedHead {
                generation: 0,
                head_seq: 0,
                next: Cursor::first(0),
                trim_before_seq: 0,
            },
            Some(h) => {
                let head_seq = if let Some(d) = &h.value.target {
                    self.load_manifest(d).await.map_err(open_to_read)?.head_seq
                } else {
                    0
                };
                CompleteFeedHead {
                    generation: h.value.generation,
                    head_seq,
                    next: Cursor {
                        partition: 0,
                        next_seq: head_seq + 1,
                    },
                    trim_before_seq: 0,
                }
            }
        })
    }

    async fn read_page(
        &self,
        cursor: Cursor,
        limits: ReadLimits,
        call: &CallContext,
    ) -> Result<ReadPage, ReadError> {
        if cursor.partition != 0 {
            return Err(ReadError::InvalidCursor {
                requested: cursor,
                next_at_head: Cursor::first(0),
            });
        }
        let mut delay = Duration::from_millis(25);
        let mut last_err = None;
        for _ in 0..8 {
            match self.read_page_once(cursor, limits, call).await {
                Ok(p) => return Ok(p),
                Err(ReadError::Unavailable { .. }) => {
                    last_err = Some(ReadError::Unavailable {
                        operation: "read_page",
                    });
                    let sleep = tokio::time::sleep(delay);
                    tokio::select! {
                        _ = sleep => {}
                        _ = call.cancellation.cancelled() => return Err(ReadError::Cancelled),
                        _ = tokio::time::sleep_until(call.deadline) => return Err(ReadError::DeadlineExceeded),
                    }
                    delay = (delay * 2).min(Duration::from_millis(400));
                }
                Err(e) => return Err(e),
            }
        }
        Err(last_err.unwrap())
    }

    async fn follow_page(
        &self,
        cursor: Cursor,
        limits: ReadLimits,
        wait: FollowWait,
        call: &CallContext,
    ) -> Result<FollowPage, ReadError> {
        let page = self.read_page(cursor, limits, call).await?;
        if !page.events.is_empty() || !page.at_head {
            return Ok(FollowPage {
                page,
                timed_out: false,
            });
        }
        let wait_until = tokio::time::Instant::now() + wait.0;
        let deadline = call.deadline.min(wait_until);
        loop {
            tokio::select! {
                _ = self.wake.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(250)) => {}
                _ = call.cancellation.cancelled() => return Err(ReadError::Cancelled),
                _ = tokio::time::sleep_until(deadline) => {
                    if tokio::time::Instant::now() >= wait_until {
                        return Ok(FollowPage { page, timed_out: true });
                    }
                    return Err(ReadError::DeadlineExceeded);
                }
            }
            let page = self.read_page(cursor, limits, call).await?;
            if !page.events.is_empty() {
                return Ok(FollowPage {
                    page,
                    timed_out: false,
                });
            }
            if tokio::time::Instant::now() >= wait_until {
                return Ok(FollowPage {
                    page,
                    timed_out: true,
                });
            }
        }
    }
}

impl CompleteFeed {
    async fn read_page_once(
        &self,
        cursor: Cursor,
        limits: ReadLimits,
        call: &CallContext,
    ) -> Result<ReadPage, ReadError> {
        let _ = call;
        let snapshot = self
            .store
            .read_head(&self.resource)
            .await
            .map_err(any_read)?;
        let Some(head) = snapshot else {
            if cursor.next_seq == 1 {
                return Ok(ReadPage {
                    events: Vec::new(),
                    next: cursor,
                    snapshot_head: 0,
                    raw_payload_bytes: 0,
                    at_head: true,
                });
            }
            return Err(ReadError::InvalidCursor {
                requested: cursor,
                next_at_head: Cursor::first(0),
            });
        };
        let Some(digest) = &head.value.target else {
            if cursor.next_seq == 1 {
                return Ok(ReadPage {
                    events: Vec::new(),
                    next: cursor,
                    snapshot_head: 0,
                    raw_payload_bytes: 0,
                    at_head: true,
                });
            }
            return Err(ReadError::InvalidCursor {
                requested: cursor,
                next_at_head: Cursor::first(0),
            });
        };
        let manifest = self.load_manifest(digest).await.map_err(open_to_read)?;
        let head_seq = manifest.head_seq;
        let at_head_cursor = Cursor {
            partition: 0,
            next_seq: head_seq + 1,
        };
        if cursor.next_seq == 0 || cursor.next_seq > head_seq + 1 {
            return Err(ReadError::InvalidCursor {
                requested: cursor,
                next_at_head: at_head_cursor,
            });
        }
        if manifest.trim_before_seq > 0 && cursor.next_seq <= manifest.trim_before_seq {
            return Err(ReadError::Trimmed {
                requested: cursor,
                resume_at: Cursor {
                    partition: 0,
                    next_seq: manifest.trim_before_seq + 1,
                },
            });
        }
        if cursor.next_seq == head_seq + 1 {
            return Ok(ReadPage {
                events: Vec::new(),
                next: cursor,
                snapshot_head: head_seq,
                raw_payload_bytes: 0,
                at_head: true,
            });
        }
        let mut events = Vec::new();
        let mut raw = 0u64;
        let mut expected = cursor.next_seq;
        let mut seq = expected;
        while seq <= head_seq && events.len() < limits.max_events.get() as usize {
            let Some(refs) = catalog::seek_leaf(&self.store, &manifest.catalog, seq)
                .await
                .map_err(|e| ReadError::Integrity(LogIntegrityError(format!("{e:#}"))))?
            else {
                return Err(ReadError::Integrity(LogIntegrityError(
                    "catalog has no leaf for sequence".into(),
                )));
            };
            let Some(chunk_ref) = refs
                .iter()
                .find(|r| seq >= r.first_seq && seq <= r.last_seq)
            else {
                return Err(ReadError::Integrity(LogIntegrityError(
                    "leaf does not cover sequence".into(),
                )));
            };
            let frames = self.load_chunk(chunk_ref).await?;
            for frame in frames {
                if frame.seq < seq {
                    continue;
                }
                if frame.seq != expected {
                    return Err(ReadError::Integrity(LogIntegrityError(format!(
                        "chunk frame {} != expected {expected}",
                        frame.seq
                    ))));
                }
                let n = frame.payload.len() as u64;
                if events.is_empty() && n > limits.max_raw_bytes.get() {
                    return Err(ReadError::EventTooLarge {
                        cursor,
                        position: Position {
                            partition: 0,
                            seq: frame.seq,
                        },
                        event_bytes: n,
                        max_bytes: limits.max_raw_bytes.get(),
                    });
                }
                if !events.is_empty()
                    && (events.len() as u32 >= limits.max_events.get()
                        || raw + n > limits.max_raw_bytes.get())
                {
                    return Ok(ReadPage {
                        events,
                        next: Cursor {
                            partition: 0,
                            next_seq: expected,
                        },
                        snapshot_head: head_seq,
                        raw_payload_bytes: raw,
                        at_head: false,
                    });
                }
                events.push(LogEvent {
                    position: Position {
                        partition: 0,
                        seq: frame.seq,
                    },
                    committed_at: frame.at,
                    payload: Bytes::from(frame.payload),
                });
                raw += n;
                expected = frame.seq + 1;
                seq = expected;
                if events.len() >= limits.max_events.get() as usize {
                    break;
                }
            }
        }
        Ok(ReadPage {
            at_head: expected == head_seq + 1,
            events,
            next: Cursor {
                partition: 0,
                next_seq: expected,
            },
            snapshot_head: head_seq,
            raw_payload_bytes: raw,
        })
    }

    async fn load_chunk(&self, chunk: &CatalogChunkRef) -> Result<Vec<Frame>, ReadError> {
        let spec = EnvelopeReadSpec {
            tenant: &self.store.tenant,
            kind: ObjectKind::Blob,
            allowed_schemas: CHUNK_SCHEMAS,
            max_encoded_bytes: NonZeroU64::new(MAX_CHUNK_OBJECT_BYTES).unwrap(),
            max_plaintext_bytes: NonZeroU64::new(MAX_CHUNK_PLAINTEXT_BYTES).unwrap(),
        };
        let (payload, _) = self
            .store
            .get_blob_limited(&chunk.digest, spec)
            .await
            .map_err(|e| ReadError::Integrity(LogIntegrityError(format!("{e:#}"))))?;
        if payload.len() as u64 > chunk.plaintext_bytes {
            return Err(ReadError::Integrity(LogIntegrityError(
                "chunk plaintext exceeds catalog ref".into(),
            )));
        }
        let body: ChunkBody = serde_json::from_slice(&payload)
            .map_err(|e| ReadError::Integrity(LogIntegrityError(format!("chunk json: {e}"))))?;
        if body.schema != CHUNK_SCHEMA {
            return Err(ReadError::Integrity(LogIntegrityError(
                "chunk schema".into(),
            )));
        }
        if body.frames.len() as u32 > MAX_CHUNK_EVENTS {
            return Err(ReadError::Integrity(LogIntegrityError(
                "chunk event count".into(),
            )));
        }
        Ok(body.frames)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChunkBody {
    schema: String,
    frames: Vec<Frame>,
}

#[derive(Serialize)]
struct ChunkBodySer<'a> {
    schema: &'a str,
    frames: &'a [Frame],
}

pub struct WriterSession {
    feed: CompleteFeed,
    instance: WriterInstanceId,
    #[allow(dead_code)]
    label: WriterLabel,
    policy: LeasePolicy,
    state: watch::Sender<WriterState>,
    _watch: watch::Receiver<WriterState>,
    renew: Option<tokio::task::JoinHandle<()>>,
}

impl WriterSession {
    pub fn state(&self) -> WriterState {
        self.state.borrow().clone()
    }

    pub async fn ready(&self, call: &CallContext) -> Result<u64, LeaseError> {
        self.acquire(call).await
    }

    async fn acquire(&self, call: &CallContext) -> Result<u64, LeaseError> {
        match &*self.state.borrow() {
            WriterState::Active { epoch, .. } => return Ok(*epoch),
            WriterState::Lost { cause, .. } => {
                return Err(LeaseError::ReacquireRequired {
                    cause: cause.clone(),
                })
            }
            WriterState::Closed => {
                return Err(LeaseError::ReacquireRequired {
                    cause: SessionLoss::OwnerChanged,
                })
            }
            _ => {}
        }
        let _ = self.state.send(WriterState::Acquiring { held_until: None });
        let op = self.feed.store.mint_operation();
        let ttl = self.policy.ttl.as_secs() as i64;
        let acquire_until = tokio::time::Instant::now() + self.policy.initial_acquire_budget;
        let deadline = call.deadline.min(acquire_until);
        loop {
            if call.cancellation.is_cancelled() {
                return Err(LeaseError::Cancelled);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(LeaseError::DeadlineExceeded);
            }
            match self
                .feed
                .store
                .claim(
                    op,
                    &self.feed.resource,
                    &self.instance.canonical(),
                    ttl,
                    false,
                )
                .await
            {
                Ok(out) => {
                    let until = Utc::now() + chrono::Duration::seconds(ttl);
                    let _ = self.state.send(WriterState::Active {
                        epoch: out.epoch,
                        lease_until: until,
                    });
                    return Ok(out.epoch);
                }
                Err(e) => {
                    if let Some(CoreError::LeaseHeld { holder, until }) = e.downcast_ref() {
                        let _until_ts = DateTime::parse_from_rfc3339(until)
                            .map(|d| d.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now());
                        let _ = holder;
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
                            _ = call.cancellation.cancelled() => return Err(LeaseError::Cancelled),
                            _ = tokio::time::sleep_until(deadline) => return Err(LeaseError::DeadlineExceeded),
                        }
                        continue;
                    }
                    return Err(LeaseError::Unavailable);
                }
            }
        }
    }

    pub async fn append_stable(
        &self,
        key: StableKey,
        payload: Bytes,
        call: &CallContext,
    ) -> Result<StableAppendReceipt, StableAppendError> {
        if payload.is_empty() {
            return Err(StableAppendError::InvalidInput);
        }
        let payload_hash = StableKey::payload_hash(&self.feed.store.key, &payload);
        for _ in 0..32 {
            if call.cancellation.is_cancelled() {
                return Err(StableAppendError::Cancelled);
            }
            if tokio::time::Instant::now() >= call.deadline {
                return Err(StableAppendError::DeadlineExceeded);
            }
            let snapshot = self
                .feed
                .store
                .read_head(&self.feed.resource)
                .await
                .map_err(|_| StableAppendError::Unavailable)?
                .unwrap_or_else(|| HeadSnapshot {
                    value: RefValue::new(&self.feed.store.tenant, &self.feed.resource),
                    version: None,
                });
            if let Some(digest) = &snapshot.value.target {
                let manifest = self.feed.load_manifest(digest).await.map_err(|e| match e {
                    OpenLogError::Integrity(i) => StableAppendError::Integrity(i),
                    _ => StableAppendError::Unavailable,
                })?;
                let found = hamt::lookup(
                    &self.feed.store,
                    &manifest.stable_index,
                    &key,
                    &self.feed.logical,
                    IndexHead {
                        generation: snapshot.value.generation,
                        head_seq: manifest.head_seq,
                    },
                )
                .await
                .map_err(|e| StableAppendError::Integrity(LogIntegrityError(format!("{e:#}"))))?;
                match found {
                    Lookup::Found(e) if e.payload_hash == payload_hash => {
                        return Ok(StableAppendReceipt {
                            payload_hash,
                            range: AppendRange {
                                first: e.first,
                                last: e.last,
                            },
                            generation: e.generation,
                        });
                    }
                    Lookup::Found(e) => {
                        return Err(StableAppendError::StableKeyConflict {
                            existing: e.payload_hash,
                            supplied: payload_hash,
                        });
                    }
                    Lookup::Absent => {}
                }
            }
            let epoch = self.acquire(call).await.map_err(StableAppendError::Lease)?;
            let snapshot = self
                .feed
                .store
                .read_head(&self.feed.resource)
                .await
                .map_err(|_| StableAppendError::Unavailable)?
                .ok_or(StableAppendError::Unavailable)?;
            match self
                .feed
                .store
                .commit_at_snapshot(
                    OpIdentity::Stable(key.clone()),
                    CompleteAppendPlan {
                        resource: self.feed.resource.clone(),
                        logical: self.feed.logical.clone(),
                        instance: self.instance.canonical(),
                        epoch,
                        key: key.clone(),
                        payload: payload.to_vec(),
                        payload_hash: payload_hash.clone(),
                    },
                    snapshot,
                )
                .await
            {
                Ok(CasResult::Committed(p)) => {
                    self.feed.wake.notify_waiters();
                    return Ok(p.outcome);
                }
                Ok(CasResult::Conflict) => continue,
                Err(e) => {
                    if let Some(CoreError::LeaseHeld { .. }) = e.downcast_ref() {
                        continue;
                    }
                    if let Some(CoreError::Fenced { caller, live }) = e.downcast_ref() {
                        let _ = self.state.send(WriterState::Lost {
                            epoch: Some(*caller),
                            cause: SessionLoss::Fenced { live_epoch: *live },
                        });
                        return Err(StableAppendError::Lease(LeaseError::Fenced {
                            session_epoch: *caller,
                            live_epoch: *live,
                        }));
                    }
                    return Err(StableAppendError::Unavailable);
                }
            }
        }
        Err(StableAppendError::Unavailable)
    }

    pub async fn close(mut self, call: &CallContext) -> Result<(), LeaseError> {
        if let Some(h) = self.renew.take() {
            h.abort();
        }
        let epoch = match &*self.state.borrow() {
            WriterState::Active { epoch, .. } => *epoch,
            _ => {
                let _ = self.state.send(WriterState::Closed);
                return Ok(());
            }
        };
        let op = self.feed.store.mint_operation();
        let _ = timed_lease(
            call,
            self.feed.store.release(op, &self.feed.resource, epoch),
        )
        .await;
        let _ = self.state.send(WriterState::Closed);
        Ok(())
    }
}

impl Drop for WriterSession {
    fn drop(&mut self) {
        if let Some(h) = self.renew.take() {
            h.abort();
        }
    }
}

struct CompleteAppendPlan {
    resource: String,
    logical: String,
    instance: String,
    epoch: u64,
    key: StableKey,
    payload: Vec<u8>,
    payload_hash: Digest,
}

impl RefMutationPlan for CompleteAppendPlan {
    type Outcome = StableAppendReceipt;

    fn resource(&self) -> &str {
        &self.resource
    }

    fn material(&self) -> Material {
        Material {
            kind: "append-stable".into(),
            preconditions: vec![("stable-key".into(), self.key.as_bytes().to_vec())],
            payload: vec![self.payload.clone()],
        }
    }

    async fn prepare(&self, ctx: PrepareCtx<'_>) -> Result<PreparedMutation<Self::Outcome>> {
        let now = ctx.now;
        let current = &ctx.snapshot.value;
        let lease = current.lease.as_ref().ok_or_else(|| {
            CoreError::Rejected("complete-feed append requires a live lease".into())
        })?;
        if lease.writer != self.instance || current.epoch != self.epoch || !current.lease_live(now)
        {
            return Err(CoreError::LeaseHeld {
                holder: lease.writer.clone(),
                until: lease.lease_until.to_rfc3339(),
            }
            .into());
        }
        if self.payload.len() as u64 > MAX_CHUNK_RAW_BYTES {
            return Err(CoreError::Rejected("event exceeds chunk raw cap".into()).into());
        }
        let (mut catalog, mut index, mut head_seq) = if current.target.is_none() {
            (
                CatalogState::Empty,
                hamt::empty_root(ctx.store).await?,
                0u64,
            )
        } else {
            let spec = EnvelopeReadSpec {
                tenant: &ctx.store.tenant,
                kind: ObjectKind::Blob,
                allowed_schemas: MANIFEST_SCHEMAS,
                max_encoded_bytes: NonZeroU64::new(MAX_MANIFEST_OBJECT_BYTES).unwrap(),
                max_plaintext_bytes: NonZeroU64::new(MAX_MANIFEST_OBJECT_BYTES).unwrap(),
            };
            let (payload, _) = ctx
                .store
                .get_blob_limited(current.target.as_ref().unwrap(), spec)
                .await?;
            let m: CompleteLogManifest = serde_json::from_slice(&payload)?;
            (m.catalog, m.stable_index, m.head_seq)
        };
        let seq = head_seq + 1;
        let frame = Frame {
            seq,
            at: now,
            payload: self.payload.clone(),
        };
        let chunk_bytes = serde_json::to_vec(&ChunkBodySer {
            schema: CHUNK_SCHEMA,
            frames: std::slice::from_ref(&frame),
        })?;
        if chunk_bytes.len() as u64 > MAX_CHUNK_PLAINTEXT_BYTES {
            return Err(CoreError::ObjectTooLarge {
                key: String::new(),
                limit: MAX_CHUNK_PLAINTEXT_BYTES,
                actual: Some(chunk_bytes.len() as u64),
            }
            .into());
        }
        let chunk_digest = ctx.store.key.digest(&chunk_bytes);
        catalog = catalog::append(
            ctx.store,
            &catalog,
            CatalogChunkRef {
                digest: chunk_digest.clone(),
                first_seq: seq,
                last_seq: seq,
                event_count: 1,
                raw_payload_bytes: self.payload.len() as u64,
                plaintext_bytes: chunk_bytes.len() as u64,
            },
        )
        .await?;
        let entry = StableIndexEntry {
            key: self.key.clone(),
            payload_hash: self.payload_hash.clone(),
            first: seq,
            last: seq,
            generation: ctx.generation,
        };
        index = hamt::insert(
            ctx.store,
            &index,
            entry.clone(),
            &self.logical,
            IndexHead {
                generation: ctx.generation,
                head_seq: seq,
            },
        )
        .await?;
        hamt::admissions_size(std::slice::from_ref(&entry))?;
        head_seq = seq;
        let header = ctx.header(current.epoch);
        let mut next = current.clone();
        next.generation = ctx.generation;
        next.updated_at = now;
        let mut ref_state = next.clone();
        ref_state.head_commit = None;
        ref_state.target = None;
        let range = AppendRange {
            first: seq,
            last: seq,
        };
        let manifest = CompleteLogManifest {
            schema: CompleteManifestSchemaV3::V3,
            header,
            log: ctx.snapshot.value.name.clone(),
            epoch: current.epoch,
            head_seq,
            retention: RetentionMode::Complete,
            trim_before_seq: 0,
            catalog,
            stable_index: index,
            admitted: Vec::new(),
            stable_admissions: vec![entry],
            result: serde_json::to_value(&range)?,
            ref_state: Some(ref_state),
        };
        let manifest_bytes = serde_json::to_vec(&manifest)?;
        Ok(PreparedMutation {
            next,
            uploads: vec![
                Upload {
                    kind: ObjectKind::Blob,
                    schema: CHUNK_SCHEMA.into(),
                    payload: chunk_bytes,
                },
                Upload {
                    kind: ObjectKind::Blob,
                    schema: COMPLETE_MANIFEST_SCHEMA.into(),
                    payload: manifest_bytes,
                },
            ],
            commit_upload: Some(1),
            change: serde_json::json!({ "kind": "append-stable", "first": seq, "last": seq }),
            outcome: StableAppendReceipt {
                payload_hash: self.payload_hash.clone(),
                range,
                generation: ctx.generation,
            },
            admitted: Vec::new(),
            companions: Vec::new(),
        })
    }
}

async fn timed<T, E>(
    call: &CallContext,
    fut: impl std::future::Future<Output = std::result::Result<T, E>>,
) -> Result<T, OpenLogError> {
    tokio::select! {
        r = fut => r.map_err(|_| OpenLogError::Unavailable),
        _ = call.cancellation.cancelled() => Err(OpenLogError::Cancelled),
        _ = tokio::time::sleep_until(call.deadline) => Err(OpenLogError::DeadlineExceeded),
    }
}

async fn timed_lease<T>(
    call: &CallContext,
    fut: impl std::future::Future<Output = Result<T>>,
) -> Result<T, LeaseError> {
    tokio::select! {
        r = fut => r.map_err(|_| LeaseError::Unavailable),
        _ = call.cancellation.cancelled() => Err(LeaseError::Cancelled),
        _ = tokio::time::sleep_until(call.deadline) => Err(LeaseError::DeadlineExceeded),
    }
}

fn open_to_read(e: OpenLogError) -> ReadError {
    match e {
        OpenLogError::Integrity(i) => ReadError::Integrity(i),
        OpenLogError::DeadlineExceeded => ReadError::DeadlineExceeded,
        OpenLogError::Cancelled => ReadError::Cancelled,
        OpenLogError::UnsupportedManifestSchema { found, .. } => {
            ReadError::Integrity(LogIntegrityError(found))
        }
        OpenLogError::Unavailable => ReadError::Unavailable { operation: "head" },
    }
}

fn any_read(e: anyhow::Error) -> ReadError {
    if let Some(CoreError::BackendUnavailable(_)) = e.downcast_ref() {
        ReadError::Unavailable { operation: "read" }
    } else {
        ReadError::Integrity(LogIntegrityError(format!("{e:#}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::LogStore;
    use comb_core::DigestKey;
    use comb_object::memory::MemoryBackend;

    fn call() -> CallContext {
        CallContext::new(
            tokio::time::Instant::now() + Duration::from_secs(30),
            CancellationToken::new(),
        )
    }

    fn store() -> Arc<Store> {
        Arc::new(Store::new(
            Arc::new(MemoryBackend::new()),
            "org_t",
            DigestKey::from_bytes([9u8; 32]),
            None,
        ))
    }

    #[tokio::test]
    async fn v2_and_v3_are_isolated_and_v2_is_not_opened() {
        let backend = Arc::new(MemoryBackend::new());
        let v2 = Arc::new(Store::new(
            backend.clone(),
            "org_t",
            DigestKey::from_bytes([9u8; 32]),
            None,
        ));
        let log = LogStore::complete_feed(&v2, "iso");
        let op = v2.mint_operation();
        log.append(op, "w", &[b"one".to_vec()], 60).await.unwrap();
        let r2 = CompleteFeed::open(v2.clone(), "iso".into(), &call()).await;
        match r2 {
            Err(OpenLogError::UnsupportedManifestSchema { found, required }) => {
                assert_eq!(found, MANIFEST_V2);
                assert_eq!(required, COMPLETE_MANIFEST_SCHEMA);
            }
            Err(other) => panic!("expected unsupported, got {other:?}"),
            Ok(_) => panic!("expected unsupported, opened a v2 log"),
        }
        let feed = CompleteFeed::open(v2.clone(), "fresh".into(), &call())
            .await
            .unwrap();
        assert!(feed
            .store
            .object_key(&v2.key.digest(b"x"))
            .starts_with("comb/v3/"));
        assert!(v2.object_key(&v2.key.digest(b"x")).starts_with("comb/v2/"));
    }

    #[tokio::test]
    async fn append_and_page_replay() {
        let store = store();
        let feed = CompleteFeed::open(store, "p".into(), &call())
            .await
            .unwrap();
        let writer = feed
            .writer_session(
                WriterLabel::try_from("bridge").unwrap(),
                LeasePolicy::bridge_default(),
            )
            .unwrap();
        for i in 1..=20u64 {
            let key = StableKey::try_from_canonical(format!("k{i}").into_bytes()).unwrap();
            writer
                .append_stable(key, Bytes::from(format!("b{i}")), &call())
                .await
                .unwrap();
        }
        let limits = ReadLimits::try_new(7, 4096).unwrap();
        let mut cursor = Cursor::first(0);
        let mut seqs = Vec::new();
        loop {
            let page = LogReader::read_page(&feed, cursor, limits, &call())
                .await
                .unwrap();
            for e in &page.events {
                seqs.push(e.position.seq);
            }
            if page.at_head {
                break;
            }
            cursor = page.next;
        }
        assert_eq!(seqs, (1..=20).collect::<Vec<_>>());
        let too = LogReader::read_page(
            &feed,
            Cursor {
                partition: 0,
                next_seq: 0,
            },
            limits,
            &call(),
        )
        .await;
        assert!(matches!(too, Err(ReadError::InvalidCursor { .. })));
    }

    #[tokio::test]
    async fn two_sessions_same_label_distinct_instances() {
        let store = store();
        let feed = CompleteFeed::open(store, "s".into(), &call())
            .await
            .unwrap();
        let a = feed
            .writer_session(
                WriterLabel::try_from("bridge").unwrap(),
                LeasePolicy::bridge_default(),
            )
            .unwrap();
        let b = feed
            .writer_session(
                WriterLabel::try_from("bridge").unwrap(),
                LeasePolicy::bridge_default(),
            )
            .unwrap();
        assert_ne!(a.instance.canonical(), b.instance.canonical());
        a.ready(&call()).await.unwrap();
        let err = b
            .ready(&CallContext::new(
                tokio::time::Instant::now() + Duration::from_millis(400),
                CancellationToken::new(),
            ))
            .await;
        assert!(matches!(
            err,
            Err(LeaseError::DeadlineExceeded) | Err(LeaseError::LeaseHeld { .. })
        ));
    }
}
