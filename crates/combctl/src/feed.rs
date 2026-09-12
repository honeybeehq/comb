//! R2 complete-feed facade: v3 catalog manifest, paged read, writer sessions.

use super::{AppendRange, Frame, RetentionMode, StableAppendReceipt, CHUNK_SCHEMA};
use crate::catalog::{self, CatalogChunkRef, CatalogState};
use crate::hamt::{self, IndexHead, Lookup, StableIndexEntry, StableIndexRoot};
use crate::publish::{
    CasResult, CommitView, HeadSnapshot, LiveLeaseGuard, PrepareCtx, PreparedMutation,
    RefMutationPlan, Upload,
};
use crate::store::{KeyLayout, Store};
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use comb_core::commit::{Admission, CommitHeader};
use comb_core::error::CoreError;
use comb_core::operation::{Material, OpIdentity, StableKey};
use comb_core::{
    Digest, Envelope, EnvelopeReadSpec, ObjectKind, RefValue, MAX_MANIFEST_OBJECT_BYTES,
};
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::sync::{watch, Mutex as TokioMutex, Notify};
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

    fn try_from_canonical(s: &str) -> Option<Self> {
        let bytes = hex::decode(s).ok()?;
        let arr: [u8; 16] = bytes.try_into().ok()?;
        Some(Self(arr))
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
        if self
            .renew_every
            .checked_add(self.clock_slack)
            .is_none_or(|renew_window| renew_window >= self.ttl)
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

fn validate_complete_manifest(
    resource: &str,
    manifest: &CompleteLogManifest,
) -> std::result::Result<(), CoreError> {
    if manifest.header.resource != resource || manifest.log != resource {
        return Err(CoreError::IntegrityError(format!(
            "manifest log {} does not bind resource {resource}",
            manifest.log
        )));
    }
    if !matches!(manifest.retention, RetentionMode::Complete) {
        return Err(CoreError::IntegrityError(
            "complete feed requires complete retention".into(),
        ));
    }
    if manifest.header.epoch != manifest.epoch {
        return Err(CoreError::IntegrityError(
            "manifest epoch disagrees with its commit header".into(),
        ));
    }
    if manifest.trim_before_seq > manifest.head_seq {
        return Err(CoreError::IntegrityError(
            "trim_before_seq is past head_seq".into(),
        ));
    }
    match &manifest.catalog {
        CatalogState::Empty => {
            if manifest.head_seq != 0 {
                return Err(CoreError::IntegrityError(
                    "empty catalog with nonzero head_seq".into(),
                ));
            }
        }
        CatalogState::Root { root } => {
            if manifest.head_seq == 0 {
                return Err(CoreError::IntegrityError(
                    "catalog root with zero head_seq".into(),
                ));
            }
            if root.first_seq == 0 || root.last_seq < root.first_seq || root.chunk_count == 0 {
                return Err(CoreError::IntegrityError(
                    "catalog root has an impossible span".into(),
                ));
            }
            let expected_first = if manifest.trim_before_seq == 0 {
                1
            } else {
                manifest
                    .trim_before_seq
                    .checked_add(1)
                    .ok_or_else(|| CoreError::IntegrityError("trim_before_seq overflows".into()))?
            };
            if root.first_seq != expected_first {
                return Err(CoreError::IntegrityError(
                    "catalog first_seq disagrees with trim/head".into(),
                ));
            }
            if root.last_seq != manifest.head_seq {
                return Err(CoreError::IntegrityError(
                    "catalog last_seq disagrees with manifest head_seq".into(),
                ));
            }
            let span = root.last_seq - root.first_seq + 1;
            if root.chunk_count > span {
                return Err(CoreError::IntegrityError(
                    "catalog chunk_count exceeds sequence span".into(),
                ));
            }
        }
    }
    manifest.stable_index.validate()?;
    hamt::check_root_against_head(
        &manifest.stable_index,
        IndexHead {
            generation: manifest.header.generation,
            head_seq: manifest.head_seq,
        },
    )?;
    if manifest.head_seq == 0 && manifest.stable_index.entries != 0 {
        return Err(CoreError::IntegrityError(
            "empty feed has a nonempty stable index".into(),
        ));
    }
    if manifest.head_seq != 0 && manifest.stable_index.entries == 0 {
        return Err(CoreError::IntegrityError(
            "published catalog has an empty stable index".into(),
        ));
    }
    Ok(())
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
        if let Some(e) = open_terminal(call) {
            return Err(e);
        }
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
                if timed(call, async {
                    v2.backend
                        .exists(&v2.ref_key(&resource))
                        .await
                        .map_err(anyhow::Error::from)
                })
                .await?
                {
                    return Err(OpenLogError::UnsupportedManifestSchema {
                        found: MANIFEST_V2.into(),
                        required: COMPLETE_MANIFEST_SCHEMA,
                    });
                }
                if timed(call, async {
                    v2.backend
                        .exists(&v2.v1_ref_key(&resource))
                        .await
                        .map_err(anyhow::Error::from)
                })
                .await?
                {
                    return Err(OpenLogError::UnsupportedManifestSchema {
                        found: "comb/v1".into(),
                        required: COMPLETE_MANIFEST_SCHEMA,
                    });
                }
            }
            Some(head) => {
                let _ = feed.validated_publication(&head.value, call).await?;
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
            acquire: TokioMutex::new(()),
            renew: StdMutex::new(None),
            loss: CancellationToken::new(),
        })
    }

    async fn load_manifest(
        &self,
        digest: &Digest,
        call: &CallContext,
    ) -> Result<CompleteLogManifest, OpenLogError> {
        let spec = EnvelopeReadSpec {
            tenant: &self.store.tenant,
            kind: ObjectKind::Blob,
            allowed_schemas: MANIFEST_SCHEMAS,
            max_encoded_bytes: NonZeroU64::new(MAX_MANIFEST_OBJECT_BYTES).unwrap(),
            max_plaintext_bytes: NonZeroU64::new(MAX_MANIFEST_OBJECT_BYTES).unwrap(),
        };
        let (payload, _) = timed(call, self.store.get_blob_limited(digest, spec)).await?;
        let raw: serde_json::Value = serde_json::from_slice(&payload).map_err(|e| {
            OpenLogError::Integrity(LogIntegrityError(format!("manifest json: {e}")))
        })?;
        if raw.get("schema").and_then(|s| s.as_str()) == Some(MANIFEST_V2) {
            return Err(OpenLogError::UnsupportedManifestSchema {
                found: MANIFEST_V2.into(),
                required: COMPLETE_MANIFEST_SCHEMA,
            });
        }
        let manifest: CompleteLogManifest = serde_json::from_value(raw)
            .map_err(|e| OpenLogError::Integrity(LogIntegrityError(format!("v3 manifest: {e}"))))?;
        validate_complete_manifest(&self.resource, &manifest)
            .map_err(|e| OpenLogError::Integrity(LogIntegrityError(e.to_string())))?;
        Ok(manifest)
    }

    async fn bound_target(
        &self,
        value: &RefValue,
        call: &CallContext,
    ) -> Result<Option<Digest>, OpenLogError> {
        if value.generation == 0 && value.head_commit.is_none() && value.target.is_none() {
            return Ok(None);
        }
        if value.generation > 0 && value.head_commit.is_none() {
            return Err(OpenLogError::Integrity(LogIntegrityError(
                "missing head_commit with published generation".into(),
            )));
        }
        let Some(commit) = &value.head_commit else {
            return Err(OpenLogError::Integrity(LogIntegrityError(
                "target without head_commit".into(),
            )));
        };
        let view = timed(call, self.store.load_commit_view(commit)).await?;
        bind_published_target(&self.resource, value, &view)
            .map_err(|e| OpenLogError::Integrity(LogIntegrityError(e.to_string())))
    }

    async fn validated_publication(
        &self,
        value: &RefValue,
        call: &CallContext,
    ) -> Result<Option<CompleteLogManifest>, OpenLogError> {
        let Some(digest) = self.bound_target(value, call).await? else {
            return Ok(None);
        };
        Ok(Some(self.load_manifest(&digest, call).await?))
    }
}

fn bind_published_target(
    resource: &str,
    value: &RefValue,
    view: &CommitView,
) -> std::result::Result<Option<Digest>, CoreError> {
    if view.header.resource != resource || value.name != resource {
        return Err(CoreError::IntegrityError(format!(
            "commit resource {} does not bind {resource}",
            view.header.resource
        )));
    }
    if view.header.generation != value.generation {
        return Err(CoreError::IntegrityError(format!(
            "commit generation {} disagrees with ref {}",
            view.header.generation, value.generation
        )));
    }
    if view.header.epoch > value.epoch {
        return Err(CoreError::IntegrityError(format!(
            "commit epoch {} is ahead of ref {}",
            view.header.epoch, value.epoch
        )));
    }
    let persisted = if view.target_follows_commit {
        Some(view.digest.clone())
    } else {
        view.ref_state.as_ref().and_then(|r| r.target.clone())
    };
    match (&value.target, persisted) {
        (None, None) => Ok(None),
        (Some(live), Some(persisted)) if live == &persisted => Ok(Some(persisted)),
        (Some(_), Some(_)) => Err(CoreError::IntegrityError(
            "live target disagrees with committed target".into(),
        )),
        (None, Some(_)) => Err(CoreError::IntegrityError(
            "committed target missing from ref".into(),
        )),
        (Some(_), None) => Err(CoreError::IntegrityError(
            "live target without committed target".into(),
        )),
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
        if let Some(e) = read_terminal(call) {
            return Err(e);
        }
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
                let (head_seq, trim_before_seq) = match self
                    .validated_publication(&h.value, call)
                    .await
                    .map_err(open_to_read)?
                {
                    Some(m) => (m.head_seq, m.trim_before_seq),
                    None => (0, 0),
                };
                CompleteFeedHead {
                    generation: h.value.generation,
                    head_seq,
                    next: Cursor {
                        partition: 0,
                        next_seq: checked_next(head_seq, "head")?,
                    },
                    trim_before_seq,
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
        if let Some(e) = read_terminal(call) {
            return Err(e);
        }
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
                        biased;
                        _ = call.cancellation.cancelled() => return Err(ReadError::Cancelled),
                        _ = tokio::time::sleep_until(call.deadline) => return Err(ReadError::DeadlineExceeded),
                        _ = sleep => {}
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
        let follow_call = CallContext {
            deadline,
            cancellation: call.cancellation.clone(),
        };
        loop {
            tokio::select! {
                biased;
                _ = call.cancellation.cancelled() => return Err(ReadError::Cancelled),
                _ = tokio::time::sleep_until(deadline) => {
                    if tokio::time::Instant::now() >= wait_until {
                        return Ok(FollowPage { page, timed_out: true });
                    }
                    return Err(ReadError::DeadlineExceeded);
                }
                _ = self.wake.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(250)) => {}
            }
            let page = self.read_page(cursor, limits, &follow_call).await?;
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
        if let Some(e) = read_terminal(call) {
            return Err(e);
        }
        let snapshot = timed_read(call, self.store.read_head(&self.resource), "read_head").await?;
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
        let Some(manifest) = self
            .validated_publication(&head.value, call)
            .await
            .map_err(open_to_read)?
        else {
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
        let head_seq = manifest.head_seq;
        let at_head_seq = checked_next(head_seq, "snapshot head")?;
        let at_head_cursor = Cursor {
            partition: 0,
            next_seq: at_head_seq,
        };
        if cursor.next_seq == 0 || cursor.next_seq > at_head_seq {
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
                    next_seq: checked_next(manifest.trim_before_seq, "trim")?,
                },
            });
        }
        if cursor.next_seq == at_head_seq {
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
            let refs = timed_read(
                call,
                catalog::seek_leaf(&self.store, &manifest.catalog, seq),
                "catalog",
            )
            .await?
            .ok_or_else(|| {
                ReadError::Integrity(LogIntegrityError("catalog has no leaf for sequence".into()))
            })?;
            let Some(chunk_ref) = refs
                .iter()
                .find(|r| seq >= r.first_seq && seq <= r.last_seq)
                .cloned()
            else {
                return Err(ReadError::Integrity(LogIntegrityError(
                    "leaf does not cover sequence".into(),
                )));
            };
            let frames = self.load_chunk(&chunk_ref, head_seq, call).await?;
            for frame in frames {
                if frame.seq < seq {
                    continue;
                }
                if frame.seq > head_seq || frame.seq > chunk_ref.last_seq {
                    break;
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
                        || raw.saturating_add(n) > limits.max_raw_bytes.get())
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
                raw = raw.checked_add(n).ok_or_else(|| {
                    ReadError::Integrity(LogIntegrityError("raw byte overflow".into()))
                })?;
                expected = checked_next(frame.seq, "frame")?;
                seq = expected;
                if events.len() >= limits.max_events.get() as usize {
                    break;
                }
            }
            if seq <= chunk_ref.last_seq
                && seq <= head_seq
                && events.len() < limits.max_events.get() as usize
            {
                return Err(ReadError::Integrity(LogIntegrityError(
                    "chunk ended before catalog last_seq".into(),
                )));
            }
        }
        Ok(ReadPage {
            at_head: expected == at_head_seq,
            events,
            next: Cursor {
                partition: 0,
                next_seq: expected,
            },
            snapshot_head: head_seq,
            raw_payload_bytes: raw,
        })
    }

    async fn load_chunk(
        &self,
        chunk: &CatalogChunkRef,
        snapshot_head: u64,
        call: &CallContext,
    ) -> Result<Vec<Frame>, ReadError> {
        let spec = EnvelopeReadSpec {
            tenant: &self.store.tenant,
            kind: ObjectKind::Blob,
            allowed_schemas: CHUNK_SCHEMAS,
            max_encoded_bytes: NonZeroU64::new(MAX_CHUNK_OBJECT_BYTES).unwrap(),
            max_plaintext_bytes: NonZeroU64::new(MAX_CHUNK_PLAINTEXT_BYTES).unwrap(),
        };
        let (payload, _) = timed_read(
            call,
            self.store.get_blob_limited(&chunk.digest, spec),
            "chunk",
        )
        .await?;
        if payload.len() as u64 != chunk.plaintext_bytes {
            return Err(ReadError::Integrity(LogIntegrityError(format!(
                "chunk plaintext {} != catalog {}",
                payload.len(),
                chunk.plaintext_bytes
            ))));
        }
        let body: ChunkBody = serde_json::from_slice(&payload)
            .map_err(|e| ReadError::Integrity(LogIntegrityError(format!("chunk json: {e}"))))?;
        if body.schema != CHUNK_SCHEMA {
            return Err(ReadError::Integrity(LogIntegrityError(
                "chunk schema".into(),
            )));
        }
        if body.frames.is_empty() {
            return Err(ReadError::Integrity(LogIntegrityError(
                "chunk has no frames".into(),
            )));
        }
        if body.frames.len() as u32 != chunk.event_count
            || body.frames.len() as u32 > MAX_CHUNK_EVENTS
        {
            return Err(ReadError::Integrity(LogIntegrityError(
                "chunk event count disagrees with catalog".into(),
            )));
        }
        let mut raw = 0u64;
        let mut expected = chunk.first_seq;
        for frame in &body.frames {
            if frame.seq != expected {
                return Err(ReadError::Integrity(LogIntegrityError(format!(
                    "chunk frames are not contiguous at {}",
                    frame.seq
                ))));
            }
            if frame.seq > snapshot_head || frame.seq > chunk.last_seq {
                return Err(ReadError::Integrity(LogIntegrityError(
                    "chunk frame exceeds snapshot or catalog range".into(),
                )));
            }
            raw = raw.checked_add(frame.payload.len() as u64).ok_or_else(|| {
                ReadError::Integrity(LogIntegrityError("chunk raw overflow".into()))
            })?;
            expected = checked_next(expected, "chunk frame")?;
        }
        let last = expected.checked_sub(1).ok_or_else(|| {
            ReadError::Integrity(LogIntegrityError("chunk sequence underflow".into()))
        })?;
        if last != chunk.last_seq || raw != chunk.raw_payload_bytes {
            return Err(ReadError::Integrity(LogIntegrityError(
                "chunk first/last/raw bytes disagree with catalog".into(),
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
    acquire: TokioMutex<()>,
    renew: StdMutex<Option<tokio::task::JoinHandle<()>>>,
    loss: CancellationToken,
}

impl WriterSession {
    pub fn state(&self) -> WriterState {
        self.state.borrow().clone()
    }

    pub async fn ready(&self, call: &CallContext) -> Result<u64, LeaseError> {
        self.acquire(call).await
    }

    async fn acquire(&self, call: &CallContext) -> Result<u64, LeaseError> {
        if let Some(e) = lease_terminal(call) {
            return Err(e);
        }
        let _guard = tokio::select! {
            biased;
            _ = call.cancellation.cancelled() => return Err(LeaseError::Cancelled),
            _ = tokio::time::sleep_until(call.deadline) => return Err(LeaseError::DeadlineExceeded),
            g = self.acquire.lock() => g,
        };
        if let Some(e) = lease_terminal(call) {
            return Err(e);
        }
        if let Some(epoch) = self.active_epoch()? {
            return Ok(epoch);
        }
        let _ = self.state.send(WriterState::Acquiring { held_until: None });
        let op = self.feed.store.mint_operation();
        let ttl = self.policy.ttl.as_secs().max(1) as i64;
        let acquire_until = tokio::time::Instant::now()
            .checked_add(self.policy.initial_acquire_budget)
            .unwrap_or(call.deadline);
        let deadline = call.deadline.min(acquire_until);
        let acquire_call = CallContext {
            deadline,
            cancellation: call.cancellation.clone(),
        };
        loop {
            if acquire_call.cancellation.is_cancelled() {
                return Err(LeaseError::Cancelled);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(LeaseError::DeadlineExceeded);
            }
            match timed_lease(
                &acquire_call,
                self.feed.store.claim(
                    op,
                    &self.feed.resource,
                    &self.instance.canonical(),
                    ttl,
                    false,
                ),
            )
            .await
            {
                Ok(out) => {
                    let snapshot = timed_lease(
                        &acquire_call,
                        self.feed.store.read_head(&self.feed.resource),
                    )
                    .await?
                    .ok_or(LeaseError::Unavailable)?;
                    let lease = snapshot
                        .value
                        .lease
                        .as_ref()
                        .ok_or(LeaseError::Unavailable)?;
                    if lease.writer != self.instance.canonical()
                        || snapshot.value.epoch != out.epoch
                    {
                        self.lose(SessionLoss::OwnerChanged);
                        return Err(LeaseError::ReacquireRequired {
                            cause: SessionLoss::OwnerChanged,
                        });
                    }
                    let _ = self.state.send(WriterState::Active {
                        epoch: out.epoch,
                        lease_until: lease.lease_until,
                    });
                    self.spawn_renew(out.epoch);
                    return Ok(out.epoch);
                }
                Err(LeaseError::LeaseHeld { owner, until }) => {
                    tokio::select! {
                        biased;
                        _ = acquire_call.cancellation.cancelled() => return Err(LeaseError::Cancelled),
                        _ = tokio::time::sleep_until(deadline) => {
                            return Err(LeaseError::LeaseHeld { owner, until });
                        }
                        _ = tokio::time::sleep(Duration::from_millis(200)) => {}
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn active_epoch(&self) -> Result<Option<u64>, LeaseError> {
        let now = self.feed.store.clock().now();
        let slack = chrono::Duration::from_std(self.policy.clock_slack)
            .unwrap_or_else(|_| chrono::Duration::seconds(0));
        let st = self.state.borrow().clone();
        match st {
            WriterState::Active { epoch, lease_until } => {
                if lease_until <= now + slack {
                    self.lose(SessionLoss::LeaseExpired);
                    return Err(LeaseError::ReacquireRequired {
                        cause: SessionLoss::LeaseExpired,
                    });
                }
                Ok(Some(epoch))
            }
            WriterState::Lost { cause, .. } => Err(LeaseError::ReacquireRequired { cause }),
            WriterState::Closed => Err(LeaseError::ReacquireRequired {
                cause: SessionLoss::OwnerChanged,
            }),
            _ => Ok(None),
        }
    }

    fn lose(&self, cause: SessionLoss) {
        self.loss.cancel();
        self.abort_renew();
        let st = self.state.borrow().clone();
        let epoch = match st {
            WriterState::Active { epoch, .. } => Some(epoch),
            WriterState::Lost { epoch, .. } => epoch,
            _ => None,
        };
        let _ = self.state.send(WriterState::Lost { epoch, cause });
    }

    fn take_renew(&self) -> Option<tokio::task::JoinHandle<()>> {
        self.renew.lock().ok().and_then(|mut slot| slot.take())
    }

    fn abort_renew(&self) {
        if let Some(h) = self.take_renew() {
            h.abort();
        }
    }

    async fn stop_renew(&self) {
        self.abort_renew();
        tokio::task::yield_now().await;
    }

    fn spawn_renew(&self, epoch: u64) {
        let mut slot = match self.renew.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        if slot.is_some() {
            return;
        }
        let store = self.feed.store.clone();
        let resource = self.feed.resource.clone();
        let instance = self.instance.canonical();
        let policy = self.policy;
        let state = self.state.clone();
        let loss = self.loss.clone();
        *slot = Some(tokio::spawn(async move {
            let ttl = policy.ttl.as_secs().max(1) as i64;
            let slack = chrono::Duration::from_std(policy.clock_slack)
                .unwrap_or_else(|_| chrono::Duration::seconds(0));
            loop {
                tokio::time::sleep(policy.renew_every).await;
                let now = store.clock().now();
                let st = state.borrow().clone();
                let WriterState::Active {
                    epoch: e,
                    lease_until,
                } = st
                else {
                    return;
                };
                if e != epoch {
                    return;
                }
                if lease_until <= now + slack {
                    loss.cancel();
                    let _ = state.send(WriterState::Lost {
                        epoch: Some(epoch),
                        cause: SessionLoss::LeaseExpired,
                    });
                    return;
                }
                let remaining = match (lease_until - slack - now).to_std() {
                    Ok(d) if !d.is_zero() => d,
                    _ => {
                        loss.cancel();
                        let _ = state.send(WriterState::Lost {
                            epoch: Some(epoch),
                            cause: SessionLoss::LeaseExpired,
                        });
                        return;
                    }
                };
                let renew = store.renew_owned_lease(&resource, &instance, epoch, ttl);
                tokio::pin!(renew);
                let result = tokio::select! {
                    biased;
                    _ = tokio::time::sleep(remaining) => {
                        loss.cancel();
                        let _ = state.send(WriterState::Lost {
                            epoch: Some(epoch),
                            cause: SessionLoss::LeaseExpired,
                        });
                        return;
                    }
                    r = &mut renew => r,
                };
                match result {
                    Ok(value) => {
                        let Some(lease) = value.lease.as_ref() else {
                            loss.cancel();
                            let _ = state.send(WriterState::Lost {
                                epoch: Some(epoch),
                                cause: SessionLoss::RenewalUncertain,
                            });
                            return;
                        };
                        if lease.writer != instance || value.epoch != epoch {
                            loss.cancel();
                            let _ = state.send(WriterState::Lost {
                                epoch: Some(epoch),
                                cause: SessionLoss::OwnerChanged,
                            });
                            return;
                        }
                        let _ = state.send(WriterState::Active {
                            epoch,
                            lease_until: lease.lease_until,
                        });
                    }
                    Err(e) => {
                        let cause = match e.downcast_ref::<CoreError>() {
                            Some(CoreError::Fenced { live, .. }) => {
                                SessionLoss::Fenced { live_epoch: *live }
                            }
                            Some(CoreError::LeaseExpired) => SessionLoss::LeaseExpired,
                            Some(CoreError::LeaseHeld { .. }) => SessionLoss::OwnerChanged,
                            _ => SessionLoss::RenewalUncertain,
                        };
                        loss.cancel();
                        let _ = state.send(WriterState::Lost {
                            epoch: Some(epoch),
                            cause,
                        });
                        return;
                    }
                }
            }
        }));
    }

    async fn lookup_committed(
        &self,
        snapshot: &HeadSnapshot,
        key: &StableKey,
        payload_hash: &Digest,
        call: &CallContext,
    ) -> Result<Option<StableAppendReceipt>, StableAppendError> {
        let Some(manifest) = self
            .feed
            .validated_publication(&snapshot.value, call)
            .await
            .map_err(open_to_append)?
        else {
            return Ok(None);
        };
        let found = timed_read(
            call,
            hamt::lookup(
                &self.feed.store,
                &manifest.stable_index,
                key,
                &self.feed.logical,
                IndexHead {
                    generation: snapshot.value.generation,
                    head_seq: manifest.head_seq,
                },
            ),
            "stable-index",
        )
        .await
        .map_err(read_to_append)?;
        match found {
            Lookup::Found(e) if e.payload_hash == *payload_hash => Ok(Some(StableAppendReceipt {
                payload_hash: payload_hash.clone(),
                range: AppendRange {
                    first: e.first,
                    last: e.last,
                },
                generation: e.generation,
            })),
            Lookup::Found(e) => Err(StableAppendError::StableKeyConflict {
                existing: e.payload_hash,
                supplied: payload_hash.clone(),
            }),
            Lookup::Absent => Ok(None),
        }
    }

    pub async fn append_stable(
        &self,
        key: StableKey,
        payload: Bytes,
        call: &CallContext,
    ) -> Result<StableAppendReceipt, StableAppendError> {
        if payload.is_empty() || payload.len() as u64 > MAX_CHUNK_RAW_BYTES {
            return Err(StableAppendError::InvalidInput);
        }
        if let Some(e) = lease_terminal(call) {
            return Err(match e {
                LeaseError::Cancelled => StableAppendError::Cancelled,
                LeaseError::DeadlineExceeded => StableAppendError::DeadlineExceeded,
                other => StableAppendError::Lease(other),
            });
        }
        let payload_hash = StableKey::payload_hash(&self.feed.store.key, &payload);
        for _ in 0..32 {
            if call.cancellation.is_cancelled() {
                return Err(StableAppendError::Cancelled);
            }
            if tokio::time::Instant::now() >= call.deadline {
                return Err(StableAppendError::DeadlineExceeded);
            }
            let snapshot = timed_lease(call, self.feed.store.read_head(&self.feed.resource))
                .await
                .map_err(StableAppendError::Lease)?
                .unwrap_or_else(|| HeadSnapshot {
                    value: RefValue::new(&self.feed.store.tenant, &self.feed.resource),
                    version: None,
                });
            if let Some(receipt) = self
                .lookup_committed(&snapshot, &key, &payload_hash, call)
                .await?
            {
                return Ok(receipt);
            }
            let epoch = self.acquire(call).await.map_err(StableAppendError::Lease)?;
            let snapshot = timed_lease(call, self.feed.store.read_head(&self.feed.resource))
                .await
                .map_err(StableAppendError::Lease)?
                .ok_or(StableAppendError::Unavailable)?;
            let lease = snapshot.value.lease.as_ref();
            if lease.is_none()
                || lease.map(|l| l.writer.as_str()) != Some(self.instance.canonical().as_str())
                || snapshot.value.epoch != epoch
                || !snapshot.value.lease_live(self.feed.store.clock().now())
            {
                self.lose(SessionLoss::OwnerChanged);
                return Err(StableAppendError::Lease(LeaseError::ReacquireRequired {
                    cause: SessionLoss::OwnerChanged,
                }));
            }
            if let Some(receipt) = self
                .lookup_committed(&snapshot, &key, &payload_hash, call)
                .await?
            {
                return Ok(receipt);
            }
            match timed_cas(
                call,
                &self.loss,
                self.feed.store.commit_at_snapshot(
                    OpIdentity::Stable(key.clone()),
                    CompleteAppendPlan {
                        resource: self.feed.resource.clone(),
                        logical: self.feed.logical.clone(),
                        instance: self.instance.canonical(),
                        epoch,
                        key: key.clone(),
                        payload: payload.to_vec(),
                        payload_hash: payload_hash.clone(),
                        loss: self.loss.clone(),
                    },
                    snapshot,
                ),
            )
            .await
            {
                Ok(CasResult::Committed(p)) => {
                    self.feed.wake.notify_waiters();
                    return Ok(p.outcome);
                }
                Ok(CasResult::Conflict) => continue,
                Err(StableAppendError::Lease(LeaseError::Fenced {
                    session_epoch,
                    live_epoch,
                })) => {
                    self.lose(SessionLoss::Fenced { live_epoch });
                    return Err(StableAppendError::Lease(LeaseError::Fenced {
                        session_epoch,
                        live_epoch,
                    }));
                }
                Err(StableAppendError::Lease(LeaseError::LeaseHeld { .. })) => {
                    self.lose(SessionLoss::OwnerChanged);
                    return Err(StableAppendError::Lease(LeaseError::ReacquireRequired {
                        cause: SessionLoss::OwnerChanged,
                    }));
                }
                Err(StableAppendError::Lease(LeaseError::ReacquireRequired { cause })) => {
                    self.lose(cause.clone());
                    return Err(StableAppendError::Lease(LeaseError::ReacquireRequired {
                        cause,
                    }));
                }
                Err(e) => return Err(e),
            }
        }
        Err(StableAppendError::Unavailable)
    }

    pub async fn close(self, call: &CallContext) -> Result<(), LeaseError> {
        self.loss.cancel();
        self.stop_renew().await;
        let st = self.state.borrow().clone();
        let epoch = match st {
            WriterState::Active { epoch, .. } => epoch,
            _ => {
                let _ = self.state.send(WriterState::Closed);
                return Ok(());
            }
        };
        let op = self.feed.store.mint_operation();
        let result = timed_lease(
            call,
            self.feed.store.release_owned(
                op,
                &self.feed.resource,
                &self.instance.canonical(),
                epoch,
            ),
        )
        .await;
        let _ = self.state.send(WriterState::Closed);
        result.map(|_| ())
    }
}

impl Drop for WriterSession {
    fn drop(&mut self) {
        self.abort_renew();
    }
}

async fn timed_cas<T>(
    call: &CallContext,
    loss: &CancellationToken,
    fut: impl std::future::Future<Output = Result<T>>,
) -> Result<T, StableAppendError> {
    if call.cancellation.is_cancelled() {
        return Err(StableAppendError::Cancelled);
    }
    if loss.is_cancelled() {
        return Err(StableAppendError::Lease(LeaseError::ReacquireRequired {
            cause: SessionLoss::LeaseExpired,
        }));
    }
    if tokio::time::Instant::now() >= call.deadline {
        return Err(StableAppendError::DeadlineExceeded);
    }
    tokio::select! {
        biased;
        _ = call.cancellation.cancelled() => Err(StableAppendError::Cancelled),
        _ = loss.cancelled() => Err(StableAppendError::Lease(LeaseError::ReacquireRequired {
            cause: SessionLoss::LeaseExpired,
        })),
        _ = tokio::time::sleep_until(call.deadline) => Err(StableAppendError::DeadlineExceeded),
        r = fut => r.map_err(cas_from_anyhow),
    }
}

fn cas_from_anyhow(e: anyhow::Error) -> StableAppendError {
    match e.downcast_ref::<CoreError>() {
        Some(CoreError::BackendUnavailable(_)) | Some(CoreError::Io(_)) => {
            StableAppendError::Unavailable
        }
        Some(CoreError::Fenced { caller, live }) => StableAppendError::Lease(LeaseError::Fenced {
            session_epoch: *caller,
            live_epoch: *live,
        }),
        Some(CoreError::LeaseExpired) => StableAppendError::Lease(LeaseError::ReacquireRequired {
            cause: SessionLoss::LeaseExpired,
        }),
        Some(CoreError::LeaseHeld { holder, until }) => {
            let owner = WriterInstanceId::try_from_canonical(holder)
                .unwrap_or_else(WriterInstanceId::generate);
            let until = DateTime::parse_from_rfc3339(until)
                .map(|d| d.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());
            StableAppendError::Lease(LeaseError::LeaseHeld { owner, until })
        }
        Some(CoreError::ObjectTooLarge { .. })
        | Some(CoreError::IntegrityError(_))
        | Some(CoreError::InvalidFormat(_))
        | Some(CoreError::RecoveryFailed(_))
        | Some(CoreError::Rejected(_)) => {
            StableAppendError::Integrity(LogIntegrityError(format!("{e:#}")))
        }
        _ => StableAppendError::Unavailable,
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
    loss: CancellationToken,
}

impl RefMutationPlan for CompleteAppendPlan {
    type Outcome = StableAppendReceipt;

    fn resource(&self) -> &str {
        &self.resource
    }

    fn live_lease(&self) -> Option<LiveLeaseGuard> {
        Some(LiveLeaseGuard {
            writer: self.instance.clone(),
            epoch: self.epoch,
            cancel: self.loss.clone(),
        })
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
        let bound = if current.generation == 0
            && current.head_commit.is_none()
            && current.target.is_none()
        {
            None
        } else {
            let commit = current.head_commit.as_ref().ok_or_else(|| {
                CoreError::IntegrityError("missing head_commit with published generation".into())
            })?;
            let view = ctx.store.load_commit_view(commit).await?;
            bind_published_target(&self.resource, current, &view)?
        };
        let (mut catalog, mut index, mut head_seq) = match bound {
            None => (
                CatalogState::Empty,
                hamt::empty_root(ctx.store).await?,
                0u64,
            ),
            Some(digest) => {
                let spec = EnvelopeReadSpec {
                    tenant: &ctx.store.tenant,
                    kind: ObjectKind::Blob,
                    allowed_schemas: MANIFEST_SCHEMAS,
                    max_encoded_bytes: NonZeroU64::new(MAX_MANIFEST_OBJECT_BYTES).unwrap(),
                    max_plaintext_bytes: NonZeroU64::new(MAX_MANIFEST_OBJECT_BYTES).unwrap(),
                };
                let (payload, _) = ctx.store.get_blob_limited(&digest, spec).await?;
                let m: CompleteLogManifest = serde_json::from_slice(&payload)?;
                validate_complete_manifest(&self.resource, &m)?;
                (m.catalog, m.stable_index, m.head_seq)
            }
        };
        let seq = head_seq
            .checked_add(1)
            .ok_or_else(|| CoreError::IntegrityError("sequence overflow".into()))?;
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
        let chunk_env = Envelope::new(
            &ctx.store.tenant,
            ObjectKind::Blob,
            CHUNK_SCHEMA,
            chunk_bytes.clone(),
            &ctx.store.key,
        );
        let chunk_encoded = chunk_env.encode()?;
        if chunk_encoded.len() as u64 > MAX_CHUNK_OBJECT_BYTES {
            return Err(CoreError::ObjectTooLarge {
                key: String::new(),
                limit: MAX_CHUNK_OBJECT_BYTES,
                actual: Some(chunk_encoded.len() as u64),
            }
            .into());
        }
        let chunk_digest = chunk_env.meta.digest.clone();
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
        let manifest_env = Envelope::new(
            &ctx.store.tenant,
            ObjectKind::Blob,
            COMPLETE_MANIFEST_SCHEMA,
            manifest_bytes.clone(),
            &ctx.store.key,
        );
        let encoded_manifest = manifest_env.encode()?;
        if encoded_manifest.len() as u64 > MAX_MANIFEST_OBJECT_BYTES {
            return Err(CoreError::ObjectTooLarge {
                key: String::new(),
                limit: MAX_MANIFEST_OBJECT_BYTES,
                actual: Some(encoded_manifest.len() as u64),
            }
            .into());
        }
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
            live_lease: Some(LiveLeaseGuard {
                writer: self.instance.clone(),
                epoch: self.epoch,
                cancel: self.loss.clone(),
            }),
        })
    }
}

fn open_terminal(call: &CallContext) -> Option<OpenLogError> {
    if call.cancellation.is_cancelled() {
        Some(OpenLogError::Cancelled)
    } else if tokio::time::Instant::now() >= call.deadline {
        Some(OpenLogError::DeadlineExceeded)
    } else {
        None
    }
}

fn read_terminal(call: &CallContext) -> Option<ReadError> {
    if call.cancellation.is_cancelled() {
        Some(ReadError::Cancelled)
    } else if tokio::time::Instant::now() >= call.deadline {
        Some(ReadError::DeadlineExceeded)
    } else {
        None
    }
}

fn lease_terminal(call: &CallContext) -> Option<LeaseError> {
    if call.cancellation.is_cancelled() {
        Some(LeaseError::Cancelled)
    } else if tokio::time::Instant::now() >= call.deadline {
        Some(LeaseError::DeadlineExceeded)
    } else {
        None
    }
}

async fn timed<T>(
    call: &CallContext,
    fut: impl std::future::Future<Output = Result<T>>,
) -> Result<T, OpenLogError> {
    if let Some(e) = open_terminal(call) {
        return Err(e);
    }
    tokio::select! {
        biased;
        _ = call.cancellation.cancelled() => Err(OpenLogError::Cancelled),
        _ = tokio::time::sleep_until(call.deadline) => Err(OpenLogError::DeadlineExceeded),
        r = fut => r.map_err(open_from_anyhow),
    }
}

async fn timed_read<T>(
    call: &CallContext,
    fut: impl std::future::Future<Output = Result<T>>,
    operation: &'static str,
) -> Result<T, ReadError> {
    if let Some(e) = read_terminal(call) {
        return Err(e);
    }
    tokio::select! {
        biased;
        _ = call.cancellation.cancelled() => Err(ReadError::Cancelled),
        _ = tokio::time::sleep_until(call.deadline) => Err(ReadError::DeadlineExceeded),
        r = fut => r.map_err(|e| read_from_anyhow(e, operation)),
    }
}

async fn timed_lease<T>(
    call: &CallContext,
    fut: impl std::future::Future<Output = Result<T>>,
) -> Result<T, LeaseError> {
    if let Some(e) = lease_terminal(call) {
        return Err(e);
    }
    tokio::select! {
        biased;
        _ = call.cancellation.cancelled() => Err(LeaseError::Cancelled),
        _ = tokio::time::sleep_until(call.deadline) => Err(LeaseError::DeadlineExceeded),
        r = fut => r.map_err(lease_from_anyhow),
    }
}

fn open_from_anyhow(e: anyhow::Error) -> OpenLogError {
    match e.downcast_ref::<CoreError>() {
        Some(CoreError::BackendUnavailable(_)) | Some(CoreError::Io(_)) => {
            OpenLogError::Unavailable
        }
        Some(CoreError::UnsupportedEnvelopeFormat { value, .. }) => {
            OpenLogError::UnsupportedManifestSchema {
                found: value.clone(),
                required: COMPLETE_MANIFEST_SCHEMA,
            }
        }
        Some(
            CoreError::IntegrityError(m)
            | CoreError::InvalidFormat(m)
            | CoreError::RecoveryFailed(m),
        ) => OpenLogError::Integrity(LogIntegrityError(m.clone())),
        Some(CoreError::ObjectTooLarge { .. }) | Some(CoreError::NotFound(_)) => {
            OpenLogError::Integrity(LogIntegrityError(format!("{e:#}")))
        }
        _ => OpenLogError::Integrity(LogIntegrityError(format!("{e:#}"))),
    }
}

fn read_from_anyhow(e: anyhow::Error, operation: &'static str) -> ReadError {
    match e.downcast_ref::<CoreError>() {
        Some(CoreError::BackendUnavailable(_)) | Some(CoreError::Io(_)) => {
            ReadError::Unavailable { operation }
        }
        Some(CoreError::Fenced { .. }) | Some(CoreError::Rejected(_)) => {
            ReadError::Integrity(LogIntegrityError(format!("{e:#}")))
        }
        Some(
            CoreError::IntegrityError(m)
            | CoreError::InvalidFormat(m)
            | CoreError::RecoveryFailed(m),
        ) => ReadError::Integrity(LogIntegrityError(m.clone())),
        _ => ReadError::Integrity(LogIntegrityError(format!("{e:#}"))),
    }
}

fn lease_from_anyhow(e: anyhow::Error) -> LeaseError {
    match e.downcast_ref::<CoreError>() {
        Some(CoreError::BackendUnavailable(_)) | Some(CoreError::Io(_)) => LeaseError::Unavailable,
        Some(CoreError::Fenced { caller, live }) => LeaseError::Fenced {
            session_epoch: *caller,
            live_epoch: *live,
        },
        Some(CoreError::LeaseHeld { holder, until }) => {
            let owner = WriterInstanceId::try_from_canonical(holder)
                .unwrap_or_else(WriterInstanceId::generate);
            let until = DateTime::parse_from_rfc3339(until)
                .map(|d| d.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());
            LeaseError::LeaseHeld { owner, until }
        }
        Some(CoreError::LeaseExpired) => LeaseError::ReacquireRequired {
            cause: SessionLoss::LeaseExpired,
        },
        _ => LeaseError::Unavailable,
    }
}

fn open_to_append(e: OpenLogError) -> StableAppendError {
    match e {
        OpenLogError::Integrity(i) => StableAppendError::Integrity(i),
        OpenLogError::DeadlineExceeded => StableAppendError::DeadlineExceeded,
        OpenLogError::Cancelled => StableAppendError::Cancelled,
        _ => StableAppendError::Unavailable,
    }
}

fn read_to_append(e: ReadError) -> StableAppendError {
    match e {
        ReadError::Integrity(i) => StableAppendError::Integrity(i),
        ReadError::DeadlineExceeded => StableAppendError::DeadlineExceeded,
        ReadError::Cancelled => StableAppendError::Cancelled,
        ReadError::Unavailable { .. } => StableAppendError::Unavailable,
        other => StableAppendError::Integrity(LogIntegrityError(format!("{other:?}"))),
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

fn checked_next(seq: u64, what: &str) -> Result<u64, ReadError> {
    seq.checked_add(1)
        .ok_or_else(|| ReadError::Integrity(LogIntegrityError(format!("{what} sequence overflow"))))
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

    #[test]
    fn overflowing_lease_policy_is_rejected() {
        let mut policy = LeasePolicy::bridge_default();
        policy.renew_every = Duration::MAX;
        assert!(policy.validate().is_err());
    }

    #[tokio::test]
    async fn oversized_acquire_budget_respects_caller_deadline() {
        let feed = CompleteFeed::open(store(), "budget".into(), &call())
            .await
            .unwrap();
        let policy = LeasePolicy {
            initial_acquire_budget: Duration::MAX,
            ..LeasePolicy::bridge_default()
        };
        let first = feed
            .writer_session(WriterLabel::try_from("first").unwrap(), policy.clone())
            .unwrap();
        first.ready(&call()).await.unwrap();
        let second = feed
            .writer_session(WriterLabel::try_from("second").unwrap(), policy)
            .unwrap();
        let bounded_call = CallContext::new(
            tokio::time::Instant::now() + Duration::from_millis(25),
            CancellationToken::new(),
        );
        let result = tokio::time::timeout(Duration::from_secs(1), second.ready(&bounded_call))
            .await
            .expect("acquisition must remain bounded by the caller");
        assert!(matches!(
            result,
            Err(LeaseError::DeadlineExceeded) | Err(LeaseError::LeaseHeld { .. })
        ));
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

    fn short_policy() -> LeasePolicy {
        LeasePolicy {
            ttl: Duration::from_secs(2),
            renew_every: Duration::from_millis(200),
            clock_slack: Duration::from_millis(100),
            initial_acquire_budget: Duration::from_secs(2),
        }
        .validate()
        .unwrap()
    }

    #[tokio::test]
    async fn all_0xff_payload_stays_within_encoded_upload_cap() {
        let store = store();
        let feed = CompleteFeed::open(store, "ff".into(), &call())
            .await
            .unwrap();
        let writer = feed
            .writer_session(WriterLabel::try_from("bridge").unwrap(), short_policy())
            .unwrap();
        let payload = Bytes::from(vec![0xff; 64 * 1024]);
        let key = StableKey::try_from_canonical(b"ff-key".to_vec()).unwrap();
        let receipt = writer
            .append_stable(key, payload.clone(), &call())
            .await
            .unwrap();
        assert_eq!(receipt.range.first, 1);
        let page = LogReader::read_page(
            &feed,
            Cursor::first(0),
            ReadLimits::try_new(1, MAX_PAGE_RAW_BYTES).unwrap(),
            &call(),
        )
        .await
        .unwrap();
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.events[0].payload.as_ref(), payload.as_ref());
        let over = writer
            .append_stable(
                StableKey::try_from_canonical(b"too-big".to_vec()).unwrap(),
                Bytes::from(vec![0xff; MAX_CHUNK_RAW_BYTES as usize + 1]),
                &call(),
            )
            .await;
        assert!(matches!(over, Err(StableAppendError::InvalidInput)));
    }

    #[tokio::test]
    async fn first_event_too_large_does_not_advance_cursor() {
        let store = store();
        let feed = CompleteFeed::open(store, "big".into(), &call())
            .await
            .unwrap();
        let writer = feed
            .writer_session(WriterLabel::try_from("bridge").unwrap(), short_policy())
            .unwrap();
        let key = StableKey::try_from_canonical(b"big1".to_vec()).unwrap();
        writer
            .append_stable(key, Bytes::from(vec![b'x'; 64]), &call())
            .await
            .unwrap();
        let cursor = Cursor::first(0);
        let err = LogReader::read_page(&feed, cursor, ReadLimits::try_new(8, 8).unwrap(), &call())
            .await
            .unwrap_err();
        match err {
            ReadError::EventTooLarge {
                cursor: got,
                event_bytes,
                max_bytes,
                ..
            } => {
                assert_eq!(got, cursor);
                assert_eq!(event_bytes, 64);
                assert_eq!(max_bytes, 8);
            }
            other => panic!("expected EventTooLarge, got {other:?}"),
        }
        let page = LogReader::read_page(
            &feed,
            cursor,
            ReadLimits::try_new(8, 4096).unwrap(),
            &call(),
        )
        .await
        .unwrap();
        assert_eq!(page.events[0].position.seq, 1);
        assert_eq!(page.next.next_seq, 2);
    }

    #[tokio::test]
    async fn idle_renewal_extends_lease_without_new_session() {
        let store = store();
        let feed = CompleteFeed::open(store, "idle".into(), &call())
            .await
            .unwrap();
        let writer = feed
            .writer_session(WriterLabel::try_from("bridge").unwrap(), short_policy())
            .unwrap();
        writer.ready(&call()).await.unwrap();
        let WriterState::Active {
            epoch,
            lease_until: first,
        } = writer.state()
        else {
            panic!("expected active");
        };
        tokio::time::sleep(Duration::from_millis(500)).await;
        match writer.state() {
            WriterState::Active {
                epoch: e2,
                lease_until: second,
            } => {
                assert_eq!(e2, epoch);
                assert!(second > first, "renewal must extend lease_until");
            }
            other => panic!("expected still active after idle renew, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fresh_instance_takes_over_after_close() {
        let store = store();
        let feed = CompleteFeed::open(store, "take-close".into(), &call())
            .await
            .unwrap();
        let a = feed
            .writer_session(WriterLabel::try_from("bridge").unwrap(), short_policy())
            .unwrap();
        a.ready(&call()).await.unwrap();
        a.close(&call()).await.unwrap();
        let b = feed
            .writer_session(WriterLabel::try_from("bridge").unwrap(), short_policy())
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), b.ready(&call()))
            .await
            .expect("takeover after close timed out")
            .unwrap();
    }

    #[tokio::test]
    async fn fresh_instance_takes_over_after_expiry() {
        let store = store();
        let feed = CompleteFeed::open(store, "take-exp".into(), &call())
            .await
            .unwrap();
        let policy = short_policy();
        let c = feed
            .writer_session(WriterLabel::try_from("bridge").unwrap(), policy)
            .unwrap();
        c.ready(&call()).await.unwrap();
        c.abort_renew();
        tokio::time::sleep(Duration::from_millis(2500)).await;
        match c.state() {
            WriterState::Active { lease_until, .. } => {
                assert!(
                    lease_until <= Utc::now(),
                    "stopped renew must not extend past ttl, lease_until={lease_until}"
                );
            }
            WriterState::Lost {
                cause: SessionLoss::LeaseExpired,
                ..
            } => {}
            other => panic!("expected expired active or lost, got {other:?}"),
        }
        let err = c.ready(&call()).await.unwrap_err();
        assert!(
            matches!(
                err,
                LeaseError::ReacquireRequired {
                    cause: SessionLoss::LeaseExpired
                }
            ),
            "{err:?}"
        );
        let d = feed
            .writer_session(WriterLabel::try_from("bridge").unwrap(), policy)
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), d.ready(&call()))
            .await
            .expect("takeover after expiry timed out")
            .unwrap();
    }

    #[tokio::test]
    async fn close_of_unacquired_session_does_not_deadlock() {
        let store = store();
        let feed = CompleteFeed::open(store, "close".into(), &call())
            .await
            .unwrap();
        let writer = feed
            .writer_session(WriterLabel::try_from("bridge").unwrap(), short_policy())
            .unwrap();
        writer.close(&call()).await.unwrap();
    }
}
