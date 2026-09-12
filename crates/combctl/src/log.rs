//! Comb Log: retry-safe append, group commit, and complete-feed stable keys.

use crate::hamt::{self, IndexHead, Lookup, StableIndexEntry, StableIndexRoot};
use crate::publish::{
    CasResult, Companion, HeadSnapshot, PrepareCtx, PreparedMutation, RefMutationPlan, Upload,
};
use crate::store::Store;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, Utc};
use comb_core::commit::{Admission, CommitHeader};
use comb_core::error::CoreError;
use comb_core::operation::{Material, OpIdentity, OperationId, StableKey};
use comb_core::{Digest, Lease, ObjectKind, RefValue};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const MANIFEST_SCHEMA: &str = "comb.log.partition-manifest/v2";
pub const CHUNK_SCHEMA: &str = "comb.log.chunk/v1";
pub const MAX_APPEND_EVENTS: usize = 2048;
pub const MAX_APPEND_BYTES: usize = 512 * 1024;
pub const MAX_ADMISSIONS: usize = 2048;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RetentionMode {
    Trimmable,
    Complete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedMode {
    Trimmable,
    Complete,
}

impl From<FeedMode> for RetentionMode {
    fn from(m: FeedMode) -> Self {
        match m {
            FeedMode::Trimmable => RetentionMode::Trimmable,
            FeedMode::Complete => RetentionMode::Complete,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkRef {
    pub digest: Digest,
    pub first: u64,
    pub last: u64,
    pub bytes: u64,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogManifest {
    pub schema: String,
    pub header: CommitHeader,
    pub log: String,
    pub epoch: u64,
    pub head_seq: u64,
    pub chunks: Vec<ChunkRef>,
    #[serde(default)]
    pub segments: Vec<ChunkRef>,
    #[serde(default)]
    pub trim_before_seq: u64,
    #[serde(default)]
    pub admitted: Vec<Admission>,
    pub retention: RetentionMode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stable_index: Option<StableIndexRoot>,
    #[serde(default)]
    pub stable_admissions: Vec<StableIndexEntry>,
    #[serde(default)]
    pub result: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ref_state: Option<RefValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Frame {
    pub seq: u64,
    pub at: DateTime<Utc>,
    #[serde(with = "hex_payload")]
    pub payload: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChunkBody {
    schema: String,
    frames: Vec<Frame>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendRange {
    pub first: u64,
    pub last: u64,
}

#[derive(Debug, Clone)]
pub struct Appended {
    pub first: u64,
    pub last: u64,
    pub generation: u64,
    pub first_delivery: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StableAppendReceipt {
    pub payload_hash: Digest,
    pub range: AppendRange,
    pub generation: u64,
}

#[derive(Debug, Clone)]
pub struct LogHead {
    pub head_seq: u64,
    pub next_seq: u64,
    pub epoch: u64,
    pub generation: u64,
}

pub struct LogStore<'a> {
    pub store: &'a Store,
    pub name: String,
    pub logical: String,
    pub feed: FeedMode,
}

impl<'a> LogStore<'a> {
    pub fn new(store: &'a Store, name: &str) -> Self {
        Self {
            store,
            logical: name.to_string(),
            name: format!("log/{name}/p0"),
            feed: FeedMode::Trimmable,
        }
    }

    pub fn complete_feed(store: &'a Store, name: &str) -> Self {
        Self {
            store,
            logical: name.to_string(),
            name: format!("log/{name}/p0"),
            feed: FeedMode::Complete,
        }
    }

    pub async fn append(
        &self,
        op: OperationId,
        writer: &str,
        payloads: &[Vec<u8>],
        lease_secs: i64,
    ) -> Result<Appended> {
        check_payloads(payloads)?;
        let published = self
            .store
            .publish(
                OpIdentity::Generic(op),
                AppendPlan {
                    resource: self.name.clone(),
                    logical: self.logical.clone(),
                    writer: writer.to_string(),
                    lease_secs,
                    payloads: payloads.to_vec(),
                    feed: self.feed,
                },
            )
            .await?;
        Ok(Appended {
            first: published.outcome.first,
            last: published.outcome.last,
            generation: published.generation,
            first_delivery: published.first_delivery,
        })
    }

    pub async fn head(&self) -> Result<LogHead> {
        match self.status().await? {
            None => Ok(LogHead {
                head_seq: 0,
                next_seq: 1,
                epoch: 0,
                generation: 0,
            }),
            Some((value, manifest)) => Ok(LogHead {
                head_seq: manifest.head_seq,
                next_seq: manifest.head_seq.saturating_add(1).max(1),
                epoch: value.epoch,
                generation: value.generation,
            }),
        }
    }

    pub async fn append_stable(
        &self,
        key: StableKey,
        writer: &str,
        payload: &[u8],
        lease_secs: i64,
    ) -> Result<StableAppendReceipt> {
        if self.feed != FeedMode::Complete {
            return Err(CoreError::Rejected(
                "stable append requires a retained complete feed".into(),
            )
            .into());
        }
        if payload.is_empty() {
            return Err(CoreError::Rejected("empty append".into()).into());
        }
        check_payloads(&[payload.to_vec()])?;
        let payload_hash = StableKey::payload_hash(&self.store.key, payload);
        for _ in 0..128 {
            let snapshot =
                self.store
                    .read_head(&self.name)
                    .await?
                    .unwrap_or_else(|| HeadSnapshot {
                        value: RefValue::new(&self.store.tenant, &self.name),
                        version: None,
                    });
            let found = if snapshot.value.target.is_none() {
                Lookup::Absent
            } else {
                let index = self.index_from_snapshot(&snapshot).await?;
                let domain = self.load_domain(&snapshot.value).await?;
                hamt::lookup(
                    self.store,
                    &index,
                    &key,
                    &self.logical,
                    IndexHead {
                        generation: snapshot.value.generation,
                        head_seq: domain.head_seq,
                    },
                )
                .await?
            };
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
                    return Err(CoreError::StableKeyConflict {
                        existing: e.payload_hash.to_string(),
                        supplied: payload_hash.to_string(),
                    }
                    .into());
                }
                Lookup::Absent => {
                    match self
                        .store
                        .commit_at_snapshot(
                            OpIdentity::Stable(key.clone()),
                            StableAppendPlan {
                                resource: self.name.clone(),
                                logical: self.logical.clone(),
                                writer: writer.to_string(),
                                lease_secs,
                                key: key.clone(),
                                payload: payload.to_vec(),
                                payload_hash: payload_hash.clone(),
                            },
                            snapshot,
                        )
                        .await
                    {
                        Ok(CasResult::Committed(p)) => return Ok(p.outcome),
                        Ok(CasResult::Conflict) => continue,
                        Err(e) if is_retryable(&e) => continue,
                        Err(e) => return Err(e),
                    }
                }
            }
        }
        Err(anyhow!("stable append did not converge"))
    }

    pub async fn steal(&self, op: OperationId, writer: &str, lease_secs: i64) -> Result<u64> {
        let published = self
            .store
            .publish(
                OpIdentity::Generic(op),
                TakeoverPlan {
                    resource: self.name.clone(),
                    writer: writer.to_string(),
                    lease_secs,
                },
            )
            .await?;
        Ok(published.epoch)
    }

    pub async fn compact(&self, op: OperationId) -> Result<usize> {
        let snapshot = match self.store.read_head(&self.name).await? {
            None => return Ok(0),
            Some(s) => s,
        };
        let domain = self.load_domain(&snapshot.value).await?;
        let plan = CompactPlan {
            resource: self.name.clone(),
            feed: self.feed,
        };
        let identity = OpIdentity::Generic(op);
        if domain.chunks.len() < 2 {
            // A prior compaction may itself have emptied the chunk list.
            // Recover it before treating this request as a fresh no-op.
            return Ok(self
                .store
                .recover_completed(&identity, &plan)
                .await?
                .map_or(0, |published| published.outcome));
        }
        let published = self.store.publish(identity, plan).await?;
        Ok(published.outcome)
    }

    pub async fn trim_before(&self, op: OperationId, seq: u64) -> Result<u64> {
        if self.feed == FeedMode::Complete {
            return Err(CoreError::Rejected(
                "complete-feed mode rejects trim; stable-key evidence would be weakened".into(),
            )
            .into());
        }
        let published = self
            .store
            .publish(
                OpIdentity::Generic(op),
                TrimPlan {
                    resource: self.name.clone(),
                    before: seq,
                },
            )
            .await?;
        Ok(published.outcome)
    }

    pub async fn status(&self) -> Result<Option<(RefValue, LogManifest)>> {
        match self.store.read_head(&self.name).await? {
            None => Ok(None),
            Some(head) => {
                let manifest = self.load_manifest(&head.value).await?;
                Ok(Some((head.value, manifest)))
            }
        }
    }

    pub async fn read(&self, from: u64) -> Result<Vec<Frame>> {
        let Some((_, manifest)) = self.status().await? else {
            return Ok(Vec::new());
        };
        if manifest.trim_before_seq > 0 && from <= manifest.trim_before_seq {
            return Err(CoreError::Trimmed {
                resume_at: manifest.trim_before_seq + 1,
            }
            .into());
        }
        let mut out = Vec::new();
        for chunk in manifest.segments.iter().chain(manifest.chunks.iter()) {
            if chunk.last < from {
                continue;
            }
            let (payload, _) = self.store.get_blob(&chunk.digest).await?;
            let body: ChunkBody = serde_json::from_slice(&payload)?;
            out.extend(
                body.frames
                    .into_iter()
                    .filter(|f| f.seq >= from && f.seq > manifest.trim_before_seq),
            );
        }
        out.sort_by_key(|f| f.seq);
        Ok(out)
    }

    pub async fn follow<F: FnMut(&Frame)>(
        &self,
        from: u64,
        poll_ms: u64,
        mut on_frame: F,
        stop: impl Fn() -> bool,
    ) -> Result<()> {
        let mut cursor = from;
        let mut last_head = 0u64;
        loop {
            if stop() {
                return Ok(());
            }
            if let Some((_, manifest)) = self.status().await? {
                if manifest.head_seq > last_head {
                    last_head = manifest.head_seq;
                    if manifest.head_seq >= cursor {
                        for frame in self.read(cursor).await? {
                            on_frame(&frame);
                            cursor = frame.seq + 1;
                        }
                    }
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(poll_ms)).await;
        }
    }

    async fn load_manifest(&self, value: &RefValue) -> Result<LogManifest> {
        match &value.target {
            None => Err(anyhow!("log {} has no manifest", self.name)),
            Some(digest) => {
                let (payload, _) = self.store.get_blob(digest).await.map_err(|e| {
                    CoreError::RecoveryFailed(format!("manifest {digest} unreadable: {e:#}"))
                })?;
                let m: LogManifest = serde_json::from_slice(&payload)
                    .map_err(|e| CoreError::RecoveryFailed(format!("manifest {digest}: {e}")))?;
                if m.schema != MANIFEST_SCHEMA {
                    return Err(CoreError::InvalidFormat(format!(
                        "unsupported log manifest schema {}",
                        m.schema
                    ))
                    .into());
                }
                m.header.validate()?;
                if m.header.resource != self.name || m.log != self.name {
                    return Err(CoreError::RecoveryFailed(format!(
                        "manifest {digest} resource {} log {} does not match {}",
                        m.header.resource, m.log, self.name
                    ))
                    .into());
                }
                Ok(m)
            }
        }
    }

    async fn load_domain(&self, value: &RefValue) -> Result<DomainState> {
        if value.target.is_none() {
            return Ok(DomainState::empty(self.feed.into(), value.epoch));
        }
        let m = self.load_manifest(value).await?;
        if matches!(m.retention, RetentionMode::Complete) && self.feed != FeedMode::Complete {
            return Err(CoreError::Rejected(
                "refusing to open a complete feed as trimmable".into(),
            )
            .into());
        }
        Ok(DomainState {
            epoch: m.epoch,
            head_seq: m.head_seq,
            chunks: m.chunks,
            segments: m.segments,
            trim_before_seq: m.trim_before_seq,
            retention: m.retention,
            stable_index: m.stable_index,
        })
    }

    async fn index_from_snapshot(&self, snapshot: &HeadSnapshot) -> Result<StableIndexRoot> {
        let domain = self.load_domain(&snapshot.value).await?;
        match domain.stable_index {
            Some(root) => {
                root.validate()?;
                hamt::check_root_against_head(
                    &root,
                    IndexHead {
                        generation: snapshot.value.generation,
                        head_seq: domain.head_seq,
                    },
                )?;
                Ok(root)
            }
            None => Err(CoreError::IntegrityError(
                "complete feed is missing its stable-key index".into(),
            )
            .into()),
        }
    }
}

#[derive(Clone)]
struct DomainState {
    epoch: u64,
    head_seq: u64,
    chunks: Vec<ChunkRef>,
    segments: Vec<ChunkRef>,
    trim_before_seq: u64,
    retention: RetentionMode,
    stable_index: Option<StableIndexRoot>,
}

impl DomainState {
    fn empty(retention: RetentionMode, epoch: u64) -> Self {
        Self {
            epoch,
            head_seq: 0,
            chunks: Vec::new(),
            segments: Vec::new(),
            trim_before_seq: 0,
            retention,
            stable_index: None,
        }
    }
}

fn check_payloads(payloads: &[Vec<u8>]) -> Result<()> {
    if payloads.is_empty() {
        return Err(CoreError::Rejected("empty append".into()).into());
    }
    if payloads.len() > MAX_APPEND_EVENTS {
        return Err(CoreError::Rejected(format!(
            "append of {} events exceeds {MAX_APPEND_EVENTS}",
            payloads.len()
        ))
        .into());
    }
    let bytes = payloads.iter().try_fold(0usize, |acc, p| {
        acc.checked_add(p.len())
            .ok_or_else(|| CoreError::Rejected("append byte count overflow".into()))
    })?;
    if bytes > MAX_APPEND_BYTES {
        return Err(CoreError::Rejected(format!(
            "append of {bytes} bytes exceeds {MAX_APPEND_BYTES}"
        ))
        .into());
    }
    Ok(())
}

fn is_retryable(e: &anyhow::Error) -> bool {
    matches!(
        e.downcast_ref::<CoreError>(),
        Some(CoreError::BackendUnavailable(_))
    )
}

fn next_writer(
    current: &RefValue,
    writer: &str,
    now: DateTime<Utc>,
    lease_secs: i64,
    steal: bool,
) -> Result<(u64, Lease)> {
    if current.lease_live(now) && !steal {
        let lease = current.lease.as_ref().unwrap();
        if lease.writer != writer {
            return Err(CoreError::LeaseHeld {
                holder: lease.writer.clone(),
                until: lease.lease_until.to_rfc3339(),
            }
            .into());
        }
        return Ok((
            current.epoch,
            Lease {
                writer: writer.into(),
                lease_until: now + Duration::seconds(lease_secs),
            },
        ));
    }
    let epoch = current
        .epoch
        .checked_add(1)
        .ok_or_else(|| CoreError::Rejected("epoch overflow".into()))?;
    Ok((
        epoch,
        Lease {
            writer: writer.into(),
            lease_until: now + Duration::seconds(lease_secs),
        },
    ))
}

async fn build_chunk(
    store: &Store,
    first: u64,
    payloads: &[Vec<u8>],
    now: DateTime<Utc>,
) -> Result<(ChunkRef, Vec<u8>, u64)> {
    let last = first
        .checked_add(payloads.len() as u64)
        .and_then(|v| v.checked_sub(1))
        .ok_or_else(|| CoreError::Rejected("sequence overflow".into()))?;
    let frames: Vec<Frame> = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| Frame {
            seq: first + i as u64,
            at: now,
            payload: p.clone(),
        })
        .collect();
    let body = ChunkBody {
        schema: CHUNK_SCHEMA.into(),
        frames,
    };
    let bytes = serde_json::to_vec(&body)?;
    let len = bytes.len() as u64;
    Ok((
        ChunkRef {
            digest: store.key.digest(&bytes),
            first,
            last,
            bytes: len,
            at: now,
        },
        bytes,
        last,
    ))
}

// digest of chunk payload is of envelope plaintext = chunk JSON. put_blob uses same.
// ChunkRef.digest must match put_blob digest. build_chunk currently uses key.digest(&bytes)
// which matches Envelope::new for Blob. We'll put_blob via Upload payload = bytes. Good.

struct AppendPlan {
    resource: String,
    logical: String,
    writer: String,
    lease_secs: i64,
    payloads: Vec<Vec<u8>>,
    feed: FeedMode,
}

impl RefMutationPlan for AppendPlan {
    type Outcome = AppendRange;

    fn resource(&self) -> &str {
        &self.resource
    }

    fn material(&self) -> Material {
        Material {
            kind: "append".into(),
            preconditions: Vec::new(),
            payload: self.payloads.clone(),
        }
    }

    async fn prepare(&self, ctx: PrepareCtx<'_>) -> Result<PreparedMutation<Self::Outcome>> {
        let item = GenericItem {
            identity: ctx.identity.clone(),
            request: ctx.request.clone(),
            payloads: self.payloads.clone(),
        };
        prepare_events(
            ctx,
            &self.logical,
            &self.writer,
            self.lease_secs,
            self.feed,
            &[item],
            &[],
            false,
        )
        .await
    }
}

struct GenericItem {
    identity: OpIdentity,
    request: Digest,
    payloads: Vec<Vec<u8>>,
}

struct StableItem {
    key: StableKey,
    payload: Vec<u8>,
    payload_hash: Digest,
}

async fn prepare_events(
    ctx: PrepareCtx<'_>,
    logical: &str,
    writer: &str,
    lease_secs: i64,
    feed: FeedMode,
    generics: &[GenericItem],
    stables: &[StableItem],
    steal: bool,
) -> Result<PreparedMutation<AppendRange>> {
    let now = ctx.now;
    let current = &ctx.snapshot.value;
    let (epoch, lease) = next_writer(current, writer, now, lease_secs, steal)?;
    let mut domain = if current.target.is_none() {
        DomainState::empty(feed.into(), epoch)
    } else {
        let (payload, _) = ctx.store.get_blob(current.target.as_ref().unwrap()).await?;
        let m: LogManifest = serde_json::from_slice(&payload)?;
        if m.schema != MANIFEST_SCHEMA {
            return Err(CoreError::InvalidFormat(format!(
                "unsupported log manifest schema {}",
                m.schema
            ))
            .into());
        }
        m.header.validate()?;
        if m.header.resource != ctx.snapshot.value.name || m.log != ctx.snapshot.value.name {
            return Err(CoreError::RecoveryFailed(format!(
                "manifest resource {} log {} does not match {}",
                m.header.resource, m.log, ctx.snapshot.value.name
            ))
            .into());
        }
        DomainState {
            epoch: m.epoch,
            head_seq: m.head_seq,
            chunks: m.chunks,
            segments: m.segments,
            trim_before_seq: m.trim_before_seq,
            retention: m.retention,
            stable_index: m.stable_index,
        }
    };
    if matches!(domain.retention, RetentionMode::Complete) {
        domain.retention = RetentionMode::Complete;
    } else {
        domain.retention = feed.into();
    }
    if feed == FeedMode::Complete {
        domain.retention = RetentionMode::Complete;
    }
    if matches!(domain.retention, RetentionMode::Complete)
        && current.target.is_some()
        && domain.stable_index.is_none()
    {
        return Err(CoreError::IntegrityError(
            "complete feed is missing its stable-key index".into(),
        )
        .into());
    }

    let mut all_payloads = Vec::new();
    let mut admitted = Vec::new();
    let mut seq = domain.head_seq;
    let mut companions = Vec::new();
    let mut leader_range = None;
    for (i, g) in generics.iter().enumerate() {
        check_payloads(&g.payloads)?;
        let first = seq
            .checked_add(1)
            .ok_or_else(|| CoreError::Rejected("sequence overflow".into()))?;
        let last = first
            .checked_add(g.payloads.len() as u64 - 1)
            .ok_or_else(|| CoreError::Rejected("sequence overflow".into()))?;
        seq = last;
        all_payloads.extend(g.payloads.iter().cloned());
        admitted.push(Admission {
            identity: g.identity.canonical(),
            request: g.request.clone(),
            first,
            last,
        });
        if i == 0 {
            leader_range = Some(AppendRange { first, last });
        } else {
            companions.push(Companion {
                identity: g.identity.clone(),
                result: serde_json::to_value(AppendRange { first, last })?,
            });
        }
    }
    let mut stable_admissions = Vec::new();
    let keep_index = feed == FeedMode::Complete
        || domain.retention == RetentionMode::Complete
        || !stables.is_empty();
    let mut index = if keep_index {
        Some(match domain.stable_index.clone() {
            Some(r) => r,
            None => hamt::empty_root(ctx.store).await?,
        })
    } else {
        None
    };
    for s in stables {
        check_payloads(&[s.payload.clone()])?;
        let first = seq
            .checked_add(1)
            .ok_or_else(|| CoreError::Rejected("sequence overflow".into()))?;
        let last = first;
        seq = last;
        all_payloads.push(s.payload.clone());
        let entry = StableIndexEntry {
            key: s.key.clone(),
            payload_hash: s.payload_hash.clone(),
            first,
            last,
            generation: ctx.generation,
        };
        let idx = index.as_mut().expect("stable insert requires an index");
        *idx = hamt::insert(
            ctx.store,
            idx,
            entry.clone(),
            logical,
            IndexHead {
                generation: ctx.generation,
                head_seq: seq,
            },
        )
        .await?;
        stable_admissions.push(entry);
        if leader_range.is_none() {
            leader_range = Some(AppendRange { first, last });
        }
    }
    if admitted.len() + stable_admissions.len() > MAX_ADMISSIONS {
        return Err(CoreError::Rejected("admission cap exceeded".into()).into());
    }
    hamt::admissions_size(&stable_admissions)?;
    if all_payloads.is_empty() {
        return Err(CoreError::Rejected("empty append".into()).into());
    }
    let first_event = domain
        .head_seq
        .checked_add(1)
        .ok_or_else(|| CoreError::Rejected("sequence overflow".into()))?;
    let (chunk, chunk_bytes, last_event) =
        build_chunk(ctx.store, first_event, &all_payloads, now).await?;
    domain.chunks.push(chunk);
    domain.head_seq = last_event;
    domain.epoch = epoch;

    let header = ctx.header(epoch);
    let range = leader_range.unwrap();
    let mut next = current.clone();
    next.generation = ctx.generation;
    next.epoch = epoch;
    next.lease = Some(lease);
    next.updated_at = now;
    let mut ref_state = next.clone();
    ref_state.head_commit = None;
    ref_state.target = None;
    let manifest = LogManifest {
        schema: MANIFEST_SCHEMA.into(),
        header: header.clone(),
        log: ctx.snapshot.value.name.clone(),
        epoch,
        head_seq: domain.head_seq,
        chunks: domain.chunks,
        segments: domain.segments,
        trim_before_seq: domain.trim_before_seq,
        admitted: admitted.clone(),
        retention: if keep_index {
            RetentionMode::Complete
        } else {
            domain.retention
        },
        stable_index: index,
        stable_admissions,
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
                schema: MANIFEST_SCHEMA.into(),
                payload: manifest_bytes,
            },
        ],
        commit_upload: Some(1),
        change: serde_json::json!({ "kind": "append", "first": range.first, "last": range.last }),
        outcome: range,
        admitted,
        companions,
        live_lease: None,
    })
}

struct StableAppendPlan {
    resource: String,
    logical: String,
    writer: String,
    lease_secs: i64,
    key: StableKey,
    payload: Vec<u8>,
    payload_hash: Digest,
}

impl RefMutationPlan for StableAppendPlan {
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
        let inner = prepare_events(
            PrepareCtx {
                store: ctx.store,
                snapshot: ctx.snapshot,
                identity: ctx.identity,
                request: ctx.request.clone(),
                generation: ctx.generation,
                parent: ctx.parent.clone(),
                skip: ctx.skip.clone(),
                now: ctx.now,
            },
            &self.logical,
            &self.writer,
            self.lease_secs,
            FeedMode::Complete,
            &[],
            &[StableItem {
                key: self.key.clone(),
                payload: self.payload.clone(),
                payload_hash: self.payload_hash.clone(),
            }],
            false,
        )
        .await?;
        let range = inner.outcome;
        Ok(PreparedMutation {
            next: inner.next,
            uploads: inner.uploads,
            commit_upload: inner.commit_upload,
            change: inner.change,
            outcome: StableAppendReceipt {
                payload_hash: self.payload_hash.clone(),
                range,
                generation: ctx.generation,
            },
            admitted: inner.admitted,
            companions: inner.companions,
            live_lease: None,
        })
    }
}

struct TakeoverPlan {
    resource: String,
    writer: String,
    lease_secs: i64,
}

#[derive(Clone, Serialize, Deserialize)]
struct EpochResult {
    epoch: u64,
}

impl RefMutationPlan for TakeoverPlan {
    type Outcome = EpochResult;

    fn resource(&self) -> &str {
        &self.resource
    }

    fn material(&self) -> Material {
        Material {
            kind: "takeover".into(),
            preconditions: vec![("writer".into(), self.writer.as_bytes().to_vec())],
            payload: Vec::new(),
        }
    }

    async fn prepare(&self, ctx: PrepareCtx<'_>) -> Result<PreparedMutation<Self::Outcome>> {
        let now = ctx.now;
        let current = &ctx.snapshot.value;
        let epoch = current
            .epoch
            .checked_add(1)
            .ok_or_else(|| CoreError::Rejected("epoch overflow".into()))?;
        let mut next = current.clone();
        next.generation = ctx.generation;
        next.epoch = epoch;
        next.lease = Some(Lease {
            writer: self.writer.clone(),
            lease_until: now + Duration::seconds(self.lease_secs),
        });
        next.updated_at = now;
        Ok(PreparedMutation {
            next,
            uploads: Vec::new(),
            commit_upload: None,
            change: serde_json::json!({ "kind": "takeover", "writer": self.writer }),
            outcome: EpochResult { epoch },
            admitted: Vec::new(),
            companions: Vec::new(),
            live_lease: None,
        })
    }
}

struct CompactPlan {
    resource: String,
    feed: FeedMode,
}

impl RefMutationPlan for CompactPlan {
    type Outcome = usize;

    fn resource(&self) -> &str {
        &self.resource
    }

    fn material(&self) -> Material {
        Material {
            kind: "compact".into(),
            preconditions: Vec::new(),
            payload: Vec::new(),
        }
    }

    async fn prepare(&self, ctx: PrepareCtx<'_>) -> Result<PreparedMutation<Self::Outcome>> {
        let current = &ctx.snapshot.value;
        let Some(digest) = current.target.clone() else {
            return Err(anyhow!("nothing to compact"));
        };
        let (payload, _) = ctx.store.get_blob(&digest).await?;
        let mut manifest: LogManifest = serde_json::from_slice(&payload)?;
        if manifest.chunks.len() < 2 {
            return Err(anyhow!("nothing to compact"));
        }
        let merged = manifest.chunks.len();
        let mut frames = Vec::new();
        for chunk in &manifest.chunks {
            let (p, _) = ctx.store.get_blob(&chunk.digest).await.map_err(|e| {
                CoreError::RecoveryFailed(format!("compact chunk {}: {e:#}", chunk.digest))
            })?;
            let body: ChunkBody = serde_json::from_slice(&p)?;
            frames.extend(body.frames);
        }
        frames.sort_by_key(|f| f.seq);
        let first = frames.first().unwrap().seq;
        let last = frames.last().unwrap().seq;
        let seg = ChunkBody {
            schema: CHUNK_SCHEMA.into(),
            frames,
        };
        let seg_bytes = serde_json::to_vec(&seg)?;
        let seg_len = seg_bytes.len() as u64;
        let header = ctx.header(current.epoch);
        manifest.header = header;
        manifest.segments.push(ChunkRef {
            digest: ctx.store.key.digest(&seg_bytes),
            first,
            last,
            bytes: seg_len,
            at: ctx.now,
        });
        manifest.chunks.clear();
        manifest.admitted.clear();
        manifest.stable_admissions.clear();
        if self.feed == FeedMode::Complete {
            manifest.retention = RetentionMode::Complete;
        }
        let mut next = current.clone();
        next.generation = ctx.generation;
        next.updated_at = ctx.now;
        manifest.result = serde_json::to_value(merged)?;
        let mut ref_state = next.clone();
        ref_state.head_commit = None;
        ref_state.target = None;
        manifest.ref_state = Some(ref_state);
        Ok(PreparedMutation {
            next,
            uploads: vec![
                Upload {
                    kind: ObjectKind::Blob,
                    schema: CHUNK_SCHEMA.into(),
                    payload: seg_bytes,
                },
                Upload {
                    kind: ObjectKind::Blob,
                    schema: MANIFEST_SCHEMA.into(),
                    payload: serde_json::to_vec(&manifest)?,
                },
            ],
            commit_upload: Some(1),
            change: serde_json::json!({ "kind": "compact", "merged": merged }),
            outcome: merged,
            admitted: Vec::new(),
            companions: Vec::new(),
            live_lease: None,
        })
    }
}

struct TrimPlan {
    resource: String,
    before: u64,
}

impl RefMutationPlan for TrimPlan {
    type Outcome = u64;

    fn resource(&self) -> &str {
        &self.resource
    }

    fn material(&self) -> Material {
        Material {
            kind: "trim".into(),
            preconditions: vec![("before".into(), self.before.to_be_bytes().to_vec())],
            payload: Vec::new(),
        }
    }

    async fn prepare(&self, ctx: PrepareCtx<'_>) -> Result<PreparedMutation<Self::Outcome>> {
        let current = &ctx.snapshot.value;
        let Some(digest) = current.target.clone() else {
            return Err(anyhow!("log {} does not exist", self.resource));
        };
        let (payload, _) = ctx.store.get_blob(&digest).await?;
        let mut manifest: LogManifest = serde_json::from_slice(&payload)?;
        if matches!(manifest.retention, RetentionMode::Complete) {
            return Err(CoreError::Rejected("complete-feed mode rejects trim".into()).into());
        }
        let floor = self.before.min(manifest.head_seq);
        manifest.trim_before_seq = manifest.trim_before_seq.max(floor);
        manifest.header = ctx.header(current.epoch);
        manifest.admitted.clear();
        let floor = manifest.trim_before_seq;
        let mut next = current.clone();
        next.generation = ctx.generation;
        next.updated_at = ctx.now;
        manifest.result = serde_json::to_value(floor)?;
        let mut ref_state = next.clone();
        ref_state.head_commit = None;
        ref_state.target = None;
        manifest.ref_state = Some(ref_state);
        Ok(PreparedMutation {
            next,
            uploads: vec![Upload {
                kind: ObjectKind::Blob,
                schema: MANIFEST_SCHEMA.into(),
                payload: serde_json::to_vec(&manifest)?,
            }],
            commit_upload: Some(0),
            change: serde_json::json!({ "kind": "trim", "floor": floor }),
            outcome: floor,
            admitted: Vec::new(),
            companions: Vec::new(),
            live_lease: None,
        })
    }
}

struct GroupPlan {
    resource: String,
    logical: String,
    writer: String,
    lease_secs: i64,
    feed: FeedMode,
    items: Vec<GenericItem>,
}

impl RefMutationPlan for GroupPlan {
    type Outcome = AppendRange;

    fn resource(&self) -> &str {
        &self.resource
    }

    fn material(&self) -> Material {
        self.items[0].clone_material()
    }

    async fn prepare(&self, ctx: PrepareCtx<'_>) -> Result<PreparedMutation<Self::Outcome>> {
        prepare_events(
            ctx,
            &self.logical,
            &self.writer,
            self.lease_secs,
            self.feed,
            &self.items,
            &[],
            false,
        )
        .await
    }
}

impl GenericItem {
    fn clone_material(&self) -> Material {
        Material {
            kind: "append".into(),
            preconditions: Vec::new(),
            payload: self.payloads.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Group commit

pub struct GroupWriter {
    tx: tokio::sync::mpsc::Sender<Submission>,
}

struct Submission {
    identity: OpIdentity,
    payloads: Vec<Vec<u8>>,
    ack: tokio::sync::oneshot::Sender<std::result::Result<Appended, String>>,
}

#[derive(Debug, Default, Clone)]
pub struct GroupStats {
    pub commits: u64,
    pub events: u64,
    pub conflicts: u64,
}

impl GroupWriter {
    pub fn spawn(
        store: Store,
        log_name: &str,
        writer: String,
        window_ms: u64,
        max_batch_events: usize,
    ) -> (Self, tokio::task::JoinHandle<GroupStats>) {
        Self::spawn_feed(
            store,
            log_name,
            writer,
            window_ms,
            max_batch_events,
            FeedMode::Trimmable,
        )
    }

    pub fn spawn_complete(
        store: Store,
        log_name: &str,
        writer: String,
        window_ms: u64,
        max_batch_events: usize,
    ) -> (Self, tokio::task::JoinHandle<GroupStats>) {
        Self::spawn_feed(
            store,
            log_name,
            writer,
            window_ms,
            max_batch_events,
            FeedMode::Complete,
        )
    }

    fn spawn_feed(
        store: Store,
        log_name: &str,
        writer: String,
        window_ms: u64,
        max_batch_events: usize,
        feed: FeedMode,
    ) -> (Self, tokio::task::JoinHandle<GroupStats>) {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Submission>(4096);
        let logical = log_name.to_string();
        let resource = format!("log/{log_name}/p0");
        let handle = tokio::spawn(async move {
            let mut stats = GroupStats::default();
            while let Some(first_sub) = rx.recv().await {
                let mut batch = vec![first_sub];
                let mut n_events = batch[0].payloads.len();
                let deadline =
                    tokio::time::Instant::now() + tokio::time::Duration::from_millis(window_ms);
                while n_events < max_batch_events {
                    match tokio::time::timeout_at(deadline, rx.recv()).await {
                        Ok(Some(sub)) => {
                            n_events += sub.payloads.len();
                            batch.push(sub);
                        }
                        Ok(None) | Err(_) => break,
                    }
                }
                commit_group(
                    &store, &resource, &logical, &writer, feed, batch, &mut stats,
                )
                .await;
            }
            stats
        });
        (Self { tx }, handle)
    }

    pub async fn submit(&self, op: OperationId, payloads: Vec<Vec<u8>>) -> Result<Appended> {
        let (ack, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(Submission {
                identity: OpIdentity::Generic(op),
                payloads,
                ack,
            })
            .await
            .map_err(|_| anyhow!("group writer stopped"))?;
        rx.await
            .map_err(|_| anyhow!("group writer dropped the batch"))?
            .map_err(|e| anyhow!(e))
    }

    pub fn close(self) {}
}

async fn commit_group(
    store: &Store,
    resource: &str,
    logical: &str,
    writer: &str,
    feed: FeedMode,
    batch: Vec<Submission>,
    stats: &mut GroupStats,
) {
    let snapshot = match store.read_head(resource).await {
        Ok(Some(s)) => s,
        Ok(None) => HeadSnapshot {
            value: RefValue::new(&store.tenant, resource),
            version: None,
        },
        Err(e) => {
            let msg = format!("{e:#}");
            for sub in batch {
                let _ = sub.ack.send(Err(msg.clone()));
            }
            return;
        }
    };
    let base = snapshot.value.generation;
    let mut unique: HashMap<
        String,
        (
            GenericItem,
            Vec<tokio::sync::oneshot::Sender<std::result::Result<Appended, String>>>,
        ),
    > = HashMap::new();
    let mut order = Vec::new();
    for sub in batch {
        if let Err(e) = check_payloads(&sub.payloads) {
            let _ = sub.ack.send(Err(format!("{e:#}")));
            continue;
        }
        let request = Material {
            kind: "append".into(),
            preconditions: Vec::new(),
            payload: sub.payloads.clone(),
        }
        .hash(&store.key, &store.tenant, resource);
        let canon = sub.identity.canonical();
        if let Some((item, acks)) = unique.get_mut(&canon) {
            if item.payloads != sub.payloads {
                let err = CoreError::IdempotencyConflict {
                    id: canon,
                    original: item.request.to_string(),
                    supplied: request.to_string(),
                };
                let _ = sub.ack.send(Err(format!("{err}")));
            } else {
                acks.push(sub.ack);
            }
            continue;
        }
        order.push(canon.clone());
        unique.insert(
            canon,
            (
                GenericItem {
                    identity: sub.identity,
                    request,
                    payloads: sub.payloads,
                },
                vec![sub.ack],
            ),
        );
    }
    let mut pending = Vec::new();
    let mut cached = Vec::new();
    for id in &order {
        let (item, acks) = unique.remove(id).unwrap();
        match store
            .ensure_pending_intent(&item.identity, resource, item.request.clone(), base)
            .await
        {
            Ok(crate::publish::IntentAdmission::Applied {
                generation, result, ..
            }) => {
                let range: AppendRange = match serde_json::from_value(result) {
                    Ok(r) => r,
                    Err(e) => {
                        for a in acks {
                            let _ = a.send(Err(format!("applied intent: {e}")));
                        }
                        continue;
                    }
                };
                cached.push((acks, range, generation, false));
            }
            Ok(crate::publish::IntentAdmission::Pending { .. }) => pending.push((item, acks)),
            Err(e) => {
                let msg = format!("{e:#}");
                for a in acks {
                    let _ = a.send(Err(msg.clone()));
                }
            }
        }
    }
    for (acks, range, generation, first) in cached {
        for a in acks {
            let _ = a.send(Ok(Appended {
                first: range.first,
                last: range.last,
                generation,
                first_delivery: first,
            }));
        }
    }
    if pending.is_empty() {
        return;
    }
    let leader = pending[0].0.identity.clone();
    let items: Vec<GenericItem> = pending
        .iter()
        .map(|(i, _)| GenericItem {
            identity: i.identity.clone(),
            request: i.request.clone(),
            payloads: i.payloads.clone(),
        })
        .collect();
    match store
        .publish(
            leader,
            GroupPlan {
                resource: resource.into(),
                logical: logical.into(),
                writer: writer.into(),
                lease_secs: 30,
                feed,
                items,
            },
        )
        .await
    {
        Ok(published) => {
            stats.commits += 1;
            for (item, acks) in pending {
                let id = item.identity.canonical();
                match published.admitted.iter().find(|a| a.identity == id) {
                    Some(adm) => {
                        stats.events += item.payloads.len() as u64;
                        for a in acks {
                            let _ = a.send(Ok(Appended {
                                first: adm.first,
                                last: adm.last,
                                generation: published.generation,
                                first_delivery: published.first_delivery,
                            }));
                        }
                    }
                    None => {
                        stats.conflicts += 1;
                        for a in acks {
                            let _ = a.send(Err(format!(
                                "grouped identity {id} was not admitted in this publication"
                            )));
                        }
                    }
                }
            }
        }
        Err(e) => {
            stats.conflicts += 1;
            let msg = format!("{e:#}");
            for (_, acks) in pending {
                for a in acks {
                    let _ = a.send(Err(msg.clone()));
                }
            }
        }
    }
}

#[path = "feed.rs"]
mod feed;
pub use feed::*;

mod hex_payload {
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&hex::encode(bytes))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(deserializer)?;
        hex::decode(&s).map_err(serde::de::Error::custom)
    }
}
