//! Minimal Comb Log (spec §8, v0.3 Phase B scope): atomic batch append,
//! ordered read, follow, and fenced leader takeover over a single
//! partition. The partition manifest ref is the linearization point:
//! chunks are uploaded create-only and become visible only when the
//! manifest ref advances. Compaction, segments, and indexes are Phase C.

use crate::store::Store;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Duration, Utc};
use comb_core::error::CoreError;
use comb_core::{Digest, Lease, RefValue};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRef {
    pub digest: Digest,
    pub first: u64,
    pub last: u64,
    pub bytes: u64,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogManifest {
    pub schema: String,
    pub log: String,
    pub epoch: u64,
    pub head_seq: u64,
    /// Live WAL chunks, bounded by compaction.
    pub chunks: Vec<ChunkRef>,
    /// Compacted segments (spec §8.11). Read path merges both.
    #[serde(default)]
    pub segments: Vec<ChunkRef>,
    /// Retention floor: sequences at or below this are trimmed (§8.11).
    #[serde(default)]
    pub trim_before_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Frame {
    pub seq: u64,
    pub at: DateTime<Utc>,
    pub payload: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChunkBody {
    schema: String,
    frames: Vec<Frame>,
}

pub struct LogStore<'a> {
    pub store: &'a Store,
    pub name: String,
}

fn is_cas_conflict(e: &anyhow::Error) -> bool {
    e.downcast_ref::<CoreError>()
        .map(|c| matches!(c, CoreError::PreconditionFailed(_) | CoreError::AlreadyExists(_)))
        .unwrap_or(false)
}

impl<'a> LogStore<'a> {
    pub fn new(store: &'a Store, name: &str) -> Self {
        Self { store, name: format!("log/{name}/p0") }
    }

    async fn load_manifest(&self, value: &RefValue) -> Result<LogManifest> {
        match &value.target {
            None => Ok(LogManifest {
                schema: "comb.log.partition-manifest/v1".into(),
                log: self.name.clone(),
                epoch: value.epoch,
                head_seq: 0,
                chunks: Vec::new(),
                segments: Vec::new(),
                trim_before_seq: 0,
            }),
            Some(digest) => {
                let (payload, _) = self.store.get_blob(digest).await?;
                Ok(serde_json::from_slice(&payload)?)
            }
        }
    }

    /// Atomically append a batch. Acknowledged only after the manifest ref
    /// advances (spec §8.7). Acquires leadership if the partition is free;
    /// a live lease held by another writer refuses with `LeaseHeld`.
    pub async fn append(&self, writer: &str, payloads: &[String], lease_secs: i64) -> Result<(u64, u64)> {
        for _attempt in 0..3 {
            let now = Utc::now();
            let (current, version) = match self.store.read_ref(&self.name).await? {
                Some((v, ver)) => (v, Some(ver)),
                None => (RefValue::new(&self.store.tenant, &self.name), None),
            };
            if current.lease_live(now) {
                let lease = current.lease.as_ref().unwrap();
                if lease.writer != writer {
                    return Err(CoreError::LeaseHeld {
                        holder: lease.writer.clone(),
                        until: lease.lease_until.to_rfc3339(),
                    }
                    .into());
                }
            }
            // New epoch when acquiring leadership; unchanged while holding it.
            let epoch = if current.lease_live(now) && current.lease.as_ref().unwrap().writer == writer {
                current.epoch
            } else {
                current.epoch + 1
            };

            let mut manifest = self.load_manifest(&current).await?;
            let first = manifest.head_seq + 1;
            let last = manifest.head_seq + payloads.len() as u64;
            let frames: Vec<Frame> = payloads
                .iter()
                .enumerate()
                .map(|(i, p)| Frame { seq: first + i as u64, at: now, payload: p.clone() })
                .collect();

            // 1. Immutable chunk, create-only. Invisible until referenced.
            let chunk = ChunkBody { schema: "comb.log.chunk/v1".into(), frames };
            let chunk_bytes = serde_json::to_vec(&chunk)?;
            let chunk_len = chunk_bytes.len() as u64;
            let (chunk_digest, _) = self.store.put_blob(chunk_bytes).await?;

            // 2. New immutable manifest referencing it.
            manifest.epoch = epoch;
            manifest.head_seq = last;
            manifest.chunks.push(ChunkRef {
                digest: chunk_digest,
                first,
                last,
                bytes: chunk_len,
                at: now,
            });
            let (manifest_digest, _) = self.store.put_blob(serde_json::to_vec(&manifest)?).await?;

            // Debug knob for the L1 kill demo: hold the window between
            // upload and publication open so a human can kill the process
            // inside it. Chunk and manifest are then invisible orphans.
            if let Ok(ms) = std::env::var("COMB_PAUSE_BEFORE_PUBLISH_MS") {
                if let Ok(ms) = ms.parse::<u64>() {
                    eprintln!("(paused {ms}ms before manifest publication — kill me now to orphan the chunk)");
                    tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                }
            }

            // 3. One guarded write advances head and lease together.
            let mut next = current.clone();
            next.generation += 1;
            next.epoch = epoch;
            next.target = Some(manifest_digest);
            next.lease = Some(Lease { writer: writer.into(), lease_until: now + Duration::seconds(lease_secs) });
            next.updated_at = now;
            let bytes = serde_json::to_vec_pretty(&next)?;
            let key = format!("comb/v1/tenants/{}/refs/{}.json", self.store.tenant, self.name);
            match self.store.backend.put_update(&key, version.as_ref(), &bytes).await {
                Ok(_) => return Ok((first, last)),
                // Lost the race (concurrent append or takeover): the chunk
                // and manifest are unreachable orphans. Reload and retry.
                Err(CoreError::PreconditionFailed(_)) | Err(CoreError::AlreadyExists(_)) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Err(anyhow!("append contention: lost the manifest race 3 times"))
    }

    /// Fenced takeover (spec §8.9): claim leadership at a new epoch without
    /// appending. The old leader's next append sees a live foreign lease.
    pub async fn steal(&self, writer: &str, lease_secs: i64) -> Result<u64> {
        let now = Utc::now();
        let (current, version) = self
            .store
            .read_ref(&self.name)
            .await?
            .ok_or_else(|| anyhow!("log {} does not exist", self.name))?;
        let mut next = current.clone();
        next.generation += 1;
        next.epoch += 1;
        next.lease = Some(Lease { writer: writer.into(), lease_until: now + Duration::seconds(lease_secs) });
        next.updated_at = now;
        let bytes = serde_json::to_vec_pretty(&next)?;
        let key = format!("comb/v1/tenants/{}/refs/{}.json", self.store.tenant, self.name);
        self.store.backend.put_update(&key, Some(&version), &bytes).await?;
        Ok(next.epoch)
    }

    pub async fn status(&self) -> Result<Option<(RefValue, LogManifest)>> {
        match self.store.read_ref(&self.name).await? {
            None => Ok(None),
            Some((value, _)) => {
                let manifest = self.load_manifest(&value).await?;
                Ok(Some((value, manifest)))
            }
        }
    }

    /// Read frames with seq >= from, in order. Positions at or below the
    /// retention floor refuse with `Trimmed{resume_at}` (spec §8, L9) —
    /// never a silent gap.
    pub async fn read(&self, from: u64) -> Result<Vec<Frame>> {
        let Some((_, manifest)) = self.status().await? else {
            return Ok(Vec::new());
        };
        if manifest.trim_before_seq > 0 && from <= manifest.trim_before_seq {
            return Err(CoreError::Trimmed { resume_at: manifest.trim_before_seq + 1 }.into());
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

    /// Merge all live WAL chunks into one segment (spec §8.11). Contents
    /// are unchanged — only the representation. Races with appends resolve
    /// through conditional-update retry (drill L4); superseded chunk
    /// objects become orphans for the sweeper.
    pub async fn compact(&self) -> Result<usize> {
        for _ in 0..5 {
            let Some((current, version)) = self.store.read_ref(&self.name).await? else {
                return Ok(0);
            };
            let manifest = self.load_manifest(&current).await?;
            if manifest.chunks.len() < 2 {
                return Ok(0);
            }
            let merged_count = manifest.chunks.len();

            let mut frames = Vec::new();
            for chunk in &manifest.chunks {
                let (payload, _) = self.store.get_blob(&chunk.digest).await?;
                let body: ChunkBody = serde_json::from_slice(&payload)?;
                frames.extend(body.frames);
            }
            frames.sort_by_key(|f| f.seq);
            let (first, last) = (frames.first().unwrap().seq, frames.last().unwrap().seq);
            let seg = ChunkBody { schema: "comb.log.chunk/v1".into(), frames };
            let seg_bytes = serde_json::to_vec(&seg)?;
            let seg_len = seg_bytes.len() as u64;
            let (seg_digest, _) = self.store.put_blob(seg_bytes).await?;

            let mut next_manifest = manifest.clone();
            next_manifest.segments.push(ChunkRef {
                digest: seg_digest,
                first,
                last,
                bytes: seg_len,
                at: Utc::now(),
            });
            next_manifest.chunks.clear();
            match self.publish_manifest(&current, &version, next_manifest).await {
                Ok(()) => return Ok(merged_count),
                Err(e) if is_cas_conflict(&e) => continue, // appender won; reload
                Err(e) => return Err(e),
            }
        }
        Err(anyhow!("compaction: lost the manifest race 5 times"))
    }

    /// Advance the retention floor. Physical bytes below it remain until
    /// the sweeper collects unreferenced objects; readers below the floor
    /// get `Trimmed{resume_at}` immediately.
    pub async fn trim_before(&self, seq: u64) -> Result<u64> {
        for _ in 0..5 {
            let Some((current, version)) = self.store.read_ref(&self.name).await? else {
                return Err(anyhow!("log {} does not exist", self.name));
            };
            let manifest = self.load_manifest(&current).await?;
            let floor = seq.min(manifest.head_seq);
            let mut next_manifest = manifest.clone();
            next_manifest.trim_before_seq = next_manifest.trim_before_seq.max(floor);
            let floor = next_manifest.trim_before_seq;
            match self.publish_manifest(&current, &version, next_manifest).await {
                Ok(()) => return Ok(floor),
                Err(e) if is_cas_conflict(&e) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(anyhow!("trim: lost the manifest race 5 times"))
    }

    /// Maintenance publication: new manifest object, one guarded ref write
    /// using the version observed together with `current` — value and token
    /// must come from the same read or a racing append could be overwritten.
    /// Leaves epoch and lease untouched (maintenance, not leadership).
    async fn publish_manifest(
        &self,
        current: &RefValue,
        version: &comb_object::backend::Version,
        manifest: LogManifest,
    ) -> Result<()> {
        let (digest, _) = self.store.put_blob(serde_json::to_vec(&manifest)?).await?;
        let mut next = current.clone();
        next.generation += 1;
        next.target = Some(digest);
        next.updated_at = Utc::now();
        let bytes = serde_json::to_vec_pretty(&next)?;
        let key = format!("comb/v1/tenants/{}/refs/{}.json", self.store.tenant, self.name);
        self.store.backend.put_update(&key, Some(version), &bytes).await?;
        Ok(())
    }

    /// Follow the log: poll the manifest ref and emit new frames. Change
    /// detection keys on `head_seq` — logical state, never the provider
    /// version token (spec §8.10). Runs until `stop` returns true.
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
}

// ---------------------------------------------------------------------------
// Group commit (spec §8.7): many concurrent producers, one chunk + one
// manifest CAS per commit window. The writer caches the ref version and
// manifest between commits so the steady state is exactly two backend
// writes and one guarded update per batch, with no reads.

pub struct GroupWriter {
    tx: tokio::sync::mpsc::Sender<Submission>,
}

struct Submission {
    payloads: Vec<String>,
    ack: tokio::sync::oneshot::Sender<std::result::Result<(u64, u64), String>>,
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
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Submission>(4096);
        let name = format!("log/{log_name}/p0");
        let handle = tokio::spawn(async move {
            let log = LogStore { store: &store, name };
            let mut stats = GroupStats::default();
            // Cached (ref value, provider version, manifest) between commits.
            let mut cache: Option<(RefValue, comb_object::backend::Version, LogManifest)> = None;

            while let Some(first_sub) = rx.recv().await {
                let mut batch = vec![first_sub];
                let mut n_events = batch[0].payloads.len();
                let deadline = tokio::time::Instant::now()
                    + tokio::time::Duration::from_millis(window_ms);
                while n_events < max_batch_events {
                    match tokio::time::timeout_at(deadline, rx.recv()).await {
                        Ok(Some(sub)) => {
                            n_events += sub.payloads.len();
                            batch.push(sub);
                        }
                        Ok(None) | Err(_) => break,
                    }
                }

                let payloads: Vec<String> =
                    batch.iter().flat_map(|s| s.payloads.iter().cloned()).collect();
                let result = commit_batch(&log, &writer, &payloads, &mut cache, &mut stats).await;
                match result {
                    Ok((first, _last)) => {
                        stats.commits += 1;
                        stats.events += payloads.len() as u64;
                        let mut seq = first;
                        for sub in batch {
                            let last = seq + sub.payloads.len() as u64 - 1;
                            let _ = sub.ack.send(Ok((seq, last)));
                            seq = last + 1;
                        }
                    }
                    Err(e) => {
                        let msg = format!("{e:#}");
                        for sub in batch {
                            let _ = sub.ack.send(Err(msg.clone()));
                        }
                    }
                }
            }
            stats
        });
        (Self { tx }, handle)
    }

    /// Submit events; resolves with the assigned (first, last) sequence
    /// range once the batch's manifest publication succeeded.
    pub async fn submit(&self, payloads: Vec<String>) -> Result<(u64, u64)> {
        let (ack, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(Submission { payloads, ack })
            .await
            .map_err(|_| anyhow!("group writer stopped"))?;
        rx.await.map_err(|_| anyhow!("group writer dropped the batch"))?
            .map_err(|e| anyhow!(e))
    }

    /// Close the intake; the spawn handle then resolves with stats.
    pub fn close(self) {}
}

async fn commit_batch(
    log: &LogStore<'_>,
    writer: &str,
    payloads: &[String],
    cache: &mut Option<(RefValue, comb_object::backend::Version, LogManifest)>,
    stats: &mut GroupStats,
) -> Result<(u64, u64)> {
    for _attempt in 0..3 {
        let now = Utc::now();
        // Load state: from cache in steady state, from the backend after a
        // conflict or on the first batch.
        let (current, version, mut manifest) = match cache.take() {
            Some(state) => state,
            None => match log.store.read_ref(&log.name).await? {
                Some((v, ver)) => {
                    let m = log.load_manifest(&v).await?;
                    (v, ver, m)
                }
                None => {
                    let v = RefValue::new(&log.store.tenant, &log.name);
                    let m = log.load_manifest(&v).await?;
                    // No provider version exists yet: fall through to the
                    // create path below via append()'s logic.
                    (v, comb_object::backend::Version(String::new()), m)
                }
            },
        };
        if current.lease_live(now) {
            let lease = current.lease.as_ref().unwrap();
            if lease.writer != writer {
                return Err(CoreError::LeaseHeld {
                    holder: lease.writer.clone(),
                    until: lease.lease_until.to_rfc3339(),
                }
                .into());
            }
        }
        let epoch = if current.lease_live(now) && current.lease.as_ref().unwrap().writer == writer {
            current.epoch
        } else {
            current.epoch + 1
        };

        let first = manifest.head_seq + 1;
        let last = manifest.head_seq + payloads.len() as u64;
        let frames: Vec<Frame> = payloads
            .iter()
            .enumerate()
            .map(|(i, p)| Frame { seq: first + i as u64, at: now, payload: p.clone() })
            .collect();
        let chunk = ChunkBody { schema: "comb.log.chunk/v1".into(), frames };
        let chunk_bytes = serde_json::to_vec(&chunk)?;
        let chunk_len = chunk_bytes.len() as u64;
        let (chunk_digest, _) = log.store.put_blob(chunk_bytes).await?;

        manifest.epoch = epoch;
        manifest.head_seq = last;
        manifest.chunks.push(ChunkRef { digest: chunk_digest, first, last, bytes: chunk_len, at: now });
        let (manifest_digest, _) = log.store.put_blob(serde_json::to_vec(&manifest)?).await?;

        let mut next = current.clone();
        next.generation += 1;
        next.epoch = epoch;
        next.target = Some(manifest_digest);
        next.lease = Some(Lease {
            writer: writer.into(),
            lease_until: now + Duration::seconds(30),
        });
        next.updated_at = now;
        let bytes = serde_json::to_vec_pretty(&next)?;
        let key = format!("comb/v1/tenants/{}/refs/{}.json", log.store.tenant, log.name);
        let expected = if version.0.is_empty() { None } else { Some(&version) };
        match log.store.backend.put_update(&key, expected, &bytes).await {
            Ok(new_version) => {
                *cache = Some((next, new_version, manifest));
                return Ok((first, last));
            }
            Err(CoreError::PreconditionFailed(_)) | Err(CoreError::AlreadyExists(_)) => {
                stats.conflicts += 1;
                // Cache is stale (someone else moved the ref): reload.
                continue;
            }
            Err(e) => return Err(e.into()),
        }
    }
    Err(anyhow!("group commit: lost the manifest race 3 times"))
}
