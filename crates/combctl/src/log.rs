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
    pub chunks: Vec<ChunkRef>,
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

    /// Read frames with seq >= from, in order.
    pub async fn read(&self, from: u64) -> Result<Vec<Frame>> {
        let Some((_, manifest)) = self.status().await? else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for chunk in &manifest.chunks {
            if chunk.last < from {
                continue;
            }
            let (payload, _) = self.store.get_blob(&chunk.digest).await?;
            let body: ChunkBody = serde_json::from_slice(&payload)?;
            out.extend(body.frames.into_iter().filter(|f| f.seq >= from));
        }
        out.sort_by_key(|f| f.seq);
        Ok(out)
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
