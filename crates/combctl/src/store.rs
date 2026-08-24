use anyhow::{anyhow, Result};
use chrono::{Duration, Utc};
use comb_core::error::CoreError;
use comb_core::refs::RefJournalEntry;
use comb_core::{Digest, DigestKey, Envelope, Lease, ObjectKind, RefValue};
use comb_object::{ObjectBackend, Version};
use std::path::PathBuf;

/// High-level store: envelopes over an object backend, a read-through
/// verified cache, refs with lease/fence semantics, and the ref journal.
pub struct Store {
    pub backend: Box<dyn ObjectBackend>,
    pub tenant: String,
    pub key: DigestKey,
    /// Node-local verified object cache (spec §15). `None` disables it
    /// (used when the backend itself is the local filesystem).
    pub cache_dir: Option<PathBuf>,
}

impl Store {
    fn object_key(&self, digest: &Digest) -> String {
        format!(
            "comb/v1/tenants/{}/objects/b3k/{}/{}",
            self.tenant,
            digest.key_prefix(),
            digest.hex()
        )
    }

    fn ref_key(&self, name: &str) -> String {
        format!("comb/v1/tenants/{}/refs/{name}.json", self.tenant)
    }

    fn journal_head_key(&self) -> String {
        // Shared journal scope for the tenant (spec §7.5a.3). Journal refs
        // are themselves never journaled.
        format!("comb/v1/tenants/{}/refs/core/journal/tenant.json", self.tenant)
    }

    // ---- blobs ----------------------------------------------------------

    /// Store plaintext as an immutable blob. Returns (digest, deduplicated).
    pub async fn put_blob(&self, payload: Vec<u8>) -> Result<(Digest, bool)> {
        let env = Envelope::new(&self.tenant, ObjectKind::Blob, "comb.object/v1", payload, &self.key);
        let digest = env.meta.digest.clone();
        let bytes = env.encode()?;
        let dedup = match self.backend.put_create(&self.object_key(&digest), &bytes).await {
            Ok(_) => false,
            Err(CoreError::AlreadyExists(_)) => true,
            Err(e) => return Err(e.into()),
        };
        self.cache_write(&digest, &bytes);
        Ok((digest, dedup))
    }

    /// Read and verify a blob, through the cache when enabled. A corrupt
    /// cache entry is quarantined and the object refetched (spec §15.1).
    pub async fn get_blob(&self, digest: &Digest) -> Result<(Vec<u8>, GetSource)> {
        if let Some(bytes) = self.cache_read(digest) {
            match Envelope::decode(&bytes, &self.key) {
                Ok(env) if env.meta.digest == *digest => {
                    return Ok((env.payload, GetSource::Cache));
                }
                Ok(_) | Err(CoreError::IntegrityError(_)) | Err(CoreError::InvalidFormat(_)) => {
                    self.cache_quarantine(digest);
                    eprintln!("warning: cache entry for {digest} failed verification — quarantined, refetching from backend");
                }
                Err(e) => return Err(e.into()),
            }
        }
        let (bytes, _) = self.backend.get(&self.object_key(digest)).await?;
        let env = Envelope::decode(&bytes, &self.key)?;
        if env.meta.digest != *digest {
            return Err(anyhow!("backend returned object whose digest does not match {digest}"));
        }
        self.cache_write(digest, &bytes);
        Ok((env.payload, GetSource::Backend))
    }

    fn cache_path(&self, digest: &Digest) -> Option<PathBuf> {
        self.cache_dir.as_ref().map(|d| d.join(digest.hex()))
    }

    fn cache_read(&self, digest: &Digest) -> Option<Vec<u8>> {
        std::fs::read(self.cache_path(digest)?).ok()
    }

    fn cache_write(&self, digest: &Digest, bytes: &[u8]) {
        if let Some(path) = self.cache_path(digest) {
            let _ = std::fs::create_dir_all(path.parent().unwrap());
            let _ = std::fs::write(path, bytes);
        }
    }

    fn cache_quarantine(&self, digest: &Digest) {
        if let Some(path) = self.cache_path(digest) {
            let _ = std::fs::rename(&path, path.with_extension("quarantine"));
        }
    }

    // ---- refs -----------------------------------------------------------

    pub async fn read_ref(&self, name: &str) -> Result<Option<(RefValue, Version)>> {
        match self.backend.get(&self.ref_key(name)).await {
            Ok((bytes, version)) => {
                let value: RefValue = serde_json::from_slice(&bytes)?;
                Ok(Some((value, version)))
            }
            Err(CoreError::NotFound(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn write_ref(&self, name: &str, expected: Option<&Version>, next: &RefValue) -> Result<Version> {
        let bytes = serde_json::to_vec_pretty(next)?;
        Ok(self.backend.put_update(&self.ref_key(name), expected, &bytes).await?)
    }

    /// Advance a ref's target: one guarded write. When the ref carries a
    /// live lease, the caller must present the matching fence (epoch);
    /// a stale fence fails with `Fenced`, no fence fails with `LeaseHeld`.
    pub async fn set_target(&self, name: &str, target: Digest, fence: Option<u64>) -> Result<RefValue> {
        let now = Utc::now();
        let (current, version) = match self.read_ref(name).await? {
            Some((v, ver)) => (v, Some(ver)),
            None => (RefValue::new(&self.tenant, name), None),
        };

        if let Some(f) = fence {
            if f != current.epoch {
                return Err(CoreError::Fenced { caller: f, live: current.epoch }.into());
            }
        } else if current.lease_live(now) {
            let lease = current.lease.as_ref().unwrap();
            return Err(CoreError::LeaseHeld {
                holder: lease.writer.clone(),
                until: lease.lease_until.to_rfc3339(),
            }
            .into());
        }

        let mut next = current.clone();
        next.generation += 1;
        next.target = Some(target);
        next.updated_at = now;
        self.write_ref(name, version.as_ref(), &next).await?;
        self.journal_append(&current, &next).await?;
        Ok(next)
    }

    /// Acquire the ref's lease at a new epoch (spec §7.6). Fails with
    /// `LeaseHeld` while a live lease exists, unless `steal` (administrative
    /// takeover). The returned epoch is the fencing token.
    pub async fn claim(&self, name: &str, writer: &str, ttl_secs: i64, steal: bool) -> Result<RefValue> {
        let now = Utc::now();
        let (current, version) = match self.read_ref(name).await? {
            Some((v, ver)) => (v, Some(ver)),
            None => (RefValue::new(&self.tenant, name), None),
        };
        if current.lease_live(now) && !steal {
            let lease = current.lease.as_ref().unwrap();
            if lease.writer != writer {
                return Err(CoreError::LeaseHeld {
                    holder: lease.writer.clone(),
                    until: lease.lease_until.to_rfc3339(),
                }
                .into());
            }
        }
        let mut next = current.clone();
        next.generation += 1;
        next.epoch += 1;
        next.lease = Some(Lease {
            writer: writer.to_string(),
            lease_until: now + Duration::seconds(ttl_secs),
        });
        next.updated_at = now;
        self.write_ref(name, version.as_ref(), &next).await?;
        self.journal_append(&current, &next).await?;
        Ok(next)
    }

    /// Renew the lease. Not a logical change: generation does not advance
    /// and nothing is journaled (spec §7.5a.3).
    pub async fn renew(&self, name: &str, fence: u64, ttl_secs: i64) -> Result<RefValue> {
        let now = Utc::now();
        let (current, version) = self
            .read_ref(name)
            .await?
            .ok_or_else(|| anyhow!("ref {name} does not exist"))?;
        if current.epoch != fence {
            return Err(CoreError::Fenced { caller: fence, live: current.epoch }.into());
        }
        let mut next = current.clone();
        let writer = next
            .lease
            .as_ref()
            .map(|l| l.writer.clone())
            .ok_or_else(|| anyhow!("ref {name} has no lease to renew"))?;
        next.lease = Some(Lease { writer, lease_until: now + Duration::seconds(ttl_secs) });
        next.updated_at = now;
        self.write_ref(name, Some(&version), &next).await?;
        Ok(next)
    }

    /// Release the lease. A logical change: journaled.
    pub async fn release(&self, name: &str, fence: u64) -> Result<RefValue> {
        let now = Utc::now();
        let (current, version) = self
            .read_ref(name)
            .await?
            .ok_or_else(|| anyhow!("ref {name} does not exist"))?;
        if current.epoch != fence {
            return Err(CoreError::Fenced { caller: fence, live: current.epoch }.into());
        }
        let mut next = current.clone();
        next.generation += 1;
        next.lease = None;
        next.updated_at = now;
        self.write_ref(name, Some(&version), &next).await?;
        self.journal_append(&current, &next).await?;
        Ok(next)
    }

    // ---- journal --------------------------------------------------------

    /// Append one entry to the tenant journal after a successful logical
    /// ref update. Written after the CAS: a lost journal write degrades
    /// auditability, never consistency (spec §7.5a.3).
    async fn journal_append(&self, prev: &RefValue, next: &RefValue) -> Result<()> {
        for _ in 0..16 {
            let head = self.journal_head().await?;
            let entry = RefJournalEntry {
                schema: "comb.journal-entry/v1".into(),
                ref_name: next.name.clone(),
                prev_generation: prev.generation,
                new_generation: next.generation,
                epoch: next.epoch,
                target: next.target.clone(),
                writer: next.lease.as_ref().map(|l| l.writer.clone()),
                parent: head.as_ref().map(|(digest, _)| digest.clone()),
                at: next.updated_at,
            };
            let payload = serde_json::to_vec(&entry)?;
            let env = Envelope::new(&self.tenant, ObjectKind::JournalEntry, "comb.journal-entry/v1", payload, &self.key);
            let digest = env.meta.digest.clone();
            match self.backend.put_create(&self.object_key(&digest), &env.encode()?).await {
                Ok(_) | Err(CoreError::AlreadyExists(_)) => {}
                Err(e) => return Err(e.into()),
            }
            let body = serde_json::to_vec(&serde_json::json!({ "entry": digest.to_string() }))?;
            let expected = head.as_ref().map(|(_, v)| v);
            match self.backend.put_update(&self.journal_head_key(), expected, &body).await {
                Ok(_) => return Ok(()),
                Err(CoreError::PreconditionFailed(_)) | Err(CoreError::AlreadyExists(_)) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Err(anyhow!("journal head contention: gave up after 16 attempts"))
    }

    async fn journal_head(&self) -> Result<Option<(Digest, Version)>> {
        match self.backend.get(&self.journal_head_key()).await {
            Ok((bytes, version)) => {
                let v: serde_json::Value = serde_json::from_slice(&bytes)?;
                let digest = Digest::parse(v["entry"].as_str().unwrap_or_default())
                    .map_err(|e| anyhow!("corrupt journal head: {e}"))?;
                Ok(Some((digest, version)))
            }
            Err(CoreError::NotFound(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Walk the journal from the head, newest first, filtered by ref name
    /// (empty filter returns everything).
    pub async fn history(&self, ref_name: &str, max: usize) -> Result<Vec<RefJournalEntry>> {
        let mut out = Vec::new();
        let mut cursor = self.journal_head().await?.map(|(d, _)| d);
        while let Some(digest) = cursor {
            if out.len() >= max {
                break;
            }
            let (payload, _) = self.get_blob(&digest).await?;
            let entry: RefJournalEntry = serde_json::from_slice(&payload)?;
            cursor = entry.parent.clone();
            if ref_name.is_empty() || entry.ref_name == ref_name {
                out.push(entry);
            }
        }
        Ok(out)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum GetSource {
    Cache,
    Backend,
}
