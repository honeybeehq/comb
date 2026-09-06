use crate::publish::{HeadSnapshot, PrepareCtx, PreparedMutation, Published, RefMutationPlan};
use anyhow::Result;
use comb_core::error::CoreError;
use comb_core::operation::{
    Clock, Material, OpIdentity, OperationId, OperationPolicy, SystemClock,
};
use comb_core::{Digest, DigestKey, Envelope, EnvelopeReadSpec, ObjectKind, RefValue};
use comb_object::{ObjectBackend, Version};
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Clone)]
pub struct Store {
    pub backend: Arc<dyn ObjectBackend>,
    pub tenant: String,
    pub key: DigestKey,
    pub cache_dir: Option<PathBuf>,
    clock: Arc<dyn Clock>,
    policy: OperationPolicy,
}

#[derive(Debug, Clone)]
pub struct MutationOutcome {
    pub generation: u64,
    pub epoch: u64,
    pub commit: Digest,
    pub value: RefValue,
    pub first_delivery: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationResult {
    pub generation: u64,
    pub epoch: u64,
}

impl Store {
    pub fn new(
        backend: Arc<dyn ObjectBackend>,
        tenant: impl Into<String>,
        key: DigestKey,
        cache_dir: Option<PathBuf>,
    ) -> Self {
        Self {
            backend,
            tenant: tenant.into(),
            key,
            cache_dir,
            clock: Arc::new(SystemClock),
            policy: OperationPolicy::default(),
        }
    }

    pub fn with_clock(self, clock: Arc<dyn Clock>) -> Self {
        Self { clock, ..self }
    }

    pub fn with_policy(self, policy: OperationPolicy) -> Self {
        Self { policy, ..self }
    }

    pub(crate) fn clock(&self) -> Arc<dyn Clock> {
        self.clock.clone()
    }

    pub(crate) fn policy(&self) -> OperationPolicy {
        self.policy.clone()
    }

    pub fn mint_operation(&self) -> OperationId {
        OperationId::mint(self.clock().as_ref())
    }

    // ---- blobs ----------------------------------------------------------

    pub async fn put_blob(&self, payload: Vec<u8>) -> Result<(Digest, bool)> {
        let env = Envelope::new(
            &self.tenant,
            ObjectKind::Blob,
            "comb.object/v1",
            payload,
            &self.key,
        );
        let digest = env.meta.digest.clone();
        let bytes = env.encode()?;
        let dedup = match self
            .backend
            .put_create(&self.object_key(&digest), &bytes)
            .await
        {
            Ok(_) => false,
            Err(CoreError::AlreadyExists(_)) => true,
            Err(e) => return Err(e.into()),
        };
        self.cache_write(&digest, &bytes);
        Ok((digest, dedup))
    }

    pub async fn get_blob(&self, digest: &Digest) -> Result<(Vec<u8>, GetSource)> {
        if let Some(bytes) = self.cache_read(digest) {
            match Envelope::decode(&bytes, &self.key) {
                Ok(env) if env.meta.digest == *digest => {
                    return Ok((env.payload, GetSource::Cache));
                }
                Ok(_) | Err(CoreError::IntegrityError(_)) | Err(CoreError::InvalidFormat(_)) => {
                    self.cache_quarantine(digest);
                    eprintln!(
                        "warning: cache entry for {digest} failed verification — quarantined, refetching from backend"
                    );
                }
                Err(e) => return Err(e.into()),
            }
        }
        let (bytes, _) = self.backend.get(&self.object_key(digest)).await?;
        let env = Envelope::decode(&bytes, &self.key)?;
        if env.meta.digest != *digest {
            return Err(anyhow_digest_mismatch(digest));
        }
        self.cache_write(digest, &bytes);
        Ok((env.payload, GetSource::Backend))
    }

    /// Bounded blob read for classed R2 objects.
    ///
    /// The disk cache is subject to the same encoded cap as the backend.
    /// Oversized cache entries are quarantined and the backend is retried.
    #[allow(dead_code)] // no log/publish callers until the retained-target SHA
    pub(crate) async fn get_blob_limited(
        &self,
        digest: &Digest,
        spec: EnvelopeReadSpec<'_>,
    ) -> Result<(Vec<u8>, GetSource)> {
        let object_key = self.object_key(digest);
        match self.cache_read_limited(digest, &object_key, spec.max_encoded_bytes) {
            Ok(Some(bytes)) => match Envelope::decode_limited(&bytes, &self.key, spec) {
                Ok(env) if env.meta.digest == *digest => {
                    return Ok((env.payload, GetSource::Cache));
                }
                Ok(_) | Err(CoreError::IntegrityError(_)) | Err(CoreError::InvalidFormat(_)) => {
                    self.cache_quarantine(digest);
                    eprintln!(
                        "warning: cache entry for {digest} failed verification — quarantined, refetching from backend"
                    );
                }
                Err(e) => return Err(size_key(e, &object_key).into()),
            },
            Ok(None) => {}
            Err(CoreError::ObjectTooLarge { .. }) => {
                self.cache_quarantine(digest);
            }
            Err(e) => return Err(e.into()),
        }

        let (bytes, _) = self
            .backend
            .get_limited(&object_key, spec.max_encoded_bytes)
            .await
            .map_err(|e| size_key(e, &object_key))?;
        let env = Envelope::decode_limited(&bytes, &self.key, spec)
            .map_err(|e| size_key(e, &object_key))?;
        if env.meta.digest != *digest {
            return Err(anyhow_digest_mismatch(digest));
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

    #[allow(dead_code)] // used by get_blob_limited
    fn cache_read_limited(
        &self,
        digest: &Digest,
        object_key: &str,
        max_encoded_bytes: NonZeroU64,
    ) -> comb_core::error::Result<Option<Vec<u8>>> {
        let Some(path) = self.cache_path(digest) else {
            return Ok(None);
        };
        let meta = match std::fs::metadata(&path) {
            Ok(m) => m,
            Err(_) => return Ok(None),
        };
        if meta.len() > max_encoded_bytes.get() {
            return Err(CoreError::ObjectTooLarge {
                key: object_key.into(),
                limit: max_encoded_bytes.get(),
                actual: Some(meta.len()),
            });
        }
        let file = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(_) => return Ok(None),
        };
        let mut buf = Vec::new();
        if file
            .take(max_encoded_bytes.get().saturating_add(1))
            .read_to_end(&mut buf)
            .is_err()
        {
            return Ok(None);
        }
        if (buf.len() as u64) > max_encoded_bytes.get() {
            return Err(CoreError::ObjectTooLarge {
                key: object_key.into(),
                limit: max_encoded_bytes.get(),
                actual: None,
            });
        }
        Ok(Some(buf))
    }

    pub(crate) fn cache_write(&self, digest: &Digest, bytes: &[u8]) {
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
        match self.read_head(name).await? {
            None => Ok(None),
            Some(HeadSnapshot { value, version }) => Ok(Some((
                value,
                version.unwrap_or_else(|| Version(String::new())),
            ))),
        }
    }

    pub async fn set_target(
        &self,
        name: &str,
        target: Digest,
        fence: Option<u64>,
    ) -> Result<MutationOutcome> {
        self.set_target_op(self.mint_operation(), name, target, fence)
            .await
    }

    pub async fn set_target_op(
        &self,
        op: OperationId,
        name: &str,
        target: Digest,
        fence: Option<u64>,
    ) -> Result<MutationOutcome> {
        let published = self
            .publish(
                OpIdentity::Generic(op),
                SetTargetPlan {
                    name: name.to_string(),
                    target,
                    fence,
                },
            )
            .await?;
        Ok(published.into())
    }

    pub async fn claim(
        &self,
        op: OperationId,
        name: &str,
        writer: &str,
        ttl_secs: i64,
        steal: bool,
    ) -> Result<MutationOutcome> {
        let published = self
            .publish(
                OpIdentity::Generic(op),
                ClaimPlan {
                    name: name.to_string(),
                    writer: writer.to_string(),
                    ttl_secs,
                    steal,
                },
            )
            .await?;
        Ok(published.into())
    }

    pub async fn release(
        &self,
        op: OperationId,
        name: &str,
        fence: u64,
    ) -> Result<MutationOutcome> {
        let published = self
            .publish(
                OpIdentity::Generic(op),
                ReleasePlan {
                    name: name.to_string(),
                    fence,
                },
            )
            .await?;
        Ok(published.into())
    }

    pub async fn renew(&self, name: &str, fence: u64, ttl_secs: i64) -> Result<RefValue> {
        self.renew_lease(name, fence, ttl_secs).await
    }

    pub async fn history(&self, name: &str, max: usize) -> Result<Vec<comb_core::HistoryEntry>> {
        self.history_chain(name, max).await
    }
}

impl From<Published<MutationResult>> for MutationOutcome {
    fn from(p: Published<MutationResult>) -> Self {
        Self {
            generation: p.generation,
            epoch: p.epoch,
            commit: p.commit,
            value: p.value,
            first_delivery: p.first_delivery,
        }
    }
}

fn anyhow_digest_mismatch(digest: &Digest) -> anyhow::Error {
    anyhow::anyhow!("backend returned object whose digest does not match {digest}")
}

#[allow(dead_code)] // used by get_blob_limited
fn size_key(err: CoreError, object_key: &str) -> CoreError {
    match err {
        CoreError::ObjectTooLarge { limit, actual, .. } => CoreError::ObjectTooLarge {
            key: object_key.into(),
            limit,
            actual,
        },
        other => other,
    }
}

async fn reject_existing_log_manifest(store: &Store, current: &RefValue) -> Result<()> {
    let Some(digest) = current.target.as_ref() else {
        return Ok(());
    };
    let (payload, _) = store.get_blob(digest).await.map_err(|e| {
        CoreError::Rejected(format!(
            "core set-target cannot overwrite a ref whose target {digest} cannot be read: {e:#}"
        ))
    })?;
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(&payload) else {
        return Ok(());
    };
    if value.get("schema").and_then(|s| s.as_str()) == Some("comb.log.partition-manifest/v2") {
        return Err(
            CoreError::Rejected("core set-target cannot overwrite a log-owned ref".into()).into(),
        );
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
pub enum GetSource {
    Cache,
    Backend,
}

struct SetTargetPlan {
    name: String,
    target: Digest,
    fence: Option<u64>,
}

impl RefMutationPlan for SetTargetPlan {
    type Outcome = MutationResult;

    fn resource(&self) -> &str {
        &self.name
    }

    fn material(&self) -> Material {
        Material {
            kind: "set-target".into(),
            preconditions: vec![("target".into(), self.target.to_string().into_bytes())],
            payload: Vec::new(),
        }
    }

    async fn prepare(&self, ctx: PrepareCtx<'_>) -> Result<PreparedMutation<Self::Outcome>> {
        let now = ctx.now;
        let current = &ctx.snapshot.value;
        reject_existing_log_manifest(ctx.store, current).await?;
        if let Some(f) = self.fence {
            if f != current.epoch {
                return Err(CoreError::Fenced {
                    caller: f,
                    live: current.epoch,
                }
                .into());
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
        next.generation = ctx.generation;
        next.target = Some(self.target.clone());
        next.updated_at = now;
        Ok(PreparedMutation {
            next: next.clone(),
            uploads: Vec::new(),
            commit_upload: None,
            change: serde_json::json!({ "kind": "set-target", "target": self.target }),
            outcome: MutationResult {
                generation: ctx.generation,
                epoch: next.epoch,
            },
            admitted: Vec::new(),
            companions: Vec::new(),
        })
    }
}

struct ClaimPlan {
    name: String,
    writer: String,
    ttl_secs: i64,
    steal: bool,
}

impl RefMutationPlan for ClaimPlan {
    type Outcome = MutationResult;

    fn resource(&self) -> &str {
        &self.name
    }

    fn material(&self) -> Material {
        Material {
            kind: "claim".into(),
            preconditions: vec![
                ("writer".into(), self.writer.as_bytes().to_vec()),
                (
                    "steal".into(),
                    if self.steal {
                        b"1".to_vec()
                    } else {
                        b"0".to_vec()
                    },
                ),
            ],
            payload: Vec::new(),
        }
    }

    async fn prepare(&self, ctx: PrepareCtx<'_>) -> Result<PreparedMutation<Self::Outcome>> {
        let now = ctx.now;
        let current = &ctx.snapshot.value;
        if current.lease_live(now) && !self.steal {
            let lease = current.lease.as_ref().unwrap();
            if lease.writer != self.writer {
                return Err(CoreError::LeaseHeld {
                    holder: lease.writer.clone(),
                    until: lease.lease_until.to_rfc3339(),
                }
                .into());
            }
        }
        let mut next = current.clone();
        next.generation = ctx.generation;
        next.epoch = current
            .epoch
            .checked_add(1)
            .ok_or_else(|| CoreError::Rejected("epoch overflow".into()))?;
        next.lease = Some(comb_core::Lease {
            writer: self.writer.clone(),
            lease_until: now + chrono::Duration::seconds(self.ttl_secs),
        });
        next.updated_at = now;
        Ok(PreparedMutation {
            next: next.clone(),
            uploads: Vec::new(),
            commit_upload: None,
            change: serde_json::json!({ "kind": "claim", "writer": self.writer, "steal": self.steal }),
            outcome: MutationResult {
                generation: ctx.generation,
                epoch: next.epoch,
            },
            admitted: Vec::new(),
            companions: Vec::new(),
        })
    }
}

struct ReleasePlan {
    name: String,
    fence: u64,
}

impl RefMutationPlan for ReleasePlan {
    type Outcome = MutationResult;

    fn resource(&self) -> &str {
        &self.name
    }

    fn material(&self) -> Material {
        Material {
            kind: "release".into(),
            preconditions: Vec::new(),
            payload: Vec::new(),
        }
    }

    async fn prepare(&self, ctx: PrepareCtx<'_>) -> Result<PreparedMutation<Self::Outcome>> {
        let current = &ctx.snapshot.value;
        if current.epoch != self.fence {
            return Err(CoreError::Fenced {
                caller: self.fence,
                live: current.epoch,
            }
            .into());
        }
        let mut next = current.clone();
        next.generation = ctx.generation;
        next.lease = None;
        next.updated_at = ctx.now;
        Ok(PreparedMutation {
            next: next.clone(),
            uploads: Vec::new(),
            commit_upload: None,
            change: serde_json::json!({ "kind": "release" }),
            outcome: MutationResult {
                generation: ctx.generation,
                epoch: next.epoch,
            },
            admitted: Vec::new(),
            companions: Vec::new(),
        })
    }
}

#[cfg(test)]
mod limited_reads {
    use super::*;
    use comb_core::error::CoreError;
    use comb_core::{DigestKey, EnvelopeReadSpec, ObjectKind};
    use comb_object::memory::MemoryBackend;
    use comb_object::ObjectBackend;
    use std::num::NonZeroU64;
    use std::path::PathBuf;
    use std::sync::Arc;

    const BLOB_SCHEMAS: &[&str] = &["comb.object/v1"];

    fn nz(n: u64) -> NonZeroU64 {
        NonZeroU64::new(n).expect("nonzero")
    }

    fn blob_spec(enc: u64, pt: u64) -> EnvelopeReadSpec<'static> {
        EnvelopeReadSpec {
            tenant: "org_t",
            kind: ObjectKind::Blob,
            allowed_schemas: BLOB_SCHEMAS,
            max_encoded_bytes: nz(enc),
            max_plaintext_bytes: nz(pt),
        }
    }

    fn store_with_cache(backend: Arc<dyn ObjectBackend>, cache: Option<PathBuf>) -> Store {
        Store::new(backend, "org_t", DigestKey::from_bytes([3u8; 32]), cache)
    }

    fn core(err: anyhow::Error) -> CoreError {
        match err.downcast::<CoreError>() {
            Ok(e) => e,
            Err(other) => panic!("expected CoreError, got {other:#}"),
        }
    }

    #[tokio::test]
    async fn get_blob_limited_exact_encoded_cap() {
        let mem: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
        let store = store_with_cache(mem.clone(), None);
        let payload = b"hello-limited".to_vec();
        let (digest, _) = store.put_blob(payload.clone()).await.unwrap();
        let encoded = mem.get(&store.object_key(&digest)).await.unwrap().0;
        let (got, source) = store
            .get_blob_limited(
                &digest,
                blob_spec(encoded.len() as u64, payload.len() as u64),
            )
            .await
            .unwrap();
        assert_eq!(got, payload);
        assert_eq!(source, GetSource::Backend);

        let err = core(
            store
                .get_blob_limited(
                    &digest,
                    blob_spec(encoded.len() as u64 - 1, payload.len() as u64),
                )
                .await
                .unwrap_err(),
        );
        match err {
            CoreError::ObjectTooLarge {
                limit,
                actual: Some(actual),
                ..
            } => {
                assert_eq!(limit, encoded.len() as u64 - 1);
                assert_eq!(actual, encoded.len() as u64);
            }
            other => panic!("expected ObjectTooLarge, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn get_blob_limited_plaintext_cap() {
        let mem: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
        let store = store_with_cache(mem, None);
        let payload = vec![0xff; 64];
        let (digest, _) = store.put_blob(payload.clone()).await.unwrap();
        let err = core(
            store
                .get_blob_limited(&digest, blob_spec(4 * 1024, 63))
                .await
                .unwrap_err(),
        );
        match err {
            CoreError::ObjectTooLarge {
                limit: 63,
                actual: Some(64),
                ..
            } => {}
            other => panic!("expected plaintext ObjectTooLarge, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn oversized_cache_is_bounded_quarantined_and_refetched() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        std::fs::create_dir_all(&cache).unwrap();
        let mem: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
        let store = store_with_cache(mem, Some(cache.clone()));
        let payload = b"tiny".to_vec();
        let (digest, _) = store.put_blob(payload.clone()).await.unwrap();
        store
            .get_blob_limited(&digest, blob_spec(4096, 4096))
            .await
            .unwrap();
        std::fs::write(cache.join(digest.hex()), vec![0u8; 256 * 1024]).unwrap();

        let (got, source) = store
            .get_blob_limited(&digest, blob_spec(1024, 1024))
            .await
            .unwrap();
        assert_eq!(got, payload);
        assert_eq!(source, GetSource::Backend);
        assert!(cache
            .join(digest.hex())
            .with_extension("quarantine")
            .exists());
    }

    #[tokio::test]
    async fn cache_hit_uses_limited_decode() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        std::fs::create_dir_all(&cache).unwrap();
        let mem: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
        let store = store_with_cache(mem, Some(cache));
        let payload = b"cached".to_vec();
        let (digest, _) = store.put_blob(payload.clone()).await.unwrap();
        let spec = blob_spec(4096, 4096);
        let _ = store.get_blob_limited(&digest, spec).await.unwrap();
        let (got, source) = store.get_blob_limited(&digest, spec).await.unwrap();
        assert_eq!(got, payload);
        assert_eq!(source, GetSource::Cache);
    }
}
