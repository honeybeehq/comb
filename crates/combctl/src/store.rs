use crate::publish::{
    map_commit_read_error, HeadSnapshot, PrepareCtx, PreparedMutation, Published, RefMutationPlan,
    LOG_MANIFEST_CARRIERS,
};
use anyhow::Result;
use comb_core::error::CoreError;
use comb_core::operation::{
    Clock, Material, OpIdentity, OperationId, OperationPolicy, SystemClock,
};
use comb_core::{
    Digest, DigestKey, Envelope, EnvelopeFormatField, EnvelopeReadSpec, ObjectKind, RefValue,
};
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
    /// Owned by this Store. Not keyed off the backend pointer or tenant, so
    /// two Stores sharing a backend keep independent clocks and policies.
    clock: Arc<dyn Clock>,
    policy: OperationPolicy,
    pub(crate) layout: KeyLayout,
}

/// Physical object/ref/intent prefix. R1 constructors stay on v2.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum KeyLayout {
    V2,
    V3,
}

impl KeyLayout {
    pub(crate) fn prefix(self) -> &'static str {
        match self {
            Self::V2 => crate::publish::NS_V2,
            Self::V3 => crate::publish::NS_V3,
        }
    }
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
            layout: KeyLayout::V2,
        }
    }

    pub fn with_clock(self, clock: Arc<dyn Clock>) -> Self {
        Self { clock, ..self }
    }

    pub(crate) fn with_layout(self, layout: KeyLayout) -> Self {
        Self { layout, ..self }
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

    pub(crate) async fn put_object(
        &self,
        kind: ObjectKind,
        schema: &str,
        payload: Vec<u8>,
        max_encoded_bytes: NonZeroU64,
    ) -> Result<Digest> {
        let env = Envelope::new(&self.tenant, kind, schema, payload, &self.key);
        let digest = env.meta.digest.clone();
        let bytes = env.encode()?;
        if bytes.len() as u64 > max_encoded_bytes.get() {
            return Err(CoreError::ObjectTooLarge {
                key: self.object_key(&digest),
                limit: max_encoded_bytes.get(),
                actual: Some(bytes.len() as u64),
            }
            .into());
        }
        match self
            .backend
            .put_create(&self.object_key(&digest), &bytes)
            .await
        {
            Ok(_) | Err(CoreError::AlreadyExists(_)) => {}
            Err(e) => return Err(e.into()),
        }
        self.cache_write(&digest, &bytes);
        Ok(digest)
    }

    pub async fn get_blob(&self, digest: &Digest) -> Result<(Vec<u8>, GetSource)> {
        if let Some(bytes) = self.cache_read(digest) {
            match Envelope::decode(&bytes, &self.key) {
                Ok(env) if env.meta.digest == *digest => {
                    return Ok((env.payload, GetSource::Cache));
                }
                Ok(_) | Err(_) => {
                    self.cache_quarantine(digest);
                    eprintln!(
                        "warning: cache entry for {digest} failed verification — quarantined, refetching from backend"
                    );
                }
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
                Err(e @ CoreError::ObjectTooLarge { .. })
                    if cache_plaintext_cap_is_authoritative(&bytes, digest, &self.key, spec) =>
                {
                    return Err(size_key(e, &object_key).into());
                }
                Ok(_) | Err(_) => {
                    self.cache_quarantine(digest);
                    eprintln!(
                        "warning: cache entry for {digest} failed verification — quarantined, refetching from backend"
                    );
                }
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
        let dir = self.cache_dir.as_ref()?;
        Some(match self.layout {
            KeyLayout::V2 => dir.join(digest.hex()),
            KeyLayout::V3 => dir.join("comb-v3").join(digest.hex()),
        })
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
        reject_log_namespace(name)?;
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
                    writer: None,
                },
            )
            .await?;
        Ok(published.into())
    }

    pub(crate) async fn release_owned(
        &self,
        op: OperationId,
        name: &str,
        writer: &str,
        fence: u64,
    ) -> Result<MutationOutcome> {
        let published = self
            .publish(
                OpIdentity::Generic(op),
                ReleasePlan {
                    name: name.to_string(),
                    fence,
                    writer: Some(writer.to_string()),
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

/// Digest-matched objects that are genuinely over the plaintext cap must fail
/// closed without quarantine: the backend copy is identical. Forged declared
/// sizes whose actual suffix still fits (or whose digest does not match) are
/// cache poison and fall through to refetch.
fn cache_plaintext_cap_is_authoritative(
    bytes: &[u8],
    digest: &Digest,
    key: &DigestKey,
    spec: EnvelopeReadSpec<'_>,
) -> bool {
    let Some(payload) = Envelope::payload_suffix(bytes) else {
        return false;
    };
    if (payload.len() as u64) <= spec.max_plaintext_bytes.get() {
        return false;
    }
    key.digest(payload) == *digest
}

fn reject_log_namespace(name: &str) -> Result<()> {
    if name.starts_with("log/") {
        return Err(
            CoreError::Rejected("core set-target cannot write a log-owned ref".into()).into(),
        );
    }
    Ok(())
}

async fn reject_existing_log_manifest(store: &Store, current: &RefValue) -> Result<()> {
    reject_log_namespace(&current.name)?;
    let Some(digest) = current.target.as_ref() else {
        return Ok(());
    };
    let payload = if store.layout == KeyLayout::V2 {
        match store.get_blob(digest).await {
            Ok((p, _)) => p,
            Err(e) => return Err(map_commit_read_error(digest, e)),
        }
    } else {
        let spec = EnvelopeReadSpec {
            tenant: &store.tenant,
            kind: ObjectKind::Blob,
            allowed_schemas: LOG_MANIFEST_CARRIERS,
            max_encoded_bytes: NonZeroU64::new(comb_core::MAX_MANIFEST_OBJECT_BYTES)
                .expect("nonzero"),
            max_plaintext_bytes: NonZeroU64::new(comb_core::MAX_MANIFEST_OBJECT_BYTES)
                .expect("nonzero"),
        };
        match store.get_blob_limited(digest, spec).await {
            Ok((p, _)) => p,
            Err(e) => {
                if let Some(CoreError::UnsupportedEnvelopeFormat {
                    field: EnvelopeFormatField::Schema,
                    ..
                }) = e.downcast_ref()
                {
                    return Ok(());
                }
                return Err(map_commit_read_error(digest, e));
            }
        }
    };
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(&payload) else {
        return Ok(());
    };
    let schema = value.get("schema").and_then(|s| s.as_str()).unwrap_or("");
    if LOG_MANIFEST_CARRIERS.contains(&schema) {
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
        reject_log_namespace(&self.name)?;
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
            live_lease: None,
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
            live_lease: None,
        })
    }
}

struct ReleasePlan {
    name: String,
    fence: u64,
    writer: Option<String>,
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
        if let Some(writer) = &self.writer {
            match current.lease.as_ref() {
                Some(lease) if lease.writer == *writer => {}
                Some(_) => {
                    return Err(CoreError::Rejected("release by non-owner".into()).into());
                }
                None => {
                    return Err(
                        CoreError::Rejected("release requires the held lease".into()).into(),
                    );
                }
            }
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
            live_lease: None,
        })
    }
}

#[cfg(test)]
mod limited_reads {
    use super::*;
    use comb_core::error::CoreError;
    use comb_core::operation::OpIdentity;
    use comb_core::{DigestKey, EnvelopeReadSpec, ObjectKind};
    use comb_object::failpoint::{CountingBackend, FailpointBackend};
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

    #[tokio::test]
    async fn cache_unsupported_format_is_quarantined_and_refetched() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        std::fs::create_dir_all(&cache).unwrap();
        let mem: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
        let store = store_with_cache(mem, Some(cache.clone()));
        let payload = b"cached-ok".to_vec();
        let (digest, _) = store.put_blob(payload.clone()).await.unwrap();
        let spec = blob_spec(4096, 4096);
        let _ = store.get_blob_limited(&digest, spec).await.unwrap();
        let path = cache.join(digest.hex());
        let mut poisoned = std::fs::read(&path).unwrap();
        poisoned[6] = 1;
        poisoned[7] = 0;
        std::fs::write(&path, poisoned).unwrap();

        let (got, source) = store.get_blob_limited(&digest, spec).await.unwrap();
        assert_eq!(got, payload);
        assert_eq!(source, GetSource::Backend);
        assert!(path.with_extension("quarantine").exists());
    }

    #[tokio::test]
    async fn cache_wrong_tenant_is_quarantined_and_refetched() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        std::fs::create_dir_all(&cache).unwrap();
        let mem: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
        let store = store_with_cache(mem, Some(cache.clone()));
        let payload = b"tenant-ok".to_vec();
        let (digest, _) = store.put_blob(payload.clone()).await.unwrap();
        let spec = blob_spec(4096, 4096);
        let _ = store.get_blob_limited(&digest, spec).await.unwrap();
        let path = cache.join(digest.hex());
        let bytes = std::fs::read(&path).unwrap();
        let env = Envelope::decode(&bytes, &store.key).unwrap();
        let mut meta = serde_json::to_value(&env.meta).unwrap();
        meta["tenant"] = serde_json::json!("other");
        let meta_bytes = serde_json::to_vec(&meta).unwrap();
        let mut poisoned = Vec::new();
        poisoned.extend_from_slice(b"COMB");
        poisoned.extend_from_slice(&1u16.to_le_bytes());
        poisoned.extend_from_slice(&0u16.to_le_bytes());
        poisoned.extend_from_slice(&(meta_bytes.len() as u32).to_le_bytes());
        poisoned.extend_from_slice(&meta_bytes);
        poisoned.extend_from_slice(&env.payload);
        std::fs::write(&path, poisoned).unwrap();

        let (got, source) = store.get_blob_limited(&digest, spec).await.unwrap();
        assert_eq!(got, payload);
        assert_eq!(source, GetSource::Backend);
        assert!(path.with_extension("quarantine").exists());
    }

    #[tokio::test]
    async fn digest_matched_plaintext_cap_is_terminal_without_quarantine() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        std::fs::create_dir_all(&cache).unwrap();
        let counting = Arc::new(CountingBackend::new(Arc::new(MemoryBackend::new())));
        let store = store_with_cache(counting.clone(), Some(cache.clone()));
        let payload = vec![0xff; 64];
        let (digest, _) = store.put_blob(payload).await.unwrap();
        store
            .get_blob_limited(&digest, blob_spec(4096, 4096))
            .await
            .unwrap();
        let gets_after_fill = counting.get_count();
        let path = cache.join(digest.hex());
        assert!(path.exists());

        let err = core(
            store
                .get_blob_limited(&digest, blob_spec(4096, 63))
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
        assert_eq!(counting.get_count(), gets_after_fill);
        assert!(path.exists());
        assert!(!path.with_extension("quarantine").exists());
    }

    #[tokio::test]
    async fn get_blob_limited_does_not_fall_back_to_unbounded_get() {
        let mem = Arc::new(MemoryBackend::new());
        let counting = Arc::new(CountingBackend::new(mem));
        let fp = Arc::new(FailpointBackend::drop_next_get_limited_request(
            counting.clone(),
            "/objects/",
        ));
        let store = store_with_cache(fp, None);
        let payload = b"no-fallback".to_vec();
        let (digest, _) = store.put_blob(payload.clone()).await.unwrap();
        assert_eq!(counting.get_count(), 0);

        let err = core(
            store
                .get_blob_limited(&digest, blob_spec(4096, 4096))
                .await
                .unwrap_err(),
        );
        assert!(
            matches!(err, CoreError::BackendUnavailable(_)),
            "GetLimited-only failure must surface, got {err:?}"
        );
        assert_eq!(
            counting.get_count(),
            0,
            "get_blob_limited must not fall back to unbounded get"
        );

        let (got, source) = store.get_blob(&digest).await.unwrap();
        assert_eq!(got, payload);
        assert_eq!(source, GetSource::Backend);
        assert_eq!(counting.get_count(), 1);
    }

    #[tokio::test]
    async fn get_blob_quarantines_unsupported_cache_format_and_refetches() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        std::fs::create_dir_all(&cache).unwrap();
        let mem: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
        let store = store_with_cache(mem, Some(cache.clone()));
        let payload = b"r1-cached".to_vec();
        let (digest, _) = store.put_blob(payload.clone()).await.unwrap();
        let _ = store.get_blob(&digest).await.unwrap();
        let path = cache.join(digest.hex());
        let mut poisoned = std::fs::read(&path).unwrap();
        poisoned[6] = 1;
        poisoned[7] = 0;
        std::fs::write(&path, poisoned).unwrap();

        let (got, source) = store.get_blob(&digest).await.unwrap();
        assert_eq!(got, payload);
        assert_eq!(source, GetSource::Backend);
        assert!(path.with_extension("quarantine").exists());
    }

    #[test]
    fn v2_and_v3_layouts_use_distinct_keys_and_cache_paths() {
        let mem: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
        let dir = tempfile::tempdir().unwrap();
        let v2 = store_with_cache(mem.clone(), Some(dir.path().to_path_buf()));
        let v3 = v2.clone().with_layout(KeyLayout::V3);
        let digest = DigestKey::from_bytes([1u8; 32]).digest(b"x");
        assert_ne!(v2.object_key(&digest), v3.object_key(&digest));
        assert!(v2.object_key(&digest).starts_with("comb/v2/"));
        assert!(v3.object_key(&digest).starts_with("comb/v3/"));
        assert_ne!(v2.ref_key("log/a/p0"), v3.ref_key("log/a/p0"));
        let op = v2.mint_operation();
        assert_ne!(
            v2.intent_key(&OpIdentity::Generic(op.clone())),
            v3.intent_key(&OpIdentity::Generic(op))
        );
        assert_ne!(v2.cache_path(&digest), v3.cache_path(&digest));
    }

    #[tokio::test]
    async fn set_target_transient_manifest_read_is_not_rejected() {
        use crate::publish::LOG_MANIFEST_SCHEMA;
        use comb_core::commit::CommitHeader;
        use comb_core::HEADER_SCHEMA;

        let mem = Arc::new(MemoryBackend::new());
        let store = store_with_cache(mem.clone(), None).with_layout(KeyLayout::V3);
        let op = store.mint_operation();
        let header = CommitHeader {
            schema: HEADER_SCHEMA.into(),
            resource: "owned".into(),
            generation: 1,
            epoch: 0,
            identity: op.to_string(),
            request: store.key.digest(b"req"),
            parent: None,
            skip: None,
            at: chrono::Utc::now(),
        };
        header.validate().unwrap();
        let payload = serde_json::to_vec(&serde_json::json!({
            "schema": LOG_MANIFEST_SCHEMA,
            "header": header,
            "log": "owned",
            "epoch": 0,
            "head_seq": 0,
            "chunks": [],
            "retention": "complete",
        }))
        .unwrap();
        let digest = store
            .put_object(
                ObjectKind::Blob,
                LOG_MANIFEST_SCHEMA,
                payload,
                nz(64 * 1024),
            )
            .await
            .unwrap();
        store
            .set_target("owned", digest.clone(), None)
            .await
            .unwrap();

        let fp = Arc::new(FailpointBackend::io_on_next_get_limited(mem, "/objects/"));
        let fenced = store_with_cache(fp, None).with_layout(KeyLayout::V3);
        let other = store.key.digest(b"other");
        let err = fenced.set_target("owned", other, None).await.unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<CoreError>(),
                Some(CoreError::BackendUnavailable(_)) | Some(CoreError::Io(_))
            ),
            "transient manifest read must not become Rejected, got {err:#}"
        );
    }
}
