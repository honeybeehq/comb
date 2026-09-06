use async_trait::async_trait;
use bytes::Bytes;
use comb_core::error::{CoreError, Result};
use std::io::Read;
use std::num::NonZeroU64;

/// Opaque provider version token used for conditional replacement
/// (spec §7.5). Never a content identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version(pub String);

/// Listing entry. Listing is for discovery, orphan scanning, and GC only —
/// never a linearization mechanism (spec §7.8).
#[derive(Debug, Clone)]
pub struct ObjectInfo {
    pub key: String,
    pub modified: chrono::DateTime<chrono::Utc>,
}

/// Object bytes that already passed an encoded-size cap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitedObject {
    pub bytes: Bytes,
    pub version: Version,
}

pub(crate) fn object_too_large(key: &str, limit: NonZeroU64, actual: Option<u64>) -> CoreError {
    CoreError::ObjectTooLarge {
        key: key.to_string(),
        limit: limit.get(),
        actual,
    }
}

pub(crate) fn read_sync_limited<R: Read>(
    reader: R,
    key: &str,
    max_encoded_bytes: NonZeroU64,
) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    reader
        .take(max_encoded_bytes.get().saturating_add(1))
        .read_to_end(&mut buf)?;
    if buf.len() as u64 > max_encoded_bytes.get() {
        return Err(object_too_large(key, max_encoded_bytes, None));
    }
    Ok(buf)
}

#[async_trait]
pub trait ObjectBackend: Send + Sync {
    /// Create-only write. Fails with `AlreadyExists` if the key exists.
    async fn put_create(&self, key: &str, body: &[u8]) -> Result<Version>;

    /// Conditional replacement. `expected = None` means "create, key must
    /// not exist"; `Some(v)` means "replace only if the live version is v".
    /// Failure is `PreconditionFailed` (or `AlreadyExists` for None).
    async fn put_update(
        &self,
        key: &str,
        expected: Option<&Version>,
        body: &[u8],
    ) -> Result<Version>;

    /// Read the object and its current version token.
    async fn get(&self, key: &str) -> Result<(Vec<u8>, Version)>;

    /// Read the object only if its encoded size is at most `max_encoded_bytes`.
    ///
    /// The cap is enforced before an unbounded clone or body collect. One
    /// extra byte above the cap is `ObjectTooLarge`, never a truncated body.
    async fn get_limited(&self, key: &str, max_encoded_bytes: NonZeroU64) -> Result<LimitedObject>;

    async fn exists(&self, key: &str) -> Result<bool>;

    /// Delete an object. Deleting a missing key is not an error.
    async fn delete(&self, key: &str) -> Result<()>;

    /// List keys under a prefix. Discovery only; may be stale (spec §7.8).
    async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>>;
}
