use async_trait::async_trait;
use comb_core::error::Result;

/// Opaque provider version token used for conditional replacement
/// (spec §7.5). Never a content identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Version(pub String);

#[async_trait]
pub trait ObjectBackend: Send + Sync {
    /// Create-only write. Fails with `AlreadyExists` if the key exists.
    async fn put_create(&self, key: &str, body: &[u8]) -> Result<Version>;

    /// Conditional replacement. `expected = None` means "create, key must
    /// not exist"; `Some(v)` means "replace only if the live version is v".
    /// Failure is `PreconditionFailed` (or `AlreadyExists` for None).
    async fn put_update(&self, key: &str, expected: Option<&Version>, body: &[u8]) -> Result<Version>;

    /// Read the object and its current version token.
    async fn get(&self, key: &str) -> Result<(Vec<u8>, Version)>;

    async fn exists(&self, key: &str) -> Result<bool>;

    /// Delete an object. Deleting a missing key is not an error.
    async fn delete(&self, key: &str) -> Result<()>;
}
