use crate::backend::{object_too_large, read_sync_limited, ObjectBackend, ObjectInfo, Version};
use async_trait::async_trait;
use comb_core::error::{CoreError, Result};
use std::fs;
use std::io::Write;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

/// Local filesystem backend (spec §7.11): create-only objects through
/// exclusive hard-link, conditional ref update through an atomic rename
/// guarded by an observed version and an advisory file lock, `fsync` on
/// file and parent directory. Version tokens are unkeyed BLAKE3 of the
/// stored bytes — a provider token, not a content identity.
pub struct LocalBackend {
    root: PathBuf,
}

impl LocalBackend {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path_for(&self, key: &str) -> Result<PathBuf> {
        if key.split('/').any(|seg| seg == ".." || seg.is_empty()) {
            return Err(CoreError::InvalidFormat(format!("bad key: {key}")));
        }
        Ok(self.root.join(key))
    }

    fn version_of(bytes: &[u8]) -> Version {
        Version(hex::encode(blake3::hash(bytes).as_bytes()))
    }

    fn fsync_dir(dir: &Path) -> Result<()> {
        // Directory fsync is required for durable rename visibility (§7.11).
        let d = fs::File::open(dir)?;
        d.sync_all()?;
        Ok(())
    }

    fn write_tmp(dir: &Path, body: &[u8]) -> Result<PathBuf> {
        fs::create_dir_all(dir)?;
        let tmp = dir.join(format!(".tmp-{}-{}", std::process::id(), rand_suffix()));
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)?;
        f.write_all(body)?;
        f.sync_all()?;
        Ok(tmp)
    }
}

fn rand_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("{nanos:08x}")
}

#[async_trait]
impl ObjectBackend for LocalBackend {
    async fn put_create(&self, key: &str, body: &[u8]) -> Result<Version> {
        let path = self.path_for(key)?;
        let dir = path.parent().expect("key has parent");
        let tmp = Self::write_tmp(dir, body)?;
        // hard_link fails if the destination exists: exclusive create.
        let result = fs::hard_link(&tmp, &path);
        fs::remove_file(&tmp).ok();
        match result {
            Ok(()) => {
                Self::fsync_dir(dir)?;
                Ok(Self::version_of(body))
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                Err(CoreError::AlreadyExists(key.into()))
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn put_update(
        &self,
        key: &str,
        expected: Option<&Version>,
        body: &[u8],
    ) -> Result<Version> {
        let Some(expected) = expected else {
            return self.put_create(key, body).await;
        };
        let path = self.path_for(key)?;
        let dir = path.parent().expect("key has parent").to_path_buf();
        fs::create_dir_all(&dir)?;

        // Advisory lock serializes local writers; the version check makes
        // the update conditional (§7.11).
        let lock_path = dir.join(format!(
            ".lock-{}",
            path.file_name().unwrap().to_string_lossy()
        ));
        let lock = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&lock_path)?;
        lock.lock()?;

        let live = match fs::read(&path) {
            Ok(bytes) => Some(Self::version_of(&bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        match live {
            None => return Err(CoreError::PreconditionFailed(format!("{key}: gone"))),
            Some(ref live) if live == expected => {}
            Some(live) => {
                return Err(CoreError::PreconditionFailed(format!(
                    "{key}: expected version {}, live {}",
                    expected.0, live.0
                )))
            }
        }

        let tmp = Self::write_tmp(&dir, body)?;
        fs::rename(&tmp, &path)?;
        Self::fsync_dir(&dir)?;
        Ok(Self::version_of(body))
    }

    async fn get(&self, key: &str) -> Result<(Vec<u8>, Version)> {
        let path = self.path_for(key)?;
        match fs::read(&path) {
            Ok(bytes) => {
                let v = Self::version_of(&bytes);
                Ok((bytes, v))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(CoreError::NotFound(key.into()))
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn get_limited(
        &self,
        key: &str,
        max_encoded_bytes: NonZeroU64,
    ) -> Result<(Vec<u8>, Version)> {
        let path = self.path_for(key)?;
        let meta = match fs::metadata(&path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(CoreError::NotFound(key.into()));
            }
            Err(e) => return Err(e.into()),
        };
        if meta.len() > max_encoded_bytes.get() {
            return Err(object_too_large(key, max_encoded_bytes, Some(meta.len())));
        }
        let file = match fs::File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(CoreError::NotFound(key.into()));
            }
            Err(e) => return Err(e.into()),
        };
        let bytes = read_sync_limited(file, key, max_encoded_bytes)?;
        let version = Self::version_of(&bytes);
        Ok((bytes, version))
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.path_for(key)?.exists())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        match fs::remove_file(self.path_for(key)?) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>> {
        let mut out = Vec::new();
        let mut stack = vec![self.root.clone()];
        while let Some(dir) = stack.pop() {
            let entries = match fs::read_dir(&dir) {
                Ok(e) => e,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            for entry in entries {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.starts_with(".tmp-") || name.starts_with(".lock-") {
                    continue;
                }
                let key = path
                    .strip_prefix(&self.root)
                    .expect("under root")
                    .to_string_lossy()
                    .replace('\\', "/");
                if !key.starts_with(prefix) {
                    continue;
                }
                let modified: chrono::DateTime<chrono::Utc> = entry.metadata()?.modified()?.into();
                out.push(ObjectInfo { key, modified });
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn mixed_create_paths_have_one_winner() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        use std::time::{Duration, Instant};

        let dir = tempfile::tempdir().unwrap();
        let backend = Arc::new(LocalBackend::new(dir.path()));
        let completed = Arc::new(AtomicBool::new(false));
        let writer = backend.clone();
        let writer_done = completed.clone();
        // Observe the temporary write, after conditional creation checked for
        // absence but before publication. A large body gives the competing
        // create time to publish; no sleep decides the ordering.
        let worker = std::thread::spawn(move || {
            let body = vec![7u8; 32 * 1024 * 1024];
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let result = runtime.block_on(writer.put_update("mixed", None, &body));
            writer_done.store(true, Ordering::Release);
            result
        });
        let deadline = Instant::now() + Duration::from_secs(10);
        let observed_temp = loop {
            let temp_exists = fs::read_dir(dir.path()).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".tmp-")
            });
            if temp_exists {
                break true;
            }
            if completed.load(Ordering::Acquire) || Instant::now() >= deadline {
                break false;
            }
            std::thread::yield_now();
        };
        let competing = backend.put_create("mixed", b"competing create").await;
        let original = worker.join().unwrap();
        eprintln!("competing create observed temporary write: {observed_temp}");
        let successes = usize::from(original.is_ok()) + usize::from(competing.is_ok());
        assert_eq!(
            successes, 1,
            "both create paths must share atomic create-only semantics"
        );
        let losing = if original.is_err() {
            original
        } else {
            competing
        };
        assert!(matches!(losing, Err(CoreError::AlreadyExists(_))));
    }

    #[tokio::test]
    async fn create_only_and_cas() {
        let dir = tempfile::tempdir().unwrap();
        let b = LocalBackend::new(dir.path());

        let v1 = b.put_create("t/objects/aa/x", b"one").await.unwrap();
        assert!(matches!(
            b.put_create("t/objects/aa/x", b"two").await,
            Err(CoreError::AlreadyExists(_))
        ));

        let v2 = b.put_update("t/refs/r.json", None, b"g1").await.unwrap();
        let v3 = b
            .put_update("t/refs/r.json", Some(&v2), b"g2")
            .await
            .unwrap();
        assert!(matches!(
            b.put_update("t/refs/r.json", Some(&v2), b"g3").await,
            Err(CoreError::PreconditionFailed(_))
        ));
        let (bytes, live) = b.get("t/refs/r.json").await.unwrap();
        assert_eq!(bytes, b"g2");
        assert_eq!(live, v3);
        let _ = v1;
    }
}
