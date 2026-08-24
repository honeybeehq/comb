use crate::backend::{ObjectBackend, Version};
use async_trait::async_trait;
use comb_core::error::{CoreError, Result};
use std::collections::HashMap;
use std::sync::Mutex;

/// In-memory backend for tests (spec §7.10). Version tokens are counters.
#[derive(Default)]
pub struct MemoryBackend {
    state: Mutex<HashMap<String, (Vec<u8>, u64)>>,
}

impl MemoryBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ObjectBackend for MemoryBackend {
    async fn put_create(&self, key: &str, body: &[u8]) -> Result<Version> {
        let mut state = self.state.lock().unwrap();
        if state.contains_key(key) {
            return Err(CoreError::AlreadyExists(key.into()));
        }
        state.insert(key.into(), (body.to_vec(), 1));
        Ok(Version("1".into()))
    }

    async fn put_update(&self, key: &str, expected: Option<&Version>, body: &[u8]) -> Result<Version> {
        let mut state = self.state.lock().unwrap();
        match (state.get(key), expected) {
            (None, None) => {
                state.insert(key.into(), (body.to_vec(), 1));
                Ok(Version("1".into()))
            }
            (None, Some(_)) => Err(CoreError::PreconditionFailed(format!("{key}: gone"))),
            (Some(_), None) => Err(CoreError::AlreadyExists(key.into())),
            (Some((_, v)), Some(exp)) => {
                if exp.0 != v.to_string() {
                    return Err(CoreError::PreconditionFailed(format!(
                        "{key}: expected version {}, live {v}",
                        exp.0
                    )));
                }
                let next = v + 1;
                state.insert(key.into(), (body.to_vec(), next));
                Ok(Version(next.to_string()))
            }
        }
    }

    async fn get(&self, key: &str) -> Result<(Vec<u8>, Version)> {
        let state = self.state.lock().unwrap();
        state
            .get(key)
            .map(|(b, v)| (b.clone(), Version(v.to_string())))
            .ok_or_else(|| CoreError::NotFound(key.into()))
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.state.lock().unwrap().contains_key(key))
    }
}
