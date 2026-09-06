//! Fault-injecting backend wrapper (spec §22.7).
//!
//! Wraps any backend and injects the two failures that matter for
//! protocol correctness:
//!
//! - **request lost**: the operation never reaches the backend;
//! - **response lost**: the operation committed but the caller sees an
//!   error (ambiguous acknowledgement).
//!
//! Injection is driven by a seeded RNG so every chaos run is reproducible.

use crate::backend::{LimitedObject, ObjectBackend, ObjectInfo, Version};
use async_trait::async_trait;
use comb_core::error::{CoreError, Result};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub struct FaultBackend {
    inner: Arc<dyn ObjectBackend>,
    rng: Mutex<StdRng>,
    /// Probability a request is lost before execution.
    pub fail_before: f64,
    /// Probability a mutation's response is lost after it committed.
    pub fail_after: f64,
    pub injected_before: AtomicU64,
    pub injected_after: AtomicU64,
}

impl FaultBackend {
    pub fn new(
        inner: Arc<dyn ObjectBackend>,
        seed: u64,
        fail_before: f64,
        fail_after: f64,
    ) -> Self {
        Self {
            inner,
            rng: Mutex::new(StdRng::seed_from_u64(seed)),
            fail_before,
            fail_after,
            injected_before: AtomicU64::new(0),
            injected_after: AtomicU64::new(0),
        }
    }

    fn roll(&self, p: f64) -> bool {
        p > 0.0 && self.rng.lock().unwrap().random_bool(p)
    }

    fn lost_request(&self, what: &str) -> CoreError {
        self.injected_before.fetch_add(1, Ordering::Relaxed);
        CoreError::BackendUnavailable(format!("injected: {what} request lost before backend"))
    }

    fn lost_response(&self, what: &str) -> CoreError {
        self.injected_after.fetch_add(1, Ordering::Relaxed);
        CoreError::BackendUnavailable(format!("injected: {what} committed but response lost"))
    }
}

#[async_trait]
impl ObjectBackend for FaultBackend {
    async fn put_create(&self, key: &str, body: &[u8]) -> Result<Version> {
        if self.roll(self.fail_before) {
            return Err(self.lost_request("put_create"));
        }
        let v = self.inner.put_create(key, body).await?;
        if self.roll(self.fail_after) {
            return Err(self.lost_response("put_create"));
        }
        Ok(v)
    }

    async fn put_update(
        &self,
        key: &str,
        expected: Option<&Version>,
        body: &[u8],
    ) -> Result<Version> {
        if self.roll(self.fail_before) {
            return Err(self.lost_request("put_update"));
        }
        let v = self.inner.put_update(key, expected, body).await?;
        if self.roll(self.fail_after) {
            return Err(self.lost_response("put_update"));
        }
        Ok(v)
    }

    async fn get(&self, key: &str) -> Result<(Vec<u8>, Version)> {
        if self.roll(self.fail_before) {
            return Err(self.lost_request("get"));
        }
        self.inner.get(key).await
    }

    async fn get_limited(&self, key: &str, max_encoded_bytes: NonZeroU64) -> Result<LimitedObject> {
        if self.roll(self.fail_before) {
            return Err(self.lost_request("get_limited"));
        }
        self.inner.get_limited(key, max_encoded_bytes).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        if self.roll(self.fail_before) {
            return Err(self.lost_request("exists"));
        }
        self.inner.exists(key).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        if self.roll(self.fail_before) {
            return Err(self.lost_request("delete"));
        }
        self.inner.delete(key).await?;
        if self.roll(self.fail_after) {
            return Err(self.lost_response("delete"));
        }
        Ok(())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>> {
        if self.roll(self.fail_before) {
            return Err(self.lost_request("list"));
        }
        self.inner.list(prefix).await
    }
}
