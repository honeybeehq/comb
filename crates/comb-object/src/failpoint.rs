//! Deterministic failpoints at named backend calls.
//!
//! Unlike [`crate::fault::FaultBackend`], this wrapper never rolls a die.
//! Tests arm a rule against a key substring and a method; the rule fires
//! after a counted number of matching successes.

use crate::backend::{LimitedObject, ObjectBackend, ObjectInfo, Version};
use async_trait::async_trait;
use comb_core::error::{CoreError, Result};
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailMethod {
    PutUpdate,
    PutCreate,
    Get,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailAction {
    /// Do not call the inner backend.
    DropRequest,
    /// Call the inner backend, then return an injected error.
    DropResponse,
    /// Return a typed `CoreError::Io` without calling the inner backend.
    Io,
}

#[derive(Debug, Clone)]
pub struct FailRule {
    pub method: FailMethod,
    pub key_contains: String,
    /// Matching successful inner calls to let through before firing.
    pub successes_before_fire: u64,
    pub fires: u64,
    pub action: FailAction,
}

struct LiveRule {
    spec: FailRule,
    seen_successes: u64,
    fired: u64,
}

pub struct FailpointBackend {
    inner: Arc<dyn ObjectBackend>,
    rules: Mutex<Vec<LiveRule>>,
    pub injected: AtomicU64,
}

impl FailpointBackend {
    pub fn new(inner: Arc<dyn ObjectBackend>) -> Self {
        Self {
            inner,
            rules: Mutex::new(Vec::new()),
            injected: AtomicU64::new(0),
        }
    }

    pub fn arm(&self, rule: FailRule) {
        self.rules.lock().unwrap().push(LiveRule {
            spec: rule,
            seen_successes: 0,
            fired: 0,
        });
    }

    /// Drop the response of the next successful `put_update` whose key contains `needle`.
    pub fn drop_next_put_update_response(inner: Arc<dyn ObjectBackend>, needle: &str) -> Self {
        let fp = Self::new(inner);
        fp.arm(FailRule {
            method: FailMethod::PutUpdate,
            key_contains: needle.into(),
            successes_before_fire: 0,
            fires: 1,
            action: FailAction::DropResponse,
        });
        fp
    }

    /// Fail the next matching `put_update` before the inner call (no commit).
    pub fn drop_next_put_update_request(inner: Arc<dyn ObjectBackend>, needle: &str) -> Self {
        let fp = Self::new(inner);
        fp.arm(FailRule {
            method: FailMethod::PutUpdate,
            key_contains: needle.into(),
            successes_before_fire: 0,
            fires: 1,
            action: FailAction::DropRequest,
        });
        fp
    }

    /// Fail the next matching `get` before the inner call (no confirmed NotFound).
    pub fn drop_next_get_request(inner: Arc<dyn ObjectBackend>, needle: &str) -> Self {
        let fp = Self::new(inner);
        fp.arm(FailRule {
            method: FailMethod::Get,
            key_contains: needle.into(),
            successes_before_fire: 0,
            fires: 1,
            action: FailAction::DropRequest,
        });
        fp
    }

    /// Fail the next matching `get` with `CoreError::Io`.
    pub fn io_on_next_get(inner: Arc<dyn ObjectBackend>, needle: &str) -> Self {
        let fp = Self::new(inner);
        fp.arm(FailRule {
            method: FailMethod::Get,
            key_contains: needle.into(),
            successes_before_fire: 0,
            fires: 1,
            action: FailAction::Io,
        });
        fp
    }

    fn injected_io(&self, what: &str) -> CoreError {
        self.injected.fetch_add(1, Ordering::Relaxed);
        CoreError::Io(std::io::Error::other(format!("injected io: {what}")))
    }

    fn decide(&self, method: FailMethod, key: &str) -> Option<FailAction> {
        let mut rules = self.rules.lock().unwrap();
        for rule in rules.iter_mut() {
            if rule.spec.method != method || !key.contains(&rule.spec.key_contains) {
                continue;
            }
            if rule.fired >= rule.spec.fires {
                continue;
            }
            match rule.spec.action {
                FailAction::DropRequest | FailAction::Io => {
                    rule.fired += 1;
                    return Some(rule.spec.action);
                }
                FailAction::DropResponse => {
                    if rule.seen_successes < rule.spec.successes_before_fire {
                        continue;
                    }
                    return Some(FailAction::DropResponse);
                }
            }
        }
        None
    }

    fn note_success(&self, method: FailMethod, key: &str, armed_drop_response: bool) {
        let mut rules = self.rules.lock().unwrap();
        for rule in rules.iter_mut() {
            if rule.spec.method != method || !key.contains(&rule.spec.key_contains) {
                continue;
            }
            if rule.fired >= rule.spec.fires {
                continue;
            }
            if rule.spec.action == FailAction::DropResponse {
                if armed_drop_response && rule.seen_successes >= rule.spec.successes_before_fire {
                    rule.fired += 1;
                } else {
                    rule.seen_successes += 1;
                }
            }
        }
    }

    fn injected_err(&self, what: &str) -> CoreError {
        self.injected.fetch_add(1, Ordering::Relaxed);
        CoreError::BackendUnavailable(format!("injected: {what}"))
    }
}

#[async_trait]
impl ObjectBackend for FailpointBackend {
    async fn put_create(&self, key: &str, body: &[u8]) -> Result<Version> {
        match self.decide(FailMethod::PutCreate, key) {
            Some(FailAction::DropRequest) => {
                return Err(self.injected_err("put_create request lost before backend"))
            }
            Some(FailAction::Io) => return Err(self.injected_io("put_create")),
            Some(FailAction::DropResponse) => {
                let v = self.inner.put_create(key, body).await?;
                self.note_success(FailMethod::PutCreate, key, true);
                let _ = v;
                return Err(self.injected_err("put_create committed but response lost"));
            }
            None => {
                let v = self.inner.put_create(key, body).await?;
                self.note_success(FailMethod::PutCreate, key, false);
                Ok(v)
            }
        }
    }

    async fn put_update(
        &self,
        key: &str,
        expected: Option<&Version>,
        body: &[u8],
    ) -> Result<Version> {
        match self.decide(FailMethod::PutUpdate, key) {
            Some(FailAction::DropRequest) => {
                return Err(self.injected_err("put_update request lost before backend"))
            }
            Some(FailAction::Io) => return Err(self.injected_io("put_update")),
            Some(FailAction::DropResponse) => {
                let v = self.inner.put_update(key, expected, body).await?;
                self.note_success(FailMethod::PutUpdate, key, true);
                let _ = v;
                return Err(self.injected_err("put_update committed but response lost"));
            }
            None => {
                let v = self.inner.put_update(key, expected, body).await?;
                self.note_success(FailMethod::PutUpdate, key, false);
                Ok(v)
            }
        }
    }

    async fn get(&self, key: &str) -> Result<(Vec<u8>, Version)> {
        match self.decide(FailMethod::Get, key) {
            Some(FailAction::DropRequest) | Some(FailAction::DropResponse) => {
                Err(self.injected_err("get request lost"))
            }
            Some(FailAction::Io) => Err(self.injected_io("get")),
            None => self.inner.get(key).await,
        }
    }

    async fn get_limited(&self, key: &str, max_encoded_bytes: NonZeroU64) -> Result<LimitedObject> {
        match self.decide(FailMethod::Get, key) {
            Some(FailAction::DropRequest) | Some(FailAction::DropResponse) => {
                Err(self.injected_err("get_limited request lost"))
            }
            Some(FailAction::Io) => Err(self.injected_io("get_limited")),
            None => self.inner.get_limited(key, max_encoded_bytes).await,
        }
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>> {
        self.inner.list(prefix).await
    }
}

/// Counts backend calls for seek-complexity tests.
pub struct CountingBackend {
    inner: Arc<dyn ObjectBackend>,
    pub gets: AtomicU64,
    pub put_updates: AtomicU64,
    pub put_creates: AtomicU64,
}

impl CountingBackend {
    pub fn new(inner: Arc<dyn ObjectBackend>) -> Self {
        Self {
            inner,
            gets: AtomicU64::new(0),
            put_updates: AtomicU64::new(0),
            put_creates: AtomicU64::new(0),
        }
    }

    pub fn get_count(&self) -> u64 {
        self.gets.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl ObjectBackend for CountingBackend {
    async fn put_create(&self, key: &str, body: &[u8]) -> Result<Version> {
        self.put_creates.fetch_add(1, Ordering::Relaxed);
        self.inner.put_create(key, body).await
    }

    async fn put_update(
        &self,
        key: &str,
        expected: Option<&Version>,
        body: &[u8],
    ) -> Result<Version> {
        self.put_updates.fetch_add(1, Ordering::Relaxed);
        self.inner.put_update(key, expected, body).await
    }

    async fn get(&self, key: &str) -> Result<(Vec<u8>, Version)> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get(key).await
    }

    async fn get_limited(&self, key: &str, max_encoded_bytes: NonZeroU64) -> Result<LimitedObject> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        self.inner.get_limited(key, max_encoded_bytes).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>> {
        self.inner.list(prefix).await
    }
}
