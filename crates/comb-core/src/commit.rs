//! Immutable commit headers, intents, and skip-pointer helpers.

use crate::digest::Digest;
use crate::error::{CoreError, Result};
use crate::operation::OpIdentity;
use crate::refs::RefValue;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const COMMIT_SCHEMA: &str = "comb.commit/v1";
pub const INTENT_SCHEMA: &str = "comb.op-intent/v1";
pub const HEADER_SCHEMA: &str = "comb.commit-header/v1";

/// Header embedded in every logical publication (plain commit or log manifest).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitHeader {
    pub schema: String,
    pub resource: String,
    pub generation: u64,
    pub epoch: u64,
    pub identity: String,
    pub request: Digest,
    pub parent: Option<Digest>,
    pub skip: Option<Digest>,
    pub at: DateTime<Utc>,
}

impl CommitHeader {
    pub fn validate(&self) -> Result<()> {
        if self.schema != HEADER_SCHEMA {
            return Err(CoreError::InvalidFormat(format!(
                "unsupported commit header schema {}",
                self.schema
            )));
        }
        OpIdentity::parse(&self.identity)?;
        Ok(())
    }
}

/// Immutable commit object for Core refs. Log publications embed the header
/// in the partition manifest instead of this envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Commit {
    pub schema: String,
    pub header: CommitHeader,
    pub change: serde_json::Value,
    pub result: serde_json::Value,
    /// Original ref value produced by this commit, without `head_commit`
    /// (that field is the digest of this object and is filled on recovery).
    pub ref_state: RefValue,
}

impl Commit {
    pub fn validate(&self) -> Result<()> {
        if self.schema != COMMIT_SCHEMA {
            return Err(CoreError::InvalidFormat(format!(
                "unsupported commit schema {}",
                self.schema
            )));
        }
        self.ref_state.validate_schema()?;
        self.header.validate()
    }
}

/// One producer admission recorded in a log publication. Bounded per commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Admission {
    pub identity: String,
    pub request: Digest,
    pub first: u64,
    pub last: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum IntentState {
    Pending,
    Applied {
        generation: u64,
        commit: Digest,
        result: serde_json::Value,
    },
}

/// CAS-guarded per-identity intent. Completed state is a cache; the commit
/// chain is recovery truth. `expires_at` is None for stable keys.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpIntent {
    pub schema: String,
    pub identity: String,
    pub resource: String,
    pub request: Digest,
    pub base_generation: u64,
    pub proposed: Vec<Digest>,
    pub state: IntentState,
    pub expires_at: Option<DateTime<Utc>>,
}

impl OpIntent {
    pub fn validate(&self) -> Result<()> {
        if self.schema != INTENT_SCHEMA {
            return Err(CoreError::InvalidFormat(format!(
                "unsupported intent schema {}",
                self.schema
            )));
        }
        OpIdentity::parse(&self.identity)?;
        if self.proposed.len() > 64 {
            return Err(CoreError::InvalidFormat(format!(
                "intent {} proposed digest list is unreasonably large",
                self.identity
            )));
        }
        Ok(())
    }

    pub fn pending(
        identity: &OpIdentity,
        resource: &str,
        request: Digest,
        base_generation: u64,
        expires_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            schema: INTENT_SCHEMA.into(),
            identity: identity.canonical(),
            resource: resource.into(),
            request,
            base_generation,
            proposed: Vec::new(),
            state: IntentState::Pending,
            expires_at,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HistoryEntry {
    pub generation: u64,
    pub epoch: u64,
    pub identity: String,
    pub request: Digest,
    pub target: Option<Digest>,
    pub at: DateTime<Utc>,
    pub commit: Digest,
}

/// Largest power of two that divides `generation` (Fenwick skip). Generation 0 is 0.
pub fn skip_distance(generation: u64) -> u64 {
    if generation == 0 {
        0
    } else {
        generation & generation.wrapping_neg()
    }
}

pub fn skip_target_generation(generation: u64) -> u64 {
    generation.saturating_sub(skip_distance(generation))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skip_distances() {
        assert_eq!(skip_distance(0), 0);
        assert_eq!(skip_distance(1), 1);
        assert_eq!(skip_distance(2), 2);
        assert_eq!(skip_distance(3), 1);
        assert_eq!(skip_distance(8), 8);
        assert_eq!(skip_distance(12), 4);
        assert_eq!(skip_target_generation(12), 8);
        assert_eq!(skip_target_generation(1), 0);
    }
}
