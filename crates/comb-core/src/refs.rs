use crate::digest::Digest;
use serde::{Deserialize, Serialize};

/// A lease on a ref (spec §7.5, §7.6). The lease lives inside the ref value,
/// so one conditional update atomically manages target and lease together
/// (v0.3). `epoch` is the fencing token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lease {
    pub writer: String,
    pub lease_until: chrono::DateTime<chrono::Utc>,
}

/// The decoded value of a named ref (spec §7.5). The only generally mutable
/// object in Comb. `generation` advances on every logical change; lease
/// renewals rewrite `lease` without advancing it. `head_commit` is the
/// digest of the commit (or log manifest) that produced this generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RefValue {
    pub schema: String,
    pub tenant: String,
    pub name: String,
    pub generation: u64,
    pub epoch: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<Digest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease: Option<Lease>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_commit: Option<Digest>,
}

impl RefValue {
    pub const SCHEMA: &'static str = "comb.ref/v2";

    pub fn new(tenant: &str, name: &str) -> Self {
        RefValue {
            schema: Self::SCHEMA.into(),
            tenant: tenant.into(),
            name: name.into(),
            generation: 0,
            epoch: 0,
            target: None,
            lease: None,
            updated_at: chrono::Utc::now(),
            head_commit: None,
        }
    }

    pub fn validate_schema(&self) -> crate::error::Result<()> {
        if self.schema != Self::SCHEMA {
            return Err(crate::error::CoreError::InvalidFormat(format!(
                "unsupported ref schema {} (want {})",
                self.schema,
                Self::SCHEMA
            )));
        }
        Ok(())
    }

    pub fn lease_live(&self, now: chrono::DateTime<chrono::Utc>) -> bool {
        self.lease.as_ref().is_some_and(|l| l.lease_until > now)
    }
}

/// One entry in the ref journal (spec §7.5a.3). Entries chain by parent
/// digest; consecutive entries for one ref carry consecutive generations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefJournalEntry {
    pub schema: String,
    pub ref_name: String,
    pub prev_generation: u64,
    pub new_generation: u64,
    pub epoch: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<Digest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub writer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<Digest>,
    pub at: chrono::DateTime<chrono::Utc>,
}
