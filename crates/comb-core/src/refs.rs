use crate::digest::Digest;
use serde::{Deserialize, Serialize};

/// A lease on a ref (spec §7.5, §7.6). The lease lives inside the ref value,
/// so one conditional update atomically manages target and lease together
/// (v0.3). `epoch` is the fencing token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub writer: String,
    pub lease_until: chrono::DateTime<chrono::Utc>,
}

/// The decoded value of a named ref (spec §7.5). The only generally mutable
/// object in Comb. `generation` advances on every logical change; lease
/// renewals rewrite `lease` without advancing it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
}

impl RefValue {
    pub fn new(tenant: &str, name: &str) -> Self {
        RefValue {
            schema: "comb.ref/v1".into(),
            tenant: tenant.into(),
            name: name.into(),
            generation: 0,
            epoch: 0,
            target: None,
            lease: None,
            updated_at: chrono::Utc::now(),
        }
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
