//! Comb Core: content identity, object envelope, and the ref model.
//!
//! This crate holds only the shared correctness primitives defined in the
//! specification (§7). It knows nothing about backends, views, or consumers.

pub mod commit;
pub mod digest;
pub mod envelope;
pub mod error;
pub mod operation;
pub mod refs;

pub use commit::{
    skip_distance, skip_target_generation, Admission, Commit, CommitHeader, HistoryEntry,
    IntentState, OpIntent, COMMIT_SCHEMA, HEADER_SCHEMA, INTENT_SCHEMA,
};
pub use digest::{Digest, DigestKey};
pub use envelope::{
    Envelope, EnvelopeExpectation, ObjectClass, ObjectKind, MAX_ENVELOPE_META_BYTES,
    MAX_MANIFEST_OBJECT_BYTES, MAX_REF_OBJECT_BYTES, MAX_STABLE_INDEX_NODE_OBJECT_BYTES,
};
pub use error::{CoreError, EnvelopeFormatField};
pub use operation::{
    domain_hash, Clock, FrozenClock, Material, OpIdentity, OperationId, OperationPolicy, StableKey,
    SystemClock, MAX_STABLE_KEY_BYTES,
};
pub use refs::{Lease, RefValue};
