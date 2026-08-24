//! Comb Core: content identity, object envelope, and the ref model.
//!
//! This crate holds only the shared correctness primitives defined in the
//! specification (§7). It knows nothing about backends, views, or consumers.

pub mod digest;
pub mod envelope;
pub mod error;
pub mod refs;

pub use digest::{Digest, DigestKey};
pub use envelope::{Envelope, ObjectKind};
pub use error::CoreError;
pub use refs::{Lease, RefValue};
