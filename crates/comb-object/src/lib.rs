//! Object backends for Comb (spec §7.8–§7.11).
//!
//! The trait is deliberately narrow: create-only puts, conditionally
//! replaced puts guarded by an opaque provider version token, and reads.
//! Listing is discovery-only and absent from this slice. Every backend must
//! pass the same conformance tests; an S3-compatible API string is not
//! evidence of equivalent semantics (§7.9).

pub mod backend;
pub mod conformance;
pub mod failpoint;
pub mod fault;
pub mod local;
pub mod memory;
pub mod s3;

pub use backend::{ObjectBackend, ObjectInfo, Version};
pub use failpoint::{CountingBackend, FailAction, FailMethod, FailRule, FailpointBackend};
