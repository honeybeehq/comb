//! Comb process bridge: JSONL v1 protocol, handler, and stdio transport.
//!
//! The binary includes this directory via `#[path]`. Shared Log/Core code
//! stays outside this module.

pub mod handler;
pub mod limits;
pub mod protocol;
pub mod stdio;
