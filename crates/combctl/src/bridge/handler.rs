//! Request dispatch onto the current shared Log.
//!
//! Durable idempotency and bounded-memory reads are not implemented here.
//! Those belong in Log/Core. This adapter returns `unsupported` until the
//! shared signatures exist. It does not keep a process-local alias map,
//! does not renew leases (shared Log owns fencing), and does not call
//! today's unbounded `LogStore::read`.

use super::limits::Limits;
use super::protocol::{seq_string, Capabilities, OkBody, Request, Response, PROTOCOL_VERSION};
use comb_core::error::CoreError;
use combctl::log::LogStore;
use combctl::store::Store;

pub struct Bridge {
    store: Store,
    #[allow(dead_code)]
    writer: String,
    #[allow(dead_code)]
    lease_secs: i64,
    pub limits: Limits,
}

pub fn mint_writer() -> String {
    let mut bytes = [0u8; 16];
    rand::Rng::fill(&mut rand::rng(), &mut bytes);
    format!("comb-bridge-{}", hex::encode(bytes))
}

impl Bridge {
    pub fn new(store: Store, writer: String, lease_secs: i64, limits: Limits) -> Self {
        Self {
            store,
            writer,
            lease_secs,
            limits,
        }
    }

    pub async fn handle_frame(&self, bytes: &[u8]) -> Response {
        match super::protocol::parse_request(bytes, &self.limits) {
            Ok(req) => self.handle_request(req).await,
            Err(resp) => resp,
        }
    }

    pub async fn handle_request(&self, req: Request) -> Response {
        match req {
            Request::Hello { id, require } => self.hello(id, require),
            Request::Append { id, .. } => Response::unsupported(
                id,
                "durable_idempotency",
                "shared Log has no stable append key yet; refusing rather than storing a process-local alias",
            ),
            Request::Head { id, log } => self.head(id, log).await,
            Request::Read { id, .. } => Response::unsupported(
                id,
                "bounded_memory_read",
                "shared Log has no bounded read yet; refusing rather than loading an unbounded tail",
            ),
            Request::Follow { id, .. } => Response::unsupported(
                id,
                "bounded_memory_read",
                "shared Log has no bounded read yet; refusing rather than loading an unbounded tail",
            ),
        }
    }

    fn hello(&self, id: String, require: Vec<String>) -> Response {
        let caps = Capabilities::offered();
        for name in require {
            match caps.supports(&name) {
                Some(true) => {}
                Some(false) => {
                    return Response::unsupported(
                        id,
                        &name,
                        format!("capability {name} is not available on this process"),
                    );
                }
                None => {
                    return Response::unsupported(id, &name, format!("unknown capability {name}"));
                }
            }
        }
        Response::ok(
            id,
            "hello",
            OkBody::Hello {
                protocol: PROTOCOL_VERSION,
                capabilities: caps,
                limits: self.limits.view(),
            },
        )
    }

    async fn head(&self, id: String, log: String) -> Response {
        let store = LogStore::new(&self.store, &log);
        match store.status().await {
            Ok(None) => Response::ok(
                id,
                "head",
                OkBody::Head {
                    log,
                    head: seq_string(0),
                    cursor: seq_string(1),
                    trim_before: seq_string(0),
                },
            ),
            Ok(Some((_, manifest))) => Response::ok(
                id,
                "head",
                OkBody::Head {
                    log,
                    head: seq_string(manifest.head_seq),
                    cursor: seq_string(manifest.head_seq + 1),
                    trim_before: seq_string(manifest.trim_before_seq),
                },
            ),
            Err(e) => map_log_error(id, e),
        }
    }
}

fn map_log_error(id: impl Into<String>, err: anyhow::Error) -> Response {
    let id = id.into();
    if let Some(core) = err.downcast_ref::<CoreError>() {
        return match core {
            CoreError::Trimmed { resume_at } => Response::err(
                id,
                super::protocol::ErrorBody {
                    code: super::protocol::ErrorCode::Trimmed,
                    message: format!(
                        "position is below the retention floor; resume at {resume_at}"
                    ),
                    resume_at: Some(seq_string(*resume_at)),
                    capability: None,
                    seq: None,
                    event_bytes: None,
                    max_bytes: None,
                },
            ),
            CoreError::BackendUnavailable(m) => Response::err(
                id,
                super::protocol::ErrorBody {
                    code: super::protocol::ErrorCode::BackendUnavailable,
                    message: m.clone(),
                    resume_at: None,
                    capability: None,
                    seq: None,
                    event_bytes: None,
                    max_bytes: None,
                },
            ),
            CoreError::Fenced { caller, live } => Response::err(
                id,
                super::protocol::ErrorBody {
                    code: super::protocol::ErrorCode::Fenced,
                    message: format!("writer epoch {caller} is stale (live epoch {live})"),
                    resume_at: None,
                    capability: None,
                    seq: None,
                    event_bytes: None,
                    max_bytes: None,
                },
            ),
            CoreError::LeaseHeld { holder, until } => Response::err(
                id,
                super::protocol::ErrorBody {
                    code: super::protocol::ErrorCode::Fenced,
                    message: format!("lease held by {holder} until {until}"),
                    resume_at: None,
                    capability: None,
                    seq: None,
                    event_bytes: None,
                    max_bytes: None,
                },
            ),
            other => Response::err(
                id,
                super::protocol::ErrorBody {
                    code: super::protocol::ErrorCode::BackendUnavailable,
                    message: other.to_string(),
                    resume_at: None,
                    capability: None,
                    seq: None,
                    event_bytes: None,
                    max_bytes: None,
                },
            ),
        };
    }
    Response::err(
        id,
        super::protocol::ErrorBody {
            code: super::protocol::ErrorCode::BackendUnavailable,
            message: format!("{err:#}"),
            resume_at: None,
            capability: None,
            seq: None,
            event_bytes: None,
            max_bytes: None,
        },
    )
}
