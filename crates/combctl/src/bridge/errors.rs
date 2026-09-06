//! Typed Log errors become machine codes; messages are never parsed.
use super::protocol::{seq_string, ErrorBody, ErrorCode, Response};
use combctl::log::{LeaseError, OpenLogError, ReadError, SessionLoss, StableAppendError};

pub fn error(id: impl Into<String>, code: ErrorCode, message: impl Into<String>) -> Response {
    Response::err(
        id,
        ErrorBody {
            code,
            message: message.into(),
            resume_at: None,
            capability: None,
            seq: None,
            event_bytes: None,
            max_bytes: None,
        },
    )
}

pub fn open_error(id: String, err: OpenLogError) -> Response {
    match err {
        OpenLogError::UnsupportedManifestSchema { .. } => Response::unsupported(
            id,
            "v3_complete_feed",
            "existing log is not a V3 complete feed; migration is not supported",
        ),
        OpenLogError::Integrity(_) => error(
            id,
            ErrorCode::Integrity,
            "log publication integrity check failed",
        ),
        OpenLogError::Unavailable => {
            error(id, ErrorCode::BackendUnavailable, "log backend unavailable")
        }
        OpenLogError::DeadlineExceeded => error(
            id,
            ErrorCode::DeadlineExceeded,
            "log open deadline exceeded",
        ),
        OpenLogError::Cancelled => error(id, ErrorCode::Cancelled, "log open cancelled"),
    }
}

pub fn read_error(id: String, err: ReadError) -> Response {
    match err {
        ReadError::Trimmed { resume_at, .. } => Response::err(
            id,
            ErrorBody {
                code: ErrorCode::Trimmed,
                message: "cursor is below the retention floor".into(),
                resume_at: Some(seq_string(resume_at.next_seq)),
                capability: None,
                seq: None,
                event_bytes: None,
                max_bytes: None,
            },
        ),
        ReadError::EventTooLarge {
            position,
            event_bytes,
            max_bytes,
            ..
        } => Response::err(
            id,
            ErrorBody {
                code: ErrorCode::EventTooLarge,
                message: "first event exceeds byte limit; cursor not advanced".into(),
                seq: Some(seq_string(position.seq)),
                event_bytes: Some(seq_string(event_bytes)),
                max_bytes: Some(seq_string(max_bytes)),
                resume_at: None,
                capability: None,
            },
        ),
        ReadError::InvalidCursor { .. } => error(
            id,
            ErrorCode::InvalidRequest,
            "cursor is outside the complete feed",
        ),
        ReadError::InvalidLimit(_) => error(id, ErrorCode::InvalidRequest, "invalid page limits"),
        ReadError::Integrity(_) => error(
            id,
            ErrorCode::Integrity,
            "log publication integrity check failed",
        ),
        ReadError::Unavailable { .. } => {
            error(id, ErrorCode::BackendUnavailable, "log backend unavailable")
        }
        ReadError::DeadlineExceeded => {
            error(id, ErrorCode::DeadlineExceeded, "read deadline exceeded")
        }
        ReadError::Cancelled => error(id, ErrorCode::Cancelled, "read cancelled"),
    }
}

pub fn lease_error(id: impl Into<String>, err: LeaseError) -> Response {
    let id = id.into();
    match err {
        LeaseError::LeaseHeld { .. } => {
            error(id, ErrorCode::LeaseHeld, "another publisher owns the lease")
        }
        LeaseError::Fenced { .. } => error(id, ErrorCode::Fenced, "publisher session was fenced"),
        LeaseError::ReacquireRequired { cause } => error(
            id,
            ErrorCode::ReacquireRequired,
            match cause {
                SessionLoss::Fenced { .. } => "session lost: fenced; start a new publisher session",
                SessionLoss::OwnerChanged => {
                    "session lost: owner changed; start a new publisher session"
                }
                SessionLoss::RenewalUncertain => {
                    "session lost: renewal uncertain; start a new publisher session"
                }
                SessionLoss::LeaseExpired => {
                    "session lost: lease expired; start a new publisher session"
                }
            },
        ),
        LeaseError::Unavailable => error(
            id,
            ErrorCode::BackendUnavailable,
            "lease backend unavailable",
        ),
        LeaseError::DeadlineExceeded => {
            error(id, ErrorCode::DeadlineExceeded, "lease deadline exceeded")
        }
        LeaseError::Cancelled => error(id, ErrorCode::Cancelled, "lease operation cancelled"),
        LeaseError::InvalidPolicy(_) => error(
            id,
            ErrorCode::InvalidRequest,
            "invalid publisher lease policy",
        ),
    }
}

pub fn append_error(id: String, err: StableAppendError) -> Response {
    match err {
        StableAppendError::StableKeyConflict { .. } => error(
            id,
            ErrorCode::Conflict,
            "stable key already committed with different bytes",
        ),
        StableAppendError::Lease(err) => lease_error(id, err),
        StableAppendError::Integrity(_) => error(
            id,
            ErrorCode::Integrity,
            "log publication integrity check failed",
        ),
        StableAppendError::Unavailable => error(
            id,
            ErrorCode::BackendUnavailable,
            "append outcome may be committed; retry the same key and bytes",
        ),
        StableAppendError::DeadlineExceeded => error(
            id,
            ErrorCode::DeadlineExceeded,
            "append deadline exceeded; retry the same key and bytes",
        ),
        StableAppendError::Cancelled => error(
            id,
            ErrorCode::Cancelled,
            "append cancelled; retry the same key and bytes",
        ),
        StableAppendError::InvalidInput => {
            error(id, ErrorCode::InvalidRequest, "invalid stable append input")
        }
    }
}
