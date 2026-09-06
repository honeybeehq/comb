use thiserror::Error;

/// Portable error classes (spec §18.3). Backends and higher layers map into
/// these; nothing above the backend matches on provider-specific errors.
#[derive(Debug, Error)]
pub enum CoreError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("already exists: {0}")]
    AlreadyExists(String),

    #[error("precondition failed: {0}")]
    PreconditionFailed(String),

    #[error("fenced: writer epoch {caller} is stale (live epoch {live})")]
    Fenced { caller: u64, live: u64 },

    #[error("lease held by {holder} until {until}")]
    LeaseHeld { holder: String, until: String },

    #[error("backend unavailable: {0}")]
    BackendUnavailable(String),

    #[error("integrity error: {0}")]
    IntegrityError(String),

    #[error("trimmed: position is below the retention floor; resume at {resume_at}")]
    Trimmed { resume_at: u64 },

    #[error("invalid format: {0}")]
    InvalidFormat(String),

    #[error("unknown operation {id} (expired at {expired_at})")]
    UnknownOperation { id: String, expired_at: String },

    #[error("idempotency conflict for {id}")]
    IdempotencyConflict {
        id: String,
        original: String,
        supplied: String,
    },

    #[error("recovery evidence missing or malformed: {0}")]
    RecoveryFailed(String),

    #[error("rejected: {0}")]
    Rejected(String),

    #[error("stable key committed for different payload bytes (existing {existing}, supplied {supplied})")]
    StableKeyConflict { existing: String, supplied: String },

    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, CoreError>;
