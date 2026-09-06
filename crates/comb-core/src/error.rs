use thiserror::Error;

/// Envelope header or metadata field that failed a strict limited decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvelopeFormatField {
    Version,
    Flags,
    Compression,
    Encryption,
    Tenant,
    ObjectKind,
    Schema,
}

impl std::fmt::Display for EnvelopeFormatField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Version => write!(f, "version"),
            Self::Flags => write!(f, "flags"),
            Self::Compression => write!(f, "compression"),
            Self::Encryption => write!(f, "encryption"),
            Self::Tenant => write!(f, "tenant"),
            Self::ObjectKind => write!(f, "kind"),
            Self::Schema => write!(f, "schema"),
        }
    }
}

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

    #[error("lease expired")]
    LeaseExpired,

    #[error("backend unavailable: {0}")]
    BackendUnavailable(String),

    #[error("integrity error: {0}")]
    IntegrityError(String),

    #[error("trimmed: position is below the retention floor; resume at {resume_at}")]
    Trimmed { resume_at: u64 },

    #[error("invalid format: {0}")]
    InvalidFormat(String),

    #[error("object too large: {key} limit {limit} actual {actual:?}")]
    ObjectTooLarge {
        key: String,
        limit: u64,
        actual: Option<u64>,
    },

    #[error("unsupported envelope format: {field}={value}")]
    UnsupportedEnvelopeFormat {
        field: EnvelopeFormatField,
        value: String,
    },

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
