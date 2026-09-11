//! JSONL v1 request/response types and boundary validation.
//!
//! Sequence numbers and cursors are decimal strings on the wire so a
//! TypeScript client cannot lose u64 precision.

use super::limits::{Limits, LimitsView};
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone)]
#[allow(dead_code)] // normalized payload hex is retained alongside its decoded bytes
pub enum Request {
    Hello {
        id: String,
        require: Vec<String>,
    },
    Append {
        id: String,
        log: String,
        idempotency_key: String,
        payload_hex: String,
        payload: Vec<u8>,
    },
    Head {
        id: String,
        log: String,
    },
    Read {
        id: String,
        log: String,
        cursor: u64,
        max_events: Option<u64>,
        max_bytes: Option<u64>,
    },
    Follow {
        id: String,
        log: String,
        cursor: u64,
        timeout_ms: Option<u64>,
        max_events: Option<u64>,
        max_bytes: Option<u64>,
    },
}

impl Request {
    pub fn id(&self) -> &str {
        match self {
            Request::Hello { id, .. }
            | Request::Append { id, .. }
            | Request::Head { id, .. }
            | Request::Read { id, .. }
            | Request::Follow { id, .. } => id,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Capabilities {
    pub ops: Vec<&'static str>,
    pub durable_idempotency: bool,
    pub bounded_memory_read: bool,
    pub payload_hex: bool,
}

impl Capabilities {
    pub fn offered() -> Self {
        Self {
            ops: vec!["hello", "append", "head", "read", "follow"],
            // Implemented by checked V3 CompleteFeed and WriterSession.
            durable_idempotency: true,
            bounded_memory_read: true,
            payload_hex: true,
        }
    }

    pub fn supports(&self, name: &str) -> Option<bool> {
        match name {
            "durable_idempotency" => Some(self.durable_idempotency),
            "bounded_memory_read" => Some(self.bounded_memory_read),
            "payload_hex" => Some(self.payload_hex),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct WireEvent {
    pub seq: String,
    pub at: String,
    pub payload_hex: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum Response {
    Ok {
        v: u32,
        id: String,
        ok: bool,
        op: String,
        #[serde(flatten)]
        body: OkBody,
    },
    Err {
        v: u32,
        id: String,
        ok: bool,
        error: ErrorBody,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub enum OkBody {
    Hello {
        protocol: u32,
        capabilities: Capabilities,
        limits: LimitsView,
    },
    Append {
        log: String,
        first: String,
        last: String,
        cursor: String,
    },
    Head {
        log: String,
        head: String,
        cursor: String,
        trim_before: String,
    },
    Page {
        log: String,
        events: Vec<WireEvent>,
        next_cursor: String,
        at_head: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        timed_out: Option<bool>,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct ErrorBody {
    pub code: ErrorCode,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resume_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capability: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_bytes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)] // conflict/unknown_operation/event_too_large stay on the wire
pub enum ErrorCode {
    InvalidRequest,
    Unsupported,
    Trimmed,
    BackendUnavailable,
    Integrity,
    DeadlineExceeded,
    Cancelled,
    LeaseHeld,
    ReacquireRequired,
    Fenced,
    Conflict,
    UnknownOperation,
    EventTooLarge,
    Busy,
}

impl Response {
    pub fn ok(id: impl Into<String>, op: &str, body: OkBody) -> Self {
        Response::Ok {
            v: PROTOCOL_VERSION,
            id: id.into(),
            ok: true,
            op: op.to_string(),
            body,
        }
    }

    pub fn err(id: impl Into<String>, error: ErrorBody) -> Self {
        Response::Err {
            v: PROTOCOL_VERSION,
            id: id.into(),
            ok: false,
            error,
        }
    }

    pub fn invalid(id: impl Into<String>, message: impl Into<String>) -> Self {
        Self::err(
            id,
            ErrorBody {
                code: ErrorCode::InvalidRequest,
                message: message.into(),
                resume_at: None,
                capability: None,
                seq: None,
                event_bytes: None,
                max_bytes: None,
            },
        )
    }

    pub fn unsupported(
        id: impl Into<String>,
        capability: &str,
        message: impl Into<String>,
    ) -> Self {
        Self::err(
            id,
            ErrorBody {
                code: ErrorCode::Unsupported,
                message: message.into(),
                resume_at: None,
                capability: Some(capability.to_string()),
                seq: None,
                event_bytes: None,
                max_bytes: None,
            },
        )
    }

    pub fn busy(id: impl Into<String>) -> Self {
        Self::err(
            id,
            ErrorBody {
                code: ErrorCode::Busy,
                message: "too many concurrent requests".into(),
                resume_at: None,
                capability: None,
                seq: None,
                event_bytes: None,
                max_bytes: None,
            },
        )
    }

    pub fn to_jsonl(&self) -> String {
        serde_json::to_string(self).expect("response is always serializable")
    }
}

pub fn parse_request(bytes: &[u8], limits: &Limits) -> Result<Request, Response> {
    let text =
        std::str::from_utf8(bytes).map_err(|_| Response::invalid("", "request is not utf-8"))?;
    let text = text.trim_end_matches(['\n', '\r']);
    if text.is_empty() {
        return Err(Response::invalid("", "empty request"));
    }
    let value = parse_json_object(text).map_err(|m| Response::invalid(extract_id(bytes), m))?;
    let id = match value.get("id") {
        Some(Value::String(s)) => s.clone(),
        Some(_) => return Err(Response::invalid("", "id must be a string")),
        None => return Err(Response::invalid("", "missing id")),
    };
    if let Err(m) = validate_id(&id, limits.max_id_len) {
        return Err(Response::invalid("", m));
    }
    let v = match value.get("v") {
        Some(Value::Number(n)) => n
            .as_u64()
            .ok_or_else(|| Response::invalid(&id, "v must be a protocol version integer"))?,
        Some(_) => {
            return Err(Response::invalid(
                &id,
                "v must be a protocol version integer",
            ))
        }
        None => return Err(Response::invalid(&id, "missing v")),
    };
    if v != PROTOCOL_VERSION as u64 {
        return Err(Response::unsupported(
            &id,
            "protocol",
            format!("unsupported protocol version {v}; this process speaks v{PROTOCOL_VERSION}"),
        ));
    }
    let op = match value.get("op") {
        Some(Value::String(s)) => s.as_str(),
        Some(_) => return Err(Response::invalid(&id, "op must be a string")),
        None => return Err(Response::invalid(&id, "missing op")),
    };
    match op {
        "hello" => parse_hello(id, &value),
        "append" => parse_append(id, &value, limits),
        "head" => parse_head(id, &value, limits),
        "read" => parse_read(id, &value, limits),
        "follow" => parse_follow(id, &value, limits),
        other => Err(Response::invalid(id, format!("unknown op {other}"))),
    }
}

fn parse_json_object(text: &str) -> Result<Value, String> {
    let mut de = serde_json::Deserializer::from_str(text);
    let value = Value::deserialize(&mut de).map_err(|e| format!("malformed json: {e}"))?;
    de.end()
        .map_err(|_| "trailing data after json object".to_string())?;
    if !value.is_object() {
        return Err("request must be a json object".into());
    }
    Ok(value)
}

fn parse_hello(id: String, value: &Value) -> Result<Request, Response> {
    let require = match value.get("require") {
        None => Vec::new(),
        Some(Value::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::String(s) => out.push(s.clone()),
                    _ => {
                        return Err(Response::invalid(
                            &id,
                            "require must be an array of strings",
                        ))
                    }
                }
            }
            out
        }
        Some(_) => {
            return Err(Response::invalid(
                &id,
                "require must be an array of strings",
            ))
        }
    };
    Ok(Request::Hello { id, require })
}

fn parse_append(id: String, value: &Value, limits: &Limits) -> Result<Request, Response> {
    if value.get("events").is_some() {
        return Err(Response::invalid(
            &id,
            "append events arrays are not supported; send one payload_hex",
        ));
    }
    let log = parse_log(id.as_str(), value, limits)?;
    let idempotency_key = match value.get("idempotency_key") {
        Some(Value::String(s)) => validate_idempotency_key(s, limits.max_idempotency_key_len)
            .map_err(|m| Response::invalid(&id, m))?,
        Some(_) => return Err(Response::invalid(&id, "idempotency_key must be a string")),
        None => return Err(Response::invalid(&id, "missing idempotency_key")),
    };
    let payload_hex = match value.get("payload_hex") {
        Some(Value::String(s)) => s.as_str(),
        Some(_) => return Err(Response::invalid(&id, "payload_hex must be a string")),
        None => return Err(Response::invalid(&id, "missing payload_hex")),
    };
    let (payload_hex, payload) =
        parse_payload_hex(payload_hex).map_err(|m| Response::invalid(&id, m))?;
    if payload.len() > limits.max_append_bytes {
        return Err(Response::invalid(
            &id,
            format!(
                "append payload bytes {} exceeds limit {}",
                payload.len(),
                limits.max_append_bytes
            ),
        ));
    }
    Ok(Request::Append {
        id,
        log,
        idempotency_key,
        payload_hex,
        payload,
    })
}

fn parse_head(id: String, value: &Value, limits: &Limits) -> Result<Request, Response> {
    let log = parse_log(id.as_str(), value, limits)?;
    Ok(Request::Head { id, log })
}

fn parse_read(id: String, value: &Value, limits: &Limits) -> Result<Request, Response> {
    let log = parse_log(id.as_str(), value, limits)?;
    let cursor = parse_cursor_field(&id, value)?;
    let max_events = parse_optional_u64(&id, value, "max_events")?;
    let max_bytes = parse_optional_u64(&id, value, "max_bytes")?;
    reject_over_limit(&id, "max_events", max_events, limits.max_read_events as u64)?;
    reject_over_limit(&id, "max_bytes", max_bytes, limits.max_read_bytes as u64)?;
    Ok(Request::Read {
        id,
        log,
        cursor,
        max_events,
        max_bytes,
    })
}

fn parse_follow(id: String, value: &Value, limits: &Limits) -> Result<Request, Response> {
    let log = parse_log(id.as_str(), value, limits)?;
    let cursor = parse_cursor_field(&id, value)?;
    let timeout_ms = parse_optional_u64(&id, value, "timeout_ms")?;
    let max_events = parse_optional_u64(&id, value, "max_events")?;
    let max_bytes = parse_optional_u64(&id, value, "max_bytes")?;
    if timeout_ms.is_some_and(|timeout| timeout > limits.max_follow_timeout_ms) {
        return Err(Response::invalid(&id, "timeout_ms exceeds limit"));
    }
    reject_over_limit(&id, "max_events", max_events, limits.max_read_events as u64)?;
    reject_over_limit(&id, "max_bytes", max_bytes, limits.max_read_bytes as u64)?;
    Ok(Request::Follow {
        id,
        log,
        cursor,
        timeout_ms,
        max_events,
        max_bytes,
    })
}

fn parse_log(id: &str, value: &Value, limits: &Limits) -> Result<String, Response> {
    let name = match value.get("log") {
        Some(Value::String(s)) => s,
        Some(_) => return Err(Response::invalid(id, "log must be a string")),
        None => return Err(Response::invalid(id, "missing log")),
    };
    validate_log_name(name, limits.max_log_name_len).map_err(|m| Response::invalid(id, m))?;
    Ok(name.clone())
}

fn parse_cursor_field(id: &str, value: &Value) -> Result<u64, Response> {
    match value.get("cursor") {
        Some(Value::String(s)) => parse_cursor(s).map_err(|m| Response::invalid(id, m)),
        Some(Value::Number(_)) => Err(Response::invalid(id, "cursor must be a decimal string")),
        Some(_) => Err(Response::invalid(id, "cursor must be a decimal string")),
        None => Err(Response::invalid(id, "missing cursor")),
    }
}

fn parse_optional_u64(id: &str, value: &Value, field: &str) -> Result<Option<u64>, Response> {
    match value.get(field) {
        None => Ok(None),
        Some(Value::Number(n)) => n.as_u64().map(Some).ok_or_else(|| {
            Response::invalid(id, format!("{field} must be a non-negative integer"))
        }),
        Some(_) => Err(Response::invalid(
            id,
            format!("{field} must be a non-negative integer"),
        )),
    }
}

fn reject_over_limit(id: &str, field: &str, value: Option<u64>, max: u64) -> Result<(), Response> {
    match value {
        Some(0) => Err(Response::invalid(
            id,
            format!("{field} must be greater than zero"),
        )),
        Some(v) if v > max => Err(Response::invalid(
            id,
            format!("{field} {v} exceeds limit {max}"),
        )),
        _ => Ok(()),
    }
}

pub fn parse_decimal_u64(s: &str) -> Result<u64, String> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return Err("must be a decimal string".into());
    }
    if s.len() > 1 && s.starts_with('0') {
        return Err("must not have leading zeros".into());
    }
    s.parse::<u64>().map_err(|_| "exceeds u64".into())
}

pub fn parse_cursor(s: &str) -> Result<u64, String> {
    let n = parse_decimal_u64(s).map_err(|m| format!("cursor {m}"))?;
    if n == 0 {
        return Err("cursor is the next sequence and must be at least 1".into());
    }
    Ok(n)
}

pub fn seq_string(n: u64) -> String {
    n.to_string()
}

pub fn parse_payload_hex(s: &str) -> Result<(String, Vec<u8>), String> {
    if s.is_empty() {
        return Err("payload_hex is empty".into());
    }
    if s.len() % 2 != 0 {
        return Err("payload_hex must have even length".into());
    }
    if !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("payload_hex must be hexadecimal".into());
    }
    let payload = hex::decode(s).map_err(|e| format!("payload_hex: {e}"))?;
    if payload.is_empty() {
        return Err("payload_hex is empty".into());
    }
    Ok((hex::encode(&payload), payload))
}

/// Best-effort id from a complete or truncated JSON object. Used so oversized
/// frames can still correlate an error to the caller.
pub fn extract_id(bytes: &[u8]) -> String {
    if let Ok(Value::Object(map)) = serde_json::from_slice::<Value>(bytes) {
        if let Some(Value::String(s)) = map.get("id") {
            return s.clone();
        }
    }
    let Ok(text) = std::str::from_utf8(bytes) else {
        return String::new();
    };
    let Some(rest) = text.split("\"id\"").nth(1) else {
        return String::new();
    };
    let rest = rest
        .trim_start()
        .strip_prefix(':')
        .unwrap_or("")
        .trim_start();
    let Some(rest) = rest.strip_prefix('"') else {
        return String::new();
    };
    let end = rest.find('"').unwrap_or(0);
    rest[..end].to_string()
}

pub fn validate_id(id: &str, max_len: usize) -> Result<(), String> {
    if id.is_empty() {
        return Err("id is empty".into());
    }
    if id.len() > max_len {
        return Err("id exceeds limit".into());
    }
    if !id.chars().all(|c| c.is_ascii_graphic()) {
        return Err("id must be printable ascii without spaces".into());
    }
    Ok(())
}

/// Raw size of an opaque idempotency key. Wire form is lowercase hex, at most
/// twice this many characters.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 512;

pub fn validate_idempotency_key(key: &str, max_hex_len: usize) -> Result<String, String> {
    if key.is_empty() {
        return Err("idempotency_key is empty".into());
    }
    if key.len() % 2 != 0 {
        return Err("idempotency_key must have even hex length".into());
    }
    if key.len() > max_hex_len {
        return Err("idempotency_key exceeds limit".into());
    }
    if !key.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("idempotency_key must be hexadecimal".into());
    }
    let raw = hex::decode(key).map_err(|e| format!("idempotency_key: {e}"))?;
    if raw.is_empty() {
        return Err("idempotency_key is empty".into());
    }
    if raw.len() > MAX_IDEMPOTENCY_KEY_BYTES {
        return Err(format!(
            "idempotency_key exceeds {MAX_IDEMPOTENCY_KEY_BYTES} bytes"
        ));
    }
    Ok(hex::encode(raw))
}

pub fn validate_log_name(name: &str, max_len: usize) -> Result<(), String> {
    if name.is_empty() {
        return Err("log name is empty".into());
    }
    if name.len() > max_len {
        return Err("log name exceeds limit".into());
    }
    if name.contains("..") || name.contains('/') || name.contains('\\') || name.contains('\0') {
        return Err("log name must not contain path separators or traversal".into());
    }
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err("log name is empty".into());
    };
    if !first.is_ascii_alphanumeric() {
        return Err("log name must start with an alphanumeric character".into());
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
    {
        return Err("log name may contain only alphanumeric characters, '.', '_' and '-'".into());
    }
    Ok(())
}
