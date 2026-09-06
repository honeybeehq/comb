/// Hard limits advertised on hello and enforced on every request.
///
/// `max_frame_bytes` is encoded JSONL line size (request and response).
/// `max_append_bytes` and `max_read_bytes` are decoded payload sizes.
#[derive(Debug, Clone)]
pub struct Limits {
    pub max_frame_bytes: usize,
    pub max_concurrent_requests: usize,
    pub max_queued_output: usize,
    pub max_append_events: usize,
    pub max_append_bytes: usize,
    pub max_read_events: usize,
    pub max_read_bytes: usize,
    pub max_follow_timeout_ms: u64,
    pub max_log_name_len: usize,
    pub max_id_len: usize,
    pub max_idempotency_key_len: usize,
    #[allow(dead_code)]
    pub poll_interval_ms: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 1_048_576,
            max_concurrent_requests: 32,
            max_queued_output: 64,
            max_append_events: 1,
            max_append_bytes: 1_048_576,
            max_read_events: 256,
            max_read_bytes: 1_048_576,
            max_follow_timeout_ms: 30_000,
            max_log_name_len: 128,
            max_id_len: 128,
            max_idempotency_key_len: 1024,
            poll_interval_ms: 250,
        }
    }
}

impl Limits {
    pub fn view(&self) -> LimitsView {
        LimitsView {
            max_frame_bytes: self.max_frame_bytes as u64,
            max_concurrent_requests: self.max_concurrent_requests as u64,
            max_queued_output: self.max_queued_output as u64,
            max_append_events: self.max_append_events as u64,
            max_append_bytes: self.max_append_bytes as u64,
            max_read_events: self.max_read_events as u64,
            max_read_bytes: self.max_read_bytes as u64,
            max_follow_timeout_ms: self.max_follow_timeout_ms,
            max_log_name_len: self.max_log_name_len as u64,
            max_id_len: self.max_id_len as u64,
            max_idempotency_key_len: self.max_idempotency_key_len as u64,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct LimitsView {
    pub max_frame_bytes: u64,
    pub max_concurrent_requests: u64,
    pub max_queued_output: u64,
    pub max_append_events: u64,
    pub max_append_bytes: u64,
    pub max_read_events: u64,
    pub max_read_bytes: u64,
    pub max_follow_timeout_ms: u64,
    pub max_log_name_len: u64,
    pub max_id_len: u64,
    pub max_idempotency_key_len: u64,
}
