//! Operation identity, clocks, and canonical material hashes (spec §18.1).

use crate::digest::{Digest, DigestKey};
use crate::error::{CoreError, Result};
use chrono::{DateTime, Duration, Utc};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::{Arc, Mutex};

const CROCKFORD: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const OP_PREFIX: &str = "op_";
const OP_BODY_LEN: usize = 39;
const STABLE_PREFIX: &str = "sk_";
pub const MAX_STABLE_KEY_BYTES: usize = 512;

/// Injected clock so expiry, first-use, and future-skew tests are deterministic.
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Clone)]
pub struct FrozenClock {
    now: Arc<Mutex<DateTime<Utc>>>,
}

impl FrozenClock {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            now: Arc::new(Mutex::new(now)),
        }
    }

    pub fn set(&self, now: DateTime<Utc>) {
        *self.now.lock().expect("clock lock") = now;
    }

    pub fn add(&self, delta: Duration) {
        let mut g = self.now.lock().expect("clock lock");
        *g += delta;
    }
}

impl Clock for FrozenClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().expect("clock lock")
    }
}

/// Configurable admission window for generic operation IDs.
#[derive(Debug, Clone)]
pub struct OperationPolicy {
    pub window: Duration,
    pub first_use: Duration,
    pub max_future_skew: Duration,
}

impl Default for OperationPolicy {
    fn default() -> Self {
        Self {
            window: Duration::days(7),
            first_use: Duration::minutes(5),
            max_future_skew: Duration::minutes(2),
        }
    }
}

/// Time-bearing 192-bit operation id: 48-bit millisecond timestamp + 144-bit entropy.
/// Canonical text is `op_` plus 39 Crockford-base32 characters.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct OperationId([u8; 24]);

impl OperationId {
    pub fn mint(clock: &dyn Clock) -> Self {
        let mut entropy = [0u8; 18];
        rand::rng().fill_bytes(&mut entropy);
        Self::from_millis_and_entropy(millis_of(clock.now()), entropy)
    }

    pub fn from_millis_and_entropy(ms: u64, entropy: [u8; 18]) -> Self {
        let ms = ms & 0x0000_ffff_ffff_ffff;
        let mut bytes = [0u8; 24];
        bytes[0] = ((ms >> 40) & 0xff) as u8;
        bytes[1] = ((ms >> 32) & 0xff) as u8;
        bytes[2] = ((ms >> 24) & 0xff) as u8;
        bytes[3] = ((ms >> 16) & 0xff) as u8;
        bytes[4] = ((ms >> 8) & 0xff) as u8;
        bytes[5] = (ms & 0xff) as u8;
        bytes[6..].copy_from_slice(&entropy);
        Self(bytes)
    }

    pub fn parse(s: &str) -> Result<Self> {
        let body = s.strip_prefix(OP_PREFIX).ok_or_else(|| {
            CoreError::InvalidFormat(format!("operation id must start with {OP_PREFIX}"))
        })?;
        if body.len() != OP_BODY_LEN {
            return Err(CoreError::InvalidFormat(format!(
                "operation id body must be {OP_BODY_LEN} Crockford characters"
            )));
        }
        let bytes = crockford_decode(body)?;
        let arr: [u8; 24] = bytes
            .try_into()
            .map_err(|_| CoreError::InvalidFormat("operation id decoded length".into()))?;
        Ok(Self(arr))
    }

    pub fn issued_at(&self) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp_millis(self.issued_at_millis() as i64)
            .unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
    }

    pub fn issued_at_millis(&self) -> u64 {
        ((self.0[0] as u64) << 40)
            | ((self.0[1] as u64) << 32)
            | ((self.0[2] as u64) << 24)
            | ((self.0[3] as u64) << 16)
            | ((self.0[4] as u64) << 8)
            | (self.0[5] as u64)
    }

    pub fn expires_at(&self, policy: &OperationPolicy) -> DateTime<Utc> {
        self.issued_at() + policy.window
    }

    /// Time gate for a generic id. `had_intent` is whether a durable record already exists.
    pub fn check_time(
        &self,
        now: DateTime<Utc>,
        policy: &OperationPolicy,
        had_intent: bool,
    ) -> Result<()> {
        let issued = self.issued_at();
        if issued > now + policy.max_future_skew {
            return Err(CoreError::InvalidFormat(format!(
                "operation id {self} is too far in the future"
            )));
        }
        let expired_at = self.expires_at(policy);
        if now >= expired_at {
            return Err(CoreError::UnknownOperation {
                id: self.to_string(),
                expired_at: expired_at.to_rfc3339(),
            });
        }
        if !had_intent && now > issued + policy.first_use {
            return Err(CoreError::UnknownOperation {
                id: self.to_string(),
                expired_at: expired_at.to_rfc3339(),
            });
        }
        Ok(())
    }
}

impl fmt::Display for OperationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{OP_PREFIX}{}", crockford_encode(&self.0))
    }
}

impl fmt::Debug for OperationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl TryFrom<String> for OperationId {
    type Error = CoreError;
    fn try_from(s: String) -> Result<Self> {
        Self::parse(&s)
    }
}

impl From<OperationId> for String {
    fn from(id: OperationId) -> String {
        id.to_string()
    }
}

impl Serialize for OperationId {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for OperationId {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).map_err(serde::de::Error::custom)
    }
}

/// Opaque stable append key. Comb does not parse caller structure.
/// Canonical text is `sk_` plus lowercase hex of 1..=MAX_STABLE_KEY_BYTES bytes.
#[derive(Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct StableKey(Box<[u8]>);

impl StableKey {
    pub fn try_from_canonical(bytes: impl Into<Box<[u8]>>) -> Result<Self> {
        let bytes = bytes.into();
        if bytes.is_empty() || bytes.len() > MAX_STABLE_KEY_BYTES {
            return Err(CoreError::Rejected(format!(
                "stable key must be 1..{MAX_STABLE_KEY_BYTES} bytes"
            )));
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(&self.0)
    }

    pub fn from_hex(s: &str) -> Result<Self> {
        // Bound encoded size before decoding so a library caller cannot force
        // a huge allocation. Host transports may already cap frames.
        if s.len() > MAX_STABLE_KEY_BYTES * 2 {
            return Err(CoreError::Rejected(format!(
                "stable key hex exceeds {} characters",
                MAX_STABLE_KEY_BYTES * 2
            )));
        }
        if !s.len().is_multiple_of(2) {
            return Err(CoreError::InvalidFormat(
                "stable key hex must have even length".into(),
            ));
        }
        let raw =
            hex::decode(s).map_err(|e| CoreError::InvalidFormat(format!("stable key hex: {e}")))?;
        Self::try_from_canonical(raw)
    }

    pub fn parse(s: &str) -> Result<Self> {
        let rest = s.strip_prefix(STABLE_PREFIX).ok_or_else(|| {
            CoreError::InvalidFormat(format!("stable key must start with {STABLE_PREFIX}"))
        })?;
        Self::from_hex(rest)
    }

    pub fn path_digest(&self, key: &DigestKey, tenant: &str, logical_log: &str) -> Digest {
        domain_hash(
            key,
            "comb.log.stable-key-path/v1",
            &[tenant.as_bytes(), logical_log.as_bytes(), self.as_bytes()],
        )
    }

    pub fn payload_hash(key: &DigestKey, payload: &[u8]) -> Digest {
        domain_hash(key, "comb.log.stable-payload/v1", &[payload])
    }

    pub fn request_hash(
        &self,
        key: &DigestKey,
        tenant: &str,
        logical_log: &str,
        payload_hash: &Digest,
    ) -> Digest {
        domain_hash(
            key,
            "comb.log.append-stable/v1",
            &[
                tenant.as_bytes(),
                logical_log.as_bytes(),
                self.as_bytes(),
                &payload_hash.raw(),
            ],
        )
    }
}

impl fmt::Display for StableKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{STABLE_PREFIX}{}", self.to_hex())
    }
}

impl fmt::Debug for StableKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl Serialize for StableKey {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for StableKey {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).map_err(serde::de::Error::custom)
    }
}

/// Tenant-local identity used as the intent key and commit attribution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OpIdentity {
    Generic(OperationId),
    Stable(StableKey),
}

impl OpIdentity {
    pub fn parse(s: &str) -> Result<Self> {
        if s.starts_with(OP_PREFIX) {
            Ok(Self::Generic(OperationId::parse(s)?))
        } else if s.starts_with(STABLE_PREFIX) {
            Ok(Self::Stable(StableKey::parse(s)?))
        } else {
            Err(CoreError::InvalidFormat(
                "identity must start with op_ or sk_".into(),
            ))
        }
    }

    pub fn canonical(&self) -> String {
        match self {
            Self::Generic(op) => op.to_string(),
            Self::Stable(k) => k.to_string(),
        }
    }

    pub fn is_stable(&self) -> bool {
        matches!(self, Self::Stable(_))
    }

    pub fn shard(&self) -> String {
        let d = blake3::hash(self.canonical().as_bytes());
        hex::encode(&d.as_bytes()[..1])
    }

    pub fn check_time(
        &self,
        now: DateTime<Utc>,
        policy: &OperationPolicy,
        had_intent: bool,
    ) -> Result<()> {
        match self {
            Self::Generic(op) => op.check_time(now, policy, had_intent),
            Self::Stable(_) => Ok(()),
        }
    }
}

impl fmt::Display for OpIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.canonical())
    }
}

/// Canonical request body for material hashing. Excludes provider tokens,
/// routes, writer instance, lease epoch, and allocated sequences.
#[derive(Debug, Clone)]
pub struct Material {
    pub kind: String,
    pub preconditions: Vec<(String, Vec<u8>)>,
    pub payload: Vec<Vec<u8>>,
}

impl Material {
    pub fn hash(&self, key: &DigestKey, tenant: &str, resource: &str) -> Digest {
        let mut buf = Vec::new();
        put_bytes(&mut buf, b"comb.material/v1");
        put_bytes(&mut buf, tenant.as_bytes());
        put_bytes(&mut buf, self.kind.as_bytes());
        put_bytes(&mut buf, resource.as_bytes());
        let mut pre = self.preconditions.clone();
        pre.sort_by(|a, b| a.0.cmp(&b.0));
        put_u64(&mut buf, pre.len() as u64);
        for (name, value) in &pre {
            put_bytes(&mut buf, name.as_bytes());
            put_bytes(&mut buf, value);
        }
        put_u64(&mut buf, self.payload.len() as u64);
        for p in &self.payload {
            put_bytes(&mut buf, p);
        }
        key.digest(&buf)
    }
}

/// Length-prefixed domain-separated hash. Each field is `u64be(len) || bytes`.
pub fn domain_hash(key: &DigestKey, domain: &str, fields: &[&[u8]]) -> Digest {
    let mut buf = Vec::new();
    put_bytes(&mut buf, domain.as_bytes());
    put_u64(&mut buf, fields.len() as u64);
    for f in fields {
        put_bytes(&mut buf, f);
    }
    key.digest(&buf)
}

fn put_u64(buf: &mut Vec<u8>, n: u64) {
    buf.extend_from_slice(&n.to_be_bytes());
}

fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    put_u64(buf, bytes.len() as u64);
    buf.extend_from_slice(bytes);
}

fn millis_of(now: DateTime<Utc>) -> u64 {
    u64::try_from(now.timestamp_millis()).unwrap_or(0)
}

fn crockford_encode(bytes: &[u8]) -> String {
    let mut bits = 0u32;
    let mut nbits = 0u32;
    let mut out = String::new();
    for &b in bytes {
        bits = (bits << 8) | b as u32;
        nbits += 8;
        while nbits >= 5 {
            nbits -= 5;
            let idx = ((bits >> nbits) & 0x1f) as usize;
            out.push(CROCKFORD[idx] as char);
        }
    }
    if nbits > 0 {
        let idx = ((bits << (5 - nbits)) & 0x1f) as usize;
        out.push(CROCKFORD[idx] as char);
    }
    out
}

fn crockford_decode(s: &str) -> Result<Vec<u8>> {
    let mut bits = 0u32;
    let mut nbits = 0u32;
    let mut out = Vec::new();
    for c in s.chars() {
        let v = crockford_value(c)?;
        bits = (bits << 5) | v as u32;
        nbits += 5;
        while nbits >= 8 {
            nbits -= 8;
            out.push(((bits >> nbits) & 0xff) as u8);
        }
    }
    Ok(out)
}

fn crockford_value(c: char) -> Result<u8> {
    let u = c.to_ascii_uppercase();
    let mapped = match u {
        'I' | 'L' => '1',
        'O' => '0',
        other => other,
    };
    CROCKFORD
        .iter()
        .position(|&b| b == mapped as u8)
        .map(|i| i as u8)
        .ok_or_else(|| CoreError::InvalidFormat(format!("invalid Crockford character {c}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_id_roundtrip_and_timestamp() {
        let ms = 1_704_000_000_000u64;
        let id = OperationId::from_millis_and_entropy(ms, [7u8; 18]);
        let s = id.to_string();
        assert!(s.starts_with("op_"));
        assert_eq!(s.len(), 3 + OP_BODY_LEN);
        let parsed = OperationId::parse(&s).unwrap();
        assert_eq!(parsed, id);
        assert_eq!(parsed.issued_at_millis(), ms);
        let lower = s.to_ascii_lowercase();
        assert_eq!(OperationId::parse(&lower).unwrap(), id);
    }

    #[test]
    fn retries_do_not_change_bytes() {
        let id = OperationId::from_millis_and_entropy(42, [1u8; 18]);
        assert_eq!(id.to_string(), id.to_string());
        assert_eq!(OperationId::parse(&id.to_string()).unwrap(), id);
    }

    #[test]
    fn future_skew_and_expiry() {
        let policy = OperationPolicy::default();
        let now = DateTime::<Utc>::from_timestamp_millis(1_800_000_000_000).unwrap();
        let clock = FrozenClock::new(now);
        let id = OperationId::mint(&clock);
        id.check_time(now, &policy, false).unwrap();
        let far =
            OperationId::from_millis_and_entropy(millis_of(now + Duration::hours(1)), [2u8; 18]);
        assert!(matches!(
            far.check_time(now, &policy, false),
            Err(CoreError::InvalidFormat(_))
        ));
        assert!(matches!(
            id.check_time(now + Duration::days(8), &policy, true),
            Err(CoreError::UnknownOperation { .. })
        ));
        assert!(matches!(
            id.check_time(now + Duration::minutes(6), &policy, false),
            Err(CoreError::UnknownOperation { .. })
        ));
        id.check_time(now + Duration::minutes(6), &policy, true)
            .unwrap();
    }

    #[test]
    fn stable_key_is_opaque_and_bounded() {
        let k = StableKey::try_from_canonical(b"doc\x00hash".to_vec()).unwrap();
        let parsed = StableKey::parse(&k.to_string()).unwrap();
        assert_eq!(parsed, k);
        assert!(StableKey::try_from_canonical(vec![0u8; MAX_STABLE_KEY_BYTES + 1]).is_err());
        assert!(StableKey::parse("op_abc").is_err());
        assert!(StableKey::try_from_canonical(Vec::new()).is_err());
        let huge = "aa".repeat(MAX_STABLE_KEY_BYTES + 1);
        assert!(matches!(
            StableKey::from_hex(&huge),
            Err(CoreError::Rejected(_))
        ));
        assert!(StableKey::from_hex("abc").is_err());
    }

    #[test]
    fn material_hash_excludes_nothing_passed_in() {
        let key = DigestKey::from_bytes([3u8; 32]);
        let a = Material {
            kind: "append".into(),
            preconditions: vec![("target".into(), b"t".to_vec())],
            payload: vec![b"one".to_vec(), b"two".to_vec()],
        };
        let b = Material {
            kind: "append".into(),
            preconditions: vec![("target".into(), b"t".to_vec())],
            payload: vec![b"one".to_vec(), b"two".to_vec()],
        };
        assert_eq!(
            a.hash(&key, "org", "log/demo/p0"),
            b.hash(&key, "org", "log/demo/p0")
        );
        let c = Material {
            payload: vec![b"one".to_vec(), b"TWO".to_vec()],
            ..a.clone()
        };
        assert_ne!(
            a.hash(&key, "org", "log/demo/p0"),
            c.hash(&key, "org", "log/demo/p0")
        );
        assert_ne!(
            a.hash(&key, "org", "log/demo/p0"),
            a.hash(&key, "org", "log/other/p0")
        );
    }
}
