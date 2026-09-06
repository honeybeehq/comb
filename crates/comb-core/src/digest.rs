use crate::error::{CoreError, Result};
use serde::{Deserialize, Serialize};
use std::fmt;

/// A tenant digest key (spec §7.4). Object identity is BLAKE3-256 in keyed
/// mode, so identical plaintext in two tenants yields unrelated digests and
/// no within-bucket existence oracle exists.
#[derive(Clone)]
pub struct DigestKey([u8; 32]);

impl DigestKey {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn from_hex(s: &str) -> Result<Self> {
        let raw =
            hex::decode(s).map_err(|e| CoreError::InvalidFormat(format!("digest key hex: {e}")))?;
        let arr: [u8; 32] = raw
            .try_into()
            .map_err(|_| CoreError::InvalidFormat("digest key must be 32 bytes".into()))?;
        Ok(Self(arr))
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Tenant-keyed digest of canonical plaintext (`b3k:<hex>`).
    pub fn digest(&self, plaintext: &[u8]) -> Digest {
        let hash = blake3::keyed_hash(&self.0, plaintext);
        Digest {
            hex: hex::encode(hash.as_bytes()),
        }
    }
}

impl fmt::Debug for DigestKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DigestKey(..)")
    }
}

/// A tagged tenant-keyed content digest. Only `b3k` exists in this slice;
/// the wire form is `b3k:<lowercase-hex>` (spec §7.2).
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Digest {
    hex: String,
}

impl Digest {
    pub const TAG: &'static str = "b3k";

    pub fn parse(s: &str) -> Result<Self> {
        let rest = s.strip_prefix("b3k:").ok_or_else(|| {
            CoreError::InvalidFormat(format!("digest must start with b3k: — got {s}"))
        })?;
        if rest.len() != 64
            || !rest
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(CoreError::InvalidFormat(format!(
                "malformed b3k digest: {s}"
            )));
        }
        Ok(Self {
            hex: rest.to_string(),
        })
    }

    pub fn hex(&self) -> &str {
        &self.hex
    }

    pub fn raw(&self) -> [u8; 32] {
        let mut out = [0u8; 32];
        hex::decode_to_slice(&self.hex, &mut out).expect("canonical digest hex");
        out
    }

    /// Two-character fan-out prefix used in object keys (spec §7.12).
    pub fn key_prefix(&self) -> &str {
        &self.hex[..2]
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", Self::TAG, self.hex)
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl TryFrom<String> for Digest {
    type Error = CoreError;
    fn try_from(s: String) -> Result<Self> {
        Digest::parse(&s)
    }
}

impl From<Digest> for String {
    fn from(d: Digest) -> String {
        d.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keyed_digests_differ_across_tenants() {
        let a = DigestKey::from_bytes([1u8; 32]);
        let b = DigestKey::from_bytes([2u8; 32]);
        assert_ne!(a.digest(b"hello"), b.digest(b"hello"));
        assert_eq!(a.digest(b"hello"), a.digest(b"hello"));
    }

    #[test]
    fn digest_roundtrip() {
        let d = DigestKey::from_bytes([0u8; 32]).digest(b"x");
        let parsed = Digest::parse(&d.to_string()).unwrap();
        assert_eq!(d, parsed);
        assert!(Digest::parse("sha256:abcd").is_err());
    }
}
