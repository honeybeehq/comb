use crate::digest::{Digest, DigestKey};
use crate::error::{CoreError, Result};
use serde::{Deserialize, Serialize};

/// Envelope binary layout (spec §7.3, decided in v0.3):
/// fixed header (magic "COMB", envelope version u16, flags u16, meta length
/// u32, all little-endian) + canonical JSON metadata + payload.
const MAGIC: &[u8; 4] = b"COMB";
const ENVELOPE_VERSION: u16 = 1;
const MAX_META_BYTES: u32 = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ObjectKind {
    Blob,
    Manifest,
    JournalEntry,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvelopeMeta {
    pub schema: String,
    pub tenant: String,
    pub kind: ObjectKind,
    pub digest: Digest,
    pub plaintext_bytes: u64,
    pub compression: String,
    pub encryption: String,
    pub created_at: String,
}

/// A decoded immutable object.
#[derive(Debug, Clone)]
pub struct Envelope {
    pub meta: EnvelopeMeta,
    pub payload: Vec<u8>,
}

impl Envelope {
    /// Build an envelope for plaintext. The digest is computed over the
    /// canonical plaintext with the tenant digest key (spec §7.2, §7.4).
    /// Compression and encryption are "none" in this slice; both are
    /// format-tagged so later versions can add them without migration.
    pub fn new(tenant: &str, kind: ObjectKind, schema: &str, payload: Vec<u8>, key: &DigestKey) -> Self {
        let digest = key.digest(&payload);
        Envelope {
            meta: EnvelopeMeta {
                schema: schema.to_string(),
                tenant: tenant.to_string(),
                kind,
                digest,
                plaintext_bytes: payload.len() as u64,
                compression: "none".into(),
                encryption: "none".into(),
                created_at: chrono::Utc::now().to_rfc3339(),
            },
            payload,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let meta = serde_json::to_vec(&self.meta)
            .map_err(|e| CoreError::InvalidFormat(format!("meta encode: {e}")))?;
        if meta.len() as u32 > MAX_META_BYTES {
            return Err(CoreError::InvalidFormat("metadata exceeds 64 KiB".into()));
        }
        let mut out = Vec::with_capacity(12 + meta.len() + self.payload.len());
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&ENVELOPE_VERSION.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes()); // flags
        out.extend_from_slice(&(meta.len() as u32).to_le_bytes());
        out.extend_from_slice(&meta);
        out.extend_from_slice(&self.payload);
        Ok(out)
    }

    /// Decode and verify. Fails closed on any integrity mismatch (spec §6.1
    /// invariant 11): wrong magic, truncated frame, plaintext length or
    /// digest mismatch all produce `IntegrityError`/`InvalidFormat`.
    pub fn decode(bytes: &[u8], key: &DigestKey) -> Result<Self> {
        if bytes.len() < 12 || &bytes[0..4] != MAGIC {
            return Err(CoreError::InvalidFormat("bad envelope magic".into()));
        }
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != ENVELOPE_VERSION {
            return Err(CoreError::InvalidFormat(format!("unsupported envelope version {version}")));
        }
        let meta_len = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
        if meta_len > MAX_META_BYTES as usize || bytes.len() < 12 + meta_len {
            return Err(CoreError::InvalidFormat("truncated envelope metadata".into()));
        }
        let meta: EnvelopeMeta = serde_json::from_slice(&bytes[12..12 + meta_len])
            .map_err(|e| CoreError::InvalidFormat(format!("meta decode: {e}")))?;
        let payload = bytes[12 + meta_len..].to_vec();
        if payload.len() as u64 != meta.plaintext_bytes {
            return Err(CoreError::IntegrityError(format!(
                "payload length {} does not match declared {}",
                payload.len(),
                meta.plaintext_bytes
            )));
        }
        let actual = key.digest(&payload);
        if actual != meta.digest {
            return Err(CoreError::IntegrityError(format!(
                "digest mismatch: declared {}, computed {actual}",
                meta.digest
            )));
        }
        Ok(Envelope { meta, payload })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> DigestKey {
        DigestKey::from_bytes([7u8; 32])
    }

    #[test]
    fn roundtrip() {
        let env = Envelope::new("org_t", ObjectKind::Blob, "comb.object/v1", b"payload".to_vec(), &key());
        let bytes = env.encode().unwrap();
        let back = Envelope::decode(&bytes, &key()).unwrap();
        assert_eq!(back.payload, b"payload");
        assert_eq!(back.meta.digest, env.meta.digest);
    }

    #[test]
    fn bit_flip_is_detected() {
        let env = Envelope::new("org_t", ObjectKind::Blob, "comb.object/v1", b"payload".to_vec(), &key());
        let mut bytes = env.encode().unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        match Envelope::decode(&bytes, &key()) {
            Err(CoreError::IntegrityError(_)) => {}
            other => panic!("expected IntegrityError, got {other:?}"),
        }
    }

    #[test]
    fn wrong_tenant_key_fails_verification() {
        let env = Envelope::new("org_t", ObjectKind::Blob, "comb.object/v1", b"payload".to_vec(), &key());
        let bytes = env.encode().unwrap();
        let other = DigestKey::from_bytes([9u8; 32]);
        assert!(matches!(Envelope::decode(&bytes, &other), Err(CoreError::IntegrityError(_))));
    }
}
