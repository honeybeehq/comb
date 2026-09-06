use crate::digest::{Digest, DigestKey};
use crate::error::{CoreError, EnvelopeFormatField, Result};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;

/// Envelope binary layout (spec §7.3, decided in v0.3):
/// fixed header (magic "COMB", envelope version u16, flags u16, meta length
/// u32, all little-endian) + canonical JSON metadata + payload.
const MAGIC: &[u8; 4] = b"COMB";
const ENVELOPE_VERSION: u16 = 1;
pub const MAX_ENVELOPE_META_BYTES: u32 = 64 * 1024;
const MAX_META_BYTES: u32 = MAX_ENVELOPE_META_BYTES;

/// Encoded-object caps for classed reads. Callers pass these into
/// `decode_limited` and `ObjectBackend::get_limited`.
pub const MAX_REF_OBJECT_BYTES: u64 = 64 * 1024;
pub const MAX_MANIFEST_OBJECT_BYTES: u64 = 512 * 1024;
pub const MAX_STABLE_INDEX_NODE_OBJECT_BYTES: u64 = 8 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ObjectKind {
    Blob,
    Manifest,
    JournalEntry,
}

impl ObjectKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Blob => "blob",
            Self::Manifest => "manifest",
            Self::JournalEntry => "journal-entry",
        }
    }

    fn from_meta_str(s: &str) -> Option<Self> {
        match s {
            "blob" => Some(Self::Blob),
            "manifest" => Some(Self::Manifest),
            "journal-entry" => Some(Self::JournalEntry),
            _ => None,
        }
    }
}

/// Kind and schema the caller requires before any payload decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EnvelopeExpectation<'a> {
    pub kind: ObjectKind,
    pub schema: &'a str,
}

/// Size and format class for a digest-addressed object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectClass {
    pub kind: ObjectKind,
    pub schema: &'static str,
    pub max_encoded_bytes: NonZeroU64,
    pub max_plaintext_bytes: NonZeroU64,
}

impl ObjectClass {
    pub fn expectation(self) -> EnvelopeExpectation<'static> {
        EnvelopeExpectation {
            kind: self.kind,
            schema: self.schema,
        }
    }

    pub fn blob(max_encoded_bytes: NonZeroU64, max_plaintext_bytes: NonZeroU64) -> Self {
        Self {
            kind: ObjectKind::Blob,
            schema: "comb.object/v1",
            max_encoded_bytes,
            max_plaintext_bytes,
        }
    }
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
    pub fn new(
        tenant: &str,
        kind: ObjectKind,
        schema: &str,
        payload: Vec<u8>,
        key: &DigestKey,
    ) -> Self {
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
            return Err(CoreError::InvalidFormat(format!(
                "unsupported envelope version {version}"
            )));
        }
        let meta_len = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]) as usize;
        if meta_len > MAX_META_BYTES as usize || bytes.len() < 12 + meta_len {
            return Err(CoreError::InvalidFormat(
                "truncated envelope metadata".into(),
            ));
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

    /// Bounded decode used by R2 classed reads.
    ///
    /// Order is fixed: encoded-size cap, fixed header and at most 64 KiB of
    /// metadata, format tags, then untransformed payload length and digest.
    /// Compression and encryption other than `"none"` never reach a codec
    /// or payload deserializer.
    pub fn decode_limited(
        bytes: &[u8],
        digest_key: &DigestKey,
        max_encoded_bytes: NonZeroU64,
        expected: EnvelopeExpectation<'_>,
        object_key: &str,
    ) -> Result<Self> {
        let limit = max_encoded_bytes.get();
        if bytes.len() as u64 > limit {
            return Err(CoreError::ObjectTooLarge {
                key: object_key.into(),
                limit,
                actual: Some(bytes.len() as u64),
            });
        }
        if bytes.len() < 12 || &bytes[0..4] != MAGIC {
            return Err(CoreError::InvalidFormat("bad envelope magic".into()));
        }
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != ENVELOPE_VERSION {
            return Err(CoreError::UnsupportedEnvelopeFormat {
                field: EnvelopeFormatField::Version,
                value: version.to_string(),
            });
        }
        let flags = u16::from_le_bytes([bytes[6], bytes[7]]);
        if flags != 0 {
            return Err(CoreError::UnsupportedEnvelopeFormat {
                field: EnvelopeFormatField::Flags,
                value: flags.to_string(),
            });
        }
        let meta_len = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        if meta_len > MAX_META_BYTES || bytes.len() < 12 + meta_len as usize {
            return Err(CoreError::InvalidFormat(
                "truncated envelope metadata".into(),
            ));
        }
        let meta_bytes = &bytes[12..12 + meta_len as usize];
        let raw: serde_json::Value = serde_json::from_slice(meta_bytes)
            .map_err(|e| CoreError::InvalidFormat(format!("meta decode: {e}")))?;
        let obj = raw.as_object().ok_or_else(|| {
            CoreError::InvalidFormat("envelope metadata must be an object".into())
        })?;

        let compression = meta_string(obj, "compression")?;
        if compression != "none" {
            return Err(CoreError::UnsupportedEnvelopeFormat {
                field: EnvelopeFormatField::Compression,
                value: compression,
            });
        }
        let encryption = meta_string(obj, "encryption")?;
        if encryption != "none" {
            return Err(CoreError::UnsupportedEnvelopeFormat {
                field: EnvelopeFormatField::Encryption,
                value: encryption,
            });
        }
        let kind_str = meta_string(obj, "kind")?;
        match ObjectKind::from_meta_str(&kind_str) {
            Some(kind) if kind == expected.kind => {}
            _ => {
                return Err(CoreError::UnsupportedEnvelopeFormat {
                    field: EnvelopeFormatField::ObjectKind,
                    value: kind_str,
                });
            }
        }
        let schema = meta_string(obj, "schema")?;
        if schema != expected.schema {
            return Err(CoreError::UnsupportedEnvelopeFormat {
                field: EnvelopeFormatField::Schema,
                value: schema,
            });
        }

        let meta: EnvelopeMeta = serde_json::from_value(raw)
            .map_err(|e| CoreError::InvalidFormat(format!("meta decode: {e}")))?;
        let payload = bytes[12 + meta_len as usize..].to_vec();
        if payload.len() as u64 != meta.plaintext_bytes {
            return Err(CoreError::IntegrityError(format!(
                "payload length {} does not match declared {}",
                payload.len(),
                meta.plaintext_bytes
            )));
        }
        let actual = digest_key.digest(&payload);
        if actual != meta.digest {
            return Err(CoreError::IntegrityError(format!(
                "digest mismatch: declared {}, computed {actual}",
                meta.digest
            )));
        }
        Ok(Envelope { meta, payload })
    }

    /// `decode_limited`, then the caller-supplied payload decoder.
    ///
    /// The payload decoder runs only after every format tag is accepted and
    /// the untransformed digest verifies. Tests pass a panicking decoder to
    /// prove unsupported envelopes never deserialize, decompress, or decrypt.
    pub fn decode_limited_with<T, F>(
        bytes: &[u8],
        digest_key: &DigestKey,
        max_encoded_bytes: NonZeroU64,
        expected: EnvelopeExpectation<'_>,
        object_key: &str,
        decode_payload: F,
    ) -> Result<(Self, T)>
    where
        F: FnOnce(&[u8]) -> Result<T>,
    {
        let env = Self::decode_limited(bytes, digest_key, max_encoded_bytes, expected, object_key)?;
        let decoded = decode_payload(&env.payload)?;
        Ok((env, decoded))
    }
}

fn meta_string(obj: &serde_json::Map<String, serde_json::Value>, name: &str) -> Result<String> {
    match obj.get(name) {
        Some(serde_json::Value::String(s)) => Ok(s.clone()),
        Some(_) => Err(CoreError::InvalidFormat(format!(
            "metadata {name} must be a string"
        ))),
        None => Err(CoreError::InvalidFormat(format!("metadata missing {name}"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::EnvelopeFormatField;

    fn key() -> DigestKey {
        DigestKey::from_bytes([7u8; 32])
    }

    #[test]
    fn roundtrip() {
        let env = Envelope::new(
            "org_t",
            ObjectKind::Blob,
            "comb.object/v1",
            b"payload".to_vec(),
            &key(),
        );
        let bytes = env.encode().unwrap();
        let back = Envelope::decode(&bytes, &key()).unwrap();
        assert_eq!(back.payload, b"payload");
        assert_eq!(back.meta.digest, env.meta.digest);
    }

    #[test]
    fn bit_flip_is_detected() {
        let env = Envelope::new(
            "org_t",
            ObjectKind::Blob,
            "comb.object/v1",
            b"payload".to_vec(),
            &key(),
        );
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
        let env = Envelope::new(
            "org_t",
            ObjectKind::Blob,
            "comb.object/v1",
            b"payload".to_vec(),
            &key(),
        );
        let bytes = env.encode().unwrap();
        let other = DigestKey::from_bytes([9u8; 32]);
        assert!(matches!(
            Envelope::decode(&bytes, &other),
            Err(CoreError::IntegrityError(_))
        ));
    }

    fn blob_expected() -> EnvelopeExpectation<'static> {
        EnvelopeExpectation {
            kind: ObjectKind::Blob,
            schema: "comb.object/v1",
        }
    }

    fn cap(n: u64) -> NonZeroU64 {
        NonZeroU64::new(n).expect("nonzero")
    }

    fn frame(version: u16, flags: u16, meta: &serde_json::Value, payload: &[u8]) -> Vec<u8> {
        let meta_bytes = serde_json::to_vec(meta).unwrap();
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&version.to_le_bytes());
        out.extend_from_slice(&flags.to_le_bytes());
        out.extend_from_slice(&(meta_bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&meta_bytes);
        out.extend_from_slice(payload);
        out
    }

    fn panic_payload(_: &[u8]) -> Result<serde_json::Value> {
        panic!("payload decoder invoked");
    }

    fn sample() -> (Envelope, Vec<u8>, serde_json::Value) {
        let env = Envelope::new(
            "org_t",
            ObjectKind::Blob,
            "comb.object/v1",
            br#"{"ok":true}"#.to_vec(),
            &key(),
        );
        let bytes = env.encode().unwrap();
        let meta = serde_json::to_value(&env.meta).unwrap();
        (env, bytes, meta)
    }

    fn reject_format(bytes: &[u8], field: EnvelopeFormatField, value: &str) {
        let err = Envelope::decode_limited_with(
            bytes,
            &key(),
            cap(4096),
            blob_expected(),
            "obj",
            panic_payload,
        )
        .expect_err("format must fail before payload decode");
        match err {
            CoreError::UnsupportedEnvelopeFormat {
                field: got_field,
                value: got_value,
            } => {
                assert_eq!(got_field, field);
                assert_eq!(got_value, value);
            }
            other => panic!("expected UnsupportedEnvelopeFormat, got {other:?}"),
        }
    }

    #[test]
    fn limited_roundtrip_then_payload_decode() {
        let (env, bytes, _) = sample();
        let (back, value) = Envelope::decode_limited_with(
            &bytes,
            &key(),
            cap(bytes.len() as u64),
            blob_expected(),
            "obj",
            |payload| {
                serde_json::from_slice::<serde_json::Value>(payload)
                    .map_err(|e| CoreError::InvalidFormat(format!("payload: {e}")))
            },
        )
        .unwrap();
        assert_eq!(back.payload, env.payload);
        assert_eq!(value["ok"], serde_json::json!(true));
    }

    #[test]
    fn limited_rejects_one_byte_over_encoded_cap() {
        let (_, bytes, _) = sample();
        let err = Envelope::decode_limited(
            &bytes,
            &key(),
            cap(bytes.len() as u64 - 1),
            blob_expected(),
            "obj",
        )
        .unwrap_err();
        match err {
            CoreError::ObjectTooLarge { key, limit, actual } => {
                assert_eq!(key, "obj");
                assert_eq!(limit, bytes.len() as u64 - 1);
                assert_eq!(actual, Some(bytes.len() as u64));
            }
            other => panic!("expected ObjectTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn unsupported_format_fields_skip_payload_decoder() {
        let (env, bytes, meta) = sample();
        let payload = env.payload.as_slice();

        let mut versioned = bytes.clone();
        versioned[4] = 2;
        versioned[5] = 0;
        reject_format(&versioned, EnvelopeFormatField::Version, "2");

        let mut flagged = bytes.clone();
        flagged[6] = 1;
        flagged[7] = 0;
        reject_format(&flagged, EnvelopeFormatField::Flags, "1");

        let mut compression = meta.clone();
        compression["compression"] = serde_json::json!("gzip");
        reject_format(
            &frame(1, 0, &compression, payload),
            EnvelopeFormatField::Compression,
            "gzip",
        );

        let mut encryption = meta.clone();
        encryption["encryption"] = serde_json::json!("aesg");
        reject_format(
            &frame(1, 0, &encryption, payload),
            EnvelopeFormatField::Encryption,
            "aesg",
        );

        let mut kind = meta.clone();
        kind["kind"] = serde_json::json!("manifest");
        reject_format(
            &frame(1, 0, &kind, payload),
            EnvelopeFormatField::ObjectKind,
            "manifest",
        );

        let mut unknown_kind = meta.clone();
        unknown_kind["kind"] = serde_json::json!("chunk");
        reject_format(
            &frame(1, 0, &unknown_kind, payload),
            EnvelopeFormatField::ObjectKind,
            "chunk",
        );

        let mut schema = meta.clone();
        schema["schema"] = serde_json::json!("comb.object/v2");
        reject_format(
            &frame(1, 0, &schema, payload),
            EnvelopeFormatField::Schema,
            "comb.object/v2",
        );
    }
}
