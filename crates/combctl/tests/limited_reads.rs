//! Bounded cache and classed blob reads. Does not touch log or publish.

use comb_core::error::CoreError;
use comb_core::{DigestKey, Envelope, EnvelopeExpectation, ObjectClass, ObjectKind};
use comb_object::memory::MemoryBackend;
use comb_object::ObjectBackend;
use combctl::store::{GetSource, Store};
use std::num::NonZeroU64;
use std::sync::Arc;

fn nz(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).expect("nonzero")
}

fn store_with_cache(backend: Arc<dyn ObjectBackend>, cache: Option<std::path::PathBuf>) -> Store {
    Store::new(backend, "org_t", DigestKey::from_bytes([3u8; 32]), cache)
}

fn core(err: anyhow::Error) -> CoreError {
    match err.downcast::<CoreError>() {
        Ok(e) => e,
        Err(other) => panic!("expected CoreError, got {other:#}"),
    }
}

#[tokio::test]
async fn get_blob_limited_exact_encoded_cap() {
    let mem: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
    let store = store_with_cache(mem.clone(), None);
    let payload = b"hello-limited".to_vec();
    let (digest, _) = store.put_blob(payload.clone()).await.unwrap();
    let encoded = mem.get(&store.object_key(&digest)).await.unwrap().0;
    let class = ObjectClass::blob(nz(encoded.len() as u64), nz(payload.len() as u64));
    let (got, source) = store.get_blob_limited(&digest, class).await.unwrap();
    assert_eq!(got, payload);
    assert_eq!(source, GetSource::Backend);

    let err = core(
        store
            .get_blob_limited(
                &digest,
                ObjectClass::blob(nz(encoded.len() as u64 - 1), nz(payload.len() as u64)),
            )
            .await
            .unwrap_err(),
    );
    match err {
        CoreError::ObjectTooLarge {
            limit,
            actual: Some(actual),
            ..
        } => {
            assert_eq!(limit, encoded.len() as u64 - 1);
            assert_eq!(actual, encoded.len() as u64);
        }
        other => panic!("expected ObjectTooLarge, got {other:?}"),
    }
}

#[tokio::test]
async fn get_blob_limited_plaintext_cap() {
    let mem: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
    let store = store_with_cache(mem, None);
    let payload = vec![0xff; 64];
    let (digest, _) = store.put_blob(payload.clone()).await.unwrap();
    let encoded_len = 4 * 1024;
    let err = core(
        store
            .get_blob_limited(&digest, ObjectClass::blob(nz(encoded_len), nz(63)))
            .await
            .unwrap_err(),
    );
    match err {
        CoreError::ObjectTooLarge {
            limit: 63,
            actual: Some(64),
            ..
        } => {}
        other => panic!("expected plaintext ObjectTooLarge, got {other:?}"),
    }
}

#[tokio::test]
async fn oversized_cache_is_bounded_quarantined_and_refetched() {
    let dir = tempfile::tempdir().unwrap();
    let cache = dir.path().join("cache");
    std::fs::create_dir_all(&cache).unwrap();
    let mem: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
    let store = store_with_cache(mem, Some(cache.clone()));
    let payload = b"tiny".to_vec();
    let (digest, _) = store.put_blob(payload.clone()).await.unwrap();
    store
        .get_blob_limited(&digest, ObjectClass::blob(nz(4096), nz(4096)))
        .await
        .unwrap();
    std::fs::write(cache.join(digest.hex()), vec![0u8; 256 * 1024]).unwrap();

    let (got, source) = store
        .get_blob_limited(&digest, ObjectClass::blob(nz(1024), nz(1024)))
        .await
        .unwrap();
    assert_eq!(got, payload);
    assert_eq!(source, GetSource::Backend);
    assert!(cache
        .join(digest.hex())
        .with_extension("quarantine")
        .exists());
}

#[tokio::test]
async fn cache_hit_uses_limited_decode() {
    let dir = tempfile::tempdir().unwrap();
    let cache = dir.path().join("cache");
    std::fs::create_dir_all(&cache).unwrap();
    let mem: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
    let store = store_with_cache(mem, Some(cache));
    let payload = b"cached".to_vec();
    let (digest, _) = store.put_blob(payload.clone()).await.unwrap();
    let class = ObjectClass::blob(nz(4096), nz(4096));
    let _ = store.get_blob_limited(&digest, class).await.unwrap();
    let (got, source) = store.get_blob_limited(&digest, class).await.unwrap();
    assert_eq!(got, payload);
    assert_eq!(source, GetSource::Cache);
}

#[tokio::test]
async fn limited_decode_rejects_wrong_kind_from_store_bytes() {
    let key = DigestKey::from_bytes([3u8; 32]);
    let env = Envelope::new(
        "org_t",
        ObjectKind::Manifest,
        "comb.object/v1",
        b"{}".to_vec(),
        &key,
    );
    let bytes = env.encode().unwrap();
    let err = Envelope::decode_limited(
        &bytes,
        &key,
        nz(4096),
        EnvelopeExpectation {
            kind: ObjectKind::Blob,
            schema: "comb.object/v1",
        },
        "obj",
    )
    .unwrap_err();
    assert!(matches!(
        err,
        CoreError::UnsupportedEnvelopeFormat {
            field: comb_core::EnvelopeFormatField::ObjectKind,
            ..
        }
    ));
}
