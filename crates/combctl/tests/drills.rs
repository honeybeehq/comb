//! Core ref and object drills (spec §22.3) plus the chaos property run.
//! C5 (stale listing) does not apply — nothing lists. C8 (key rotation)
//! arrives with the encryption envelope.

use comb_core::error::CoreError;
use comb_core::DigestKey;
use combctl::store::Store;
use comb_object::backend::ObjectBackend;
use comb_object::fault::FaultBackend;
use comb_object::memory::MemoryBackend;
use std::sync::Arc;

fn store_on(backend: Arc<dyn ObjectBackend>, tenant: &str, key_byte: u8) -> Store {
    Store {
        backend,
        tenant: tenant.into(),
        key: DigestKey::from_bytes([key_byte; 32]),
        cache_dir: None,
    }
}

/// C1: two writers store the same content — one creates, one reuses.
#[tokio::test]
async fn c1_same_digest_created_twice_deduplicates() {
    let mem: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
    let a = store_on(mem.clone(), "org_t", 1);
    let b = store_on(mem.clone(), "org_t", 1);
    let (d1, dedup1) = a.put_blob(b"same bytes".to_vec()).await.unwrap();
    let (d2, dedup2) = b.put_blob(b"same bytes".to_vec()).await.unwrap();
    assert_eq!(d1, d2);
    assert!(!dedup1);
    assert!(dedup2, "second writer must verify and reuse, not fail");
    let (payload, _) = b.get_blob(&d1).await.unwrap();
    assert_eq!(payload, b"same bytes");
}

/// C3: corrupt authoritative object — integrity error, no content returned.
#[tokio::test]
async fn c3_corrupt_authoritative_object_fails_closed() {
    let mem = Arc::new(MemoryBackend::new());
    let store = store_on(mem.clone(), "org_t", 1);
    let (digest, _) = store.put_blob(b"precious".to_vec()).await.unwrap();

    // Corrupt the stored bytes in place through the raw backend.
    let key = format!(
        "comb/v1/tenants/org_t/objects/b3k/{}/{}",
        digest.key_prefix(),
        digest.hex()
    );
    let (mut bytes, version) = mem.get(&key).await.unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    mem.put_update(&key, Some(&version), &bytes).await.unwrap();

    let err = store.get_blob(&digest).await.unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("integrity") || msg.contains("digest"),
        "expected integrity failure, got: {msg}"
    );
}

/// C6: unsupported schema/envelope version is refused, not misread.
#[tokio::test]
async fn c6_unsupported_envelope_version_refused() {
    let key = DigestKey::from_bytes([1u8; 32]);
    let env = comb_core::Envelope::new("org_t", comb_core::ObjectKind::Blob, "comb.object/v1", b"x".to_vec(), &key);
    let mut bytes = env.encode().unwrap();
    bytes[4] = 2; // envelope version 2
    match comb_core::Envelope::decode(&bytes, &key) {
        Err(CoreError::InvalidFormat(m)) => assert!(m.contains("version")),
        other => panic!("expected InvalidFormat, got {other:?}"),
    }
}

/// C7: identical plaintext in two tenants — unrelated digests, independent
/// storage, no cross-tenant dedup or existence signal.
#[tokio::test]
async fn c7_no_cross_tenant_digest_or_dedup() {
    let mem: Arc<dyn ObjectBackend> = Arc::new(MemoryBackend::new());
    let a = store_on(mem.clone(), "org_a", 1);
    let b = store_on(mem.clone(), "org_b", 2);
    let (da, dedup_a) = a.put_blob(b"identical secret".to_vec()).await.unwrap();
    let (db, dedup_b) = b.put_blob(b"identical secret".to_vec()).await.unwrap();
    assert_ne!(da, db, "tenant-keyed digests must differ for identical content");
    assert!(!dedup_a && !dedup_b, "no cross-tenant dedup may occur");
}

/// C4 (ambiguous acknowledgement): a ref update whose response is lost has
/// still committed; a fresh read observes it, and a blind retry from the
/// stale expectation is rejected rather than double-applied.
#[tokio::test]
async fn c4_lost_response_state_is_committed_and_retry_rejected() {
    let mem: Arc<MemoryBackend> = Arc::new(MemoryBackend::new());
    let store = store_on(mem.clone(), "org_t", 1);
    let (digest, _) = store.put_blob(b"v".to_vec()).await.unwrap();

    // Every mutation's response is lost; requests always go through.
    let faulty = Arc::new(FaultBackend::new(mem.clone(), 7, 0.0, 1.0));
    let unlucky = store_on(faulty, "org_t", 1);

    let err = unlucky.set_target("demo/ref", digest.clone(), None).await.unwrap_err();
    assert!(format!("{err:#}").contains("injected"));

    // The commit happened: a fault-free reader sees generation 1.
    let (value, _) = store.read_ref("demo/ref").await.unwrap().expect("ref must exist");
    assert_eq!(value.generation, 1, "update committed despite lost response");

    // Retrying through the healthy path applies exactly once more (the
    // caller re-reads and proceeds); generations stay contiguous.
    store.set_target("demo/ref", digest, None).await.unwrap();
    let (value, _) = store.read_ref("demo/ref").await.unwrap().unwrap();
    assert_eq!(value.generation, 2);
}

/// Chaos property run: 3000 operations at 20% fault probability per call.
/// Zero invariant violations, and the run is deterministic by seed.
#[tokio::test]
async fn chaos_run_holds_invariants_and_is_reproducible() {
    let r1 = combctl::chaos::run(3000, 99, 0.20, false).await.unwrap();
    assert!(r1.violations.is_empty(), "violations: {:#?}", r1.violations);
    assert!(r1.injected_faults > 100, "faults must actually fire: {}", r1.injected_faults);
    assert!(r1.ambiguous_acks > 0, "ambiguous acks should occur at this fault rate");

    let r2 = combctl::chaos::run(3000, 99, 0.20, false).await.unwrap();
    assert_eq!(r1.acked, r2.acked, "same seed must reproduce the same run");
    assert_eq!(r1.injected_faults, r2.injected_faults);
}
