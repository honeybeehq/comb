//! Opt-in retry drills on a real backend. No secrets in the repo.
//!
//!   COMB_DRILL_BACKEND=memory|local|s3 cargo test -p combctl --test backend_drills -- --nocapture
//!
//! S3/MinIO uses the environment the operator already has:
//! COMB_S3_BUCKET, COMB_S3_REGION, COMB_S3_ENDPOINT, COMB_S3_PREFIX, AWS_PROFILE.

use comb_core::DigestKey;
use comb_object::failpoint::FailpointBackend;
use comb_object::local::LocalBackend;
use comb_object::memory::MemoryBackend;
use comb_object::s3::S3Backend;
use comb_object::ObjectBackend;
use combctl::log::LogStore;
use combctl::store::Store;
use std::sync::atomic::Ordering;
use std::sync::Arc;

fn evs(v: &[&str]) -> Vec<Vec<u8>> {
    v.iter().map(|s| s.as_bytes().to_vec()).collect()
}

async fn backend_from_env() -> Option<(Arc<dyn ObjectBackend>, Option<std::path::PathBuf>)> {
    match std::env::var("COMB_DRILL_BACKEND").ok()?.as_str() {
        "memory" => Some((Arc::new(MemoryBackend::new()), None)),
        "local" => {
            let dir = tempfile::tempdir().ok()?;
            let path = dir.path().to_path_buf();
            std::mem::forget(dir);
            Some((Arc::new(LocalBackend::new(&path)), None))
        }
        "s3" => {
            let bucket = std::env::var("COMB_S3_BUCKET").ok()?;
            let region = std::env::var("COMB_S3_REGION").unwrap_or_else(|_| "us-east-1".into());
            let prefix = std::env::var("COMB_S3_PREFIX").unwrap_or_else(|_| "comb-drill".into());
            let endpoint = std::env::var("COMB_S3_ENDPOINT").ok();
            let profile = std::env::var("AWS_PROFILE").ok();
            let backend = S3Backend::connect(
                profile.as_deref(),
                Some(&region),
                &bucket,
                &prefix,
                endpoint.as_deref(),
            )
            .await;
            Some((Arc::new(backend), None))
        }
        other => {
            eprintln!("unknown COMB_DRILL_BACKEND={other}");
            None
        }
    }
}

fn store_on(
    backend: Arc<dyn ObjectBackend>,
    tenant: &str,
    cache: Option<std::path::PathBuf>,
) -> Store {
    Store::new(
        backend,
        tenant.to_string(),
        DigestKey::from_bytes([11u8; 32]),
        cache,
    )
}

/// Drop the ref CAS reply after the inner put_update succeeds. The needle is
/// the exact ref object key so blob `put_create` paths cannot fire the rule.
fn lost_ref_cas(inner: Arc<dyn ObjectBackend>, ref_key: &str) -> Arc<FailpointBackend> {
    Arc::new(FailpointBackend::drop_next_put_update_response(
        inner, ref_key,
    ))
}

#[tokio::test]
async fn live_backend_lost_reply_and_stable_key() {
    let Some((inner, cache)) = backend_from_env().await else {
        eprintln!("skip: set COMB_DRILL_BACKEND=memory|local|s3 to run live drills");
        return;
    };
    let tenant = format!("drill_{}", chrono::Utc::now().timestamp_millis());
    let healthy = store_on(inner.clone(), &tenant, cache.clone());

    // C4: Core set_target CAS reply lost after the ref write commits.
    let (digest, _) = healthy.put_blob(b"c4-target".to_vec()).await.unwrap();
    let c4_key = healthy.ref_key("c4");
    let fp_c4 = lost_ref_cas(inner.clone(), &c4_key);
    let unlucky = store_on(fp_c4.clone(), &tenant, cache.clone());
    let op_c4 = unlucky.mint_operation();
    let err = unlucky
        .set_target_op(op_c4, "c4", digest.clone(), None)
        .await
        .unwrap_err();
    assert!(
        format!("{err:#}").contains("injected"),
        "C4 must surface the lost CAS reply, got {err:#}"
    );
    assert!(
        fp_c4.injected.load(Ordering::Relaxed) >= 1,
        "C4 failpoint did not fire"
    );
    let c4_again = healthy
        .set_target_op(op_c4, "c4", digest, None)
        .await
        .unwrap();
    assert_eq!(c4_again.generation, 1);
    assert!(!c4_again.first_delivery);
    let (c4_head, _) = healthy.read_ref("c4").await.unwrap().unwrap();
    assert_eq!(
        c4_head.generation, 1,
        "C4 must not publish a second generation"
    );

    // L2: Log append CAS reply lost after the partition ref commits.
    let l2_key = healthy.ref_key("log/l2/p0");
    let fp_l2 = lost_ref_cas(inner.clone(), &l2_key);
    let unlucky = store_on(fp_l2.clone(), &tenant, cache.clone());
    let log_l2 = LogStore::new(&unlucky, "l2");
    let op_l2 = unlucky.mint_operation();
    let payload = evs(&["alpha", "beta"]);
    let err = log_l2.append(op_l2, "w", &payload, 60).await.unwrap_err();
    assert!(
        format!("{err:#}").contains("injected"),
        "L2 must surface the lost CAS reply, got {err:#}"
    );
    assert!(
        fp_l2.injected.load(Ordering::Relaxed) >= 1,
        "L2 failpoint did not fire"
    );
    let log_l2 = LogStore::new(&healthy, "l2");
    let l2_again = log_l2.append(op_l2, "w", &payload, 60).await.unwrap();
    assert_eq!((l2_again.first, l2_again.last), (1, 2));
    assert_eq!(l2_again.generation, 1);
    assert!(!l2_again.first_delivery);
    let frames = log_l2.read(1).await.unwrap();
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].payload, b"alpha");
    assert_eq!(frames[1].payload, b"beta");

    // Stable append: same complete-feed key+bytes after a lost ref CAS reply.
    let st_key = healthy.ref_key("log/st/p0");
    let fp_st = lost_ref_cas(inner, &st_key);
    let unlucky = store_on(fp_st.clone(), &tenant, cache.clone());
    let slog = LogStore::complete_feed(&unlucky, "st");
    let sk = comb_core::StableKey::try_from_canonical(b"live-key".to_vec()).unwrap();
    let first = slog.append_stable(sk.clone(), "w", b"payload", 60).await;
    assert!(
        fp_st.injected.load(Ordering::Relaxed) >= 1,
        "stable failpoint did not fire"
    );
    // append_stable retries BackendUnavailable internally, so a single lost
    // reply may recover here. Either way the CAS committed once.
    let rec = match first {
        Err(e) => {
            assert!(
                format!("{e:#}").contains("injected"),
                "stable lost CAS must surface, got {e:#}"
            );
            LogStore::complete_feed(&healthy, "st")
                .append_stable(sk.clone(), "w", b"payload", 60)
                .await
                .unwrap()
        }
        Ok(rec) => rec,
    };
    assert_eq!((rec.range.first, rec.range.last), (1, 1));
    let slog = LogStore::complete_feed(&healthy, "st");
    let rec2 = slog.append_stable(sk, "w", b"payload", 60).await.unwrap();
    assert_eq!(rec2.range.first, rec.range.first);
    assert_eq!(rec2.generation, rec.generation);
    let frames = slog.read(1).await.unwrap();
    assert_eq!(frames.len(), 1, "stable key must not be published twice");
    assert_eq!(frames[0].payload, b"payload");
}
