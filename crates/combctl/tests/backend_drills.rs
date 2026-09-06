//! Opt-in retry drills on a real backend. No secrets in the repo.
//!
//!   COMB_DRILL_BACKEND=memory|local|s3 cargo test -p combctl --test backend_drills -- --nocapture
//!
//! S3/MinIO uses the environment the operator already has:
//! COMB_S3_BUCKET, COMB_S3_REGION, COMB_S3_ENDPOINT, COMB_S3_PREFIX, AWS_PROFILE.

use comb_core::DigestKey;
use comb_object::local::LocalBackend;
use comb_object::memory::MemoryBackend;
use comb_object::s3::S3Backend;
use comb_object::ObjectBackend;
use combctl::log::LogStore;
use combctl::store::Store;
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

#[tokio::test]
async fn live_backend_lost_reply_and_stable_key() {
    let Some((backend, cache)) = backend_from_env().await else {
        eprintln!("skip: set COMB_DRILL_BACKEND=memory|local|s3 to run live drills");
        return;
    };
    let tenant = format!("drill_{}", chrono::Utc::now().timestamp_millis());
    let store = Store::new(backend, tenant, DigestKey::from_bytes([11u8; 32]), cache);
    let log = LogStore::complete_feed(&store, "live");
    let op = store.mint_operation();
    let first = log.append(op, "w", &evs(&["one"]), 60).await.unwrap();
    let again = log.append(op, "w", &evs(&["one"]), 60).await.unwrap();
    assert_eq!(again.first, first.first);
    let key = comb_core::StableKey::try_from_canonical(b"live-key".to_vec()).unwrap();
    let rec = log
        .append_stable(key.clone(), "w", b"payload", 60)
        .await
        .unwrap();
    let rec2 = log.append_stable(key, "w", b"payload", 60).await.unwrap();
    assert_eq!(rec.range.first, rec2.range.first);
    let frames = log.read(1).await.unwrap();
    assert_eq!(frames.last().unwrap().payload, b"payload");
}
