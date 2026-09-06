//! Backend conformance suite (spec §7.9, §17.3).
//!
//! No backend is authoritative until it passes these checks. An
//! S3-compatible API string is not evidence of equivalent semantics; this
//! suite is the evidence. Checks run under a unique key prefix and clean up
//! after themselves.

use crate::backend::{ObjectBackend, Version};
use comb_core::error::CoreError;
use std::num::NonZeroU64;
use std::sync::Arc;

#[derive(Debug)]
pub struct CheckResult {
    pub name: &'static str,
    pub passed: bool,
    pub detail: String,
}

pub async fn run(backend: Arc<dyn ObjectBackend>, key_prefix: &str) -> Vec<CheckResult> {
    let mut results = Vec::new();
    let k = |name: &str| format!("{key_prefix}/{name}");
    let mut keys_used: Vec<String> = Vec::new();
    let mut track = |key: String| {
        keys_used.push(key.clone());
        key
    };

    // 1. create-only: first create wins, second fails AlreadyExists.
    {
        let key = track(k("create-only"));
        let first = backend.put_create(&key, b"a").await;
        let second = backend.put_create(&key, b"b").await;
        let passed = first.is_ok() && matches!(second, Err(CoreError::AlreadyExists(_)));
        results.push(CheckResult {
            name: "create-only enforced",
            passed,
            detail: format!(
                "first={:?} second={:?}",
                first.map(|_| "ok"),
                second.map(|_| "ok")
            ),
        });
    }

    // 2. read-after-write: an immediate get returns the created bytes.
    {
        let key = track(k("raw"));
        backend.put_create(&key, b"payload-raw").await.ok();
        let got = backend.get(&key).await;
        let passed = matches!(&got, Ok((bytes, _)) if bytes == b"payload-raw");
        results.push(CheckResult {
            name: "read-after-write (create)",
            passed,
            detail: format!("{:?}", got.map(|(b, _)| b.len())),
        });
    }

    // 3. conditional update happy path: version token guards the write and
    //    changes on success.
    {
        let key = track(k("cas"));
        let v1 = backend.put_update(&key, None, b"g1").await;
        let (v1_ok, v2) = match &v1 {
            Ok(v) => (true, backend.put_update(&key, Some(v), b"g2").await),
            Err(_) => (false, Err(CoreError::NotFound("skipped".into()))),
        };
        let token_changed = matches!((&v1, &v2), (Ok(a), Ok(b)) if a != b);
        results.push(CheckResult {
            name: "conditional update with current version",
            passed: v1_ok && v2.is_ok() && token_changed,
            detail: format!("token_changed={token_changed}"),
        });

        // 4. stale version is rejected and the content is untouched.
        if let (Ok(stale), Ok(_)) = (&v1, &v2) {
            let rejected = backend.put_update(&key, Some(stale), b"g3-stale").await;
            let content = backend.get(&key).await;
            let passed = matches!(rejected, Err(CoreError::PreconditionFailed(_)))
                && matches!(&content, Ok((b, _)) if b == b"g2");
            results.push(CheckResult {
                name: "stale version rejected, content intact",
                passed,
                detail: format!("rejected={:?}", rejected.map(|_| "ACCEPTED (bad)")),
            });
        }

        // 5. create-if-absent on an existing key is rejected.
        let create_existing = backend.put_update(&key, None, b"clobber").await;
        results.push(CheckResult {
            name: "conditional create on existing key rejected",
            passed: matches!(create_existing, Err(CoreError::AlreadyExists(_))),
            detail: format!("{:?}", create_existing.map(|_| "ACCEPTED (bad)")),
        });
    }

    // 6. guarded update of a missing key fails cleanly.
    {
        let key = track(k("missing"));
        let res = backend
            .put_update(&key, Some(&Version("bogus".into())), b"x")
            .await;
        results.push(CheckResult {
            name: "guarded update of missing key rejected",
            passed: matches!(
                res,
                Err(CoreError::PreconditionFailed(_)) | Err(CoreError::NotFound(_))
            ),
            detail: format!("{:?}", res.map(|_| "ACCEPTED (bad)")),
        });
    }

    // 7. concurrent CAS from one observed version: exactly one winner.
    //    This is the linearization property every ref depends on.
    {
        let key = track(k("race"));
        let base = backend.put_update(&key, None, b"base").await;
        if let Ok(base) = base {
            const RACERS: usize = 8;
            let mut tasks = Vec::new();
            for i in 0..RACERS {
                let backend = backend.clone();
                let key = key.clone();
                let expected = base.clone();
                tasks.push(tokio::spawn(async move {
                    backend
                        .put_update(&key, Some(&expected), format!("racer-{i}").as_bytes())
                        .await
                        .is_ok()
                }));
            }
            let mut winners = 0;
            for t in tasks {
                if t.await.unwrap_or(false) {
                    winners += 1;
                }
            }
            results.push(CheckResult {
                name: "concurrent CAS has exactly one winner",
                passed: winners == 1,
                detail: format!("{winners}/{RACERS} writes accepted"),
            });
        } else {
            results.push(CheckResult {
                name: "concurrent CAS has exactly one winner",
                passed: false,
                detail: "setup failed".into(),
            });
        }
    }

    // 8. exists reflects reality.
    {
        let key = track(k("exists"));
        backend.put_create(&key, b"x").await.ok();
        let present = backend.exists(&key).await;
        let absent = backend.exists(&k("never-written")).await;
        results.push(CheckResult {
            name: "exists is accurate",
            passed: matches!(present, Ok(true)) && matches!(absent, Ok(false)),
            detail: format!("present={present:?} absent={absent:?}"),
        });
    }

    // 9. delete removes; deleting a missing key is not an error.
    {
        let key = track(k("delete"));
        backend.put_create(&key, b"x").await.ok();
        let d1 = backend.delete(&key).await;
        let gone = backend.exists(&key).await;
        let d2 = backend.delete(&key).await;
        results.push(CheckResult {
            name: "delete removes; idempotent on missing",
            passed: d1.is_ok() && matches!(gone, Ok(false)) && d2.is_ok(),
            detail: format!("gone={gone:?}"),
        });
    }

    // 10. get_limited accepts an object at the exact cap and rejects one
    //     extra byte as ObjectTooLarge, never a truncated body.
    {
        let key = track(k("limited-exact"));
        let body = vec![0xab; 32];
        backend.put_create(&key, &body).await.ok();
        let exact = NonZeroU64::new(32).expect("nonzero");
        let under = NonZeroU64::new(31).expect("nonzero");
        let got = backend.get_limited(&key, exact).await;
        let over = backend.get_limited(&key, under).await;
        let full = backend.get(&key).await;
        let passed = matches!(&got, Ok((bytes, _)) if bytes == &body)
            && matches!(
                &over,
                Err(CoreError::ObjectTooLarge {
                    limit: 31,
                    actual: Some(32),
                    ..
                })
            )
            && matches!(&full, Ok((bytes, _)) if bytes == &body);
        results.push(CheckResult {
            name: "get_limited exact cap; one extra byte rejected",
            passed,
            detail: format!("exact={got:?} over={over:?}"),
        });
    }

    // 11. missing key through get_limited is confirmed NotFound.
    {
        let key = k("limited-missing");
        let res = backend
            .get_limited(&key, NonZeroU64::new(8).expect("nonzero"))
            .await;
        results.push(CheckResult {
            name: "get_limited missing key is NotFound",
            passed: matches!(res, Err(CoreError::NotFound(_))),
            detail: format!("{res:?}"),
        });
    }

    // Cleanup.
    for key in keys_used {
        backend.delete(&key).await.ok();
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::failpoint::{CountingBackend, FailpointBackend};
    use crate::fault::FaultBackend;
    use crate::local::LocalBackend;
    use crate::memory::MemoryBackend;
    use crate::s3::S3Backend;

    #[tokio::test]
    async fn memory_backend_conforms() {
        let results = run(Arc::new(MemoryBackend::new()), "conformance-test").await;
        assert!(results.iter().all(|r| r.passed), "{results:#?}");
    }

    #[tokio::test]
    async fn local_backend_conforms() {
        let dir = tempfile::tempdir().unwrap();
        let results = run(Arc::new(LocalBackend::new(dir.path())), "conformance-test").await;
        assert!(results.iter().all(|r| r.passed), "{results:#?}");
    }

    #[tokio::test]
    async fn wrappers_preserve_get_limited_cap_and_error_class() {
        let mem = Arc::new(MemoryBackend::new());
        for (name, backend) in [
            (
                "fault",
                Arc::new(FaultBackend::new(mem.clone(), 1, 0.0, 0.0)) as Arc<dyn ObjectBackend>,
            ),
            (
                "failpoint",
                Arc::new(FailpointBackend::new(mem.clone())) as Arc<dyn ObjectBackend>,
            ),
            (
                "counting",
                Arc::new(CountingBackend::new(mem.clone())) as Arc<dyn ObjectBackend>,
            ),
        ] {
            let results = run(backend, &format!("wrap-{name}")).await;
            assert!(
                results.iter().all(|r| r.passed),
                "{name} wrapper: {results:#?}"
            );
        }
    }

    #[tokio::test]
    async fn failpoint_get_limited_transient_is_not_not_found() {
        let mem = Arc::new(MemoryBackend::new());
        mem.put_create("k/present", b"abcd").await.unwrap();
        let fp = FailpointBackend::drop_next_get_limited_request(mem.clone(), "k/present");
        let cap = NonZeroU64::new(16).unwrap();
        match fp.get_limited("k/present", cap).await {
            Err(CoreError::BackendUnavailable(_)) => {}
            other => panic!("injected failure must not be NotFound, got {other:?}"),
        }
        match fp.get_limited("k/absent", cap).await {
            Err(CoreError::NotFound(_)) => {}
            other => panic!("missing key must stay NotFound, got {other:?}"),
        }
        let io = FailpointBackend::io_on_next_get_limited(mem.clone(), "k/present");
        match io.get_limited("k/present", cap).await {
            Err(CoreError::Io(_)) => {}
            other => panic!("injected io must stay Io, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn s3_get_limited_conforms_if_configured() {
        let Ok(bucket) = std::env::var("COMB_S3_BUCKET") else {
            return;
        };
        let region = std::env::var("COMB_S3_REGION").unwrap_or_else(|_| "us-east-1".into());
        let prefix = format!(
            "comb-r2-limited-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
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
        let results = run(Arc::new(backend), "conformance-test").await;
        assert!(results.iter().all(|r| r.passed), "{results:#?}");
    }
}
