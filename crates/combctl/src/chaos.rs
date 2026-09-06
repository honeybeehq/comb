//! Chaos engine (spec §22.7): drive the ref protocol through a
//! fault-injecting backend and verify the core invariants after every
//! operation. Reproducible by seed. Zero violations is the only pass.

use crate::store::Store;
use anyhow::Result;
use comb_core::error::CoreError;
use comb_core::DigestKey;
use comb_object::fault::FaultBackend;
use comb_object::memory::MemoryBackend;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct ChaosReport {
    pub iterations: u64,
    pub acked: u64,
    pub clean_failures: u64,
    pub injected_faults: u64,
    pub ambiguous_acks: u64,
    pub violations: Vec<String>,
}

pub async fn run(
    iterations: u64,
    seed: u64,
    fail_prob: f64,
    progress: bool,
) -> Result<ChaosReport> {
    let inner: Arc<MemoryBackend> = Arc::new(MemoryBackend::new());
    let faulty = Arc::new(FaultBackend::new(inner.clone(), seed, fail_prob, fail_prob));
    let key = DigestKey::from_bytes([42u8; 32]);
    let tenant = "org_chaos".to_string();

    // The store under attack, and a fault-free verifier over the same
    // authoritative state (what a fresh reader would observe).
    let store = Store::new(faulty.clone(), tenant.clone(), key.clone(), None);
    let verify = Store::new(inner.clone(), tenant, key, None);

    let ref_name = "chaos/main";
    let mut rng = StdRng::seed_from_u64(seed ^ 0x9e3779b97f4a7c15);
    let mut report = ChaosReport {
        iterations,
        ..Default::default()
    };

    // Last state a verifier confirmed committed. Invariants are checked
    // against this after every operation.
    let mut committed_gen: u64 = 0;
    let mut committed_epoch: u64 = 0;

    let (payload_digest, _) = verify.put_blob(b"chaos payload".to_vec()).await?;

    for i in 0..iterations {
        // Fence knowledge a real writer would hold: read from the
        // authoritative state (the store's own reads may be fault-injected).
        let live = verify.read_ref(ref_name).await?;
        let live_epoch = live.as_ref().map(|(v, _)| v.epoch).unwrap_or(0);
        let leased = live
            .as_ref()
            .map(|(v, _)| v.lease_live(chrono::Utc::now()))
            .unwrap_or(false);

        let op = rng.random_range(0..6u8);
        let outcome = match op {
            0 | 1 => {
                let fence = if leased { Some(live_epoch) } else { None };
                let id = store.mint_operation();
                store
                    .set_target_op(id, ref_name, payload_digest.clone(), fence)
                    .await
                    .map(|_| ())
            }
            2 => {
                let id = store.mint_operation();
                store
                    .claim(id, ref_name, "chaos-writer", 60, false)
                    .await
                    .map(|_| ())
            }
            3 => {
                let id = store.mint_operation();
                store
                    .claim(id, ref_name, "chaos-thief", 60, true)
                    .await
                    .map(|_| ())
            }
            4 => {
                if leased {
                    let id = store.mint_operation();
                    store.release(id, ref_name, live_epoch).await.map(|_| ())
                } else {
                    Ok(())
                }
            }
            _ => {
                // Deliberately stale fence: must fail with Fenced, must
                // never advance anything.
                if live_epoch > 0 {
                    let id = store.mint_operation();
                    match store
                        .set_target_op(id, ref_name, payload_digest.clone(), Some(live_epoch - 1))
                        .await
                    {
                        Err(e) if is_fenced(&e) => Ok(()),
                        Err(e) => Err(e),
                        Ok(_) => {
                            report.violations.push(format!(
                                "iter {i}: stale fence {} accepted at live epoch {live_epoch}",
                                live_epoch - 1
                            ));
                            Ok(())
                        }
                    }
                } else {
                    Ok(())
                }
            }
        };

        let last_was_injected = match outcome {
            Ok(()) => {
                report.acked += 1;
                false
            }
            Err(e) => {
                if format!("{e:#}").contains("injected") {
                    true
                } else {
                    report.clean_failures += 1;
                    false
                }
            }
        };

        // ---- invariant checks against authoritative state --------------
        match verify.read_ref(ref_name).await {
            Ok(Some((value, _))) => {
                if value.generation < committed_gen {
                    report.violations.push(format!(
                        "iter {i}: generation went backwards {committed_gen} -> {}",
                        value.generation
                    ));
                }
                if value.epoch < committed_epoch {
                    report.violations.push(format!(
                        "iter {i}: epoch went backwards {committed_epoch} -> {}",
                        value.epoch
                    ));
                }
                // An op the caller saw fail may still have committed
                // (ambiguous ack): state advanced although no ack was seen.
                if value.generation > committed_gen && last_was_injected {
                    report.ambiguous_acks += 1;
                }
                committed_gen = value.generation;
                committed_epoch = value.epoch;
            }
            Ok(None) => {
                if committed_gen > 0 {
                    report
                        .violations
                        .push(format!("iter {i}: committed ref disappeared"));
                }
            }
            Err(e) => report
                .violations
                .push(format!("iter {i}: committed ref unreadable: {e:#}")),
        }

        if progress && (i + 1) % 200 == 0 {
            let faults = faulty.injected_before.load(Ordering::Relaxed)
                + faulty.injected_after.load(Ordering::Relaxed);
            eprintln!(
                "  {}/{iterations}  acked {}  faults {}  ambiguous {}  violations {}",
                i + 1,
                report.acked,
                faults,
                report.ambiguous_acks,
                report.violations.len()
            );
        }
    }

    // ---- final journal audit -------------------------------------------
    let entries = verify.history(ref_name, usize::MAX).await;
    match entries {
        Ok(entries) => {
            let mut last_gen = u64::MAX;
            for e in &entries {
                if e.generation >= last_gen {
                    report.violations.push(format!(
                        "commit chain: generations not strictly decreasing from head ({} then {})",
                        last_gen, e.generation
                    ));
                }
                last_gen = e.generation;
            }
            // Every journal entry's object decoded and digest-verified in
            // history(); reaching here means the chain is intact.
        }
        Err(e) => report
            .violations
            .push(format!("journal walk failed: {e:#}")),
    }

    report.injected_faults = faulty.injected_before.load(Ordering::Relaxed)
        + faulty.injected_after.load(Ordering::Relaxed);
    Ok(report)
}

fn is_fenced(e: &anyhow::Error) -> bool {
    e.downcast_ref::<CoreError>()
        .map(|c| matches!(c, CoreError::Fenced { .. }))
        .unwrap_or(false)
}
