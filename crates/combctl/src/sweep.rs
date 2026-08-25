//! Orphan sweeper: reachability-based collection of unreferenced objects
//! (spec §19). Roots are the tenant's refs; reachability is a BFS through
//! every digest referenced by reachable objects. Objects younger than the
//! grace window are never touched (a concurrent writer may be about to
//! publish them — drill G4/L1). Dry-run by default.

use crate::store::Store;
use anyhow::Result;
use chrono::{Duration, Utc};
use comb_core::Digest;
use std::collections::{HashSet, VecDeque};

#[derive(Debug, Default)]
pub struct SweepReport {
    pub refs_scanned: usize,
    pub objects_scanned: usize,
    pub reachable: usize,
    pub in_grace: usize,
    pub candidates: Vec<String>,
    pub deleted: usize,
}

/// Collect every `b3k:` digest appearing anywhere in a JSON value.
fn digests_in(value: &serde_json::Value, out: &mut Vec<Digest>) {
    match value {
        serde_json::Value::String(s) => {
            if let Ok(d) = Digest::parse(s) {
                out.push(d);
            }
        }
        serde_json::Value::Array(items) => items.iter().for_each(|v| digests_in(v, out)),
        serde_json::Value::Object(map) => map.values().for_each(|v| digests_in(v, out)),
        _ => {}
    }
}

pub async fn sweep(store: &Store, grace_mins: i64, delete: bool) -> Result<SweepReport> {
    let mut report = SweepReport::default();
    let tenant_prefix = format!("comb/v1/tenants/{}", store.tenant);

    // 1. Roots: every ref file (live refs and journal heads).
    let refs = store.backend.list(&format!("{tenant_prefix}/refs/")).await?;
    report.refs_scanned = refs.len();
    let mut queue: VecDeque<Digest> = VecDeque::new();
    for info in &refs {
        let (bytes, _) = store.backend.get(&info.key).await?;
        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes) {
            let mut found = Vec::new();
            digests_in(&value, &mut found);
            queue.extend(found);
        }
    }

    // 2. BFS through reachable objects. Any digest mentioned by a
    //    reachable object's payload is reachable (schema-agnostic:
    //    manifests, journal entries, and future kinds all qualify).
    let mut reachable: HashSet<String> = HashSet::new();
    while let Some(digest) = queue.pop_front() {
        if !reachable.insert(digest.hex().to_string()) {
            continue;
        }
        let Ok((payload, _)) = store.get_blob(&digest).await else {
            continue; // referenced but missing/corrupt: fsck's problem, not GC's
        };
        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&payload) {
            let mut found = Vec::new();
            digests_in(&value, &mut found);
            queue.extend(found);
        }
    }
    report.reachable = reachable.len();

    // 3. Candidates: stored objects that are unreachable and older than
    //    the grace window.
    let cutoff = Utc::now() - Duration::minutes(grace_mins);
    let objects = store.backend.list(&format!("{tenant_prefix}/objects/b3k/")).await?;
    report.objects_scanned = objects.len();
    for info in &objects {
        let Some(hex) = info.key.rsplit('/').next() else { continue };
        if reachable.contains(hex) {
            continue;
        }
        if info.modified > cutoff {
            report.in_grace += 1;
            continue;
        }
        report.candidates.push(info.key.clone());
    }

    // 4. Delete in the sweep phase only when asked.
    if delete {
        for key in &report.candidates {
            store.backend.delete(key).await?;
            report.deleted += 1;
        }
    }
    Ok(report)
}
