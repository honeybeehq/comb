//! Orphan sweeper: reachability-based collection of unreferenced objects
//! (spec §19). Roots are the tenant's refs; reachability is a BFS through
//! every digest referenced by reachable objects. Objects younger than the
//! grace window are never touched (a concurrent writer may be about to
//! publish them — drill G4/L1). Dry-run by default.

use crate::store::Store;
use anyhow::Result;
use chrono::{Duration, Utc};
use comb_core::error::CoreError;
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
    if delete {
        anyhow::bail!(
            "destructive online sweep is disabled until a separately proven generation barrier exists"
        );
    }
    let v1_prefix = format!("comb/v1/tenants/{}", store.tenant);
    let v1 = store.backend.list(&format!("{v1_prefix}/")).await?;
    if !v1.is_empty() {
        anyhow::bail!(
            "comb/v1 keys exist for tenant {}; this process uses comb/v2 only",
            store.tenant
        );
    }
    let tenant_prefix = format!("comb/v2/tenants/{}", store.tenant);

    let mut queue: VecDeque<Digest> = VecDeque::new();
    for suffix in ["refs/", "ops/", "stable/"] {
        let listed = store
            .backend
            .list(&format!("{tenant_prefix}/{suffix}"))
            .await?;
        report.refs_scanned += listed.len();
        for info in &listed {
            let (bytes, _) =
                store.backend.get(&info.key).await.map_err(|e| {
                    anyhow::anyhow!("sweep aborted: unreadable root {}: {e}", info.key)
                })?;
            let value: serde_json::Value = serde_json::from_slice(&bytes)
                .map_err(|e| anyhow::anyhow!("sweep aborted: malformed root {}: {e}", info.key))?;
            let mut found = Vec::new();
            digests_in(&value, &mut found);
            queue.extend(found);
        }
    }

    let mut reachable: HashSet<String> = HashSet::new();
    while let Some(digest) = queue.pop_front() {
        if !reachable.insert(digest.hex().to_string()) {
            continue;
        }
        let (payload, _) = match store.get_blob(&digest).await {
            Ok(v) => v,
            Err(e) => {
                // Intent request hashes and similar fingerprints are `b3k:`
                // strings that are not stored objects. A confirmed NotFound
                // is therefore not permission to delete, and not proof of
                // a missing required node. Integrity/unavailable still abort.
                if e.downcast_ref::<CoreError>()
                    .is_some_and(|c| matches!(c, CoreError::NotFound(_)))
                {
                    continue;
                }
                return Err(anyhow::anyhow!(
                    "sweep aborted: missing reachable object {digest}: {e:#}"
                ));
            }
        };
        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&payload) {
            let mut found = Vec::new();
            digests_in(&value, &mut found);
            queue.extend(found);
        }
    }
    report.reachable = reachable.len();

    let cutoff = Utc::now() - Duration::minutes(grace_mins);
    let objects = store
        .backend
        .list(&format!("{tenant_prefix}/objects/b3k/"))
        .await?;
    report.objects_scanned = objects.len();
    for info in &objects {
        let Some(hex) = info.key.rsplit('/').next() else {
            continue;
        };
        if reachable.contains(hex) {
            continue;
        }
        if info.modified > cutoff {
            report.in_grace += 1;
            continue;
        }
        report.candidates.push(info.key.clone());
    }
    Ok(report)
}
