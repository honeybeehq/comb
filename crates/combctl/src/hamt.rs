//! Bounded HAMT for complete-feed stable keys.
//!
//! 5-bit fan-out, depths 0..=MAX_DEPTH (52 nodes, covering a 256-bit path
//! with the last nibble padded). Each encoded node is at most 4 KiB. The
//! index root lives in the log manifest; nodes are create-only blobs.

use crate::store::Store;
use anyhow::Result;
use comb_core::error::CoreError;
use comb_core::{
    Digest, EnvelopeReadSpec, ObjectKind, StableKey, MAX_STABLE_INDEX_NODE_OBJECT_BYTES,
};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;

pub const NODE_SCHEMA: &str = "comb.log.stable-index-node/v1";
pub const ROOT_SCHEMA: &str = "comb.log.stable-index-root/v1";
pub const MAX_NODE_BYTES: usize = 4 * 1024;
const HAMT_ENVELOPE_SCHEMAS: &[&str] = &["comb.object/v1"];
pub const MAX_STABLE_ADMISSIONS: usize = 1_024;
pub const MAX_STABLE_ADMISSION_BYTES: usize = 256 * 1024;
const FANOUT: usize = 32;
const FANOUT_BITS: u32 = 5;
/// Inclusive maximum *branch* depth. Nibbles 0..=51 cover 256 bits (last
/// nibble padded). A fully-prefixed leaf sits at walk depth 52.
pub const MAX_DEPTH: usize = 51;
const MAX_LEAF_DEPTH: usize = 52;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StableIndexRoot {
    pub schema: String,
    pub digest: Digest,
    pub entries: u64,
}

impl StableIndexRoot {
    pub fn new(digest: Digest, entries: u64) -> Self {
        Self {
            schema: ROOT_SCHEMA.into(),
            digest,
            entries,
        }
    }

    pub fn validate(&self) -> std::result::Result<(), CoreError> {
        if self.schema != ROOT_SCHEMA {
            return Err(CoreError::IntegrityError(format!(
                "unsupported stable index root schema {}",
                self.schema
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StableIndexEntry {
    pub key: StableKey,
    pub payload_hash: Digest,
    pub first: u64,
    pub last: u64,
    pub generation: u64,
}

/// Authoritative head used to reject leaves that name an impossible range.
#[derive(Debug, Clone, Copy)]
pub struct IndexHead {
    pub generation: u64,
    pub head_seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum HamtNode {
    Branch {
        schema: String,
        depth: u8,
        prefix: Vec<u8>,
        children: Vec<HamtChild>,
    },
    Leaf {
        schema: String,
        key: StableKey,
        payload_hash: Digest,
        first: u64,
        last: u64,
        generation: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HamtChild {
    slot: u8,
    digest: Digest,
}

#[derive(Debug)]
pub enum Lookup {
    Found(StableIndexEntry),
    Absent,
}

pub async fn empty_root(store: &Store) -> Result<StableIndexRoot> {
    let digest = put_node(store, &empty_branch()).await?;
    Ok(StableIndexRoot::new(digest, 0))
}

pub async fn lookup(
    store: &Store,
    root: &StableIndexRoot,
    key: &StableKey,
    logical_log: &str,
    head: IndexHead,
) -> Result<Lookup> {
    root.validate()?;
    let path = key.path_digest(&store.key, &store.tenant, logical_log);
    let mut digest = root.digest.clone();
    let mut depth = 0usize;
    loop {
        if depth > MAX_LEAF_DEPTH {
            return Err(CoreError::IntegrityError(
                "stable index walk exceeded the 256-bit path".into(),
            )
            .into());
        }
        let node = load_node(store, &digest, logical_log, &path, depth, Some(head)).await?;
        if depth == 0 {
            check_root_shape(root, &node, &digest)?;
            if root.entries == 0 {
                return Ok(Lookup::Absent);
            }
        }
        match node {
            HamtNode::Leaf {
                key: existing,
                payload_hash,
                first,
                last,
                generation,
                ..
            } => {
                if &existing == key {
                    return Ok(Lookup::Found(StableIndexEntry {
                        key: existing,
                        payload_hash,
                        first,
                        last,
                        generation,
                    }));
                }
                // Same prefix, different key: a valid negative. A leaf stored
                // under the wrong path is rejected in load_node.
                return Ok(Lookup::Absent);
            }
            HamtNode::Branch { children, .. } => {
                if depth > MAX_DEPTH {
                    return Err(CoreError::IntegrityError(format!(
                        "stable index branch {digest} sits past nibble 51"
                    ))
                    .into());
                }
                let slot = nibble(&path, depth)?;
                match child(&children, slot) {
                    None => return Ok(Lookup::Absent),
                    Some(next) => {
                        digest = next.clone();
                        depth += 1;
                    }
                }
            }
        }
    }
}

pub async fn insert(
    store: &Store,
    root: &StableIndexRoot,
    entry: StableIndexEntry,
    logical_log: &str,
    head: IndexHead,
) -> Result<StableIndexRoot> {
    root.validate()?;
    validate_leaf_range(
        entry.first,
        entry.last,
        entry.generation,
        &root.digest,
        head,
    )?;
    let path = entry
        .key
        .path_digest(&store.key, &store.tenant, logical_log);
    let root_node = load_node(store, &root.digest, logical_log, &path, 0, Some(head)).await?;
    check_root_shape(root, &root_node, &root.digest)?;
    let new_digest = insert_at(
        store,
        Some(&root.digest),
        &entry,
        &path,
        logical_log,
        0,
        head,
    )
    .await?;
    let entries = root
        .entries
        .checked_add(1)
        .ok_or_else(|| CoreError::Rejected("stable index entry count overflow".into()))?;
    Ok(StableIndexRoot::new(new_digest, entries))
}

async fn insert_at(
    store: &Store,
    current: Option<&Digest>,
    entry: &StableIndexEntry,
    path: &Digest,
    logical_log: &str,
    depth: usize,
    head: IndexHead,
) -> Result<Digest> {
    if depth > MAX_LEAF_DEPTH {
        return Err(CoreError::Rejected("stable key path hash collision".into()).into());
    }
    let node = match current {
        Some(d) => load_node(store, d, logical_log, path, depth, Some(head)).await?,
        None => branch_node(depth, path, Vec::new())?,
    };
    match node {
        HamtNode::Leaf {
            key: existing,
            payload_hash,
            first,
            last,
            generation,
            ..
        } => {
            if existing == entry.key {
                return Err(CoreError::RecoveryFailed(
                    "stable index insert of a key that already has a leaf".into(),
                )
                .into());
            }
            if depth > MAX_DEPTH {
                return Err(CoreError::Rejected("stable key path hash collision".into()).into());
            }
            let old = StableIndexEntry {
                key: existing,
                payload_hash,
                first,
                last,
                generation,
            };
            let old_path = old.key.path_digest(&store.key, &store.tenant, logical_log);
            split(store, &old, &old_path, entry, path, depth).await
        }
        HamtNode::Branch { mut children, .. } => {
            if depth > MAX_DEPTH {
                return Err(CoreError::IntegrityError(
                    "stable index branch sits past nibble 51".into(),
                )
                .into());
            }
            let slot = nibble(path, depth)?;
            match child(&children, slot).cloned() {
                None => {
                    let leaf = put_node(store, &leaf_node(entry)).await?;
                    upsert_child(&mut children, slot, leaf);
                    put_node(store, &branch_node(depth, path, children)?).await
                }
                Some(next) => {
                    let updated = Box::pin(insert_at(
                        store,
                        Some(&next),
                        entry,
                        path,
                        logical_log,
                        depth + 1,
                        head,
                    ))
                    .await?;
                    upsert_child(&mut children, slot, updated);
                    put_node(store, &branch_node(depth, path, children)?).await
                }
            }
        }
    }
}

async fn split(
    store: &Store,
    a: &StableIndexEntry,
    a_path: &Digest,
    b: &StableIndexEntry,
    b_path: &Digest,
    depth: usize,
) -> Result<Digest> {
    if depth > MAX_DEPTH {
        return Err(CoreError::Rejected("stable key path hash collision".into()).into());
    }
    let sa = nibble(a_path, depth)?;
    let sb = nibble(b_path, depth)?;
    if sa != sb {
        let da = put_node(store, &leaf_node(a)).await?;
        let db = put_node(store, &leaf_node(b)).await?;
        let mut children = Vec::new();
        upsert_child(&mut children, sa, da);
        upsert_child(&mut children, sb, db);
        return put_node(store, &branch_node(depth, a_path, children)?).await;
    }
    if depth == MAX_DEPTH {
        return Err(CoreError::Rejected("stable key path hash collision".into()).into());
    }
    let child = Box::pin(split(store, a, a_path, b, b_path, depth + 1)).await?;
    let mut children = Vec::new();
    upsert_child(&mut children, sa, child);
    put_node(store, &branch_node(depth, a_path, children)?).await
}

fn leaf_node(entry: &StableIndexEntry) -> HamtNode {
    HamtNode::Leaf {
        schema: NODE_SCHEMA.into(),
        key: entry.key.clone(),
        payload_hash: entry.payload_hash.clone(),
        first: entry.first,
        last: entry.last,
        generation: entry.generation,
    }
}

fn child(children: &[HamtChild], slot: u8) -> Option<&Digest> {
    children.iter().find(|c| c.slot == slot).map(|c| &c.digest)
}

fn upsert_child(children: &mut Vec<HamtChild>, slot: u8, digest: Digest) {
    if let Some(c) = children.iter_mut().find(|c| c.slot == slot) {
        c.digest = digest;
    } else {
        children.push(HamtChild { slot, digest });
        children.sort_by_key(|c| c.slot);
    }
}

fn nibble(path: &Digest, depth: usize) -> std::result::Result<u8, CoreError> {
    if depth > MAX_DEPTH {
        return Err(CoreError::Rejected("stable key path depth".into()));
    }
    let raw = path.raw();
    let bit = depth * FANOUT_BITS as usize;
    let mut acc = 0u16;
    for i in 0..5 {
        let b = bit + i;
        let byte = if b / 8 < 32 { raw[b / 8] } else { 0 };
        let bitv = (byte >> (7 - (b % 8))) & 1;
        acc = (acc << 1) | bitv as u16;
    }
    Ok(acc as u8)
}

fn validate_slots(children: &[HamtChild], digest: &Digest) -> std::result::Result<(), CoreError> {
    if children.len() > FANOUT {
        return Err(CoreError::IntegrityError(format!(
            "stable index node {digest} fanout {} exceeds {FANOUT}",
            children.len()
        )));
    }
    let mut prev: Option<u8> = None;
    for c in children {
        if c.slot as usize >= FANOUT {
            return Err(CoreError::IntegrityError(format!(
                "stable index node {digest} has slot {}",
                c.slot
            )));
        }
        if let Some(p) = prev {
            if c.slot <= p {
                return Err(CoreError::IntegrityError(format!(
                    "stable index node {digest} slots are not strictly ordered"
                )));
            }
        }
        prev = Some(c.slot);
    }
    Ok(())
}

fn validate_leaf_range(
    first: u64,
    last: u64,
    generation: u64,
    digest: &Digest,
    head: IndexHead,
) -> std::result::Result<(), CoreError> {
    if first == 0 || last < first {
        return Err(CoreError::IntegrityError(format!(
            "stable index leaf {digest} has impossible range {first}..{last}"
        )));
    }
    if generation == 0 || generation > head.generation {
        return Err(CoreError::IntegrityError(format!(
            "stable index leaf {digest} generation {generation} disagrees with head {}",
            head.generation
        )));
    }
    if last > head.head_seq {
        return Err(CoreError::IntegrityError(format!(
            "stable index leaf {digest} range ends at {last} past head_seq {}",
            head.head_seq
        )));
    }
    Ok(())
}

async fn load_node(
    store: &Store,
    digest: &Digest,
    logical_log: &str,
    walk_path: &Digest,
    depth: usize,
    head: Option<IndexHead>,
) -> Result<HamtNode> {
    let spec = EnvelopeReadSpec {
        tenant: &store.tenant,
        kind: ObjectKind::Blob,
        allowed_schemas: HAMT_ENVELOPE_SCHEMAS,
        max_encoded_bytes: NonZeroU64::new(MAX_STABLE_INDEX_NODE_OBJECT_BYTES).expect("nonzero"),
        max_plaintext_bytes: NonZeroU64::new(MAX_NODE_BYTES as u64).expect("nonzero"),
    };
    let (payload, _) = match store.get_blob_limited(digest, spec).await {
        Ok(v) => v,
        Err(e) => return Err(map_node_read_error(digest, e)),
    };
    if payload.len() > MAX_NODE_BYTES {
        return Err(CoreError::IntegrityError(format!(
            "stable index node {digest} exceeds {MAX_NODE_BYTES} bytes"
        ))
        .into());
    }
    let node: HamtNode = serde_json::from_slice(&payload).map_err(|e| {
        CoreError::IntegrityError(format!("stable index node {digest} is malformed: {e}"))
    })?;
    match &node {
        HamtNode::Branch {
            schema,
            depth: node_depth,
            prefix,
            children,
        } => {
            if schema != NODE_SCHEMA {
                return Err(CoreError::IntegrityError(format!(
                    "stable index node {digest} has schema {schema}"
                ))
                .into());
            }
            if *node_depth as usize != depth {
                return Err(CoreError::IntegrityError(format!(
                    "stable index branch {digest} claims depth {node_depth}, walk is at {depth}"
                ))
                .into());
            }
            if depth > MAX_DEPTH {
                return Err(CoreError::IntegrityError(format!(
                    "stable index branch {digest} sits past nibble 51"
                ))
                .into());
            }
            if prefix.len() != depth {
                return Err(CoreError::IntegrityError(format!(
                    "stable index branch {digest} prefix length {} disagrees with depth {depth}",
                    prefix.len()
                ))
                .into());
            }
            for (d, slot) in prefix.iter().enumerate() {
                let walked = nibble(walk_path, d)?;
                if *slot != walked {
                    return Err(CoreError::IntegrityError(format!(
                        "stable index branch {digest} is stored on the wrong path at depth {d}"
                    ))
                    .into());
                }
            }
            validate_slots(children, digest)?;
        }
        HamtNode::Leaf {
            schema,
            key,
            first,
            last,
            generation,
            ..
        } => {
            if schema != NODE_SCHEMA {
                return Err(CoreError::IntegrityError(format!(
                    "stable index node {digest} has schema {schema}"
                ))
                .into());
            }
            let leaf_path = key.path_digest(&store.key, &store.tenant, logical_log);
            // A leaf reached after `depth` branch hops must match the walked
            // path on those hops (0..depth). Later nibbles may differ: that is
            // a valid prefix-collision negative, not a swapped leaf.
            for d in 0..depth {
                let walked = nibble(walk_path, d)?;
                let own = nibble(&leaf_path, d)?;
                if walked != own {
                    return Err(CoreError::IntegrityError(format!(
                        "stable index leaf {digest} is stored on the wrong path at depth {d}"
                    ))
                    .into());
                }
            }
            if let Some(h) = head {
                validate_leaf_range(*first, *last, *generation, digest, h)?;
            }
        }
    }
    Ok(node)
}

fn map_node_read_error(digest: &Digest, e: anyhow::Error) -> anyhow::Error {
    match e.downcast::<CoreError>() {
        Ok(CoreError::NotFound(_)) => {
            CoreError::IntegrityError(format!("stable index node {digest} is missing")).into()
        }
        Ok(CoreError::BackendUnavailable(m)) => {
            CoreError::BackendUnavailable(format!("stable index node {digest}: {m}")).into()
        }
        Ok(CoreError::Io(io)) => {
            CoreError::BackendUnavailable(format!("stable index node {digest}: {io}")).into()
        }
        Ok(e @ (CoreError::IntegrityError(_) | CoreError::InvalidFormat(_))) => e.into(),
        Ok(other) => CoreError::RecoveryFailed(format!(
            "unclassified stable index node error {digest}: {other}"
        ))
        .into(),
        Err(e) => CoreError::IntegrityError(format!("stable index node {digest}: {e:#}")).into(),
    }
}

fn empty_branch() -> HamtNode {
    HamtNode::Branch {
        schema: NODE_SCHEMA.into(),
        depth: 0,
        prefix: Vec::new(),
        children: Vec::new(),
    }
}

fn path_prefix(path: &Digest, depth: usize) -> std::result::Result<Vec<u8>, CoreError> {
    (0..depth).map(|d| nibble(path, d)).collect()
}

fn branch_node(
    depth: usize,
    path: &Digest,
    children: Vec<HamtChild>,
) -> std::result::Result<HamtNode, CoreError> {
    if depth > MAX_DEPTH {
        return Err(CoreError::Rejected("stable key path hash collision".into()));
    }
    Ok(HamtNode::Branch {
        schema: NODE_SCHEMA.into(),
        depth: u8::try_from(depth)
            .map_err(|_| CoreError::Rejected("stable key path depth".into()))?,
        prefix: path_prefix(path, depth)?,
        children,
    })
}

fn check_root_shape(
    root: &StableIndexRoot,
    node: &HamtNode,
    digest: &Digest,
) -> std::result::Result<(), CoreError> {
    match (node, root.entries) {
        (
            HamtNode::Branch {
                depth: 0,
                prefix,
                children,
                ..
            },
            0,
        ) if prefix.is_empty() && children.is_empty() => Ok(()),
        (_, 0) => Err(CoreError::IntegrityError(format!(
            "stable index root {digest} claims zero entries but is not an empty depth-0 branch"
        ))),
        (HamtNode::Branch { children, .. }, n) if n > 0 && children.is_empty() => {
            Err(CoreError::IntegrityError(format!(
                "stable index root {digest} claims {n} entries but has no children"
            )))
        }
        _ => Ok(()),
    }
}

async fn put_node(store: &Store, node: &HamtNode) -> Result<Digest> {
    let bytes = serde_json::to_vec(node)?;
    if bytes.len() > MAX_NODE_BYTES {
        return Err(CoreError::Rejected(format!(
            "stable index node encodes to {} bytes (max {MAX_NODE_BYTES})",
            bytes.len()
        ))
        .into());
    }
    let (digest, _) = store.put_blob(bytes).await?;
    Ok(digest)
}

pub fn admissions_size(entries: &[StableIndexEntry]) -> Result<()> {
    if entries.len() > MAX_STABLE_ADMISSIONS {
        return Err(CoreError::Rejected(format!(
            "stable admissions {} exceed {MAX_STABLE_ADMISSIONS}",
            entries.len()
        ))
        .into());
    }
    let encoded = serde_json::to_vec(entries)?;
    if encoded.len() > MAX_STABLE_ADMISSION_BYTES {
        return Err(CoreError::Rejected(format!(
            "stable admissions encode to {} bytes (max {MAX_STABLE_ADMISSION_BYTES})",
            encoded.len()
        ))
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;
    use comb_core::{DigestKey, StableKey};
    use comb_object::failpoint::FailpointBackend;
    use comb_object::memory::MemoryBackend;
    use std::sync::Arc;

    fn store() -> Store {
        Store::new(
            Arc::new(MemoryBackend::new()),
            "org_t",
            DigestKey::from_bytes([9u8; 32]),
            None,
        )
    }

    fn key(label: &str) -> StableKey {
        StableKey::try_from_canonical(label.as_bytes().to_vec()).unwrap()
    }

    fn hash(store: &Store, payload: &[u8]) -> Digest {
        StableKey::payload_hash(&store.key, payload)
    }

    fn entry(
        store: &Store,
        label: &str,
        first: u64,
        last: u64,
        generation: u64,
    ) -> StableIndexEntry {
        let k = key(label);
        StableIndexEntry {
            payload_hash: hash(store, label.as_bytes()),
            key: k,
            first,
            last,
            generation,
        }
    }

    #[test]
    fn depth_boundary_rejects_past_256_bits() {
        let path = Digest::parse(&format!("b3k:{}", "ab".repeat(32))).unwrap();
        assert!(nibble(&path, MAX_DEPTH).is_ok());
        assert!(nibble(&path, MAX_DEPTH + 1).is_err());
    }

    #[tokio::test]
    async fn insert_and_lookup_roundtrip() {
        let store = store();
        let mut root = empty_root(&store).await.unwrap();
        let e = entry(&store, "doc-a", 1, 1, 1);
        root = insert(
            &store,
            &root,
            e.clone(),
            "feed",
            IndexHead {
                generation: 1,
                head_seq: 1,
            },
        )
        .await
        .unwrap();
        let found = lookup(
            &store,
            &root,
            &e.key,
            "feed",
            IndexHead {
                generation: 1,
                head_seq: 1,
            },
        )
        .await
        .unwrap();
        match found {
            Lookup::Found(got) => {
                assert_eq!(got.first, 1);
                assert_eq!(got.payload_hash, e.payload_hash);
            }
            Lookup::Absent => panic!("expected hit"),
        }
        let missing = lookup(
            &store,
            &root,
            &key("doc-b"),
            "feed",
            IndexHead {
                generation: 1,
                head_seq: 1,
            },
        )
        .await
        .unwrap();
        assert!(matches!(missing, Lookup::Absent));
    }

    #[tokio::test]
    async fn missing_node_is_integrity() {
        let store = store();
        let bogus = store.put_blob(b"unused".to_vec()).await.unwrap().0;
        let root = StableIndexRoot::new(bogus, 1);
        let err = lookup(
            &store,
            &root,
            &key("x"),
            "feed",
            IndexHead {
                generation: 1,
                head_seq: 1,
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<CoreError>(),
            Some(CoreError::IntegrityError(_))
        ));
    }

    #[tokio::test]
    async fn swapped_leaf_is_integrity_not_absent() {
        let store = store();
        let a = entry(&store, "key-a", 1, 1, 1);
        let a_path = a.key.path_digest(&store.key, &store.tenant, "feed");
        let a0 = nibble(&a_path, 0).unwrap();
        let mut b = entry(&store, "key-b", 1, 1, 1);
        for label in ["key-b", "key-c", "key-d", "other", "zzzz"] {
            let candidate = entry(&store, label, 1, 1, 1);
            let p = candidate.key.path_digest(&store.key, &store.tenant, "feed");
            if nibble(&p, 0).unwrap() != a0 {
                b = candidate;
                break;
            }
        }
        let leaf_b = put_node(&store, &leaf_node(&b)).await.unwrap();
        let slot = a0;
        let mut children = Vec::new();
        upsert_child(&mut children, slot, leaf_b);
        let a_path = a.key.path_digest(&store.key, &store.tenant, "feed");
        let branch = put_node(&store, &branch_node(0, &a_path, children).unwrap())
            .await
            .unwrap();
        let root = StableIndexRoot::new(branch, 1);
        let err = lookup(
            &store,
            &root,
            &a.key,
            "feed",
            IndexHead {
                generation: 1,
                head_seq: 1,
            },
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<CoreError>(),
                Some(CoreError::IntegrityError(m)) if m.contains("wrong path")
            ),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn invalid_slots_unknown_schema_and_bad_range() {
        let store = store();
        let e = entry(&store, "k", 1, 1, 1);
        let leaf = put_node(&store, &leaf_node(&e)).await.unwrap();

        let bad_slots = HamtNode::Branch {
            schema: NODE_SCHEMA.into(),
            depth: 0,
            prefix: Vec::new(),
            children: vec![
                HamtChild {
                    slot: 5,
                    digest: leaf.clone(),
                },
                HamtChild {
                    slot: 5,
                    digest: leaf.clone(),
                },
            ],
        };
        let d = put_node(&store, &bad_slots).await.unwrap();
        let err = lookup(
            &store,
            &StableIndexRoot::new(d, 1),
            &e.key,
            "feed",
            IndexHead {
                generation: 1,
                head_seq: 1,
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<CoreError>(),
            Some(CoreError::IntegrityError(_))
        ));

        let unknown = HamtNode::Branch {
            schema: "comb.log.stable-index-node/v0".into(),
            depth: 0,
            prefix: Vec::new(),
            children: Vec::new(),
        };
        let d = put_node(&store, &unknown).await.unwrap();
        let err = lookup(
            &store,
            &StableIndexRoot::new(d, 0),
            &e.key,
            "feed",
            IndexHead {
                generation: 1,
                head_seq: 1,
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<CoreError>(),
            Some(CoreError::IntegrityError(_))
        ));

        let mut root = empty_root(&store).await.unwrap();
        root = insert(
            &store,
            &root,
            e.clone(),
            "feed",
            IndexHead {
                generation: 1,
                head_seq: 1,
            },
        )
        .await
        .unwrap();
        let err = lookup(
            &store,
            &root,
            &e.key,
            "feed",
            IndexHead {
                generation: 1,
                head_seq: 0,
            },
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<CoreError>(),
                Some(CoreError::IntegrityError(m)) if m.contains("head_seq")
            ),
            "{err:#}"
        );

        let root_bad = StableIndexRoot {
            schema: "nope".into(),
            digest: root.digest,
            entries: 1,
        };
        let err = lookup(
            &store,
            &root_bad,
            &e.key,
            "feed",
            IndexHead {
                generation: 1,
                head_seq: 1,
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<CoreError>(),
            Some(CoreError::IntegrityError(_))
        ));
    }

    #[tokio::test]
    async fn malformed_body_is_integrity() {
        let store = store();
        let digest = store.put_blob(b"not-a-node".to_vec()).await.unwrap().0;
        let err = lookup(
            &store,
            &StableIndexRoot::new(digest, 1),
            &key("x"),
            "feed",
            IndexHead {
                generation: 1,
                head_seq: 1,
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<CoreError>(),
            Some(CoreError::IntegrityError(_))
        ));
    }

    fn head(seq: u64) -> IndexHead {
        IndexHead {
            generation: 1,
            head_seq: seq,
        }
    }

    fn pair_sharing_nibble0(store: &Store) -> (StableIndexEntry, StableIndexEntry) {
        let a = entry(store, "key-a", 1, 1, 1);
        let a0 = nibble(&a.key.path_digest(&store.key, &store.tenant, "feed"), 0).unwrap();
        for label in [
            "key-b", "key-c", "key-d", "key-e", "doc-1", "doc-2", "zzzz", "aaaa", "same",
        ] {
            let b = entry(store, label, 1, 1, 1);
            let b0 = nibble(&b.key.path_digest(&store.key, &store.tenant, "feed"), 0).unwrap();
            if b0 == a0 && b.key != a.key {
                return (a, b);
            }
        }
        panic!("could not find a second key sharing nibble 0");
    }

    #[tokio::test]
    async fn io_on_node_read_is_unavailable() {
        let mem = Arc::new(MemoryBackend::new());
        let store = Store::new(mem.clone(), "org_t", DigestKey::from_bytes([9u8; 32]), None);
        let mut root = empty_root(&store).await.unwrap();
        let e = entry(&store, "doc-a", 1, 1, 1);
        root = insert(&store, &root, e.clone(), "feed", head(1))
            .await
            .unwrap();
        let io_store = Store::new(
            Arc::new(FailpointBackend::io_on_next_get(mem, "/objects/")),
            "org_t",
            DigestKey::from_bytes([9u8; 32]),
            None,
        );
        let err = lookup(&io_store, &root, &e.key, "feed", head(1))
            .await
            .unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<CoreError>(),
                Some(CoreError::BackendUnavailable(_))
            ),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn grafted_branch_is_integrity_not_absent() {
        let store = store();
        let a = entry(&store, "key-a", 1, 1, 1);
        let a_path = a.key.path_digest(&store.key, &store.tenant, "feed");
        let a0 = nibble(&a_path, 0).unwrap();
        let mut b = entry(&store, "key-b", 1, 1, 1);
        for label in ["key-b", "key-c", "key-d", "other", "zzzz", "qqqq"] {
            let candidate = entry(&store, label, 1, 1, 1);
            let p = candidate.key.path_digest(&store.key, &store.tenant, "feed");
            if nibble(&p, 0).unwrap() != a0 {
                b = candidate;
                break;
            }
        }
        let b_path = b.key.path_digest(&store.key, &store.tenant, "feed");
        let b0 = nibble(&b_path, 0).unwrap();
        assert_ne!(a0, b0);
        let leaf_b = put_node(&store, &leaf_node(&b)).await.unwrap();
        let mut inner = Vec::new();
        upsert_child(&mut inner, nibble(&b_path, 1).unwrap(), leaf_b);
        let grafted = put_node(&store, &branch_node(1, &b_path, inner).unwrap())
            .await
            .unwrap();
        let mut children = Vec::new();
        upsert_child(&mut children, a0, grafted);
        let root_d = put_node(&store, &branch_node(0, &a_path, children).unwrap())
            .await
            .unwrap();
        let err = lookup(
            &store,
            &StableIndexRoot::new(root_d, 1),
            &a.key,
            "feed",
            head(1),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<CoreError>(),
                Some(CoreError::IntegrityError(m)) if m.contains("wrong path")
            ),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn insert_refuses_to_copy_an_invalid_sibling_leaf() {
        let store = store();
        let (a, b) = pair_sharing_nibble0(&store);
        let a_path = a.key.path_digest(&store.key, &store.tenant, "feed");
        let mut bad = a.clone();
        bad.last = 99;
        let leaf = put_node(&store, &leaf_node(&bad)).await.unwrap();
        let mut children = Vec::new();
        upsert_child(&mut children, nibble(&a_path, 0).unwrap(), leaf);
        let root_d = put_node(&store, &branch_node(0, &a_path, children).unwrap())
            .await
            .unwrap();
        let root = StableIndexRoot::new(root_d, 1);
        let err = insert(&store, &root, b, "feed", head(2)).await.unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<CoreError>(),
                Some(CoreError::IntegrityError(m)) if m.contains("head_seq")
            ),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn fully_prefixed_leaf_at_depth_52_is_accepted() {
        let store = store();
        let e = entry(&store, "deep", 1, 1, 1);
        let path = e.key.path_digest(&store.key, &store.tenant, "feed");
        let mut digest = put_node(&store, &leaf_node(&e)).await.unwrap();
        for depth in (0..MAX_LEAF_DEPTH).rev() {
            let mut children = Vec::new();
            upsert_child(&mut children, nibble(&path, depth).unwrap(), digest);
            digest = put_node(&store, &branch_node(depth, &path, children).unwrap())
                .await
                .unwrap();
        }
        let found = lookup(
            &store,
            &StableIndexRoot::new(digest, 1),
            &e.key,
            "feed",
            head(1),
        )
        .await
        .unwrap();
        assert!(matches!(found, Lookup::Found(_)));
    }

    #[tokio::test]
    async fn empty_root_count_disagreement_is_integrity() {
        let store = store();
        let empty = empty_root(&store).await.unwrap();
        let err = lookup(
            &store,
            &StableIndexRoot::new(empty.digest.clone(), 4),
            &key("x"),
            "feed",
            head(1),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<CoreError>(),
                Some(CoreError::IntegrityError(m)) if m.contains("no children")
            ),
            "{err:#}"
        );
    }
}
