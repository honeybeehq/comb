//! Bounded HAMT for complete-feed stable keys.
//!
//! 5-bit fan-out, depth at most 51 (52 nodes on a 256-bit path; the last
//! nibble uses 1 live bit and 4 zero pads). Each encoded node is <= 4 KiB.
//! The index root lives in the log manifest; nodes are create-only blobs.
//!
//! Missing child on a verified path is a valid negative. Missing required
//! node or malformed required evidence is Integrity/Unavailable, never Absent.

use crate::store::Store;
use anyhow::Result;
use comb_core::error::CoreError;
use comb_core::{Digest, StableKey};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;

pub const NODE_SCHEMA: &str = "comb.log.stable-index-node/v1";
pub const ROOT_SCHEMA: &str = "comb.log.stable-index-root/v1";
pub const MAX_NODE_BYTES: usize = 4 * 1024;
pub const MAX_STABLE_ADMISSIONS: usize = 1_024;
pub const MAX_STABLE_ADMISSION_BYTES: usize = 256 * 1024;
const FANOUT_BITS: u32 = 5;
const FANOUT: u8 = 32;
/// Last nibble index. A branch may sit at depths 0..=51.
pub const MAX_BRANCH_DEPTH: usize = 51;
/// Leaf under nibble 51 is at walk depth 52.
pub const MAX_LEAF_DEPTH: usize = 52;
const PATH_BITS: usize = 256;

#[derive(Debug, Clone, Copy)]
pub struct IndexHead {
    pub generation: u64,
    pub head_seq: u64,
}

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

    fn validate(&self) -> Result<(), CoreError> {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum HamtNode {
    Branch {
        schema: String,
        depth: u8,
        prefix: Digest,
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
    let digest = put_node(
        store,
        &branch_node(0, &Digest::from_raw([0u8; 32]), Vec::new()),
    )
    .await?;
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
    lookup_at(store, root, key, &path, logical_log, head).await
}

fn lookup_at<'a>(
    store: &'a Store,
    root: &'a StableIndexRoot,
    key: &'a StableKey,
    path: &'a Digest,
    logical_log: &'a str,
    head: IndexHead,
) -> Pin<Box<dyn Future<Output = Result<Lookup>> + Send + 'a>> {
    Box::pin(async move {
        let mut digest = root.digest.clone();
        let mut depth = 0usize;
        loop {
            if depth > MAX_LEAF_DEPTH {
                return Err(CoreError::IntegrityError(
                    "stable index exceeds maximum path depth".into(),
                )
                .into());
            }
            let node = load_node(store, &digest, path, depth, Some(head), logical_log).await?;
            if depth == 0 {
                match &node {
                    HamtNode::Branch { children, .. } => {
                        if root.entries == 0 && !children.is_empty() {
                            return Err(CoreError::IntegrityError(
                                "stable index root count is 0 but the root branch has children"
                                    .into(),
                            )
                            .into());
                        }
                    }
                    HamtNode::Leaf { .. } => {
                        return Err(CoreError::IntegrityError(
                            "stable index root digest must address a branch".into(),
                        )
                        .into());
                    }
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
                        if root.entries == 0 {
                            return Err(CoreError::IntegrityError(
                                "stable index root count is 0 but a leaf was found".into(),
                            )
                            .into());
                        }
                        return Ok(Lookup::Found(StableIndexEntry {
                            key: existing,
                            payload_hash,
                            first,
                            last,
                            generation,
                        }));
                    }
                    return Ok(Lookup::Absent);
                }
                HamtNode::Branch { children, .. } => {
                    if depth > MAX_BRANCH_DEPTH {
                        return Err(CoreError::IntegrityError(
                            "stable index branch exceeds nibble depth".into(),
                        )
                        .into());
                    }
                    let slot = nibble(path, depth)?;
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
    })
}

pub async fn insert(
    store: &Store,
    root: &StableIndexRoot,
    entry: StableIndexEntry,
    logical_log: &str,
    head: IndexHead,
) -> Result<StableIndexRoot> {
    root.validate()?;
    validate_receipt(
        entry.first,
        entry.last,
        entry.generation,
        entry.generation,
        entry.last,
    )?;
    let path = entry
        .key
        .path_digest(&store.key, &store.tenant, logical_log);
    let new_digest = insert_at(
        store,
        Some(root.digest.clone()),
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

fn insert_at<'a>(
    store: &'a Store,
    current: Option<Digest>,
    entry: &'a StableIndexEntry,
    path: &'a Digest,
    logical_log: &'a str,
    depth: usize,
    head: IndexHead,
) -> Pin<Box<dyn Future<Output = Result<Digest>> + Send + 'a>> {
    Box::pin(async move {
        if depth > MAX_LEAF_DEPTH {
            return Err(CoreError::Rejected("stable key path hash collision".into()).into());
        }
        let node = match &current {
            Some(d) => load_node(store, d, path, depth, Some(head), logical_log).await?,
            None => branch_node(depth, path, Vec::new()),
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
                if depth >= MAX_LEAF_DEPTH {
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
                split(store, &old, &old_path, entry, path, depth, head).await
            }
            HamtNode::Branch { mut children, .. } => {
                if depth > MAX_BRANCH_DEPTH {
                    return Err(CoreError::Rejected("stable key path hash collision".into()).into());
                }
                let slot = nibble(path, depth)?;
                match child(&children, slot).cloned() {
                    None => {
                        let leaf = put_node(store, &leaf_node(entry)).await?;
                        upsert_child(&mut children, slot, leaf);
                        put_node(store, &branch_node(depth, path, children)).await
                    }
                    Some(next) => {
                        let updated =
                            insert_at(store, Some(next), entry, path, logical_log, depth + 1, head)
                                .await?;
                        upsert_child(&mut children, slot, updated);
                        put_node(store, &branch_node(depth, path, children)).await
                    }
                }
            }
        }
    })
}

fn split<'a>(
    store: &'a Store,
    a: &'a StableIndexEntry,
    a_path: &'a Digest,
    b: &'a StableIndexEntry,
    b_path: &'a Digest,
    depth: usize,
    head: IndexHead,
) -> Pin<Box<dyn Future<Output = Result<Digest>> + Send + 'a>> {
    Box::pin(async move {
        if depth > MAX_BRANCH_DEPTH {
            return Err(CoreError::Rejected("stable key path hash collision".into()).into());
        }
        validate_receipt(
            a.first,
            a.last,
            a.generation,
            head.generation,
            head.head_seq,
        )?;
        validate_receipt(b.first, b.last, b.generation, b.generation, b.last)?;
        let sa = nibble(a_path, depth)?;
        let sb = nibble(b_path, depth)?;
        if sa != sb {
            let da = put_node(store, &leaf_node(a)).await?;
            let db = put_node(store, &leaf_node(b)).await?;
            let mut children = Vec::new();
            upsert_child(&mut children, sa, da);
            upsert_child(&mut children, sb, db);
            return put_node(store, &branch_node(depth, a_path, children)).await;
        }
        let child = split(store, a, a_path, b, b_path, depth + 1, head).await?;
        let mut children = Vec::new();
        upsert_child(&mut children, sa, child);
        put_node(store, &branch_node(depth, a_path, children)).await
    })
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

fn branch_node(depth: usize, path: &Digest, children: Vec<HamtChild>) -> HamtNode {
    HamtNode::Branch {
        schema: NODE_SCHEMA.into(),
        depth: depth as u8,
        prefix: masked_path(path, depth),
        children,
    }
}

fn masked_path(path: &Digest, depth: usize) -> Digest {
    let mut raw = path.raw();
    let keep_bits = depth.saturating_mul(FANOUT_BITS as usize).min(PATH_BITS);
    for bit in keep_bits..PATH_BITS {
        let byte = bit / 8;
        let mask = 1u8 << (7 - (bit % 8));
        raw[byte] &= !mask;
    }
    Digest::from_raw(raw)
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

fn nibble(path: &Digest, depth: usize) -> Result<u8, CoreError> {
    if depth > MAX_BRANCH_DEPTH {
        return Err(CoreError::Rejected("stable key path depth".into()));
    }
    let raw = path.raw();
    let bit = depth * FANOUT_BITS as usize;
    let mut acc = 0u8;
    for i in 0..5 {
        let b = bit + i;
        let byte = if b / 8 < 32 { raw[b / 8] } else { 0 };
        let bitv = (byte >> (7 - (b % 8))) & 1;
        acc = (acc << 1) | bitv;
    }
    Ok(acc)
}

fn slot_mask(depth: usize) -> u8 {
    let bit = depth * FANOUT_BITS as usize;
    let mut mask = 0u8;
    for i in 0..5 {
        if bit + i < PATH_BITS {
            mask |= 1 << (4 - i);
        }
    }
    mask
}

fn path_prefix_matches(
    leaf_path: &Digest,
    search_path: &Digest,
    depth: usize,
) -> Result<bool, CoreError> {
    for d in 0..depth {
        if nibble(leaf_path, d)? != nibble(search_path, d)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn validate_receipt(
    first: u64,
    last: u64,
    generation: u64,
    head_generation: u64,
    head_seq: u64,
) -> Result<(), CoreError> {
    if first == 0 || first > last {
        return Err(CoreError::IntegrityError(format!(
            "stable index leaf range {first}..={last} is invalid"
        )));
    }
    if generation == 0 {
        return Err(CoreError::IntegrityError(
            "stable index leaf generation must be > 0".into(),
        ));
    }
    if generation > head_generation {
        return Err(CoreError::IntegrityError(format!(
            "stable index leaf generation {generation} is ahead of head {head_generation}"
        )));
    }
    if last > head_seq {
        return Err(CoreError::IntegrityError(format!(
            "stable index leaf last {last} is ahead of head_seq {head_seq}"
        )));
    }
    Ok(())
}

fn validate_branch_slots(children: &[HamtChild], depth: usize) -> Result<(), CoreError> {
    let mask = slot_mask(depth);
    let mut prev: Option<u8> = None;
    for c in children {
        if c.slot >= FANOUT {
            return Err(CoreError::IntegrityError(format!(
                "stable index slot {} exceeds fanout {FANOUT}",
                c.slot
            )));
        }
        if c.slot & !mask != 0 {
            return Err(CoreError::IntegrityError(format!(
                "stable index slot {} is not valid at depth {depth}",
                c.slot
            )));
        }
        if let Some(p) = prev {
            if c.slot <= p {
                return Err(CoreError::IntegrityError(
                    "stable index branch slots must be unique and strictly increasing".into(),
                ));
            }
        }
        prev = Some(c.slot);
    }
    Ok(())
}

fn validate_node(
    digest: &Digest,
    node: &HamtNode,
    path: &Digest,
    depth: usize,
) -> Result<(), CoreError> {
    match node {
        HamtNode::Branch {
            schema,
            depth: node_depth,
            prefix,
            children,
        } => {
            if schema != NODE_SCHEMA {
                return Err(CoreError::IntegrityError(format!(
                    "unsupported stable index node schema {schema} at {digest}"
                )));
            }
            if *node_depth as usize != depth {
                return Err(CoreError::IntegrityError(format!(
                    "stable index branch depth {node_depth} does not match walk {depth}"
                )));
            }
            if prefix != &masked_path(path, depth) {
                return Err(CoreError::IntegrityError(
                    "stable index branch is not bound to the path that reached it".into(),
                ));
            }
            validate_branch_slots(children, depth)?;
            Ok(())
        }
        HamtNode::Leaf {
            schema,
            first,
            last,
            generation,
            ..
        } => {
            if schema != NODE_SCHEMA {
                return Err(CoreError::IntegrityError(format!(
                    "unsupported stable index node schema {schema} at {digest}"
                )));
            }
            if *first == 0 || first > last || *generation == 0 {
                return Err(CoreError::IntegrityError(format!(
                    "stable index leaf {digest} has an invalid range or generation"
                )));
            }
            Ok(())
        }
    }
}

async fn load_node(
    store: &Store,
    digest: &Digest,
    path: &Digest,
    depth: usize,
    head: Option<IndexHead>,
    logical_log: &str,
) -> Result<HamtNode> {
    let (payload, _) = match store.get_blob(digest).await {
        Ok(v) => v,
        Err(e) => return Err(map_node_load_err(digest, e)),
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
    validate_node(digest, &node, path, depth)?;
    if let HamtNode::Leaf {
        key,
        first,
        last,
        generation,
        ..
    } = &node
    {
        let leaf_path = key.path_digest(&store.key, &store.tenant, logical_log);
        if !path_prefix_matches(&leaf_path, path, depth)? {
            return Err(CoreError::IntegrityError(
                "stable index leaf is not on the path that reached it".into(),
            )
            .into());
        }
        if let Some(head) = head {
            validate_receipt(*first, *last, *generation, head.generation, head.head_seq)?;
        }
    }
    Ok(node)
}

fn map_node_load_err(digest: &Digest, e: anyhow::Error) -> anyhow::Error {
    match e.downcast_ref::<CoreError>() {
        Some(CoreError::BackendUnavailable(_)) => e,
        Some(CoreError::Io(_)) => {
            CoreError::BackendUnavailable(format!("stable index node {digest}: {e:#}")).into()
        }
        Some(CoreError::NotFound(_)) => {
            CoreError::IntegrityError(format!("stable index node {digest} is missing")).into()
        }
        Some(CoreError::IntegrityError(_) | CoreError::InvalidFormat(_)) => {
            CoreError::IntegrityError(format!("stable index node {digest}: {e:#}")).into()
        }
        Some(_) => CoreError::RecoveryFailed(format!(
            "unclassified error loading stable index node {digest}: {e:#}"
        ))
        .into(),
        None => CoreError::RecoveryFailed(format!(
            "unclassified error loading stable index node {digest}: {e:#}"
        ))
        .into(),
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
    use comb_core::DigestKey;
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

    fn hash(store: &Store) -> Digest {
        store.key.digest(b"payload")
    }

    fn entry(
        store: &Store,
        label: &str,
        first: u64,
        last: u64,
        generation: u64,
    ) -> StableIndexEntry {
        StableIndexEntry {
            key: key(label),
            payload_hash: hash(store),
            first,
            last,
            generation,
        }
    }

    fn head(generation: u64, head_seq: u64) -> IndexHead {
        IndexHead {
            generation,
            head_seq,
        }
    }

    #[test]
    fn depth_boundary_is_51() {
        let d = DigestKey::from_bytes([1u8; 32]).digest(b"path");
        assert!(nibble(&d, MAX_BRANCH_DEPTH).is_ok());
        assert!(nibble(&d, MAX_BRANCH_DEPTH + 1).is_err());
        assert_eq!(slot_mask(MAX_BRANCH_DEPTH), 0b10000);
        assert_eq!(slot_mask(0), 0b11111);
    }

    #[tokio::test]
    async fn missing_child_on_verified_path_is_absent() {
        let store = store();
        let root = empty_root(&store).await.unwrap();
        let found = lookup(&store, &root, &key("a"), "feed", head(0, 0))
            .await
            .unwrap();
        assert!(matches!(found, Lookup::Absent));
    }

    #[tokio::test]
    async fn missing_required_node_is_integrity_not_absent() {
        let store = store();
        let missing = Digest::parse(&format!("b3k:{}", "ab".repeat(32))).unwrap();
        let k = key("a");
        let path = k.path_digest(&store.key, &store.tenant, "feed");
        let slot = nibble(&path, 0).unwrap();
        let digest = put_node(
            &store,
            &branch_node(
                0,
                &path,
                vec![HamtChild {
                    slot,
                    digest: missing,
                }],
            ),
        )
        .await
        .unwrap();
        let root = StableIndexRoot::new(digest, 1);
        let err = lookup(&store, &root, &k, "feed", head(1, 1))
            .await
            .unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<CoreError>(),
                Some(CoreError::IntegrityError(_))
            ),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn wrong_path_leaf_is_integrity_not_absent() {
        let store = store();
        let mut a = entry(&store, "alpha", 1, 1, 1);
        let mut b = entry(&store, "beta", 1, 1, 1);
        let mut slot_a = 0u8;
        let mut slot_b = 0u8;
        for i in 0..256u16 {
            a = entry(&store, &format!("a{i}"), 1, 1, 1);
            b = entry(&store, &format!("b{i}"), 1, 1, 1);
            let path_a = a.key.path_digest(&store.key, &store.tenant, "feed");
            let path_b = b.key.path_digest(&store.key, &store.tenant, "feed");
            slot_a = nibble(&path_a, 0).unwrap();
            slot_b = nibble(&path_b, 0).unwrap();
            if slot_a != slot_b {
                break;
            }
        }
        assert_ne!(slot_a, slot_b, "test needs distinct first nibbles");
        let leaf_b = put_node(&store, &leaf_node(&b)).await.unwrap();
        let path_a = a.key.path_digest(&store.key, &store.tenant, "feed");
        let digest = put_node(
            &store,
            &branch_node(
                0,
                &path_a,
                vec![HamtChild {
                    slot: slot_a,
                    digest: leaf_b,
                }],
            ),
        )
        .await
        .unwrap();
        let root = StableIndexRoot::new(digest, 1);
        let err = lookup(&store, &root, &a.key, "feed", head(1, 1))
            .await
            .unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<CoreError>(),
                Some(CoreError::IntegrityError(_))
            ),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn invalid_slots_unknown_schema_and_bad_range_fail_closed() {
        let store = store();
        let k = key("a");
        let path = k.path_digest(&store.key, &store.tenant, "feed");
        let slot = nibble(&path, 0).unwrap();
        let leaf = put_node(
            &store,
            &HamtNode::Leaf {
                schema: NODE_SCHEMA.into(),
                key: k.clone(),
                payload_hash: hash(&store),
                first: 5,
                last: 3,
                generation: 1,
            },
        )
        .await
        .unwrap();
        let digest = put_node(
            &store,
            &branch_node(0, &path, vec![HamtChild { slot, digest: leaf }]),
        )
        .await
        .unwrap();
        let root = StableIndexRoot::new(digest, 1);
        let err = lookup(&store, &root, &k, "feed", head(10, 10))
            .await
            .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<CoreError>(),
            Some(CoreError::IntegrityError(_))
        ));

        let unsorted = put_node(
            &store,
            &branch_node(
                0,
                &path,
                vec![
                    HamtChild {
                        slot: 3,
                        digest: hash(&store),
                    },
                    HamtChild {
                        slot: 1,
                        digest: hash(&store),
                    },
                ],
            ),
        )
        .await
        .unwrap();
        let root = StableIndexRoot::new(unsorted, 1);
        let err = lookup(&store, &root, &k, "feed", head(1, 1))
            .await
            .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<CoreError>(),
            Some(CoreError::IntegrityError(_))
        ));

        let wide = put_node(
            &store,
            &branch_node(
                0,
                &path,
                vec![HamtChild {
                    slot: 32,
                    digest: hash(&store),
                }],
            ),
        )
        .await
        .unwrap();
        let root = StableIndexRoot::new(wide, 1);
        let err = lookup(&store, &root, &k, "feed", head(1, 1))
            .await
            .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<CoreError>(),
            Some(CoreError::IntegrityError(_))
        ));

        let unknown = put_node(
            &store,
            &HamtNode::Branch {
                schema: "comb.log.stable-index-node/v0".into(),
                depth: 0,
                prefix: masked_path(&path, 0),
                children: Vec::new(),
            },
        )
        .await
        .unwrap();
        let mut root = StableIndexRoot::new(unknown, 0);
        let err = lookup(&store, &root, &k, "feed", head(0, 0))
            .await
            .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<CoreError>(),
            Some(CoreError::IntegrityError(_))
        ));
        root.schema = "nope".into();
        let err = lookup(&store, &root, &k, "feed", head(0, 0))
            .await
            .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<CoreError>(),
            Some(CoreError::IntegrityError(_))
        ));
    }

    #[tokio::test]
    async fn receipt_ahead_of_head_is_integrity() {
        let store = store();
        let mut root = empty_root(&store).await.unwrap();
        let e = entry(&store, "a", 1, 4, 2);
        root = insert(&store, &root, e, "feed", head(2, 4)).await.unwrap();
        let err = lookup(&store, &root, &key("a"), "feed", head(1, 2))
            .await
            .unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<CoreError>(),
                Some(CoreError::IntegrityError(_))
            ),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn roundtrip_lookup_finds_inserted_key() {
        let store = store();
        let mut root = empty_root(&store).await.unwrap();
        let e = entry(&store, "doc", 1, 1, 1);
        root = insert(&store, &root, e.clone(), "feed", head(1, 1))
            .await
            .unwrap();
        match lookup(&store, &root, &e.key, "feed", head(1, 1))
            .await
            .unwrap()
        {
            Lookup::Found(got) => {
                assert_eq!(got.first, 1);
                assert_eq!(got.generation, 1);
            }
            Lookup::Absent => panic!("expected found"),
        }
        assert!(matches!(
            lookup(&store, &root, &key("other"), "feed", head(1, 1))
                .await
                .unwrap(),
            Lookup::Absent
        ));
    }

    #[test]
    fn io_is_unavailable_not_integrity() {
        let d = Digest::from_raw([0u8; 32]);
        let e = anyhow::Error::from(CoreError::Io(std::io::Error::other("eio")));
        let out = map_node_load_err(&d, e);
        assert!(
            matches!(
                out.downcast_ref::<CoreError>(),
                Some(CoreError::BackendUnavailable(_))
            ),
            "{out:#}"
        );
    }

    #[tokio::test]
    async fn grafted_branch_is_integrity_not_absent() {
        let store = store();
        let mut a = entry(&store, "alpha", 1, 1, 1);
        let mut b = entry(&store, "beta", 1, 1, 1);
        let mut path_a = a.key.path_digest(&store.key, &store.tenant, "feed");
        let mut path_b = b.key.path_digest(&store.key, &store.tenant, "feed");
        for i in 0..256u16 {
            path_a = a.key.path_digest(&store.key, &store.tenant, "feed");
            path_b = b.key.path_digest(&store.key, &store.tenant, "feed");
            if nibble(&path_a, 0).unwrap() != nibble(&path_b, 0).unwrap() {
                break;
            }
            a = entry(&store, &format!("ga{i}"), 1, 1, 1);
            b = entry(&store, &format!("gb{i}"), 1, 1, 1);
        }
        let grafted = put_node(&store, &branch_node(1, &path_b, Vec::new()))
            .await
            .unwrap();
        let digest = put_node(
            &store,
            &branch_node(
                0,
                &path_a,
                vec![HamtChild {
                    slot: nibble(&path_a, 0).unwrap(),
                    digest: grafted,
                }],
            ),
        )
        .await
        .unwrap();
        let root = StableIndexRoot::new(digest, 1);
        let err = lookup(&store, &root, &a.key, "feed", head(1, 1))
            .await
            .unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<CoreError>(),
                Some(CoreError::IntegrityError(_))
            ),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn path_copy_validates_existing_leaf_range() {
        let store = store();
        let mut a = entry(&store, "share-a", 1, 1, 1);
        let mut b = entry(&store, "share-b", 1, 9, 1);
        for i in 0..64u16 {
            a = entry(&store, &format!("sa{i}"), 1, 1, 1);
            b = entry(&store, &format!("sb{i}"), 1, 9, 1);
            let pa = a.key.path_digest(&store.key, &store.tenant, "feed");
            let pb = b.key.path_digest(&store.key, &store.tenant, "feed");
            if nibble(&pa, 0).unwrap() == nibble(&pb, 0).unwrap() {
                break;
            }
        }
        let bad = put_node(&store, &leaf_node(&b)).await.unwrap();
        let pa = a.key.path_digest(&store.key, &store.tenant, "feed");
        let slot = nibble(&pa, 0).unwrap();
        let digest = put_node(
            &store,
            &branch_node(0, &pa, vec![HamtChild { slot, digest: bad }]),
        )
        .await
        .unwrap();
        let root = StableIndexRoot::new(digest, 1);
        let err = insert(&store, &root, a, "feed", head(1, 1))
            .await
            .unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<CoreError>(),
                Some(CoreError::IntegrityError(_))
            ),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn constructed_path_leaf_at_depth_52() {
        let store = store();
        let e = entry(&store, "deep", 1, 1, 1);
        let path = e.key.path_digest(&store.key, &store.tenant, "feed");
        let mut child = put_node(&store, &leaf_node(&e)).await.unwrap();
        for d in (0..=MAX_BRANCH_DEPTH).rev() {
            let slot = nibble(&path, d).unwrap();
            child = put_node(
                &store,
                &branch_node(
                    d,
                    &path,
                    vec![HamtChild {
                        slot,
                        digest: child,
                    }],
                ),
            )
            .await
            .unwrap();
        }
        let root = StableIndexRoot::new(child, 1);
        match lookup(&store, &root, &e.key, "feed", head(1, 1))
            .await
            .unwrap()
        {
            Lookup::Found(got) => assert_eq!(got.generation, 1),
            Lookup::Absent => panic!("depth-52 leaf must be reachable"),
        }
    }
}
