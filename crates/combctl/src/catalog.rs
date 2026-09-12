//! Immutable append-oriented chunk catalog (B+tree). Internal to Log.

use crate::store::Store;
use anyhow::Result;
use comb_core::error::CoreError;
use comb_core::{Digest, EnvelopeReadSpec, ObjectKind};
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;

pub const MAX_CATALOG_NODE_OBJECT_BYTES: u64 = 16 * 1024;
pub const MAX_CATALOG_ITEMS: usize = 32;
pub const MAX_CATALOG_HEIGHT: u8 = 8;
pub const CATALOG_NODE_SCHEMA: &str = "comb.log.chunk-catalog-node/v1";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CatalogSchemaV1 {
    #[serde(rename = "comb.log.chunk-catalog-node/v1")]
    V1,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogChunkRef {
    pub digest: Digest,
    pub first_seq: u64,
    pub last_seq: u64,
    pub event_count: u32,
    pub raw_payload_bytes: u64,
    pub plaintext_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogChild {
    pub first_seq: u64,
    pub last_seq: u64,
    pub chunk_count: u64,
    pub digest: Digest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkCatalogRoot {
    pub schema: CatalogSchemaV1,
    pub digest: Digest,
    pub height: u8,
    pub first_seq: u64,
    pub last_seq: u64,
    pub chunk_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum CatalogState {
    Empty,
    Root { root: ChunkCatalogRoot },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
enum CatalogNode {
    Leaf {
        schema: CatalogSchemaV1,
        refs: Vec<CatalogChunkRef>,
    },
    Branch {
        schema: CatalogSchemaV1,
        height: u8,
        children: Vec<CatalogChild>,
    },
}

const CATALOG_SCHEMAS: &[&str] = &[CATALOG_NODE_SCHEMA];

fn spec(tenant: &str) -> EnvelopeReadSpec<'_> {
    EnvelopeReadSpec {
        tenant,
        kind: ObjectKind::Blob,
        allowed_schemas: CATALOG_SCHEMAS,
        max_encoded_bytes: NonZeroU64::new(MAX_CATALOG_NODE_OBJECT_BYTES).expect("nonzero"),
        max_plaintext_bytes: NonZeroU64::new(MAX_CATALOG_NODE_OBJECT_BYTES).expect("nonzero"),
    }
}

fn validate_ref(r: &CatalogChunkRef) -> Result<()> {
    if r.first_seq == 0 || r.last_seq < r.first_seq || r.event_count == 0 {
        return Err(
            CoreError::IntegrityError("catalog chunk ref has an impossible range".into()).into(),
        );
    }
    let span = r.last_seq - r.first_seq + 1;
    if span != u64::from(r.event_count) {
        return Err(CoreError::IntegrityError(
            "catalog chunk ref event_count does not match sequence span".into(),
        )
        .into());
    }
    Ok(())
}

fn validate_leaf_refs(refs: &[CatalogChunkRef]) -> Result<()> {
    if refs.is_empty() || refs.len() > MAX_CATALOG_ITEMS {
        return Err(CoreError::IntegrityError("catalog leaf width is invalid".into()).into());
    }
    let mut prev_last: Option<u64> = None;
    for r in refs {
        validate_ref(r)?;
        if let Some(prev) = prev_last {
            if Some(r.first_seq) != prev.checked_add(1) {
                return Err(CoreError::IntegrityError(
                    "catalog leaf ranges are gapped or overlapping".into(),
                )
                .into());
            }
        }
        prev_last = Some(r.last_seq);
    }
    Ok(())
}

fn validate_children(children: &[CatalogChild], height: u8) -> Result<()> {
    if children.is_empty() || children.len() > MAX_CATALOG_ITEMS {
        return Err(CoreError::IntegrityError("catalog branch width is invalid".into()).into());
    }
    if height == 0 || height > MAX_CATALOG_HEIGHT {
        return Err(CoreError::IntegrityError("catalog branch height is invalid".into()).into());
    }
    let mut prev_last: Option<u64> = None;
    for c in children {
        if c.first_seq == 0
            || c.last_seq < c.first_seq
            || c.chunk_count == 0
            || c.chunk_count > c.last_seq - c.first_seq + 1
        {
            return Err(
                CoreError::IntegrityError("catalog child has an impossible range".into()).into(),
            );
        }
        if let Some(prev) = prev_last {
            if Some(c.first_seq) != prev.checked_add(1) {
                return Err(CoreError::IntegrityError(
                    "catalog child ranges are gapped or overlapping".into(),
                )
                .into());
            }
        }
        prev_last = Some(c.last_seq);
    }
    Ok(())
}

fn map_catalog_read_error(digest: &Digest, e: anyhow::Error) -> anyhow::Error {
    match e.downcast::<CoreError>() {
        Ok(CoreError::BackendUnavailable(m)) => {
            CoreError::BackendUnavailable(format!("catalog node {digest}: {m}")).into()
        }
        Ok(CoreError::Io(io)) => {
            CoreError::BackendUnavailable(format!("catalog node {digest}: {io}")).into()
        }
        Ok(CoreError::NotFound(_)) => {
            CoreError::IntegrityError(format!("catalog node {digest} is missing")).into()
        }
        Ok(other) => other.into(),
        Err(e) => CoreError::IntegrityError(format!("catalog node {digest}: {e:#}")).into(),
    }
}

async fn put_node(store: &Store, node: &CatalogNode) -> Result<Digest> {
    match node {
        CatalogNode::Leaf { refs, .. } => validate_leaf_refs(refs)?,
        CatalogNode::Branch {
            height, children, ..
        } => validate_children(children, *height)?,
    }
    let payload = serde_json::to_vec(node)?;
    store
        .put_object(
            ObjectKind::Blob,
            CATALOG_NODE_SCHEMA,
            payload,
            NonZeroU64::new(MAX_CATALOG_NODE_OBJECT_BYTES).expect("nonzero"),
        )
        .await
}

async fn load_node(store: &Store, digest: &Digest) -> Result<CatalogNode> {
    let (payload, _) = store
        .get_blob_limited(digest, spec(&store.tenant))
        .await
        .map_err(|e| map_catalog_read_error(digest, e))?;
    let node: CatalogNode = serde_json::from_slice(&payload).map_err(|e| {
        CoreError::IntegrityError(format!("catalog node {digest} is malformed: {e}"))
    })?;
    match &node {
        CatalogNode::Leaf { schema, refs } => {
            if !matches!(schema, CatalogSchemaV1::V1) {
                return Err(CoreError::IntegrityError("catalog leaf schema".into()).into());
            }
            validate_leaf_refs(refs)?;
        }
        CatalogNode::Branch {
            schema,
            height,
            children,
        } => {
            if !matches!(schema, CatalogSchemaV1::V1) {
                return Err(CoreError::IntegrityError("catalog branch schema".into()).into());
            }
            validate_children(children, *height)?;
        }
    }
    Ok(node)
}

fn leaf_span(refs: &[CatalogChunkRef]) -> (u64, u64, u64) {
    let first = refs.first().unwrap().first_seq;
    let last = refs.last().unwrap().last_seq;
    (first, last, refs.len() as u64)
}

fn child_span(children: &[CatalogChild]) -> (u64, u64, u64) {
    let first = children.first().unwrap().first_seq;
    let last = children.last().unwrap().last_seq;
    let count = children.iter().map(|c| c.chunk_count).sum();
    (first, last, count)
}

fn child_from_leaf(digest: Digest, refs: &[CatalogChunkRef]) -> CatalogChild {
    let (first_seq, last_seq, chunk_count) = leaf_span(refs);
    CatalogChild {
        first_seq,
        last_seq,
        chunk_count,
        digest,
    }
}

fn child_from_branch(digest: Digest, children: &[CatalogChild]) -> CatalogChild {
    let (first_seq, last_seq, chunk_count) = child_span(children);
    CatalogChild {
        first_seq,
        last_seq,
        chunk_count,
        digest,
    }
}

/// Append one chunk ref. Path-copies the right spine. A full node grows a
/// right sibling and at most one split per level.
pub async fn append(
    store: &Store,
    state: &CatalogState,
    chunk: CatalogChunkRef,
) -> Result<CatalogState> {
    validate_ref(&chunk)?;
    match state {
        CatalogState::Empty => {
            if chunk.first_seq != 1 {
                return Err(CoreError::IntegrityError(
                    "first catalog chunk must start at sequence 1".into(),
                )
                .into());
            }
            let node = CatalogNode::Leaf {
                schema: CatalogSchemaV1::V1,
                refs: vec![chunk.clone()],
            };
            let digest = put_node(store, &node).await?;
            Ok(CatalogState::Root {
                root: ChunkCatalogRoot {
                    schema: CatalogSchemaV1::V1,
                    digest,
                    height: 0,
                    first_seq: chunk.first_seq,
                    last_seq: chunk.last_seq,
                    chunk_count: 1,
                },
            })
        }
        CatalogState::Root { root } => {
            let expected = root
                .last_seq
                .checked_add(1)
                .ok_or_else(|| CoreError::IntegrityError("catalog sequence overflow".into()))?;
            if chunk.first_seq != expected {
                return Err(CoreError::IntegrityError(format!(
                    "catalog append {} is not contiguous after {}",
                    chunk.first_seq, root.last_seq
                ))
                .into());
            }
            match append_at(store, &root.digest, root.height, chunk).await? {
                SpineUpdate::Grown { digest, .. } => {
                    let node = load_node(store, &digest).await?;
                    Ok(CatalogState::Root {
                        root: root_from_node(digest, node)?,
                    })
                }
                SpineUpdate::Split {
                    left,
                    right,
                    height,
                } => {
                    let new_height = height
                        .checked_add(1)
                        .ok_or_else(|| CoreError::Rejected("catalog height exceeded".into()))?;
                    if new_height > MAX_CATALOG_HEIGHT {
                        return Err(CoreError::Rejected("catalog height exceeded".into()).into());
                    }
                    let children = vec![left, right];
                    let (first_seq, last_seq, chunk_count) = child_span(&children);
                    let digest = put_node(
                        store,
                        &CatalogNode::Branch {
                            schema: CatalogSchemaV1::V1,
                            height: new_height,
                            children,
                        },
                    )
                    .await?;
                    Ok(CatalogState::Root {
                        root: ChunkCatalogRoot {
                            schema: CatalogSchemaV1::V1,
                            digest,
                            height: new_height,
                            first_seq,
                            last_seq,
                            chunk_count,
                        },
                    })
                }
            }
        }
    }
}

enum SpineUpdate {
    Grown {
        digest: Digest,
        height: u8, // kept for parent-child checks at the call site
    },
    Split {
        left: CatalogChild,
        right: CatalogChild,
        height: u8,
    },
}

fn root_from_node(digest: Digest, node: CatalogNode) -> Result<ChunkCatalogRoot> {
    Ok(match node {
        CatalogNode::Leaf { refs, .. } => {
            let (first_seq, last_seq, chunk_count) = leaf_span(&refs);
            ChunkCatalogRoot {
                schema: CatalogSchemaV1::V1,
                digest,
                height: 0,
                first_seq,
                last_seq,
                chunk_count,
            }
        }
        CatalogNode::Branch {
            height, children, ..
        } => {
            let (first_seq, last_seq, chunk_count) = child_span(&children);
            ChunkCatalogRoot {
                schema: CatalogSchemaV1::V1,
                digest,
                height,
                first_seq,
                last_seq,
                chunk_count,
            }
        }
    })
}

async fn append_at(
    store: &Store,
    digest: &Digest,
    height: u8,
    chunk: CatalogChunkRef,
) -> Result<SpineUpdate> {
    let node = load_node(store, digest).await?;
    match node {
        CatalogNode::Leaf { mut refs, .. } => {
            if height != 0 {
                return Err(CoreError::IntegrityError(
                    "catalog leaf reached at nonzero height".into(),
                )
                .into());
            }
            if refs.len() < MAX_CATALOG_ITEMS {
                refs.push(chunk);
                let digest = put_node(
                    store,
                    &CatalogNode::Leaf {
                        schema: CatalogSchemaV1::V1,
                        refs,
                    },
                )
                .await?;
                return Ok(SpineUpdate::Grown { digest, height: 0 });
            }
            let right_refs = vec![chunk];
            let right_digest = put_node(
                store,
                &CatalogNode::Leaf {
                    schema: CatalogSchemaV1::V1,
                    refs: right_refs.clone(),
                },
            )
            .await?;
            Ok(SpineUpdate::Split {
                left: child_from_leaf(digest.clone(), &refs),
                right: child_from_leaf(right_digest, &right_refs),
                height: 0,
            })
        }
        CatalogNode::Branch {
            height: h,
            mut children,
            ..
        } => {
            if h != height {
                return Err(CoreError::IntegrityError(format!(
                    "catalog branch height {h} disagrees with parent {height}"
                ))
                .into());
            }
            let last = children.last().expect("branch nonempty").clone();
            let child_update = Box::pin(append_at(store, &last.digest, h - 1, chunk)).await?;
            match child_update {
                SpineUpdate::Grown {
                    digest: child_digest,
                    height: grown_h,
                } => {
                    if grown_h + 1 != h {
                        return Err(CoreError::IntegrityError(
                            "catalog grown child height is not parent minus one".into(),
                        )
                        .into());
                    }
                    let child_node = load_node(store, &child_digest).await?;
                    *children.last_mut().unwrap() = match child_node {
                        CatalogNode::Leaf { refs, .. } => child_from_leaf(child_digest, &refs),
                        CatalogNode::Branch {
                            children: ch,
                            height: ch_h,
                            ..
                        } => {
                            if ch_h + 1 != h {
                                return Err(CoreError::IntegrityError(
                                    "catalog child height is not parent minus one".into(),
                                )
                                .into());
                            }
                            child_from_branch(child_digest, &ch)
                        }
                    };
                    let digest = put_node(
                        store,
                        &CatalogNode::Branch {
                            schema: CatalogSchemaV1::V1,
                            height: h,
                            children,
                        },
                    )
                    .await?;
                    Ok(SpineUpdate::Grown { digest, height: h })
                }
                SpineUpdate::Split {
                    left,
                    right,
                    height: sh,
                } => {
                    if sh + 1 != h {
                        return Err(CoreError::IntegrityError(
                            "catalog split height is not parent minus one".into(),
                        )
                        .into());
                    }
                    children.pop();
                    children.push(left);
                    children.push(right);
                    if children.len() <= MAX_CATALOG_ITEMS {
                        let digest = put_node(
                            store,
                            &CatalogNode::Branch {
                                schema: CatalogSchemaV1::V1,
                                height: h,
                                children,
                            },
                        )
                        .await?;
                        return Ok(SpineUpdate::Grown { digest, height: h });
                    }
                    if h >= MAX_CATALOG_HEIGHT {
                        return Err(CoreError::Rejected("catalog height exceeded".into()).into());
                    }
                    let right_children: Vec<CatalogChild> = children.split_off(MAX_CATALOG_ITEMS);
                    let left_digest = put_node(
                        store,
                        &CatalogNode::Branch {
                            schema: CatalogSchemaV1::V1,
                            height: h,
                            children: children.clone(),
                        },
                    )
                    .await?;
                    let right_digest = put_node(
                        store,
                        &CatalogNode::Branch {
                            schema: CatalogSchemaV1::V1,
                            height: h,
                            children: right_children.clone(),
                        },
                    )
                    .await?;
                    Ok(SpineUpdate::Split {
                        left: child_from_branch(left_digest, &children),
                        right: child_from_branch(right_digest, &right_children),
                        height: h,
                    })
                }
            }
        }
    }
}

pub async fn seek_leaf(
    store: &Store,
    state: &CatalogState,
    seq: u64,
) -> Result<Option<Vec<CatalogChunkRef>>> {
    let CatalogState::Root { root } = state else {
        return Ok(None);
    };
    if seq < root.first_seq || seq > root.last_seq {
        return Ok(None);
    }
    let mut digest = root.digest.clone();
    let mut depth = 0u8;
    loop {
        if depth > MAX_CATALOG_HEIGHT {
            return Err(
                CoreError::IntegrityError("catalog seek exceeded height bound".into()).into(),
            );
        }
        match load_node(store, &digest).await? {
            CatalogNode::Leaf { refs, .. } => {
                let first = refs.first().map(|r| r.first_seq);
                let last = refs.last().map(|r| r.last_seq);
                match (first, last) {
                    (Some(f), Some(l)) if f <= seq && seq <= l => return Ok(Some(refs)),
                    _ => {
                        return Err(CoreError::IntegrityError(
                            "catalog leaf does not cover the seek".into(),
                        )
                        .into())
                    }
                }
            }
            CatalogNode::Branch { children, .. } => {
                let Some(child) = children
                    .iter()
                    .find(|c| seq >= c.first_seq && seq <= c.last_seq)
                else {
                    return Err(CoreError::IntegrityError(
                        "catalog branch has no child covering the seek".into(),
                    )
                    .into());
                };
                digest = child.digest.clone();
                depth = depth.saturating_add(1);
            }
        }
    }
}

#[allow(dead_code)]
pub async fn walk_right_spine_digests(store: &Store, state: &CatalogState) -> Result<Vec<Digest>> {
    let CatalogState::Root { root } = state else {
        return Ok(Vec::new());
    };
    let mut out = vec![root.digest.clone()];
    let mut digest = root.digest.clone();
    let mut depth = 0u8;
    loop {
        if depth > MAX_CATALOG_HEIGHT {
            return Err(
                CoreError::IntegrityError("catalog spine exceeded height bound".into()).into(),
            );
        }
        match load_node(store, &digest).await? {
            CatalogNode::Leaf { .. } => return Ok(out),
            CatalogNode::Branch { children, .. } => {
                digest = children.last().unwrap().digest.clone();
                out.push(digest.clone());
                depth = depth.saturating_add(1);
            }
        }
    }
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
            DigestKey::from_bytes([4u8; 32]),
            None,
        )
    }

    fn tiny(seq: u64, digest: &Digest) -> CatalogChunkRef {
        CatalogChunkRef {
            digest: digest.clone(),
            first_seq: seq,
            last_seq: seq,
            event_count: 1,
            raw_payload_bytes: 1,
            plaintext_bytes: 8,
        }
    }

    #[test]
    fn catalog_rejects_sequence_wrap_without_panicking() {
        let s = store();
        let digest = s.key.digest(b"chunk");
        let refs = vec![tiny(u64::MAX, &digest), tiny(1, &digest)];
        assert!(matches!(
            validate_leaf_refs(&refs)
                .unwrap_err()
                .downcast_ref::<CoreError>(),
            Some(CoreError::IntegrityError(_))
        ));
        let children: Vec<_> = refs
            .iter()
            .map(|r| CatalogChild {
                first_seq: r.first_seq,
                last_seq: r.last_seq,
                chunk_count: 1,
                digest: r.digest.clone(),
            })
            .collect();
        assert!(matches!(
            validate_children(&children, 1)
                .unwrap_err()
                .downcast_ref::<CoreError>(),
            Some(CoreError::IntegrityError(_))
        ));
    }

    #[test]
    fn catalog_rejects_more_chunks_than_events() {
        let s = store();
        let digest = s.key.digest(b"chunk");
        let children = vec![CatalogChild {
            first_seq: 1,
            last_seq: 1,
            chunk_count: u64::MAX,
            digest,
        }];
        assert!(matches!(
            validate_children(&children, 1)
                .unwrap_err()
                .downcast_ref::<CoreError>(),
            Some(CoreError::IntegrityError(_))
        ));
    }

    #[tokio::test]
    async fn seventy_chunks_split_leaves_and_create_root() {
        let store = store();
        let d = store.key.digest(b"chunk");
        let mut state = CatalogState::Empty;
        for seq in 1..=70u64 {
            state = append(&store, &state, tiny(seq, &d)).await.unwrap();
        }
        let CatalogState::Root { root } = &state else {
            panic!("expected root");
        };
        assert_eq!(root.first_seq, 1);
        assert_eq!(root.last_seq, 70);
        assert_eq!(root.chunk_count, 70);
        assert!(root.height >= 1, "root must be a branch after leaf splits");
        for seq in [1, 32, 33, 64, 65, 70] {
            let leaf = seek_leaf(&store, &state, seq).await.unwrap().unwrap();
            assert!(leaf.iter().any(|r| r.first_seq <= seq && seq <= r.last_seq));
        }
        assert_height_invariant(&store, &state).await;
    }

    #[tokio::test]
    async fn one_thousand_twenty_five_chunks_split_an_internal_branch() {
        let store = store();
        let d = store.key.digest(b"chunk");
        let mut state = CatalogState::Empty;
        for seq in 1..=1025u64 {
            state = append(&store, &state, tiny(seq, &d)).await.unwrap();
        }
        let CatalogState::Root { root } = &state else {
            panic!("expected root");
        };
        assert_eq!(root.chunk_count, 1025);
        assert!(
            root.height >= 2,
            "1025 refs exceed 32*32 and must split an internal branch, height={}",
            root.height
        );
        let leaf = seek_leaf(&store, &state, 1025).await.unwrap().unwrap();
        assert_eq!(leaf.last().unwrap().last_seq, 1025);
        assert_height_invariant(&store, &state).await;
    }

    #[tokio::test]
    async fn twenty_one_hundred_chunks_keep_parent_child_height() {
        let store = store();
        let d = store.key.digest(b"chunk");
        let mut state = CatalogState::Empty;
        for seq in 1..=2100u64 {
            state = append(&store, &state, tiny(seq, &d)).await.unwrap();
        }
        let CatalogState::Root { root } = &state else {
            panic!("expected root");
        };
        assert_eq!(root.chunk_count, 2100);
        assert_eq!(root.first_seq, 1);
        assert_eq!(root.last_seq, 2100);
        assert_height_invariant(&store, &state).await;
        for seq in [1, 32, 1024, 1025, 2048, 2050, 2100] {
            let leaf = seek_leaf(&store, &state, seq).await.unwrap().unwrap();
            assert!(leaf.first().unwrap().first_seq <= seq && seq <= leaf.last().unwrap().last_seq);
        }
    }

    #[tokio::test]
    async fn seek_leaf_rejects_child_last_seq_off_by_one() {
        let store = store();
        let d = store.key.digest(b"chunk");
        let leaf = CatalogNode::Leaf {
            schema: CatalogSchemaV1::V1,
            refs: vec![tiny(1, &d)],
        };
        let leaf_digest = put_node(&store, &leaf).await.unwrap();
        let branch = CatalogNode::Branch {
            schema: CatalogSchemaV1::V1,
            height: 1,
            children: vec![CatalogChild {
                first_seq: 1,
                last_seq: 2,
                chunk_count: 1,
                digest: leaf_digest,
            }],
        };
        let root_digest = put_node(&store, &branch).await.unwrap();
        let state = CatalogState::Root {
            root: ChunkCatalogRoot {
                schema: CatalogSchemaV1::V1,
                digest: root_digest,
                height: 1,
                first_seq: 1,
                last_seq: 2,
                chunk_count: 1,
            },
        };
        let err = seek_leaf(&store, &state, 2).await.unwrap_err();
        match err.downcast_ref::<CoreError>() {
            Some(CoreError::IntegrityError(m)) => {
                assert!(m.contains("does not cover"), "{m}");
            }
            other => panic!("expected IntegrityError, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn seek_leaf_rejects_unbounded_spine() {
        let store = store();
        let d = store.key.digest(b"chunk");
        let mut child = put_node(
            &store,
            &CatalogNode::Leaf {
                schema: CatalogSchemaV1::V1,
                refs: vec![tiny(1, &d)],
            },
        )
        .await
        .unwrap();
        let mut height = 0u8;
        for h in 1..=MAX_CATALOG_HEIGHT + 2 {
            child = put_node(
                &store,
                &CatalogNode::Branch {
                    schema: CatalogSchemaV1::V1,
                    height: h.min(MAX_CATALOG_HEIGHT),
                    children: vec![CatalogChild {
                        first_seq: 1,
                        last_seq: 1,
                        chunk_count: 1,
                        digest: child,
                    }],
                },
            )
            .await
            .unwrap();
            height = h.min(MAX_CATALOG_HEIGHT);
        }
        let state = CatalogState::Root {
            root: ChunkCatalogRoot {
                schema: CatalogSchemaV1::V1,
                digest: child,
                height,
                first_seq: 1,
                last_seq: 1,
                chunk_count: 1,
            },
        };
        let err = seek_leaf(&store, &state, 1).await.unwrap_err();
        match err.downcast_ref::<CoreError>() {
            Some(CoreError::IntegrityError(m)) => {
                assert!(m.contains("height bound"), "{m}");
            }
            other => panic!("expected height-bound IntegrityError, got {other:?}"),
        }
    }

    async fn assert_height_invariant(store: &Store, state: &CatalogState) {
        let CatalogState::Root { root } = state else {
            panic!("expected root");
        };
        let mut digest = root.digest.clone();
        let mut expected_height = root.height;
        let mut depth = 0u8;
        loop {
            match load_node(store, &digest).await.unwrap() {
                CatalogNode::Leaf { .. } => {
                    assert_eq!(
                        expected_height, 0,
                        "leaf at claimed height {expected_height}"
                    );
                    assert_eq!(depth, root.height, "root.height must equal descent depth");
                    return;
                }
                CatalogNode::Branch {
                    height, children, ..
                } => {
                    assert_eq!(height, expected_height);
                    for child in &children {
                        match load_node(store, &child.digest).await.unwrap() {
                            CatalogNode::Leaf { .. } => {
                                assert_eq!(height, 1, "leaf child under height {height}");
                            }
                            CatalogNode::Branch { height: ch, .. } => {
                                assert_eq!(
                                    ch,
                                    height - 1,
                                    "child height {ch} must be parent {height} minus one"
                                );
                            }
                        }
                    }
                    digest = children[0].digest.clone();
                    expected_height = height - 1;
                    depth += 1;
                    assert!(depth <= MAX_CATALOG_HEIGHT);
                }
            }
        }
    }

    #[tokio::test]
    async fn put_node_rejects_structurally_invalid_leaf() {
        let store = store();
        let err = put_node(
            &store,
            &CatalogNode::Leaf {
                schema: CatalogSchemaV1::V1,
                refs: Vec::new(),
            },
        )
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
}

#[cfg(test)]
mod parent_review {
    use super::*;
    use crate::store::Store;
    use comb_core::DigestKey;
    use comb_object::memory::MemoryBackend;
    use std::sync::Arc;

    fn store() -> Store {
        Store::new(
            Arc::new(MemoryBackend::new()),
            "review",
            DigestKey::from_bytes([4; 32]),
            None,
        )
    }

    fn tiny(seq: u64, digest: &Digest) -> CatalogChunkRef {
        CatalogChunkRef {
            digest: digest.clone(),
            first_seq: seq,
            last_seq: seq,
            event_count: 1,
            raw_payload_bytes: 1,
            plaintext_bytes: 8,
        }
    }

    #[tokio::test]
    async fn review_2100_refs_preserve_every_child_height() {
        let s = store();
        let d = s.key.digest(b"chunk");
        let mut state = CatalogState::Empty;
        for seq in 1..=2100 {
            state = append(&s, &state, tiny(seq, &d)).await.unwrap();
        }
        let CatalogState::Root { root } = state else {
            panic!("missing root");
        };
        let mut pending = vec![(root.digest, root.height)];
        while let Some((d, expected)) = pending.pop() {
            match load_node(&s, &d).await.unwrap() {
                CatalogNode::Leaf { .. } => {
                    assert_eq!(
                        expected, 0,
                        "leaf depth differs from declared root/parent height"
                    );
                }
                CatalogNode::Branch {
                    height, children, ..
                } => {
                    assert_eq!(height, expected, "branch child did not decrease height");
                    assert!(height > 0);
                    for c in children {
                        pending.push((c.digest, height - 1));
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn review_seek_rejects_noncovering_leaf() {
        let s = store();
        let d = s.key.digest(b"chunk");
        let leaf = put_node(
            &s,
            &CatalogNode::Leaf {
                schema: CatalogSchemaV1::V1,
                refs: vec![tiny(1, &d)],
            },
        )
        .await
        .unwrap();
        let branch = put_node(
            &s,
            &CatalogNode::Branch {
                schema: CatalogSchemaV1::V1,
                height: 1,
                children: vec![CatalogChild {
                    first_seq: 1,
                    last_seq: 2,
                    chunk_count: 1,
                    digest: leaf,
                }],
            },
        )
        .await
        .unwrap();
        let state = CatalogState::Root {
            root: ChunkCatalogRoot {
                schema: CatalogSchemaV1::V1,
                digest: branch,
                height: 1,
                first_seq: 1,
                last_seq: 2,
                chunk_count: 1,
            },
        };
        assert!(
            seek_leaf(&s, &state, 2).await.is_err(),
            "noncovering leaf is integrity failure"
        );
    }

    #[tokio::test]
    async fn review_seek_rejects_depth_beyond_bound() {
        let s = store();
        let d = s.key.digest(b"chunk");
        let mut node = put_node(
            &s,
            &CatalogNode::Leaf {
                schema: CatalogSchemaV1::V1,
                refs: vec![tiny(1, &d)],
            },
        )
        .await
        .unwrap();
        for _ in 0..10 {
            node = put_node(
                &s,
                &CatalogNode::Branch {
                    schema: CatalogSchemaV1::V1,
                    height: 1,
                    children: vec![CatalogChild {
                        first_seq: 1,
                        last_seq: 1,
                        chunk_count: 1,
                        digest: node,
                    }],
                },
            )
            .await
            .unwrap();
        }
        let state = CatalogState::Root {
            root: ChunkCatalogRoot {
                schema: CatalogSchemaV1::V1,
                digest: node,
                height: 1,
                first_seq: 1,
                last_seq: 1,
                chunk_count: 1,
            },
        };
        assert!(
            seek_leaf(&s, &state, 1).await.is_err(),
            "ten branch hops exceed height bound"
        );
    }
}
