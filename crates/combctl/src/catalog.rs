//! Immutable append-oriented chunk catalog (B+tree). Internal to Log.

use crate::store::Store;
use anyhow::Result;
use comb_core::error::CoreError;
use comb_core::{Digest, Envelope, EnvelopeReadSpec, ObjectKind};
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
    let mut prev_last = None;
    for r in refs {
        validate_ref(r)?;
        if let Some(prev) = prev_last {
            if r.first_seq != prev + 1 {
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
    let mut prev_last = None;
    for c in children {
        if c.first_seq == 0 || c.last_seq < c.first_seq || c.chunk_count == 0 {
            return Err(
                CoreError::IntegrityError("catalog child has an impossible range".into()).into(),
            );
        }
        if let Some(prev) = prev_last {
            if c.first_seq != prev + 1 {
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

async fn put_node(store: &Store, node: &CatalogNode) -> Result<Digest> {
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
        .map_err(|e| CoreError::IntegrityError(format!("catalog node {digest}: {e:#}")))?;
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
            if chunk.first_seq != root.last_seq + 1 {
                return Err(CoreError::IntegrityError(format!(
                    "catalog append {} is not contiguous after {}",
                    chunk.first_seq, root.last_seq
                ))
                .into());
            }
            let (digest, _height, _split) =
                append_at(store, &root.digest, root.height, chunk).await?;
            let node = load_node(store, &digest).await?;
            let root = match node {
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
            };
            Ok(CatalogState::Root { root })
        }
    }
}

async fn append_at(
    store: &Store,
    digest: &Digest,
    height: u8,
    chunk: CatalogChunkRef,
) -> Result<(Digest, u8, bool)> {
    let node = load_node(store, digest).await?;
    match node {
        CatalogNode::Leaf { mut refs, .. } => {
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
                Ok((digest, 0, false))
            } else {
                let right = CatalogNode::Leaf {
                    schema: CatalogSchemaV1::V1,
                    refs: vec![chunk],
                };
                let right_digest = put_node(store, &right).await?;
                let left_child = child_from_leaf(digest.clone(), &refs);
                let CatalogNode::Leaf {
                    refs: right_refs, ..
                } = &right
                else {
                    unreachable!();
                };
                let right_child = child_from_leaf(right_digest, right_refs);
                if height != 0 {
                    // caller rebuilds parent
                }
                let branch = CatalogNode::Branch {
                    schema: CatalogSchemaV1::V1,
                    height: 1,
                    children: vec![left_child, right_child],
                };
                let new_root = put_node(store, &branch).await?;
                Ok((new_root, 1, true))
            }
        }
        CatalogNode::Branch {
            height: h,
            mut children,
            ..
        } => {
            let last = children.last().expect("branch nonempty").clone();
            let (child_digest, child_height, child_split) =
                Box::pin(append_at(store, &last.digest, h - 1, chunk)).await?;
            if !child_split {
                let child_node = load_node(store, &child_digest).await?;
                let updated = match child_node {
                    CatalogNode::Leaf { refs, .. } => child_from_leaf(child_digest, &refs),
                    CatalogNode::Branch { children, .. } => {
                        child_from_branch(child_digest, &children)
                    }
                };
                *children.last_mut().unwrap() = updated;
                let digest = put_node(
                    store,
                    &CatalogNode::Branch {
                        schema: CatalogSchemaV1::V1,
                        height: h,
                        children,
                    },
                )
                .await?;
                Ok((digest, h, false))
            } else {
                // child split produced a new node; append_at on a full leaf
                // currently returns a new branch. For internal levels we need
                // the right sibling only. Handle leaf-split specially:
                let split_node = load_node(store, &child_digest).await?;
                match split_node {
                    CatalogNode::Branch {
                        children: split_children,
                        height: sh,
                        ..
                    } if child_height == 1 && h == 1 && sh == 1 && split_children.len() == 2 => {
                        // Leaf split bubbled as a 2-child branch. If we are the
                        // root of height 1, that branch IS the new root when
                        // we only had one... actually we already had children.
                        // Replace last child with split_children[0] and push [1].
                        children.pop();
                        children.extend(split_children);
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
                            Ok((digest, h, false))
                        } else {
                            let right: Vec<CatalogChild> = children.split_off(MAX_CATALOG_ITEMS);
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
                                    children: right.clone(),
                                },
                            )
                            .await?;
                            let branch = CatalogNode::Branch {
                                schema: CatalogSchemaV1::V1,
                                height: h + 1,
                                children: vec![
                                    child_from_branch(left_digest, &children),
                                    child_from_branch(right_digest, &right),
                                ],
                            };
                            if h + 1 > MAX_CATALOG_HEIGHT {
                                return Err(
                                    CoreError::Rejected("catalog height exceeded".into()).into()
                                );
                            }
                            let digest = put_node(store, &branch).await?;
                            Ok((digest, h + 1, true))
                        }
                    }
                    other => {
                        let updated = match other {
                            CatalogNode::Leaf { refs, .. } => child_from_leaf(child_digest, &refs),
                            CatalogNode::Branch { children: ch, .. } => {
                                child_from_branch(child_digest, &ch)
                            }
                        };
                        *children.last_mut().unwrap() = updated;
                        let digest = put_node(
                            store,
                            &CatalogNode::Branch {
                                schema: CatalogSchemaV1::V1,
                                height: h,
                                children,
                            },
                        )
                        .await?;
                        Ok((digest, h, false))
                    }
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
    loop {
        match load_node(store, &digest).await? {
            CatalogNode::Leaf { refs, .. } => return Ok(Some(refs)),
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
            }
        }
    }
}

pub async fn encoded_node_bytes(store: &Store, digest: &Digest) -> Result<u64> {
    let (payload, _) = store.get_blob_limited(digest, spec(&store.tenant)).await?;
    let env = Envelope::new(
        &store.tenant,
        ObjectKind::Blob,
        CATALOG_NODE_SCHEMA,
        payload,
        &store.key,
    );
    Ok(env.encode()?.len() as u64)
}

pub async fn walk_right_spine_digests(store: &Store, state: &CatalogState) -> Result<Vec<Digest>> {
    let CatalogState::Root { root } = state else {
        return Ok(Vec::new());
    };
    let mut out = vec![root.digest.clone()];
    let mut digest = root.digest.clone();
    loop {
        match load_node(store, &digest).await? {
            CatalogNode::Leaf { .. } => return Ok(out),
            CatalogNode::Branch { children, .. } => {
                digest = children.last().unwrap().digest.clone();
                out.push(digest.clone());
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
        for digest in walk_right_spine_digests(&store, &state).await.unwrap() {
            assert!(
                encoded_node_bytes(&store, &digest).await.unwrap() <= MAX_CATALOG_NODE_OBJECT_BYTES
            );
        }
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
        for digest in walk_right_spine_digests(&store, &state).await.unwrap() {
            assert!(
                encoded_node_bytes(&store, &digest).await.unwrap() <= MAX_CATALOG_NODE_OBJECT_BYTES
            );
        }
    }
}
