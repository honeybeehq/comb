
#[cfg(test)]
mod parent_review {
 use super::*;
 use std::sync::Arc;
 use comb_core::DigestKey;
 use comb_object::memory::MemoryBackend;
 fn store() -> Store { Store::new(Arc::new(MemoryBackend::new()),"review",DigestKey::from_bytes([4;32]),None) }
 fn tiny(seq:u64,digest:&Digest)->CatalogChunkRef { CatalogChunkRef{digest:digest.clone(),first_seq:seq,last_seq:seq,event_count:1,raw_payload_bytes:1,plaintext_bytes:8} }
 #[tokio::test]
 async fn review_2100_refs_preserve_every_child_height() {
  let s=store();let d=s.key.digest(b"chunk");let mut state=CatalogState::Empty;
  for seq in 1..=2100 {state=append(&s,&state,tiny(seq,&d)).await.unwrap();}
  let CatalogState::Root{root}=state else {panic!("missing root")};
  let mut pending=vec![(root.digest,root.height)];
  while let Some((d,expected))=pending.pop() {
   match load_node(&s,&d).await.unwrap() {
    CatalogNode::Leaf{..}=>assert_eq!(expected,0,"leaf depth differs from declared root/parent height"),
    CatalogNode::Branch{height,children,..}=>{assert_eq!(height,expected,"branch child did not decrease height");assert!(height>0);for c in children{pending.push((c.digest,height-1));}}
   }
  }
 }
 #[tokio::test]
 async fn review_seek_rejects_noncovering_leaf() {
  let s=store();let d=s.key.digest(b"chunk");
  let leaf=put_node(&s,&CatalogNode::Leaf{schema:CatalogSchemaV1::V1,refs:vec![tiny(1,&d)]}).await.unwrap();
  let branch=put_node(&s,&CatalogNode::Branch{schema:CatalogSchemaV1::V1,height:1,children:vec![CatalogChild{first_seq:1,last_seq:2,chunk_count:1,digest:leaf}]}).await.unwrap();
  let state=CatalogState::Root{root:ChunkCatalogRoot{schema:CatalogSchemaV1::V1,digest:branch,height:1,first_seq:1,last_seq:2,chunk_count:1}};
  assert!(seek_leaf(&s,&state,2).await.is_err(),"noncovering leaf is integrity failure");
 }
 #[tokio::test]
 async fn review_seek_rejects_depth_beyond_bound() {
  let s=store();let d=s.key.digest(b"chunk");
  let mut node=put_node(&s,&CatalogNode::Leaf{schema:CatalogSchemaV1::V1,refs:vec![tiny(1,&d)]}).await.unwrap();
  for _ in 0..10 {node=put_node(&s,&CatalogNode::Branch{schema:CatalogSchemaV1::V1,height:1,children:vec![CatalogChild{first_seq:1,last_seq:1,chunk_count:1,digest:node}]}).await.unwrap();}
  let state=CatalogState::Root{root:ChunkCatalogRoot{schema:CatalogSchemaV1::V1,digest:node,height:1,first_seq:1,last_seq:1,chunk_count:1}};
  assert!(seek_leaf(&s,&state,1).await.is_err(),"ten branch hops exceed height bound");
 }
}
