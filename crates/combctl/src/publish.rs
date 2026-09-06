//! Crate-private publication engine. Every logical ref CAS goes through here.
//!
//! Recovery truth is the immutable commit chain linked from the ref. The
//! per-identity intent is a CAS-guarded attempt record and a result cache.

use crate::store::Store;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use comb_core::commit::{
    skip_target_generation, Admission, Commit, CommitHeader, HistoryEntry, IntentState, OpIntent,
    COMMIT_SCHEMA, HEADER_SCHEMA,
};
use comb_core::error::CoreError;
use comb_core::operation::{Material, OpIdentity, OperationPolicy};
use comb_core::{Digest, Envelope, ObjectKind, RefValue};
use comb_object::Version;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub const NS_V2: &str = "comb/v2";
pub const NS_V3: &str = "comb/v3";
pub const NS_V1: &str = "comb/v1";
const LOG_MANIFEST_SCHEMA: &str = "comb.log.partition-manifest/v2";
const LOG_MANIFEST_SCHEMA_V3: &str = "comb.log.partition-manifest/v3";
const MAX_PUBLISH_ATTEMPTS: u32 = 128;
const MAX_SEEK_HOPS: u32 = 10_000;

#[derive(Debug, Clone)]
pub struct HeadSnapshot {
    pub value: RefValue,
    pub version: Option<Version>,
}

#[derive(Debug, Clone)]
pub struct Published<T> {
    pub outcome: T,
    pub generation: u64,
    pub epoch: u64,
    pub commit: Digest,
    pub value: RefValue,
    pub first_delivery: bool,
    pub admitted: Vec<Admission>,
}

pub(crate) struct Upload {
    pub kind: ObjectKind,
    pub schema: String,
    pub payload: Vec<u8>,
}

pub(crate) struct Companion {
    pub identity: OpIdentity,
    pub result: serde_json::Value,
}

pub(crate) struct PreparedMutation<R> {
    pub next: RefValue,
    pub uploads: Vec<Upload>,
    /// If set, that upload already embeds the commit header (log manifest).
    /// Otherwise the engine writes a separate `comb.commit/v1` object.
    pub commit_upload: Option<usize>,
    pub change: serde_json::Value,
    pub outcome: R,
    pub admitted: Vec<Admission>,
    /// Other identities in this CAS. Their intents are moved to the same
    /// base before the ref write so a crash cannot re-append them.
    pub companions: Vec<Companion>,
}

pub(crate) struct PrepareCtx<'a> {
    pub store: &'a Store,
    pub snapshot: &'a HeadSnapshot,
    pub identity: &'a OpIdentity,
    pub request: Digest,
    pub generation: u64,
    pub parent: Option<Digest>,
    pub skip: Option<Digest>,
    pub now: DateTime<Utc>,
}

impl<'a> PrepareCtx<'a> {
    pub fn header(&self, epoch: u64) -> CommitHeader {
        CommitHeader {
            schema: HEADER_SCHEMA.into(),
            resource: self.snapshot.value.name.clone(),
            generation: self.generation,
            epoch,
            identity: self.identity.canonical(),
            request: self.request.clone(),
            parent: self.parent.clone(),
            skip: self.skip.clone(),
            at: self.now,
        }
    }
}

pub(crate) trait RefMutationPlan: Send + Sync {
    type Outcome: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;

    fn resource(&self) -> &str;
    fn material(&self) -> Material;
    fn prepare(
        &self,
        ctx: PrepareCtx<'_>,
    ) -> impl std::future::Future<Output = Result<PreparedMutation<Self::Outcome>>> + Send;
}

#[derive(Debug, Clone)]
pub(crate) struct CommitView {
    pub digest: Digest,
    pub header: CommitHeader,
    pub result: serde_json::Value,
    pub admitted: Vec<Admission>,
    pub ref_state: Option<RefValue>,
    /// Log publications store the manifest as the ref target; that digest is
    /// the commit object and is filled after the object exists.
    pub target_follows_commit: bool,
}

impl Store {
    pub fn object_key(&self, digest: &Digest) -> String {
        format!(
            "{}/tenants/{}/objects/b3k/{}/{}",
            self.layout.prefix(),
            self.tenant,
            digest.key_prefix(),
            digest.hex()
        )
    }

    pub fn ref_key(&self, name: &str) -> String {
        format!(
            "{}/tenants/{}/refs/{name}.json",
            self.layout.prefix(),
            self.tenant
        )
    }

    pub fn v1_ref_key(&self, name: &str) -> String {
        format!("{NS_V1}/tenants/{}/refs/{name}.json", self.tenant)
    }

    pub fn intent_key(&self, identity: &OpIdentity) -> String {
        match identity {
            OpIdentity::Generic(_) => format!(
                "{}/tenants/{}/ops/{}/{}.json",
                self.layout.prefix(),
                self.tenant,
                identity.shard(),
                identity.canonical()
            ),
            OpIdentity::Stable(k) => format!(
                "{}/tenants/{}/stable/{}.json",
                self.layout.prefix(),
                self.tenant,
                k.to_hex()
            ),
        }
    }

    pub async fn reject_v1(&self, name: &str) -> Result<()> {
        if self.backend.exists(&self.v1_ref_key(name)).await? {
            return Err(CoreError::InvalidFormat(format!(
                "comb/v1 ref {name} exists; this process uses {NS_V2} only and will not read or rewrite v1 tenants"
            ))
            .into());
        }
        Ok(())
    }

    pub async fn read_head(&self, name: &str) -> Result<Option<HeadSnapshot>> {
        self.reject_v1(name).await?;
        match self.backend.get(&self.ref_key(name)).await {
            Ok((bytes, version)) => {
                let value: RefValue = serde_json::from_slice(&bytes)
                    .map_err(|e| CoreError::InvalidFormat(format!("ref {name}: {e}")))?;
                value.validate_schema()?;
                if value.tenant != self.tenant || value.name != name {
                    return Err(CoreError::RecoveryFailed(format!(
                        "ref {name} identity mismatch (stored tenant {} name {})",
                        value.tenant, value.name
                    ))
                    .into());
                }
                if value.generation > 0 && value.head_commit.is_none() {
                    return Err(CoreError::RecoveryFailed(format!(
                        "ref {name} generation {} has no head_commit",
                        value.generation
                    ))
                    .into());
                }
                Ok(Some(HeadSnapshot {
                    value,
                    version: Some(version),
                }))
            }
            Err(CoreError::NotFound(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn empty_head(&self, name: &str) -> HeadSnapshot {
        HeadSnapshot {
            value: RefValue::new(&self.tenant, name),
            version: None,
        }
    }

    pub(crate) async fn publish<P: RefMutationPlan>(
        &self,
        identity: OpIdentity,
        plan: P,
    ) -> Result<Published<P::Outcome>> {
        self.reject_v1(plan.resource()).await?;
        let request = plan
            .material()
            .hash(&self.key, &self.tenant, plan.resource());
        for _ in 0..MAX_PUBLISH_ATTEMPTS {
            let now = self.clock().now();
            let loaded = self.load_intent(&identity).await?;
            identity.check_time(now, &self.policy(), loaded.is_some())?;
            if let Some((intent, version)) = loaded {
                intent.validate()?;
                self.check_intent(&intent, &identity, plan.resource(), &request)?;
                match &intent.state {
                    IntentState::Applied {
                        generation, commit, ..
                    } => {
                        if let Some(p) = self
                            .try_cached_applied::<P::Outcome>(
                                plan.resource(),
                                &identity,
                                &request,
                                *generation,
                                commit,
                            )
                            .await?
                        {
                            return Ok(p);
                        }
                        if let Some(p) = self
                            .recover_without_intent::<P::Outcome>(
                                &identity,
                                &request,
                                plan.resource(),
                            )
                            .await?
                        {
                            return Ok(p);
                        }
                        let snapshot =
                            self.read_head(plan.resource()).await?.unwrap_or_else(|| {
                                HeadSnapshot {
                                    value: RefValue::new(&self.tenant, plan.resource()),
                                    version: None,
                                }
                            });
                        let mut pending = intent.clone();
                        pending.state = IntentState::Pending;
                        pending.base_generation = snapshot.value.generation;
                        match self
                            .resolve_pending::<P>(&identity, &pending, &request, plan.resource())
                            .await?
                        {
                            PendingResolution::Done(p) => return Ok(p),
                            PendingResolution::Attempt { base } => {
                                match self
                                    .attempt(&identity, &pending, version, base, &plan, &request)
                                    .await?
                                {
                                    AttemptResult::Done(p) => return Ok(p),
                                    AttemptResult::Retry => continue,
                                }
                            }
                        }
                    }
                    IntentState::Pending => {
                        match self
                            .resolve_pending::<P>(&identity, &intent, &request, plan.resource())
                            .await?
                        {
                            PendingResolution::Done(p) => return Ok(p),
                            PendingResolution::Attempt { base } => {
                                match self
                                    .attempt(&identity, &intent, version, base, &plan, &request)
                                    .await?
                                {
                                    AttemptResult::Done(p) => return Ok(p),
                                    AttemptResult::Retry => continue,
                                }
                            }
                        }
                    }
                }
            } else {
                if let Some(p) = self
                    .recover_without_intent::<P::Outcome>(&identity, &request, plan.resource())
                    .await?
                {
                    return Ok(p);
                }
                let snapshot =
                    self.read_head(plan.resource())
                        .await?
                        .unwrap_or_else(|| HeadSnapshot {
                            value: RefValue::new(&self.tenant, plan.resource()),
                            version: None,
                        });
                let intent = OpIntent::pending(
                    &identity,
                    plan.resource(),
                    request.clone(),
                    snapshot.value.generation,
                    intent_expiry(&identity, &self.policy()),
                );
                let body = serde_json::to_vec_pretty(&intent)?;
                match self
                    .backend
                    .put_update(&self.intent_key(&identity), None, &body)
                    .await
                {
                    Ok(_)
                    | Err(CoreError::AlreadyExists(_))
                    | Err(CoreError::PreconditionFailed(_)) => {
                        continue;
                    }
                    Err(CoreError::BackendUnavailable(_)) => continue,
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Err(anyhow!(
            "publication did not converge after {MAX_PUBLISH_ATTEMPTS} attempts"
        ))
    }

    /// Best-effort intent create used by group commit so every producer is
    /// named before the shared ref CAS.
    pub(crate) async fn ensure_pending_intent(
        &self,
        identity: &OpIdentity,
        resource: &str,
        request: Digest,
        base_generation: u64,
    ) -> Result<IntentAdmission<serde_json::Value>> {
        self.reject_v1(resource).await?;
        for _ in 0..MAX_PUBLISH_ATTEMPTS {
            let now = self.clock().now();
            let loaded = self.load_intent(identity).await?;
            identity.check_time(now, &self.policy(), loaded.is_some())?;
            if let Some((mut intent, version)) = loaded {
                intent.validate()?;
                self.check_intent(&intent, identity, resource, &request)?;
                match intent.state.clone() {
                    IntentState::Applied {
                        generation, commit, ..
                    } => {
                        if let Some(p) = self
                            .try_cached_applied::<serde_json::Value>(
                                resource, identity, &request, generation, &commit,
                            )
                            .await?
                        {
                            return Ok(IntentAdmission::Applied {
                                generation: p.generation,
                                commit: p.commit,
                                result: p.outcome,
                            });
                        }
                        if let Some(p) = self
                            .recover_without_intent::<serde_json::Value>(
                                identity, &request, resource,
                            )
                            .await?
                        {
                            return Ok(IntentAdmission::Applied {
                                generation: p.generation,
                                commit: p.commit,
                                result: p.outcome,
                            });
                        }
                        intent.state = IntentState::Pending;
                        let snapshot =
                            self.read_head(resource)
                                .await?
                                .unwrap_or_else(|| HeadSnapshot {
                                    value: RefValue::new(&self.tenant, resource),
                                    version: None,
                                });
                        intent.base_generation = snapshot.value.generation;
                        let body = serde_json::to_vec_pretty(&intent)?;
                        match self
                            .backend
                            .put_update(&self.intent_key(identity), version.as_ref(), &body)
                            .await
                        {
                            Ok(_) => {
                                return Ok(IntentAdmission::Pending {
                                    base: intent.base_generation,
                                });
                            }
                            Err(CoreError::PreconditionFailed(_))
                            | Err(CoreError::AlreadyExists(_)) => continue,
                            Err(e) => return Err(e.into()),
                        }
                    }
                    IntentState::Pending => {
                        let snapshot =
                            self.read_head(resource)
                                .await?
                                .unwrap_or_else(|| HeadSnapshot {
                                    value: RefValue::new(&self.tenant, resource),
                                    version: None,
                                });
                        let head = snapshot.value.generation;
                        if head == intent.base_generation {
                            return Ok(IntentAdmission::Pending {
                                base: intent.base_generation,
                            });
                        }
                        if head < intent.base_generation {
                            return Err(CoreError::RecoveryFailed(format!(
                                "ref {resource} generation {head} is behind intent base {}",
                                intent.base_generation
                            ))
                            .into());
                        }
                        let target = intent
                            .base_generation
                            .checked_add(1)
                            .ok_or_else(|| CoreError::Rejected("generation overflow".into()))?;
                        let Some(head_commit) = snapshot.value.head_commit.clone() else {
                            return Err(CoreError::RecoveryFailed(format!(
                                "ref {resource} advanced past {} without a commit",
                                intent.base_generation
                            ))
                            .into());
                        };
                        let (view, _) =
                            self.seek_generation(&head_commit, target, resource).await?;
                        if identity_in_commit(&view, identity) {
                            let outcome = outcome_from_view::<serde_json::Value>(&view, identity)?;
                            let _ = self
                                .finalize_intent(
                                    identity,
                                    view.header.generation,
                                    view.digest.clone(),
                                    outcome.clone(),
                                )
                                .await;
                            return Ok(IntentAdmission::Applied {
                                generation: view.header.generation,
                                commit: view.digest,
                                result: outcome,
                            });
                        }
                        intent.base_generation = head;
                        let body = serde_json::to_vec_pretty(&intent)?;
                        match self
                            .backend
                            .put_update(&self.intent_key(identity), version.as_ref(), &body)
                            .await
                        {
                            Ok(_) => {
                                return Ok(IntentAdmission::Pending { base: head });
                            }
                            Err(CoreError::PreconditionFailed(_))
                            | Err(CoreError::AlreadyExists(_)) => continue,
                            Err(e) => return Err(e.into()),
                        }
                    }
                }
            }
            if let Some(p) = self
                .recover_without_intent::<serde_json::Value>(identity, &request, resource)
                .await?
            {
                return Ok(IntentAdmission::Applied {
                    generation: p.generation,
                    commit: p.commit,
                    result: p.outcome,
                });
            }
            let intent = OpIntent::pending(
                identity,
                resource,
                request.clone(),
                base_generation,
                intent_expiry(identity, &self.policy()),
            );
            let body = serde_json::to_vec_pretty(&intent)?;
            match self
                .backend
                .put_update(&self.intent_key(identity), None, &body)
                .await
            {
                Ok(_) => {
                    return Ok(IntentAdmission::Pending {
                        base: base_generation,
                    })
                }
                Err(CoreError::AlreadyExists(_)) | Err(CoreError::PreconditionFailed(_)) => {
                    continue
                }
                Err(CoreError::BackendUnavailable(_)) => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Err(anyhow!("intent admission did not converge"))
    }

    pub(crate) async fn finalize_intent(
        &self,
        identity: &OpIdentity,
        generation: u64,
        commit: Digest,
        result: serde_json::Value,
    ) -> Result<()> {
        let Some((mut intent, version)) = self.load_intent(identity).await? else {
            return Ok(());
        };
        intent.state = IntentState::Applied {
            generation,
            commit,
            result,
        };
        let body = serde_json::to_vec_pretty(&intent)?;
        let _ = self
            .backend
            .put_update(&self.intent_key(identity), version.as_ref(), &body)
            .await;
        Ok(())
    }

    async fn load_intent(
        &self,
        identity: &OpIdentity,
    ) -> Result<Option<(OpIntent, Option<Version>)>> {
        match self.backend.get(&self.intent_key(identity)).await {
            Ok((bytes, version)) => {
                let intent: OpIntent = serde_json::from_slice(&bytes).map_err(|e| {
                    CoreError::RecoveryFailed(format!(
                        "malformed intent {}: {e}",
                        identity.canonical()
                    ))
                })?;
                intent.validate().map_err(|e| {
                    CoreError::RecoveryFailed(format!("intent {}: {e}", identity.canonical()))
                })?;
                if intent.identity != identity.canonical() {
                    return Err(CoreError::RecoveryFailed(format!(
                        "intent {} stored identity {}",
                        identity.canonical(),
                        intent.identity
                    ))
                    .into());
                }
                Ok(Some((intent, Some(version))))
            }
            Err(CoreError::NotFound(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn resolve_pending<P: RefMutationPlan>(
        &self,
        identity: &OpIdentity,
        intent: &OpIntent,
        request: &Digest,
        resource: &str,
    ) -> Result<PendingResolution<P::Outcome>> {
        let snapshot = self
            .read_head(resource)
            .await?
            .unwrap_or_else(|| HeadSnapshot {
                value: RefValue::new(&self.tenant, resource),
                version: None,
            });
        let head_gen = snapshot.value.generation;
        if head_gen == intent.base_generation {
            return Ok(PendingResolution::Attempt {
                base: intent.base_generation,
            });
        }
        if head_gen < intent.base_generation {
            return Err(CoreError::RecoveryFailed(format!(
                "ref {} generation {head_gen} is behind intent base {}",
                resource, intent.base_generation
            ))
            .into());
        }
        let Some(head_commit) = snapshot.value.head_commit.clone() else {
            return Err(CoreError::RecoveryFailed(format!(
                "ref {resource} advanced past {} without a commit",
                intent.base_generation
            ))
            .into());
        };
        let target = intent
            .base_generation
            .checked_add(1)
            .ok_or_else(|| CoreError::Rejected("generation overflow".into()))?;
        let (view, _reads) = self.seek_generation(&head_commit, target, resource).await?;
        if view.header.generation != target {
            return Err(CoreError::RecoveryFailed(format!(
                "seek for generation {target} landed on {}",
                view.header.generation
            ))
            .into());
        }
        if view.header.resource != resource {
            return Err(CoreError::RecoveryFailed(format!(
                "commit at {target} belongs to resource {}",
                view.header.resource
            ))
            .into());
        }
        if identity_in_commit(&view, identity) {
            if view.header.request != *request
                && !view
                    .admitted
                    .iter()
                    .any(|a| a.identity == identity.canonical() && a.request == *request)
            {
                return Err(CoreError::IdempotencyConflict {
                    id: identity.canonical(),
                    original: view.header.request.to_string(),
                    supplied: request.to_string(),
                }
                .into());
            }
            let published =
                published_from_view::<P::Outcome>(&view, identity, &self.tenant, false)?;
            let _ = self
                .finalize_intent(
                    identity,
                    published.generation,
                    published.commit.clone(),
                    serde_json::to_value(&published.outcome)?,
                )
                .await;
            return Ok(PendingResolution::Done(published));
        }
        Ok(PendingResolution::Attempt { base: head_gen })
    }

    async fn attempt<P: RefMutationPlan>(
        &self,
        identity: &OpIdentity,
        intent: &OpIntent,
        intent_version: Option<Version>,
        base: u64,
        plan: &P,
        request: &Digest,
    ) -> Result<AttemptResult<P::Outcome>> {
        let snapshot = self
            .read_head(plan.resource())
            .await?
            .unwrap_or_else(|| HeadSnapshot {
                value: RefValue::new(&self.tenant, plan.resource()),
                version: None,
            });
        if snapshot.value.generation != base {
            return Ok(AttemptResult::Retry);
        }
        if snapshot.value.generation > 0 && snapshot.value.head_commit.is_none() {
            return Err(
                CoreError::RecoveryFailed("head has generation but no commit".into()).into(),
            );
        }
        let now = self.clock().now();
        let generation = snapshot
            .value
            .generation
            .checked_add(1)
            .ok_or_else(|| CoreError::Rejected("generation overflow".into()))?;
        let parent = snapshot.value.head_commit.clone();
        let skip = self
            .compute_skip(generation, parent.as_ref(), plan.resource())
            .await?;
        let ctx = PrepareCtx {
            store: self,
            snapshot: &snapshot,
            identity,
            request: request.clone(),
            generation,
            parent,
            skip,
            now,
        };
        let prepared = plan.prepare(ctx).await?;
        if prepared.next.generation != generation {
            return Err(anyhow!(
                "plan set generation {} want {generation}",
                prepared.next.generation
            ));
        }

        let mut intent = intent.clone();
        intent.base_generation = base;
        intent.proposed = Vec::new();
        let mut uploaded: Vec<(Digest, Vec<u8>, ObjectKind, String)> = Vec::new();
        for u in &prepared.uploads {
            let env = Envelope::new(
                &self.tenant,
                u.kind,
                &u.schema,
                u.payload.clone(),
                &self.key,
            );
            uploaded.push((
                env.meta.digest.clone(),
                env.encode()?,
                u.kind,
                u.schema.clone(),
            ));
            intent.proposed.push(env.meta.digest.clone());
        }
        let commit_bytes_and_kind: (Vec<u8>, ObjectKind, String) =
            if let Some(idx) = prepared.commit_upload {
                let u = prepared
                    .uploads
                    .get(idx)
                    .ok_or_else(|| anyhow!("commit_upload out of range"))?;
                (u.payload.clone(), u.kind, u.schema.clone())
            } else {
                let header = CommitHeader {
                    schema: HEADER_SCHEMA.into(),
                    resource: plan.resource().into(),
                    generation,
                    epoch: prepared.next.epoch,
                    identity: identity.canonical(),
                    request: request.clone(),
                    parent: snapshot.value.head_commit.clone(),
                    skip: self
                        .compute_skip(
                            generation,
                            snapshot.value.head_commit.as_ref(),
                            plan.resource(),
                        )
                        .await?,
                    at: now,
                };
                header.validate()?;
                let commit = Commit {
                    schema: COMMIT_SCHEMA.into(),
                    header,
                    change: prepared.change.clone(),
                    result: serde_json::to_value(&prepared.outcome)?,
                    ref_state: durable_ref_state(&prepared.next),
                };
                (
                    serde_json::to_vec(&commit)?,
                    ObjectKind::Blob,
                    COMMIT_SCHEMA.into(),
                )
            };
        if prepared.commit_upload.is_none() {
            let env = Envelope::new(
                &self.tenant,
                commit_bytes_and_kind.1,
                &commit_bytes_and_kind.2,
                commit_bytes_and_kind.0.clone(),
                &self.key,
            );
            uploaded.push((
                env.meta.digest.clone(),
                env.encode()?,
                commit_bytes_and_kind.1,
                commit_bytes_and_kind.2.clone(),
            ));
            intent.proposed.push(env.meta.digest.clone());
        }

        let intent_body = serde_json::to_vec_pretty(&intent)?;
        match self
            .backend
            .put_update(
                &self.intent_key(identity),
                intent_version.as_ref(),
                &intent_body,
            )
            .await
        {
            Ok(_) => {}
            Err(CoreError::PreconditionFailed(_)) | Err(CoreError::AlreadyExists(_)) => {
                return Ok(AttemptResult::Retry);
            }
            Err(e) => return Err(e.into()),
        }

        for (digest, bytes, _, _) in &uploaded {
            match self
                .backend
                .put_create(&self.object_key(digest), bytes)
                .await
            {
                Ok(_) | Err(CoreError::AlreadyExists(_)) => {}
                Err(e) => return Err(e.into()),
            }
            self.cache_write(digest, bytes);
        }

        let commit_digest = if let Some(idx) = prepared.commit_upload {
            uploaded[idx].0.clone()
        } else {
            uploaded
                .last()
                .map(|u| u.0.clone())
                .ok_or_else(|| anyhow!("no commit object"))?
        };

        if !self
            .sync_companions(&prepared.companions, base, &intent.proposed)
            .await?
        {
            return Ok(AttemptResult::Retry);
        }

        let mut next = prepared.next;
        next.head_commit = Some(commit_digest.clone());
        if prepared.commit_upload.is_some() {
            next.target = Some(commit_digest.clone());
        }
        next.updated_at = now;
        next.schema = RefValue::SCHEMA.into();
        let ref_bytes = serde_json::to_vec_pretty(&next)?;
        match self
            .backend
            .put_update(
                &self.ref_key(plan.resource()),
                snapshot.version.as_ref(),
                &ref_bytes,
            )
            .await
        {
            Ok(_) => {
                let leader_result = serde_json::to_value(&prepared.outcome)?;
                let _ = self
                    .finalize_intent(identity, generation, commit_digest.clone(), leader_result)
                    .await;
                for c in &prepared.companions {
                    let _ = self
                        .finalize_intent(
                            &c.identity,
                            generation,
                            commit_digest.clone(),
                            c.result.clone(),
                        )
                        .await;
                }
                Ok(AttemptResult::Done(Published {
                    outcome: prepared.outcome,
                    generation,
                    epoch: next.epoch,
                    commit: commit_digest,
                    value: next,
                    first_delivery: true,
                    admitted: prepared.admitted,
                }))
            }
            Err(CoreError::PreconditionFailed(_)) | Err(CoreError::AlreadyExists(_)) => {
                Ok(AttemptResult::Retry)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn sync_companions(
        &self,
        companions: &[Companion],
        base: u64,
        proposed: &[Digest],
    ) -> Result<bool> {
        for c in companions {
            let Some((mut intent, version)) = self.load_intent(&c.identity).await? else {
                return Err(CoreError::RecoveryFailed(format!(
                    "companion intent {} missing before ref CAS",
                    c.identity
                ))
                .into());
            };
            if matches!(intent.state, IntentState::Applied { .. }) {
                return Ok(false);
            }
            if intent.base_generation != base {
                return Ok(false);
            }
            intent.proposed = proposed.to_vec();
            let body = serde_json::to_vec_pretty(&intent)?;
            match self
                .backend
                .put_update(&self.intent_key(&c.identity), version.as_ref(), &body)
                .await
            {
                Ok(_) => {}
                Err(CoreError::PreconditionFailed(_)) | Err(CoreError::AlreadyExists(_)) => {
                    return Ok(false);
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(true)
    }

    /// Intent-free publication against a caller-supplied snapshot. Used by the
    /// stable-key path: membership is the index, not an intent row.
    pub(crate) async fn commit_at_snapshot<P: RefMutationPlan>(
        &self,
        identity: OpIdentity,
        plan: P,
        snapshot: HeadSnapshot,
    ) -> Result<CasResult<P::Outcome>> {
        self.reject_v1(plan.resource()).await?;
        if snapshot.value.generation > 0 && snapshot.value.head_commit.is_none() {
            return Err(
                CoreError::RecoveryFailed("head has generation but no commit".into()).into(),
            );
        }
        let now = self.clock().now();
        let generation = snapshot
            .value
            .generation
            .checked_add(1)
            .ok_or_else(|| CoreError::Rejected("generation overflow".into()))?;
        let parent = snapshot.value.head_commit.clone();
        let skip = self
            .compute_skip(generation, parent.as_ref(), plan.resource())
            .await?;
        let request = plan
            .material()
            .hash(&self.key, &self.tenant, plan.resource());
        let ctx = PrepareCtx {
            store: self,
            snapshot: &snapshot,
            identity: &identity,
            request: request.clone(),
            generation,
            parent,
            skip,
            now,
        };
        let prepared = plan.prepare(ctx).await?;
        let mut uploaded: Vec<(Digest, Vec<u8>)> = Vec::new();
        for u in &prepared.uploads {
            let env = Envelope::new(
                &self.tenant,
                u.kind,
                &u.schema,
                u.payload.clone(),
                &self.key,
            );
            uploaded.push((env.meta.digest.clone(), env.encode()?));
        }
        if prepared.commit_upload.is_none() {
            let header = CommitHeader {
                schema: HEADER_SCHEMA.into(),
                resource: plan.resource().into(),
                generation,
                epoch: prepared.next.epoch,
                identity: identity.canonical(),
                request,
                parent: snapshot.value.head_commit.clone(),
                skip: self
                    .compute_skip(
                        generation,
                        snapshot.value.head_commit.as_ref(),
                        plan.resource(),
                    )
                    .await?,
                at: now,
            };
            header.validate()?;
            let commit = Commit {
                schema: COMMIT_SCHEMA.into(),
                header,
                change: prepared.change.clone(),
                result: serde_json::to_value(&prepared.outcome)?,
                ref_state: durable_ref_state(&prepared.next),
            };
            let env = Envelope::new(
                &self.tenant,
                ObjectKind::Blob,
                COMMIT_SCHEMA,
                serde_json::to_vec(&commit)?,
                &self.key,
            );
            uploaded.push((env.meta.digest.clone(), env.encode()?));
        }
        for (digest, bytes) in &uploaded {
            match self
                .backend
                .put_create(&self.object_key(digest), bytes)
                .await
            {
                Ok(_) | Err(CoreError::AlreadyExists(_)) => {}
                Err(e) => return Err(e.into()),
            }
            self.cache_write(digest, bytes);
        }
        let commit_digest = if let Some(idx) = prepared.commit_upload {
            uploaded[idx].0.clone()
        } else {
            uploaded
                .last()
                .map(|u| u.0.clone())
                .ok_or_else(|| anyhow!("no commit object"))?
        };
        let mut next = prepared.next;
        next.head_commit = Some(commit_digest.clone());
        if prepared.commit_upload.is_some() {
            next.target = Some(commit_digest.clone());
        }
        next.updated_at = now;
        next.schema = RefValue::SCHEMA.into();
        let ref_bytes = serde_json::to_vec_pretty(&next)?;
        match self
            .backend
            .put_update(
                &self.ref_key(plan.resource()),
                snapshot.version.as_ref(),
                &ref_bytes,
            )
            .await
        {
            Ok(_) => Ok(CasResult::Committed(Published {
                outcome: prepared.outcome,
                generation,
                epoch: next.epoch,
                commit: commit_digest,
                value: next,
                first_delivery: true,
                admitted: prepared.admitted,
            })),
            Err(CoreError::PreconditionFailed(_)) | Err(CoreError::AlreadyExists(_)) => {
                Ok(CasResult::Conflict)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn compute_skip(
        &self,
        generation: u64,
        parent: Option<&Digest>,
        resource: &str,
    ) -> Result<Option<Digest>> {
        let target = skip_target_generation(generation);
        if target == 0 {
            return Ok(None);
        }
        let Some(parent) = parent else {
            return Ok(None);
        };
        if target == generation.saturating_sub(1) {
            return Ok(Some(parent.clone()));
        }
        let (view, _) = self.seek_generation(parent, target, resource).await?;
        Ok(Some(view.digest))
    }

    /// Walk skip pointers then parents until `target` generation. Returns the
    /// commit and the number of commit objects read (for tests).
    pub(crate) async fn seek_generation(
        &self,
        start: &Digest,
        target: u64,
        resource: &str,
    ) -> Result<(CommitView, u64)> {
        let mut current = self.load_commit_view(start).await?;
        self.check_view_resource(&current, resource)?;
        let mut reads = 1u64;
        let mut hops = 0u32;
        while current.header.generation != target {
            hops += 1;
            if hops > MAX_SEEK_HOPS {
                return Err(CoreError::RecoveryFailed(format!(
                    "seek from {} to {target} exceeded {MAX_SEEK_HOPS} hops",
                    current.header.generation
                ))
                .into());
            }
            if current.header.generation < target {
                return Err(CoreError::RecoveryFailed(format!(
                    "commit generation {} is below target {target}",
                    current.header.generation
                ))
                .into());
            }
            let mut followed_skip = false;
            if let Some(skip_d) = &current.header.skip {
                let skip_view = self.load_commit_view(skip_d).await.map_err(|e| {
                    CoreError::RecoveryFailed(format!("skip target {}: {e:#}", skip_d))
                })?;
                reads += 1;
                self.check_view_resource(&skip_view, resource)?;
                if skip_view.header.generation >= current.header.generation {
                    return Err(CoreError::RecoveryFailed(
                        "skip pointer does not strictly decrease generation".into(),
                    )
                    .into());
                }
                let claimed = skip_target_generation(current.header.generation);
                if skip_view.header.generation != claimed {
                    return Err(CoreError::RecoveryFailed(format!(
                        "skip claimed generation {claimed}, found {}",
                        skip_view.header.generation
                    ))
                    .into());
                }
                if skip_view.header.generation >= target {
                    current = skip_view;
                    followed_skip = true;
                }
            }
            if followed_skip {
                continue;
            }
            let Some(parent) = current.header.parent.clone() else {
                return Err(CoreError::RecoveryFailed(format!(
                    "commit generation {} missing parent while seeking {target}",
                    current.header.generation
                ))
                .into());
            };
            let parent_view = self
                .load_commit_view(&parent)
                .await
                .map_err(|e| CoreError::RecoveryFailed(format!("parent {parent}: {e:#}")))?;
            reads += 1;
            self.check_view_resource(&parent_view, resource)?;
            if parent_view.header.generation.checked_add(1) != Some(current.header.generation) {
                return Err(CoreError::RecoveryFailed(format!(
                    "dense ancestry broken: parent {} then {}",
                    parent_view.header.generation, current.header.generation
                ))
                .into());
            }
            current = parent_view;
        }
        Ok((current, reads))
    }

    pub(crate) async fn load_commit_view(&self, digest: &Digest) -> Result<CommitView> {
        let (payload, _) = self
            .get_blob(digest)
            .await
            .map_err(|e| CoreError::RecoveryFailed(format!("commit {digest} unreadable: {e:#}")))?;
        let value: serde_json::Value = serde_json::from_slice(&payload)
            .map_err(|e| CoreError::RecoveryFailed(format!("commit {digest} is not JSON: {e}")))?;
        let schema = value
            .get("schema")
            .and_then(|s| s.as_str())
            .ok_or_else(|| CoreError::RecoveryFailed(format!("object {digest} has no schema")))?;
        if schema == COMMIT_SCHEMA {
            let commit: Commit = serde_json::from_value(value)
                .map_err(|e| CoreError::RecoveryFailed(format!("commit {digest} schema: {e}")))?;
            commit.validate()?;
            return Ok(CommitView {
                digest: digest.clone(),
                header: commit.header,
                result: commit.result,
                admitted: Vec::new(),
                ref_state: Some(commit.ref_state),
                target_follows_commit: false,
            });
        }
        if schema != LOG_MANIFEST_SCHEMA && schema != LOG_MANIFEST_SCHEMA_V3 {
            return Err(CoreError::RecoveryFailed(format!(
                "object {digest} has unknown commit schema {schema}"
            ))
            .into());
        }
        let header_v = value.get("header").cloned().ok_or_else(|| {
            CoreError::RecoveryFailed(format!("manifest {digest} has no commit header"))
        })?;
        let header: CommitHeader = serde_json::from_value(header_v)
            .map_err(|e| CoreError::RecoveryFailed(format!("header in {digest}: {e}")))?;
        header.validate()?;
        let admitted = match value.get("admitted") {
            Some(v) => serde_json::from_value(v.clone())
                .map_err(|e| CoreError::RecoveryFailed(format!("admitted in {digest}: {e}")))?,
            None => Vec::new(),
        };
        let result = value
            .get("result")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let ref_state = match value.get("ref_state") {
            Some(v) if !v.is_null() => {
                Some(serde_json::from_value(v.clone()).map_err(|e| {
                    CoreError::RecoveryFailed(format!("ref_state in {digest}: {e}"))
                })?)
            }
            _ => None,
        };
        Ok(CommitView {
            digest: digest.clone(),
            header,
            result,
            admitted,
            ref_state,
            target_follows_commit: true,
        })
    }

    pub async fn history_chain(&self, name: &str, max: usize) -> Result<Vec<HistoryEntry>> {
        let Some(head) = self.read_head(name).await? else {
            return Ok(Vec::new());
        };
        let Some(mut digest) = head.value.head_commit.clone() else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        while out.len() < max {
            let view = self.load_commit_view(&digest).await?;
            if view.header.resource != name {
                return Err(CoreError::RecoveryFailed(format!(
                    "history for {name} includes resource {}",
                    view.header.resource
                ))
                .into());
            }
            out.push(HistoryEntry {
                generation: view.header.generation,
                epoch: view.header.epoch,
                identity: view.header.identity.clone(),
                request: view.header.request.clone(),
                target: head.value.target.clone(),
                at: view.header.at,
                commit: view.digest.clone(),
            });
            match view.header.parent {
                Some(p) => digest = p,
                None => break,
            }
        }
        Ok(out)
    }

    /// Lease renewal: fresh snapshot, no generation bump, no new commit.
    pub async fn renew_lease(&self, name: &str, fence: u64, ttl_secs: i64) -> Result<RefValue> {
        self.reject_v1(name).await?;
        for _ in 0..16 {
            let now = self.clock().now();
            let snapshot = self
                .read_head(name)
                .await?
                .ok_or_else(|| anyhow!("ref {name} does not exist"))?;
            if snapshot.value.epoch != fence {
                return Err(CoreError::Fenced {
                    caller: fence,
                    live: snapshot.value.epoch,
                }
                .into());
            }
            let writer = snapshot
                .value
                .lease
                .as_ref()
                .map(|l| l.writer.clone())
                .ok_or_else(|| anyhow!("ref {name} has no lease to renew"))?;
            let mut next = snapshot.value.clone();
            next.lease = Some(comb_core::Lease {
                writer,
                lease_until: now + chrono::Duration::seconds(ttl_secs),
            });
            next.updated_at = now;
            let bytes = serde_json::to_vec_pretty(&next)?;
            match self
                .backend
                .put_update(&self.ref_key(name), snapshot.version.as_ref(), &bytes)
                .await
            {
                Ok(_) => return Ok(next),
                Err(CoreError::PreconditionFailed(_)) | Err(CoreError::AlreadyExists(_)) => {
                    continue
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(anyhow!("renew: lost the ref race 16 times"))
    }

    fn check_intent(
        &self,
        intent: &OpIntent,
        identity: &OpIdentity,
        resource: &str,
        request: &Digest,
    ) -> Result<()> {
        if intent.identity != identity.canonical() {
            return Err(CoreError::RecoveryFailed(format!(
                "intent {} stored identity {}",
                identity.canonical(),
                intent.identity
            ))
            .into());
        }
        if intent.resource != resource || intent.request != *request {
            return Err(CoreError::IdempotencyConflict {
                id: identity.canonical(),
                original: intent.request.to_string(),
                supplied: request.to_string(),
            }
            .into());
        }
        Ok(())
    }

    fn check_view_resource(&self, view: &CommitView, resource: &str) -> Result<()> {
        if view.header.resource != resource {
            return Err(CoreError::RecoveryFailed(format!(
                "commit {} belongs to resource {}",
                view.digest, view.header.resource
            ))
            .into());
        }
        Ok(())
    }

    /// Applied is a cache. The blob named by `commit` may have been uploaded
    /// before the ref CAS, so existence is not publication. The digest must
    /// equal the canonical commit at `generation` in live head history.
    async fn try_cached_applied<T: DeserializeOwned>(
        &self,
        resource: &str,
        identity: &OpIdentity,
        request: &Digest,
        generation: u64,
        commit: &Digest,
    ) -> Result<Option<Published<T>>> {
        let Some(head) = self.read_head(resource).await? else {
            return Ok(None);
        };
        if generation == 0 || generation > head.value.generation {
            return Ok(None);
        }
        let Some(head_commit) = head.value.head_commit.clone() else {
            return Ok(None);
        };
        let (view, _) = self
            .seek_generation(&head_commit, generation, resource)
            .await?;
        if &view.digest != commit {
            return Ok(None);
        }
        if view.header.generation != generation || view.header.resource != resource {
            return Ok(None);
        }
        if view.header.request != *request
            && !view
                .admitted
                .iter()
                .any(|a| a.identity == identity.canonical() && a.request == *request)
        {
            return Ok(None);
        }
        if !identity_in_commit(&view, identity) {
            return Ok(None);
        }
        match published_from_view(&view, identity, &self.tenant, false) {
            Ok(p) => Ok(Some(p)),
            Err(_) => Ok(None),
        }
    }

    async fn recover_without_intent<T: DeserializeOwned>(
        &self,
        identity: &OpIdentity,
        request: &Digest,
        resource: &str,
    ) -> Result<Option<Published<T>>> {
        let Some(head) = self.read_head(resource).await? else {
            return Ok(None);
        };
        let Some(mut digest) = head.value.head_commit.clone() else {
            return Ok(None);
        };
        let mut hops = 0u32;
        loop {
            hops += 1;
            if hops > MAX_SEEK_HOPS {
                return Err(CoreError::RecoveryFailed(format!(
                    "scan for {} on {resource} exceeded {MAX_SEEK_HOPS} hops",
                    identity.canonical()
                ))
                .into());
            }
            let view = self.load_commit_view(&digest).await?;
            self.check_view_resource(&view, resource)?;
            if identity_in_commit(&view, identity) {
                if view.header.request != *request
                    && !view
                        .admitted
                        .iter()
                        .any(|a| a.identity == identity.canonical() && a.request == *request)
                {
                    return Err(CoreError::IdempotencyConflict {
                        id: identity.canonical(),
                        original: view.header.request.to_string(),
                        supplied: request.to_string(),
                    }
                    .into());
                }
                return Ok(Some(published_from_view(
                    &view,
                    identity,
                    &self.tenant,
                    false,
                )?));
            }
            match view.header.parent {
                Some(p) => digest = p,
                None => return Ok(None),
            }
        }
    }
}

#[allow(dead_code)]
pub(crate) enum IntentAdmission<T> {
    Applied {
        generation: u64,
        commit: Digest,
        result: T,
    },
    Pending {
        base: u64,
    },
}

enum PendingResolution<T> {
    Done(Published<T>),
    Attempt { base: u64 },
}

enum AttemptResult<T> {
    Done(Published<T>),
    Retry,
}

pub(crate) enum CasResult<T> {
    Committed(Published<T>),
    Conflict,
}

fn identity_in_commit(view: &CommitView, identity: &OpIdentity) -> bool {
    let id = identity.canonical();
    view.header.identity == id || view.admitted.iter().any(|a| a.identity == id)
}

fn outcome_from_view<T: DeserializeOwned>(view: &CommitView, identity: &OpIdentity) -> Result<T> {
    let id = identity.canonical();
    if view.header.identity == id && !view.result.is_null() {
        return Ok(serde_json::from_value(view.result.clone())
            .map_err(|e| CoreError::RecoveryFailed(format!("commit result for {id}: {e}")))?);
    }
    if let Some(a) = view.admitted.iter().find(|a| a.identity == id) {
        let value = serde_json::json!({ "first": a.first, "last": a.last });
        return Ok(serde_json::from_value(value)
            .map_err(|e| CoreError::RecoveryFailed(format!("admission result for {id}: {e}")))?);
    }
    Err(CoreError::RecoveryFailed(format!("commit does not record {id}")).into())
}

fn intent_expiry(identity: &OpIdentity, policy: &OperationPolicy) -> Option<DateTime<Utc>> {
    match identity {
        OpIdentity::Generic(op) => Some(op.expires_at(policy)),
        OpIdentity::Stable(_) => None,
    }
}

fn durable_ref_state(next: &RefValue) -> RefValue {
    let mut value = next.clone();
    value.head_commit = None;
    value
}

fn published_from_view<T: DeserializeOwned>(
    view: &CommitView,
    identity: &OpIdentity,
    tenant: &str,
    first_delivery: bool,
) -> Result<Published<T>> {
    let outcome = outcome_from_view(view, identity)?;
    let mut value = match &view.ref_state {
        Some(v) => v.clone(),
        None => {
            return Err(CoreError::RecoveryFailed(format!(
                "commit {} is missing the original ref state",
                view.digest
            ))
            .into());
        }
    };
    if value.tenant != tenant || value.name != view.header.resource {
        return Err(CoreError::RecoveryFailed(format!(
            "commit {} ref state identity {}/{} does not match {tenant}/{}",
            view.digest, value.tenant, value.name, view.header.resource
        ))
        .into());
    }
    if value.generation != view.header.generation || value.epoch != view.header.epoch {
        return Err(CoreError::RecoveryFailed(format!(
            "commit {} ref state generation/epoch {}/{} disagrees with header {}/{}",
            view.digest, value.generation, value.epoch, view.header.generation, view.header.epoch
        ))
        .into());
    }
    value.head_commit = Some(view.digest.clone());
    if view.target_follows_commit {
        value.target = Some(view.digest.clone());
    }
    Ok(Published {
        outcome,
        generation: view.header.generation,
        epoch: view.header.epoch,
        commit: view.digest.clone(),
        value,
        first_delivery,
        admitted: view.admitted.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;
    use comb_core::{
        Admission, Commit, CommitHeader, DigestKey, OpIdentity, COMMIT_SCHEMA, HEADER_SCHEMA,
    };
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

    #[tokio::test]
    async fn seek_rejects_bad_skips_and_foreign_resource() {
        let store = store();
        let op = store.mint_operation();
        let now = chrono::Utc::now();
        let request = store.key.digest(b"req");

        let mk = |generation: u64, resource: &str, parent, skip| {
            let mut ref_state = RefValue::new("org_t", resource);
            ref_state.generation = generation;
            Commit {
                schema: COMMIT_SCHEMA.into(),
                header: CommitHeader {
                    schema: HEADER_SCHEMA.into(),
                    resource: resource.into(),
                    generation,
                    epoch: 0,
                    identity: op.to_string(),
                    request: request.clone(),
                    parent,
                    skip,
                    at: now,
                },
                change: serde_json::json!({"kind": "set-target"}),
                result: serde_json::json!({"generation": generation, "epoch": 0}),
                ref_state,
            }
        };

        let (g1, _) = store
            .put_blob(serde_json::to_vec(&mk(1, "r", None, None)).unwrap())
            .await
            .unwrap();
        let (foreign, _) = store
            .put_blob(serde_json::to_vec(&mk(8, "other", None, None)).unwrap())
            .await
            .unwrap();
        let (same, _) = store
            .put_blob(serde_json::to_vec(&mk(3, "r", Some(g1.clone()), None)).unwrap())
            .await
            .unwrap();
        let (mismatch, _) = store
            .put_blob(serde_json::to_vec(&mk(12, "r", Some(g1.clone()), Some(g1.clone()))).unwrap())
            .await
            .unwrap();

        let err = store.seek_generation(&mismatch, 1, "r").await.unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<CoreError>(),
                Some(CoreError::RecoveryFailed(m)) if m.contains("skip claimed")
            ),
            "{err:#}"
        );

        let (looped, _) = store
            .put_blob(serde_json::to_vec(&mk(3, "r", Some(g1), Some(same))).unwrap())
            .await
            .unwrap();
        let err = store.seek_generation(&looped, 1, "r").await.unwrap_err();
        assert!(
            format!("{err:#}").contains("strictly decrease") || format!("{err:#}").contains("skip"),
            "{err:#}"
        );

        let err = store.seek_generation(&foreign, 8, "r").await.unwrap_err();
        assert!(format!("{err:#}").contains("resource"), "{err:#}");
    }

    #[test]
    fn outcome_from_view_does_not_return_foreign_result() {
        let a = store().mint_operation();
        let b = store().mint_operation();
        let request = DigestKey::from_bytes([9u8; 32]).digest(b"req");
        let view = CommitView {
            digest: request.clone(),
            header: CommitHeader {
                schema: HEADER_SCHEMA.into(),
                resource: "r".into(),
                generation: 1,
                epoch: 0,
                identity: a.to_string(),
                request: request.clone(),
                parent: None,
                skip: None,
                at: chrono::Utc::now(),
            },
            result: serde_json::json!({"generation": 1, "epoch": 0}),
            admitted: vec![Admission {
                identity: b.to_string(),
                request: request.clone(),
                first: 3,
                last: 4,
            }],
            ref_state: None,
            target_follows_commit: false,
        };
        let other = store().mint_operation();
        let err =
            outcome_from_view::<serde_json::Value>(&view, &OpIdentity::Generic(other)).unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<CoreError>(),
                Some(CoreError::RecoveryFailed(_))
            ),
            "{err:#}"
        );
        let leader: serde_json::Value = outcome_from_view(&view, &OpIdentity::Generic(a)).unwrap();
        assert_eq!(leader["generation"], 1);
        let companion: serde_json::Value =
            outcome_from_view(&view, &OpIdentity::Generic(b)).unwrap();
        assert_eq!(companion["first"], 3);
        assert_eq!(companion["last"], 4);
    }

    #[test]
    fn published_from_view_keeps_lease_and_retained_target() {
        let op = store().mint_operation();
        let request = DigestKey::from_bytes([9u8; 32]).digest(b"req");
        let target = DigestKey::from_bytes([2u8; 32]).digest(b"blob");
        let mut ref_state = RefValue::new("org_t", "r");
        ref_state.generation = 2;
        ref_state.epoch = 1;
        ref_state.target = Some(target.clone());
        ref_state.lease = Some(comb_core::Lease {
            writer: "w".into(),
            lease_until: chrono::Utc::now(),
        });
        let view = CommitView {
            digest: request.clone(),
            header: CommitHeader {
                schema: HEADER_SCHEMA.into(),
                resource: "r".into(),
                generation: 2,
                epoch: 1,
                identity: op.to_string(),
                request: request.clone(),
                parent: None,
                skip: None,
                at: chrono::Utc::now(),
            },
            result: serde_json::json!({"generation": 2, "epoch": 1}),
            admitted: Vec::new(),
            ref_state: Some(ref_state.clone()),
            target_follows_commit: false,
        };
        let published = published_from_view::<serde_json::Value>(
            &view,
            &OpIdentity::Generic(op),
            "org_t",
            false,
        )
        .unwrap();
        assert_eq!(published.value.lease, ref_state.lease);
        assert_eq!(published.value.target, Some(target));
        assert_eq!(published.value.head_commit, Some(view.digest));
        assert_ne!(published.value.lease, None);
    }
}
