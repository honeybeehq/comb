//! Crate-private publication engine. Every logical ref CAS goes through here.
//!
//! Recovery truth is the immutable commit chain linked from the ref. The
//! per-identity intent is a CAS-guarded attempt record and a result cache.

use crate::store::Store;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use comb_core::commit::{
    skip_target_generation, Admission, Commit, CommitHeader, HistoryEntry, IntentState, OpIntent,
    COMMIT_SCHEMA, HEADER_SCHEMA, LOG_MANIFEST_SCHEMA,
};
use comb_core::error::CoreError;
use comb_core::operation::{Material, OpIdentity, OperationPolicy};
use comb_core::{Digest, Envelope, ObjectKind, RefValue};
use comb_object::Version;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub const NS_V2: &str = "comb/v2";
pub const NS_V1: &str = "comb/v1";
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
    pub change: serde_json::Value,
    pub admitted: Vec<Admission>,
    pub carrier: String,
}

impl Store {
    pub fn object_key(&self, digest: &Digest) -> String {
        format!(
            "{NS_V2}/tenants/{}/objects/b3k/{}/{}",
            self.tenant,
            digest.key_prefix(),
            digest.hex()
        )
    }

    pub fn ref_key(&self, name: &str) -> String {
        format!("{NS_V2}/tenants/{}/refs/{name}.json", self.tenant)
    }

    pub fn v1_ref_key(&self, name: &str) -> String {
        format!("{NS_V1}/tenants/{}/refs/{name}.json", self.tenant)
    }

    pub fn intent_key(&self, identity: &OpIdentity) -> String {
        match identity {
            OpIdentity::Generic(_) => format!(
                "{NS_V2}/tenants/{}/ops/{}/{}.json",
                self.tenant,
                identity.shard(),
                identity.canonical()
            ),
            OpIdentity::Stable(k) => {
                format!("{NS_V2}/tenants/{}/stable/{}.json", self.tenant, k.to_hex())
            }
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
                if value.tenant != self.tenant {
                    return Err(CoreError::IntegrityError(format!(
                        "ref {name} tenant {} does not match store tenant {}",
                        value.tenant, self.tenant
                    ))
                    .into());
                }
                if value.name != name {
                    return Err(CoreError::IntegrityError(format!(
                        "ref key {name} does not match stored name {}",
                        value.name
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
        if identity.is_stable() {
            return Err(CoreError::Rejected(
                "stable keys do not use the timed intent registry".into(),
            )
            .into());
        }
        self.reject_v1(plan.resource()).await?;
        let request = plan
            .material()
            .hash(&self.key, &self.tenant, plan.resource());
        for _ in 0..MAX_PUBLISH_ATTEMPTS {
            let now = self.clock.now();
            let loaded = self.load_intent(&identity).await?;
            identity.check_time(now, &self.policy, loaded.is_some())?;
            if let Some((intent, version)) = loaded {
                intent.validate()?;
                self.check_intent_binding(&intent, &identity, plan.resource())?;
                if intent.request != request {
                    return Err(CoreError::IdempotencyConflict {
                        id: identity.canonical(),
                        original: intent.request.to_string(),
                        supplied: request.to_string(),
                    }
                    .into());
                }
                match &intent.state {
                    IntentState::Applied {
                        generation,
                        commit,
                        result,
                    } => {
                        return self
                            .published_from_applied(
                                &identity,
                                &request,
                                plan.resource(),
                                *generation,
                                commit,
                                result,
                            )
                            .await;
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
                    intent_expiry(&identity, &self.policy),
                );
                let body = serde_json::to_vec_pretty(&intent)?;
                match self
                    .backend
                    .put_update(&self.intent_key(&identity), None, &body)
                    .await
                {
                    Ok(_)
                    | Err(CoreError::AlreadyExists(_))
                    | Err(CoreError::PreconditionFailed(_))
                    | Err(CoreError::BackendUnavailable(_)) => {
                        continue;
                    }
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
        if identity.is_stable() {
            return Err(CoreError::Rejected(
                "stable keys do not use the timed intent registry".into(),
            )
            .into());
        }
        self.reject_v1(resource).await?;
        for _ in 0..MAX_PUBLISH_ATTEMPTS {
            let now = self.clock.now();
            let loaded = self.load_intent(identity).await?;
            identity.check_time(now, &self.policy, loaded.is_some())?;
            if let Some((intent, _)) = loaded {
                intent.validate()?;
                self.check_intent_binding(&intent, identity, resource)?;
                if intent.request != request {
                    return Err(CoreError::IdempotencyConflict {
                        id: identity.canonical(),
                        original: intent.request.to_string(),
                        supplied: request.to_string(),
                    }
                    .into());
                }
                match intent.state {
                    IntentState::Applied {
                        generation,
                        commit,
                        result,
                    } => {
                        let published = self
                            .published_from_applied::<serde_json::Value>(
                                identity, &request, resource, generation, &commit, &result,
                            )
                            .await?;
                        return Ok(IntentAdmission::Applied {
                            generation: published.generation,
                            commit: published.commit,
                            result: serde_json::to_value(&published.outcome)?,
                        });
                    }
                    IntentState::Pending => {
                        return Ok(IntentAdmission::Pending {
                            base: intent.base_generation,
                        })
                    }
                }
            }
            let intent = OpIntent::pending(
                identity,
                resource,
                request.clone(),
                base_generation,
                intent_expiry(identity, &self.policy),
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
                Err(CoreError::AlreadyExists(_))
                | Err(CoreError::PreconditionFailed(_))
                | Err(CoreError::BackendUnavailable(_)) => continue,
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

    pub(crate) async fn load_intent(
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

    async fn require_readable_head(&self, snapshot: &HeadSnapshot) -> Result<()> {
        if snapshot.value.generation == 0 {
            if snapshot.value.head_commit.is_some() {
                return Err(CoreError::RecoveryFailed(
                    "generation 0 must not carry a head_commit".into(),
                )
                .into());
            }
            return Ok(());
        }
        let Some(digest) = &snapshot.value.head_commit else {
            return Err(
                CoreError::RecoveryFailed("head has generation but no commit".into()).into(),
            );
        };
        let view = self.load_commit_view(digest).await?;
        if view.header.generation != snapshot.value.generation {
            return Err(CoreError::RecoveryFailed(format!(
                "head_commit generation {} does not match ref generation {}",
                view.header.generation, snapshot.value.generation
            ))
            .into());
        }
        if view.header.resource != snapshot.value.name {
            return Err(CoreError::RecoveryFailed(format!(
                "head_commit resource {} does not match {}",
                view.header.resource, snapshot.value.name
            ))
            .into());
        }
        Ok(())
    }

    fn check_intent_binding(
        &self,
        intent: &OpIntent,
        identity: &OpIdentity,
        resource: &str,
    ) -> Result<()> {
        if intent.identity != identity.canonical() {
            return Err(CoreError::RecoveryFailed(format!(
                "intent {} stored identity {}",
                identity.canonical(),
                intent.identity
            ))
            .into());
        }
        if intent.resource != resource {
            return Err(CoreError::IdempotencyConflict {
                id: identity.canonical(),
                original: intent.resource.clone(),
                supplied: resource.into(),
            }
            .into());
        }
        Ok(())
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
        let head_view = self.load_commit_view(&head_commit).await?;
        if head_view.header.generation != head_gen {
            return Err(CoreError::RecoveryFailed(format!(
                "head_commit generation {} does not match ref generation {head_gen}",
                head_view.header.generation
            ))
            .into());
        }
        if head_view.header.resource != resource {
            return Err(CoreError::RecoveryFailed(format!(
                "head_commit resource {} does not match {resource}",
                head_view.header.resource
            ))
            .into());
        }
        let target = intent
            .base_generation
            .checked_add(1)
            .ok_or_else(|| CoreError::Rejected("generation overflow".into()))?;
        let (view, _reads) = self.seek_generation(&head_commit, target).await?;
        if view.header.generation != target {
            return Err(CoreError::RecoveryFailed(format!(
                "seek for generation {target} landed on {}",
                view.header.generation
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
            let outcome = outcome_from_view::<P::Outcome>(&view, identity)?;
            let _ = self
                .finalize_intent(
                    identity,
                    view.header.generation,
                    view.digest.clone(),
                    serde_json::to_value(&outcome)?,
                )
                .await;
            return Ok(PendingResolution::Done(
                self.published_from_view(view, outcome, false)?,
            ));
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
        self.require_readable_head(&snapshot).await?;
        let now = self.clock.now();
        let generation = snapshot
            .value
            .generation
            .checked_add(1)
            .ok_or_else(|| CoreError::Rejected("generation overflow".into()))?;
        let parent = snapshot.value.head_commit.clone();
        let skip = self.compute_skip(generation, parent.as_ref()).await?;
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
                        .compute_skip(generation, snapshot.value.head_commit.as_ref())
                        .await?,
                    at: now,
                };
                header.validate()?;
                let commit = Commit {
                    schema: COMMIT_SCHEMA.into(),
                    header,
                    change: prepared.change.clone(),
                    result: serde_json::to_value(&prepared.outcome)?,
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
            Err(CoreError::PreconditionFailed(_))
            | Err(CoreError::AlreadyExists(_))
            | Err(CoreError::BackendUnavailable(_)) => {
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
                Err(CoreError::BackendUnavailable(_)) => return Ok(AttemptResult::Retry),
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
            .sync_companions(
                &prepared.companions,
                plan.resource(),
                base,
                &intent.proposed,
            )
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
        resource: &str,
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
            self.check_intent_binding(&intent, &c.identity, resource)?;
            match intent.state {
                IntentState::Applied { .. } => {
                    return Ok(false);
                }
                IntentState::Pending => {
                    if intent.base_generation != base {
                        match self
                            .resolve_companion_base(&c.identity, &intent, resource)
                            .await?
                        {
                            CompanionBase::Committed | CompanionBase::StillOpen => {
                                return Ok(false);
                            }
                            CompanionBase::ForeignConsumed => {}
                        }
                    }
                    intent.base_generation = base;
                    intent.proposed = proposed.to_vec();
                    let body = serde_json::to_vec_pretty(&intent)?;
                    match self
                        .backend
                        .put_update(&self.intent_key(&c.identity), version.as_ref(), &body)
                        .await
                    {
                        Ok(_) => {}
                        Err(CoreError::PreconditionFailed(_))
                        | Err(CoreError::AlreadyExists(_))
                        | Err(CoreError::BackendUnavailable(_)) => {
                            return Ok(false);
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }
        Ok(true)
    }

    async fn resolve_companion_base(
        &self,
        identity: &OpIdentity,
        intent: &OpIntent,
        resource: &str,
    ) -> Result<CompanionBase> {
        let snapshot = self
            .read_head(resource)
            .await?
            .unwrap_or_else(|| HeadSnapshot {
                value: RefValue::new(&self.tenant, resource),
                version: None,
            });
        let head_gen = snapshot.value.generation;
        if head_gen == intent.base_generation {
            return Ok(CompanionBase::StillOpen);
        }
        if head_gen < intent.base_generation {
            return Err(CoreError::RecoveryFailed(format!(
                "companion {} base {} is ahead of head {head_gen}",
                identity.canonical(),
                intent.base_generation
            ))
            .into());
        }
        let Some(head_commit) = snapshot.value.head_commit.clone() else {
            return Err(CoreError::RecoveryFailed(format!(
                "companion {} missing head commit",
                identity.canonical()
            ))
            .into());
        };
        let target = intent
            .base_generation
            .checked_add(1)
            .ok_or_else(|| CoreError::Rejected("generation overflow".into()))?;
        let (view, _) = self.seek_generation(&head_commit, target).await?;
        if identity_in_commit(&view, identity) {
            let outcome = outcome_from_view::<serde_json::Value>(&view, identity)?;
            let _ = self
                .finalize_intent(identity, view.header.generation, view.digest, outcome)
                .await;
            return Ok(CompanionBase::Committed);
        }
        Ok(CompanionBase::ForeignConsumed)
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
        self.require_readable_head(&snapshot).await?;
        let now = self.clock.now();
        let generation = snapshot
            .value
            .generation
            .checked_add(1)
            .ok_or_else(|| CoreError::Rejected("generation overflow".into()))?;
        let parent = snapshot.value.head_commit.clone();
        let skip = self.compute_skip(generation, parent.as_ref()).await?;
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
                    .compute_skip(generation, snapshot.value.head_commit.as_ref())
                    .await?,
                at: now,
            };
            header.validate()?;
            let commit = Commit {
                schema: COMMIT_SCHEMA.into(),
                header,
                change: prepared.change.clone(),
                result: serde_json::to_value(&prepared.outcome)?,
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
        let (view, _) = self.seek_generation(parent, target).await?;
        Ok(Some(view.digest))
    }

    /// Walk skip pointers then parents until `target` generation. Returns the
    /// commit and the number of commit objects read (for tests).
    pub(crate) async fn seek_generation(
        &self,
        start: &Digest,
        target: u64,
    ) -> Result<(CommitView, u64)> {
        let mut current = self.load_commit_view(start).await?;
        let resource = current.header.resource.clone();
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
            if current.header.resource != resource {
                return Err(CoreError::RecoveryFailed(format!(
                    "commit resource {} does not match {resource} while seeking {target}",
                    current.header.resource
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
                if skip_view.header.resource != resource {
                    return Err(CoreError::RecoveryFailed(format!(
                        "skip target {} resource {} does not match {resource}",
                        skip_d, skip_view.header.resource
                    ))
                    .into());
                }
                if skip_view.header.generation >= current.header.generation {
                    return Err(CoreError::RecoveryFailed(format!(
                        "skip from generation {} is not strictly decreasing (landed on {})",
                        current.header.generation, skip_view.header.generation
                    ))
                    .into());
                }
                let claimed = skip_target_generation(current.header.generation);
                if skip_view.header.generation != claimed {
                    return Err(CoreError::RecoveryFailed(format!(
                        "skip from generation {} must target {claimed}, pointed at {}",
                        current.header.generation, skip_view.header.generation
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
            if parent_view.header.resource != resource {
                return Err(CoreError::RecoveryFailed(format!(
                    "parent {parent} resource {} does not match {resource}",
                    parent_view.header.resource
                ))
                .into());
            }
            if parent_view.header.generation.checked_add(1) != Some(current.header.generation) {
                return Err(CoreError::RecoveryFailed(format!(
                    "dense ancestry broken: parent {} then {}",
                    parent_view.header.generation, current.header.generation
                ))
                .into());
            }
            current = parent_view;
        }
        if current.header.resource != resource {
            return Err(CoreError::RecoveryFailed(format!(
                "target commit resource {} does not match {resource}",
                current.header.resource
            ))
            .into());
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
                change: commit.change,
                admitted: Vec::new(),
                carrier: COMMIT_SCHEMA.into(),
            });
        }
        if schema != LOG_MANIFEST_SCHEMA {
            return Err(CoreError::RecoveryFailed(format!(
                "object {digest} schema {schema} is not a known commit carrier"
            ))
            .into());
        }
        let header_v = value.get("header").cloned().ok_or_else(|| {
            CoreError::RecoveryFailed(format!("object {digest} has no commit header"))
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
        let change = value
            .get("change")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        Ok(CommitView {
            digest: digest.clone(),
            header,
            result,
            change,
            admitted,
            carrier: LOG_MANIFEST_SCHEMA.into(),
        })
    }

    async fn published_from_applied<T: DeserializeOwned + Serialize>(
        &self,
        identity: &OpIdentity,
        request: &Digest,
        resource: &str,
        generation: u64,
        commit: &Digest,
        cached: &serde_json::Value,
    ) -> Result<Published<T>> {
        let snapshot = self
            .read_head(resource)
            .await?
            .ok_or_else(|| CoreError::RecoveryFailed("applied intent but ref missing".into()))?;
        let Some(head_commit) = snapshot.value.head_commit.clone() else {
            return Err(CoreError::RecoveryFailed(format!(
                "applied intent {} but ref {resource} has no head_commit",
                identity.canonical()
            ))
            .into());
        };
        if snapshot.value.generation < generation {
            return Err(CoreError::RecoveryFailed(format!(
                "applied cache generation {generation} is ahead of head {}",
                snapshot.value.generation
            ))
            .into());
        }
        let (view, _) = self.seek_generation(&head_commit, generation).await?;
        if view.digest != *commit {
            return Err(CoreError::RecoveryFailed(format!(
                "applied cache digest {commit} is not the canonical commit {} at generation {generation}",
                view.digest
            ))
            .into());
        }
        if view.header.resource != resource {
            return Err(CoreError::RecoveryFailed(format!(
                "applied commit resource {} does not match {resource}",
                view.header.resource
            ))
            .into());
        }
        if !identity_in_commit(&view, identity) {
            return Err(CoreError::RecoveryFailed(format!(
                "applied commit {commit} does not record {}",
                identity.canonical()
            ))
            .into());
        }
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
        let outcome = outcome_from_view::<T>(&view, identity)?;
        if !cached.is_null() {
            let from_commit = serde_json::to_value(&outcome)?;
            if from_commit != *cached {
                return Err(CoreError::RecoveryFailed(format!(
                    "applied cache for {} disagrees with commit {commit}",
                    identity.canonical()
                ))
                .into());
            }
        }
        self.published_from_view(view, outcome, false)
    }

    fn published_from_view<T>(
        &self,
        view: CommitView,
        outcome: T,
        first_delivery: bool,
    ) -> Result<Published<T>> {
        Ok(Published {
            generation: view.header.generation,
            epoch: view.header.epoch,
            commit: view.digest.clone(),
            value: original_ref_value(&self.tenant, &view)?,
            outcome,
            first_delivery,
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
            let now = self.clock.now();
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
}

pub(crate) enum IntentAdmission<T> {
    Applied {
        generation: u64,
        #[allow(dead_code)]
        commit: Digest,
        result: T,
    },
    Pending {
        #[allow(dead_code)]
        base: u64,
    },
}

#[allow(clippy::large_enum_variant)]
enum PendingResolution<T> {
    Done(Published<T>),
    Attempt { base: u64 },
}

#[allow(clippy::large_enum_variant)]
enum AttemptResult<T> {
    Done(Published<T>),
    Retry,
}

enum CompanionBase {
    Committed,
    ForeignConsumed,
    StillOpen,
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum CasResult<T> {
    Committed(Published<T>),
    Conflict,
}

fn intent_expiry(
    identity: &OpIdentity,
    policy: &OperationPolicy,
) -> Option<chrono::DateTime<chrono::Utc>> {
    match identity {
        OpIdentity::Generic(op) => Some(op.expires_at(policy)),
        OpIdentity::Stable(_) => None,
    }
}

pub(crate) fn persist_ref_state(next: &RefValue) -> serde_json::Value {
    let mut v = next.clone();
    v.head_commit = None;
    serde_json::to_value(&v).unwrap_or(serde_json::Value::Null)
}

fn original_ref_value(tenant: &str, view: &CommitView) -> Result<RefValue> {
    if let Some(state) = view.change.get("ref_state") {
        let mut value: RefValue = serde_json::from_value(state.clone())
            .map_err(|e| CoreError::RecoveryFailed(format!("commit ref_state: {e}")))?;
        if value.tenant != tenant {
            return Err(CoreError::RecoveryFailed(format!(
                "commit ref_state tenant {} does not match {tenant}",
                value.tenant
            ))
            .into());
        }
        if value.name != view.header.resource {
            return Err(CoreError::RecoveryFailed(format!(
                "commit ref_state name {} does not match {}",
                value.name, view.header.resource
            ))
            .into());
        }
        if value.generation != view.header.generation {
            return Err(CoreError::RecoveryFailed(format!(
                "commit ref_state generation {} does not match header {}",
                value.generation, view.header.generation
            ))
            .into());
        }
        value.head_commit = Some(view.digest.clone());
        return Ok(value);
    }
    let mut value = RefValue::new(tenant, &view.header.resource);
    value.generation = view.header.generation;
    value.epoch = view.header.epoch;
    value.head_commit = Some(view.digest.clone());
    value.updated_at = view.header.at;
    if view.carrier == LOG_MANIFEST_SCHEMA {
        value.target = Some(view.digest.clone());
    } else if let Some(t) = view
        .change
        .get("target")
        .and_then(|v| v.as_str())
        .and_then(|s| Digest::parse(s).ok())
    {
        value.target = Some(t);
    }
    Ok(value)
}

pub(crate) fn identity_in_commit(view: &CommitView, identity: &OpIdentity) -> bool {
    let id = identity.canonical();
    view.header.identity == id || view.admitted.iter().any(|a| a.identity == id)
}

pub(crate) fn outcome_from_view<T: DeserializeOwned>(
    view: &CommitView,
    identity: &OpIdentity,
) -> Result<T> {
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
    if !view.result.is_null() {
        return Ok(serde_json::from_value(view.result.clone())
            .map_err(|e| CoreError::RecoveryFailed(format!("commit result for {id}: {e}")))?);
    }
    Err(CoreError::RecoveryFailed(format!("commit does not record {id}")).into())
}
