# Collection protocol under review

This is a proposed later slice, not an implemented guarantee. Complete-feed Foundation integration does not wait for physical collection. Destructive sweep stays disabled until the following protocol and backend capabilities pass adversarial tests.

## Why a grace period is insufficient

The current backend allows unconditional delete and stale listing. A publisher can pause after validating an old object, then publish its digest after a collector rechecks roots and deletes that object. A longer grace period only changes the pause needed. Reupload after CAS still exposes a committed missing object.

A second race survives a publication pause. A collector can issue a delete, lose the reply, and later finish that request after publication resumes and recreates the same content key. A collector lease alone does not fence an already issued backend delete. Conditional deletion must distinguish the old physical object incarnation from its replacement.

## Proposed first correct implementation

Pause logical publication for one tenant while collecting. This deliberately favors a testable protocol over uninterrupted writes. Registration uses a tenant root catalog only when a resource or pin is created, not on every append. Existing resource writes retain their own ref CAS.

The catalog is an immutable bounded-node map named by one CAS-guarded control record. It contains every resource and pin key that can publish a durable root. Listing cannot establish this set. Tombstones prevent resource identity reuse. A resource is registered before its first publication; an absent registered ref can be atomically installed as a frozen empty placeholder by a collector.

1. CAS the catalog control from Open to Closing for a unique collection round. This prevents registration of new roots. It does not yet imply existing publishers have stopped.
2. Read the captured immutable catalog. CAS every registered ref or pin into Frozen(round), preserving its logical value and incrementing a non-logical storage revision. A publication that already won appears in the captured value; a delayed CAS against the old token fails. Renewal also refuses frozen refs.
3. Only after every catalog member is frozen may the control enter Deleting. Traverse the captured roots with verified direct reads. Any missing root, unknown schema, corrupt object or unreadable required child aborts deletion. Capture pins even if their lease expires during this round.
4. Enumerate candidate object keys in bounded pages. Stale listing may miss garbage and may list a removed key; neither permits an unmarked live root to be deleted. Exclude grace-protected objects. Record a bounded, durable deletion plan with physical incarnation tokens.
5. Conditionally delete only the recorded incarnation. Each object creation must have a unique physical incarnation, even if its logical payload/digest is identical to an earlier deleted object. Delayed deletes must fail against a recreated object. Both the capability and the token's non-reuse must be proven on memory, local, MinIO and S3.
6. Persist deletion completion, then unfreeze registered refs. A crashed round remains closed until a helper completes or aborts it. The resumed publisher revalidates/reuploads every object it plans to reference after acquiring a fresh ref snapshot. It must not reuse a pre-freeze prepared plan without verification.
7. Reopen root registration last. Duplicate helpers may replay only the same immutable deletion plan with incarnation guards. An old helper must never compute a new plan after a round has closed.

## Backend capability still to establish

ObjectBackend currently has delete(key) only. S3 Version uses ETag, which can repeat when the same bytes are recreated. Adding an If-Match parameter without a unique incarnation would preserve the delayed-delete race. A possible envelope extension is a random upload-incarnation nonce outside the logical payload digest, causing recreated wire bytes and ETags to differ. This is not yet selected or verified. Native provider version IDs may be another option, but bucket versioning is optional in the specification.

A backend that cannot provide the required conditional semantics must refuse destructive collection. It can still run append, replay and dry-run collection.

## Reader and metadata contracts

Readers that need an old snapshot across collection must hold a registered pin before relying on it. A snapshot read without a pin can fail with an explicit retry or Trimmed result if publication changed. A pin must include the entire required object graph and remain in the captured catalog for the collection round. Lease expiry cannot remove a root halfway through marking.

Reachability must follow schema-defined retention edges. A compact commit receipt containing a historical target digest does not automatically promise permanent retention of that target's data. Complete-feed mode does retain the full feed and stable-key index. Finite-retention mode needs separate commit-metadata and readable-data windows. Unknown schemas stop deletion rather than guessing which JSON strings are edges.

## Required adversarial checks

- A publisher pauses immediately before its final CAS; collection freezes the ref and deletes its uncommitted objects; the old CAS fails, and a later retry republishes only after recreating/verifying its graph.
- A new resource is registered just before Closing but its first ref is absent. Collector installs a frozen placeholder and the stale first publication fails.
- A new registration races Closing. Exactly one catalog CAS wins and the other path reloads.
- A pin expires during traversal. Its captured graph survives the round.
- Any reachable read fails or returns corrupt/unknown data. No candidate deletion starts.
- A conditional delete pauses on the server, another helper finishes the round, and a publisher recreates the same digest. The delayed delete cannot delete the new incarnation.
- A collector dies at every phase transition. Helpers recover the same round without reopening before safe completion.
- Listings omit, duplicate and retain stale candidates. They affect reclamation completeness only.

If the incarnation guard or complete catalog cannot be established, this proposal is rejected. Rechecking roots or adding a grace interval is not a substitute.

## Live capability result

On September 6, the isolated AWS CLI probe passed all five conditional-delete checks on S3. The current MinIO service accepted a delete with a deliberately wrong If-Match token, then returned NoSuchKey on read. It therefore cannot enable this proposed deletion protocol. See conditional-delete-backends.json. This does not weaken the complete-feed integration, which disables destructive collection.

## Root disposition after review

Accept the review's T1 counterexample. A delayed delete can remove an existing orphan after a later publisher adopts it by reference; unique recreation tokens do not cover an object that was never recreated. The proposed sequence above is therefore not approved for implementation as written.

T2 through T8 identify required rules for ref freeze retry, observable Collecting state, pin ordering, a single deletion-plan digest, enforced catalog registration, closed root classes, and captured metadata/data floors. They belong to the later collection slice. Current timed-operation and complete-feed work keeps all needed evidence and disables destructive sweep, so it does not need speculative fields for an unimplemented collector.

The proposed touch rule changes an existing immutable object's physical envelope through put_update. The specification says object creation is create-only. That change needs an explicit physical-incarnation contract and proof before selection; it cannot be hidden inside a generic revalidation call. Memory also needs non-reused generations across delete/recreate, while local content-derived tokens repeat for identical wire bytes.

Do not adopt the review's recommendation to ship fenced-owner deletion immediately. The current generic Store can target arbitrary digests, and an old delayed delete must also be proven harmless if later compaction reuses an equivalent representation. A namespace convention or fenced publisher lease alone does not enforce those facts against an already issued delete. No physical deletion is authorized by this design artifact.

The first complete-feed integration remains independent of these unresolved collection rules. The broader authorized Comb retention work is still outstanding.
