# R2 closure review at 8171fea

R2 is not accepted. The catalog split/depth fixes pass independent execution. The feed still has cancellation, ownership, and integrity failures that block Foundation integration.

Root rebuilt a fixed git archive in a private target and copied each executable before running it. [Receipt and binary hashes](../implementation/reliable-log/verification/r2-closure-8171fea.json), [test source](../implementation/reliable-log/verification/r2-closure-8171fea.rs), [logs](../implementation/reliable-log/verification/r2-closure-8171fea.txt). Catalog tests are unchanged from the [2587de1 source](../implementation/reliable-log/verification/r2-catalog-parent-review-2587de1.rs).

## Verified progress

All three previous catalog cases pass: 2,100 refs preserve child heights, excessive descent is rejected, and a noncovering leaf is rejected. The previous unused-session close hang, later-v1 interference, foreign manifest replay, max-head overflow, unbounded page ref read, and transient manifest read cases also pass.

The expired-call test passed once but failed on repetition. That case is not closed. Pre-cancelled reads failed 7 of 12 runs; expired reads failed 4 of 12. The timed helpers use an unbiased select, so ready I/O can win over an already-terminal call.

## Executed blockers

| Case | Result |
| --- | --- |
| Pre-cancelled or expired read | Sometimes returns success |
| Second ready call behind an acquisition | Exceeds its 20ms deadline and the external 200ms bound |
| Replay after append, close, fresh writer.ready | Rejects retained manifest epoch 1 against live lease epoch 2 |
| Store clock advanced beyond TTL before renewal | Renews expired lease back to Active |
| Clear committed ref target, keep head_commit and lease | Existing reader reports empty success |
| Append same stable key with different bytes after that fault | Succeeds at a new generation after recreating the index |
| Transient GetLimited failure on exact catalog root digest | Reports Integrity instead of recovering |
| Lower manifest head from 2 to 1, retain catalog | Reports at_head=true after only the first retained event |

The missing-target and reduced-head cases inject inconsistent metadata deliberately. The contract requires these states to fail closed. They are not claims that ordinary append currently clears the target itself.

The catalog transient test initially armed a broad DropRequest rule with successes_before_fire. That counter does not delay DropRequest, so the preliminary run exercised the manifest path instead. The final test obtains the actual catalog digest and targets it exactly; the final failure and binary hash are recorded here.

## Required corrections

- Check call state before work and before waiting on the acquisition mutex. Prioritize already-terminal cancellation/deadlines in selects.
- Keep retained data readable across lease epoch changes. Validate its original publication evidence rather than requiring its epoch to equal the later lease epoch.
- Renew and release through an actual instance+epoch-aware path. Refuse expired leases, use a fresh clock/snapshot and bound renewal by confirmed lease expiry/slack. The current background task still calls the older epoch-only renew_lease and checks owner after the write.
- Share one validated feed snapshot across open, head, read and append. Prove legitimate lease-only initialization before creating an empty index. Missing required commit evidence is an error. Check ref/commit/target identity and manifest/catalog/head consistency; do not let missing target or inconsistent head erase retained history.
- Preserve transient errors at catalog::load_node so bounded retry remains reachable.

## Legacy v2 compatibility

Independent source review found another blocker. load_commit_view now caps v2 inline manifests at 512 KiB while generic attempt can still publish larger manifests. This can leave committed v2 history unreadable. Root chose to preserve legacy v2 read behavior by Store layout while keeping v3 reads and writes bounded by shared class limits. An arbitrary larger v2 cap would merely move the compatibility failure. This item is source-inspected here, not independently executed.

The independent reviewer closed the original catalog and strict v3 schema findings. Their future-carrier hardening suggestions are not first-gate blockers. Full review follows for traceability.

## Independent source closure review

# R2 catalog closure review — 8171fea

Read-only, pinned to `8171fea` ("fix: bound complete-feed reads and owner-aware sessions") in
`comb-reliable-log-r2`, extracted with `git archive` to `/tmp/r2clo`. No edits, builds, children,
push. Slice: catalog C1/C2/C3 and strict v3 recovery / bounded `store.rs`, `publish.rs`, `hamt.rs`
reads, including regressions caused by the fixes. Feed/session and the 11 executed cases are root's.
Per instruction, the absent-v3 legacy guard (closed in `CompleteFeed::open`) and the legacy
`Envelope::decode` flag/codec checks (landed in `8f0f5cb`) are not reopened.

## Closure verdict

All six of my prior items are closed, three of them with a stronger fix than proposed. One new
regression was introduced by the read-side bounding, and it is the reason not to sign off yet.

| Item | Status | Evidence |
|---|---|---|
| C1 ragged split | **Closed, rewritten** | `append_at` now returns `SpineUpdate::Grown \| Split` (`catalog.rs:311-321`). A full leaf returns two sibling `CatalogChild`s (`388-392`) instead of synthesizing a branch; the parent pops and pushes both (`457-459`), splits itself at the same height on overflow (`475-498`), and only `append` mints a new root at `height + 1` (`273-305`). Five height checks now exist: leaf-at-nonzero-height (`361-366`), branch self-height vs parent (`399-404`), grown child (`412-417`), grown branch child (`426-431`), split child (`451-456`). |
| C1 secondary (cap after upload) | **Closed** | `h >= MAX_CATALOG_HEIGHT` is checked **before** both `put_node` calls (`472-474`), and `append` checks `new_height > MAX_CATALOG_HEIGHT` before writing (`281-283`). |
| C2 non-covering leaf | **Closed** | `seek_leaf` requires `f <= seq <= l` on the landing leaf, else `IntegrityError` (`catalog.rs:525-536`). |
| C3 unbounded descent | **Closed** | Both loops carry `depth` with `depth > MAX_CATALOG_HEIGHT` → `IntegrityError` (`517-523`, `562-568`). The off-by-one is correct: depth is checked before the load, so exactly `MAX_CATALOG_HEIGHT` branch hops are allowed and the ninth fails. |
| C4 vacuous size assertions | **Closed by deletion** | `encoded_node_bytes` is gone; the cap stays enforced at write time in `put_object`. `assert_height_invariant` (`catalog.rs:763-802`) replaces it with a real structural check. |
| V2 unbounded log-target read | **Closed** | `reject_existing_log_manifest` reads through `get_blob_limited` with an explicit two-carrier `allowed_schemas` and manifest caps (`store.rs:453-464`). |
| V3 permissive v3 decode | **Closed** | The v3 arm requires `log`, `stable_admissions`, `admitted`, `result`, `ref_state` via `require_v3_field` and rejects a null `ref_state` with `IntegrityError` at decode (`publish.rs:1224-1247`); the v2 arm keeps permissive defaults for R1 compatibility (`1248-1266`). |

Also landed and verified in my slice, beyond my items:

- `read_head` is bounded (`get_limited` at `MAX_REF_OBJECT_BYTES`, `publish.rs:179-180`) and now checks
  stored tenant/name against the addressed key (`185-191`) — the R1 boundary-identity item.
- `reject_v1` runs only under `KeyLayout::V2` (`publish.rs:176-178`), so a v1 ref appearing after a v3
  feed exists no longer blocks it — root's executed namespace failure.
- `map_commit_read_error` (`publish.rs:1566-1582`) classifies correctly: `BackendUnavailable` and `Io`
  stay transient, `NotFound` → `IntegrityError`, other → `RecoveryFailed`. This is root's item 3 done
  right on the commit path.
- `catalog.rs:255-258` now uses `checked_add` for the contiguity computation.
- Regression tests exist and are named for each item: `twenty_one_hundred_chunks_keep_parent_child_height`
  (`catalog.rs:653`, exactly the 2100-chunk threshold), `seek_leaf_rejects_child_last_seq_off_by_one`
  (`674`), `seek_leaf_rejects_unbounded_spine` (`713`).

**Interop check on the strict v3 decode — clean.** The strictness I asked for could have bricked
every v3 feed if the writer omitted a field. It does not: `CompleteLogManifest` (`feed.rs:330-344`)
declares all five fields with no `skip_serializing_if`, and the writer always sets
`ref_state: Some(ref_state)` (`feed.rs:1486`). The writer clears both `head_commit` and `target`
(`feed.rs:1466-1468`), which is correctly reconstructed on read by
`target_follows_commit` → `value.target = Some(view.digest)` (`publish.rs:1622-1623`).

---

## N1 — Regression: `load_commit_view` gained a read cap that no v2 write path enforces

**This is the blocker. It is caused by the fix, and it breaks R1 compatibility.**

`load_commit_view` now reads through `get_blob_limited` capped at `MAX_MANIFEST_OBJECT_BYTES`
(512 KiB) (`publish.rs:1182-1192`). It previously used the unbounded `get_blob`.

The matching write-side cap, `encoded_upload_cap`, is applied **only in `commit_at_snapshot`**
(`publish.rs:965-973` for plan uploads, `1010-1018` for the synthesized commit). The other
publication path, `attempt`, encodes and `put_create`s with **no cap check** — its upload loop calls
`env.encode()` and goes straight to `put_create`.

So the v3 path (`commit_at_snapshot`) is capped on both sides, while the generic/v2 path (`attempt`)
is capped on read only.

Trace, and it does not need a malicious input:

1. An R1 v2 log manifest embeds `chunks: Vec<ChunkRef>` inline and grows with every append —
   that inline growth is precisely why R2 built the catalog. At roughly 200 bytes of JSON per
   `ChunkRef`, a manifest crosses 512 KiB after on the order of 2,600 un-compacted appends.
   `compact` is explicitly invoked, not automatic, so this is reachable in normal operation.
2. `attempt` publishes that manifest with no size check. The CAS succeeds; the feed is live.
3. Any later `load_commit_view` on that digest — `history_chain`, `resolve_pending`,
   `seek_generation` — now fails the 512 KiB cap. Via `map_commit_read_error` the
   `ObjectTooLarge` lands in the `Ok(other)` arm and surfaces as
   `RecoveryFailed("commit … unreadable")`.

The result is a **published, committed, permanently unreadable manifest**: R1 retry resolution and
history traversal both fail on an established v2 log that was healthy before this commit.

Minimal correction, either direction, but one is required:
- apply `encoded_upload_cap` in `attempt`'s upload loop so writes and reads share one bound (this
  makes the failure a rejected append rather than an unreadable feed, but it will start rejecting
  appends on v2 logs that are already near the bound); **or**
- give `load_commit_view` a larger cap for the v2 carrier specifically, keeping 512 KiB for v3 where
  the catalog bounds manifest growth by construction.

The second is the safer one for existing data. Either way the two caps must be derived from one
constant per carrier, not chosen independently on each side.

## N2 — Carrier lists have drifted into three independent literals, and one fails open

The same carrier set is now written out three times:

- `CARRIERS` in `load_commit_view` (`publish.rs:1176-1181`)
- `LOG_MANIFESTS` in `reject_existing_log_manifest` (`store.rs:453-456`)
- the `schema ==` chain in `encoded_upload_cap` (`publish.rs:1544-1553`)

Adding a future `…/v4` carrier requires editing all three. Missing the second one is silent and
fails **open**: `reject_existing_log_manifest` treats
`UnsupportedEnvelopeFormat { field: Schema, .. }` as "not a log manifest" and returns `Ok(())`
(`store.rs:466-473`), so an unrecognized manifest carrier lets Core `set_target` overwrite a
log-owned ref — exactly what the guard exists to prevent.

The fail-open behavior itself is not new (the previous code allowed the overwrite whenever the
payload schema was not v2/v3), so this is a hardening item rather than a regression. Fix: one shared
`const LOG_MANIFEST_CARRIERS` consumed by all three sites.

## N3 — `reject_existing_log_manifest` still turns a transient read into a permanent `Rejected`

`store.rs:474-477` maps every non-schema error to
`Rejected("core set-target cannot overwrite a ref whose target … cannot be read")`. A
`BackendUnavailable` on the manifest read therefore fails a Core `set_target` with a
permanent-looking verdict.

Pre-existing, not caused by this commit — but it is the one remaining site in my slice violating
root's item 3, and `load_commit_view` immediately alongside it now does the classification correctly.
Reusing `map_commit_read_error` here is a two-line change.

## N4 — `CARRIERS` admits `"comb.object/v1"` (observation, not a defect)

`load_commit_view`'s envelope allowlist includes the generic blob schema (`publish.rs:1180`), so any
`put_blob`-written object passes the envelope gate. No wrong object is accepted: the payload `schema`
dispatch immediately after is strict (`publish.rs:1212-1217`), and unlike envelope metadata the
payload is covered by the digest, so dispatching on it is the stronger check. Recording the rationale
so the looser envelope list is not later mistaken for an oversight.

## N5 — Catalog nodes are validated on load but not on put (hardening)

`validate_leaf_refs` / `validate_children` run in `load_node` (`catalog.rs:168-185`); `put_node` →
`put_object` performs no structural validation, including for the new root branch written in
`append` (`catalog.rs:286-294`). Inputs are derived from already-validated nodes, so this is not a
live bug — but a construction defect would publish a catalog root that no reader can load, which for
a complete feed is unrecoverable. Validating on put makes such a defect fail at write instead.

---

## Suggested tests (not written)

1. N1: publish a v2 manifest above 512 KiB through `attempt` (or hand-write one), then call
   `history_chain` / `resolve_pending` on it; assert whichever behavior is chosen — rejected at
   append, or readable at the v2 cap — and assert the two caps come from one constant.
2. N2: add a synthetic `…/v4` carrier to `load_commit_view` only, then assert Core `set_target` is
   still refused against a ref whose target uses it.
3. N3: inject `BackendUnavailable` on the target read during `SetTargetPlan::prepare`; assert a
   transient error, not `Rejected`.
