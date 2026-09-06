# Retry protocol synthesis

The independent Claude opus/max review selects candidate A, immutable commit-history recovery. Root accepts that result. It replaces the initial forward-helping proposal because successor writes should not require durable finalization of every preceding receipt.

## Caller contract

Generic callers create an OperationId once. Within its configured seven-day default window, identical retries return the original logical result. Changed material requests conflict. Expired IDs return UnknownOperation, including after cleanup. Validate future skew and first-use allowance.

Foundation has a separate stable append identity, scoped by tenant, document feed and caller key. Its docId plus envelope hash remains valid for the complete retained feed's lifetime, including fresh-peer recovery and retries after seven days. Different bytes with that key conflict. These keys never become newly admissible through timestamp reminting or local alias loss. Comb owns their durable intent and result evidence. Complete-feed mode retains both the feed and its stable-key evidence; it does not enable trim or physical deletion.

## Publication and recovery

Use A's atomic ref link to immutable commit history and a CAS-guarded per-operation intent. A commit records its request hash, result, dense generation, parent and skip ancestor. An ambiguous attempt at base generation b resolves by looking up generation b+1. A foreign commit consumes that base; our commit resolves the retry. Missing history fails closed. Completed intent results accelerate lookup but never replace commit evidence as the recovery proof.

Graft B's common RefCommitter and re-runnable RefMutationPlan, semantic request hashing, bounded producer admissions, injectable clocks, targeted failure matrix and narrow consumer read types. Head value and provider version travel together in one snapshot. All logical ref publication paths use the same engine. Renewal derives from a fresh snapshot and preserves generation and commit identity.

Material hashes include tenant, resource, kind, semantic preconditions and ordered payload. They exclude routing, provider tokens and the current writer session. A completed retry resolves before checking today's writer authority. A new append still requires the current fence.

## Review corrections

- Never overwrite another twin's only upload protection and claim GC safety. Proposed objects must be deterministic or protected by bounded attempt records. General collection remains disabled pending a separate proof.
- Enforce dense commit ancestry and validate skip targets. State and test the actual read-cost bound; do not claim logarithmic work without measurements.
- Stable-key records and compact commit evidence do not expire in complete-feed mode. Missing or malformed required recovery evidence is an error, not permission to append again.
- Cap producer admission count and encoded bytes. Coalesce identical duplicates within one group and reject changed input.
- Rebuilding producer admission state fails closed on every unreadable required object.
- Old binaries ignore added fields. A capability marker is not a fence. Use a distinct v2 storage namespace for the first fresh-prefix gate or a tested rejectable v1 seal with explicit migration. Never claim transparent mixed-version writing. Existing live tenants are untouched.
- A missing root or reachable object aborts destructive collection. Post-CAS repair and namespace-only root classification are not GC safety proofs.

The first implementation slice covers the common publication engine, generic retries and stable-key Log append. Bounded replay, renewable writer sessions and the bridge integration follow immediately. General physical retention and compaction remain later items in the authorized Comb program; they do not block the first complete-feed Foundation gate.

## Consumer ownership

Foundation agent 2efd79a9-ded6-40a3-9251-30a291369cb0 owns the process bridge and Foundation acceptance fixtures in feat/foundation-bridge at comb-foundation-bridge. The first gate uses JSONL v1 over long-lived stdio brokered by the Foundation host. The handler may later serve a local socket. Shared Log owns writer sessions, leases and every durable format. Position values are decimal strings, opaque bytes are hex, and frame, event and byte limits are explicit.

Root owns this directory and shared integration. One worker owns Comb Store/Log production files; the Pheromone worker owns its separate repository branch.

## Evidence

The baseline has 23 passing Comb tests, 32 passing Pheromone tests, and nine passing backend conformance checks on each of MinIO and S3. See baseline-local.json, baseline-backends.json and cross-judge.md. Candidate documents record alternatives, not the final implementation contract.
