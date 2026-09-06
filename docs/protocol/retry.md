# Retry-safe mutations

Generic callers mint an `OperationId` once (`op_` plus Crockford-base32). Identical retries within the seven-day window return the original result. A different material request conflicts. After expiry the id is `UnknownOperation` even if the intent row was deleted. Retries must not mint a second id.

CLI mutating commands take `--operation`. If omitted, combctl prints `operation: op_...` before sending.

Stable Log append is a separate identity: `StableKey::new(Vec<u8>)`, 1..=512 opaque bytes. The bridge hex-decodes `idempotency_key`; Comb does not parse docId. One key is one payload (`append_stable(key, payload)`). The key is valid for the lifetime of a complete retained feed. Same key and bytes return the original range (`first==last`). Same key and different bytes conflict. Complete-feed is durable metadata; trim is rejected even through `LogStore::new`. Bounded `read_page(from, max_events, max_bytes)` returns a page with `head_seq` and `next`. Unbounded `read(from)` remains for Comb tests. PublisherSession.renew is Comb-owned.

Storage is `comb/v2/...`. A `comb/v1` key for the same tenant is a hard error. Existing live v1 tenants are not rewritten.

The linearization point is still one ref CAS. That write links a dense commit (operation identity, material hash, result, parent, skip). Ambiguous attempts at base `b` look up generation `b+1`. Skip pointers must land on the Fenwick target, strictly decrease generation, and keep the same resource. Missing or malformed history fails closed. Completed intents are a cache of the original logical result; they are not publication and must not return the live head.

Stable membership is a HAMT in the same log manifest CAS. A missing child on a verified path is a negative. A missing required node, unknown schema, illegal slot, or a leaf on the wrong path is integrity, never `Absent`.

Material hashes include tenant, kind, resource, semantic preconditions, and ordered payload bytes. They exclude provider tokens, routes, the current writer, lease epoch, and allocated sequences. A completed append resolves before today's writer fence. A new append still needs authority.

Destructive online sweep is refused until a later, separately proven barrier.
