# Retry-safe mutations

Generic callers mint an `OperationId` once (`op_` plus Crockford-base32). Identical retries within the seven-day window return the original result. A different material request conflicts. After expiry the id is `UnknownOperation` even if the intent row was deleted. Retries must not mint a second id.

CLI mutating commands take `--operation`. If omitted, combctl prints `operation: op_...` before sending.

Stable Log append is a separate identity: opaque bytes, at most 512, hex on the wire as `sk_<hex>`. Comb does not parse Foundation document format. The key is valid for the lifetime of a complete retained feed, including a fresh process and more than seven days. Same key and bytes return the original range. Same key and different bytes conflict. Complete-feed mode rejects trim.

Storage is `comb/v2/...`. A `comb/v1` key for the same tenant is a hard error. Existing live v1 tenants are not rewritten.

The linearization point is still one ref CAS. That write links a dense commit (operation identity, material hash, result, parent, skip). Ambiguous attempts at base `b` look up generation `b+1`. Missing or malformed history fails closed. Completed intents are a cache; they are not publication. Caller-visible results come from that commit, not from the live ref after later mutations.

A group companion is acknowledged only from the committed admission record. Arithmetic from the leader range is not a receipt. A companion whose stored base is behind live head is resolved at `base+1` before it can join another batch. An `Applied` companion aborts the attempt so its payload is not published twice.

Material hashes include tenant, kind, resource, semantic preconditions, and ordered payload bytes. They exclude provider tokens, routes, the current writer, lease epoch, and allocated sequences. A completed append resolves before today's writer fence. A new append still needs authority.

Destructive online sweep is refused until a later, separately proven barrier.

R1 `LogStore::read` is unbounded: it materialises the retained sequence from the requested position into one `Vec`. Bounded paging and renewable writer sessions are R2. This release does not claim Foundation-ready bridge capability.
