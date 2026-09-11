# Comb

Comb stores immutable binary data and ordered event feeds on local or S3-compatible object storage. It owns durable publication, retry deduplication, resumable reads and publisher fencing. Consumers own their data formats and merge rules.

## Entry points

- `combctl` is the operator CLI for configuration, objects, refs and logs.
- `comb-bridge` is the application-facing executable. A host such as Foundation starts it as a child process and exchanges one JSON request or response per line over stdin/stdout. Diagnostics go to stderr. It is not a network server.

The bridge supports a capability handshake, keyed single-event append, head, bounded read and bounded long-poll follow. Retrying the same stable key and bytes returns the original append receipt; different bytes conflict. Foundation sends its immutable `.fdnc` changes through this boundary and uses Loro to merge them itself. Multiple logical producers can share one bridge and its lazy publisher session per log.

Build both executables:

```sh
cargo build --locked -p combctl --bins
```

Run `comb-bridge --dir <config-directory>` with an existing Comb configuration. See the [JSONL protocol, limits and caller contract](docs/testing/foundation-bridge.md) for requests and recovery behavior.

## Implemented scope

Default Core and legacy Log APIs use the `comb/v2` namespace. The bridge uses isolated `comb/v3` complete feeds with durable stable-key indexing and bounded catalog-backed replay. Existing v2 data is not silently migrated or mixed with v3 writes.

The [Foundation integration gate](docs/review/foundation-comb-integration-gate.md) records successful process restart, exact-byte replay and fresh Loro reconstruction on local storage, MinIO and S3. [Generic retry semantics](docs/protocol/retry.md) remain separate from complete-feed stable keys.

Complete feeds retain their history. Destructive collection is disabled. Trees, Volumes, physical stable batching and hosted service deployment are not implemented by this slice.

The [canonical specification](comb-specification-v0_3.md) describes the broader design, including capabilities beyond the implemented scope above.
