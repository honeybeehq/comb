# Comb

Tenant-scoped, object-backed durable-state substrate: immutable objects, conditionally updated refs, leases with fencing, pins, and disposable local materialization — specialized into ordered Logs, immutable Trees, and branchable Volumes.

The canonical specification is [`comb-specification-v0_3.md`](comb-specification-v0_3.md). Example consumers (Pheromone, Apiary Cells, Nectar, Flight, Forum, Brood) are built as views over Core's public API; a consumer that needs a Core change is redesigned, not accommodated.

Status: slices 1–5 plus retry-safe Core mutations and Log append on `comb/v2`. Trees, Volumes, and hosting are not implemented. See [`docs/protocol/retry.md`](docs/protocol/retry.md).
