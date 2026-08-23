# Comb

Tenant-scoped, object-backed durable-state substrate: immutable objects, conditionally updated refs, leases with fencing, pins, and disposable local materialization — specialized into ordered Logs, immutable Trees, and branchable Volumes.

The canonical specification is [`comb-specification-v0_3.md`](comb-specification-v0_3.md). Example consumers (Pheromone, Apiary Cells, Nectar, Flight, Forum, Brood) are built as views over Core's public API; a consumer that needs a Core change is redesigned, not accommodated.

Status: planning phase. No implementation yet.
