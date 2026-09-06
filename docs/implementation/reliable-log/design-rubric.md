# Design comparison criteria

Compare each candidate on these criteria before choosing an implementation.

1. Atomic retry outcome. Lost final CAS replies, later ref updates, identical concurrent submissions, mismatched requests, and expiry have a precise result. No operation executes twice within the contract.
2. Bounded active state. Ref and manifest size do not grow with the lifetime event count. Reads and maintenance have explicit size limits. Avoid a tenant-wide CAS on every append.
3. Recoverable ownership. Crashed or paused writers cannot publish under a new owner's fence. Idle renewal and maintenance conflict behavior are explicit.
4. Safe lifecycle. GC cannot delete data behind a failed traversal or a concurrently published root. Reader protection and operation-result roots have an enforceable lifetime. A check immediately before delete is not an atomic guard.
5. Small consumer interface. Pheromone sees positions and append/read results, not manifest layout, provider versions, or recovery steps. One implementation owns publication semantics for direct and grouped appends.
6. Verifiable delivery. The initial correctness slice has deterministic tests and a CLI scenario. Later storage, replay, and consumer integration slices can be checked separately.

Two structural candidates are being compared: immutable commit/history indexing and operation-intent recovery. Two independent model lanes fit the bounded design task; a third lane traces Pheromone's consumer boundary. Root owns backend checks and synthesis.
