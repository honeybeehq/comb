# Slice 3 — human test script: fault injection and the chaos run

**What this slice is:** proof that the invariants hold under failure, not
just under demo conditions. A fault-injecting backend loses requests and
loses responses (ambiguous acknowledgements — the hardest failure in
distributed storage), the core drills from spec §22.3 run in CI, and a
chaos mode lets you watch a torture run live.

## 1. Watch the torture run

```sh
cd ~/Projects/honeybee/comb/repos/comb
cargo build
./target/debug/combctl chaos --iterations 2000 --seed 1 --fail-prob 0.15
```

What you're looking at: thousands of claim / set / steal / release /
stale-fence operations where **every backend call has a 15% chance of
losing its request and 15% of losing its response**. The checker verifies
after every single operation, against the authoritative state:

- generation never goes backwards;
- epoch never goes backwards;
- no stale fence ever advances state;
- committed state never becomes unreadable (no torn writes);
- finally, the whole journal chain is walked and digest-verified.

The summary distinguishes **ambiguous acks** — operations that committed
even though the caller saw an error. That's the failure mode that silently
corrupts naive systems; here it's counted, expected, and harmless.

Crank it up and try to break it:

```sh
./target/debug/combctl chaos --iterations 10000 --seed 42 --fail-prob 0.35
```

Any run ending with `invariant violations 0` is a pass. A violation would
print in detail and exit 2 — if you ever see one, that's a stop-the-line
finding.

Reproducibility: the same `--seed` gives the identical run (same acked
count, same faults). That's what makes a violation debuggable.

## 2. The drills (CI form)

```sh
cargo test -p combctl --test drills
```

| Drill | What it proves |
|---|---|
| C1 | two writers storing identical content: one creates, one verifies and reuses |
| C3 | corrupted authoritative object → integrity error, content never returned |
| C4 | response lost after a successful ref update → the commit is real, a fresh read sees it, a blind retry cannot double-apply |
| C6 | unknown envelope version → refused, not misread |
| C7 | identical plaintext in two tenants → unrelated digests, no cross-tenant dedup, no existence oracle |
| chaos | 3000 ops at 20% fault rate, zero violations, deterministic by seed |

Not applicable yet: C5 (nothing uses listing), C8 (arrives with the
encryption envelope), C2 (cache corruption — already covered physically in
slice 1, step 5).

## Notes

- The chaos run uses the in-memory backend so faults are precise and the
  run is fast. The same invariants were physically demonstrated against S3
  and MinIO in slices 1–2; chaos covers the timing windows a human can't hit.
