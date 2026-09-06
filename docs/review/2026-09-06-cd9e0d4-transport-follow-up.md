# Review of transport follow-up cd9e0d4

Recommendation: do not integrate `cd9e0d429887a32e6819733bfb799d47009de89a`.
Keep the verified transport from `df98595`, integrated as `1619f08`.
The two production failures cited by the follow-up were already fixed there.

## Findings

1. **P2: cancelling the transport detaches its reader and writer.**
   `stdio.rs:71` replaces the owning `JoinSet` with raw `JoinHandle`s. Dropping a
   handle does not abort its task. Cancelling `run` therefore leaves the child
   reader, writer, and admitted work alive. The reader-error branch also returns
   through `res??` without aborting a blocked writer. This breaks cancellation of
   the reusable handler even though the standalone broken-stdout test passes.
   Retain the previous owning `JoinSet` supervision.

2. **P2: the output fallback loses valid request IDs.**
   `stdio.rs:90` always emits an empty ID when an error frame exceeds the wire
   budget. A valid 976-byte request with ID `known-request` and a 940-character
   unknown operation produces a larger error under a 1024-byte limit. The previous
   implementation retained the valid ID in its bounded fallback. Losing it prevents
   the host from matching the response to its pending request.

3. **P2: large-frame ID extraction no longer decodes JSON strings.**
   `protocol.rs:520` skips the JSON parser for input over 8 KiB, then takes bytes
   through the first quote. An ID containing an escaped quote or backslash becomes
   a different ID even when the supplied JSON is complete. The length validation
   then accepts the wrong printable ID. Retain JSON decoding when recovering IDs;
   do not substitute a raw quote search for string decoding.

The stricter oversized-input buffer cap and invalid-ID validation are separable
changes. They do not justify replacing supervision or discarding valid correlation.
The existing frame reader already bounds memory independently of line length.

## Reproduction

All three focused regressions fail against `cd9e0d4`:

- `cancelling_transport_closes_its_owned_output`: after a successful hello, abort
  the parent transport future while keeping client input open. Its stdout remains
  open beyond the one-second test deadline.
- `oversized_error_preserves_a_valid_request_id`: the bounded fallback returns
  `""` instead of `"known-request"`.
- `extracting_id_from_large_json_preserves_json_escaping`: extraction returns the
  prefix `quoted` followed by a backslash rather than the original escaped ID.

The protocol target built and ran through Cargo. After it reported the first
failure, the already-built transport test executable was run directly to capture
both transport failures. A redundant parent Cargo invocation was interrupted;
no shared R1 process was stopped.

All three cases pass against `df98595` in the isolated
`feat/foundation-bridge-review` worktree. The complete protocol target passes 22
tests, and the transport target passes 7. Test cleanup tolerates an already-closed
socket after successful cancellation. Formatting and diff checks pass. Compilation
reports the existing unused-code warnings from path-included test modules.

This branch adds only the three regression tests and this review document to
`df98595`. It changes no production transport code and does not depend on
`cd9e0d4`. Root can integrate the evidence while preserving its current transport
and pending constructor migrations.

## Coordination

Comb root confirmed `cd9e0d4` is absent from integration HEAD `c8689d7` and placed
it on hold. Root's pending `Store::new` and current-schema test-fixture migrations
remain separate. This review does not change storage capabilities or authorize
adapter wiring against the retired `4706b79` APIs.
