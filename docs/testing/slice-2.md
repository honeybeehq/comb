# Slice 2 — human test script: backend conformance + sovereign MinIO

**What this slice is:** the backend certification discipline (spec §7.9,
§17.3) made executable — `combctl backend test` — plus MinIO running on your
own metal (`trmd-metal-1`, Hetzner), which is the sovereign production
baseline. No backend is authoritative until it passes this suite; an
"S3-compatible" label is not evidence.

**Infrastructure that now exists:**

- MinIO on `trmd-metal-1` as a systemd *user* service (no root needed),
  data in `~/minio/data`, credentials in `~/minio/credentials` (0600),
  linger enabled so it survives logout and reboot.
  Console: `http://trmd-metal-1:9001` (login: see credentials file).
- Bucket `comb-dev` on that MinIO.
- A `minio` profile in your `~/.aws/credentials` on this Mac.

## 1. Certify AWS S3

```sh
cd /tmp/comb-play        # your slice-1 playground (S3 backend)
combctl backend test
# -> 9 checks, "backend CONFORMS"
```

The interesting check is `concurrent CAS has exactly one winner`: eight
tasks race a conditional write from the same observed version; the backend
must accept exactly one.

## 2. Certify your own MinIO

```sh
mkdir -p /tmp/comb-minio && cd /tmp/comb-minio
combctl init --tenant org_trmd --backend s3 \
  --bucket comb-dev --region eu-north-1 \
  --profile minio --endpoint http://trmd-metal-1:9000
combctl backend test
# -> same 9 checks against your Hetzner box
```

## 3. Same product, your hardware

Re-run any part of the slice-1 script (`docs/testing/slice-1.md`) in
`/tmp/comb-minio` — put/get, the two-terminal race, claim/steal/fence, the
journal. Identical semantics, zero AWS involvement. Browse the stored
objects in the MinIO console at `http://trmd-metal-1:9001`.

## 4. Watch it refuse a broken backend

Point a config at something that is not a conformant object store and watch
the suite fail loudly instead of trusting it:

```sh
mkdir -p /tmp/comb-bad && cd /tmp/comb-bad
combctl init --tenant org_bad --backend s3 \
  --bucket comb-dev --region eu-north-1 \
  --profile minio --endpoint http://trmd-metal-1:9001   # console port, not S3
combctl backend test
# -> FAIL rows, "backend DOES NOT CONFORM", exit code 2
```

*Proves: the certification gate exists and bites (§7.9). Backends earn
trust; they are never granted it.*

## Notes

- MinIO here is single-node on one disk: conformance covers **semantics**
  (create-only, CAS, read-after-write), not durability. Drill L10 (§22.2)
  applies: losing that disk loses the data. Production MinIO gets erasure
  coding and its own review before any durability claim.
- Conformance runs clean up after themselves (unique key prefix, deleted at
  the end).
