# Comb nightly review — September 11, 2026

All 92 frozen source commits through `7441fdec28a0972f0125750c9360051fe2458496` were reviewed, including merge resolutions, callers, tests and historical implementation stages. Five reproduced regression groups were repaired. The required simplification pass covered 172 module records: 22 candidates resolved as 13 implemented (five repairs and eight simplification candidates across seven simplification commits) and nine retained. No Comb source inspection or candidate proof is deferred.

Local verification source head: `c8af9f0c776a5f2117317e7e8b62c5bab7c16c66`. Integration, remote publication and CI are owned by the nightly coordinator and are not claimed by this worker report.

## Reproduced repairs

| Problem and decisive baseline evidence | Repair |
|---|---|
| Concurrent `put_update(None)` and `put_create` both succeeded: `left: 2`, expected one winner. The old rename path could overwrite the independent hard-link winner. | `7fa3a78272c149a066bb26c9f910bcc4b10227ee` routes absent-version writes through atomic create; versioned locking/CAS remains. The race test still requires one winner and `AlreadyExists` regardless of whether its scheduling observation succeeds. |
| A sequence after `u64::MAX` panicked; an impossible child count was accepted. | `4f2fbad41601dcab82eb3c8eac5be3cbad91ce39` checks adjacency and bounds chunk counts by the positive inclusive span, returning typed integrity errors. |
| Oversized renewal duration panicked before returning `InvalidLeasePolicy`. | `39c357dcc0d896a050f5c455b785a023e4603674` checks the duration sum before comparing it with TTL. |
| Lost compaction reply retried as zero instead of the original two chunks; trim recovery failed to decode its result. | `6067d3e53df0659e302537941295587bbc532439` persists maintenance result/ref state and checks completed recovery before a compaction no-op. Tests include removed intents, newer heads, original generation/results and no fresh intent or object for a new no-op. |
| Accepted `Duration::MAX` acquisition budget panicked during `Instant` addition. | `9700edc4eb29a733499e9ab1f845891b1210357f` falls back to the existing caller deadline on overflow and retains the minimum deadline. Two writers exercise real contention under a 25 ms caller bound. |

Baseline failures were observed before production repairs. Expanded maintenance assertions subsequently cover restart/recovery interactions; the original failing outcome assertions remain. Decisive log SHA-256 digests:

- `local-red.log`: `bb928b94cb891eb29a2f272087fda5bbab96bbe327f783c861f09a07401f9590`
- `catalog-red.log`: `318c90b39da9243ca0b9aa225814994a502e26fe55c71010830a8a76c577cdc2`
- `lease-red.log`: `1337d9893804381fa76cf2462286dc1911afcf0ff847e58eefcd8573b138e6ea`
- `maintenance-red.log`: `ef352b9f862f1f315f4c12d2a872c5586253c8a19579f501917b8044f8cd343d`
- `acquire-budget-red.log`: `d503d61888055ea2c8d3b82e3d045628c4692ecc7a58bdd27f51b4d0ad720803`
- `local-green.log`: `a3352860754057faa006d015869f1425a9c6df3b8ef407e24940033bf38ae671`
- `catalog-lease-green.log`: `b83e4e1c0b1c24643e3915a1959de9241c47b318b1f9c7df0bf655c717c53163`
- `maintenance-green.log`: `600d18c3f02529041f8a84b0995e804e9c4b1444deb8a5217a5409e7de3cf93f`
- `maintenance-interactions-green.log`: `42827e811b1d03c0d092c19ef05f9ea52d3f513f0129cce6d33d68c7cd5ef457`
- `acquire-budget-green.log`: `7e3def1404828b50f02e324fcb589ac5d2838916fc81d73983dd81af93b1b390`

## Behavior-preserving simplifications

- `b5c5f52d7681c05aa50f510922b224682c99a854`: peek at a request ID only when admission is busy. Existing parser/error semantics remain; 40 bridge tests passed before/after, and the final run has no temporary benchmark tests or ignores.
- `bc281cce049a6f221584bdbe4a2f741b6dd63e18`: S3 create delegates to the conditional-write owner. Twelve real pinned-SDK loopback wire cases preserve bucket/key, binary body, mutually exclusive condition headers, ETag and success/conflict/missing/unavailable error mapping.
- `c6493f7354b13b71e2242ba3fff3de35755fb62f`: one catalog node validation owner replaces duplicate put/load decisions and impossible post-deserialization schema branches. Strict schema and unknown-field rejection remain; all 12 catalog tests passed, including malformed/numeric inputs and tree splits.
- `82aebeaa1355042b50eda49e845989360c14bfdc`: insertion transfers an already validated HAMT node to its private recursive operation, removing the impossible absent-node state and repeated root fetch/decode. Measured root backend GETs fell from two to one. Fourteen tests preserve old-root immutability, collision/depth/range/prefix validation and typed bounds. The bounded decoder also replaces an unreachable duplicate payload check.
- `2282a2cb954fdfcaadcde93b7b4b135cf37ea295`: remove unused private maintenance fields and three String clones. Material hashes, public interfaces and stored schemas are unchanged; all 37 log/phase-C/retry tests passed.
- `cf87cc16eaeb1a0ca588f7b96d14a63acb0bb813`: remove the unreachable V3 post-read size guard. The decoder still applies the same limit on cache and backend, while supported large V2 manifests retain their unbounded reader. All five publication tests passed.

Commit `c8af9f0c776a5f2117317e7e8b62c5bab7c16c66` (the final stable-validation unit) replaces a temporary full payload clone with std::slice::from_ref; the validator reads lengths only. Bounded allocation comparison across 0, 1, 4096, 524288 and 524289 bytes preserved outcomes/errors and removed exactly one allocation and the payload length in allocated bytes for nonempty payloads. This is an isolated allocation check, not a whole-product timing claim.

Each accepted unit was checked separately. The final source pass found no further compatible, sufficiently proven simplification. Explicit boundaries retained include V2/V3 behavior, typed feed errors, legacy/limited envelope decoding, session lifetime state, fault-injection timing, and terminal transport/retry state. The small memory-backend duplication remains: async delegation adds a boxed future, while a separate helper adds an abstraction for three mutation statements.

Borrowed framing buffers were rejected after an optimized paired oversized-frame median of 1.13× baseline. Borrowed material sorting reduced allocations but did not establish timing preservation (one-precondition paired median 1.139× baseline). The latter used an optimized standalone harness linked to a debug hash dependency; samples were noisy. Neither product change shipped and neither benchmark proves whole-product performance. The material ordering/golden-digest characterization remains as a permanent test.

## Final verification

`cargo +1.93.0 test --locked --offline --workspace --all-targets`: 216 framework-reported passes, zero failures, zero ignored across 15 test targets on the source before the final singleton-slice borrowing change. After that one-line delta, all 37 affected log/retry/phase-C tests, workspace clippy and build were rerun successfully; the unrelated full suite was not repeated, per coordinator instruction. Two opt-in tests returned early without provider configuration; these are not live-backend successes. A separate pre-delta run explicitly enabled the local backend drill and passed; all 24 Node tests and the four syntax checks also passed on that pre-delta source.

All commands below exited zero on macOS arm64. Node was the installed v22.13.0 binary, matching the CI major version. Rust CI uses the same workspace test/clippy targets; `--offline` prevented dependency downloads here.

| Check receipt | Command | Log SHA-256 |
|---|---|---|
| `workspace-final` | `cargo +1.93.0 test --locked --offline --workspace --all-targets` | `05944d9e9b683fd7892cdd3070f417cf56005cf598c4849ffe6ceec496bd684f` |
| `local-drill-final` | `env COMB_DRILL_BACKEND=local TMPDIR=<owned-fixture-dir> cargo +1.93.0 test --locked --offline -p combctl --test backend_drills -- --nocapture` | `4909dac9b46ae21564d6a158147c0b6ddac4612a47630caa9a3b0c8fe097e4cc` |
| `clippy-final` | `cargo +1.93.0 clippy --locked --offline --workspace --all-targets` | `2a7e29bb5128566c82387986cf9bccab9705570c7b06d66f55a5ba2f664b9ad2` |
| `build-final` | `cargo +1.93.0 build --locked --offline --workspace` | `53fcf87d5f3216664f471b67bff805942d611293b5ecc8cd73519ad933f43933` |
| `node-version-final` | `node --version` | `eedf4a8e103f4e071c070963a242093d56e05a04ee224d6bf3960ea171110c91` |
| `node-tests-final` | `node --test scripts/foundation-bridge-key.test.mjs scripts/foundation-bridge-client.test.mjs` | `6f0752078f3d41403ad2f25700d45a558aebb18286167735ff63fec85910f1fa` |
| `node-check-foundation-bridge-fixture` | `node --check scripts/foundation-bridge-fixture.mjs` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `node-check-foundation-bridge-verify` | `node --check scripts/foundation-bridge-verify.mjs` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `node-check-foundation-bridge-acceptance` | `node --check scripts/foundation-bridge-acceptance.mjs` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `node-check-bridge-load` | `node --check docs/implementation/reliable-log/verification/bridge-load.mjs` | `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` |
| `payload-borrow-before` | `cargo +1.93.0 test --locked --offline -p combctl --test retry_drills --test log_drills --test phase_c_drills -- --nocapture` | `7f397966a0671b2342a84f6b2f651a07b7a1dd06ed48286a949a597256977589` |
| `payload-borrow-after` | `cargo +1.93.0 test --locked --offline -p combctl --test retry_drills --test log_drills --test phase_c_drills -- --nocapture` | `a7aa4ac99811174322836244874293b2f04f7253e7345678dbf9075754f5f211` |
| `clippy-borrow-final` | `cargo +1.93.0 clippy --locked --offline --workspace --all-targets` | `7c9d17a4df9e509544f726e0dcd05ab95fd4beb2a257369267afd9479f0ed76d` |
| `build-borrow-final` | `cargo +1.93.0 build --locked --offline --workspace` | `1fe8c3c1a4469dc4ab80fc8b7cb35d21aa3cb43c12f685db74cc2b8e2c12ae48` |

Clippy exited zero with warnings, including open options, complex types, large variants/errors, argument counts and style suggestions. The original frozen main was not linted again for a warning-by-warning baseline comparison. The new framing matrix produces a type-complexity warning at stdio.rs:274; it remains disclosed. No test assertion was weakened and no ignored tests were added. The root framing implementation was retained byte-for-byte apart from the separate admission change.

## Evidence limits

No current live S3 or MinIO provider was exercised. The SDK HTTP fixture proves request/error contracts against a bounded loopback server, not provider readiness. Historical S3/MinIO/Foundation/Pheromone reports were inspected as source evidence and were not reused as current runtime passes. Current bridge integration tests exercise the repository adapters and spawned transport; they do not certify a separately deployed Foundation or Pheromone installation. Remote Linux CI is pending coordinator publication. Electron is a separate frozen lane with its own unresolved native gaps; this report does not clear them.

Raw execution logs, source/diff hashes, per-unit receipts and the full module/candidate ledger remain in the nightly local evidence directory. The excerpts and hashes here are portable evidence accompanying eligible source changes, not a report-only publication. Exact full-diff generation was `git show --format=fuller --binary --full-index --no-ext-diff --no-renames --diff-merges=first-parent <sha>` against the frozen repository. Immutable reuse references exact reviewed blob or full-diff identities, never commit subjects.

## Exact reviewed scope

The following 92 unique SHAs reconcile with the frozen inventory. Historical versions remain source-review evidence; runtime evidence applies to the exact source snapshots qualified above.

```text
7441fdec28a0972f0125750c9360051fe2458496
831fd0d90bf4b3336d7273a364be6f006dfc3a42
c4ad9817b615a4e04ae971635dac465362857689
a1fb80324adb1a1a28828fc77e27ab1711f215c7
62689ce7535483a4e17ad19c4416296abdf495b4
b1597eb0ae84c6464bad2e0784e7b048cee1dc5b
b4714024869940119d4ae567c8013fdf5cf77ad8
d1f8f7cf27603a6dd0a0db2dd7046574e2d71568
fe8d7ca5c2fd0137be81653ff78cac1c75a2e57a
d655c3ee7a1d373c04d4d0b01e49e4bb4bad5546
bff2edb6f7160b2c12793330f9bf5125b4b8c626
842ebf1d7620b2aa7397782e31b326d538c22d32
dea1b29954014d6eb57d9a92628ea7f9f4a63b9b
51674ba082b12a732a90d1cba375dab580d7d2e6
bd0b9adb30a911c542b6cbee35deaab297ef4e68
7467b054d84bd0d6b372bea96628bfe5ffce4940
1698ecd47824c69df8d47e849404c7092e937577
aa8367047c3e6dd2624bd5c68f55e3afd33823a0
44e085a5f9bf167e13467622b8626fc21cc03e59
a53d49b769020720195dc19b98d50a3a1f831f2c
7c8a94471bc20e06caa3dc77088a262eceefb82f
7ac9f7a70b9f8252048d9cae0cd16d0942d262a8
3a2fcf357c583e79a0045a5ee5a946a84757a051
5b0d1d71f3754cb63b8f8873907638009e7117ec
176a7f80e356a676fdf552ff743bfa2cb0b1ceaf
ba6f49bbbc1d95abb19ed58611d2fa77d6df669a
f12fb54faaa0b6828707998a6e31819d98445d3c
9d1fdabac32662b8f3d314316f33a1648778d426
8171fea1f65b6faa82c86558868fb7740d7ca02f
04e1765a0b71fdd0463334014d36a2a97ee7c21a
9c43e23ae9eea22532060bf0fe2dba179cf55daa
3d6a49a74427b627a1024ffd2da2e853dcf849c0
e7ce714a286fff865a06960d34738cf0f3031c29
b4f82611df35bcb7c55d33488d030238b1611ffa
01bb78fa1c1237523e18afa189ff58d2fb2027d7
bfc5fba3644a0c49b8414df60a5e47a1cabf1373
edab314814b8e3e49d27ff2fecbe92a3c36fd640
ec9b0f09c1078bda51246bfa3cadb7e4ad7ea070
2587de1953d1acdc866e0df6e5f2f5db522da1c0
6d2b7be7f667ca363b57894e1055fd516ceb306e
069824a5ea41ab2dc3eb193f67ab0cd2e78c2994
8f0f5cb9174295df5f3310ccfcffb5c76ac209b9
edc624616c100a99ee0a9416f891a8d385e3cc12
e61d872b735d24a5063e8af04abdbf76f211dd1a
b5fc5d2a4e2de798f9c07e1955fff740ced1d5f3
aa94980bf7fa54810999c144af89a4a7cf7cfafd
9e60cd767fc6073ff6046b4aab8c34346fee0a7f
05d38167d228afd2d2661367f14d5721742a8296
918e27aa8a2d2b624d6d68437e00dbd2fac94334
a2d9810529def4fec97f36284448e2a341f12cea
e0c911e01633c8c9edff476bf064b43a0f131788
b004b4b25cf070835265402689771cb3bfc79bd2
db41bce8f7fb1bfc452f358f2c61ac555eaf1c1f
a675d3806b1b06019fa34ec407acc32e847efac9
bfba9a0167fc4a49c7ff1541f44d80152bcb5420
052ca112c65077c9eca150e4ba285108b1cfb541
6643c014b863357948522a2dd04a73a2cd545458
52c1963aa8ab52deceaa6b5cae04b26c9b24970f
84cb96c23362479b2c473819a797fae0edf16d9f
4b350f0fb5bde319eb49a846e9d109194e8c797f
383d29dac40cb8d2833c55e871a0c5e65e26264a
d8d4d1797908a022642d272a855be6a22a9202dd
ae0ae8c1e620c00d82b0aa2c3e23474c8860e49f
c347f47d80ac17c8940f2d5a209e9f8bd3dc3ec4
c8689d7d22b21e90b5ae730cce948b956c7fe6ac
0890a40377462f069e03f272d592039103bc317f
7bfc41f6612f5f4d7fd45e001d9b08a4a1a066ae
733c94346c44bacf1db4c5e0529dc2016641797b
1b00b740b48b98212afffdf2eab8f431f45b7b86
1619f08ab40991edb7605a6af5b94f475c48cddd
e93f37c0aaa7120b80167e2785fc2918771e3084
cbd99c194343662fb622cd5b8c3d3a87215a141a
e3d401923bc6e5c70879bdadd54eac35d460d48f
35bdd5c2d08b2616642fdffdfb1604f0636a1ae5
67cfe44c9bfbf76c1a953aae3a0f13bfcea78459
578a992e2a95b056c034135aae3fe00a35d17b60
84ba2430d0eef828fee7aabbbf4e16793632000e
be897677b08e057e00e60356528f06a011434642
9f52abf67d1b7e6999be2e075062c9b4e70c2f51
c9483060da7adcf4db7748a741f6d2ec63fe74ef
f4b9f9f9b36e55c0a8906f6ec768473ac9235995
e6b1d2b6613e068ec0b2820150ee14403698cadd
4b55c9903c46d0da7be8c3d6176c6192db5900c6
8942202ed2c435294e3f3091db076624140ef4cf
a5563a9eef95e915ca2c713b2f76e89e0754e354
3e5e2c87265d0a0b882d48d5884eccdb1e70b5ec
9a95f164ea1682f659ef4687fffb67986849b96a
c2a391441819bd5a7c71227392d4aa839be3a444
dbe683a5fa2d60f8207a0f83296c738a0e732981
436658c9cbd20f27c40a3e5b918436a09ac34f3c
b7966bc680535a4b85bc9271298918a5d75c4936
b77b64eeb0790f49527779e69df4a57623d1f5e1
```

Allocation fixture log SHA-256: `ba6743c89a314cd87f4bbbf460d46e983711d2529572046bb0812b4605db9bbf`. At 524288 payload bytes, `(allocation calls, allocated bytes)` changed from `(1, 524288)` to `(0, 0)`; at 524289 bytes the same rejection was returned, with counts `(3, 524467)` to `(2, 178)`.
