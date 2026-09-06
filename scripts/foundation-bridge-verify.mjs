import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { resolve, join } from 'node:path';
import { pathToFileURL } from 'node:url';

if (process.argv.length !== 4) {
  throw new Error('usage: tsx foundation-bridge-verify.mjs <foundation-repo> <captured-feed.json>');
}
const foundationRepo = resolve(process.argv[2]);
const capture = JSON.parse(await readFile(process.argv[3], 'utf8'));
const fixture = JSON.parse(await readFile(new URL('../docs/testing/fixtures/foundation-bridge.json', import.meta.url), 'utf8'));
const expectedByKey = new Map(fixture.changes.map(change => [change.idempotency_key, change]));
assert.equal(capture.changes.length, expectedByKey.size, 'feed must contain one entry per stable key');
const seen = new Set();
const updates = [];
for (const change of capture.changes) {
  const expected = expectedByKey.get(change.idempotency_key);
  assert.ok(expected, `unexpected key ${change.idempotency_key}`);
  assert.ok(!seen.has(change.idempotency_key), `duplicate key ${change.idempotency_key}`);
  seen.add(change.idempotency_key);
  assert.equal(change.payload_hex, expected.payload_hex, 'stored Foundation bytes changed');
  const bytes = Buffer.from(change.payload_hex, 'hex');
  const headerSize = bytes.readUInt32LE(0);
  const envelope = JSON.parse(bytes.subarray(4, 4 + headerSize).toString('utf8'));
  assert.equal(envelope.hash, expected.envelope_hash);
  updates.push(bytes.subarray(4 + headerSize));
}

const { createChain } = await import(pathToFileURL(join(foundationRepo, 'packages/engine/src/chain/index.ts')).href);
const { projectDocument } = await import(pathToFileURL(join(foundationRepo, 'packages/engine/src/project/index.ts')).href);
const { baseDocument } = await import(pathToFileURL(join(foundationRepo, 'packages/engine/test/chain-fixtures.ts')).href);
const scratch = createChain(baseDocument(), { author: 'fixture:recovery', message: 'Recovery helper' });
// docFromUpdates imports only the supplied updates into an empty Loro document.
const recovered = scratch.docFromUpdates(updates);
assert.deepEqual(recovered.annotations, fixture.expected_annotations);
const hash = createHash('sha256').update(projectDocument(recovered)).digest('hex');
assert.equal(hash, fixture.expected_projection_sha256, 'fresh-peer projection differs');
console.log(JSON.stringify({ ok: true, recovered_changes: updates.length, annotations: recovered.annotations.length, projection_sha256: hash }));
