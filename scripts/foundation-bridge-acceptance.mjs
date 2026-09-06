import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { createReadStream } from 'node:fs';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { resolve, join } from 'node:path';
import { tmpdir } from 'node:os';
import { Bridge, reserveOutputDirectory, writeSuccessfulCapture } from './foundation-bridge-client.mjs';

if (![5, 6].includes(process.argv.length)) {
  throw new Error('usage: node foundation-bridge-acceptance.mjs <comb-bridge> <config-dir> <output-dir> [forward|reverse]');
}
const [binary, configDir, outputDir] = process.argv.slice(2, 5).map(path => resolve(path));
const fixture = JSON.parse(await readFile(new URL('../docs/testing/fixtures/foundation-bridge.json', import.meta.url), 'utf8'));
const log = `foundation-${randomUUID()}`;
const arrivalOrder = process.argv[5] ?? 'reverse';
assert.ok(['forward', 'reverse'].includes(arrivalOrder), 'arrival order must be forward or reverse');
const changes = [...fixture.changes];
if (arrivalOrder === 'reverse') changes.reverse();

await reserveOutputDirectory(outputDir);

const binaryHash = createHash('sha256');
for await (const chunk of createReadStream(binary)) binaryHash.update(chunk);
const receipt = {
  log,
  fixture: fixture.doc_id,
  arrival_order: arrivalOrder,
  foundation_commit: fixture.foundation_commit,
  binary_sha256: binaryHash.digest('hex'),
  checks: [],
};
const recoveryConfig = await mkdtemp(join(tmpdir(), 'comb-bridge-recovery-'));
let bridge = new Bridge(binary, configDir);
try {
  const hello = await bridge.ok('hello');
  receipt.hello = hello;
  assert.ok(Number.isSafeInteger(hello.limits.max_frame_bytes) && hello.limits.max_frame_bytes > 0);
  bridge.maxFrameBytes = hello.limits.max_frame_bytes;
  for (const capability of ['durable_idempotency', 'bounded_memory_read']) {
    assert.equal(hello.capabilities[capability], true, `integration blocked: ${capability} is unsupported`);
  }
  assert.ok(hello.capabilities.ops.includes('follow'));
  const initialHead = await bridge.ok('head', { log });
  assert.equal(initialHead.head, '0');
  const following = bridge.ok('follow', { log, cursor: '1', max_events: 1, max_bytes: 16_384, timeout_ms: 30_000 }, 'reader');
  const appends = changes.map((change, index) => bridge.ok('append', {
    log, idempotency_key: change.idempotency_key, payload_hex: change.payload_hex,
  }, `writer-${index}`));
  const [results, firstPage] = await Promise.all([Promise.all(appends), following]);
  assert.equal(firstPage.events.length, 1, 'pending follow must see a concurrent append');
  assert.equal(firstPage.events[0].seq, '1');
  assert.deepEqual(results.map(result => BigInt(result.first)).sort((a, b) => a < b ? -1 : a > b ? 1 : 0), [1n, 2n, 3n]);
  for (const result of results) assert.equal(result.first, result.last);
  receipt.checks.push('multiple logical clients append while follow is pending');

  const retries = await Promise.all(changes.map((change, index) => bridge.ok('append', {
    log, idempotency_key: change.idempotency_key, payload_hex: change.payload_hex,
  }, `retry-${index}`)));
  retries.forEach((result, index) => assert.deepEqual(result, results[index], 'retry result changed'));
  assert.equal((await bridge.ok('head', { log })).head, '3');
  receipt.checks.push('same stable keys return original ranges without advancing head');

  const conflict = await bridge.request('append', {
    log, idempotency_key: changes[0].idempotency_key, payload_hex: '00',
  });
  assert.equal(conflict.ok, false);
  assert.equal(conflict.error.code, 'conflict');
  assert.equal((await bridge.ok('head', { log })).head, '3');
  receipt.checks.push('changed bytes conflict without append');

  const beforeRestart = await bridge.close();
  assert.equal(beforeRestart.code, 0);
  await writeFile(join(recoveryConfig, 'config.toml'), await readFile(join(configDir, 'config.toml')), { mode: 0o600 });
  bridge = new Bridge(binary, recoveryConfig);
  await bridge.ok('hello');
  const restartedRetry = await bridge.ok('append', {
    log, idempotency_key: changes[0].idempotency_key, payload_hex: changes[0].payload_hex,
  }, 'fresh-client');
  assert.deepEqual(restartedRetry, results[0]);
  assert.equal((await bridge.ok('head', { log })).head, '3');
  receipt.checks.push('fresh process and client recover committed stable key without prior cache');

  const byPayload = new Map(changes.map(change => [change.payload_hex, change]));
  const captured = [];
  let cursor = '1';
  while (BigInt(cursor) <= 3n) {
    const page = await bridge.ok('read', { log, cursor, max_events: 1, max_bytes: 16_384 });
    assert.equal(page.events.length, 1);
    assert.equal(page.events[0].seq, cursor);
    assert.equal(page.next_cursor, String(BigInt(cursor) + 1n));
    assert.ok(page.events.reduce((size, event) => size + event.payload_hex.length / 2, 0) <= 16_384);
    const original = byPayload.get(page.events[0].payload_hex);
    assert.ok(original, 'bridge changed the Foundation bytes');
    captured.push({ idempotency_key: original.idempotency_key, payload_hex: page.events[0].payload_hex });
    cursor = page.next_cursor;
  }
  assert.equal(new Set(captured.map(change => change.idempotency_key)).size, 3);
  const end = await bridge.ok('read', { log, cursor, max_events: 1, max_bytes: 16_384 });
  assert.deepEqual(end.events, []);
  assert.equal(end.next_cursor, cursor);
  const tooSmall = await bridge.request('read', { log, cursor: '1', max_events: 1, max_bytes: 1 });
  assert.equal(tooSmall.ok, false, 'oversized first event must not be skipped');
  assert.equal(tooSmall.error.code, 'event_too_large');
  assert.equal(tooSmall.error.seq, '1');
  const firstEventBytes = captured[0].payload_hex.length / 2;
  const byteLimited = await bridge.ok('read', { log, cursor: '1', max_events: 3, max_bytes: firstEventBytes });
  assert.equal(byteLimited.events.length, 1, 'byte budget must bound a page independently of event count');
  assert.equal(byteLimited.next_cursor, '2');
  receipt.checks.push('fresh client replays exact bytes with bounded pages and no silent gaps');

  const success = await writeSuccessfulCapture(bridge, outputDir, captured, receipt);
  console.log(JSON.stringify(success));
} finally {
  await bridge.close().catch(() => {});
  await rm(recoveryConfig, { recursive: true, force: true });
}
