import assert from 'node:assert/strict';
import { randomUUID, createHash } from 'node:crypto';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { readFile, copyFile, chmod, writeFile } from 'node:fs/promises';
import { resolve, join } from 'node:path';
import { performance } from 'node:perf_hooks';
import { Bridge, reserveOutputDirectory } from '../../../../scripts/foundation-bridge-client.mjs';

const [inputBinary, configDir, outputDir, countText = '256', sizeText = '64', concurrencyText = '4'] = process.argv.slice(2);
assert.ok(inputBinary && configDir && outputDir, 'usage: node bridge-load.mjs <binary> <config-dir> <fresh-output-dir> [count=256] [payload-bytes=64] [producers=4]');
const count = Number(countText), payloadBytes = Number(sizeText), producers = Number(concurrencyText);
for (const [value, max] of [[count, 65_536], [payloadBytes, 262_144], [producers, 64]]) assert.ok(Number.isSafeInteger(value) && value >= 1 && value <= max);
const output = resolve(outputDir);
await reserveOutputDirectory(output);
// Freeze the executable across the append and fresh-process replay phases.
const binary = join(output, 'comb-bridge');
await copyFile(resolve(inputBinary), binary);
await chmod(binary, 0o700);
const binarySha256 = createHash('sha256').update(await readFile(binary)).digest('hex');
const config = resolve(configDir);
const log = `load-${randomUUID()}`;
const payload = Buffer.alloc(payloadBytes, 255).toString('hex');
const sampleProcess = promisify(execFile);
const samples = [];
let sampling = true;
let bridge = new Bridge(binary, config);
let phase = 'append';
const sampleTask = (async () => {
  while (sampling) {
    try {
      const sampledPhase = phase;
      const { stdout } = await sampleProcess('/bin/ps', ['-p', String(bridge.child.pid), '-o', 'rss=']);
      const kib = Number(stdout.trim());
      if (Number.isFinite(kib) && kib > 0) samples.push({ phase: sampledPhase, rss_kib: kib });
    } catch { /* A process can close between observing its pid and ps. */ }
    await new Promise(resolve => setTimeout(resolve, 100));
  }
})();
try {
  const hello = await bridge.ok('hello');
  assert.equal(hello.capabilities.durable_idempotency, true, 'stable append unsupported');
  assert.equal(hello.capabilities.bounded_memory_read, true, 'bounded replay unsupported');
  bridge.maxFrameBytes = hello.limits.max_frame_bytes;
  const latencies = [];
  const sequences = new Set();
  let next = 0;
  const began = performance.now();
  await Promise.all(Array.from({ length: producers }, async (_, producer) => {
    for (;;) {
      const index = next++;
      if (index >= count) return;
      const started = performance.now();
      const appended = await bridge.ok('append', {
        log,
        idempotency_key: Buffer.from(`${log}/${index}`).toString('hex'),
        payload_hex: payload,
      }, `producer-${producer}`);
      assert.equal(appended.first, appended.last);
      const seq = BigInt(appended.first);
      assert.ok(seq >= 1n && seq <= BigInt(count));
      assert.ok(!sequences.has(appended.first), 'duplicate append receipt');
      sequences.add(appended.first);
      latencies.push(performance.now() - started);
    }
  }));
  const appendMs = performance.now() - began;
  assert.equal(sequences.size, count);
  assert.equal((await bridge.ok('head', { log })).head, String(count));
  await bridge.close();
  phase = 'replay';
  bridge = new Bridge(binary, config);
  const replayHello = await bridge.ok('hello');
  bridge.maxFrameBytes = replayHello.limits.max_frame_bytes;
  let cursor = '1', pages = 0, readCount = 0;
  const readBegan = performance.now();
  const pageBytes = Math.max(payloadBytes, Math.min(65_536, payloadBytes * 7));
  while (BigInt(cursor) <= BigInt(count)) {
    const page = await bridge.ok('read', { log, cursor, max_events: 7, max_bytes: pageBytes });
    assert.ok(page.events.length >= 1 && page.events.length <= 7);
    assert.ok(page.events.length * payloadBytes <= pageBytes);
    let expected = BigInt(cursor);
    for (const event of page.events) {
      assert.equal(event.seq, String(expected++));
      assert.equal(event.payload_hex, payload);
    }
    assert.equal(page.next_cursor, String(expected));
    cursor = page.next_cursor;
    readCount += page.events.length;
    pages++;
  }
  assert.equal(readCount, count);
  const end = await bridge.ok('read', { log, cursor, max_events: 7, max_bytes: pageBytes });
  assert.deepEqual(end.events, []);
  assert.equal(end.next_cursor, cursor);
  const replayMs = performance.now() - readBegan;
  await bridge.close();
  bridge.assertHealthy();
  sampling = false;
  await sampleTask;
  latencies.sort((a, b) => a - b);
  const percentile = p => latencies[Math.min(latencies.length - 1, Math.ceil(latencies.length * p) - 1)];
  const result = {
    ok: true, binary_sha256: binarySha256, log, count, payload_bytes: payloadBytes, producers,
    append_ms: appendMs, events_per_second: count * 1000 / appendMs,
    append_p50_ms: percentile(0.50), append_p99_ms: percentile(0.99), replay_ms: replayMs, replay_pages: pages,
    rss_samples: samples,
    peak_rss_kib: Object.fromEntries(['append', 'replay'].map(name => [name, samples.filter(s => s.phase === name).reduce((peak, sample) => Math.max(peak, sample.rss_kib), 0)])),
    memory_evidence: 'Sampled process RSS; this is a measurement at this workload, not proof of an asymptotic bound.',
  };
  await writeFile(join(output, 'receipt.json'), JSON.stringify(result, null, 2) + '\n', { flag: 'wx' });
  console.log(JSON.stringify(result));
} finally {
  sampling = false;
  await bridge.close().catch(() => {});
  await sampleTask;
}
