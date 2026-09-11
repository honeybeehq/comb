import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { access, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PassThrough, Writable } from 'node:stream';
import test from 'node:test';
import { appendWithRetry, Bridge, reserveOutputDirectory, writeSuccessfulCapture } from './foundation-bridge-client.mjs';

// Only client transport and response handling are simulated. This child has no storage.
function client(onClose = () => {}, onRequest = request => ({ v: 1, id: request.id, ok: true, op: request.op })) {
  const child = new EventEmitter();
  child.stdout = new PassThrough();
  child.stderr = new PassThrough();
  child.closed = false;
  child.stdin = new Writable({
    write(chunk, encoding, callback) {
      const request = JSON.parse(chunk.toString());
      const response = onRequest(request);
      if (response) child.stdout.write(JSON.stringify(response) + '\n');
      callback();
    },
  });
  function finish(code, signal) {
    if (child.closed) return;
    child.closed = true;
    child.stdout.end();
    child.stderr.end();
    child.emit('close', code, signal);
  }
  child.kill = () => queueMicrotask(() => finish(null, 'SIGTERM'));
  child.stdin.on('finish', () => queueMicrotask(async () => {
    child.emit('exit', 0, null);
    await onClose(child);
    finish(0, null);
  }));
  return { child, bridge: new Bridge('fake-client-only', 'unused', () => child) };
}

for (const [name, junk] of [
  ['invalid JSON', 'late junk\n'],
  ['unsolicited response', '{"v":1,"id":"nobody","ok":true}\n'],
]) {
  test(`${name} with no pending request prevents a success receipt`, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'comb-client-test-'));
    const { child, bridge } = client();
    try {
      await bridge.ok('hello');
      assert.equal(bridge.pending.size, 0);
      child.stdout.write(junk);
      assert.ok(bridge.terminalError);
      const terminalError = bridge.terminalError;
      await assert.rejects(bridge.request('hello'), error => error === terminalError);
      await assert.rejects(bridge.ok('hello'), error => error === terminalError);
      await assert.rejects(writeSuccessfulCapture(bridge, directory, [], {}));
      await assert.rejects(access(join(directory, 'receipt.json')), { code: 'ENOENT' });
    } finally {
      await bridge.close().catch(() => {});
      await rm(directory, { recursive: true, force: true });
    }
  });
}

test('partial stdout arriving after exit but before close prevents success', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'comb-client-test-'));
  const { bridge } = client(child => child.stdout.write('{'));
  try {
    await bridge.ok('hello');
    await assert.rejects(writeSuccessfulCapture(bridge, directory, [], {}), /partial_frame=1/);
    await assert.rejects(access(join(directory, 'receipt.json')), { code: 'ENOENT' });
  } finally {
    await bridge.close().catch(() => {});
    await rm(directory, { recursive: true, force: true });
  }
});

test('receipt writer waits for a clean close', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'comb-client-test-'));
  let releaseClose;
  const closeGate = new Promise(resolve => { releaseClose = resolve; });
  const { child, bridge } = client(() => closeGate);
  try {
    await bridge.ok('hello');
    assert.equal(child.closed, false);
    const writing = writeSuccessfulCapture(bridge, directory, [], { test: 'client-only' });
    await new Promise(resolve => setImmediate(resolve));
    assert.equal(child.closed, false);
    await assert.rejects(access(join(directory, 'receipt.json')), { code: 'ENOENT' });
    releaseClose();
    const receipt = await writing;
    assert.equal(child.closed, true);
    assert.equal(receipt.ok, true);
    assert.deepEqual(JSON.parse(await readFile(join(directory, 'receipt.json'), 'utf8')), receipt);
    await assert.rejects(bridge.request('hello'), /request after bridge close/);
  } finally {
    releaseClose();
    await bridge.close().catch(() => {});
    await rm(directory, { recursive: true, force: true });
  }
});

test('reusing an output directory fails without disturbing its old receipt', async () => {
  const root = await mkdtemp(join(tmpdir(), 'comb-client-test-'));
  const directory = join(root, 'run');
  try {
    await reserveOutputDirectory(directory);
    const previous = '{"ok":true,"run":"old"}\n';
    await writeFile(join(directory, 'receipt.json'), previous);
    await assert.rejects(reserveOutputDirectory(directory), { code: 'EEXIST' });
    assert.equal(await readFile(join(directory, 'receipt.json'), 'utf8'), previous);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


const appendFields = { log: 'doc', idempotency_key: '00aAFF', payload_hex: '00ff8000' };
const retryPolicy = { timeoutMs: 1_000, maxAttempts: 4, initialBackoffMs: 100, maxBackoffMs: 250 };
const unavailable = request => ({ v: 1, id: request.id, ok: false, error: { code: 'backend_unavailable', message: 'retry the same key and bytes' } });
const appended = request => ({ v: 1, id: request.id, op: 'append', ok: true, log: request.log, first: '7', last: '7', cursor: '8' });
function timing() {
  let time = 0;
  const sleeps = [];
  return { sleeps, now: () => time, advance: ms => { time += ms; },
    sleep: async ms => { sleeps.push(ms); time += ms; } };
}

test('unavailable append retries exact frozen identity with new request ids and records recovery', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'comb-client-retry-test-'));
  const requests = [];
  const { bridge } = client(undefined, request => {
    requests.push(request);
    return requests.length < 4 ? unavailable(request) : appended(request);
  });
  const fields = { ...appendFields };
  const evidence = [];
  const clock = timing();
  try {
    const result = await appendWithRetry(bridge, fields, 'peer', {
      policy: retryPolicy, ...clock,
      onAttempt: event => { evidence.push(event); fields.idempotency_key = 'reminted'; fields.payload_hex = 'changed'; fields.log = 'other'; },
    });
    assert.deepEqual(result, { log: 'doc', first: '7', last: '7', cursor: '8' });
    assert.equal(new Set(requests.map(request => request.id)).size, 4);
    for (const { v, id, op, ...identity } of requests) assert.deepEqual(identity, appendFields);
    assert.deepEqual(clock.sleeps, [100, 200, 250]);
    assert.deepEqual(evidence.map(event => event.outcome), ['backend_unavailable', 'backend_unavailable', 'backend_unavailable', 'ok']);
    assert.deepEqual(evidence.map(event => event.retry), [true, true, true, false]);
    assert.equal(new Set(evidence.map(event => event.key_hex_sha256)).size, 1);
    assert.equal(new Set(evidence.map(event => event.payload_hex_sha256)).size, 1);
    await writeSuccessfulCapture(bridge, directory, [], { append_attempts: evidence });
    const receipt = JSON.parse(await readFile(join(directory, 'receipt.json'), 'utf8'));
    assert.equal(receipt.ok, true);
    assert.deepEqual(receipt.append_attempts, evidence);
  } finally {
    await bridge.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('bounded append exhaustion cannot write a success artifact', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'comb-client-retry-test-'));
  let attempts = 0;
  const { bridge } = client(undefined, request => { attempts++; return unavailable(request); });
  const clock = timing();
  try {
    await assert.rejects(async () => {
      await appendWithRetry(bridge, appendFields, 'peer', { policy: retryPolicy, ...clock });
      await writeSuccessfulCapture(bridge, directory, [], {});
    }, /attempts exhausted/);
    assert.equal(attempts, 4);
    assert.deepEqual(clock.sleeps, [100, 200, 250]);
    await assert.rejects(access(join(directory, 'receipt.json')), { code: 'ENOENT' });
    await assert.rejects(access(join(directory, 'captured-feed.json')), { code: 'ENOENT' });
  } finally {
    await bridge.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('one overall deadline shrinks request budgets and prevents another retry', async () => {
  const clock = timing();
  const budgets = [];
  const bridge = { assertHealthy() {}, async request(op, fields, client, { timeoutMs }) {
    budgets.push(timeoutMs);
    clock.advance(300);
    return unavailable({ id: String(budgets.length) });
  } };
  await assert.rejects(appendWithRetry(bridge, appendFields, 'peer', {
    policy: retryPolicy, ...clock,
  }), /deadline exceeded/);
  assert.deepEqual(budgets, [1000, 600, 100]);
  assert.deepEqual(clock.sleeps, [100, 200]);
});

for (const code of ['conflict', 'cancelled', 'deadline_exceeded', 'fenced', 'reacquire_required', 'integrity', 'invalid_request']) {
  test(`append ${code} is terminal with no retry`, async () => {
    let attempts = 0;
    const clock = timing();
    const { bridge } = client(undefined, request => {
      attempts++;
      return { v: 1, id: request.id, ok: false, error: { code } };
    });
    try {
      await assert.rejects(appendWithRetry(bridge, appendFields, 'peer', { policy: retryPolicy, ...clock }), new RegExp(code));
      assert.equal(attempts, 1);
      assert.deepEqual(clock.sleeps, []);
    } finally { await bridge.close(); }
  });
}

test('cancellation during retry backoff sends no second append', async () => {
  const controller = new AbortController();
  let attempts = 0;
  const { bridge } = client(undefined, request => { attempts++; return unavailable(request); });
  try {
    await assert.rejects(appendWithRetry(bridge, appendFields, 'peer', {
      signal: controller.signal, policy: retryPolicy,
      sleep: async () => { controller.abort(); controller.signal.throwIfAborted(); },
    }), { name: 'AbortError' });
    assert.equal(attempts, 1);
  } finally { await bridge.close(); }
});

test('cancelling a pending request is terminal and cannot retry or write a receipt', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'comb-client-retry-test-'));
  const controller = new AbortController();
  let attempts = 0;
  const { bridge } = client(undefined, () => { attempts++; return null; });
  try {
    const appending = appendWithRetry(bridge, appendFields, 'peer', { signal: controller.signal });
    controller.abort();
    await assert.rejects(appending, { name: 'AbortError' });
    assert.equal(attempts, 1);
    await assert.rejects(bridge.request('hello'), { name: 'AbortError' });
    await assert.rejects(writeSuccessfulCapture(bridge, directory, [], {}));
    await assert.rejects(access(join(directory, 'receipt.json')), { code: 'ENOENT' });
  } finally {
    await bridge.close().catch(() => {});
    await rm(directory, { recursive: true, force: true });
  }
});

test('a late success past the overall deadline does not become an accepted append', async () => {
  const clock = timing();
  const bridge = { assertHealthy() {}, async request() { clock.advance(1001); return appended({ id: 'late', log: 'doc' }); } };
  await assert.rejects(appendWithRetry(bridge, appendFields, 'peer', { policy: retryPolicy, ...clock }), /deadline exceeded/);
});


test('invalid response and late protocol junk are terminal during append recovery', async () => {
  for (const malformed of [true, false]) {
    let attempts = 0;
    const { child, bridge } = client(undefined, request => {
      attempts++;
      return malformed ? { ...unavailable(request), ok: 'false' } : unavailable(request);
    });
    try {
      await assert.rejects(appendWithRetry(bridge, appendFields, 'peer', {
        policy: retryPolicy, ...timing(),
        onAttempt: () => { if (!malformed) child.stdout.write('late junk\n'); },
      }));
      assert.equal(attempts, 1);
    } finally { await bridge.close().catch(() => {}); }
  }
});

test('the actual acceptance runner records exhausted attempts without success artifacts', async () => {
  const { execFile } = await import('node:child_process');
  const { promisify } = await import('node:util');
  const { fileURLToPath } = await import('node:url');
  const root = await mkdtemp(join(tmpdir(), 'comb-runner-retry-test-'));
  const output = join(root, 'capture');
  const binary = join(root, 'unavailable-child.mjs');
  // A transport-only failure fixture, never a storage/readiness substitute.
  await writeFile(binary, `#!/usr/bin/env node
import { createInterface } from 'node:readline';
createInterface({ input: process.stdin }).on('line', line => {
  const r = JSON.parse(line);
  const response = { v: 1, id: r.id, ok: true, op: r.op };
  if (r.op === 'hello') Object.assign(response, {
    limits: { max_frame_bytes: 1048576 },
    capabilities: { durable_idempotency: true, bounded_memory_read: true, ops: ['follow'] }
  });
  if (r.op === 'head') response.head = '0';
  if (r.op === 'follow') response.events = [];
  if (r.op === 'append') Object.assign(response, { ok: false, error: { code: 'backend_unavailable' } });
  process.stdout.write(JSON.stringify(response) + '\\n');
});
`, { mode: 0o700 });
  try {
    await assert.rejects(promisify(execFile)(process.execPath, [
      fileURLToPath(new URL('./foundation-bridge-acceptance.mjs', import.meta.url)), binary, root, output,
    ], { timeout: 15_000 }), /attempts exhausted/);
    const evidence = JSON.parse(await readFile(join(output, 'append-attempts.json'), 'utf8'));
    assert.equal(evidence.policy.maxAttempts, 6);
    assert.ok(evidence.attempts.some(event => event.attempt === 6 && event.outcome === 'backend_unavailable'));
    assert.ok(evidence.attempts.every(event => event.attempt <= 6));
    await assert.rejects(access(join(output, 'receipt.json')), { code: 'ENOENT' });
    await assert.rejects(access(join(output, 'captured-feed.json')), { code: 'ENOENT' });
  } finally { await rm(root, { recursive: true, force: true }); }
});


test('a pending append hits its remaining request deadline without retry', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  let attempts = 0;
  const { bridge } = client(undefined, () => { attempts++; return null; });
  const evidence = [];
  const appending = appendWithRetry(bridge, appendFields, 'peer', {
    policy: retryPolicy, ...timing(), onAttempt: event => evidence.push(event),
  });
  t.mock.timers.tick(retryPolicy.timeoutMs);
  await assert.rejects(appending, /timed out/);
  assert.equal(attempts, 1);
  assert.equal(bridge.pending.size, 0);
  assert.ok(bridge.terminalError);
  assert.equal(evidence[0].outcome, 'transport_or_client_failure');
  await bridge.close().catch(() => {});
});
