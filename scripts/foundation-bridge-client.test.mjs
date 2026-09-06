import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { access, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { PassThrough, Writable } from 'node:stream';
import test from 'node:test';
import { Bridge, reserveOutputDirectory, writeSuccessfulCapture } from './foundation-bridge-client.mjs';

// Only the client lifecycle is simulated. This child has no append/read/storage.
function client(onClose = () => {}) {
  const child = new EventEmitter();
  child.stdout = new PassThrough();
  child.stderr = new PassThrough();
  child.closed = false;
  child.stdin = new Writable({
    write(chunk, encoding, callback) {
      const request = JSON.parse(chunk.toString());
      child.stdout.write(JSON.stringify({ v: 1, id: request.id, ok: true, op: request.op }) + '\n');
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
