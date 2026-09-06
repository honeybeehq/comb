import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';

export class Bridge {
  pending = new Map();
  nextId = 0;
  buffer = '';
  stderr = '';
  maxFrameBytes = 1_048_576;
  terminalError = null;
  closing = false;
  closed = false;

  constructor(binary, directory, spawnProcess = spawn) {
    this.child = spawnProcess(binary, ['--dir', directory], { stdio: ['pipe', 'pipe', 'pipe'] });
    this.exit = new Promise(resolve => {
      // close follows exit and the final stdio data. A receipt cannot precede it.
      this.child.once('close', (code, signal) => {
        this.closed = true;
        if (!this.closing || code !== 0 || this.pending.size || this.buffer.length) {
          this.fail(new Error(`unclean bridge close: code=${code}, signal=${signal}, pending=${this.pending.size}, partial_frame=${this.buffer.length}; ${this.stderr}`));
        }
        resolve({ code, signal });
      });
      this.child.once('error', error => {
        this.closed = true;
        this.fail(error);
        resolve({ code: null, signal: null, error: error.message });
      });
    });
    this.child.stdin.on('error', error => this.fail(error));
    this.child.stdout.on('error', error => this.fail(error));
    this.child.stderr.on('error', error => this.fail(error));
    this.child.stderr.setEncoding('utf8');
    this.child.stderr.on('data', text => { this.stderr = (this.stderr + text).slice(-32_768); });
    this.child.stdout.setEncoding('utf8');
    this.child.stdout.on('data', text => {
      if (this.terminalError) return;
      try {
        this.buffer += text;
        let newline;
        while ((newline = this.buffer.indexOf('\n')) !== -1) {
          assert.ok(Buffer.byteLength(this.buffer.slice(0, newline)) <= this.maxFrameBytes, 'bridge response exceeds its wire frame limit');
          const response = JSON.parse(this.buffer.slice(0, newline));
          this.buffer = this.buffer.slice(newline + 1);
          assert.equal(response.v, 1);
          const pending = this.pending.get(response.id);
          assert.ok(pending, `unsolicited response ${response.id}`);
          this.pending.delete(response.id);
          pending.cleanup();
          pending.resolve(response);
        }
        assert.ok(Buffer.byteLength(this.buffer) <= this.maxFrameBytes, 'bridge emitted an unbounded partial frame');
      } catch (error) {
        this.fail(error);
        this.child.kill();
      }
    });
  }

  fail(error) {
    this.terminalError ??= error;
    for (const pending of this.pending.values()) {
      pending.cleanup();
      pending.reject(this.terminalError);
    }
    this.pending.clear();
  }

  assertHealthy() {
    if (this.terminalError) throw this.terminalError;
  }

  async request(method, fields = {}, client = 'client', { timeoutMs = 60_000, signal } = {}) {
    this.assertHealthy();
    signal?.throwIfAborted();
    assert.ok(Number.isFinite(timeoutMs) && timeoutMs > 0, 'request timeout must be finite and positive');
    assert.ok(!this.closing && !this.closed, 'request after bridge close');
    const id = `${client}-${++this.nextId}`;
    const frame = JSON.stringify({ v: 1, id, op: method, ...fields });
    assert.ok(Buffer.byteLength(frame) <= this.maxFrameBytes, 'test request exceeds negotiated frame limit');
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.fail(new Error(`request ${id}/${method} timed out`));
        this.child.kill();
      }, timeoutMs);
      const abort = () => {
        this.fail(signal.reason ?? new DOMException('request cancelled', 'AbortError'));
        this.child.kill();
      };
      const cleanup = () => {
        clearTimeout(timer);
        signal?.removeEventListener('abort', abort);
      };
      this.pending.set(id, { resolve, reject, cleanup });
      signal?.addEventListener('abort', abort, { once: true });
      try {
        this.child.stdin.write(frame + '\n');
      } catch (error) {
        this.fail(error);
      }
    });
  }

  async ok(method, fields, client) {
    const response = await this.request(method, fields, client);
    this.assertHealthy();
    assert.equal(response.ok, true, JSON.stringify(response));
    const { v, id, ok, op, ...result } = response;
    return result;
  }

  async close() {
    const endInput = !this.closing && !this.closed;
    this.closing = true;
    if (endInput) this.child.stdin.end();
    const timer = setTimeout(() => {
      this.fail(new Error('bridge did not close within 5 seconds'));
      this.child.kill();
    }, 5_000);
    try {
      const result = await this.exit;
      this.assertHealthy();
      return result;
    } finally {
      clearTimeout(timer);
    }
  }
}

export async function reserveOutputDirectory(directory) {
  await mkdir(dirname(directory), { recursive: true });
  // Deliberately no recursive/exist-ok here: every invocation owns a new directory.
  await mkdir(directory);
}

export async function writeSuccessfulCapture(bridge, directory, changes, receipt) {
  await bridge.close();
  bridge.assertHealthy();
  const success = { ...receipt, ok: true };
  await writeFile(join(directory, 'captured-feed.json'), JSON.stringify({ changes }, null, 2) + '\n', { flag: 'wx' });
  await writeFile(join(directory, 'receipt.json'), JSON.stringify(success, null, 2) + '\n', { flag: 'wx' });
  return success;
}

export const APPEND_RETRY_POLICY = Object.freeze({
  timeoutMs: 60_000,
  maxAttempts: 6,
  initialBackoffMs: 100,
  maxBackoffMs: 2_000,
});

// Only an explicit unavailable append response is retried. Transport failures,
// request timeouts, conflict, and cancellation remain terminal for this run.
export async function appendWithRetry(bridge, fields, client, {
  policy = APPEND_RETRY_POLICY,
  signal,
  onAttempt = () => {},
  now = () => performance.now(),
  sleep = (ms, signal) => delay(ms, undefined, { signal }),
} = {}) {
  const { timeoutMs, maxAttempts, initialBackoffMs, maxBackoffMs } = policy;
  assert.ok(Number.isFinite(timeoutMs) && timeoutMs > 0);
  assert.ok(Number.isSafeInteger(maxAttempts) && maxAttempts > 0);
  assert.ok(Number.isFinite(initialBackoffMs) && initialBackoffMs > 0);
  assert.ok(Number.isFinite(maxBackoffMs) && maxBackoffMs >= initialBackoffMs);
  const original = Object.freeze({
    log: fields.log,
    idempotency_key: fields.idempotency_key,
    payload_hex: fields.payload_hex,
  });
  for (const value of Object.values(original)) assert.equal(typeof value, 'string');
  const fingerprint = text => createHash('sha256').update(text).digest('hex');
  const identity = {
    client,
    log: original.log,
    // Hash the exact wire strings so casing changes are observable too.
    key_hex_sha256: fingerprint(original.idempotency_key),
    payload_hex_sha256: fingerprint(original.payload_hex),
  };
  const start = now();
  const deadline = start + timeoutMs;
  const exhausted = why => new Error(`append retry ${why}; outcome may be committed; retry the same log, key and bytes`);
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    signal?.throwIfAborted();
    bridge.assertHealthy();
    const remaining = deadline - now();
    if (remaining <= 0) throw exhausted('deadline exceeded');
    let response;
    try {
      response = await bridge.request('append', original, client, { timeoutMs: remaining, signal });
      bridge.assertHealthy();
    } catch (error) {
      onAttempt({ ...identity, attempt, elapsed_ms: now() - start, outcome: 'transport_or_client_failure', message: error.message });
      throw error;
    }
    const code = response.ok === true ? 'ok' : response.ok === false ? response.error?.code : undefined;
    const backoff = Math.min(initialBackoffMs * 2 ** (attempt - 1), maxBackoffMs);
    const expired = now() >= deadline;
    const retry = !expired && code === 'backend_unavailable' && attempt < maxAttempts && deadline - now() > backoff;
    onAttempt({ ...identity, attempt, request_id: response.id, elapsed_ms: now() - start,
      outcome: code ?? 'invalid_response', deadline_exceeded: expired, retry, backoff_ms: retry ? backoff : 0 });
    signal?.throwIfAborted();
    if (expired) throw exhausted('deadline exceeded');
    if (response.ok === true) {
      const { v, id, ok, op, ...result } = response;
      return result;
    }
    assert.equal(code, 'backend_unavailable', JSON.stringify(response));
    if (attempt === maxAttempts) throw exhausted('attempts exhausted');
    if (!retry) throw exhausted('deadline exceeded');
    await sleep(backoff, signal);
  }
}
