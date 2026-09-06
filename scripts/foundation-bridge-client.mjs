import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
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
          clearTimeout(pending.timer);
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
      clearTimeout(pending.timer);
      pending.reject(this.terminalError);
    }
    this.pending.clear();
  }

  assertHealthy() {
    if (this.terminalError) throw this.terminalError;
  }

  async request(method, fields = {}, client = 'client') {
    this.assertHealthy();
    assert.ok(!this.closing && !this.closed, 'request after bridge close');
    const id = `${client}-${++this.nextId}`;
    const frame = JSON.stringify({ v: 1, id, op: method, ...fields });
    assert.ok(Buffer.byteLength(frame) <= this.maxFrameBytes, 'test request exceeds negotiated frame limit');
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.fail(new Error(`request ${id}/${method} timed out`));
        this.child.kill();
      }, 60_000);
      this.pending.set(id, { resolve, reject, timer });
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
