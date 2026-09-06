import assert from 'node:assert/strict';
import test from 'node:test';
import { foundationStableKey } from './foundation-bridge-key.mjs';

test('stable key uses big-endian byte lengths and raw SHA256 bytes', () => {
  assert.equal(foundationStableKey('doc', 'ab'.repeat(32)), '00000003646f6300000020' + 'ab'.repeat(32));
});

test('UTF-8 IDs retain their bytes without Unicode normalization', () => {
  assert.equal(foundationStableKey('ø', '00'.repeat(32)), '00000002c3b800000020' + '00'.repeat(32));
  assert.notEqual(foundationStableKey('é', '00'.repeat(32)), foundationStableKey('e\u0301', '00'.repeat(32)));
});

test('invalid identity bytes fail before constructing a key', () => {
  for (const doc of ['', '\ud800', 'x'.repeat(473)]) {
    assert.throws(() => foundationStableKey(doc, 'ab'.repeat(32)));
  }
  for (const hash of ['ab'.repeat(31), 'AB'.repeat(32), 'zz'.repeat(32)]) {
    assert.throws(() => foundationStableKey('doc', hash));
  }
  assert.equal(foundationStableKey('x'.repeat(472), 'ab'.repeat(32)).length, 1024);
});
