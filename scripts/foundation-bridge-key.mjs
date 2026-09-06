import assert from 'node:assert/strict';

// Foundation identity encoding from the Comb stable-key addendum. This is a
// caller key, not a receipt or a Comb storage format.
export function foundationStableKey(docId, envelopeHash) {
  assert.equal(typeof docId, 'string');
  assert.ok(docId.length > 0, 'document ID must not be empty');
  assert.match(envelopeHash, /^[0-9a-f]{64}$/, 'envelope hash must be canonical SHA256 hex');
  const document = Buffer.from(docId, 'utf8');
  assert.equal(document.toString('utf8'), docId, 'document ID must encode losslessly as UTF-8');
  assert.ok(document.length + 40 <= 512, 'stable key exceeds 512 bytes');
  const key = Buffer.alloc(document.length + 40);
  key.writeUInt32BE(document.length, 0);
  document.copy(key, 4);
  key.writeUInt32BE(32, document.length + 4);
  Buffer.from(envelopeHash, 'hex').copy(key, document.length + 8);
  return key.toString('hex');
}
