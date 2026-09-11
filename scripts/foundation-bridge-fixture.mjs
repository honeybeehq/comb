import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, readdir, rm, writeFile, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { execFileSync } from 'node:child_process';
import { foundationStableKey } from './foundation-bridge-key.mjs';

const foundationRepo = resolve(process.argv[2] ?? process.cwd());
const { createChain, loadChain, exportBlobs, importBlobs } = await import(
  pathToFileURL(join(foundationRepo, 'packages/engine/src/chain/index.ts')).href
);
const { projectDocument } = await import(
  pathToFileURL(join(foundationRepo, 'packages/engine/src/project/index.ts')).href
);
const { baseDocument } = await import(
  pathToFileURL(join(foundationRepo, 'packages/engine/test/chain-fixtures.ts')).href
);

const docId = '85c0bba4-9d61-4544-a2d3-4e9205ae821f';
const seed = createChain(baseDocument(), { author: 'fixture:seed', message: 'Create shared board' }, { docId });
const peers = ['alice', 'bob'].map(actor => loadChain(seed.save(), { actor: `fixture:${actor}` }));
peers[0].apply({ author: 'fixture:alice', message: 'Edit title and comment offline' }, [
  { op: 'set-text', id: 'n-child-1', text: 'Alice edited this offline' },
  { op: 'annotate', annotation: { id: 'alice:1', nodeId: 'n-child-1', text: 'Check the title', status: 'open' } },
]);
peers[1].apply({ author: 'fixture:bob', message: 'Edit footer and comment offline' }, [
  { op: 'set-style', id: 'n-second-root', prop: 'color', value: 'green' },
  { op: 'annotate', annotation: { id: 'bob:1', nodeId: 'n-second-root', text: 'Check the footer', status: 'open' } },
]);

const mailbox = await mkdtemp(join(tmpdir(), 'comb-foundation-fixture-'));
try {
  await exportBlobs(seed, mailbox);
  const genesisHash = seed.changes()[0].envelope.hash;
  for (const peer of peers) await exportBlobs(peer, mailbox);
  for (const peer of peers) await importBlobs(peer, mailbox);
  assert.deepEqual(peers[0].doc(), peers[1].doc());
  assert.equal(peers[0].doc().annotations.length, 2);
  const projection = projectDocument(peers[0].doc());
  const directory = join(mailbox, docId);
  const filenames = (await readdir(directory)).sort();
  const changes = await Promise.all(filenames.map(async filename => {
    const bytes = await readFile(join(directory, filename));
    const envelopeHash = filename.slice(0, -'.fdnc'.length);
    return {
      idempotency_key: foundationStableKey(docId, envelopeHash),
      envelope_hash: envelopeHash,
      is_genesis: envelopeHash === genesisHash,
      payload_hex: bytes.toString('hex'),
    };
  }));
  assert.equal(changes.length, 3);
  const fixture = {
    description: 'Foundation-generated .fdnc changes from two offline peers. IDs are supplied explicitly to isolate transport from the known Foundation annotation-minter collision.',
    foundation_commit: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: foundationRepo, encoding: 'utf8' }).trim(),
    doc_id: docId,
    changes,
    expected_projection_sha256: createHash('sha256').update(projection).digest('hex'),
    expected_annotations: peers[0].doc().annotations,
  };
  const destination = fileURLToPath(new URL('../docs/testing/fixtures/foundation-bridge.json', import.meta.url));
  await mkdir(resolve(destination, '..'), { recursive: true });
  await writeFile(destination, JSON.stringify(fixture, null, 2) + '\n');
  console.log(JSON.stringify({ fixture: destination, changes: changes.length, annotations: 2, projection_sha256: fixture.expected_projection_sha256 }));
} finally {
  await rm(mailbox, { recursive: true, force: true });
}
