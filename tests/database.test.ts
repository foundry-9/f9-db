import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { afterEach, describe, expect, jest, test } from '@jest/globals';
import { createDatabase } from '../src/index.js';
import type { Database } from '../src/types.js';

function createTempDirs(): { dataDir: string; binaryDir: string } {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'f9-db-data-'));
  const binaryDir = path.join(dataDir, 'binaries');
  fs.mkdirSync(binaryDir, { recursive: true });
  return { dataDir, binaryDir };
}

describe('JsonFileDatabase', () => {
  let dataDir: string;
  let binaryDir: string;
  let db: Database;

  afterEach(() => {
    if (dataDir) {
      fs.rmSync(dataDir, { recursive: true, force: true });
    }
  });

  test('insert and get round trip', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    db = createDatabase({ dataDir, binaryDir });

    const saved = await db.insert('users', { name: 'Ada' });
    expect(saved._id).toBeDefined();

    const fetched = await db.get('users', saved._id as string);
    expect(fetched).toEqual(saved);
  });

  test('update overwrites fields while keeping id', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    db = createDatabase({ dataDir, binaryDir });

    const saved = await db.insert('users', { name: 'Bob', age: 40 });
    const mutated = await db.update('users', saved._id as string, {
      age: 41,
      city: 'Paris'
    });

    expect(mutated).toMatchObject({
      _id: saved._id,
      name: 'Bob',
      age: 41,
      city: 'Paris'
    });
  });

  test('remove deletes a document', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    db = createDatabase({ dataDir, binaryDir });

    const saved = await db.insert('users', { name: 'Carol' });
    await db.remove('users', saved._id as string);

    await expect(db.get('users', saved._id as string)).resolves.toBeNull();
  });

  test('find supports filters, sorting, skip, and limit', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    db = createDatabase({ dataDir, binaryDir });

    await db.insert('users', { name: 'Ada', age: 32 });
    await db.insert('users', { name: 'Bob', age: 40 });
    await db.insert('users', { name: 'Carol', age: 28 });

    const bobs = await db.find('users', { name: 'Bob' });
    expect(bobs).toHaveLength(1);
    expect(bobs[0]?.name).toBe('Bob');

    const sorted = await db.find(
      'users',
      {},
      { sort: { age: 1 }, skip: 1, limit: 2 }
    );
    expect(sorted.map((u) => u.name)).toEqual(['Ada', 'Bob']);
  });

  test('stream yields matching documents respecting limit/skip', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    db = createDatabase({ dataDir, binaryDir });

    await db.insert('users', { name: 'Ada', age: 32 });
    await db.insert('users', { name: 'Bob', age: 40 });
    await db.insert('users', { name: 'Carol', age: 28 });

    const seen: string[] = [];
    for await (const doc of db.stream(
      'users',
      {},
      { sort: { age: -1 }, skip: 1, limit: 1 }
    )) {
      seen.push(doc.name as string);
    }

    expect(seen).toEqual(['Ada']);
  });

  test('auto snapshots after the configured write interval', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    db = createDatabase({
      dataDir,
      binaryDir,
      snapshotInterval: 2,
      autoCompact: true
    });

    await db.insert('users', { name: 'Ada' });
    await db.insert('users', { name: 'Bea' });

    const snapshotPath = path.join(dataDir, 'users.snapshot.json');
    const manifestPath = path.join(dataDir, 'manifest.json');
    const logPath = path.join(dataDir, 'users.jsonl');

    expect(fs.existsSync(snapshotPath)).toBe(true);
    expect(fs.existsSync(manifestPath)).toBe(true);

    const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
    const checkpoint = manifest.collections?.users?.checkpoint;
    expect(typeof checkpoint).toBe('number');

    const logSize = fs.statSync(logPath).size;
    expect(checkpoint).toBe(logSize);

    const snapshot = JSON.parse(fs.readFileSync(snapshotPath, 'utf8'));
    expect(snapshot.docs).toHaveLength(2);
  });

  test('startup replays log entries only after the manifest checkpoint', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    const manifestPath = path.join(dataDir, 'manifest.json');
    const snapshotPath = path.join(dataDir, 'users.snapshot.json');
    const logPath = path.join(dataDir, 'users.jsonl');

    const docId = 'abc123';
    const checkpointPrefix = 'BROKEN JSON\n';
    const snapshotDocs = { docs: [{ _id: docId, name: 'before' }] };
    fs.writeFileSync(snapshotPath, JSON.stringify(snapshotDocs, null, 2), 'utf8');

    const updateEntry = {
      _id: docId,
      data: { _id: docId, name: 'after' }
    };
    fs.writeFileSync(
      logPath,
      `${checkpointPrefix}${JSON.stringify(updateEntry)}\n`,
      'utf8'
    );

    fs.writeFileSync(
      manifestPath,
      JSON.stringify(
        {
          manifestVersion: 1,
          collections: {
            users: {
              checkpoint: Buffer.byteLength(checkpointPrefix),
              snapshotPath
            }
          }
        },
        null,
        2
      ),
      'utf8'
    );

    db = createDatabase({ dataDir, binaryDir });
    const result = await db.get('users', docId);

    expect(result?.name).toBe('after');
  });

  test('truncates log after snapshot by default to reclaim disk', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    db = createDatabase({
      dataDir,
      binaryDir,
      snapshotInterval: 1,
      autoCompact: true
    });

    await db.insert('users', { name: 'Ada' });

    const logPath = path.join(dataDir, 'users.jsonl');
    const manifestPath = path.join(dataDir, 'manifest.json');
    const snapshotPath = path.join(dataDir, 'users.snapshot.json');

    expect(fs.readFileSync(logPath, 'utf8')).toBe('');
    const checkpoint = JSON.parse(fs.readFileSync(manifestPath, 'utf8')).collections
      ?.users?.checkpoint;
    expect(checkpoint).toBe(0);

    const snapshot = JSON.parse(fs.readFileSync(snapshotPath, 'utf8'));
    expect(snapshot.docs).toHaveLength(1);
    expect(snapshot.docs[0]?.name).toBe('Ada');
  });

  test('rotates log after snapshot when logRetention is rotate', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    db = createDatabase({
      dataDir,
      binaryDir,
      snapshotInterval: 1,
      autoCompact: true,
      logRetention: 'rotate'
    });

    await db.insert('users', { name: 'Ada' });

    const logPath = path.join(dataDir, 'users.jsonl');
    const manifestPath = path.join(dataDir, 'manifest.json');
    const rotatedFiles = fs
      .readdirSync(dataDir)
      .filter((file) => file.startsWith('users.jsonl.') && file.endsWith('.bak'));

    expect(rotatedFiles.length).toBe(1);
    expect(fs.readFileSync(logPath, 'utf8')).toBe('');

    const checkpoint = JSON.parse(fs.readFileSync(manifestPath, 'utf8')).collections
      ?.users?.checkpoint;
    expect(checkpoint).toBe(0);

    const rotatedContent = fs.readFileSync(
      path.join(dataDir, rotatedFiles[0] as string),
      'utf8'
    );
    expect(rotatedContent).toContain('"name":"Ada"');
  });

  test('ensureIndex builds index file and records checkpoint/stats', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    db = createDatabase({ dataDir, binaryDir, autoCompact: false });

    const ada = await db.insert('users', { name: 'Ada Lovelace' });
    const bob = await db.insert('users', { name: 'Bob' });

    await db.ensureIndex('users', 'name');

    const manifestPath = path.join(dataDir, 'manifest.json');
    const logPath = path.join(dataDir, 'users.jsonl');
    const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
    const metadata = manifest.collections?.users?.indexes?.name;

    expect(metadata?.state).toBe('ready');
    expect(metadata?.checkpoint).toBe(fs.statSync(logPath).size);
    expect(metadata?.stats?.docCount).toBe(2);
    expect(metadata?.stats?.tokenCount).toBeGreaterThanOrEqual(2);

    const indexPath = path.join(dataDir, 'indexes', 'users', 'name.json');
    const indexFile = JSON.parse(fs.readFileSync(indexPath, 'utf8'));
    expect(indexFile.meta.state).toBe('ready');
    expect(indexFile.entries.ada).toEqual([ada._id]);
    expect(indexFile.entries.bob).toEqual([bob._id]);
  });

  test('rebuildIndex refreshes entries and bumps version/checkpoint', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    db = createDatabase({ dataDir, binaryDir, autoCompact: false });

    await db.insert('users', { name: 'Ada' });
    await db.ensureIndex('users', 'name');

    const manifestPath = path.join(dataDir, 'manifest.json');
    const firstMetadata = JSON.parse(fs.readFileSync(manifestPath, 'utf8')).collections
      ?.users?.indexes?.name;

    const carol = await db.insert('users', { name: 'Carol' });
    await db.rebuildIndex('users', 'name');

    const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
    const metadata = manifest.collections?.users?.indexes?.name;
    const logPath = path.join(dataDir, 'users.jsonl');

    expect(metadata?.version).toBe((firstMetadata?.version ?? 0) + 1);
    expect(metadata?.state).toBe('ready');
    expect(metadata?.checkpoint).toBe(fs.statSync(logPath).size);
    expect(metadata?.stats?.docCount).toBe(2);

    const indexPath = path.join(dataDir, 'indexes', 'users', 'name.json');
    const indexFile = JSON.parse(fs.readFileSync(indexPath, 'utf8'));
    expect(indexFile.entries.carol).toEqual([carol._id]);
  });

  test('find emits log when an index is used to satisfy a filter', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    const debug = jest.fn();
    db = createDatabase({ dataDir, binaryDir, log: { debug } });

    await db.insert('users', { name: 'Ada Lovelace' });
    await db.insert('users', { name: 'Bob' });
    await db.ensureIndex('users', 'name');

    debug.mockClear();
    const result = await db.find('users', { name: 'Ada Lovelace' });
    expect(result).toHaveLength(1);

    expect(debug).toHaveBeenCalledWith(
      'Using index for query',
      expect.objectContaining({
        collection: 'users',
        fields: expect.arrayContaining(['name']),
        candidateCount: 1
      })
    );
  });

  test('unique indexes are enforced on insert, update, and remove', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    db = createDatabase({ dataDir, binaryDir });

    await db.ensureIndex('users', 'name', { unique: true });
    const first = await db.insert('users', { name: 'Ada' });

    await expect(db.insert('users', { name: 'Ada' })).rejects.toThrow(
      /Unique constraint violated/
    );

    const second = await db.insert('users', { name: 'Bob' });
    const third = await db.insert('users', { name: 'Carol' });

    await expect(db.update('users', third._id as string, { name: 'Bob' })).rejects.toThrow(
      /Unique constraint violated/
    );

    await db.remove('users', first._id as string);
    const replacement = await db.insert('users', { name: 'Ada' });

    const indexPath = path.join(dataDir, 'indexes', 'users', 'name.json');
    const indexFile = JSON.parse(fs.readFileSync(indexPath, 'utf8'));
    expect(indexFile.entries.ada).toEqual([replacement._id]);
    expect(Object.values(indexFile.entries).flat().filter((id) => id === second._id)).toHaveLength(
      1
    );
  });

  test('schema validation enforces types, required fields, and defaults', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    db = createDatabase({
      dataDir,
      binaryDir,
      schemas: {
        users: {
          fields: {
            name: { type: 'string', required: true, minLength: 2 },
            age: { type: 'number', min: 0 },
            active: { type: 'boolean', default: true },
            tags: {
              type: 'array',
              required: true,
              minItems: 1,
              items: { type: 'string' }
            },
            profile: {
              type: 'object',
              required: true,
              fields: { city: { type: 'string', required: true } }
            }
          }
        }
      }
    });

    const saved = await db.insert('users', {
      name: 'Ada',
      age: 32,
      tags: ['math'],
      profile: { city: 'London' }
    });
    expect(saved.active).toBe(true);

    await expect(
      db.insert('users', {
        age: 10,
        tags: ['kid'],
        profile: { city: 'NYC' }
      })
    ).rejects.toThrow(/Field 'name' is required/);

    await expect(
      db.insert('users', {
        name: 'Bo',
        age: 5,
        tags: [],
        profile: { city: 'SF' }
      })
    ).rejects.toThrow(/must have at least 1 items/);

    await expect(
      db.update('users', saved._id as string, {
        age: 'old' as unknown as number
      })
    ).rejects.toThrow(/must be a number/);
  });

  test('join resolves relations with batching and projection', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    db = createDatabase({ dataDir, binaryDir });

    const author = await db.insert('users', { name: 'Ada', role: 'author' });
    const reviewer = await db.insert('users', { name: 'Bob', role: 'reviewer' });
    const extra = await db.insert('users', { name: 'Carol', role: 'reviewer' });

    const post = await db.insert('posts', {
      title: 'Hello',
      authorId: author._id,
      reviewerIds: [reviewer._id, extra._id, 'missing-id']
    });

    const joined = await db.join('posts', post, {
      author: {
        localField: 'authorId',
        foreignCollection: 'users',
        projection: ['name']
      },
      reviewers: {
        localField: 'reviewerIds',
        foreignCollection: 'users',
        many: true,
        projection: ['name', 'role']
      }
    });

    expect(joined.title).toBe('Hello');
    expect(joined.author).toEqual({ _id: author._id, name: 'Ada' });
    expect(Array.isArray(joined.reviewers)).toBe(true);
    expect((joined.reviewers as unknown[])?.length).toBe(2);
    expect((joined.reviewers as Record<string, unknown>[])[0]).toMatchObject({
      _id: reviewer._id,
      name: 'Bob',
      role: 'reviewer'
    });
  });

  test('join returns null/empty when relations are missing', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    db = createDatabase({ dataDir, binaryDir });

    const post = await db.insert('posts', { title: 'Lonely' });
    const joined = await db.join('posts', post, {
      author: { localField: 'authorId', foreignCollection: 'users' },
      tags: { localField: 'tagIds', foreignCollection: 'tags', many: true }
    });

    expect(joined.author).toBeNull();
    expect(joined.tags).toEqual([]);
  });

  test('join cache clears after writes so subsequent joins see updates', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    db = createDatabase({ dataDir, binaryDir });

    const user = await db.insert('users', { name: 'Ada' });
    const post = await db.insert('posts', { authorId: user._id });

    const firstJoin = await db.join('posts', post, {
      author: { localField: 'authorId', foreignCollection: 'users', projection: ['name'] }
    });
    expect(firstJoin.author).toEqual({ _id: user._id, name: 'Ada' });

    await db.update('users', user._id as string, { name: 'Ada Lovelace' });

    const secondJoin = await db.join('posts', post, {
      author: { localField: 'authorId', foreignCollection: 'users', projection: ['name'] }
    });
    expect(secondJoin.author).toEqual({ _id: user._id, name: 'Ada Lovelace' });
  });

  test('join projections support nested fields', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    db = createDatabase({ dataDir, binaryDir });

    const user = await db.insert('users', {
      name: 'Ada',
      profile: { city: 'London', employer: { name: 'Analytical Engine' } }
    });
    const post = await db.insert('posts', { authorId: user._id });

    const joined = await db.join('posts', post, {
      author: {
        localField: 'authorId',
        foreignCollection: 'users',
        projection: ['profile.city', 'profile.employer.name']
      }
    });

    expect(joined.author).toEqual({
      _id: user._id,
      profile: { city: 'London', employer: { name: 'Analytical Engine' } }
    });
  });

  test('join cache obeys max entries and evicts oldest', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    db = createDatabase({ dataDir, binaryDir, joinCacheMaxEntries: 1 });

    const alice = await db.insert('users', { name: 'Alice' });
    const bob = await db.insert('users', { name: 'Bob' });

    const postA = await db.insert('posts', { authorId: alice._id });
    const postB = await db.insert('posts', { authorId: bob._id });

    const first = await db.join('posts', postA, {
      author: { localField: 'authorId', foreignCollection: 'users', projection: ['name'] }
    });
    expect(first.author).toEqual({ _id: alice._id, name: 'Alice' });

    await db.join('posts', postB, {
      author: { localField: 'authorId', foreignCollection: 'users', projection: ['name'] }
    });

    const state = await (db as unknown as { loadCollection: (c: string) => Promise<unknown> }).loadCollection(
      'users'
    );
    const usersState = (state as { docs: Map<string, Record<string, unknown>> }).docs;
    usersState.set(alice._id as string, { _id: alice._id, name: 'Alice updated' });

    const rejoined = await db.join('posts', postA, {
      author: { localField: 'authorId', foreignCollection: 'users', projection: ['name'] }
    });
    expect(rejoined.author).toEqual({ _id: alice._id, name: 'Alice updated' });
  });

  test('join cache TTL refreshes entries after expiry', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    db = createDatabase({ dataDir, binaryDir, joinCacheTTLms: 5 });

    const alice = await db.insert('users', { name: 'Alice' });
    const post = await db.insert('posts', { authorId: alice._id });

    const first = await db.join('posts', post, {
      author: { localField: 'authorId', foreignCollection: 'users', projection: ['name'] }
    });
    expect(first.author).toEqual({ _id: alice._id, name: 'Alice' });

    const state = await (db as unknown as { loadCollection: (c: string) => Promise<unknown> }).loadCollection(
      'users'
    );
    const usersState = (state as { docs: Map<string, Record<string, unknown>> }).docs;
    usersState.set(alice._id as string, { _id: alice._id, name: 'Alice new' });

    const beforeExpiry = await db.join('posts', post, {
      author: { localField: 'authorId', foreignCollection: 'users', projection: ['name'] }
    });
    expect(beforeExpiry.author).toEqual({ _id: alice._id, name: 'Alice' });

    await new Promise((resolve) => setTimeout(resolve, 10));

    const afterExpiry = await db.join('posts', post, {
      author: { localField: 'authorId', foreignCollection: 'users', projection: ['name'] }
    });
    expect(afterExpiry.author).toEqual({ _id: alice._id, name: 'Alice new' });
  });

  test('clearJoinCache manually flushes cached relations', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    db = createDatabase({ dataDir, binaryDir });

    const alice = await db.insert('users', { name: 'Alice' });
    const post = await db.insert('posts', { authorId: alice._id });

    const first = await db.join('posts', post, {
      author: { localField: 'authorId', foreignCollection: 'users', projection: ['name'] }
    });
    expect(first.author).toEqual({ _id: alice._id, name: 'Alice' });

    await db.update('users', alice._id as string, { name: 'Alice updated' });
    const cached = await db.join('posts', post, {
      author: { localField: 'authorId', foreignCollection: 'users', projection: ['name'] }
    });
    expect(cached.author).toEqual({ _id: alice._id, name: 'Alice updated' });
    await db.update('users', alice._id as string, { name: 'Alice final' });

    db.clearJoinCache();

    const refreshed = await db.join('posts', post, {
      author: { localField: 'authorId', foreignCollection: 'users', projection: ['name'] }
    });
    expect(refreshed.author).toEqual({ _id: alice._id, name: 'Alice final' });
  });

  test('binary store writes hashed files, dedupes, and reads content', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    db = createDatabase({ dataDir, binaryDir });

    const saved = await db.saveBinary('hello', { mimeType: 'text/plain' });
    const filePath = path.join(binaryDir, saved.sha256);
    expect(fs.existsSync(filePath)).toBe(true);
    expect(saved.deduped).toBe(false);

    const deduped = await db.saveBinary('hello');
    expect(deduped.sha256).toBe(saved.sha256);
    expect(deduped.deduped).toBe(true);

    const read = await db.readBinary(saved.sha256);
    expect(read?.toString()).toBe('hello');

    const manifest = JSON.parse(
      fs.readFileSync(path.join(dataDir, 'manifest.json'), 'utf8')
    );
    expect(manifest.binaries[saved.sha256]).toMatchObject({
      size: saved.size,
      mimeType: 'text/plain',
      refCount: 0
    });
  });

  test('binary refs on documents update manifest counts and gate deletion', async () => {
    ({ dataDir, binaryDir } = createTempDirs());
    db = createDatabase({ dataDir, binaryDir });

    const binary = await db.saveBinary(Buffer.from('payload'), { mimeType: 'application/octet-stream' });
    const doc = await db.insert('files', {
      name: 'file1',
      _binRefs: [{ field: 'content', sha256: binary.sha256, size: binary.size, mimeType: binary.mimeType }]
    });

    let manifest = JSON.parse(fs.readFileSync(path.join(dataDir, 'manifest.json'), 'utf8'));
    expect(manifest.binaries[binary.sha256].refCount).toBe(1);

    await expect(db.deleteBinary(binary.sha256)).rejects.toThrow(/Cannot delete binary/);

    await db.update('files', doc._id as string, {
      _binRefs: []
    });

    manifest = JSON.parse(fs.readFileSync(path.join(dataDir, 'manifest.json'), 'utf8'));
    expect(manifest.binaries[binary.sha256].refCount).toBe(0);

    const removed = await db.deleteBinary(binary.sha256);
    expect(removed).toBe(true);
    expect(fs.existsSync(path.join(binaryDir, binary.sha256))).toBe(false);
  });
});
