import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { afterEach, describe, expect, test } from '@jest/globals';
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
});
