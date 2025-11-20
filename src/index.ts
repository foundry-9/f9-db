import path from 'node:path';
import fs from 'node:fs';
import { promises as fsp } from 'node:fs';
import crypto from 'node:crypto';
import type {
  Database,
  DatabaseOptions,
  Document,
  DocumentId,
  Filter,
  FindOptions,
  IndexOptions,
  Logger
} from './types.js';

interface ResolvedOptions extends DatabaseOptions {
  dataDir: string;
  binaryDir: string;
  log: Logger;
}

interface CollectionState {
  docs: Map<DocumentId, Document>;
  loaded: boolean;
  logPath: string;
  snapshotPath: string;
}

const noopLogger: Logger = {
  debug: () => undefined,
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined
};

class JsonFileDatabase implements Database {
  private options: ResolvedOptions;
  private collections = new Map<string, CollectionState>();

  constructor(options: DatabaseOptions = {}) {
    this.options = resolveOptions(options);
    ensureDirectories(this.options);
  }

  async insert(collection: string, doc: Document): Promise<Document> {
    const state = await this.loadCollection(collection);
    const _id = (doc._id as DocumentId | undefined) ?? crypto.randomUUID();

    if (state.docs.has(_id)) {
      throw new Error(`Duplicate _id '${_id}' in collection '${collection}'`);
    }

    const stored: Document = { ...doc, _id };
    state.docs.set(_id, stored);
    await this.appendLog(state, { _id, data: stored });
    return cloneDocument(stored);
  }

  async get(collection: string, id: DocumentId): Promise<Document | null> {
    const state = await this.loadCollection(collection);
    const found = state.docs.get(id);
    return found ? cloneDocument(found) : null;
  }

  async update(
    collection: string,
    id: DocumentId,
    mutation: Partial<Document>
  ): Promise<Document> {
    const state = await this.loadCollection(collection);
    const existing = state.docs.get(id);

    if (!existing) {
      throw new Error(`Document '${id}' not found in collection '${collection}'`);
    }

    const updated: Document = { ...existing, ...mutation, _id: id };
    state.docs.set(id, updated);
    await this.appendLog(state, { _id: id, data: updated });
    return cloneDocument(updated);
  }

  async remove(collection: string, id: DocumentId): Promise<void> {
    const state = await this.loadCollection(collection);

    if (!state.docs.has(id)) {
      throw new Error(`Document '${id}' not found in collection '${collection}'`);
    }

    state.docs.delete(id);
    await this.appendLog(state, { _id: id, tombstone: true });
  }

  async find(
    collection: string,
    filter: Filter = {},
    options: FindOptions = {}
  ): Promise<Document[]> {
    const state = await this.loadCollection(collection);
    const { limit = Infinity, skip = 0, sort } = options;
    const sortKeys = sort ? Object.keys(sort) : [];

    let results = Array.from(state.docs.values()).filter((doc) =>
      matchesFilter(doc, filter)
    );

    if (sortKeys.length > 0) {
      results = results.sort((left, right) =>
        compareDocuments(left, right, sort as Record<string, 1 | -1>)
      );
    }

    return results.slice(skip, skip + limit).map(cloneDocument);
  }

  async *stream(
    collection: string,
    filter: Filter = {},
    options: FindOptions = {}
  ): AsyncIterable<Document> {
    const state = await this.loadCollection(collection);
    const { limit = Infinity, skip = 0, sort } = options;
    const sortKeys = sort ? Object.keys(sort) : [];
    let results = Array.from(state.docs.values()).filter((doc) =>
      matchesFilter(doc, filter)
    );

    if (sortKeys.length > 0) {
      results = results.sort((left, right) =>
        compareDocuments(left, right, sort as Record<string, 1 | -1>)
      );
    }

    let yielded = 0;
    for (let idx = 0; idx < results.length && yielded < limit; idx += 1) {
      if (idx < skip) {
        continue;
      }

      yield cloneDocument(results[idx]);
      yielded += 1;
    }
  }

  async ensureIndex(
    collection: string,
    field: string,
    options: IndexOptions = {}
  ): Promise<void> {
    void options;
    throw new Error(`Indexes are not implemented yet for ${collection}.${field}`);
  }

  async rebuildIndex(collection: string, field: string): Promise<void> {
    throw new Error(`Indexes are not implemented yet for ${collection}.${field}`);
  }

  async join(
    collection: string,
    doc: Document,
    relations: Record<string, unknown>
  ): Promise<Document> {
    void relations;
    throw new Error(`join not implemented for ${collection} with id ${doc._id}`);
  }

  private async loadCollection(collection: string): Promise<CollectionState> {
    const existing = this.collections.get(collection);
    if (existing?.loaded) {
      return existing;
    }

    const state: CollectionState =
      existing ??
      {
        docs: new Map(),
        loaded: false,
        logPath: path.join(this.options.dataDir, `${collection}.jsonl`),
        snapshotPath: path.join(
          this.options.dataDir,
          `${collection}.snapshot.json`
        )
      };

    const snapshotDocs = await readSnapshot(state.snapshotPath);
    snapshotDocs.forEach((doc) => {
      if (doc._id) {
        state.docs.set(doc._id, doc);
      }
    });

    const logEntries = await readLog(state.logPath);
    logEntries.forEach((entry) => {
      if (!entry._id) {
        return;
      }

      if (entry.tombstone) {
        state.docs.delete(entry._id);
        return;
      }

      if (entry.data) {
        state.docs.set(entry._id, entry.data);
      }
    });

    state.loaded = true;
    this.collections.set(collection, state);
    return state;
  }

  private async appendLog(
    state: CollectionState,
    entry: Record<string, unknown>
  ): Promise<void> {
    await fsp.appendFile(state.logPath, `${JSON.stringify(entry)}\n`, 'utf8');
  }
}

function resolveOptions(options: DatabaseOptions): ResolvedOptions {
  const dataDir = options.dataDir ?? path.resolve(process.cwd(), 'data');
  const binaryDir =
    options.binaryDir ?? path.resolve(process.cwd(), 'binaries');
  return {
    ...options,
    dataDir,
    binaryDir,
    log: options.log ?? noopLogger
  };
}

function ensureDirectories(options: ResolvedOptions): void {
  fs.mkdirSync(options.dataDir, { recursive: true });
  fs.mkdirSync(options.binaryDir, { recursive: true });
}

async function readSnapshot(filePath: string): Promise<Document[]> {
  try {
    const raw = await fsp.readFile(filePath, 'utf8');
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      return parsed as Document[];
    }

    if (Array.isArray((parsed as { docs?: unknown }).docs)) {
      return (parsed as { docs: Document[] }).docs;
    }

    return [];
  } catch (error) {
    if (isEnoentError(error)) {
      return [];
    }
    throw error;
  }
}

async function readLog(
  filePath: string
): Promise<Array<{ _id?: DocumentId; data?: Document; tombstone?: boolean }>> {
  try {
    const raw = await fsp.readFile(filePath, 'utf8');
    return raw
      .split('\n')
      .filter(Boolean)
      .map((line) => JSON.parse(line));
  } catch (error) {
    if (isEnoentError(error)) {
      return [];
    }
    throw error;
  }
}

function matchesFilter(doc: Document, filter: Filter): boolean {
  return Object.entries(filter).every(([key, expected]) => {
    if (expected === undefined) {
      return true;
    }

    const value = (doc as Record<string, unknown>)[key];
    if (Array.isArray(expected)) {
      return expected.includes(value);
    }

    return value === expected;
  });
}

function compareDocuments(
  left: Document,
  right: Document,
  sort: Record<string, 1 | -1>
): number {
  for (const key of Object.keys(sort)) {
    const direction = sort[key];
    const a = (left as Record<string, unknown>)[key] as
      | string
      | number
      | boolean
      | null
      | undefined;
    const b = (right as Record<string, unknown>)[key] as
      | string
      | number
      | boolean
      | null
      | undefined;

    if (a === b) {
      continue;
    }

    if (a === undefined || a === null) {
      return direction === 1 ? -1 : 1;
    }

    if (b === undefined || b === null) {
      return direction === 1 ? 1 : -1;
    }

    if (a > b) {
      return direction === 1 ? 1 : -1;
    }

    if (a < b) {
      return direction === 1 ? -1 : 1;
    }
  }

  return 0;
}

function cloneDocument<T>(doc: T): T {
  // structuredClone is available in Node 22; fallback keeps behavior predictable for tests.
  if (typeof structuredClone === 'function') {
    return structuredClone(doc);
  }

  return JSON.parse(JSON.stringify(doc)) as T;
}

function isEnoentError(error: unknown): boolean {
  if (!error || typeof error !== 'object') {
    return false;
  }

  const code = (error as { code?: unknown }).code;
  return code === 'ENOENT';
}

export function createDatabase(options: DatabaseOptions = {}): Database {
  return new JsonFileDatabase(options);
}

export * from './types.js';
