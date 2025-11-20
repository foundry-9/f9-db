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
  IndexMetadata,
  IndexStats,
  JoinRelations,
  Logger,
  NormalizedIndexOptions,
  TokenizerOptions
} from './types.js';

interface ResolvedOptions extends DatabaseOptions {
  dataDir: string;
  binaryDir: string;
  log: Logger;
  logRetention: 'truncate' | 'rotate' | 'keep';
  indexDir: string;
  tokenizer: TokenizerOptions;
}

interface CollectionState {
  name: string;
  docs: Map<DocumentId, Document>;
  loaded: boolean;
  logPath: string;
  snapshotPath: string;
  writesSinceSnapshot: number;
  lastCheckpoint: number;
}

interface ManifestCollectionEntry {
  checkpoint: number;
  snapshotPath: string;
  schema?: Record<string, unknown>;
  indexes: Record<string, IndexMetadata | undefined>;
  updatedAt?: string;
}

interface Manifest {
  manifestVersion: number;
  collections: Record<string, ManifestCollectionEntry | undefined>;
}

interface IndexFilePayload {
  meta: IndexMetadata;
  entries: Record<string, DocumentId[]>;
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
  private manifest: Manifest | null = null;
  private manifestPath: string;

  constructor(options: DatabaseOptions = {}) {
    this.options = resolveOptions(options);
     this.manifestPath = path.join(this.options.dataDir, 'manifest.json');
    ensureDirectories(this.options);
  }

  async insert(collection: string, doc: Document): Promise<Document> {
    const state = await this.loadCollection(collection);
    const _id = (doc._id as DocumentId | undefined) ?? crypto.randomUUID();

    if (state.docs.has(_id)) {
      throw new Error(`Duplicate _id '${_id}' in collection '${collection}'`);
    }

    const stored: Document = { ...doc, _id };
    await this.enforceUniqueConstraints(collection, stored, state, null);
    state.docs.set(_id, stored);
    await this.appendLog(collection, state, { _id, data: stored });
    await this.refreshIndexesForWrite(collection, state, stored, null);
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
    await this.enforceUniqueConstraints(collection, updated, state, id);
    state.docs.set(id, updated);
    await this.appendLog(collection, state, { _id: id, data: updated });
    await this.refreshIndexesForWrite(collection, state, updated, existing);
    return cloneDocument(updated);
  }

  async remove(collection: string, id: DocumentId): Promise<void> {
    const state = await this.loadCollection(collection);

    if (!state.docs.has(id)) {
      throw new Error(`Document '${id}' not found in collection '${collection}'`);
    }

    const existing = state.docs.get(id) ?? null;
    state.docs.delete(id);
    await this.appendLog(collection, state, { _id: id, tombstone: true });
    await this.refreshIndexesForWrite(collection, state, null, existing);
  }

  async find(
    collection: string,
    filter: Filter = {},
    options: FindOptions = {}
  ): Promise<Document[]> {
    const state = await this.loadCollection(collection);
    const { limit = Infinity, skip = 0, sort } = options;
    const sortKeys = sort ? Object.keys(sort) : [];

    const candidates = await this.getIndexedCandidates(collection, filter, state);
    const docsToScan = candidates
      ? Array.from(candidates)
          .map((id) => state.docs.get(id))
          .filter(Boolean) as Document[]
      : Array.from(state.docs.values());

    let results = docsToScan.filter((doc) => matchesFilter(doc, filter));

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
    const candidates = await this.getIndexedCandidates(collection, filter, state);
    const docsToScan = candidates
      ? Array.from(candidates)
          .map((id) => state.docs.get(id))
          .filter(Boolean) as Document[]
      : Array.from(state.docs.values());
    let results = docsToScan.filter((doc) => matchesFilter(doc, filter));

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
    const state = await this.loadCollection(collection);
    const normalized = normalizeIndexOptions(options, this.options.tokenizer);
    const indexPath = this.getIndexPath(collection, field);

    const manifest = await this.getManifest();
    const collectionEntry = manifest.collections[collection];
    const existing = collectionEntry?.indexes?.[field];
    const now = new Date().toISOString();
    const optionsChanged = existing
      ? !indexOptionsEqual(existing.options, normalized)
      : true;

    const metadata: IndexMetadata = {
      field,
      path: indexPath,
      options: normalized,
      version: optionsChanged
        ? (existing?.version ?? 0) + 1
        : existing?.version ?? 1,
      state: optionsChanged ? 'pending' : existing?.state ?? 'pending',
      checkpoint: optionsChanged ? 0 : existing?.checkpoint ?? 0,
      createdAt: existing?.createdAt ?? now,
      updatedAt: now,
      stats: optionsChanged ? undefined : existing?.stats,
      lastError: optionsChanged ? undefined : existing?.lastError
    };

    await this.updateManifest(collection, {
      indexes: { [field]: metadata }
    });

    await writeIndexStub(indexPath, metadata);
    await this.buildIndex(collection, field, metadata, state);
    this.options.log?.info?.('Index metadata recorded', {
      collection,
      field,
      state: metadata.state,
      version: metadata.version
    });
  }

  async rebuildIndex(collection: string, field: string): Promise<void> {
    const state = await this.loadCollection(collection);
    const manifest = await this.getManifest();
    const existing = manifest.collections[collection]?.indexes?.[field];

    if (!existing) {
      await this.ensureIndex(collection, field);
      return;
    }

    const metadata: IndexMetadata = {
      ...existing,
      state: 'pending',
      checkpoint: 0,
      stats: undefined,
      lastError: undefined,
      version: existing.version + 1,
      updatedAt: new Date().toISOString()
    };

    await this.updateManifest(collection, {
      indexes: { [field]: metadata }
    });

    await writeIndexStub(this.getIndexPath(collection, field), metadata);
    await this.buildIndex(collection, field, metadata, state);
    this.options.log?.info?.('Index rebuild scheduled', {
      collection,
      field,
      version: metadata.version
    });
  }

  async join(
    collection: string,
    doc: Document,
    relations: JoinRelations
  ): Promise<Document> {
    this.options.log?.warn?.('join is stubbed; returning original document', {
      collection,
      id: doc._id,
      relations: Object.keys(relations ?? {})
    });

    return cloneDocument(doc);
  }

  async compact(collection: string): Promise<void> {
    const state = await this.loadCollection(collection);
    await this.compactCollection(collection, state);
  }

  private async loadCollection(collection: string): Promise<CollectionState> {
    const existing = this.collections.get(collection);
    if (existing?.loaded) {
      return existing;
    }

    const manifest = await this.getManifest();
    const manifestEntry = manifest.collections[collection];

    const state: CollectionState =
      existing ??
      {
        name: collection,
        docs: new Map(),
        loaded: false,
        logPath: path.join(this.options.dataDir, `${collection}.jsonl`),
        snapshotPath: path.join(
          this.options.dataDir,
          `${collection}.snapshot.json`
        ),
        writesSinceSnapshot: 0,
        lastCheckpoint: manifestEntry?.checkpoint ?? 0
      };

    await this.ensureManifestEntry(collection, state.snapshotPath);

    const snapshotDocs = await readSnapshot(state.snapshotPath);
    snapshotDocs.forEach((doc) => {
      if (doc._id) {
        state.docs.set(doc._id, doc);
      }
    });

    const logEntries = await readLog(state.logPath, state.lastCheckpoint);
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

    state.writesSinceSnapshot = logEntries.length;
    state.loaded = true;
    this.collections.set(collection, state);
    return state;
  }

  private async appendLog(
    collection: string,
    state: CollectionState,
    entry: Record<string, unknown>
  ): Promise<void> {
    await fsp.appendFile(state.logPath, `${JSON.stringify(entry)}\n`, 'utf8');
    state.writesSinceSnapshot += 1;

    const interval = this.options.snapshotInterval ?? 0;
    const shouldCompact =
      (this.options.autoCompact ?? true) &&
      interval > 0 &&
      state.writesSinceSnapshot >= interval;

    if (shouldCompact) {
      await this.compactCollection(collection, state);
    }
  }

  private async compactCollection(
    collection: string,
    state: CollectionState
  ): Promise<void> {
    const docs = Array.from(state.docs.values()).map(cloneDocument);
    const snapshotPayload = JSON.stringify({ docs }, null, 2);
    await fsp.writeFile(state.snapshotPath, snapshotPayload, 'utf8');

    let checkpoint = await getFileSize(state.logPath);

    if (this.options.logRetention === 'truncate') {
      await truncateFile(state.logPath);
      checkpoint = 0;
    } else if (this.options.logRetention === 'rotate') {
      const rotatedPath = await rotateLogFile(state.logPath);
      this.options.log?.info?.('Rotated log file', {
        collection,
        rotatedPath
      });
      await fsp.writeFile(state.logPath, '', 'utf8');
      checkpoint = 0;
    }

    state.lastCheckpoint = checkpoint;
    state.writesSinceSnapshot = 0;

    await this.updateManifest(collection, {
      checkpoint,
      snapshotPath: state.snapshotPath
    });
  }

  private async getManifest(): Promise<Manifest> {
    if (this.manifest) {
      return this.manifest;
    }

    try {
      const raw = await fsp.readFile(this.manifestPath, 'utf8');
      const parsed = JSON.parse(raw) as Manifest;
      this.manifest = normalizeManifest(parsed);
    } catch (error) {
      if (isEnoentError(error)) {
        this.manifest = { manifestVersion: 1, collections: {} };
      } else {
        throw error;
      }
    }

    return this.manifest;
  }

  private async persistManifest(manifest: Manifest): Promise<void> {
    this.manifest = manifest;
    await fsp.writeFile(
      this.manifestPath,
      JSON.stringify(manifest, null, 2),
      'utf8'
    );
  }

  private async ensureManifestEntry(
    collection: string,
    snapshotPath: string
  ): Promise<void> {
    const manifest = await this.getManifest();
    const entry = manifest.collections[collection];
    if (entry) {
      return;
    }

    manifest.collections[collection] = {
      checkpoint: 0,
      snapshotPath,
      schema: {},
      indexes: {}
    };

    await this.persistManifest(manifest);
  }

  private async updateManifest(
    collection: string,
    updates: Partial<
      Pick<ManifestCollectionEntry, 'checkpoint' | 'snapshotPath' | 'schema'>
    > & {
      indexes?: Record<string, IndexMetadata | undefined>;
    }
  ): Promise<void> {
    const manifest = await this.getManifest();
    const existing = manifest.collections[collection] ?? {
      checkpoint: 0,
      snapshotPath:
        updates.snapshotPath ??
        path.join(this.options.dataDir, `${collection}.snapshot.json`),
      schema: {},
      indexes: {}
    };

    manifest.collections[collection] = {
      ...existing,
      ...updates,
      indexes:
        updates.indexes !== undefined
          ? { ...existing.indexes, ...updates.indexes }
          : existing.indexes,
      updatedAt: new Date().toISOString()
    };

    await this.persistManifest(manifest);
  }

  private getIndexPath(collection: string, field: string): string {
    return path.join(this.options.indexDir, collection, `${field}.json`);
  }

  private async persistIndex(
    collection: string,
    field: string,
    meta: IndexMetadata,
    entries: Record<string, DocumentId[]>
  ): Promise<IndexMetadata> {
    await ensureIndexDirectory(meta.path);
    await fsp.writeFile(
      meta.path,
      JSON.stringify({ meta, entries }, null, 2),
      'utf8'
    );

    const sizeBytes = await getFileSize(meta.path);
    const finalMeta: IndexMetadata = {
      ...meta,
      stats: meta.stats
        ? { ...meta.stats, sizeBytes }
        : { docCount: 0, tokenCount: 0, sizeBytes }
    };

    await this.updateManifest(collection, {
      indexes: { [field]: finalMeta }
    });

    return finalMeta;
  }

  private async buildIndex(
    collection: string,
    field: string,
    metadata: IndexMetadata,
    state: CollectionState
  ): Promise<void> {
    const now = new Date().toISOString();
    const baseMeta: IndexMetadata = { ...metadata, updatedAt: now };
    const logCheckpoint = await getFileSize(state.logPath);

    try {
      const { entries, tokenCount } = buildIndexEntries(
        state.docs,
        field,
        metadata.options
      );
      const readyMeta: IndexMetadata = {
        ...baseMeta,
        state: 'ready',
        checkpoint: logCheckpoint,
        lastError: undefined,
        stats: {
          docCount: state.docs.size,
          tokenCount,
          builtAt: now
        }
      };
      await this.persistIndex(collection, field, { ...readyMeta }, entries);
    } catch (error) {
      const failedMeta: IndexMetadata = {
        ...baseMeta,
        state: 'error',
        lastError: error instanceof Error ? error.message : String(error)
      };

      await this.updateManifest(collection, {
        indexes: { [field]: failedMeta }
      });

      throw error;
    }
  }

  private async enforceUniqueConstraints(
    collection: string,
    doc: Document,
    state: CollectionState,
    existingId: DocumentId | null
  ): Promise<void> {
    const manifest = await this.getManifest();
    const indexEntries = manifest.collections[collection]?.indexes ?? {};
    const logSize = await getFileSize(state.logPath);

    for (const [field, metadata] of Object.entries(indexEntries)) {
      if (!metadata?.options.unique || metadata.state !== 'ready') {
        continue;
      }

      if (metadata.checkpoint < logSize) {
        this.options.log?.warn?.('Skipping unique check for stale index', {
          collection,
          field,
          checkpoint: metadata.checkpoint,
          logSize
        });
        continue;
      }

      const value = (doc as Record<string, unknown>)[field];
      const tokens = collectTokens(value, metadata.options);
      let enforcedThroughIndex = false;

      if (tokens.length > 0) {
        const indexFile = await loadIndexFile(metadata.path);
        if (indexFile) {
          enforcedThroughIndex = true;
          const candidateIds = intersectPostingLists(indexFile.entries, tokens);
          for (const candidateId of candidateIds) {
            if (existingId && candidateId === existingId) {
              continue;
            }

            const candidateDoc = state.docs.get(candidateId);
            if (!candidateDoc) {
              continue;
            }

            const candidateValue = (candidateDoc as Record<string, unknown>)[field];
            if (isValueEqual(candidateValue, value)) {
              throw new Error(
                `Unique constraint violated for field '${field}' in collection '${collection}'`
              );
            }
          }
        }
      }

      if (!enforcedThroughIndex) {
        for (const [candidateId, candidateDoc] of state.docs.entries()) {
          if (existingId && candidateId === existingId) {
            continue;
          }

          const candidateValue = (candidateDoc as Record<string, unknown>)[field];
          if (isValueEqual(candidateValue, value)) {
            throw new Error(
              `Unique constraint violated for field '${field}' in collection '${collection}'`
            );
          }
        }
      }
    }
  }

  private async markIndexStale(
    collection: string,
    field: string,
    metadata: IndexMetadata,
    lastError?: string
  ): Promise<void> {
    const stale: IndexMetadata = {
      ...metadata,
      state: 'stale',
      lastError: lastError ?? metadata.lastError,
      updatedAt: new Date().toISOString()
    };

    await this.updateManifest(collection, {
      indexes: { [field]: stale }
    });
  }

  private async refreshIndexesForWrite(
    collection: string,
    state: CollectionState,
    newDoc: Document | null,
    previousDoc: Document | null
  ): Promise<void> {
    const manifest = await this.getManifest();
    const indexEntries = manifest.collections[collection]?.indexes ?? {};
    if (Object.keys(indexEntries).length === 0) {
      return;
    }

    const logCheckpoint = await getFileSize(state.logPath);
    for (const [field, metadata] of Object.entries(indexEntries)) {
      if (!metadata || metadata.state !== 'ready') {
        continue;
      }

      const indexFile = await loadIndexFile(metadata.path);
      if (!indexFile) {
        await this.markIndexStale(collection, field, metadata, 'index file missing');
        continue;
      }

      try {
        const entries = { ...indexFile.entries };
        const prevTokens = previousDoc
          ? collectTokens(
              (previousDoc as Record<string, unknown>)[field],
              metadata.options
            )
          : [];
        const nextTokens = newDoc
          ? collectTokens((newDoc as Record<string, unknown>)[field], metadata.options)
          : [];

        const prevId = (previousDoc?._id ?? '') as DocumentId;
        const nextId = (newDoc?._id ?? '') as DocumentId;

        prevTokens.forEach((token) => removePostingId(entries, token, prevId));
        nextTokens.forEach((token) => addPostingId(entries, token, nextId));

        const stats: IndexStats = {
          docCount: state.docs.size,
          tokenCount: calculateTokenCount(entries),
          builtAt: indexFile.meta.stats?.builtAt
        };

        const updatedMeta: IndexMetadata = {
          ...metadata,
          state: 'ready',
          lastError: undefined,
          checkpoint: logCheckpoint,
          updatedAt: new Date().toISOString(),
          stats: { ...stats }
        };

        await this.persistIndex(collection, field, updatedMeta, entries);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        this.options.log?.error?.('Failed to refresh index after write', {
          collection,
          field,
          error: message
        });
        await this.markIndexStale(collection, field, metadata, message);
      }
    }
  }

  private async getIndexedCandidates(
    collection: string,
    filter: Filter,
    state: CollectionState
  ): Promise<Set<DocumentId> | null> {
    const manifest = await this.getManifest();
    const indexEntries = manifest.collections[collection]?.indexes ?? {};
    const logSize = await getFileSize(state.logPath);
    let candidates: Set<DocumentId> | null = null;
    const usedFields: string[] = [];

    for (const [field, expected] of Object.entries(filter)) {
      if (expected === undefined) {
        continue;
      }

      const metadata = indexEntries[field];
      if (!metadata || metadata.state !== 'ready' || metadata.checkpoint < logSize) {
        continue;
      }

      const indexFile = await loadIndexFile(metadata.path);
      if (!indexFile) {
        continue;
      }

      const expectedValues = Array.isArray(expected) ? expected : [expected];
      const perValueCandidates: Set<DocumentId>[] = [];

      for (const value of expectedValues) {
        if (typeof value !== 'string' && !Array.isArray(value)) {
          continue;
        }

        const tokens = collectTokens(value, metadata.options);
        if (tokens.length === 0) {
          continue;
        }

        perValueCandidates.push(intersectPostingLists(indexFile.entries, tokens));
      }

      if (perValueCandidates.length === 0) {
        continue;
      }

      let fieldCandidates = perValueCandidates[0];
      for (let idx = 1; idx < perValueCandidates.length; idx += 1) {
        fieldCandidates = unionSets(fieldCandidates, perValueCandidates[idx]);
      }

      usedFields.push(field);
      candidates = candidates ? intersectSets(candidates, fieldCandidates) : fieldCandidates;

      if (candidates.size === 0) {
        break;
      }
    }

    if (usedFields.length > 0) {
      this.options.log?.debug?.('Using index for query', {
        collection,
        fields: usedFields,
        candidateCount: candidates?.size ?? 0
      });
    }

    return usedFields.length > 0 ? candidates ?? new Set<DocumentId>() : null;
  }
}

function resolveOptions(options: DatabaseOptions): ResolvedOptions {
  const dataDir = options.dataDir ?? path.resolve(process.cwd(), 'data');
  const binaryDir =
    options.binaryDir ?? path.resolve(process.cwd(), 'binaries');
  const indexDir = options.indexDir ?? path.join(dataDir, 'indexes');
  const tokenizer = resolveTokenizer(options.tokenizer);
  return {
    ...options,
    dataDir,
    binaryDir,
    indexDir,
    tokenizer,
    log: options.log ?? noopLogger,
    autoCompact: options.autoCompact ?? true,
    snapshotInterval: options.snapshotInterval ?? 100,
    logRetention: options.logRetention ?? 'truncate'
  };
}

function resolveTokenizer(
  overrides: Partial<TokenizerOptions> | undefined
): TokenizerOptions {
  return {
    lowerCase: overrides?.lowerCase ?? true,
    minTokenLength: overrides?.minTokenLength ?? 2,
    splitRegex: overrides?.splitRegex ?? '[^a-zA-Z0-9]+',
    stopwords: overrides?.stopwords ?? []
  };
}

function normalizeManifest(manifest: Manifest): Manifest {
  const manifestVersion =
    typeof manifest?.manifestVersion === 'number' ? manifest.manifestVersion : 1;
  const collections: Record<string, ManifestCollectionEntry | undefined> = {};

  if (manifest && typeof manifest.collections === 'object') {
    Object.entries(manifest.collections).forEach(([name, entry]) => {
      if (!entry) {
        collections[name] = undefined;
        return;
      }

      const indexes =
        entry.indexes && typeof entry.indexes === 'object' ? entry.indexes : {};

      collections[name] = {
        checkpoint:
          typeof entry.checkpoint === 'number' ? entry.checkpoint : 0,
        snapshotPath:
          typeof entry.snapshotPath === 'string' ? entry.snapshotPath : '',
        schema:
          entry.schema && typeof entry.schema === 'object'
            ? entry.schema
            : undefined,
        indexes,
        updatedAt: entry.updatedAt
      };
    });
  }

  return { manifestVersion, collections };
}

function ensureDirectories(options: ResolvedOptions): void {
  fs.mkdirSync(options.dataDir, { recursive: true });
  fs.mkdirSync(options.binaryDir, { recursive: true });
  fs.mkdirSync(options.indexDir, { recursive: true });
}

function normalizeIndexOptions(
  options: IndexOptions,
  tokenizer: TokenizerOptions
): NormalizedIndexOptions {
  return {
    unique: options.unique ?? false,
    prefixLength: options.prefixLength,
    tokenizer: { ...tokenizer }
  };
}

function indexOptionsEqual(
  left: NormalizedIndexOptions,
  right: NormalizedIndexOptions
): boolean {
  return (
    left.unique === right.unique &&
    left.prefixLength === right.prefixLength &&
    tokenizerEqual(left.tokenizer, right.tokenizer)
  );
}

function tokenizerEqual(
  left: TokenizerOptions,
  right: TokenizerOptions
): boolean {
  return (
    left.lowerCase === right.lowerCase &&
    left.minTokenLength === right.minTokenLength &&
    left.splitRegex === right.splitRegex &&
    arrayShallowEqual(left.stopwords, right.stopwords)
  );
}

function arrayShallowEqual(left: string[], right: string[]): boolean {
  if (left.length !== right.length) {
    return false;
  }

  return left.every((value, idx) => value === right[idx]);
}

async function ensureIndexDirectory(indexPath: string): Promise<void> {
  await fsp.mkdir(path.dirname(indexPath), { recursive: true });
}

async function writeIndexStub(
  indexPath: string,
  metadata: IndexMetadata
): Promise<void> {
  const payload = {
    meta: {
      field: metadata.field,
      state: metadata.state,
      version: metadata.version,
      checkpoint: metadata.checkpoint,
      options: metadata.options,
      createdAt: metadata.createdAt,
      updatedAt: metadata.updatedAt,
      stats: metadata.stats
    },
    entries: []
  };

  await ensureIndexDirectory(indexPath);
  await fsp.writeFile(indexPath, JSON.stringify(payload, null, 2), 'utf8');
}

async function loadIndexFile(indexPath: string): Promise<IndexFilePayload | null> {
  try {
    const raw = await fsp.readFile(indexPath, 'utf8');
    const parsed = JSON.parse(raw) as IndexFilePayload;
    if (!parsed || typeof parsed !== 'object') {
      return null;
    }
    return parsed;
  } catch (error) {
    if (isEnoentError(error)) {
      return null;
    }
    throw error;
  }
}

function intersectPostingLists(
  entries: Record<string, DocumentId[]>,
  tokens: string[]
): Set<DocumentId> {
  let result: Set<DocumentId> | null = null;

  for (const token of tokens) {
    const ids = entries[token];
    if (!ids || ids.length === 0) {
      return new Set();
    }

    const idSet = new Set(ids);
    result = result ? intersectSets(result, idSet) : idSet;

    if (result.size === 0) {
      return result;
    }
  }

  return result ?? new Set();
}

function unionSets<T>(left: Set<T>, right: Set<T>): Set<T> {
  const union = new Set<T>(left);
  right.forEach((value) => union.add(value));
  return union;
}

function intersectSets<T>(left: Set<T>, right: Set<T>): Set<T> {
  const intersect = new Set<T>();
  left.forEach((value) => {
    if (right.has(value)) {
      intersect.add(value);
    }
  });
  return intersect;
}

function addPostingId(entries: Record<string, DocumentId[]>, token: string, id: DocumentId): void {
  const current = entries[token];
  if (!current) {
    entries[token] = [id];
    return;
  }

  if (current.includes(id)) {
    return;
  }

  current.push(id);
  current.sort();
}

function removePostingId(
  entries: Record<string, DocumentId[]>,
  token: string,
  id: DocumentId
): void {
  const current = entries[token];
  if (!current) {
    return;
  }

  const idx = current.indexOf(id);
  if (idx === -1) {
    return;
  }

  current.splice(idx, 1);
  if (current.length === 0) {
    delete entries[token];
  }
}

function calculateTokenCount(entries: Record<string, DocumentId[]>): number {
  return Object.values(entries).reduce((sum, ids) => sum + ids.length, 0);
}

function isValueEqual(left: unknown, right: unknown): boolean {
  if (left === undefined || right === undefined) {
    return left === right;
  }
  return JSON.stringify(left) === JSON.stringify(right);
}

function buildIndexEntries(
  docs: Map<DocumentId, Document>,
  field: string,
  options: NormalizedIndexOptions
): { entries: Record<string, DocumentId[]>; tokenCount: number } {
  const postings = new Map<string, Set<DocumentId>>();
  let tokenCount = 0;

  for (const doc of docs.values()) {
    const id = doc._id;
    if (typeof id !== 'string') {
      continue;
    }

    const value = (doc as Record<string, unknown>)[field];
    const tokens = collectTokens(value, options);

    for (const token of tokens) {
      const entry = postings.get(token) ?? new Set<DocumentId>();
      const before = entry.size;
      entry.add(id);
      postings.set(token, entry);

      if (entry.size > before) {
        tokenCount += 1;
      }
    }
  }

  const entries: Record<string, DocumentId[]> = {};
  Array.from(postings.keys())
    .sort()
    .forEach((token) => {
      const ids = postings.get(token);
      if (!ids) {
        return;
      }
      entries[token] = Array.from(ids).sort();
    });

  return { entries, tokenCount };
}

function collectTokens(
  value: unknown,
  options: NormalizedIndexOptions
): string[] {
  const values = Array.isArray(value) ? value : [value];
  const tokens: string[] = [];

  for (const candidate of values) {
    if (typeof candidate !== 'string') {
      continue;
    }

    const baseTokens = tokenizeString(candidate, options.tokenizer);
    const prefixLength = options.prefixLength ?? 0;

    for (const token of baseTokens) {
      tokens.push(token);

      if (prefixLength > 0 && token.length >= prefixLength) {
        tokens.push(token.slice(0, prefixLength));
      }
    }
  }

  return tokens;
}

function tokenizeString(
  value: string,
  tokenizer: TokenizerOptions
): string[] {
  const normalized = tokenizer.lowerCase ? value.toLowerCase() : value;
  const splitRegex = new RegExp(tokenizer.splitRegex, 'g');
  const stopwords = tokenizer.lowerCase
    ? tokenizer.stopwords.map((word) => word.toLowerCase())
    : tokenizer.stopwords;

  return normalized
    .split(splitRegex)
    .map((token) => token.trim())
    .filter(
      (token) =>
        token.length >= tokenizer.minTokenLength && !stopwords.includes(token)
    );
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

async function readFileFromPosition(filePath: string, offset: number): Promise<string> {
  return new Promise((resolve, reject) => {
    let data = '';
    const stream = fs.createReadStream(filePath, {
      encoding: 'utf8',
      start: offset
    });

    stream.on('data', (chunk) => {
      data += chunk;
    });

    stream.on('error', (error) => {
      if (isEnoentError(error)) {
        resolve('');
        return;
      }
      reject(error);
    });

    stream.on('end', () => {
      resolve(data);
    });
  });
}

async function getFileSize(filePath: string): Promise<number> {
  try {
    const stats = await fsp.stat(filePath);
    return stats.size;
  } catch (error) {
    if (isEnoentError(error)) {
      return 0;
    }
    throw error;
  }
}

async function truncateFile(filePath: string): Promise<void> {
  try {
    await fsp.truncate(filePath, 0);
  } catch (error) {
    if (isEnoentError(error)) {
      return;
    }
    throw error;
  }
}

async function rotateLogFile(filePath: string): Promise<string> {
  const dir = path.dirname(filePath);
  const base = path.basename(filePath);
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  let candidate = path.join(dir, `${base}.${timestamp}.bak`);
  let counter = 0;

  // Ensure unique rotated filename if called rapidly.
  while (await fileExists(candidate)) {
    counter += 1;
    candidate = path.join(dir, `${base}.${timestamp}-${counter}.bak`);
  }

  try {
    await fsp.rename(filePath, candidate);
    return candidate;
  } catch (error) {
    if (isEnoentError(error)) {
      return candidate;
    }
    throw error;
  }
}

async function fileExists(filePath: string): Promise<boolean> {
  try {
    await fsp.access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function readLog(
  filePath: string,
  offset = 0
): Promise<Array<{ _id?: DocumentId; data?: Document; tombstone?: boolean }>> {
  try {
    const size = await getFileSize(filePath);
    if (offset >= size) {
      return [];
    }

    const raw = await readFileFromPosition(filePath, offset);
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
