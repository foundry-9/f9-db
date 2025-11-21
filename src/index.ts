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
  JoinRelation,
  JoinRelations,
  Logger,
  NormalizedIndexOptions,
  TokenizerOptions,
  CollectionSchema,
  FieldSchema,
  BinaryMetadata,
  BinaryWriteOptions,
  BinaryDeleteOptions,
  BinaryReference
} from './types.js';

interface ResolvedOptions extends DatabaseOptions {
  dataDir: string;
  binaryDir: string;
  logDir: string;
  log: Logger;
  logRetention: 'truncate' | 'rotate' | 'keep';
  fsync: 'always' | 'batch' | 'never';
  lockMode: 'lockfile' | 'flock' | 'none';
  lockRetryMs: number;
  lockTimeoutMs: number;
  indexDir: string;
  tokenizer: TokenizerOptions;
  schemas: Record<string, CollectionSchema>;
  joinCacheMaxEntries: number;
  joinCacheTTLms?: number;
  dedupeBinaries: boolean;
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
  schema?: CollectionSchema;
  indexes: Record<string, IndexMetadata | undefined>;
  updatedAt?: string;
}

interface Manifest {
  manifestVersion: number;
  collections: Record<string, ManifestCollectionEntry | undefined>;
  binaries?: Record<string, BinaryManifestEntry | undefined>;
}

interface BinaryManifestEntry {
  size: number;
  mimeType?: string;
  createdAt: string;
  updatedAt?: string;
  refCount: number;
}

interface IndexFilePayload {
  meta: IndexMetadata;
  entries: Record<string, DocumentId[]>;
}

interface JoinCacheEntry {
  value: Document | null;
  expiresAt: number;
}

interface LockConfig {
  path: string;
  retryMs: number;
  timeoutMs: number;
}

class FileLock {
  private handle: fs.promises.FileHandle | null = null;
  private readonly path: string;
  private readonly retryMs: number;
  private readonly timeoutMs: number;

  constructor(config: LockConfig) {
    this.path = config.path;
    this.retryMs = config.retryMs;
    this.timeoutMs = config.timeoutMs;
  }

  async acquire(): Promise<void> {
    const start = Date.now();

    // Cheap retry/backoff loop that prefers fast acquisition.
    while (true) {
      try {
        this.handle = await fsp.open(this.path, 'wx');
        return;
      } catch (error) {
        if (!isEexistError(error)) {
          throw error;
        }
      }

      if (Date.now() - start >= this.timeoutMs) {
        throw new Error(`Timed out acquiring lock at ${this.path}`);
      }

      await delay(this.retryMs);
    }
  }

  async release(): Promise<void> {
    const handle = this.handle;
    this.handle = null;

    if (handle) {
      try {
        await handle.close();
      } catch (error) {
        if (!isEnoentError(error)) {
          throw error;
        }
      }
    }

    try {
      await fsp.unlink(this.path);
    } catch (error) {
      if (error && !isEnoentError(error)) {
        throw error;
      }
    }
  }
}

function createFileLogger(logDir: string): Logger {
  const logPath = path.join(logDir, 'app.log');
  fs.mkdirSync(path.dirname(logPath), { recursive: true });
  const stream = fs.createWriteStream(logPath, { flags: 'a' });

  const write = (level: string, msg: string, context?: Record<string, unknown>): void => {
    const payload: Record<string, unknown> = {
      level,
      time: new Date().toISOString(),
      msg
    };

    if (context && Object.keys(context).length > 0) {
      payload.context = context;
    }

    stream.write(`${JSON.stringify(payload)}\n`);
  };

  return {
    debug: (msg, context) => write('debug', msg, context),
    info: (msg, context) => write('info', msg, context),
    warn: (msg, context) => write('warn', msg, context),
    error: (msg, context) => write('error', msg, context)
  };
}

class JsonFileDatabase implements Database {
  private options: ResolvedOptions;
  private collections = new Map<string, CollectionState>();
  private manifest: Manifest | null = null;
  private manifestPath: string;
  private joinCache = new Map<
    string,
    Map<string, Map<string, JoinCacheEntry>>
  >();
  private flockWarningEmitted = false;

  constructor(options: DatabaseOptions = {}) {
    this.options = resolveOptions(options);
    this.manifestPath = path.join(this.options.dataDir, 'manifest.json');
    ensureDirectories(this.options);
  }

  async insert(collection: string, doc: Document): Promise<Document> {
    return this.withCollectionLock(collection, async () => {
      const state = await this.loadCollection(collection);
      const _id = (doc._id as DocumentId | undefined) ?? crypto.randomUUID();

      if (state.docs.has(_id)) {
        throw new Error(`Duplicate _id '${_id}' in collection '${collection}'`);
      }

      const stored: Document = { ...doc, _id };
      const normalized = await this.normalizeAndValidateDocument(
        collection,
        stored,
        true
      );
      await this.enforceUniqueConstraints(collection, normalized, state, null);
      state.docs.set(_id, normalized);
      await this.appendLog(collection, state, { _id, data: normalized });
      await this.refreshIndexesForWrite(collection, state, normalized, null);
      await this.applyBinaryRefChanges(normalized, null);
      this.resetJoinCache();
      return cloneDocument(normalized);
    });
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
    return this.withCollectionLock(collection, async () => {
      const state = await this.loadCollection(collection);
      const existing = state.docs.get(id);

      if (!existing) {
        throw new Error(`Document '${id}' not found in collection '${collection}'`);
      }

      const updated: Document = { ...existing, ...mutation, _id: id };
      const normalized = await this.normalizeAndValidateDocument(
        collection,
        updated,
        false
      );
      await this.enforceUniqueConstraints(collection, normalized, state, id);
      state.docs.set(id, normalized);
      await this.appendLog(collection, state, { _id: id, data: normalized });
      await this.refreshIndexesForWrite(collection, state, normalized, existing);
      await this.applyBinaryRefChanges(normalized, existing);
      this.resetJoinCache();
      return cloneDocument(normalized);
    });
  }

  async remove(collection: string, id: DocumentId): Promise<void> {
    await this.withCollectionLock(collection, async () => {
      const state = await this.loadCollection(collection);

      if (!state.docs.has(id)) {
        throw new Error(`Document '${id}' not found in collection '${collection}'`);
      }

      const existing = state.docs.get(id) ?? null;
      state.docs.delete(id);
      await this.appendLog(collection, state, { _id: id, tombstone: true });
      await this.refreshIndexesForWrite(collection, state, null, existing);
      await this.applyBinaryRefChanges(null, existing);
      this.resetJoinCache();
    });
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
    await this.withCollectionLock(collection, async () => {
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

      await writeIndexStub(indexPath, metadata, this.options.fsync);
      await this.buildIndex(collection, field, metadata, state);
      this.options.log?.info?.('Index metadata recorded', {
        collection,
        field,
        state: metadata.state,
        version: metadata.version
      });
    });
  }

  async rebuildIndex(collection: string, field: string): Promise<void> {
    await this.withCollectionLock(collection, async () => {
      const state = await this.loadCollection(collection);
      const manifest = await this.getManifest();
      const existing = manifest.collections[collection]?.indexes?.[field];

      if (!existing) {
        const normalized = normalizeIndexOptions({}, this.options.tokenizer);
        const indexPath = this.getIndexPath(collection, field);
        const now = new Date().toISOString();
        const metadata: IndexMetadata = {
          field,
          path: indexPath,
          options: normalized,
          version: 1,
          state: 'pending',
          checkpoint: 0,
          createdAt: now,
          updatedAt: now
        };

        await this.updateManifest(collection, {
          indexes: { [field]: metadata }
        });

        await writeIndexStub(indexPath, metadata, this.options.fsync);
        await this.buildIndex(collection, field, metadata, state);
        this.options.log?.info?.('Index metadata recorded', {
          collection,
          field,
          state: metadata.state,
          version: metadata.version
        });
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

      await writeIndexStub(this.getIndexPath(collection, field), metadata, this.options.fsync);
      await this.buildIndex(collection, field, metadata, state);
      this.options.log?.info?.('Index rebuild scheduled', {
        collection,
        field,
        version: metadata.version
      });
    });
  }

  async join(
    collection: string,
    doc: Document,
    relations: JoinRelations
  ): Promise<Document> {
    if (!relations || Object.keys(relations).length === 0) {
      return cloneDocument(doc);
    }

    const base = cloneDocument(doc);
    const normalized = normalizeJoinRelations(base, relations);
    if (normalized.length === 0) {
      return base;
    }

    const fetchPlan = new Map<
      string,
      { collection: string; field: string; values: Set<DocumentId> }
    >();

    normalized.forEach((entry) => {
      if (entry.values.length === 0) {
        return;
      }

      const key = serializeJoinKey(entry.relation.foreignCollection, entry.relation.foreignField);
      const existing =
        fetchPlan.get(key) ??
        {
          collection: entry.relation.foreignCollection,
          field: entry.relation.foreignField ?? '_id',
          values: new Set<DocumentId>()
        };
      entry.values.forEach((value) => existing.values.add(value));
      fetchPlan.set(key, existing);
    });

    const fetched = new Map<string, Map<DocumentId, Document | null>>();
    for (const plan of fetchPlan.values()) {
      const key = serializeJoinKey(plan.collection, plan.field);
      fetched.set(
        key,
        await this.fetchJoinTargets(plan.collection, plan.field, plan.values)
      );
    }

    for (const entry of normalized) {
      const key = serializeJoinKey(entry.relation.foreignCollection, entry.relation.foreignField);
      const lookup = fetched.get(key) ?? new Map<DocumentId, Document | null>();
      const projected = resolveProjection(entry, lookup);
      setValueAtPath(base as Record<string, unknown>, entry.targetPath, projected);
    }

    return base;
  }

  async compact(collection: string): Promise<void> {
    await this.withCollectionLock(collection, async () => {
      const state = await this.loadCollection(collection);
      await this.compactCollection(collection, state);
    });
  }

  clearJoinCache(): void {
    this.resetJoinCache();
  }

  private async withCollectionLock<T>(
    collection: string,
    action: () => Promise<T>
  ): Promise<T> {
    if (this.options.lockMode === 'none') {
      return action();
    }

    if (this.options.lockMode === 'flock' && !this.flockWarningEmitted) {
      this.options.log?.warn?.('flock lockMode requested; falling back to lockfile', {
        collection
      });
      this.flockWarningEmitted = true;
    }

    const lock = new FileLock({
      path: this.getCollectionLockPath(collection),
      retryMs: this.options.lockRetryMs,
      timeoutMs: this.options.lockTimeoutMs
    });

    await lock.acquire();
    try {
      return await action();
    } finally {
      await lock.release();
    }
  }

  private getCollectionLockPath(collection: string): string {
    return path.join(this.options.dataDir, `${collection}.lock`);
  }

  async saveBinary(
    data: Buffer | ArrayBuffer | Uint8Array | string,
    options: BinaryWriteOptions = {}
  ): Promise<BinaryMetadata> {
    const buffer = normalizeBinaryInput(data);
    const sha256 = hashBuffer(buffer);
    const filePath = path.join(this.options.binaryDir, sha256);
    const dedupe = options.dedupe ?? this.options.dedupeBinaries;
    const exists = await fileExists(filePath);
    const shouldWrite = !dedupe || !exists;

    if (shouldWrite) {
      await writeFileWithSync(filePath, buffer, this.options.fsync === 'never' ? 'never' : 'batch');
    }

    const now = new Date().toISOString();
    const manifest = await this.getManifest();
    const binaries = manifest.binaries ?? {};
    const existing = binaries[sha256];
    const entry: BinaryManifestEntry = {
      size: buffer.length,
      mimeType: options.mimeType ?? existing?.mimeType,
      createdAt: existing?.createdAt ?? now,
      updatedAt: now,
      refCount: existing?.refCount ?? 0
    };
    binaries[sha256] = entry;
    manifest.binaries = binaries;
    await this.persistManifest(manifest);

    return {
      sha256,
      size: buffer.length,
      mimeType: entry.mimeType,
      path: filePath,
      createdAt: entry.createdAt,
      updatedAt: entry.updatedAt,
      refCount: entry.refCount,
      deduped: !shouldWrite && exists
    };
  }

  async readBinary(sha256: string): Promise<Buffer | null> {
    const filePath = path.join(this.options.binaryDir, sha256);
    try {
      return await fsp.readFile(filePath);
    } catch (error) {
      if (isEnoentError(error)) {
        return null;
      }
      throw error;
    }
  }

  async deleteBinary(
    sha256: string,
    options: BinaryDeleteOptions = {}
  ): Promise<boolean> {
    const manifest = await this.getManifest();
    const binaries = manifest.binaries ?? {};
    const existing = binaries[sha256];

    if (existing && existing.refCount > 0 && !options.force) {
      throw new Error(
        `Cannot delete binary '${sha256}' while refCount=${existing.refCount}`
      );
    }

    const filePath = path.join(this.options.binaryDir, sha256);
    try {
      await fsp.unlink(filePath);
    } catch (error) {
      if (!isEnoentError(error)) {
        throw error;
      }
    }

    if (existing) {
      delete binaries[sha256];
      manifest.binaries = binaries;
      await this.persistManifest(manifest);
      return true;
    }

    return false;
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
    await appendFileWithSync(
      state.logPath,
      `${JSON.stringify(entry)}\n`,
      this.options.fsync
    );
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
    for (const doc of docs) {
      await this.normalizeAndValidateDocument(collection, doc, false);
    }
    const snapshotPayload = JSON.stringify({ docs }, null, 2);
    await writeFileWithSync(
      state.snapshotPath,
      snapshotPayload,
      this.options.fsync === 'never' ? 'never' : 'batch'
    );

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
      await writeFileWithSync(
        state.logPath,
        '',
        this.options.fsync === 'never' ? 'never' : 'batch'
      );
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
        this.manifest = { manifestVersion: 1, collections: {}, binaries: {} };
      } else {
        throw error;
      }
    }

    return this.manifest;
  }

  private async persistManifest(manifest: Manifest): Promise<void> {
    this.manifest = manifest;
    await writeFileWithSync(
      this.manifestPath,
      JSON.stringify(manifest, null, 2),
      this.options.fsync === 'never' ? 'never' : 'batch'
    );
  }

  private async ensureManifestEntry(
    collection: string,
    snapshotPath: string
  ): Promise<void> {
    const manifest = await this.getManifest();
    const entry = manifest.collections[collection];
    const providedSchema = this.options.schemas[collection];
    if (entry) {
      if (!entry.schema && providedSchema) {
        manifest.collections[collection] = {
          ...entry,
          schema: cloneDocument(providedSchema)
        };
        await this.persistManifest(manifest);
      }
      return;
    }

    manifest.collections[collection] = {
      checkpoint: 0,
      snapshotPath,
      schema: providedSchema ? cloneDocument(providedSchema) : undefined,
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
      schema: this.options.schemas[collection]
        ? cloneDocument(this.options.schemas[collection])
        : undefined,
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

  private async getCollectionSchema(collection: string): Promise<CollectionSchema | null> {
    const manifest = await this.getManifest();
    return manifest.collections[collection]?.schema ?? null;
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
    await writeFileWithSync(
      meta.path,
      JSON.stringify({ meta, entries }, null, 2),
      this.options.fsync === 'never' ? 'never' : 'batch'
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

  private async normalizeAndValidateDocument(
    collection: string,
    doc: Document,
    applyDefaults: boolean
  ): Promise<Document> {
    const schema = await this.getCollectionSchema(collection);
    if (!schema) {
      return cloneDocument(doc);
    }

    const cloned = cloneDocument(doc);
    const withDefaults = applyDefaults
      ? applyDefaultsToDocument(cloned, schema)
      : cloned;

    validateDocumentAgainstSchema(withDefaults, schema);
    return withDefaults;
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

  private async applyBinaryRefChanges(
    newDoc: Document | null,
    previousDoc: Document | null
  ): Promise<void> {
    const nextRefs = countBinaryRefs(extractBinaryRefs(newDoc));
    const prevRefs = countBinaryRefs(extractBinaryRefs(previousDoc));
    const shas = new Set([...Object.keys(nextRefs), ...Object.keys(prevRefs)]);
    if (shas.size === 0) {
      return;
    }

    const manifest = await this.getManifest();
    const binaries = { ...(manifest.binaries ?? {}) };
    const now = new Date().toISOString();
    let changed = false;

    shas.forEach((sha) => {
      const next = nextRefs[sha]?.count ?? 0;
      const prev = prevRefs[sha]?.count ?? 0;
      const delta = next - prev;
      if (delta === 0) {
        return;
      }

      const existing = binaries[sha];
      if (delta < 0 && !existing) {
        return;
      }

      const size = nextRefs[sha]?.size ?? existing?.size ?? 0;
      const mimeType = nextRefs[sha]?.mimeType ?? existing?.mimeType;
      const createdAt = existing?.createdAt ?? now;
      const refCount = Math.max(0, (existing?.refCount ?? 0) + delta);

      binaries[sha] = {
        size,
        mimeType,
        createdAt,
        updatedAt: now,
        refCount
      };
      changed = true;
    });

    if (changed) {
      manifest.binaries = binaries;
      await this.persistManifest(manifest);
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

  private getJoinCacheBucket(
    collection: string,
    field: string
  ): Map<DocumentId, JoinCacheEntry> {
    let fields = this.joinCache.get(collection);
    if (!fields) {
      fields = new Map();
      this.joinCache.set(collection, fields);
    }

    let bucket = fields.get(field);
    if (!bucket) {
      bucket = new Map();
      fields.set(field, bucket);
    }

    return bucket;
  }

  private resetJoinCache(): void {
    this.joinCache.clear();
  }

  private touchJoinCache(
    bucket: Map<DocumentId, JoinCacheEntry>,
    id: DocumentId,
    value: Document | null
  ): void {
    bucket.delete(id);
    const ttl = this.options.joinCacheTTLms;
    const expiresAt =
      typeof ttl === 'number' && ttl > 0 ? Date.now() + ttl : Number.POSITIVE_INFINITY;
    bucket.set(id, { value, expiresAt });
    this.trimJoinCacheBucket(bucket, this.options.joinCacheMaxEntries);
  }

  private trimJoinCacheBucket(
    bucket: Map<DocumentId, JoinCacheEntry>,
    maxEntries: number
  ): void {
    if (maxEntries <= 0) {
      bucket.clear();
      return;
    }

    while (bucket.size > maxEntries) {
      const oldest = bucket.keys().next().value;
      if (oldest === undefined) {
        break;
      }
      bucket.delete(oldest as DocumentId);
    }
  }

  private async fetchJoinTargets(
    collection: string,
    field: string,
    values: Set<DocumentId>
  ): Promise<Map<DocumentId, Document | null>> {
    const cache = this.getJoinCacheBucket(collection, field);
    const results = new Map<DocumentId, Document | null>();
    const missing = new Set<DocumentId>();

    values.forEach((value) => {
      const cached = cache.get(value);
      if (cached && !isJoinCacheEntryExpired(cached)) {
        this.touchJoinCache(cache, value, cached.value);
        results.set(value, cached.value ? cloneDocument(cached.value) : null);
      } else {
        if (cached && isJoinCacheEntryExpired(cached)) {
          cache.delete(value);
        }
        missing.add(value);
      }
    });

    if (missing.size === 0) {
      return results;
    }

    const state = await this.loadCollection(collection);

    if (field === '_id') {
      missing.forEach((value) => {
        const doc = state.docs.get(value) ?? null;
        const cachedDoc = doc ? cloneDocument(doc) : null;
        this.touchJoinCache(cache, value, cachedDoc);
        results.set(value, cachedDoc ? cloneDocument(cachedDoc) : null);
      });
      return results;
    }

    const candidates = new Map<string, Document>();
    for (const document of state.docs.values()) {
      const candidateValue = (document as Record<string, unknown>)[field];
      if (typeof candidateValue !== 'string' || !missing.has(candidateValue)) {
        continue;
      }
      candidates.set(candidateValue, document);
    }

    missing.forEach((value) => {
      const doc = candidates.get(value) ?? null;
      const cachedDoc = doc ? cloneDocument(doc) : null;
      this.touchJoinCache(cache, value, cachedDoc);
      results.set(value, cachedDoc ? cloneDocument(cachedDoc) : null);
    });

    return results;
  }
}

function resolveOptions(options: DatabaseOptions): ResolvedOptions {
  const dataDir = options.dataDir ?? path.resolve(process.cwd(), 'data');
  const binaryDir =
    options.binaryDir ?? path.resolve(process.cwd(), 'binaries');
  const indexDir = options.indexDir ?? path.join(dataDir, 'indexes');
  const logDir = options.logDir ?? path.join(dataDir, 'logs');
  const tokenizer = resolveTokenizer(options.tokenizer);
  return {
    ...options,
    dataDir,
    binaryDir,
    logDir,
    indexDir,
    tokenizer,
    schemas: options.schemas ?? {},
    log: options.log ?? createFileLogger(logDir),
    autoCompact: options.autoCompact ?? true,
    snapshotInterval: options.snapshotInterval ?? 100,
    logRetention: options.logRetention ?? 'truncate',
    fsync: options.fsync ?? 'batch',
    lockMode: options.lockMode ?? 'lockfile',
    lockRetryMs: options.lockRetryMs ?? 25,
    lockTimeoutMs: options.lockTimeoutMs ?? 2000,
    joinCacheMaxEntries: options.joinCacheMaxEntries ?? 1000,
    joinCacheTTLms: options.joinCacheTTLms,
    dedupeBinaries: options.dedupeBinaries ?? true
  };
}

interface NormalizedJoin {
  key: string;
  relation: JoinRelation;
  targetPath: string;
  values: DocumentId[];
  isMany: boolean;
}

function normalizeJoinRelations(
  doc: Document,
  relations: JoinRelations
): NormalizedJoin[] {
  const flattened: NormalizedJoin[] = [];
  if (!relations) {
    return flattened;
  }

  Object.entries(relations).forEach(([key, raw]) => {
    const relationList = Array.isArray(raw) ? raw : [raw];

    relationList.forEach((relation) => {
      if (!relation?.localField || !relation.foreignCollection) {
        return;
      }

      const localValue = getValueAtPath(
        doc as Record<string, unknown>,
        relation.localField
      );
      const isMany = resolveManyFlag(relation, localValue);
      const values = extractJoinValues(localValue, isMany);
      const foreignField = relation.foreignField ?? '_id';

      flattened.push({
        key,
        relation: { ...relation, foreignField },
        targetPath: relation.as ?? key ?? relation.localField,
        values,
        isMany
      });
    });
  });

  return flattened;
}

function resolveProjection(
  entry: NormalizedJoin,
  lookup: Map<DocumentId, Document | null>
): Document | Document[] | null {
  if (entry.isMany) {
    const resolved = entry.values
      .map((value) => lookup.get(value))
      .filter((value): value is Document => !!value)
      .map((value) => projectDocument(value, entry.relation.projection));
    return resolved;
  }

  const value = entry.values[0];
  if (!value) {
    return null;
  }
  const found = lookup.get(value);
  return found ? projectDocument(found, entry.relation.projection) : null;
}

function extractJoinValues(
  localValue: unknown,
  isMany: boolean
): DocumentId[] {
  if (isMany) {
    if (!Array.isArray(localValue)) {
      return [];
    }
    return localValue
      .filter((value): value is DocumentId => typeof value === 'string')
      .map((value) => value as DocumentId);
  }

  return typeof localValue === 'string' ? [localValue as DocumentId] : [];
}

function resolveManyFlag(relation: JoinRelation, localValue: unknown): boolean {
  if (typeof relation.many === 'boolean') {
    return relation.many;
  }
  return Array.isArray(localValue);
}

function serializeJoinKey(collection: string, field?: string): string {
  return `${collection}::${field ?? '_id'}`;
}

function isJoinCacheEntryExpired(entry: JoinCacheEntry): boolean {
  return entry.expiresAt !== Number.POSITIVE_INFINITY && entry.expiresAt <= Date.now();
}

function projectDocument(
  doc: Document,
  projection?: string[]
): Document {
  if (!projection || projection.length === 0) {
    return cloneDocument(doc);
  }

  const projected: Document = {};
  if (doc._id !== undefined) {
    projected._id = doc._id;
  }

  const source = doc as Record<string, unknown>;
  projection.forEach((fieldPath) => {
    const value = getValueAtPath(source, fieldPath);
    if (value === undefined) {
      return;
    }
    setValueAtPath(
      projected as Record<string, unknown>,
      fieldPath,
      cloneDocument(value) as unknown
    );
  });

  return projected;
}

function getValueAtPath(
  source: Record<string, unknown>,
  path: string
): unknown {
  if (!path.includes('.')) {
    return source[path];
  }

  return path.split('.').reduce<unknown>((current, segment) => {
    if (!isPlainObject(current)) {
      return undefined;
    }
    return (current as Record<string, unknown>)[segment];
  }, source);
}

function setValueAtPath(
  target: Record<string, unknown>,
  path: string,
  value: unknown
): void {
  const segments = path.split('.');
  let cursor: Record<string, unknown> = target;

  for (let idx = 0; idx < segments.length - 1; idx += 1) {
    const segment = segments[idx] as string;
    const next = cursor[segment];
    if (!isPlainObject(next)) {
      cursor[segment] = {};
    }
    cursor = cursor[segment] as Record<string, unknown>;
  }

  cursor[segments[segments.length - 1] as string] = value;
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

function isCollectionSchema(value: unknown): value is CollectionSchema {
  return (
    !!value &&
    typeof value === 'object' &&
    !Array.isArray(value) &&
    typeof (value as CollectionSchema).fields === 'object'
  );
}

function normalizeManifest(manifest: Manifest): Manifest {
  const manifestVersion =
    typeof manifest?.manifestVersion === 'number' ? manifest.manifestVersion : 1;
  const collections: Record<string, ManifestCollectionEntry | undefined> = {};
  const binaries: Record<string, BinaryManifestEntry | undefined> = {};

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
        schema: isCollectionSchema(entry.schema) ? entry.schema : undefined,
        indexes,
        updatedAt: entry.updatedAt
      };
    });
  }

  if (manifest && typeof manifest.binaries === 'object') {
    Object.entries(manifest.binaries).forEach(([sha, entry]) => {
      if (!entry) {
        binaries[sha] = undefined;
        return;
      }

      binaries[sha] = {
        size: typeof entry.size === 'number' ? entry.size : 0,
        mimeType: typeof entry.mimeType === 'string' ? entry.mimeType : undefined,
        createdAt: entry.createdAt ?? new Date(0).toISOString(),
        updatedAt: entry.updatedAt,
        refCount: typeof entry.refCount === 'number' ? entry.refCount : 0
      };
    });
  }

  return { manifestVersion, collections, binaries };
}

function ensureDirectories(options: ResolvedOptions): void {
  fs.mkdirSync(options.dataDir, { recursive: true });
  fs.mkdirSync(options.binaryDir, { recursive: true });
  fs.mkdirSync(options.indexDir, { recursive: true });
  fs.mkdirSync(options.logDir, { recursive: true });
}

async function appendFileWithSync(
  filePath: string,
  content: string,
  fsyncMode: 'always' | 'batch' | 'never'
): Promise<void> {
  const handle = await fsp.open(filePath, 'a');
  try {
    await handle.writeFile(content, { encoding: 'utf8' });
    if (fsyncMode === 'always') {
      await handle.sync();
    }
  } finally {
    await handle.close();
  }
}

async function writeFileWithSync(
  filePath: string,
  content: string | Buffer,
  fsyncMode: 'always' | 'batch' | 'never'
): Promise<void> {
  const handle = await fsp.open(filePath, 'w');
  try {
    if (typeof content === 'string') {
      await handle.writeFile(content, { encoding: 'utf8' });
    } else {
      await handle.writeFile(content);
    }

    if (fsyncMode !== 'never') {
      await handle.sync();
    }
  } finally {
    await handle.close();
  }
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
  metadata: IndexMetadata,
  fsyncMode: 'always' | 'batch' | 'never'
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
  await writeFileWithSync(
    indexPath,
    JSON.stringify(payload, null, 2),
    fsyncMode === 'never' ? 'never' : 'batch'
  );
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

function applyDefaultsToDocument(doc: Document, schema: CollectionSchema): Document {
  const record = doc as Record<string, unknown>;

  for (const [field, definition] of Object.entries(schema.fields)) {
    const value = record[field];
    if (value === undefined && definition.default !== undefined) {
      record[field] = cloneDocument(definition.default) as unknown;
      continue;
    }

    if (definition.type === 'object' && isPlainObject(value)) {
      record[field] = applyDefaultsToObject(
        value as Record<string, unknown>,
        definition.fields
      );
    } else if (definition.type === 'array' && Array.isArray(value)) {
      record[field] = value.map((item) =>
        applyDefaultsToValue(item, definition.items)
      );
    }
  }

  return record as Document;
}

function applyDefaultsToObject(
  value: Record<string, unknown>,
  fields: Record<string, FieldSchema>
): Record<string, unknown> {
  const clone = { ...value };
  Object.entries(fields).forEach(([key, fieldSchema]) => {
    if (clone[key] === undefined && fieldSchema.default !== undefined) {
      clone[key] = cloneDocument(fieldSchema.default) as unknown;
      return;
    }

    if (fieldSchema.type === 'object' && isPlainObject(clone[key])) {
      clone[key] = applyDefaultsToObject(
        clone[key] as Record<string, unknown>,
        fieldSchema.fields
      );
    } else if (fieldSchema.type === 'array' && Array.isArray(clone[key])) {
      clone[key] = (clone[key] as unknown[]).map((item) =>
        applyDefaultsToValue(item, fieldSchema.items)
      );
    }
  });

  return clone;
}

function applyDefaultsToValue(value: unknown, schema: FieldSchema): unknown {
  if (value === undefined && schema.default !== undefined) {
    return cloneDocument(schema.default) as unknown;
  }

  if (schema.type === 'object' && isPlainObject(value)) {
    return applyDefaultsToObject(
      value as Record<string, unknown>,
      schema.fields
    );
  }

  if (schema.type === 'array' && Array.isArray(value)) {
    return value.map((item) => applyDefaultsToValue(item, schema.items));
  }

  return value;
}

function validateDocumentAgainstSchema(
  doc: Document,
  schema: CollectionSchema
): void {
  const record = doc as Record<string, unknown>;
  Object.keys(record).forEach((key) => {
    if (key === '_id' || key === '_binRefs') {
      return;
    }
    if (!schema.fields[key]) {
      throw new Error(`Unexpected field '${key}' not defined in schema`);
    }
  });

  Object.entries(schema.fields).forEach(([field, definition]) => {
    const value = record[field];
    validateField(`${field}`, value, definition);
  });
}

function validateField(pathname: string, value: unknown, schema: FieldSchema): void {
  if (value === undefined) {
    if (schema.required) {
      throw new Error(`Field '${pathname}' is required`);
    }
    return;
  }

  switch (schema.type) {
    case 'string':
      assertString(pathname, value);
      if (schema.minLength !== undefined && (value as string).length < schema.minLength) {
        throw new Error(
          `Field '${pathname}' must be at least ${schema.minLength} characters`
        );
      }
      if (schema.maxLength !== undefined && (value as string).length > schema.maxLength) {
        throw new Error(
          `Field '${pathname}' must be at most ${schema.maxLength} characters`
        );
      }
      if (schema.pattern) {
        const regex = new RegExp(schema.pattern);
        if (!regex.test(value as string)) {
          throw new Error(`Field '${pathname}' does not match pattern ${schema.pattern}`);
        }
      }
      if (schema.enum && !schema.enum.includes(value as string)) {
        throw new Error(
          `Field '${pathname}' must be one of: ${schema.enum.join(', ')}`
        );
      }
      break;
    case 'number':
      assertNumber(pathname, value);
      if (schema.min !== undefined && (value as number) < schema.min) {
        throw new Error(`Field '${pathname}' must be >= ${schema.min}`);
      }
      if (schema.max !== undefined && (value as number) > schema.max) {
        throw new Error(`Field '${pathname}' must be <= ${schema.max}`);
      }
      if (schema.integer && !Number.isInteger(value as number)) {
        throw new Error(`Field '${pathname}' must be an integer`);
      }
      break;
    case 'boolean':
      if (typeof value !== 'boolean') {
        throw new Error(`Field '${pathname}' must be a boolean`);
      }
      break;
    case 'date':
      if (
        !(typeof value === 'string' || value instanceof Date) ||
        Number.isNaN(new Date(value as string | Date).getTime())
      ) {
        throw new Error(`Field '${pathname}' must be a valid date/ISO string`);
      }
      break;
    case 'object':
      if (!isPlainObject(value)) {
        throw new Error(`Field '${pathname}' must be an object`);
      }
      validateNestedObject(pathname, value as Record<string, unknown>, schema.fields);
      break;
    case 'array':
      if (!Array.isArray(value)) {
        throw new Error(`Field '${pathname}' must be an array`);
      }
      if (schema.minItems !== undefined && value.length < schema.minItems) {
        throw new Error(
          `Field '${pathname}' must have at least ${schema.minItems} items`
        );
      }
      if (schema.maxItems !== undefined && value.length > schema.maxItems) {
        throw new Error(
          `Field '${pathname}' must have at most ${schema.maxItems} items`
        );
      }
      value.forEach((item, idx) => {
        validateField(`${pathname}[${idx}]`, item, schema.items);
      });
      break;
    case 'json':
      // json accepts any value
      break;
    default:
      throw new Error(`Field '${pathname}' has unsupported schema type`);
  }
}

function validateNestedObject(
  pathname: string,
  value: Record<string, unknown>,
  fields: Record<string, FieldSchema>
): void {
  Object.keys(value).forEach((key) => {
    if (!fields[key]) {
      throw new Error(`Unexpected field '${pathname}.${key}' not defined in schema`);
    }
  });

  Object.entries(fields).forEach(([key, definition]) => {
    const childValue = value[key];
    validateField(`${pathname}.${key}`, childValue, definition);
  });
}

function assertString(pathname: string, value: unknown): void {
  if (typeof value !== 'string') {
    throw new Error(`Field '${pathname}' must be a string`);
  }
}

function assertNumber(pathname: string, value: unknown): void {
  if (typeof value !== 'number' || Number.isNaN(value)) {
    throw new Error(`Field '${pathname}' must be a number`);
  }
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
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

function normalizeBinaryInput(
  data: Buffer | ArrayBuffer | Uint8Array | string
): Buffer {
  if (Buffer.isBuffer(data)) {
    return data;
  }

  if (typeof data === 'string') {
    return Buffer.from(data);
  }

  if (data instanceof ArrayBuffer) {
    return Buffer.from(data);
  }

  if (ArrayBuffer.isView(data)) {
    return Buffer.from(data.buffer);
  }

  return Buffer.from([]);
}

function hashBuffer(buffer: Buffer): string {
  const hash = crypto.createHash('sha256');
  hash.update(buffer);
  return hash.digest('hex');
}

function extractBinaryRefs(doc: Document | null): BinaryReference[] {
  if (!doc) {
    return [];
  }

  const refs = (doc as Document)._binRefs;
  if (!Array.isArray(refs)) {
    return [];
  }

  return refs.filter(isBinaryReference).map((ref) => ({
    field: ref.field,
    sha256: ref.sha256,
    size: typeof ref.size === 'number' ? ref.size : undefined,
    mimeType: typeof ref.mimeType === 'string' ? ref.mimeType : undefined
  }));
}

function isBinaryReference(value: unknown): value is BinaryReference {
  if (!isPlainObject(value)) {
    return false;
  }
  const candidate = value as Record<string, unknown>;
  return typeof candidate.field === 'string' && typeof candidate.sha256 === 'string';
}

function countBinaryRefs(
  refs: BinaryReference[]
): Record<string, { count: number; size?: number; mimeType?: string }> {
  const counts: Record<string, { count: number; size?: number; mimeType?: string }> = {};

  refs.forEach((ref) => {
    const current = counts[ref.sha256] ?? { count: 0 };
    current.count += 1;
    if (current.size === undefined && ref.size !== undefined) {
      current.size = ref.size;
    }
    if (current.mimeType === undefined && ref.mimeType !== undefined) {
      current.mimeType = ref.mimeType;
    }
    counts[ref.sha256] = current;
  });

  return counts;
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

async function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
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

function isEexistError(error: unknown): boolean {
  if (!error || typeof error !== 'object') {
    return false;
  }

  const code = (error as { code?: unknown }).code;
  return code === 'EEXIST';
}

export function createDatabase(options: DatabaseOptions = {}): Database {
  return new JsonFileDatabase(options);
}

export * from './types.js';
