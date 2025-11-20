import path from 'node:path';
import fs from 'node:fs';
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

const noopLogger: Logger = {
  debug: () => undefined,
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined
};

class JsonFileDatabase implements Database {
  private options: ResolvedOptions;

  constructor(options: DatabaseOptions = {}) {
    this.options = resolveOptions(options);
    ensureDirectories(this.options);
  }

  async insert(collection: string, doc: Document): Promise<Document> {
    void doc;
    throw new Error(`insert not implemented for ${collection}`);
  }

  async get(collection: string, id: DocumentId): Promise<Document | null> {
    throw new Error(`get not implemented for ${collection}:${id}`);
  }

  async update(
    collection: string,
    id: DocumentId,
    mutation: Partial<Document>
  ): Promise<Document> {
    void mutation;
    throw new Error(`update not implemented for ${collection}:${id}`);
  }

  async remove(collection: string, id: DocumentId): Promise<void> {
    throw new Error(`remove not implemented for ${collection}:${id}`);
  }

  async find(
    collection: string,
    filter: Filter = {},
    options: FindOptions = {}
  ): Promise<Document[]> {
    void options;
    throw new Error(
      `find not implemented for ${collection} with filter ${JSON.stringify(
        filter
      )}`
    );
  }

  async *stream(
    collection: string,
    filter: Filter = {},
    options: FindOptions = {}
  ): AsyncIterable<Document> {
    void options;
    yield* [];
    throw new Error(
      `stream not implemented for ${collection} with filter ${JSON.stringify(
        filter
      )}`
    );
  }

  async ensureIndex(
    collection: string,
    field: string,
    options: IndexOptions = {}
  ): Promise<void> {
    void options;
    throw new Error(`ensureIndex not implemented for ${collection}.${field}`);
  }

  async rebuildIndex(collection: string, field: string): Promise<void> {
    throw new Error(`rebuildIndex not implemented for ${collection}.${field}`);
  }

  async join(
    collection: string,
    doc: Document,
    relations: Record<string, unknown>
  ): Promise<Document> {
    void relations;
    throw new Error(`join not implemented for ${collection} with id ${doc._id}`);
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

export function createDatabase(options: DatabaseOptions = {}): Database {
  return new JsonFileDatabase(options);
}

export * from './types.js';
