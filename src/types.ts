export type DocumentId = string;

export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

export interface Document extends Record<string, unknown> {
  _id?: DocumentId;
}

export interface Filter {
  [key: string]: unknown;
}

export interface FindOptions {
  limit?: number;
  skip?: number;
  sort?: Record<string, 1 | -1>;
  stream?: boolean;
}

export interface IndexOptions {
  unique?: boolean;
  prefixLength?: number;
}

export interface Logger {
  debug?: (msg: string, context?: Record<string, unknown>) => void;
  info?: (msg: string, context?: Record<string, unknown>) => void;
  warn?: (msg: string, context?: Record<string, unknown>) => void;
  error?: (msg: string, context?: Record<string, unknown>) => void;
}

export interface DatabaseOptions {
  dataDir?: string;
  binaryDir?: string;
  log?: Logger;
  autoCompact?: boolean;
  snapshotInterval?: number;
  fsync?: 'always' | 'batch' | 'never';
}

export interface Database {
  insert: (collection: string, doc: Document) => Promise<Document>;
  get: (collection: string, id: DocumentId) => Promise<Document | null>;
  update: (
    collection: string,
    id: DocumentId,
    mutation: Partial<Document>
  ) => Promise<Document>;
  remove: (collection: string, id: DocumentId) => Promise<void>;
  find: (
    collection: string,
    filter?: Filter,
    options?: FindOptions
  ) => Promise<Document[]>;
  stream: (
    collection: string,
    filter?: Filter,
    options?: FindOptions
  ) => AsyncIterable<Document>;
  ensureIndex: (
    collection: string,
    field: string,
    options?: IndexOptions
  ) => Promise<void>;
  rebuildIndex: (collection: string, field: string) => Promise<void>;
  join: (
    collection: string,
    doc: Document,
    relations: Record<string, unknown>
  ) => Promise<Document>;
  compact: (collection: string) => Promise<void>;
}
