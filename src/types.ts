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
  _binRefs?: BinaryReference[];
}

export type FieldType =
  | 'string'
  | 'number'
  | 'boolean'
  | 'date'
  | 'object'
  | 'array'
  | 'json';

export interface BaseFieldSchema {
  type: FieldType;
  required?: boolean;
  default?: JsonValue;
}

export interface StringFieldSchema extends BaseFieldSchema {
  type: 'string';
  minLength?: number;
  maxLength?: number;
  pattern?: string;
  enum?: string[];
}

export interface NumberFieldSchema extends BaseFieldSchema {
  type: 'number';
  min?: number;
  max?: number;
  integer?: boolean;
}

export interface BooleanFieldSchema extends BaseFieldSchema {
  type: 'boolean';
}

export interface DateFieldSchema extends BaseFieldSchema {
  type: 'date';
}

export interface ObjectFieldSchema extends BaseFieldSchema {
  type: 'object';
  fields: Record<string, FieldSchema>;
}

export interface ArrayFieldSchema extends BaseFieldSchema {
  type: 'array';
  items: FieldSchema;
  minItems?: number;
  maxItems?: number;
}

export interface JsonFieldSchema extends BaseFieldSchema {
  type: 'json';
}

export type FieldSchema =
  | StringFieldSchema
  | NumberFieldSchema
  | BooleanFieldSchema
  | DateFieldSchema
  | ObjectFieldSchema
  | ArrayFieldSchema
  | JsonFieldSchema;

export interface CollectionSchema {
  fields: Record<string, FieldSchema>;
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

export type IndexState = 'pending' | 'ready' | 'stale' | 'error';

export interface TokenizerOptions {
  lowerCase: boolean;
  minTokenLength: number;
  splitRegex: string;
  stopwords: string[];
}

export interface NormalizedIndexOptions {
  unique: boolean;
  prefixLength?: number;
  tokenizer: TokenizerOptions;
}

export interface IndexStats {
  docCount: number;
  tokenCount: number;
  sizeBytes?: number;
  builtAt?: string;
}

export interface IndexMetadata {
  field: string;
  path: string;
  state: IndexState;
  version: number;
  checkpoint: number;
  options: NormalizedIndexOptions;
  createdAt?: string;
  updatedAt?: string;
  lastError?: string;
  stats?: IndexStats;
}

export interface JoinRelation {
  localField: string;
  foreignCollection: string;
  foreignField?: string;
  as?: string;
  many?: boolean;
  projection?: string[];
}

export type JoinRelations = Record<string, JoinRelation | JoinRelation[]>;

export interface BinaryReference {
  field: string;
  sha256: string;
  size?: number;
  mimeType?: string;
}

export interface BinaryMetadata {
  sha256: string;
  size: number;
  mimeType?: string;
  path: string;
  createdAt: string;
  updatedAt?: string;
  refCount?: number;
  deduped?: boolean;
}

export interface BinaryWriteOptions {
  mimeType?: string;
  dedupe?: boolean;
}

export interface BinaryDeleteOptions {
  force?: boolean;
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
  logRetention?: 'truncate' | 'rotate' | 'keep';
  fsync?: 'always' | 'batch' | 'never';
  indexDir?: string;
  tokenizer?: Partial<TokenizerOptions>;
  schemas?: Record<string, CollectionSchema>;
  joinCacheMaxEntries?: number;
  joinCacheTTLms?: number;
  dedupeBinaries?: boolean;
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
    relations: JoinRelations
  ) => Promise<Document>;
  compact: (collection: string) => Promise<void>;
  clearJoinCache: () => void;
  saveBinary: (
    data: Buffer | ArrayBuffer | Uint8Array | string,
    options?: BinaryWriteOptions
  ) => Promise<BinaryMetadata>;
  readBinary: (sha256: string) => Promise<Buffer | null>;
  deleteBinary: (sha256: string, options?: BinaryDeleteOptions) => Promise<boolean>;
}
