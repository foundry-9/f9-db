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
  | 'json'
  | 'custom';

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

export interface CustomFieldSchema extends BaseFieldSchema {
  type: 'custom';
  customType: string;
  /**
   * Arbitrary options forwarded to the custom type definition (e.g., { maxLength: 128 }).
   */
  options?: Record<string, unknown>;
}

export type FieldSchema =
  | StringFieldSchema
  | NumberFieldSchema
  | BooleanFieldSchema
  | DateFieldSchema
  | ObjectFieldSchema
  | ArrayFieldSchema
  | JsonFieldSchema
  | CustomFieldSchema;

export interface CollectionSchema {
  fields: Record<string, FieldSchema>;
}

export type ComparableValue = string | number | boolean | null | Date;

export interface CustomTypeDefinition<Internal = unknown, Projected = unknown> {
  name: string;
  description?: string;
  /**
   * Used purely for docs/error messaging to hint at the base primitive.
   */
  baseType: 'string' | 'number' | 'boolean' | 'date' | 'json';
  /**
   * Normalize/coerce user input (or defaults) into the internal representation that will be
   * persisted in the database.
   */
  fromInput: (value: unknown, options?: Record<string, unknown>) => Internal;
  /**
   * Convert the stored internal value into something that can be compared/sorted/filtered.
   * If omitted, the internal value is used directly.
   */
  toComparable?: (value: Internal, options?: Record<string, unknown>) => ComparableValue;
  /**
   * Optional comparator for cases where string/number comparison is not sufficient
   * (e.g., lexical-safe decimal comparison).
   */
  compare?: (
    left: ComparableValue,
    right: ComparableValue,
    options?: Record<string, unknown>
  ) => number | null;
  /**
   * Build the projected value returned to callers. Defaults to the internal value.
   */
  project?: (value: Internal, options?: Record<string, unknown>) => Projected;
  /**
   * Human-friendly hint of acceptable inputs (used only in docs/error messages).
   */
  accepts?: string[];
}

export type CustomTypeRegistry = Record<string, CustomTypeDefinition<unknown, unknown>>;

export interface FieldOperator {
  $eq?: ComparableValue;
  $ne?: ComparableValue;
  $gt?: ComparableValue;
  $gte?: ComparableValue;
  $lt?: ComparableValue;
  $lte?: ComparableValue;
  $in?: ComparableValue[];
  $nin?: ComparableValue[];
  $between?: [ComparableValue, ComparableValue];
  $like?: string;
  $ilike?: string;
  $exists?: boolean;
  $isNull?: boolean;
  $not?: FieldPredicate;
}

export type FieldPredicate = ComparableValue | ComparableValue[] | FieldOperator;

export interface Filter {
  [field: string]: FieldPredicate | Filter | Filter[] | undefined;
  $and?: Filter[];
  $or?: Filter[];
  $not?: Filter;
}

export interface FindOptions {
  limit?: number;
  skip?: number;
  sort?: Record<string, 1 | -1>;
  projection?: string[];
  groupBy?: string[];
  aggregates?: Record<string, AggregateDefinition>;
  partitionBy?: string[];
  rowNumber?: boolean | RowNumberOptions;
  stream?: boolean;
  /**
   * When true, `stream` reads directly from snapshot/log files instead of loading the in-memory collection map.
   * This is best for very large datasets where memory pressure is a concern.
   */
  streamFromFiles?: boolean;
  diagnostics?: (stats: StreamDiagnostics) => void;
}

export type UpdateMutation = Partial<Document> | ((doc: Document) => Partial<Document>);

export type UpdateWhereOptions = Pick<FindOptions, 'limit' | 'skip' | 'sort'>;
export type RemoveWhereOptions = Pick<FindOptions, 'limit' | 'skip' | 'sort'>;

export type AggregateOperator = 'count' | 'sum' | 'avg' | 'min' | 'max';

export interface AggregateDefinition {
  op: AggregateOperator;
  field?: string;
}

export interface RowNumberOptions {
  as?: string;
  orderBy?: Record<string, 1 | -1>;
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

export interface StreamDiagnostics {
  scannedDocs: number;
  matchedDocs: number;
  yieldedDocs: number;
  maxBufferedDocs: number;
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
  logDir?: string;
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
  lockMode?: 'lockfile' | 'flock' | 'none';
  lockRetryMs?: number;
  lockTimeoutMs?: number;
  customTypes?: CustomTypeRegistry;
}

export interface Database {
  insert: (collection: string, doc: Document) => Promise<Document>;
  get: (collection: string, id: DocumentId) => Promise<Document | null>;
  update: (
    collection: string,
    id: DocumentId,
    mutation: UpdateMutation
  ) => Promise<Document>;
  updateWhere: (
    collection: string,
    mutation: UpdateMutation,
    filter: Filter,
    options?: UpdateWhereOptions
  ) => Promise<Document[]>;
  removeWhere: (
    collection: string,
    filter?: Filter,
    options?: RemoveWhereOptions
  ) => Promise<Document[]>;
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
  ) => AsyncIterable<string>;
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
