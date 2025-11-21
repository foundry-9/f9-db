/** Unique identifier assigned to each stored document. */
export type DocumentId = string;

/**
 * JSON-compatible value used for defaults, schema literals, and arbitrary payloads.
 */
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

/**
 * Core document shape persisted in collections. `_id` is optional on insert and `_binRefs`
 * tracks related binary attachments.
 */
export interface Document extends Record<string, unknown> {
  _id?: DocumentId;
  _binRefs?: BinaryReference[];
}

/** Allowed primitive field types when defining a schema. */
export type FieldType =
  | 'string'
  | 'number'
  | 'boolean'
  | 'date'
  | 'object'
  | 'array'
  | 'json'
  | 'custom';

/** Options shared by all schema field definitions. */
export interface BaseFieldSchema {
  type: FieldType;
  required?: boolean;
  default?: JsonValue;
}

/** Schema definition for strings with optional length/pattern constraints. */
export interface StringFieldSchema extends BaseFieldSchema {
  type: 'string';
  minLength?: number;
  maxLength?: number;
  pattern?: string;
  enum?: string[];
}

/** Schema definition for numeric fields (integers or floats). */
export interface NumberFieldSchema extends BaseFieldSchema {
  type: 'number';
  min?: number;
  max?: number;
  integer?: boolean;
}

/** Schema definition for booleans. */
export interface BooleanFieldSchema extends BaseFieldSchema {
  type: 'boolean';
}

/** Schema definition for ISO date strings/Date instances. */
export interface DateFieldSchema extends BaseFieldSchema {
  type: 'date';
}

/** Schema definition for nested objects with per-field sub-schemas. */
export interface ObjectFieldSchema extends BaseFieldSchema {
  type: 'object';
  fields: Record<string, FieldSchema>;
}

/** Schema definition for homogeneous arrays. */
export interface ArrayFieldSchema extends BaseFieldSchema {
  type: 'array';
  items: FieldSchema;
  minItems?: number;
  maxItems?: number;
}

/** Schema definition for flexible JSON blobs with no validation. */
export interface JsonFieldSchema extends BaseFieldSchema {
  type: 'json';
}

/** Schema definition that references a custom type implementation. */
export interface CustomFieldSchema extends BaseFieldSchema {
  type: 'custom';
  customType: string;
  /**
   * Arbitrary options forwarded to the custom type definition (e.g., { maxLength: 128 }).
   */
  options?: Record<string, unknown>;
}

/** Union of supported schema definitions. */
export type FieldSchema =
  | StringFieldSchema
  | NumberFieldSchema
  | BooleanFieldSchema
  | DateFieldSchema
  | ObjectFieldSchema
  | ArrayFieldSchema
  | JsonFieldSchema
  | CustomFieldSchema;

/** Describes the schema for a collection/table. */
export interface CollectionSchema {
  fields: Record<string, FieldSchema>;
}

/** Primitive values that can participate in comparisons and filtering. */
export type ComparableValue = string | number | boolean | null | Date;

/**
 * Declarative contract for custom field types that plug into validation/comparison logic.
 */
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

/** Map of custom type names to their definitions. */
export type CustomTypeRegistry = Record<string, CustomTypeDefinition<unknown, unknown>>;

/** Supported per-field filter operations. */
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

/** Filter predicate for a single field (value, operator expression, or array). */
export type FieldPredicate = ComparableValue | ComparableValue[] | FieldOperator;

/** Root filter shape supporting nested fields and boolean combinations. */
export interface Filter {
  [field: string]: FieldPredicate | Filter | Filter[] | undefined;
  $and?: Filter[];
  $or?: Filter[];
  $not?: Filter;
}

/** Additional options that control `find` and `stream` queries. */
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

/** Mutation payload applied during `update` or `updateWhere`. */
export type UpdateMutation = Partial<Document> | ((doc: Document) => Partial<Document>);

/** Supported pagination options during `updateWhere`. */
export type UpdateWhereOptions = Pick<FindOptions, 'limit' | 'skip' | 'sort'>;
/** Supported pagination options during `removeWhere`. */
export type RemoveWhereOptions = Pick<FindOptions, 'limit' | 'skip' | 'sort'>;

/** Aggregate operations available in queries. */
export type AggregateOperator = 'count' | 'sum' | 'avg' | 'min' | 'max';

/** Definition of a single aggregate computed during `find`. */
export interface AggregateDefinition {
  op: AggregateOperator;
  field?: string;
}

/** Options for `ROW_NUMBER()` style calculations. */
export interface RowNumberOptions {
  as?: string;
  orderBy?: Record<string, 1 | -1>;
}

/** User-facing index creation options. */
export interface IndexOptions {
  unique?: boolean;
  prefixLength?: number;
}

/** Lifecycle state emitted for index metadata. */
export type IndexState = 'pending' | 'ready' | 'stale' | 'error';

/** Tokenizer configuration leveraged by text indexes. */
export interface TokenizerOptions {
  lowerCase: boolean;
  minTokenLength: number;
  splitRegex: string;
  stopwords: string[];
}

/** Fully normalized index options persisted in manifests. */
export interface NormalizedIndexOptions {
  unique: boolean;
  prefixLength?: number;
  tokenizer: TokenizerOptions;
}

/** Basic stats reported for each index. */
export interface IndexStats {
  docCount: number;
  tokenCount: number;
  sizeBytes?: number;
  builtAt?: string;
}

/** Manifest metadata describing a single index. */
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

/** Configuration for a single join between collections. */
export interface JoinRelation {
  localField: string;
  foreignCollection: string;
  foreignField?: string;
  as?: string;
  many?: boolean;
  projection?: string[];
}

/** Join definitions keyed by field alias. */
export type JoinRelations = Record<string, JoinRelation | JoinRelation[]>;

/** Reference to a binary attachment associated with a document field. */
export interface BinaryReference {
  field: string;
  sha256: string;
  size?: number;
  mimeType?: string;
}

/** Metadata describing saved binary blobs. */
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

/** Options for saving binary data (e.g., MIME type hints). */
export interface BinaryWriteOptions {
  mimeType?: string;
  dedupe?: boolean;
}

/** Options provided when deleting binaries. */
export interface BinaryDeleteOptions {
  force?: boolean;
}

/** Diagnostics produced when scanning documents for a query/stream. */
export interface StreamDiagnostics {
  scannedDocs: number;
  matchedDocs: number;
  yieldedDocs: number;
  maxBufferedDocs: number;
}

/** Structured logger implementation passed into the database. */
export interface Logger {
  debug?: (msg: string, context?: Record<string, unknown>) => void;
  info?: (msg: string, context?: Record<string, unknown>) => void;
  warn?: (msg: string, context?: Record<string, unknown>) => void;
  error?: (msg: string, context?: Record<string, unknown>) => void;
}

/** Options accepted by `createDatabase`. */
export interface DatabaseOptions {
  /** Directory used for manifests, snapshots, logs, and indexes (defaults to `./data`). */
  dataDir?: string;
  /** Directory dedicated to binary blobs (defaults to `./binaries`). */
  binaryDir?: string;
  /** Directory for JSON log files written by the default logger. */
  logDir?: string;
  /** Custom logger implementation for structured logs. */
  log?: Logger;
  /** When true, collections automatically compact after accumulating writes. */
  autoCompact?: boolean;
  /** Number of writes between automatic snapshots (defaults to 1000). */
  snapshotInterval?: number;
  /** Determines how to handle collection logs when snapshots run. */
  logRetention?: 'truncate' | 'rotate' | 'keep';
  /** Durability level for fsync operations (`always`, `batch`, or `never`). */
  fsync?: 'always' | 'batch' | 'never';
  /** Directory that holds index data (defaults to `${dataDir}/indexes`). */
  indexDir?: string;
  /** Override default tokenizer behavior for text indexes. */
  tokenizer?: Partial<TokenizerOptions>;
  /** Strict schemas per collection enforced on insert/update. */
  schemas?: Record<string, CollectionSchema>;
  /** Maximum number of entries stored in the join cache. */
  joinCacheMaxEntries?: number;
  /** Optional time-to-live for join cache entries in milliseconds. */
  joinCacheTTLms?: number;
  /** Toggle binary deduplication behavior (default true). */
  dedupeBinaries?: boolean;
  /** Locking mechanism for coordinating multiple writers. */
  lockMode?: 'lockfile' | 'flock' | 'none';
  /** Milliseconds to wait between lock acquisition retries. */
  lockRetryMs?: number;
  /** Milliseconds before lock acquisition gives up and throws. */
  lockTimeoutMs?: number;
  /** Custom type registry for schema validation/parsing. */
  customTypes?: CustomTypeRegistry;
}

/** Runtime contract returned by `createDatabase`. */
export interface Database {
  /** Insert a single document into the target collection. */
  insert: (collection: string, doc: Document) => Promise<Document>;
  /** Retrieve a document by `_id`, returning `null` when it does not exist. */
  get: (collection: string, id: DocumentId) => Promise<Document | null>;
  /** Update a document by `_id` using a partial object or mutation function. */
  update: (
    collection: string,
    id: DocumentId,
    mutation: UpdateMutation
  ) => Promise<Document>;
  /** Apply a mutation to every document that matches the provided filter. */
  updateWhere: (
    collection: string,
    mutation: UpdateMutation,
    filter: Filter,
    options?: UpdateWhereOptions
  ) => Promise<Document[]>;
  /** Remove every document that matches the filter and return deleted rows. */
  removeWhere: (
    collection: string,
    filter?: Filter,
    options?: RemoveWhereOptions
  ) => Promise<Document[]>;
  /** Delete a single document by `_id`. */
  remove: (collection: string, id: DocumentId) => Promise<void>;
  /** Find documents, optionally with projections, sorting, grouping, and aggregates. */
  find: (
    collection: string,
    filter?: Filter,
    options?: FindOptions
  ) => Promise<Document[]>;
  /** Stream JSONL rows for very large result sets. */
  stream: (
    collection: string,
    filter?: Filter,
    options?: FindOptions
  ) => AsyncIterable<string>;
  /** Build or update an index to speed lookups and enforce uniqueness. */
  ensureIndex: (
    collection: string,
    field: string,
    options?: IndexOptions
  ) => Promise<void>;
  /** Force a full rebuild of the specified index. */
  rebuildIndex: (collection: string, field: string) => Promise<void>;
  /** Resolve referenced documents across collections, returning a hydrated doc. */
  join: (
    collection: string,
    doc: Document,
    relations: JoinRelations
  ) => Promise<Document>;
  /** Compact the underlying log/snapshot for the given collection. */
  compact: (collection: string) => Promise<void>;
  /** Clear the join cache, forcing the next join to refetch dependencies. */
  clearJoinCache: () => void;
  /** Save binary data and return metadata describing the stored blob. */
  saveBinary: (
    data: Buffer | ArrayBuffer | Uint8Array | string,
    options?: BinaryWriteOptions
  ) => Promise<BinaryMetadata>;
  /** Read binary data by SHA-256 hash. Returns `null` when not found. */
  readBinary: (sha256: string) => Promise<Buffer | null>;
  /** Delete binary data, optionally forcing the removal even with outstanding references. */
  deleteBinary: (sha256: string, options?: BinaryDeleteOptions) => Promise<boolean>;
}
