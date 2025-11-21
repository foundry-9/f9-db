# f9-db API Guide

This guide explains every public capability exposed by `f9-db`, from setup and configuration through querying, joins, binaries, and custom types. It is meant to be read alongside the high-level overview in `README.md`.

## Installation

```sh
npm install f9-db
# or
pnpm add f9-db
```

The package targets Node.js 22+ with native ESM syntax. TypeScript definitions ship with the library.

## Creating a Database Instance

```ts
import { createDatabase } from 'f9-db';

const db = createDatabase({
  dataDir: './data',        // default: ./data
  binaryDir: './binaries',  // default: ./binaries
  logDir: './logs'          // default: ./logs
});
```

`createDatabase` resolves paths, creates directories where needed, loads existing manifests/snapshots, and returns a `Database` interface with the methods documented below. Instantiation is lightweight even for large datasets because collections are lazily loaded.

### Supported Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `dataDir` | `string` | `./data` | Directory for append-only JSONL logs, snapshots, manifest, and indexes. |
| `binaryDir` | `string` | `./binaries` | Directory that stores binary blobs keyed by SHA-256. |
| `logDir` | `string` | `./logs` | Directory for JSON log files when the default logger is used. |
| `log` | `Logger` | built-in JSON logger | Inject your own logger (e.g., pino/winston). Anything implementing `debug/info/warn/error` works. |
| `autoCompact` | `boolean` | `true` | Automatically compacts a collection after a threshold of writes. You can call `compact` manually at any time. |
| `snapshotInterval` | `number` | `1000` | Number of writes between automatic snapshots. |
| `logRetention` | `'truncate' \| 'rotate' \| 'keep'` | `'rotate'` | How to prune log files when a snapshot is taken. |
| `fsync` | `'always' \| 'batch' \| 'never'` | `'batch'` | Durability mode for writes and index updates. |
| `indexDir` | `string` | `${dataDir}/indexes` | Location that stores inverted index JSON files. |
| `tokenizer` | `Partial<TokenizerOptions>` | defaults documented below | Tune text tokenization used by indexes (`lowerCase`, `splitRegex`, `minTokenLength`, `stopwords`). |
| `schemas` | `Record<string, CollectionSchema>` | `{}` | Define strict schemas for collections. Validation runs on insert/update/compact. |
| `joinCacheMaxEntries` | `number` | `1000` | Maximum documents stored in the join result cache. |
| `joinCacheTTLms` | `number` | `undefined` | TTL for join cache entries. Items never expire when omitted. |
| `dedupeBinaries` | `boolean` | `true` | Controls whether identical binary uploads are deduped by SHA-256. |
| `customTypes` | `CustomTypeRegistry` | `createDefaultCustomTypes()` | Extend or override built-in custom types (`int`, `float`, `decimal`, `varchar`). |
| `lockMode` | `'lockfile' \| 'flock' \| 'none'` | `'lockfile'` | Strategy for coordinating multi-process writes. |
| `lockRetryMs` | `number` | `50` | Delay between attempts to acquire a lock. |
| `lockTimeoutMs` | `number` | `15000` | Max time spent retrying lock acquisition. |

## Collections and Schemas

Collections behave like tables. Only collections referenced during runtime need to exist on disk; the rest are created lazily when the first document is inserted.

```ts
const db = createDatabase({
  schemas: {
    users: {
      fields: {
        name: { type: 'string', minLength: 2, required: true },
        email: { type: 'string', required: true },
        status: { type: 'string', enum: ['pending', 'active', 'disabled'], default: 'pending' },
        profile: {
          type: 'object',
          fields: {
            city: { type: 'string' },
            skills: { type: 'array', items: { type: 'string' } }
          }
        }
      }
    }
  }
});
```

Every document gets a string `_id` assigned automatically unless you supply one yourself. The `Document` interface also exposes `_binRefs`, which tracks referenced binaries (see **Binary Storage**).

## CRUD Operations

`Database` exposes ergonomic CRUD helpers:

```ts
const inserted = await db.insert('users', { name: 'Ada', email: 'ada@example.com' });
const fetched = await db.get('users', inserted._id!);

const updated = await db.update('users', inserted._id!, (doc) => ({
  ...doc,
  status: 'active'
}));

await db.remove('users', inserted._id!);
```

- `insert(collection, document)` returns the stored document (with `_id` assigned).
- `get(collection, id)` retrieves a single document or `null`.
- `update(collection, id, mutation)` accepts either a partial object or a function `(doc) => mutation`. `UpdateMutation` results are merged with the stored doc.
- `remove(collection, id)` deletes one document by `_id`.

### Bulk Mutation Helpers

- `updateWhere(collection, mutation, filter, options?)` applies a mutation across every document that matches `filter` and returns all modified documents. `options` supports `limit`, `skip`, and `sort` to scope which matches are edited.
- `removeWhere(collection, filter?, options?)` removes every matched document and returns the deleted rows (useful for audit logs).

These helpers run validations and index maintenance exactly once per affected document, making them safer than manual loops.

## Querying and Streaming

### Filters

Filters mirror Mongo/SQL-style predicates:

| Operator | Description |
| --- | --- |
| `{ field: value }` | Equality match. Arrays imply `$in`. |
| `$gt`, `$gte`, `$lt`, `$lte` | Range comparisons. |
| `$between: [min, max]` | Inclusive bounded range. |
| `$in`, `$nin` | Membership or anti-membership. |
| `$like`, `$ilike` | SQL-style wildcard strings (`%` and `_`). |
| `$exists`, `$isNull` | Nullability checks. |
| `$and`, `$or`, `$not` | Boolean composition of filters. |

### `find`

```ts
const results = await db.find('users', { status: 'active' }, {
  projection: ['name', 'profile.city'],
  limit: 25,
  sort: { createdAt: -1 }
});
```

`find` loads matching documents into memory, applies optional projections, and returns a JSON array. Supported options:

- `limit`, `skip`, `sort`
- `projection`: whitelist of fields returned (always includes `_id`).
- `groupBy` + `aggregates`: define SQL-like aggregates (`count`, `sum`, `avg`, `min`, `max`). Each aggregate is defined as `{ op, field? }`.
- `partitionBy` + `rowNumber`: compute windowed row numbers per partition (optionally customize the alias via `rowNumber.as` and ordering via `rowNumber.orderBy`).
- `diagnostics(stats)`: callback receiving scan statistics (documents scanned, matched, yielded, max buffered).

### `stream`

```ts
for await (const line of db.stream('users', { status: 'active' }, { stream: true })) {
  process.stdout.write(line);
}
```

`stream` returns an async iterator that yields JSON strings (one per document). Set `streamFromFiles: true` to bypass in-memory collection loading and read directly from snapshot/log files—ideal for very large datasets or ETL pipelines.

## Index Management

Indexes accelerate filtering on string fields and enforce uniqueness.

- `ensureIndex(collection, field, options?)`: Build or update an index for `field`. Options include `unique` and `prefixLength` for prefix indexes. Indexes reuse the tokenizer configuration documented above.
- `rebuildIndex(collection, field)`: Recompute an index when files are manually edited or corrupted.

`IndexMetadata` exposes stats such as build time, document count, and token count via manifest files (inspect `data/indexes/<collection>/<field>.json`).

## Joins

Use `join` to enrich one document with data pulled from other collections:

```ts
const invoice = await db.get('invoices', 'inv_123');
const hydrated = await db.join('invoices', invoice!, {
  customer: { localField: 'customerId', foreignCollection: 'customers', as: 'customer' },
  lineItems: {
    localField: 'itemIds',
    foreignCollection: 'items',
    many: true,
    projection: ['name', 'price']
  }
});
```

Relations can be defined as a single object or an array per key. When `many` is true, an array of matching documents is returned; otherwise the first match (or `null`). Joins are batched per relation to avoid N+1 lookups, and results are cached per collection until `joinCacheMaxEntries` or `joinCacheTTLms` thresholds are hit. Call `clearJoinCache()` to flush the cache manually.

## Binary Storage

The binary store lets you persist blobs alongside documents:

```ts
const metadata = await db.saveBinary(Buffer.from('hello world'), { mimeType: 'text/plain' });

await db.insert('files', {
  name: 'hello.txt',
  _binRefs: [{ field: 'content', sha256: metadata.sha256, mimeType: metadata.mimeType }]
});

const buffer = await db.readBinary(metadata.sha256);
await db.deleteBinary(metadata.sha256);
```

- `saveBinary(data, options?)` stores a buffer/string/typed array, returning `BinaryMetadata` (size, mime type, dedupe info, timestamps).
- `readBinary(sha256)` retrieves the original bytes or `null` if missing.
- `deleteBinary(sha256, { force })` decrements the reference count and deletes the blob when it is no longer referenced. Use `force: true` to bypass reference tracking.

When `dedupeBinaries` is enabled (default), the database hashes incoming data and stores only unique payloads.

## Custom Types

Custom field types allow domain-specific validation and comparison logic. Four helpers are provided by default: `int`, `float`, `decimal`, and `varchar`. Override them or register new ones:

```ts
import { createDefaultCustomTypes } from 'f9-db';

const customTypes = createDefaultCustomTypes({
  slug: {
    name: 'slug',
    baseType: 'string',
    accepts: ['string'],
    fromInput(value) {
      if (typeof value !== 'string' || !/^[a-z0-9-]+$/.test(value)) {
        throw new Error('slug must be lowercase alphanumeric or "-"');
      }
      return value;
    },
    toComparable: (value) => value,
    project: (value) => value
  }
});

const db = createDatabase({ customTypes });
```

Each `CustomTypeDefinition` implements:

- `fromInput(value, options?)`: normalize and validate user input.
- `toComparable(value)`: convert stored values into something comparable for sorting/filtering.
- `compare(left, right)`: optional custom comparator when lexical comparison is insufficient (e.g., decimals).
- `project(value)`: shape the value returned to callers.
- `accepts`: describe valid user inputs for docs/errors.

See `docs/custom-types.md` for deep-dives and extension tips.

## Logging and Diagnostics

The default logger appends JSON entries to `${logDir}/app.log` with `{ level, time, msg, context }`. You can override all logging by passing a `Logger` with any subset of `debug`, `info`, `warn`, and `error`. Additional observability hooks:

- `FindOptions.diagnostics`: inspect scan statistics.
- JSON manifest/index files capture metadata for offline inspection (`data/manifest.json`, `data/indexes/**`).
- Streams expose row-level data for ingestion tools without buffering entire collections.

## Error Handling

Every method rejects with meaningful errors (e.g., validation failures, duplicate key violations, lock timeouts). All errors are plain `Error` instances with human-friendly messages; you can pattern-match via `.message` or inspect `code` when provided (e.g., `'ELOCKTIMEOUT'`).

## Further Reading

- `README.md`: feature overview, architecture, and roadmap.
- `docs/sql-to-f9-db.md`: SQL-to-f9 mapping guide with schema translation tips.
- `docs/custom-types.md`: implementation details for custom field types.
