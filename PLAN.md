# JSON-Backed Node 22.x Database — Implementation Plan

## Goals

- File-backed database using only JSON/JSONL files plus a directory for binaries.
- Supports CRUD with optional relational-style lookups that return nested objects/arrays.
- Provides string indexing for fast search; tunable to balance speed vs. simplicity.
- Emits responses as JSON objects or streamed JSONL depending on query mode.
- Logging is always on (structured JSON logs).
- Targeted for drop-in use in Node.js 22.x backends (Express/Next.js/Electron/etc).

## High-Level Design

- **Collections**: Each collection stored as append-only JSONL log (`data/<collection>.jsonl`) plus compacted snapshot (`data/<collection>.snapshot.json`). Each line is `{_id, data, meta}` for writes, or `{_id, tombstone:true}` for deletes.
- **Indexes**: Per-collection index files in `data/indexes/<collection>/<field>.json`. String fields use an inverted index map of normalized tokens → sorted array of `_id`s. A small LRU cache holds hot posting lists in-memory. Composite indexes supported by storing joined field tokens.
- **Binary store**: Opaque binaries stored under `binaries/` as hashed filenames (e.g., `<sha256>`), with references from documents (e.g., `_binRefs: [{field, sha256, size, mime}]`).
- **Metadata**: `data/manifest.json` maintains collection schemas (optional), index definitions, and last compaction offsets.
- **API Surface (ES module)**:
  - `db = createDatabase(opts)`; opts include `dataDir`, `binaryDir`, `log`, `serializer`.
  - CRUD: `insert(collection, doc)`, `get(collection, id)`, `update(collection, id, mutation)`, `remove(collection, id)`.
  - Query: `find(collection, filter, opts)`, `stream(collection, filter, opts) -> AsyncIterator` (yields JSONL).
  - Index mgmt: `ensureIndex(collection, field, options)`, `rebuildIndex`.
  - Relational helpers: `join(collection, doc, relations)` that pulls referenced docs into nested shapes.
- **Output formats**:
  - Default `find` returns JSON array/object.
  - `stream`/`export` returns UTF-8 JSONL chunked results for large scans.
- **Logging**: Pluggable logger; default writes JSON logs to `logs/app.log` with levels. Hookable via `opts.log`.

## File/Directory Layout

- `data/` — root data dir (configurable).
- `data/<collection>.jsonl` — append-only operation log.
- `data/<collection>.snapshot.json` — compacted full state (periodic).
- `data/indexes/<collection>/<field>.json` — inverted index for string fields; may store bloom filter metadata.
- `data/manifest.json` — collection/index metadata + compaction checkpoints.
- `binaries/` — hashed binary blobs.
- `logs/app.log` — JSON logs (rotated).

## Data & Index Strategy

- **Append-only log** for durability and fast writes; fsync optional per write or batched.
- **Snapshot/compaction**:
  - Periodically fold log into snapshot to drop tombstones and stale versions.
  - Record last processed offset in manifest; use it to resume on startup.
- **Indexes**:
  - Tokenizer: lowercase, split on non-alphanumerics, optional min length; store tokens.
  - Structure: `{ token: [id1, id2, ...] }` persisted as JSON with chunked arrays to avoid huge single strings.
  - Optional **prefix index**: store leading `N` chars token for starts-with queries.
  - Rebuild on demand or during compaction; incremental updates on CRUD.
- **Query execution**:
  - Use index when filter includes indexed string field; intersect posting lists for multi-field AND.
  - Fallback to scan (streaming reader) with predicate when no index matches.
  - Sorting via in-memory comparator on result set or streaming with bounded heap for `limit`.
- **Relational ability**:
  - Documents may include foreign keys (e.g., `_ref: {collection, id}` or arrays).
  - `join()` resolves refs and returns nested objects; batching fetches to avoid N+1.
  - Optionally cache referenced docs during a request to speed repeated joins.

## Logging & Observability

- Default logger: structured JSON (`{level,time,msg,context}`). Levels: debug/info/warn/error.
- Emit trace logs for writes, index updates, compaction, and slow queries (>threshold).
- Add metrics hooks (callbacks) to report counts/durations; no external deps required.

## Concurrency/Safety

- Single-writer lock per collection file (advisory via `fs` file lock or mutex in process).
- Multi-process safety: use `flock`/lockfile where supported; otherwise warn and default to single-process usage.
- Crash recovery: on startup, replay from snapshot + log; rebuild indexes that are behind.

## Configuration Options

- `dataDir`, `binaryDir`, `logDir`
- `autoCompact` (boolean/threshold), `compactInterval`, `maxLogBytes`
- `fsync` mode: `always` | `batch` | `never` (with risk)
- `index`: tokenizer settings, prefix length, stopwords (optional)
- `limits`: max document size, max result size, stream chunk size
- `serialization`: custom replacer/reviver for JSON

## Performance Plan

- Keep files small via compaction; chunk index files to stay under a few MB each.
- Use streaming readers for scans; avoid loading whole collection in memory.
- LRU cache for hot documents and index posting lists.
- Node 22 features: `fs/promises` with `FileHandle.read/writev`, `structuredClone` for snapshots, `AbortSignal` for cancelable queries, `test runner` for tests.

## Testing Plan

- Unit: CRUD ops, index updates, snapshots, tombstone handling, tokenizer correctness.
- Integration: startup recovery from snapshot+log, rebuild index, join helper, streaming JSONL.
- Performance smoke: insert 100k docs; ensure index query under target latency.
- Concurrency: overlapping writes rejected/queued; crash simulation (truncate mid-write).

## Rollout Plan

- Ship as small NPM package (ESM), zero native deps.
- Provide TypeScript types via `d.ts`.
- Add examples for Express middleware and Next.js route handler.

## Open Questions / Decisions

- Locking mechanism choice (pure JS lockfile vs. advisory `flock`).
- Index tokenization: include stemming? (default no, keep simple).
- Binary deduplication: default on via sha256 hash comparison.
- Manifest format versioning to allow future migrations.
