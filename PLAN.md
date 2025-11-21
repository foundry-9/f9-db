# JSON-Backed Node 22.x Database — Implementation Plan

## Goals

- File-backed database using only JSON/JSONL files plus a directory for binaries.
- Supports CRUD with optional relational-style lookups that return nested objects/arrays.
- Provides string indexing for fast search; tunable to balance speed vs. simplicity.
- Emits responses as JSON objects or streamed JSONL depending on query mode.
- Logging is always on (structured JSON logs).
- Targeted for drop-in use in Node.js 22.x backends (Express/Next.js/Electron/etc).

## Progress Notes

- Core schema validation implemented: schemas provided at init are stored in the manifest, defaults applied on insert, unexpected fields rejected, and type/constraint checks run on insert/update and during compaction.

## High-Level Design

- **Collections**: Each collection stored as append-only JSONL log (`data/<collection>.jsonl`) plus compacted snapshot (`data/<collection>.snapshot.json`). Each line is `{_id, data, meta}` for writes, or `{_id, tombstone:true}` for deletes.
- **Indexes**: Per-collection index files in `data/indexes/<collection>/<field>.json`. String fields use an inverted index map of normalized tokens → sorted array of `_id`s. A small LRU cache holds hot posting lists in-memory. Composite indexes supported by storing joined field tokens. Unique constraints are backed by their corresponding indexes.
- **Binary store**: Opaque binaries stored under `binaries/` as hashed filenames (e.g., `<sha256>`), with references from documents (e.g., `_binRefs: [{field, sha256, size, mime}]`). Deduplication on by default (sha256); opt-out supported.
- **Metadata**: `data/manifest.json` maintains rigid collection schemas, constraints, index definitions, last compaction offsets, and manifest/schema versions.
- **API Surface (ES module)**:
  - `db = createDatabase(opts)`; opts include `dataDir`, `binaryDir`, `log`, `serializer`.
  - CRUD: `insert(collection, doc)`, `get(collection, id)`, `update(collection, id, mutation)`, `remove(collection, id)`.
  - Query: `find(collection, filter, opts)`, `stream(collection, filter, opts) -> AsyncIterator` (yields JSONL).
  - Index mgmt: `ensureIndex(collection, field, options)`, `rebuildIndex`.
  - Relational helpers: `join(collection, doc, relations)` that pulls referenced docs into nested shapes with configurable cache size/TTL and manual `clearJoinCache`.
- **Output formats**:
  - Default `find` returns JSON array/object.
  - `stream`/`export` returns UTF-8 JSONL chunked results for large scans.
- **Logging**: Pluggable logger; default writes JSON logs to `logs/app.log` with levels. Hookable via `opts.log`.

## File/Directory Layout

- `data/` — root data dir (configurable).
- `data/<collection>.jsonl` — append-only operation log.
- `data/<collection>.snapshot.json` — compacted full state (periodic).
- `data/indexes/<collection>/<field>.json` — inverted index for string fields; may store bloom filter metadata.
- `data/manifest.json` — collection/index metadata, schema/manifest versions, + compaction checkpoints.
- `binaries/` — hashed binary blobs.
- `logs/app.log` — JSON logs (rotated).

## Schema & Constraints

- **Schema model**: Every collection/table declares a rigid schema with defined fields and types. The only flexible field type is `json` (opaque payload); all other fields must be declared (string, number, boolean, date/iso-string, binary ref, array/object of typed fields).
- **Constraints**:
  - Required/not-null fields validated on write.
  - Type validation enforced on insert/update; rejects mismatches.
  - String constraints: min/max length, regex, enum.
  - Number constraints: min/max, integer-only flag.
  - Array constraints: item type validation; min/max length.
  - Object constraints: nested rigid schema unless type is `json`.
  - Uniqueness: per-field or composite unique constraints enforced via index + manifest metadata.
  - Defaults: applied on insert when omitted.
- **Schema storage**: Manifest stores per-collection schema, constraints, defaults, indexes, and unique keys. Versioning supports migrations.
- **Validation timing**: On insert/update; compaction re-validates and can emit warnings for corrupt rows before repairing.

## Data & Index Strategy

- **Append-only log** for durability and fast writes; fsync optional per write or batched.
- **Snapshot/compaction**:
  - Periodically fold log into snapshot to drop tombstones and stale versions.
  - Record last processed offset in manifest; use it to resume on startup.
- **Indexes**:
  - Tokenizer: lowercase, split on non-alphanumerics, min length 2; stemming off by default; allow user hook for custom tokenizers.
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

- Single-writer lock per collection file using JS lockfile with retry/backoff for portability.
- Multi-process safety: optional `flock` mode via feature flag (`lockMode: "lockfile" | "flock"`, default `lockfile`).
- Crash recovery: on startup, replay from snapshot + log; rebuild indexes that are behind.

## Configuration Options

- `dataDir`, `binaryDir`, `logDir`
- `autoCompact` (boolean/threshold), `compactInterval`, `maxLogBytes`
- `fsync` mode: `always` | `batch` | `never` (with risk)
- `index`: tokenizer settings (min length 2, lowercase, no stemming), prefix length, stopwords (optional), custom tokenizer hook
- `manifestVersion` (start 1) and per-collection schema/index versioning
- `limits`: max document size, max result size, stream chunk size
- `serialization`: custom replacer/reviver for JSON
- `dedupeBinaries`: default true (sha256-based), can disable

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

## Decisions Recorded

- Locking: default JS lockfile with retry/backoff; optional `flock` mode via `lockMode`.
- Tokenization: lowercase + non-alphanumerics split, min length 2, no stemming by default; custom tokenizer hook allowed.
- Binary deduplication: default on via sha256 hashing with option to disable.
- Manifest versioning: include `manifestVersion` (start at 1) and per-collection `schemaVersion`/index versions for migrations.
