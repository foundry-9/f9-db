# f9-db — JSON/JSONL File Database for Node 22.x

File-backed database intended as a drop-in for Node/Express/Next.js/Electron backends. Uses append-only JSONL logs, periodic snapshots, and simple inverted indexes for fast string search. Responses are always JSON or JSONL; logging is built-in.

## What This Is

- Pure Node 22.x (ESM) library, no native deps.
- Collections stored as JSONL + compacted snapshots on disk.
- Indexes for string fields to speed searches; optional prefix indexes.
- Binaries stored in a dedicated directory and referenced from documents.
- Supports relational-style joins to return nested objects/arrays.
- Streams large results as JSONL; regular queries return JSON.
- Structured JSON logging everywhere.

## Why (Quilltap Context)

Quilltap’s future direction includes moving off Postgres, favoring a portable internal database with secure local/S3-backed storage and per-user encryption. This project provides the lightweight JSON/JSONL core and binary store that can run locally, queue updates, and later layer on encryption and multi-user key handling.

## High-Level Architecture

- **Append-only logs** per collection: `data/<collection>.jsonl`.
- **Snapshots**: `data/<collection>.snapshot.json` to compact logs and speed startup.
- **Indexes**: `data/indexes/<collection>/<field>.json` inverted index (tokens → ids); incremental updates + rebuild hooks.
- **Manifest**: `data/manifest.json` stores schemas/index definitions/checkpoints.
- **Binary store**: `binaries/<sha256>` with references in docs (e.g., `_binRefs`).
- **Logs**: JSON logs in `logs/app.log` (rotated) or user-supplied logger.

## API Sketch

- `createDatabase(opts)` → db
  - Options: `dataDir`, `binaryDir`, `log` (custom logger), `autoCompact`, `fsync` mode, tokenizer/index config, limits.
- CRUD: `insert(collection, doc)`, `get(collection, id)`, `update(collection, id, mutation)`, `remove(collection, id)`.
- Query: `find(collection, filter, opts)` (JSON result), `stream(collection, filter, opts)` (AsyncIterator yielding JSONL).
- Index mgmt: `ensureIndex(collection, field, options)`, `rebuildIndex`.
- Relations: `join(collection, doc, relations)` resolves foreign refs into nested objects/arrays with batching to avoid N+1.

## Data & Query Behavior

- Writes append to log; optional fsync per write or batched.
- Compaction folds logs into snapshots, drops tombstones/stale versions, refreshes indexes, and records checkpoints in manifest.
- Indexed queries intersect posting lists; fall back to streaming scan when no index applies.
- Sorting in-memory for small result sets; bounded heap for streaming with limits.

## Logging & Observability

- Default logger emits `{level,time,msg,context}` as JSON.
- Trace logs for writes, index updates, compaction, and slow queries.
- Hooks for metrics counters/timers without external deps.

## Performance & Safety

- Streaming readers avoid loading full collections.
- LRU caches for hot documents and posting lists.
- Single-writer locks per collection; option to use `flock`/lockfile for multi-process safety.
- Crash recovery: replay snapshot + log; rebuild indexes that are behind.

## Binaries & Encryption (Quilltap Direction)

- Binary blobs saved under `binaries/` with sha256 names; docs store references.
- Future Quilltap-facing extensions: transparent encryption at rest (local/S3), per-user key model, queued sync to S3 after local edits, and dual-key derivation for multi-user support.

## Configuration Highlights

- Paths: `dataDir`, `binaryDir`, `logDir`
- Compaction: `autoCompact`, `compactInterval`, `maxLogBytes`
- Durability: `fsync` = `always | batch | never`
- Index: tokenizer, prefix length, optional stopwords
- Limits: max doc size, max result size, stream chunk size
- Serialization: custom `replacer`/`reviver`

## Testing Approach

- Unit: CRUD, index updates, tokenizer, tombstones, snapshots.
- Integration: startup recovery, rebuild index, joins, streaming JSONL.
- Perf smoke: ≥100k inserts and indexed queries under target latency.
- Concurrency: overlapping writes and crash simulation at mid-write.

## Roadmap

- Decide locking mechanism (lockfile vs `flock`).
- Finalize tokenizer defaults (stemming off by default).
- Binary deduplication policy (default on via sha256).
- Add examples for Express middleware and Next.js route handlers.
- Layer in S3-backed encrypted storage and per-user keys for Quilltap use cases.
