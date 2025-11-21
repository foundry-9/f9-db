# SQL to f9-db: Query Equivalents

Guidance for SQL users on how to express familiar SELECT queries with f9-db. Examples assume an initialized database instance:

```ts
import { createDatabase } from 'f9-db';

const db = createDatabase({ dataDir: './data', binaryDir: './binaries' });
```

## Quick Mappings

- `SELECT ... FROM table` → `db.find('table', filter?, options?)`
- `WHERE` predicates → `filter` object with `$gt/$gte/$lt/$lte/$between/$in/$nin/$like/$ilike/$isNull/$exists/$not`, plus `$and/$or` for boolean logic
- `IN (..)` → array shorthand (`{ city: ['Paris', 'Berlin'] }`) or `{ city: { $in: [...] } }`
- `ORDER BY` → `options.sort` (`{ createdAt: -1 }` for DESC, `1` for ASC)
- `LIMIT` / `OFFSET` → `limit` / `skip`
- Column selection → `projection: ['name', 'profile.city']` (always returns `_id`)
- `GROUP BY` / aggregates → `groupBy` + `aggregates` on `find`
- `ROW_NUMBER() OVER (PARTITION BY ...)` → `partitionBy` + `rowNumber` on `find`
- `JOIN` → fetch docs, then `db.join(collection, doc, relations)` to resolve foreign keys into nested objects/arrays
- `DELETE FROM ... WHERE ...` → `db.removeWhere('table', filter?, { sort?, limit?, skip? })` (returns deleted docs)
- `CREATE INDEX` → `db.ensureIndex(collection, field, { unique?: true })`
- `UPDATE ... SET ... WHERE ...` → `db.updateWhere('table', mutation|((doc) => mutation), filter, { sort?, limit?, skip? })` (use `update(id, patch)` when you already have the `_id`)

## Simple SELECT Examples

**Basic filter**

SQL

```sql
SELECT * FROM users WHERE name = 'Ada';
```

f9-db

```ts
await db.find('users', { name: 'Ada' });
```

**Projection + pagination**

SQL

```sql
SELECT name, city FROM users WHERE age > 30 ORDER BY age DESC LIMIT 10 OFFSET 5;
```

f9-db

```ts
await db.find(
  'users',
  { age: { $gt: 30 } },
  { projection: ['name', 'city'], sort: { age: -1 }, limit: 10, skip: 5 }
);
```

**LIKE/ILIKE and NULL checks**

SQL

```sql
SELECT * FROM users WHERE (name ILIKE 'a%' OR city IS NULL) AND age BETWEEN 30 AND 40;
```

f9-db

```ts
await db.find('users', {
  $and: [
    { $or: [{ name: { $ilike: 'a%' } }, { city: { $isNull: true } }] },
    { age: { $between: [30, 40] } }
  ]
});
```

**IN list**

SQL

```sql
SELECT * FROM users WHERE city IN ('London', 'Lisbon');
```

f9-db

```ts
await db.find('users', { city: ['London', 'Lisbon'] });
// equivalent: { city: { $in: ['London', 'Lisbon'] } }
```

## UPDATE Equivalents

Use `updateWhere` to mirror SQL’s `UPDATE ... WHERE ...` semantics. It returns the updated docs (array). Pass a plain object for static patches or a function if the new values depend on the current row.

**Basic UPDATE**

SQL

```sql
UPDATE orders SET status = 'complete', reviewed_at = NOW() WHERE status = 'pending';
```

f9-db

```ts
await db.updateWhere('orders', {
  status: 'complete',
  reviewedAt: new Date().toISOString()
}, { status: 'pending' });
```

**UPDATE with ORDER BY/LIMIT and computed values**

SQL

```sql
UPDATE users SET score = score + 1 WHERE city = 'London' ORDER BY score ASC LIMIT 1;
```

f9-db

```ts
const [bumped] = await db.updateWhere(
  'users',
  (user) => ({ score: (user.score as number) + 1 }),
  { city: 'London' },
  { sort: { score: 1 }, limit: 1 }
);
// bumped contains the updated doc with the lowest previous score in London
```

## DELETE Equivalents

Use `removeWhere` to mirror SQL’s `DELETE FROM ... WHERE ...` semantics. It deletes matching documents, respects optional `sort`, `limit`, and `skip`, and returns the removed rows for auditing or cleanup logic.

**Basic DELETE**

SQL

```sql
DELETE FROM orders WHERE status = 'pending';
```

f9-db

```ts
await db.removeWhere('orders', { status: 'pending' });
```

**DELETE with ORDER BY/LIMIT**

SQL

```sql
DELETE FROM users WHERE city = 'London' ORDER BY score ASC LIMIT 1;
```

f9-db

```ts
const [deleted] = await db.removeWhere(
  'users',
  { city: 'London' },
  { sort: { score: 1 }, limit: 1 }
);
// deleted contains the single doc that was removed
```

## Aggregations and Windows

**GROUP BY with aggregates**

SQL

```sql
SELECT status, COUNT(*) AS order_count, SUM(amount) AS total
FROM orders
GROUP BY status
ORDER BY status;
```

f9-db

```ts
await db.find('orders', {}, {
  groupBy: ['status'],
  aggregates: {
    order_count: { op: 'count' },
    total: { op: 'sum', field: 'amount' }
  },
  sort: { status: 1 }
});
```

**ROW_NUMBER() window**

SQL

```sql
SELECT *, ROW_NUMBER() OVER (PARTITION BY team ORDER BY score DESC) AS row_number
FROM users
ORDER BY team, score DESC;
```

f9-db

```ts
await db.find('users', {}, {
  partitionBy: ['team'],
  rowNumber: { as: 'row_number', orderBy: { score: -1 } },
  sort: { team: 1, score: -1 }
});
```

## Resolving Relations (JOIN-like)

Fetch records, then hydrate referenced docs with `join` to mimic SQL joins.

**Single JOIN**

SQL

```sql
SELECT p.title, u.name AS author_name
FROM posts p
JOIN users u ON p.author_id = u.id
WHERE p.id = 'post-1';
```

f9-db

```ts
const post = await db.get('posts', 'post-1');
const hydrated = await db.join('posts', post!, {
  author: {
    localField: 'authorId',
    foreignCollection: 'users',
    projection: ['name']
  }
});
// hydrated.author → { _id, name }
```

**JOIN with array of foreign keys**

SQL

```sql
SELECT p.title, array_agg(r.name) AS reviewer_names
FROM posts p
JOIN users r ON r.id = ANY (p.reviewer_ids)
WHERE p.status = 'ready'
ORDER BY p.created_at DESC
LIMIT 20;
```

f9-db

```ts
const posts = await db.find('posts', { status: 'ready' }, { sort: { createdAt: -1 }, limit: 20 });
const hydrated = await Promise.all(
  posts.map((post) =>
    db.join('posts', post, {
      reviewers: {
        localField: 'reviewerIds',
        foreignCollection: 'users',
        many: true,
        projection: ['name']
      }
    })
  )
);
```

## Streaming Large SELECTs

For result sets that would be a long `SELECT` in SQL, stream JSONL instead of materializing everything:

```ts
for await (const line of db.stream('events', { createdAt: { $gte: '2024-01-01' } }, { sort: { createdAt: 1 } })) {
  const event = JSON.parse(line);
  // handle event
}
```

## Index Hints

Create indexes before running heavy filters, similar to SQL `CREATE INDEX`:

```ts
await db.ensureIndex('users', 'name');          // speeds up exact/LIKE/ILIKE on name
await db.ensureIndex('users', 'email', { unique: true }); // enforces unique constraint
```
