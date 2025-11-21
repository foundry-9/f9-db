# Custom Field Types

Custom types let you define how values are ingested, stored, compared, and projected out of the database. They are registered on the database instance and referenced from collection schemas with `type: 'custom'` + `customType`.

## Lifecycle

- `fromInput(value, options)` — normalize/validate incoming values (user input or defaults) into the internal representation that is persisted.
- `toComparable(value, options)` — optional. Convert the stored value to a primitive for filtering, sorting, and aggregations.
- `compare(left, right, options)` — optional comparator when lexical/number comparison is not enough (e.g., decimals that must sort numerically even though they are stored as strings).
- `project(value, options)` — optional. Transforms the stored value into what queries return. Defaults to the internal representation.

Indexes operate on the stored value. Inverted indexes still tokenize strings only; non-string custom types fall back to scan/unique-check without an index. If you need to index objects, store a canonical JSON string (e.g., via `fromInput`).

## Built-in custom types

- `int`: accepts integers as numbers or digit strings, stored as a safe integer number, projected as a number.
- `float`: accepts finite floats as numbers or strings, stored as a number, projected as a number.
- `decimal`: accepts plain decimal strings (or finite numbers without exponents), stored as a canonical string, compared with a decimal-safe comparator, and projected as `Number.parseFloat(...)`. Use strings for large/precise values to avoid JS float rounding.
- `varchar`: string with optional `options.maxLength`, stored and projected as a string.

`createDefaultCustomTypes` exposes the built-ins; `createDatabase` uses them automatically and merges any overrides you pass via `customTypes`.

## Using built-ins in a schema

```ts
import { createDatabase } from 'f9-db';

const db = createDatabase({
  schemas: {
    accounts: {
      fields: {
        // canonical decimal string stored; projections and sorting use numeric order
        balance: { type: 'custom', customType: 'decimal', required: true },
        // enforce max length but otherwise behaves like string
        code: { type: 'custom', customType: 'varchar', options: { maxLength: 12 } },
        // coerces to a safe integer even when passed as a string
        visits: { type: 'custom', customType: 'int', default: 0 }
      }
    }
  }
});
```

## Defining your own custom type

Pass `customTypes` to `createDatabase` to add or override definitions:

```ts
import { createDatabase, createDefaultCustomTypes } from 'f9-db';

const db = createDatabase({
  customTypes: createDefaultCustomTypes({
    uuid: {
      name: 'uuid',
      description: 'Lowercase canonical UUID',
      baseType: 'string',
      fromInput: (value) => {
        const text = String(value).toLowerCase();
        if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(text)) {
          throw new Error('Invalid uuid');
        }
        return text;
      },
      toComparable: (value) => value, // lexical comparisons and indexes use this string
      project: (value) => value
    }
  }),
  schemas: {
    users: { fields: { id: { type: 'custom', customType: 'uuid', required: true } } }
  }
});
```

Guidance:

- Normalize aggressively in `fromInput` (strip whitespace, enforce casing, canonicalize JSON) because that representation is what gets written to disk and indexed.
- Provide `toComparable` or `compare` when you need stable ordering beyond default string/number semantics (decimals, semantic versions, etc.).
- Use `project` to return a different shape than what is stored (e.g., store decimals as strings for precision but project them as numbers, or unwrap stored `{ raw, display }` objects).
- If you need indexed objects, canonicalize them into a string (e.g., `JSON.stringify` with stable key ordering) in `fromInput` so the inverted index can tokenize it.
