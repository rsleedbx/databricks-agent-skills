# Query-based connectors

Query-based connectors (QBC) ingest from databases that have no CDC feed configured, or where you don't want to set one up. Instead of reading a change log, the connector runs a query on each scheduled run and pulls the rows whose cursor value moved past the last high-water mark.

Supported sources (foreign connection): **Oracle, Teradata, SQL Server, MySQL, MariaDB, PostgreSQL**. Any Lakehouse Federation source is also supported through a foreign catalog. Status: Public Preview.

## How it differs from the gateway pattern

QBC is **not** the gateway pattern in [standard-cdc-gateway.md](standard-cdc-gateway.md):

| | CDC (SQL Server, PG CDC, MySQL CDC) | Query-based |
|---|---|---|
| Pipelines | Integrated CDC: one. Standard CDC: gateway + ingestion. Compute per the current docs and the workspace's policy | One: serverless ingestion |
| Staging Volume | Yes | No |
| Source setup | CT / CDC / logical replication / binlog | None — a `SELECT` grant and a cursor column |
| Change fidelity | Every change event | Latest state of each changed row at run time |
| Deletes | Captured from the log | Soft deletes via `deletion_condition`; hard deletes Beta |
| Source load | Log reads | A query per table per run |
| Network path | The compute that reads the change log (integrated pipeline or gateway, classic or serverless) → source | **Serverless compute** → source |

Pick QBC when you can't (or don't want to) turn on CDC at the source, or for demos where speed of setup matters. Pick CDC when you need every intermediate change, hard deletes, or minimal source load.

---

## How a run works

1. **First run** — full snapshot of each table.
2. **Later runs** — `SELECT ... WHERE <cursor> > <high-water mark>`. The new high-water mark is saved after a successful run.
3. **Lookback window** — for `SCD_TYPE_1` and `SCD_TYPE_2` with a timestamp cursor, each run also re-reads a short window just below the high-water mark to catch rows committed late, then reconciles by primary key. `APPEND_ONLY` skips the window.

Rows that change several times between two runs land once, in their latest state. If the demo or use case needs every intermediate version, use CDC.

---

## Cursor column rules

The cursor drives correctness. Get it wrong and changes are silently missed.

- **Exactly one column.** Composite cursors are not supported.
- **Prefer a timestamp** that is bumped on every insert *and* update (e.g. `updated_at`).
- **A numeric cursor works if it moves on every change**, for example a version column set from a sequence by an insert/update trigger. An auto-increment ID only moves on insert, so it suits append-only tables: an update that leaves the ID unchanged is never picked up.
- **Values must never decrease.**
- **NULL cursor rows are skipped after the first run.** The initial snapshot loads them, but a row inserted or left with a NULL cursor later is never picked up. Make the column `NOT NULL` with a default.
- **No usable cursor = full load every run.** That also reflects hard deletes, at the cost of reading the whole table each time.
- **The field is `cursor_columns` (a list), even with one column.** Some docs spell it `cursor_column`, but the API doesn't know that field: the CLI prints `Warning: unknown field: cursor_column`, drops it, and creates the pipeline **without a cursor**, so every run silently becomes a full load. After creating a pipeline, check `databricks pipelines get <id>` shows `query_based_connector_config.cursor_columns`.

On Postgres, a `DEFAULT now()` column only covers inserts. Updates need a trigger:

```sql
ALTER TABLE public.orders
  ADD COLUMN updated_at timestamptz NOT NULL DEFAULT now();

CREATE OR REPLACE FUNCTION public.set_updated_at() RETURNS trigger AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER orders_set_updated_at
  BEFORE UPDATE ON public.orders
  FOR EACH ROW EXECUTE FUNCTION public.set_updated_at();
```

Add an index on the cursor column on large tables so the incremental query doesn't scan the whole table.

---

## What each source shape does with each target

On Postgres (Lakebase) through a UC connection, with inserts, updates, and deletes between two runs:

| Source shape | `SCD_TYPE_1` | `SCD_TYPE_2` | `APPEND_ONLY` |
|---|---|---|---|
| Primary key + timestamp cursor (bumped on update) | ✅ inserts, updates, soft deletes | ✅ new version per change; soft delete closes the version | ✅ every insert, update, and soft delete appended as a new row |
| Primary key + numeric cursor (bumped on update) | ✅ | ✅ | ✅ |
| Unique key, no primary key (declare the unique column as `primary_keys`) | ✅ | ✅ | ✅ |
| No key + cursor, `primary_keys` omitted | ❌ runtime error `CANNOT_INGEST_TABLE_WITHOUT_PRIMARY_KEY` | ❌ same | ✅ (duplicates kept) |
| No key, **no cursor**, `primary_keys` omitted | ✅ full replace each run | Not verified | ❌ rejected at create: "APPEND_ONLY can only be used for incremental ingestion ... cursor_columns must be set" |
| Primary key, no cursor | ✅ full load each run, including hard deletes | Not verified | ❌ needs a cursor |
| Declared key that isn't unique / has NULLs | ⚠️ **accepted, silent data loss**: source rows are lost, and new rows overwrite existing ones | ⚠️ same risk | n/a |
| Cursor set on insert only (no update trigger) | ⚠️ **updates never arrive**, no warning | ⚠️ same | ⚠️ same |
| Cursor with NULLs | ⚠️ snapshot loads them; later NULL-cursor rows are skipped | ⚠️ same | ⚠️ same |
| Hard deletes, `hard_deletion_sync_min_interval_in_seconds` set | ✅ deleted rows removed | not supported (per docs) | n/a |

The two most common shapes were also run on PostgreSQL 16, MySQL 8.4, SQL Server 2022 and Oracle 23ai Free (the same tables on each, through a UC connection to a cloud VM): a primary key + timestamp cursor bumped by a trigger under `SCD_TYPE_1`, and a keyless table + cursor under `APPEND_ONLY`. On the keyless table, an update appends a new row. The results were identical across engines, with the updates applied.

Through a UC connection, a table with no key gets no synthetic key from all columns: SCD without `primary_keys` fails. Declare a key, or use `APPEND_ONLY`.

---

## Pre-flight checks before creating the pipeline (advise; the product doesn't check)

The product accepts whatever you declare. A `primary_keys` value that isn't unique, a cursor without an index, and a cursor full of NULLs all pass create and run as COMPLETED with no warning. You already have the schema from introspection ([source-sql-through-connection.md](source-sql-through-connection.md)), so check each table's planned configuration and tell the user what you find **before** creating anything:

| Check | How (from the introspection results, or one `remote_query` for all tables) | If it fails, tell the user |
|---|---|---|
| Declared key is a real primary key or unique constraint | The key columns match a `primary key` or `unique` row in the keys-and-indexes result | "`<cols>` isn't a primary key or unique constraint on `<table>`." Then check the data: `count(*)`, `count(DISTINCT <key>)`, and `SUM(CASE WHEN <key> IS NULL THEN 1 ELSE 0 END)`. Duplicates or NULLs mean **silent data loss** under SCD1/SCD2. |
| Cursor column has an index | An `index`, `unique`, or `primary key` row whose first column is the cursor | "`<cursor>` has no index on `<table>`: every incremental run scans the whole table." |
| Cursor has no NULLs | `SUM(CASE WHEN <cursor> IS NULL THEN 1 ELSE 0 END)` | "N rows have a NULL cursor: the first snapshot loads them, but later changes to them are missed." |
| No duplicate keys | For multi-column key `<k1>, <k2>`: `SELECT COUNT(*) FROM (SELECT <k1>, <k2> FROM <table> GROUP BY <k1>, <k2> HAVING COUNT(*) > 1) d` returns 0 | "N rows have duplicate key values: the pipeline will overwrite them with silent data loss." |
| Cursor moves on update | Postgres: an update trigger that sets it; MySQL: `on update` in `extra`; SQL Server: a trigger or the app sets it | "Updates won't be picked up unless `<cursor>` changes on every update." |
| No key | No `primary key` / `unique` row | Offer `APPEND_ONLY` (needs a cursor), or SCD1 with no cursor (full load each run). SCD1/SCD2 with a cursor need a declared key. |

Run the data checks for all tables in one query (`UNION ALL`), not one table at a time.

**The user can skip these.** Query-based ingestion is often used just to snapshot a table, or for a quick test, where a fake key or a missing index doesn't matter. Report the findings once, say what goes wrong in production, and go ahead if the user says so. Don't ask them to add indexes or constraints to the source; that's their call, and it changes their database.

---

## Deletes

- **Soft deletes (supported):** keep the row, flag it, and set `deletion_condition` so the connector applies it as a delete in the destination:
  ```yaml
  query_based_connector_config:
    cursor_columns: [updated_at]
    deletion_condition: "is_deleted = true"
  ```
  The flag update must also bump the cursor, or the connector never sees it.
- **Hard deletes:** Beta, enabled through API configuration. A plain `DELETE FROM` at the source is otherwise invisible to a query-based connector. See the [connector reference](https://docs.databricks.com/ingestion/lakeflow-connect/).
