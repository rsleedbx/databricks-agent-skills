# Query-based connectors

Query-based connectors (QBC) ingest from databases that have no CDC feed configured, or where you don't want to set one up. Instead of reading a change log, the connector runs a query on each scheduled run and pulls the rows whose cursor value moved past the last high-water mark.

Supported sources (foreign connection): **Oracle, Teradata, SQL Server, MySQL, MariaDB, PostgreSQL**. Any Lakehouse Federation source is also supported through a foreign catalog. Status: Public Preview.

---

## How it differs from the gateway pattern

QBC is **not** the gateway pattern in [2-database-connectors.md](2-database-connectors.md):

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

## What each source shape does with each target (tested)

Tested on Postgres (Lakebase) through a UC connection, two runs with inserts, updates, and deletes in between. The fixture is in [databricks-agent-skills-tests](https://github.com/rsleedbx/databricks-agent-skills-tests) (`fixtures/lakebase-shapes`). Where this differs from the docs, it's what the product actually did.

| Source shape | `SCD_TYPE_1` | `SCD_TYPE_2` | `APPEND_ONLY` |
|---|---|---|---|
| Primary key + timestamp cursor (bumped on update) | ✅ inserts, updates, soft deletes | ✅ new version per change; soft delete closes the version | ✅ every insert, update, and soft delete appended as a new row |
| Primary key + numeric cursor (bumped on update) | ✅ | ✅ | ✅ |
| Unique key, no primary key (declare the unique column as `primary_keys`) | ✅ | ✅ | ✅ |
| No key + cursor, `primary_keys` omitted | ❌ runtime error `CANNOT_INGEST_TABLE_WITHOUT_PRIMARY_KEY` | ❌ same | ✅ (duplicates kept) |
| No key, **no cursor**, `primary_keys` omitted | ✅ full replace each run | not tested | ❌ rejected at create: "APPEND_ONLY can only be used for incremental ingestion ... cursor_columns must be set" |
| Primary key, no cursor | ✅ full load each run, including hard deletes | not tested | ❌ needs a cursor |
| Declared key that isn't unique / has NULLs | ⚠️ **accepted, silent data loss**: 102 source rows became 86, and new rows overwrote existing ones | ⚠️ same risk | n/a |
| Cursor set on insert only (no update trigger) | ⚠️ **updates never arrive**, no warning | ⚠️ same | ⚠️ same |
| Cursor with NULLs | ⚠️ snapshot loads them; later NULL-cursor rows are skipped | ⚠️ same | ⚠️ same |
| Hard deletes, `hard_deletion_sync_min_interval_in_seconds` set | ✅ deleted rows removed | not supported (per docs) | n/a |

The two most common shapes were also run on PostgreSQL 16, MySQL 8.4, SQL Server 2022 and Oracle 23ai Free (the same tables on each, through a UC connection to a cloud VM): a primary key + timestamp cursor bumped by a trigger under `SCD_TYPE_1`, and a keyless table + cursor under `APPEND_ONLY`. After 5 inserts and 3 updates, every engine landed 15 rows with the 3 updates applied. The keyless table got 5 new rows plus 1 appended row for its update. The results were identical across engines.

The docs say a table with no key gets a synthetic key from all columns. Through a UC connection it didn't: SCD without `primary_keys` failed. Declare a key, or use `APPEND_ONLY`.

---

## Pre-flight checks before creating the pipeline (advise; the product doesn't check)

The product accepts whatever you declare. A `primary_keys` value that isn't unique, a cursor without an index, and a cursor full of NULLs all pass create and run as COMPLETED with no warning. You already have the schema from introspection ([7-source-sql-through-connection.md](7-source-sql-through-connection.md)), so check each table's planned configuration and tell the user what you find **before** creating anything:

| Check | How (from the introspection results, or one `remote_query` for all tables) | If it fails, tell the user |
|---|---|---|
| Declared key is a real primary key or unique constraint | The key columns match a `primary key` or `unique` row in the keys-and-indexes result | "`<cols>` isn't a primary key or unique constraint on `<table>`." Then check the data: `count(*)`, `count(DISTINCT <key>)`, and `count_if(<key> IS NULL)`. Duplicates or NULLs mean **silent data loss** under SCD1/SCD2. |
| Cursor column has an index | An `index`, `unique`, or `primary key` row whose first column is the cursor | "`<cursor>` has no index on `<table>`: every incremental run scans the whole table." |
| Cursor has no NULLs | `count_if(<cursor> IS NULL)` | "N rows have a NULL cursor: the first snapshot loads them, but later changes to them are missed." |
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

---

## Step 0 — find the source tables through the connection

If the user names a connection but not the tables, look at the source through the connection with `remote_query`. It's read-only, needs no foreign catalog, and also proves that serverless can reach the source. Write each query to a `.sql` file with your file tool and run it with:

```bash
databricks experimental aitools tools query --file <file>.sql --profile <PROFILE> -o csv
```

Each query lists every table with its primary key and the date/time columns that could be the cursor. Pick tables that have a primary key and a NOT NULL cursor that changes on every update. Tables without a key need `APPEND_ONLY` (which needs a cursor) or SCD1 with no cursor (a full load each run). See [the shape table](#what-each-source-shape-does-with-each-target-tested) and run the [pre-flight checks](#pre-flight-checks-before-creating-the-pipeline-advise-the-product-doesnt-check) before creating the pipeline.

**PostgreSQL:** list databases first, then the tables in one database.

```sql
SELECT * FROM remote_query('<connection>', database => 'postgres', query => "SELECT datname FROM pg_database WHERE NOT datistemplate") ORDER BY 1
```

```sql
SELECT * FROM remote_query('<connection>', database => '<database>', query => "
  SELECT c.table_schema, c.table_name,
         (SELECT string_agg(k.column_name, ',' ORDER BY k.ordinal_position)
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage k
              ON k.constraint_schema = tc.constraint_schema AND k.constraint_name = tc.constraint_name
           WHERE tc.table_schema = c.table_schema AND tc.table_name = c.table_name
             AND tc.constraint_type = 'PRIMARY KEY') AS primary_key,
         string_agg(c.column_name || ' ' || c.data_type || CASE WHEN c.is_nullable = 'NO' THEN ' not null' ELSE '' END,
                    ', ' ORDER BY c.ordinal_position)
           FILTER (WHERE c.data_type LIKE 'timestamp%' OR c.data_type = 'date') AS cursor_candidates
    FROM information_schema.columns c
    JOIN information_schema.tables t
      ON t.table_schema = c.table_schema AND t.table_name = c.table_name AND t.table_type = 'BASE TABLE'
   WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema')
   GROUP BY c.table_schema, c.table_name")
ORDER BY 1, 2
```

**MySQL:** a MySQL database is the schema level, so there's no `database =>` argument. `on update` marks a column that MySQL bumps on every update, which makes it a good cursor.

```sql
SELECT * FROM remote_query('<connection>', query => "
  SELECT c.table_schema, c.table_name,
         group_concat(CASE WHEN c.column_key = 'PRI' THEN c.column_name END ORDER BY c.ordinal_position) AS primary_key,
         group_concat(CASE WHEN c.data_type IN ('timestamp', 'datetime', 'date')
                           THEN concat(c.column_name, ' ', c.column_type, IF(c.is_nullable = 'NO', ' not null', ''),
                                       IF(c.extra LIKE '%on update%', ' on update', '')) END
                      ORDER BY c.ordinal_position) AS cursor_candidates
    FROM information_schema.columns c
    JOIN information_schema.tables t
      ON t.table_schema = c.table_schema AND t.table_name = c.table_name AND t.table_type = 'BASE TABLE'
   WHERE c.table_schema NOT IN ('mysql', 'sys', 'information_schema', 'performance_schema')
   GROUP BY c.table_schema, c.table_name")
ORDER BY 1, 2
```

**SQL Server:** list databases on `master`, then the tables in one database. Run the table query once per database; the connection's login may not be able to open every database.

```sql
SELECT * FROM remote_query('<connection>', database => 'master', query => "SELECT name FROM sys.databases WHERE database_id > 4") ORDER BY name
```

```sql
SELECT * FROM remote_query('<connection>', database => '<database>', query => "
  SELECT s.name AS table_schema, t.name AS table_name,
         (SELECT STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal)
            FROM sys.indexes i
            JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
           WHERE i.object_id = t.object_id AND i.is_primary_key = 1) AS primary_key,
         (SELECT STRING_AGG(c.name + ' ' + ty.name + CASE WHEN c.is_nullable = 0 THEN ' not null' ELSE '' END, ', ')
                 WITHIN GROUP (ORDER BY c.column_id)
            FROM sys.columns c JOIN sys.types ty ON ty.user_type_id = c.user_type_id
           WHERE c.object_id = t.object_id
             AND ty.name IN ('datetime', 'datetime2', 'datetimeoffset', 'smalldatetime', 'date', 'rowversion', 'timestamp')) AS cursor_candidates
    FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id
   WHERE t.is_ms_shipped = 0") ORDER BY table_schema, table_name
```

**Oracle:** pass the service (in a multi-tenant database, the PDB, for example `FREEPDB1`) as `service_name =>`. Names come back in uppercase, and the pipeline needs them that way.

```sql
SELECT * FROM remote_query('<connection>', service_name => '<service>', query => "
  SELECT t.owner AS table_schema, t.table_name,
         (SELECT LISTAGG(cc.column_name, ',') WITHIN GROUP (ORDER BY cc.position)
            FROM all_constraints c JOIN all_cons_columns cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
           WHERE c.owner = t.owner AND c.table_name = t.table_name AND c.constraint_type = 'P') AS primary_key,
         (SELECT LISTAGG(col.column_name || ' ' || col.data_type || CASE WHEN col.nullable = 'N' THEN ' not null' END, ', ')
                 WITHIN GROUP (ORDER BY col.column_id)
            FROM all_tab_columns col
           WHERE col.owner = t.owner AND col.table_name = t.table_name
             AND (col.data_type = 'DATE' OR col.data_type LIKE 'TIMESTAMP%')) AS cursor_candidates
    FROM all_tables t
   WHERE t.owner NOT IN (SELECT username FROM all_users WHERE oracle_maintained = 'Y')") ORDER BY 1, 2
```

How these queries are written:

- **Outer string in double quotes,** so the source SQL can use single-quoted literals without escaping.
- **No `ORDER BY` inside `remote_query`.** It wraps the query in a subquery, and SQL Server rejects `ORDER BY` there. Sort in the outer query.
- **One file per query** and plain commands only. Don't loop over databases in a shell `for`: run one file per database.

---

## Step 1 — UC connection

A query-based pipeline points at a regular Unity Catalog connection (the same object Lakehouse Federation uses). Write the request body to a private file so the password never appears on a command line, then delete it:

```bash
umask 077
cat > conn.json <<EOF
{"name": "<connection>", "connection_type": "POSTGRESQL",
 "options": {"host": "<host>", "port": "5432", "user": "<user>", "password": "$(cat <user>.pw)"}}
EOF
databricks connections create --json @conn.json --profile <PROFILE> && rm conn.json
```

Per source (each tested end to end with a query-based pipeline):

| Source | `connection_type` | Notes |
|---|---|---|
| PostgreSQL | `POSTGRESQL` | |
| MySQL | `MYSQL` | |
| SQL Server | `SQLSERVER` | A server with a self-signed certificate (a VM or container install) needs `"trustServerCertificate": "true"` in `options`, or every query fails with `FAILED_JDBC.CONNECTION`. Managed services don't. |
| Oracle | `ORACLE` | `user` in uppercase (`C##LFC_USER` for a common user). No service in the connection: pass it per query (`service_name =>`) and per pipeline (`source_catalog`). |

Use a dedicated, read-only database user. Requires `CREATE CONNECTION` on the metastore. Grant `USE CONNECTION` to whoever creates the pipeline if that is a different principal. Check the connection with a Step 0 query before building the pipeline: `FAILED_JDBC.CONNECTION ... Failed to connect to the database` is the network path or, on SQL Server, the certificate.

Source-side grants (Postgres):

```sql
GRANT CONNECT ON DATABASE <db> TO <pg-user>;
GRANT USAGE ON SCHEMA public TO <pg-user>;
GRANT SELECT ON public.orders, public.customers TO <pg-user>;
```

---

## Step 2 — pipeline (foreign connection)

```yaml
resources:
  pipelines:
    pg_query_based:
      name: <base>                     # <firstname><lastname>-<connection>, see SKILL.md
      catalog: ${var.catalog}
      schema: pg_raw
      ingestion_definition:
        connection_name: pg_source_conn
        objects:
          - table:
              source_catalog: <pg-database>       # the Postgres database name
              source_schema: public
              source_table: orders
              destination_catalog: ${var.catalog}
              destination_schema: pg_raw
              table_configuration:
                scd_type: SCD_TYPE_1              # SCD_TYPE_1 | SCD_TYPE_2 | APPEND_ONLY
                primary_keys: [order_id]
                query_based_connector_config:
                  cursor_columns: [updated_at]
                  deletion_condition: "is_deleted = true"
          - table:
              source_catalog: <pg-database>
              source_schema: public
              source_table: customers
              destination_catalog: ${var.catalog}
              destination_schema: pg_raw
              table_configuration:
                scd_type: SCD_TYPE_2              # keeps history: __START_AT / __END_AT
                primary_keys: [customer_id]
                query_based_connector_config:
                  cursor_columns: [updated_at]
```

**How `source_catalog` and `source_schema` map per source** (each confirmed by a successful run):

| Source | `source_catalog` | `source_schema` |
|---|---|---|
| PostgreSQL | database | schema (e.g. `public`) |
| SQL Server | database | schema (e.g. `dbo`) |
| MySQL | omit | database (MySQL has no separate schema level) |
| Oracle | service name: the PDB in a multi-tenant database (e.g. `FREEPDB1`) | owner, uppercase (e.g. `LFC`); `source_table`, `primary_keys`, and `cursor_columns` uppercase too |

Keep scratch files (the Step 0 discovery `.sql` files, report queries) out of the bundle directory, for example in a `queries/` folder listed under `sync.exclude`. Otherwise `bundle deploy` uploads them to the workspace.

The pipeline is serverless by default. Classic compute is Beta (`serverless: false` plus a `clusters` block); prefer serverless.

**Foreign catalog variant** — if the source is already a Lakehouse Federation foreign catalog, set `ingest_from_uc_foreign_catalog: true` under `ingestion_definition`, and set `source_catalog` to the foreign catalog name. `primary_keys` is required unless `scd_type` is `APPEND_ONLY`.

---

## Step 3 — schedule

QBC is triggered-only, like every Lakeflow Connect pipeline. Each run is one incremental pull, so the schedule *is* the freshness SLA.

```yaml
resources:
  jobs:
    pg_query_based_schedule:
      name: <base>-schedule            # the pipeline name + -schedule
      trigger:
        periodic:
          interval: 1
          unit: HOURS
      tasks:
        - task_key: ingest
          pipeline_task:
            pipeline_id: ${resources.pipelines.pg_query_based.id}
```

---

## Networking

Serverless compute must reach the source database. Cloud databases usually need either public access restricted to the serverless egress IPs, or Private Link through a network connectivity configuration (NCC). If the workspace uses serverless egress control (network policies), allow the source host.

---

## Gotchas

| Symptom | Likely cause | Fix |
|---|---|---|
| Updates at the source never show up | Cursor isn't bumped on UPDATE (e.g. `DEFAULT now()` with no trigger) | Add an update trigger, or have the application set the cursor. |
| Some rows never land | NULL cursor values | Make the cursor `NOT NULL` and backfill. |
| Every run is a full reload | No cursor configured, the field was spelled `cursor_column` (dropped with an "unknown field" warning), or the cursor isn't monotonic | Set `cursor_columns` (a list) to a monotonic column, then check `pipelines get` shows it. |
| Deleted rows still in destination | Hard `DELETE` at the source | Switch to soft deletes + `deletion_condition`, or use the Beta hard-delete option / CDC. |
| Soft-deleted row still present | Delete flag set without bumping the cursor | Make sure the flag update fires the cursor trigger. |
| SCD2 shows fewer versions than source changes | Several changes between runs collapse to one | Expected for QBC. Run more often, or use CDC. |
| Connection or timeout errors from serverless | Serverless can't reach the source | Fix the network path (allowlist / NCC / network policy). |
| Whole update fails on a Postgres table with a `uuid` primary key | The connector runs `min(<pk>)` on the source, and Postgres before 18 has no `min(uuid)` | Use an integer (identity) primary key and keep the uuid as a `UNIQUE` column, then full-refresh that table. |

**Postgres types that change in Delta.** Seen on a Postgres 17 source; mention them in a demo:

| Postgres | Delta |
|---|---|
| `timestamptz` | `timestamp` |
| `time`, `timetz` | `timestamp` on 1970-01-01 |
| `numeric` with no precision | `decimal(38,18)` |
| enum | `string` |
| `bit(n)` | `binary` holding the characters `'0'` / `'1'` |
| `uuid`, `json`/`jsonb`, `xml`, `money` (keeps the `$`), `interval`, `inet`/`cidr`/`macaddr`, geometric, range, `varbit` | `string` |
| `int[]`, `text[]`, `numeric[]`, `boolean[]` | `array<...>` |
