# Source-side SQL through a UC connection

Lakeflow Connect setup and cleanup often needs SQL on the source: list tables, check CDC settings, create or drop a Postgres replication slot. Do it through the existing **Unity Catalog connection** with `remote_query`. You don't need the database password, a secret, or network access from your machine: the SQL runs on Databricks compute (a SQL warehouse for the CLI, serverless for the notebook), which must reach the source.

Two ways to run it:

- **One query, from the CLI:** `databricks experimental aitools tools query --file <file>.sql` (it picks a SQL warehouse unless `--warehouse <id>` is set). This is what Step 0 of [query-based-discovery.md](query-based-discovery.md) uses. Good for a single check.
- **Several queries and actions, as a notebook:** upload the notebook in [source-sql-notebook.md](source-sql-notebook.md) and run it once with `databricks jobs submit`. That's a one-time run: it doesn't create a job in the Jobs UI. Use it for listing everything at once on MySQL, SQL Server and PostgreSQL connections, and for slot create / drop. For Oracle, use the queries in [query-based-discovery.md](query-based-discovery.md).

Don't read UC secrets or ask for the password to do this. The connection is the credential.

---

## What works through a connection

| Operation | Through `remote_query` | Notes |
|---|---|---|
| `SELECT` on tables and catalog views (`information_schema`, `sys.*`, `pg_catalog`) | Yes | |
| Postgres slot functions called from a `SELECT`: `pg_create_logical_replication_slot`, `pg_drop_replication_slot` | Yes | Works even on a connection marked read-only. `remote_query` is documented as SELECT-only, so this may change: if it fails, fall back to the user. |
| DDL and DML: `CREATE` / `DROP PUBLICATION`, `ALTER TABLE ... REPLICA IDENTITY`, enabling SQL Server CDC, `INSERT` / `UPDATE` / `DELETE` | **No** | `remote_query` runs only queries, and refuses options such as `sessionInitStatement`. Give the user the exact SQL to run on the source. |

**Slot functions need `REPLICATION` on the connection's user.** A read-only connector role (like the Lakebase demo's `<schema>_reader`) can list slots but not create them; on Lakebase the failure shows up only as "An unexpected error occurred" with a trace ID. Use a connection whose user has `REPLICATION` (or `rds_replication`), or have the user create the slot.

**Each `remote_query` call is its own database session.** A temporary slot (`pg_create_logical_replication_slot(name, 'pgoutput', true)`) disappears as soon as the call returns, so it's a safe probe but useless for a pipeline. Create permanent slots for CDC.

**Each Postgres CDC pipeline gets its own publication and its own slot.** Never reuse another pipeline's publication or slot, even one that already covers the same tables: a slot has one consumer at a time, and a shared publication ties table lists and cleanup together. The slot can be created through the connection. The publication can't, because `remote_query` rejects DDL and the session options that could run it (`sessionInitStatement` is refused), so give the user the exact `CREATE PUBLICATION` statement to run on the source, as the owner of the tables.

---

## Introspect the source: all columns, keys, and indexes in one shot

Use this when the user doesn't know their tables, or before generating DML. **Never fetch one table at a time:** one query returns every column in the database, and one more returns every primary key, unique constraint, and index with its columns in order. Each takes a couple of seconds, even for a database with more than 100 tables.

### 1. Pick the catalog (source database)

- **List what the connection can see:** the source databases (Step 0 of [query-based-discovery.md](query-based-discovery.md)), plus any foreign catalogs already on this connection (`databricks catalogs list -o json`, filtered on `connection_name`).
- **A foreign catalog already exists on this connection** (any name, for example one named after the connection): use it for introspection; don't create another.
- **Exactly one source database:** use it.
- **More than one:** ask the user which, and show the list. Don't introspect them all; users can have access to many.
- **None of that helps** (the user has no idea where their data is): offer to create a foreign catalog, as below.

### 2. Optional: create a foreign catalog

A foreign catalog (Lakehouse Federation) makes the source browsable in Catalog Explorer and queryable as `<catalog>.<schema>.<table>`. Offer it, don't just create it. Check permission first:

```bash
MS=$(databricks metastores current --profile <PROFILE> -o json | jq -r .metastore_id)
databricks grants get-effective metastore $MS --principal <user> --profile <PROFILE> -o json \
  | jq -r '[.privilege_assignments[].privileges[].privilege] | unique | join(",")'   # needs CREATE_CATALOG
databricks connections get <connection> --profile <PROFILE> -o json | jq -r .owner     # owner, or CREATE_FOREIGN_CATALOG on it
```

Without `CREATE_CATALOG` on the metastore, creation fails with "User does not have CREATE CATALOG on Metastore". Say so and use the `remote_query` introspection below instead; it needs nothing extra.

- **Name:** `<firstname><lastname>-<connection>`, with no epoch: like the connection, a foreign catalog is shared by many pipelines. If a catalog with that name exists for something else, append `-2`, `-3`. The name has hyphens, so quote it with backticks in SQL.
- **Create.** Options per source: PostgreSQL and SQL Server `{"database": "<database>"}`; MySQL none (leave `options` out); Oracle `{"service_name": "<PDB>"}`, for example `FREEPDB1` (see the Oracle row in [query-based-pipeline.md](query-based-pipeline.md#step-1--uc-connection)). Use `--json` only: `catalogs create` has no `--options` flag, and it rejects a positional name together with `--json`. When the connection's name already starts with `<firstname><lastname>` followed by `-` or `_` (the [default names](prerequisite-checks.md#default-names) rule), the catalog is `<connection>-fc` (for example `janedoe-mysql-azvm-fc`), not the prefix twice.

```bash
# PostgreSQL, SQL Server
databricks catalogs create --profile <PROFILE> --json '{"name": "<catalog>", "connection_name": "<connection>", "options": {"database": "<database>"}}'
# MySQL
databricks catalogs create --profile <PROFILE> --json '{"name": "<catalog>", "connection_name": "<connection>"}'
# Oracle: the PDB
databricks catalogs create --profile <PROFILE> --json '{"name": "<catalog>", "connection_name": "<connection>", "options": {"service_name": "FREEPDB1"}}'
```

- **What it gives you:** a foreign catalog's `information_schema` covers columns and constraints, each in one query, without `remote_query`:
  - **Columns:** `` SELECT * FROM `<catalog>`.information_schema.columns WHERE table_schema NOT IN ('information_schema', 'pg_catalog') ``
  - **Primary keys and unique constraints, with columns in order** (on a Postgres foreign catalog, the same as the `remote_query` results below):

    ```sql
    SELECT tc.table_schema, tc.table_name, tc.constraint_type, tc.constraint_name,
           concat_ws(',', transform(sort_array(collect_list(struct(k.ordinal_position, k.column_name))), x -> x.column_name)) AS columns
    FROM `<catalog>`.information_schema.table_constraints tc
    JOIN `<catalog>`.information_schema.key_column_usage k
      ON k.constraint_schema = tc.constraint_schema AND k.constraint_name = tc.constraint_name
    WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE') AND tc.table_schema NOT IN ('information_schema', 'pg_catalog')
    GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3
    ```

  - **What it doesn't cover:** plain non-unique indexes, which only the source knows. Use the keys-and-indexes query below for those. Foreign keys and check constraints are in `table_constraints` too, if you need them.

### 3. One-shot queries through the connection

Write each to a file and run it with `databricks experimental aitools tools query --file <file>.sql --profile <PROFILE> -o csv`. Filter by schema in the outer `WHERE` if the database is large.

**PostgreSQL: all columns, then all keys and indexes**

```sql
SELECT * FROM remote_query('<connection>', database => '<database>', query => "
  SELECT c.table_schema, c.table_name, c.ordinal_position, c.column_name, c.data_type,
         c.character_maximum_length, c.numeric_precision, c.numeric_scale, c.is_nullable, c.column_default
    FROM information_schema.columns c
    JOIN information_schema.tables t
      ON t.table_schema = c.table_schema AND t.table_name = c.table_name AND t.table_type = 'BASE TABLE'
   WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema')")
ORDER BY table_schema, table_name, ordinal_position
```

```sql
SELECT * FROM remote_query('<connection>', database => '<database>', query => "
  SELECT n.nspname AS table_schema, c.relname AS table_name, i.relname AS index_name,
         CASE WHEN ix.indisprimary THEN 'primary key' WHEN ix.indisunique AND ix.indpred IS NOT NULL THEN 'unique (partial)'
              WHEN ix.indisunique THEN 'unique' ELSE 'index' END AS kind,
         string_agg(a.attname, ',' ORDER BY k.ord) AS columns
    FROM pg_index ix
    JOIN pg_class c ON c.oid = ix.indrelid
    JOIN pg_class i ON i.oid = ix.indexrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    CROSS JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord)
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
   WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
   GROUP BY n.nspname, c.relname, i.relname, ix.indisprimary, ix.indisunique, ix.indpred IS NOT NULL")
ORDER BY table_schema, table_name, kind, index_name
```

**MySQL** (`<database>` is the MySQL database, which is the schema level)

```sql
SELECT * FROM remote_query('<connection>', query => "
  SELECT c.table_schema, c.table_name, c.ordinal_position, c.column_name, c.column_type, c.is_nullable,
         c.column_default, c.extra
    FROM information_schema.columns c
    JOIN information_schema.tables t
      ON t.table_schema = c.table_schema AND t.table_name = c.table_name AND t.table_type = 'BASE TABLE'
   WHERE c.table_schema = '<database>'")
ORDER BY table_schema, table_name, ordinal_position
```

```sql
SELECT * FROM remote_query('<connection>', query => "
  SELECT table_schema, table_name, index_name,
         CASE WHEN index_name = 'PRIMARY' THEN 'primary key' WHEN non_unique = 0 THEN 'unique' ELSE 'index' END AS kind,
         group_concat(column_name ORDER BY seq_in_index) AS columns
    FROM information_schema.statistics
   WHERE table_schema = '<database>'
   GROUP BY table_schema, table_name, index_name, non_unique")
ORDER BY table_schema, table_name, kind, index_name
```

**SQL Server**

```sql
SELECT * FROM remote_query('<connection>', database => '<database>', query => "
  SELECT s.name AS table_schema, t.name AS table_name, c.column_id AS ordinal_position, c.name AS column_name,
         ty.name AS data_type, c.max_length, c.precision, c.scale,
         CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END AS is_nullable,
         c.is_identity, c.is_computed
    FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    JOIN sys.columns c ON c.object_id = t.object_id
    JOIN sys.types ty ON ty.user_type_id = c.user_type_id
   WHERE t.is_ms_shipped = 0")
ORDER BY table_schema, table_name, ordinal_position
```

```sql
SELECT * FROM remote_query('<connection>', database => '<database>', query => "
  SELECT s.name AS table_schema, t.name AS table_name, i.name AS index_name,
         CASE WHEN i.is_primary_key = 1 THEN 'primary key' WHEN i.is_unique = 1 AND i.has_filter = 1 THEN 'unique (filtered)'
              WHEN i.is_unique = 1 THEN 'unique' ELSE 'index' END AS kind,
         STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
    FROM sys.indexes i
    JOIN sys.tables t ON t.object_id = i.object_id
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
    JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
   WHERE t.is_ms_shipped = 0 AND i.type > 0
   GROUP BY s.name, t.name, i.name, i.is_primary_key, i.is_unique, i.has_filter")
ORDER BY table_schema, table_name, kind, index_name
```

How to read `kind`: `primary key`, `unique` (a unique constraint or unique index, which can serve as the key when a table has no primary key), `unique (filtered)` / `unique (partial)` (unique only for rows matching the index's `WHERE`: not a key for the whole table, and duplicates outside it are lost under SCD1/SCD2), or `index`. A table with no `primary key` or `unique` row has no key: query-based needs `APPEND_ONLY` with a cursor, or SCD1 with no cursor, and integrated CDC keys it on all columns.

---

## Generate DML for test changes

With the column and key lists, write change scripts that exercise a pipeline: for example 10 inserts, 10 updates, and 10 deletes per table. Build them from the metadata, not by guessing:

- **Inserts:** new key values above the current maximum (get every table's `max(<key>)` in one `remote_query` with `UNION ALL`). Fill every NOT NULL column without a default with a value of the right type. Skip identity, computed, and defaulted columns.
- **Updates:** change a non-key column and, for query-based pipelines, also move the cursor column forward (`dt = now()` / `CURRENT_TIMESTAMP` / `SYSDATETIME()`), or the change is never picked up.
- **Deletes:** by key. For query-based pipelines, prefer a soft delete (set the `deletion_condition` column) because a hard delete isn't seen.
- **Tables without a key:** inserts only, or updates and deletes matched on all columns. Say that duplicate rows make these ambiguous.
- **Dialect:** `LIMIT` (Postgres, MySQL) vs `TOP` (SQL Server); `now()` vs `SYSDATETIME()`; identifier quoting.

**Running it:** the UC connection can't run DML (`remote_query` only runs queries). Give the user the script to run on the source, and say which tables and how many rows it touches. For Lakebase you're the project owner, so run it yourself with `databricks psql ... -f` (see the mutate step in [lakebase-as-postgres-source.md](lakebase-as-postgres-source.md)). After the next pipeline run, check that the changes arrived. SCD1 tables: the rows-per-run report in [rows-per-run.md](rows-per-run.md#rows-per-table-per-run-snapshot-inserts-upserts-deletes) should show your inserts plus updates as `upserted` and your deletes as `deleted`. SCD2 tables: use the per-key count from the table in the same file, because the event-log counts for SCD2 aren't change counts. Also spot-check the rows you touched (the new keys exist, updated columns have the new values, deleted keys are gone or closed).

---

## Postgres slot lifecycle for standard CDC

An unused slot holds WAL on the source until someone drops it, so create it as late as possible:

1. **Pick the names, one pair per pipeline:** slot `<slot_name>` and publication `<slot_name>_pub`, where `<slot_name>` is `<base>_<epoch>` of this pipeline pair (see [Default names](prerequisite-checks.md#default-names): the pipelines are `<base>_gw_<epoch>` / `<base>_ap_<epoch>`). Take it from the pipelines, never from the destination schema: the user may name the schema, and several pipelines can share one. For example slot `janedoe_my_pg_conn_1792351234` and publication `janedoe_my_pg_conn_1792351234_pub`. Run `action: list` to make sure neither name is taken. Put both in the ingestion pipeline's `source_configurations` → `postgres.slot_config` (`slot_name`, `publication_name`); see [postgresql-cdc.md](postgresql-cdc.md#pipelines).
2. **Publication (the user runs it), before anything else exists:** give the user this statement, listing only this pipeline's tables, and wait for them to confirm. Then check it with `action: list`:

   ```sql
   CREATE PUBLICATION <slot_name>_pub FOR TABLE <source_schema>.<table1>, <source_schema>.<table2>;
   ```

3. **Create the pipelines:** `bundle deploy`. This is where a workspace without Postgres CDC fails ("Ingestion source 'POSTGRESQL' is not supported in your workspace", 403). If it fails, check what was created (`databricks bundle summary`): stop a created gateway at once (`databricks pipelines stop <gateway-id>`), offer `bundle destroy` for the rest, then give the user `DROP PUBLICATION <slot_name>_pub;` and report anything left. If it succeeds, the gateway is a continuous pipeline and **starts on its own**, and errors until the slot exists: stop it with `databricks pipelines stop <gateway-id>`.
4. **Slot (through the connection):** `action: create_slot` with `slot_name: <slot_name>` and `database: <database>` (the database the publication is in; a slot belongs to one database). Then start the gateway (`databricks pipelines start-update <gateway-id>`) and run the ingestion pipeline.
5. **Cleanup** (ask first): `bundle destroy` (or delete the pipelines) so the gateway stops reading. Then run `action: drop_slot` with this pipeline's slot name and database, and give the user `DROP PUBLICATION <slot_name>_pub;` to run. Finish with `action: list` to confirm both are gone.

If anything fails after the slot exists, drop the slot before you stop, or tell the user it's there and why it matters: a slot nobody reads keeps holding WAL and can fill the source disk, especially when `max_slot_wal_keep_size` is `-1`.

Cleanup rules:

- **Drop only the slot you created, by exact name.** Never drop slots or publications by pattern or age on a shared server; other people's pipelines use them. The notebook refuses to drop an active slot.
- **Publications:** drop only this pipeline's own publication (through the user, as above). Leave every other publication alone, including ones that cover the same tables.
