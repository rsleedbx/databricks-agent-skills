# Source-side SQL through a UC connection

Lakeflow Connect setup and cleanup often needs SQL on the source: list tables, check CDC settings, create or drop a Postgres replication slot. Do it through the existing **Unity Catalog connection** with `remote_query`. You don't need the database password, a secret, or network access from your machine: the SQL runs on Databricks serverless compute, which already reaches the source.

Two ways to run it:

- **One query, from the CLI:** `databricks experimental aitools tools query --file <file>.sql`. This is what Step 0 of [3-query-based-connectors.md](3-query-based-connectors.md) uses. Good for a single check.
- **Several queries and actions, as a notebook:** upload the notebook below and run it once with `databricks jobs submit`. That's a one-time run: it doesn't create a job in the Jobs UI. Use it for listing everything at once, and for slot create / drop.

Don't read UC secrets or ask for the password to do this. The connection is the credential.

---

## What works through a connection

| Operation | Through `remote_query` | Notes |
|---|---|---|
| `SELECT` on tables and catalog views (`information_schema`, `sys.*`, `pg_catalog`) | Yes | |
| Postgres slot functions called from a `SELECT`: `pg_create_logical_replication_slot`, `pg_drop_replication_slot` | Yes (tested) | Works even on a connection marked read-only. `remote_query` is documented as SELECT-only, so this may change: if it fails, fall back to the user. |
| DDL and DML: `CREATE` / `DROP PUBLICATION`, `ALTER TABLE ... REPLICA IDENTITY`, enabling SQL Server CDC, `INSERT` / `UPDATE` / `DELETE` | **No** (tested) | `remote_query` runs only queries, and refuses options such as `sessionInitStatement`. Give the user the exact SQL to run on the source. |

**Each `remote_query` call is its own database session.** A temporary slot (`pg_create_logical_replication_slot(name, 'pgoutput', true)`) disappears as soon as the call returns, so it's a safe probe but useless for a pipeline. Create permanent slots for CDC.

**Each Postgres CDC pipeline gets its own publication and its own slot.** Never reuse another pipeline's publication or slot, even one that already covers the same tables: a slot has one consumer at a time, and a shared publication ties table lists and cleanup together. The slot can be created through the connection. The publication can't, because `remote_query` rejects DDL and the session options that could run it (`sessionInitStatement` is refused), so give the user the exact `CREATE PUBLICATION` statement to run on the source, as the owner of the tables.

---

## Introspect the source: all columns, keys, and indexes in one shot

Use this when the user doesn't know their tables, or before generating DML. **Never fetch one table at a time:** one query returns every column in the database, and one more returns every primary key, unique constraint, and index with its columns in order. Each takes a couple of seconds, even for a database with more than 100 tables.

### 1. Pick the catalog (source database)

- **List what the connection can see:** the source databases (Step 0 of [3-query-based-connectors.md](3-query-based-connectors.md)), plus any foreign catalogs already on this connection (`databricks catalogs list -o json`, filtered on `connection_name`).
- **Exactly one:** use it.
- **More than one:** ask the user which, and show the list. Don't introspect them all; users can have access to many.
- **None of that helps** (the user has no idea where their data is): offer to create a foreign catalog, as below.

### 2. Optional: create a foreign catalog

A foreign catalog (Lakehouse Federation) makes the source browsable in Catalog Explorer and queryable as `<catalog>.<schema>.<table>`. Offer it, don't just create it. Check permission first:

```bash
MS=$(databricks metastores current --profile <PROFILE> -o json | jq -r .metastore_id)
databricks grants get-effective metastore $MS --principal <user> --profile <PROFILE> -o json \
  | jq -r '[.privilege_assignments[]?.privileges[]?.privilege] | unique | join(",")'   # needs CREATE_CATALOG
databricks connections get <connection> --profile <PROFILE> -o json | jq -r .owner     # owner, or CREATE_FOREIGN_CATALOG on it
```

Without `CREATE_CATALOG` on the metastore, creation fails with "User does not have CREATE CATALOG on Metastore". Say so and use the `remote_query` introspection below instead; it needs nothing extra.

- **Name:** `<firstname><lastname>-<connection>`, the same convention as pipelines (see **Default pipeline names** in SKILL.md). If that catalog already exists on the same connection, reuse it. If it exists for something else, append `-2`, `-3`. The name has hyphens, so quote it with backticks in SQL.
- **Create** (MySQL has no `database` option):

```bash
databricks catalogs create --profile <PROFILE> --json '{"name": "<firstname><lastname>-<connection>", "connection_name": "<connection>", "options": {"database": "<database>"}}'
```

- **What it gives you:** with a foreign catalog, every column is in one query: `` SELECT * FROM `<catalog>`.information_schema.columns ``. Keys and indexes still come from the source with the queries below, because UC doesn't record source indexes.

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
         CASE WHEN ix.indisprimary THEN 'primary key' WHEN ix.indisunique THEN 'unique' ELSE 'index' END AS kind,
         string_agg(a.attname, ',' ORDER BY k.ord) AS columns
    FROM pg_index ix
    JOIN pg_class c ON c.oid = ix.indrelid
    JOIN pg_class i ON i.oid = ix.indexrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    CROSS JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord)
    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
   WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
   GROUP BY n.nspname, c.relname, i.relname, ix.indisprimary, ix.indisunique")
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
         CASE WHEN i.is_primary_key = 1 THEN 'primary key' WHEN i.is_unique = 1 THEN 'unique' ELSE 'index' END AS kind,
         STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS columns
    FROM sys.indexes i
    JOIN sys.tables t ON t.object_id = i.object_id
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
    JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
   WHERE t.is_ms_shipped = 0 AND i.type > 0
   GROUP BY s.name, t.name, i.name, i.is_primary_key, i.is_unique")
ORDER BY table_schema, table_name, kind, index_name
```

How to read `kind`: `primary key`, `unique` (a unique constraint or unique index, which can serve as the key when a table has no primary key), or `index`. A table with no `primary key` or `unique` row has no key: query-based needs `APPEND_ONLY`, and integrated CDC keys it on all columns.

---

## Generate DML for test changes

With the column and key lists, write change scripts that exercise a pipeline: for example 10 inserts, 10 updates, and 10 deletes per table. Build them from the metadata, not by guessing:

- **Inserts:** new key values above the current maximum (get every table's `max(<key>)` in one `remote_query` with `UNION ALL`). Fill every NOT NULL column without a default with a value of the right type. Skip identity, computed, and defaulted columns.
- **Updates:** change a non-key column and, for query-based pipelines, also move the cursor column forward (`dt = now()` / `CURRENT_TIMESTAMP` / `SYSDATETIME()`), or the change is never picked up.
- **Deletes:** by key. For query-based pipelines, prefer a soft delete (set the `deletion_condition` column) because a hard delete isn't seen.
- **Tables without a key:** inserts only, or updates and deletes matched on all columns. Say that duplicate rows make these ambiguous.
- **Dialect:** `LIMIT` (Postgres, MySQL) vs `TOP` (SQL Server); `now()` vs `SYSDATETIME()`; identifier quoting.

**Running it:** the UC connection can't run DML (`remote_query` only runs queries). Give the user the script to run on the source, and say which tables and how many rows it touches. For Lakebase you're the project owner, so run it yourself with `databricks psql ... -f` (see the mutate step in [6-lakebase-as-postgres-source.md](6-lakebase-as-postgres-source.md)). After the next pipeline run, check that the changes arrived. SCD1 tables: the rows-per-run report in [5-troubleshooting-and-monitoring.md](5-troubleshooting-and-monitoring.md#rows-per-table-per-run-snapshot-inserts-upserts-deletes) should show your inserts plus updates as `upserted` and your deletes as `deleted`. SCD2 tables: use the per-key count from the table in the same file, because the event-log counts for SCD2 aren't change counts. Also spot-check the rows you touched (the new keys exist, updated columns have the new values, deleted keys are gone or closed).

---

## The notebook

Save as `source_sql.py` (the first line marks it as a Databricks notebook). It takes the connection type as a parameter, so look it up first:

```bash
databricks connections get <connection> --profile <PROFILE> -o json | jq -r .connection_type   # POSTGRESQL | MYSQL | SQLSERVER
```

```python
# Databricks notebook source
# Source-side SQL through a Unity Catalog connection (remote_query), for Lakeflow Connect setup and cleanup.
# Widgets:
#   connection  UC connection name
#   conn_type   connection type from `databricks connections get <name>`: POSTGRESQL | MYSQL | SQLSERVER
#   database    source database (blank for MySQL)
#   action      list | create_slot | drop_slot   (slot actions: PostgreSQL only)
#   slot_name   exact replication slot name for create_slot / drop_slot
import json

dbutils.widgets.text("connection", "")
dbutils.widgets.text("conn_type", "")
dbutils.widgets.text("database", "")
dbutils.widgets.dropdown("action", "list", ["list", "create_slot", "drop_slot"])
dbutils.widgets.text("slot_name", "")
conn = dbutils.widgets.get("connection")
database = dbutils.widgets.get("database")
action = dbutils.widgets.get("action")
slot_name = dbutils.widgets.get("slot_name").strip()
conn_type = dbutils.widgets.get("conn_type").upper()


def rq(query):
    """Run one query on the source through the UC connection; returns a list of dicts."""
    db_arg = f", database => '{database}'" if database else ""
    q = query.replace("\\", "\\\\").replace('"', '\\"')
    rows = spark.sql(f"SELECT * FROM remote_query('{conn}'{db_arg}, query => \"{q}\")").collect()
    return [{k: (str(v) if v is not None else None) for k, v in r.asDict().items()} for r in rows]


def pg_slots():
    return rq("SELECT slot_name, plugin, database, active, temporary FROM pg_replication_slots")  # no inactive_since: PG 17+ only


out = {"connection": conn, "type": conn_type, "database": database or None, "action": action}

if action in ("create_slot", "drop_slot"):
    if conn_type != "POSTGRESQL":
        raise ValueError(f"{action} is PostgreSQL only, connection type is {conn_type}")
    if not slot_name.replace("_", "").isalnum() or slot_name.lower() != slot_name:
        raise ValueError("slot_name must be lowercase letters, digits, and underscores")
    exists = any(s["slot_name"] == slot_name for s in pg_slots())
    if action == "create_slot":
        out["result"] = "already exists" if exists else rq(
            f"SELECT slot_name, lsn FROM pg_create_logical_replication_slot('{slot_name}', 'pgoutput')")
    else:
        # Drop only the slot named here, and only if nothing is reading from it.
        active = [s for s in pg_slots() if s["slot_name"] == slot_name and str(s["active"]).lower() == "true"]
        if active:
            raise ValueError(f"slot {slot_name} is active; stop the gateway first")
        out["result"] = "not found" if not exists else rq(
            f"SELECT '{slot_name}' AS dropped FROM (SELECT pg_drop_replication_slot('{slot_name}')) d")
    out["slots"] = pg_slots()
else:
    if conn_type == "MYSQL":
        out["tables"] = rq("""
            SELECT table_schema, table_name FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('mysql', 'sys', 'information_schema', 'performance_schema')""")
    elif conn_type == "SQLSERVER":
        out["tables"] = rq("SELECT s.name AS table_schema, t.name AS table_name FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.is_ms_shipped = 0")
    elif conn_type == "POSTGRESQL":
        out["tables"] = rq("""
            SELECT table_schema, table_name FROM information_schema.tables
            WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema')""")
        out["publications"] = rq("""
            SELECT p.pubname, pg_get_userbyid(p.pubowner) AS owner,
                   (SELECT string_agg(schemaname || '.' || tablename, ', ') FROM pg_publication_tables t WHERE t.pubname = p.pubname) AS tables
            FROM pg_publication p""")
        out["slots"] = pg_slots()
    else:
        raise ValueError(f"unsupported connection type {conn_type}")
    out["table_count"] = len(out["tables"])

dbutils.notebook.exit(json.dumps(out))
```

## Running it

Upload once, then submit one-time runs. Several tasks in one submit run in parallel, or in order with `depends_on`.

```bash
NB=/Workspace/Users/<user>/.lfc_skill/source_sql
databricks workspace mkdirs /Workspace/Users/<user>/.lfc_skill --profile <PROFILE>
databricks workspace import $NB --file source_sql.py --format SOURCE --language PYTHON --overwrite --profile <PROFILE>
databricks jobs submit --json @submit.json --profile <PROFILE> -o json > run.json
```

`submit.json`, listing a Postgres source (use `"action": "create_slot"` or `"drop_slot"` with `"slot_name"` for slot work):

```json
{
  "run_name": "lfc-source-sql",
  "tasks": [
    {"task_key": "list", "notebook_task": {
      "notebook_path": "/Workspace/Users/<user>/.lfc_skill/source_sql",
      "base_parameters": {"connection": "<connection>", "conn_type": "POSTGRESQL", "database": "<database>", "action": "list"}}}
  ]
}
```

No cluster spec means it runs on serverless. `jobs submit` waits for the run (about a minute including startup). Read each task's result from its **task** run ID, not the parent run ID:

```bash
RID=$(jq -r .run_id run.json)
databricks jobs get-run $RID --profile <PROFILE> -o json | jq -r '.tasks[] | [.task_key, .run_id, .state.result_state] | @tsv'
databricks jobs get-run-output <task-run-id> --profile <PROFILE> -o json | jq -r '.notebook_output.result // .error'
```

If a task fails, `jobs submit` exits non-zero and `run.json` can be empty: find the run with `databricks jobs list-runs --limit 5 -o json` by `run_name`.

---

## Postgres slot lifecycle for standard CDC

An unused slot holds WAL on the source until someone drops it, so create it as late as possible:

1. **Pick the names, one pair per pipeline:** name both after the pipeline's destination schema, for example slot `<schema>` and publication `<schema>_pub`. Run `action: list` to make sure neither name is taken. Put both in the ingestion pipeline's `source_configurations` → `postgres.slot_config` (`slot_name`, `publication_name`); see [2-database-connectors.md](2-database-connectors.md#postgresql-cdc).
2. **Create the pipelines first:** `bundle deploy`, without starting the gateway. This is where a workspace without Postgres CDC fails ("Ingestion source 'POSTGRESQL' is not supported in your workspace", 403). If it fails, stop: nothing exists on the source yet.
3. **Publication (the user runs it):** give the user this statement, listing only this pipeline's tables, and wait for them to confirm. Then check it with `action: list`:

   ```sql
   CREATE PUBLICATION <schema>_pub FOR TABLE <source_schema>.<table1>, <source_schema>.<table2>;
   ```

4. **Slot (through the connection),** after the publication exists: `action: create_slot` with `slot_name: <schema>`. Then start the gateway and run the ingestion pipeline.
5. **Cleanup after the test** (ask first): `bundle destroy` (or delete the pipelines) so the gateway stops reading. Then run `action: drop_slot` with this pipeline's slot name, and give the user `DROP PUBLICATION <schema>_pub;` to run. Finish with `action: list` to confirm both are gone.

If anything fails after the slot exists, drop the slot before you stop, or tell the user it's there and why it matters: a slot nobody reads keeps holding WAL and can fill the source disk, especially when `max_slot_wal_keep_size` is `-1`.

Cleanup rules:

- **Drop only the slot you created, by exact name.** Never drop slots or publications by pattern or age on a shared server; other people's pipelines use them. The notebook refuses to drop an active slot.
- **Publications:** drop only this pipeline's own publication (through the user, as above). Leave every other publication alone, including ones that cover the same tables.
