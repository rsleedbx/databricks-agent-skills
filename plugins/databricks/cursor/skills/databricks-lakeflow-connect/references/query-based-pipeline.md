# Query-based pipeline: connection, pipeline, schedule

Create the UC connection, the foreign-connection pipeline, and its schedule, then check the gotchas. Concepts and cursor rules: [query-based-connectors.md](query-based-connectors.md). Finding the tables: [Step 0](#step-0--find-the-source-tables-through-the-connection).

## Step 0 — find the source tables through the connection

If the user names a connection but not the tables, look at the source through the connection with `remote_query`. It's read-only, needs no foreign catalog, and shows whether the compute it runs on reaches the source (a serverless warehouse, for the serverless path: [compute](compute-and-feature-gates.md#compute-serverless-or-classic)). Write each query to a `.sql` file with your file tool and run it with:

```bash
databricks experimental aitools tools query --file <file>.sql --profile <PROFILE> -o csv
```

Each query lists every table with its primary key and the date/time columns that could be the cursor. Pick tables that have a primary key and a NOT NULL cursor that changes on every update. Tables without a key need `APPEND_ONLY` (which needs a cursor) or SCD1 with no cursor (a full load each run). See [the shape table](query-based-connectors.md#what-each-source-shape-does-with-each-target) and run the [pre-flight checks](query-based-connectors.md#pre-flight-checks-before-creating-the-pipeline-advise-the-product-doesnt-check) before creating the pipeline.

**PostgreSQL:** list databases first, through a database the connection can open (the one the user named, or `postgres` if it exists), then the tables in one database. Keys come from `pg_index`: `information_schema.table_constraints` lists only tables the role has more than `SELECT` on, so a read-only login would see no keys.

```sql
SELECT * FROM remote_query('<connection>', database => '<any-accessible-database>', query => "SELECT datname FROM pg_database WHERE NOT datistemplate") ORDER BY 1
```

```sql
SELECT * FROM remote_query('<connection>', database => '<database>', query => "
  SELECT c.table_schema, c.table_name,
         (SELECT string_agg(a.attname, ',' ORDER BY k.ord)
            FROM pg_index ix
            CROSS JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord)
            JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = k.attnum
           WHERE ix.indrelid = format('%I.%I', c.table_schema, c.table_name)::regclass
             AND ix.indisprimary) AS primary_key,
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

**Oracle:** run these queries through a connection whose `service_name` is the PDB (for example `FREEPDB1`); why, and which connection the pipeline uses instead, is in the Oracle row of the [Step 1 table](query-based-pipeline.md#step-1--uc-connection). The query below passes no `service_name`. Names come back in uppercase, and the pipeline needs them that way.

```sql
SELECT * FROM remote_query('<pdb-connection>', query => "
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

A query-based pipeline points at a regular Unity Catalog connection (the same object Lakehouse Federation uses). Write the request body to a private file so the password never appears on a command line, and delete it whether the create works or not. `jq` builds the JSON, so a password with `"` or `\` in it stays intact:

```bash
umask 077
jq -n --rawfile pw <password-file> '{name: "<connection>", connection_type: "POSTGRESQL",
  options: {host: "<host>", port: "5432", user: "<user>", password: ($pw | rtrimstr("\n"))}}' > conn.json
databricks connections create --json @conn.json --profile <PROFILE>; rm -f conn.json
```

`<password-file>` holds only the password, mode 600, outside the bundle directory so `bundle deploy` never uploads it. Delete it when you no longer need it, unless it's where the user keeps that password.

Per source:

| Source | `connection_type` | Notes |
|---|---|---|
| PostgreSQL | `POSTGRESQL` | |
| MySQL | `MYSQL` | Query-based works on a server with a self-signed certificate; CDC on the same connection then needs `"trustServerCertificate": "true"` in `options` ([TLS on the connection](integrated-cdc.md#source-prerequisites)), so add it at the start if CDC may follow. |
| SQL Server | `SQLSERVER` | A server with a self-signed certificate (a VM or container install) needs `"trustServerCertificate": "true"` in `options`, or every query fails with `FAILED_JDBC.CONNECTION`. Managed services don't. |
| Oracle | `ORACLE` | `user` in uppercase (`C##LFC_USER` for a common user). `"service_name"`: the root container's service in a multi-tenant database, which is the CDB's name (`FREE` on Oracle Free, not the literal `CDB$ROOT`, which the listener doesn't serve); integrated CDC refuses a connection without it (`Missing service_name option in the connection`). Pipelines on it reach the PDB through `source_catalog` (query-based and integrated CDC). SQL through the connection (`remote_query`, Step 0) runs in the connection's service and ignores a `service_name =>` argument, so for the PDB's tables add a second connection with `"service_name": "<PDB>"`. Find the services with `lsnrctl status`. |

Use a dedicated, read-only database user. Requires `CREATE CONNECTION` on the metastore. Grant `USE CONNECTION` to whoever creates the pipeline if that is a different principal. Check the connection with a Step 0 query ([query-based discovery](#step-0--find-the-source-tables-through-the-connection)) before building the pipeline: `FAILED_JDBC.CONNECTION ... Failed to connect to the database` is the network path or, on SQL Server, the certificate.

Source-side grants (Postgres):

```sql
GRANT CONNECT ON DATABASE <db> TO <pg-user>;
GRANT USAGE ON SCHEMA public TO <pg-user>;
GRANT SELECT ON public.orders, public.customers TO <pg-user>;
```

## Step 2 — pipeline (foreign connection)

```yaml
resources:
  pipelines:
    pg_query_based:
      name: <name>                     # <firstname><lastname>_<connection>_<epoch>, see prerequisite-checks.md#default-names
      catalog: ${var.catalog}
      schema: <name>                   # the same name as the pipeline
      ingestion_definition:
        connection_name: pg_source_conn
        objects:
          - table:
              source_catalog: <pg-database>       # the Postgres database name
              source_schema: public
              source_table: orders
              destination_catalog: ${var.catalog}
              destination_schema: <name>
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
              destination_schema: <name>
              table_configuration:
                scd_type: SCD_TYPE_2              # keeps history: __START_AT / __END_AT
                primary_keys: [customer_id]
                query_based_connector_config:
                  cursor_columns: [updated_at]
```

**How `source_catalog` and `source_schema` map per source:**

| Source | `source_catalog` | `source_schema` |
|---|---|---|
| PostgreSQL | database | schema (e.g. `public`) |
| SQL Server | database | schema (e.g. `dbo`) |
| MySQL | omit | database (MySQL has no separate schema level) |
| Oracle | service name: the PDB in a multi-tenant database (e.g. `FREEPDB1`) | owner, uppercase (e.g. `LFC`); `source_table`, `primary_keys`, and `cursor_columns` uppercase too |

Keep scratch files (the Step 0 discovery `.sql` files, report queries) out of the bundle directory, for example in a `queries/` folder listed under `sync.exclude`. Otherwise `bundle deploy` uploads them to the workspace.

The pipeline is serverless by default: leave `serverless` out of the bundle, and read back what it ran on ([compute-and-feature-gates.md](compute-and-feature-gates.md#compute-serverless-or-classic)). Classic compute is Beta (`serverless: false` plus a `clusters` block); prefer serverless.

**Foreign catalog variant** — if the source is already a Lakehouse Federation foreign catalog, set `ingest_from_uc_foreign_catalog: true` under `ingestion_definition`, and set `source_catalog` to the foreign catalog name. `primary_keys` is required unless `scd_type` is `APPEND_ONLY`.

## Step 3 — schedule

Each run is one incremental pull, so the schedule *is* the freshness SLA.

```yaml
resources:
  jobs:
    pg_query_based_schedule:
      name: <name>                     # the pipeline's name: jobs and pipelines are named separately
      trigger:
        periodic:
          interval: 1
          unit: HOURS
      tasks:
        - task_key: ingest
          pipeline_task:
            pipeline_id: ${resources.pipelines.pg_query_based.id}
```

## Networking

Serverless compute must reach the source database. Cloud databases usually need either public access restricted to the serverless egress IPs, or Private Link through a network connectivity configuration (NCC). If the workspace uses serverless egress control (network policies), allow the source host.

## Gotchas

| Symptom | Likely cause | Fix |
|---|---|---|
| Updates at the source never show up | Cursor isn't bumped on UPDATE (e.g. `DEFAULT now()` with no trigger) | [cursor rules](query-based-connectors.md#cursor-column-rules) |
| Some rows never land | NULL cursor values | [cursor rules](query-based-connectors.md#cursor-column-rules) |
| Every run is a full reload | No cursor configured, the field was spelled `cursor_column` (dropped with an "unknown field" warning), or the cursor isn't monotonic | [cursor rules](query-based-connectors.md#cursor-column-rules) |
| Deleted rows still in destination | Hard `DELETE` at the source | Switch to soft deletes + `deletion_condition`, or use the Beta hard-delete option / CDC. |
| Soft-deleted row still present | Delete flag set without bumping the cursor | Make sure the flag update fires the cursor trigger. |
| SCD2 shows fewer versions than source changes | Several changes between runs collapse to one | Expected for QBC. Run more often, or use CDC. |
| Connection or timeout errors from serverless | Serverless can't reach the source | Fix the network path (allowlist / NCC / network policy). |
| Whole update fails on a Postgres table with a `uuid` primary key | The connector runs `min(<pk>)` on the source, and Postgres before 18 has no `min(uuid)` | Use an integer (identity) primary key and keep the uuid as a `UNIQUE` column, then full-refresh that table. |

**Postgres types that change in Delta.** On Postgres 17; mention them in a demo:

| Postgres | Delta |
|---|---|
| `timestamptz` | `timestamp` |
| `time`, `timetz` | `timestamp` on 1970-01-01 |
| `numeric` with no precision | `decimal(38,18)` |
| enum | `string` |
| `bit(n)` | `binary` holding the characters `'0'` / `'1'` |
| `uuid`, `json`/`jsonb`, `xml`, `money` (keeps the `$`), `interval`, `inet`/`cidr`/`macaddr`, geometric, range, `varbit` | `string` |
| `int[]`, `text[]`, `numeric[]`, `boolean[]` | `array<...>` |
