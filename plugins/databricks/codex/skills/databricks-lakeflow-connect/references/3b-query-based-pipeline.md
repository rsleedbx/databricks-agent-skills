# Query-based pipeline: connection, pipeline, schedule

Create the UC connection, the foreign-connection pipeline, and its schedule, then check the gotchas. Concepts and cursor rules: [3-query-based-connectors.md](3-query-based-connectors.md). Finding the tables: [3a-query-based-discovery.md](3a-query-based-discovery.md).

## Step 1 — UC connection

A query-based pipeline points at a regular Unity Catalog connection (the same object Lakehouse Federation uses). Write the request body to a private file so the password never appears on a command line, and delete it whether the create works or not. `jq` builds the JSON, so a password with `"` or `\` in it stays intact:

```bash
umask 077
jq -n --rawfile pw <password-file> '{name: "<connection>", connection_type: "POSTGRESQL",
  options: {host: "<host>", port: "5432", user: "<user>", password: ($pw | rtrimstr("\n"))}}' > conn.json
databricks connections create --json @conn.json --profile <PROFILE>; rm -f conn.json
```

`<password-file>` holds only the password, mode 600, outside the bundle directory so `bundle deploy` never uploads it. Delete it when you no longer need it, unless it's where the user keeps that password.

Per source (each tested end to end with a query-based pipeline):

| Source | `connection_type` | Notes |
|---|---|---|
| PostgreSQL | `POSTGRESQL` | |
| MySQL | `MYSQL` | Query-based works on a server with a self-signed certificate; CDC on the same connection then needs `"trustServerCertificate": "true"` in `options` ([TLS on the connection](2a-integrated-cdc.md#source-prerequisites)), so add it at the start if CDC may follow. |
| SQL Server | `SQLSERVER` | A server with a self-signed certificate (a VM or container install) needs `"trustServerCertificate": "true"` in `options`, or every query fails with `FAILED_JDBC.CONNECTION`. Managed services don't. |
| Oracle | `ORACLE` | `user` in uppercase (`C##LFC_USER` for a common user). `"service_name"`: the root container's service in a multi-tenant database, which is the CDB's name (`FREE` on Oracle Free, not the literal `CDB$ROOT`, which the listener doesn't serve); integrated CDC refuses a connection without it (`Missing service_name option in the connection`). Pipelines on it reach the PDB through `source_catalog` (tested: query-based and integrated CDC). SQL through the connection (`remote_query`, Step 0) runs in the connection's service and ignores a `service_name =>` argument, so for the PDB's tables add a second connection with `"service_name": "<PDB>"`. Find the services with `lsnrctl status`. |

Use a dedicated, read-only database user. Requires `CREATE CONNECTION` on the metastore. Grant `USE CONNECTION` to whoever creates the pipeline if that is a different principal. Check the connection with a Step 0 query before building the pipeline: `FAILED_JDBC.CONNECTION ... Failed to connect to the database` is the network path or, on SQL Server, the certificate.

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
      name: <name>                     # <firstname><lastname>_<connection>_<epoch>, see 9-default-names.md
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

The pipeline is serverless by default: leave `serverless` out of the bundle, and read back what it ran on ([2-database-connectors.md](2f-compute-and-feature-gates.md#compute-serverless-or-classic)). Classic compute is Beta (`serverless: false` plus a `clusters` block); prefer serverless.

**Foreign catalog variant** — if the source is already a Lakehouse Federation foreign catalog, set `ingest_from_uc_foreign_catalog: true` under `ingestion_definition`, and set `source_catalog` to the foreign catalog name. `primary_keys` is required unless `scd_type` is `APPEND_ONLY`.

## Step 3 — schedule

QBC pipelines are triggered: each run is one incremental pull, so the schedule *is* the freshness SLA. (A standard CDC gateway, by contrast, runs continuously until you stop it.)

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
