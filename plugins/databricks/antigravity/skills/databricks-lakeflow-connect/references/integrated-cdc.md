# Integrated CDC: setup

Integrated CDC is one pipeline per source: extraction runs inside each update. This file covers choosing the mode, source prerequisites, the pipeline definition, and how source tables map to destination tables. How updates run and what they log: [integrated-cdc-updates.md](integrated-cdc-updates.md). Standard (gateway) CDC: [standard-cdc-gateway.md](standard-cdc-gateway.md).

## Integrated CDC (default) vs standard CDC

There are two ways to run a database CDC connector:

- **Integrated CDC:** one pipeline. Extraction runs inside each pipeline update, so there's no always-on gateway. The pipeline points at the UC connection with `connection_name` and sets `connector_type: CDC`.
- **Standard CDC:** two pipelines, an always-on ingestion gateway plus an ingestion pipeline that references it by `ingestion_gateway_id`. See [standard-cdc-gateway.md](standard-cdc-gateway.md).

**Which one to build:**
- The user asks for CDC and doesn't say which kind → **integrated**, where the source supports it.
- The user asks for "standard CDC", "two-pipeline CDC", or "gateway" → **standard**.
- The source doesn't support integrated CDC (check the docs; PostgreSQL has standard CDC only) → say so, and offer standard CDC. Don't switch silently: standard runs an always-on gateway, which costs more.

**Check current status before you build.** Integrated CDC is changing quickly: which sources support it, release stage, classic vs serverless compute, the runtime channel, and how long updates run all change from release to release. Treat the table below as a starting point, not the answer. Before creating a pipeline, fetch the current page for the source (or find it from `https://docs.databricks.com/llms.txt`, see **databricks-docs**) and use what it says:

| Source | Integrated CDC page | Release stage |
|---|---|---|
| SQL Server | [sql-server-integrated-pipeline](https://docs.databricks.com/ingestion/lakeflow-connect/sql-server-integrated-pipeline) | GA |
| MySQL | [mysql-integrated-pipeline](https://docs.databricks.com/ingestion/lakeflow-connect/mysql-integrated-pipeline) | GA |
| Oracle | [oracle-integrated-overview](https://docs.databricks.com/ingestion/lakeflow-connect/oracle-integrated-overview) | Beta; `channel: PREVIEW`; serverless works; integrated only; [grants and errors](database-source-behaviors-oracle.md) |
| PostgreSQL | none yet: standard CDC only | — |
| All | [smart-closure](https://docs.databricks.com/ingestion/lakeflow-connect/smart-closure): how long an update runs | — |

From the page, check: release stage, whether it needs `channel: PREVIEW`, whether it supports serverless, classic, or both (to know what the default may pick; still leave `serverless` unset, see [Compute: serverless or classic](compute-and-feature-gates.md#compute-serverless-or-classic)), and the source prerequisites. If a source now has an integrated CDC page that isn't listed here, use it. If the docs can't be fetched, say so and follow the table above as written. For a source marked "none yet", don't try integrated CDC: offer standard CDC. Don't add settings the table doesn't list (for example, only set `channel: PREVIEW` where the table or docs page requires it). Then let the workspace correct you.

**The workspace's error wins over the docs and this skill.** For example, a workspace that enforces serverless rejects classic compute (a `serverless` left unset that defaults to classic, or `serverless: false`) with "You must use serverless compute in this workspace". Set `serverless: true` and retry. If it rejects `connector_type: CDC`, integrated CDC isn't enabled on that workspace (the Databricks account team turns it on): tell the user integrated CDC isn't available, and offer standard CDC where the source has it (never switch silently: the gateway costs more and runs continuously).

### Source prerequisites

The source setup is the same as for standard CDC:

- **SQL Server:** CDC or change tracking enabled on each table ([sql-server-cdc.md](sql-server-cdc.md#sql-server-change-tracking-vs-cdc)), on the **primary** instance (no read replicas). SCD Type 2 needs CDC, not change tracking.
- **MySQL:** binary log on, with `binlog_format=ROW` and `binlog_row_image=FULL`. Binlog sessions and retention: [database-source-behaviors-mysql.md](database-source-behaviors-mysql.md).

Check through the existing connection with `remote_query` (read-only), using the same `--file` pattern as Step 0 in [query-based-discovery.md](query-based-discovery.md):

```sql
-- SQL Server: per-table CDC / change tracking status in one database
SELECT * FROM remote_query('<connection>', database => '<database>', query => "
  SELECT s.name AS table_schema, t.name AS table_name, t.is_tracked_by_cdc,
         CASE WHEN ct.object_id IS NULL THEN 0 ELSE 1 END AS change_tracking
    FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id
    LEFT JOIN sys.change_tracking_tables ct ON ct.object_id = t.object_id
   WHERE s.name = '<schema>'")
```

```sql
-- MySQL: binlog settings
SELECT * FROM remote_query('<connection>', query => "SELECT @@log_bin AS log_bin, @@binlog_format AS binlog_format, @@binlog_row_image AS binlog_row_image")
```

**TLS on the connection.** The CDC extractor enforces TLS. On MySQL, query-based ingestion through the same connection works without a trusted certificate and only CDC fails; on SQL Server, every query fails too (`FAILED_JDBC.CONNECTION`, see the connection table in [query-based-pipeline.md](query-based-pipeline.md#step-1--uc-connection)). A source with a self-signed certificate (a VM or container install of MySQL or SQL Server) fails at the start of the update: `Extractor FAILED`, with `INGESTION_GATEWAY_CONNECTION_ERROR.TLS_VALIDATION_FAILURE ... TLS is enforced but no server certificate provided. Provide a server certificate or set trustServerCertificate=true`. Add `"trustServerCertificate": "true"` to the connection's options with `databricks connections update <connection> --json @opts.json` (pass every option, including the password, from a private file), then start a new update. Managed services with trusted certificates don't need it.

A table with neither CDC nor change tracking can't be ingested with CDC: enable it at the source (ask first; it's a change to the customer's database), or use query-based for that table.

### Pipeline

One pipeline per source. Create the destination schema and a staging schema first; the pipeline creates a staging volume in the staging schema, so it needs `CREATE VOLUME` there. Without `data_staging_options`, it creates the volume in the destination schema instead, named `__databricks_ingestion_gateway_staging_data-<pipeline-id>`; deleting that schema removes it.

These fields are stable: `connection_name`, `connector_type: CDC`, `objects`, and `data_staging_options`. Set `channel` from the current docs page, as above. Leave `serverless` out and read back what the pipeline ran on ([Compute: serverless or classic](compute-and-feature-gates.md#compute-serverless-or-classic)).

```yaml
resources:
  pipelines:
    sqlserver_cdc:
      name: <name>                     # <firstname><lastname>_<connection>_<epoch>, see default-names.md
      catalog: ${var.catalog}
      schema: <name>                   # the same name as the pipeline
      ingestion_definition:
        connection_name: my_sqlserver_connection
        connector_type: CDC
        objects:
          - table:
              source_catalog: <database>
              source_schema: dbo
              source_table: customers
              destination_catalog: ${var.catalog}
              destination_schema: <name>
              table_configuration:
                scd_type: SCD_TYPE_1     # SCD_TYPE_2: SQL Server CDC, or change tracking + VIEW SERVER STATE
        data_staging_options:
          catalog_name: ${var.catalog}
          schema_name: <name>_staging   # create it with the destination schema
```

On a serverless-only workspace, integrated CDC on SQL Server (change tracking on a table with a primary key, SQL Server CDC on one without), MySQL and Oracle (`channel: PREVIEW`, `serverless: true`, each object's `source_catalog` = the PDB, with the connection's `service_name` = the CDB's service; see [query-based-pipeline.md](query-based-pipeline.md) Step 1) takes the snapshot, then applies inserts, updates, and deletes while the update is still running. Tables without a primary key are keyed on all columns ([updates](integrated-cdc-updates.md#behavior-that-doesnt-change-between-releases)).

For MySQL, address tables by `source_schema: <mysql-database>` with no `source_catalog`; `channel` is optional (default `CURRENT`). The MySQL page lists classic compute. A serverless-only workspace refuses classic ("You must use serverless compute in this workspace"), and `serverless: true` runs. The example leaves `serverless` out, as everywhere: add `serverless: true` only when the workspace asks for it, as that one did.

```yaml
    mysql_cdc:
      name: <name>
      catalog: ${var.catalog}
      schema: <name>
      ingestion_definition:
        connection_name: my_mysql_connection
        connector_type: CDC
        objects:
          - table:
              source_schema: <mysql-database>
              source_table: orders
              destination_catalog: ${var.catalog}
              destination_schema: <name>
        data_staging_options:
          catalog_name: ${var.catalog}
          schema_name: <name>_staging
```

**Clean up** an integrated CDC pipeline in this order: delete the pipeline (`databricks bundle destroy`, or `databricks pipelines delete <pipeline-id>`), then the schemas, only if this setup created them (the default `<name>` and `<name>_staging`): `databricks schemas delete <catalog>.<name> --force` and `databricks schemas delete <catalog>.<name>_staging --force` remove everything in them, the staging volume included. If the user gave an existing schema, drop only this pipeline's tables and staging volume, and ask before deleting anything. The bundle doesn't own schemas you created by hand, so `bundle destroy` leaves them.

Deleting a pipeline drops its destination tables: copy them out first if they're needed.

### Where each table lands: what LFC names, what you choose

LFC names the destination tables; you choose where they go. That split decides what a user has to write in `objects`:

| | LFC does it | You specify |
|---|---|---|
| Destination table name | the source table's name (`lfcdb.orders` -> `orders`) | `destination_table`, only to rename |
| Destination catalog and schema | | `destination_catalog` and `destination_schema` on every object |
| Which tables (schema object) | every table in the source schema, under its own name, and tables added to it later | nothing per table; a schema object can't rename one |
| Two source tables with the same name (`sales.orders`, `finance.orders`) | nothing: it doesn't rename a clash | a different `destination_schema` for each (or two schema objects, one per source schema), or a `destination_table` for one |
| Staging volume | created in the staging (or destination) schema | `data_staging_options`, to choose the schema |

Two same-name tables in one pipeline work when each goes to its own destination schema or name: each destination keeps only its own source's rows and changes. The [MySQL limits page](https://docs.databricks.com/ingestion/lakeflow-connect/mysql-limits) states the rule: not into the **same** destination schema. Check the source's limits page for its current wording.

Pipeline create doesn't check the mapping, so check it before deploying:

- **Two sources to one destination table:** the create succeeds, then the first update fails with `CDC_MULTIPLE_SOURCE_TABLES_MAPPED_TO_SAME_DESTINATION_TABLE`, naming both source tables. Nothing loads.
- **Two destination names that are equal once `.` becomes `_`** (`bronze_raw.orders` and `bronze.raw_orders`): every update fails with `FLOW_DEFINED_IN_MULTIPLE_DATASETS` and the pipeline retries on its own. Rename one.
