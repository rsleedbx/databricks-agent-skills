# PostgreSQL CDC

Same gateway pattern as SQL Server. Postgres logical replication (`pgoutput`) replaces CT/CDC. Supports PostgreSQL 13+, including RDS / Aurora, Azure Database for PostgreSQL Flexible Server, and Cloud SQL.

Slot creation and loss, `wal_level`, replica identity and TLS errors, with their exact text and fixes: [database-source-behaviors-postgresql.md](database-source-behaviors-postgresql.md).

### Source setup

Run as an admin or the table owner — the replication user can't create its own grants or publications.

```sql
SHOW wal_level;                       -- must be 'logical' (RDS/Aurora: rds.logical_replication = 1)

CREATE USER databricks_replication WITH PASSWORD '<strong-generated-password>';
GRANT CONNECT ON DATABASE <db> TO databricks_replication;
GRANT USAGE ON SCHEMA <schema> TO databricks_replication;
GRANT SELECT ON TABLE <schema>.<table> TO databricks_replication;
ALTER USER databricks_replication WITH REPLICATION;
-- RDS/Aurora only: GRANT rds_replication TO databricks_replication;

-- Tables with a PK and no large TOASTable columns: DEFAULT. Otherwise (or no PK): FULL.
ALTER TABLE <schema>.<table> REPLICA IDENTITY DEFAULT;

-- One publication and one slot per pipeline pair, named after it: <slot_name> is the pair's <base>_<epoch>
-- (e.g. janedoe_my_pg_conn_1792351234), not the destination schema's name. Never share either between pipelines.
CREATE PUBLICATION <slot_name>_pub FOR TABLE <schema>.<table1>, <schema>.<table2>;

-- Slot: only pgoutput is supported, and a slot has one consumer at a time. Create it after the pipelines
-- deploy, so a rejected pipeline doesn't leave an unused slot holding WAL. Slots have no owner: create it
-- as any role with REPLICATION (or rds_replication). No SET ROLE needed; on PG 16+ a non-superuser admin
-- usually can't SET ROLE to a role it created.
SELECT pg_create_logical_replication_slot('<slot_name>', 'pgoutput');
```

Server parameters:

- `max_slot_wal_keep_size` — set a finite value. `-1` lets a stalled slot keep WAL forever and fill the disk.
- `max_replication_slots` / `max_wal_senders` — each pipeline pair gets one slot. Set these to the total number of pipeline pairs on top of existing use (one per pair, plus headroom).

### Pipelines

The UC connection uses `connection_type: POSTGRESQL` with the replication user's credentials. The ingestion pipeline names the slot and publication per source database:

```yaml
resources:
  pipelines:
    pg_gateway:
      name: <base>_gw_<epoch>
      gateway_definition:
        connection_name: my_pg_connection
        gateway_storage_catalog: ${var.catalog}
        gateway_storage_schema: <base>_<epoch>_stg
        gateway_storage_name: <base>_gw_<epoch>

    pg_ingestion:
      name: <base>_ap_<epoch>
      ingestion_definition:
        ingestion_gateway_id: ${resources.pipelines.pg_gateway.id}
        source_type: POSTGRESQL
        source_configurations:
          - catalog:
              source_catalog: <db>
              postgres:
                slot_config:
                  slot_name: <slot_name>             # this pipeline's own slot
                  publication_name: <slot_name>_pub  # this pipeline's own publication
        objects:
          - table:
              source_catalog: <db>
              source_schema: public
              source_table: orders
              destination_catalog: ${var.catalog}
              destination_schema: <base>_<epoch>
```

Check the `source_configurations` shape against the [connector reference](https://docs.databricks.com/ingestion/lakeflow-connect/) for your CLI version — it is Public Preview and may change.

**Each pipeline needs its own publication and slot.** The gateway doesn't create either one, and a slot can't be shared. If all you have is the UC connection, create and drop the slot with the notebook in [source-sql-through-connection.md](source-sql-through-connection.md). `CREATE PUBLICATION` can't go through the connection, so the user runs that one statement.

### Verify the source: before deploying, and the slot after

Before `bundle deploy`, the role, the publication and the replica identity must be in place, and no slot may have this pipeline's name yet. The slot itself comes after the pipelines deploy (see above): then the same slot query shows it, `active` once the gateway runs.

```sql
SELECT rolname, rolreplication FROM pg_roles WHERE rolname = 'databricks_replication';
SELECT * FROM pg_publication WHERE pubname = '<slot_name>_pub';
SELECT slot_name, plugin, active, restart_lsn FROM pg_replication_slots WHERE slot_name = '<slot_name>';   -- none before deploy
SELECT c.relname, c.relreplident FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = '<schema>' AND c.relkind = 'r';   -- 'd' (DEFAULT) or 'f' (FULL)
```

### Postgres-specific gotchas

Each error with its exact text, and the behaviors on a named slot after delete: [database-source-behaviors-postgresql.md](database-source-behaviors-postgresql.md).

| Symptom | Likely cause | Fix |
|---|---|---|
| Source disk filling up | Gateway stopped or deleted; the slot keeps holding WAL | Restart the gateway, or drop that pipeline's slot. Set a finite `max_slot_wal_keep_size`. |
| Slot left behind after deleting the pipeline | Slots aren't dropped automatically | `SELECT pg_drop_replication_slot('<slot_name>');`, or `action: drop_slot` in [source-sql-through-connection.md](source-sql-through-connection.md). Drop only your own slot, by name. |
| Updates / deletes missing columns | `REPLICA IDENTITY DEFAULT` on a table without a PK or with TOASTed columns | `ALTER TABLE ... REPLICA IDENTITY FULL`. |
| Table not replicating | Not in the publication | `ALTER PUBLICATION <slot_name>_pub ADD TABLE <schema>.<table>;` |
| Gateway create fails: "Ingestion source 'POSTGRESQL' is not supported in your workspace" (403) | Postgres CDC isn't enabled on this workspace (query-based Postgres can still work) | Ask the Databricks account team or support to enable it. Don't work around it. If you already created a slot, drop it. The deploy may have created the destination and staging schemas before failing: offer `bundle destroy` to remove them. |
| Slot creation fails | Wrong role, or `wal_level` not `logical` | Create the slot as the `REPLICATION` role, after the publication, with `pgoutput`. |
| Gateway: `INGESTION_GATEWAY_CONNECTION_ERROR.SOURCE_AUTHENTICATION_FAILURE` "… The server does not support SSL" | The gateway requires SSL; `remote_query` through the same connection works without it | Turn on `ssl` on the source. With a self-signed certificate, add `"trustServerCertificate": "true"` to the connection's options |
| A table stops with `INGESTION_GATEWAY_DDL_NOT_SUPPORTED.UNSUPPORTED_DDL` after `ADD COLUMN` (nullable or not), `DROP COLUMN` or `RENAME COLUMN` | By default, Postgres column changes aren't applied in place | Full refresh of the table, or `auto_full_refresh_policy` on it. Type changes and a `TRUNCATE` (`…TRUNCATE_TABLE`, also covered by the policy) are handled; the destination keeps a column's old type |
| `INGESTION_GATEWAY_CDC_NOT_ENABLED`: "has a TOAST-able type column, and needs FULL REPLICA IDENTITY" | A table with an unbounded TOAST-able column (`TEXT` and alike) under `REPLICA IDENTITY DEFAULT`, including after adding such a column or switching a table to `DEFAULT` or `NOTHING` (it fails at the next changes, not at the DDL) | `ALTER TABLE … REPLICA IDENTITY FULL`, then a full refresh of the table. Bounded types (`VARCHAR(n)`) are fine under `DEFAULT` |

Using Lakebase as the Postgres source for a demo: see [lakebase-as-postgres-source.md](lakebase-as-postgres-source.md). For CDC on Lakebase, try creating the publication first: if Lakebase refuses it ("CREATE PUBLICATION is not enabled on this Lakebase instance"), CDC can't run there yet, so use query-based.

---

## Other database connectors (brief)

Use the [connector reference](https://docs.databricks.com/ingestion/lakeflow-connect/) for current setup steps:

- **MySQL CDC**: the integrated CDC pipeline (GA, no separate gateway pipeline) is the default ([integrated-cdc.md](integrated-cdc.md), [docs](https://docs.databricks.com/ingestion/lakeflow-connect/mysql-integrated-pipeline)); the gateway-based pattern is Public Preview. Binlog replaces CT/CDC.
- **Oracle / Teradata / SQL Server / Postgres / MySQL / MariaDB query-based** (GA): no separate gateway pipeline; see [query-based-connectors.md](query-based-connectors.md).
- **Snowflake / Redshift / Synapse / BigQuery (Foreign Catalog)** (GA): you create the foreign catalog with Lakehouse Federation; Lakeflow Connect reads through it and materializes the queried subset to Delta. Most useful for warehouse-to-lakehouse migration scenarios.
