# SQL Server connector

SQL Server (cloud and on-prem) is the GA database connector. This file covers the source mechanism (change tracking or CDC), source table changes, and cloud and on-prem setup. Pipeline definitions: [integrated-cdc.md](integrated-cdc.md) and [standard-cdc-gateway.md](standard-cdc-gateway.md).

## SQL Server: change tracking vs CDC

SQL Server offers two source mechanisms; pick one per database.

Behaviors and errors on SQL Server (utility grants, `TRUNCATE`, capture instances, cursors, governor limits): [database-source-behaviors-sql-server.md](database-source-behaviors-sql-server.md).

**Change Tracking (CT)** — lightweight. The source tracks "which rows changed since version X" but not the actual change history. The gateway re-reads changed rows from the base table.

- Lower overhead on the source.
- Adequate when downstream only needs the latest state per PK.
- Cannot reconstruct historical change order.

**Change Data Capture (CDC)** — full change log. The source writes inserts/updates/deletes into change tables that the gateway reads directly.

- Higher overhead on the source (separate change tables, log reader job).
- Required when downstream needs per-event history (audit, SCD2 from raw deltas, etc.).

Most pipelines start with CT and switch to CDC only when audit or SCD2 demands it.

**`SCD_TYPE_2` needs CDC on the table, and pipeline create doesn't check it.** An `SCD_TYPE_2` table on change tracking is accepted. Its inserts don't arrive until the next schema change on the table, and no error is reported (integrated CDC). Before creating the pipeline, check each table you'll set to `SCD_TYPE_2` (on the source, or through the connection with `remote_query`, see [source-sql-through-connection.md](source-sql-through-connection.md)):

```sql
SELECT SCHEMA_NAME(t.schema_id) + '.' + t.name AS source_table, t.is_tracked_by_cdc,
       CASE WHEN ct.object_id IS NULL THEN 0 ELSE 1 END AS is_change_tracked
FROM sys.tables t LEFT JOIN sys.change_tracking_tables ct ON ct.object_id = t.object_id
WHERE SCHEMA_NAME(t.schema_id) + '.' + t.name IN ('dbo.customers', 'dbo.orders');
```

`is_tracked_by_cdc = 1` is what `SCD_TYPE_2` needs. A table with only `is_change_tracked = 1` gets `SCD_TYPE_1`, or CDC enabled on it first (ask: it's a change to the customer's database). If a change-tracking table must stay on `SCD_TYPE_2`, the connection's login needs `VIEW SERVER STATE` (`GRANT VIEW SERVER STATE TO <login>;` in `master`): the change-tracking path reads each change's commit time from `sys.dm_tran_commit_table`, which returns no rows to a login without it, and that holds the changes. The utility objects don't grant it.

**Source table changes:**

| Symptom | Likely cause | Fix |
|---|---|---|
| Applied like any other change | `DELETE` every row, then `INSERT` | Nothing |
| SQL Server refuses it (Msg 4711). CDC off, `TRUNCATE`, CDC on (`lakeflowSetupChangeDataCapture`): the table stops with `INGESTION_GATEWAY_CDC_LOG_TRUNCATED` | `TRUNCATE` on a CDC table | Full refresh of the table, or `auto_full_refresh_policy` on it |
| Depends on the build: either no error and the destination keeps the truncated rows, or the update fails with `INGESTION_GATEWAY_UNTRACEABLE_TABLE_MODIFICATION` and later inserts don't arrive ([SQL Server behaviors](database-source-behaviors-sql-server.md)) | `TRUNCATE` on a change-tracking table | Full refresh of the table after every `TRUNCATE`. Prefer `DELETE` on change-tracking tables |
| **No error; the destination keeps the old rows** | Drop and recreate a change-tracking table | Full refresh of the table |
| `INGESTION_GATEWAY_CDC_LOG_TRUNCATED`; a full refresh then reports success but keeps the old rows, and the next update fails with `MANAGED_INGESTION_CANNOT_CHANGE_SOURCE_FOR_EXISTING_DESTINATION_TABLE` | Drop and recreate a CDC table | Map the recreated table to a new `destination_table` |
| SQL Server refuses it while CDC captures the table (Msg 22979, 4928). CDC off, change, CDC on: `INGESTION_GATEWAY_CDC_LOG_TRUNCATED` | Primary key change or column rename on a CDC table | Full refresh, or the policy |
| `CDC_INCOMPATIBLE_SCHEMA_CHANGES.COLUMNS_ADDED_AND_DELETED` | Column rename on a change-tracking table | Full refresh, or the policy |
| Applied; the destination keeps the old type and the dropped column | Widen or narrow a column type, `DROP COLUMN` | Nothing (full refresh to take the new type) |
| The new table gets its own snapshot; the others aren't reset | Add a table to the ingestion definition | Nothing |

---

## SQL Server cloud setup

Prerequisites:

1. **SQL Server 2012+** (cloud-managed: Azure SQL DB, Azure SQL MI, RDS for SQL Server).
2. **A dedicated database user** for the connector's ongoing reads, granted the minimum CT/CDC privileges for the source (see the connector reference). `db_owner` is the broad, simple alternative but is over-privileged for steady-state ingestion — if you use it, keep it to a DBA enabling CDC transiently, not the connector's read account.
3. **CT or CDC enabled** on the source tables (`ALTER DATABASE ... SET CHANGE_TRACKING = ON` for CT; `sys.sp_cdc_enable_table` for CDC).
4. **Network reachability** — the gateway compute must reach the source database. For cloud SQL Server this is usually VPC peering or PrivateLink.

A DAB stub with both pipelines:

```yaml
resources:
  pipelines:
    sqlserver_gateway:
      name: <base>_gw_<epoch>          # <firstname><lastname>_<connection>_gw_<epoch>, see default-names.md
      gateway_definition:
        connection_name: my_sqlserver_connection
        gateway_storage_catalog: ${var.catalog}
        gateway_storage_schema: <base>_<epoch>_stg
        gateway_storage_name: <base>_gw_<epoch>

    sqlserver_ingestion:
      name: <base>_ap_<epoch>          # the same epoch as the gateway
      ingestion_definition:
        ingestion_gateway_id: ${resources.pipelines.sqlserver_gateway.id}
        objects:
          - table:
              source_catalog: sales_db
              source_schema: dbo
              source_table: orders
              destination_catalog: ${var.catalog}
              destination_schema: <base>_<epoch>   # the schema takes the pair's name
```

The SDK Python equivalent uses `w.pipelines.create` twice — once with `gateway_definition`, once with `ingestion_definition` referencing the gateway's pipeline ID.

---

## SQL Server on-prem

Same setup as cloud, plus private networking from the gateway to the on-prem source:

- **Azure**: ExpressRoute or VPN gateway between the customer VNet and the on-prem network.
- **AWS**: Direct Connect or Site-to-Site VPN between the customer VPC and the on-prem network.
- **GCP**: Cloud Interconnect or Cloud VPN.

The gateway compute itself runs on Databricks-managed VPC infrastructure inside the customer's workspace, so the private link only needs to extend that far.
