# Database connectors

SQL Server (cloud and on-prem) is the GA database connector, as standard or integrated CDC. Postgres CDC is Public Preview and covered below. Release stages change often: check the [connector reference](https://docs.databricks.com/ingestion/lakeflow-connect/) for current status. MySQL CDC and Foreign Catalog connectors are Public Preview and covered briefly; see the [connector reference](https://docs.databricks.com/ingestion/lakeflow-connect/) for their setup.

**Query-based connectors don't use a gateway** — they're a single serverless pipeline. See [3-query-based-connectors.md](3-query-based-connectors.md).

---

## Integrated CDC (default) vs standard CDC

There are two ways to run a database CDC connector:

- **Integrated CDC:** one pipeline. Extraction runs inside each pipeline update, so there's no always-on gateway. The pipeline points at the UC connection with `connection_name` and sets `connector_type: CDC`.
- **Standard CDC:** two pipelines, an always-on ingestion gateway plus an ingestion pipeline that references it by `ingestion_gateway_id`. See [The gateway pattern](#the-gateway-pattern) below.

**Which one to build:**
- The user asks for CDC and doesn't say which kind → **integrated**, where the source supports it.
- The user asks for "standard CDC", "two-pipeline CDC", or "gateway" → **standard**.
- The source doesn't support integrated CDC (check the docs; PostgreSQL had no integrated CDC when this was written) → say so, and offer standard CDC. Don't switch silently: standard runs an always-on gateway, which costs more.

**Check current status before you build.** Integrated CDC is changing quickly: which sources support it, release stage, classic vs serverless compute, the runtime channel, and how long updates run all change from release to release. Treat the table below as a starting point, not the answer. Before creating a pipeline, fetch the current page for the source (or find it from `https://docs.databricks.com/llms.txt`, see **databricks-docs**) and use what it says:

| Source | Integrated CDC page | Last seen |
|---|---|---|
| SQL Server | [sql-server-integrated-pipeline](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/sql-server-integrated-pipeline) | GA |
| MySQL | [mysql-integrated-pipeline](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/mysql-integrated-pipeline) | Beta |
| Oracle | [oracle-integrated-overview](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/oracle-integrated-overview) | Beta; `channel: PREVIEW`; serverless works; integrated only |
| PostgreSQL | none yet: standard CDC only | — |
| All | [smart-closure](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/smart-closure): how long an update runs | — |

From the page, check: release stage, whether it needs `channel: PREVIEW`, whether it supports serverless, classic, or both (to know what the default may pick; still leave `serverless` unset, see [Compute: serverless or classic](#compute-serverless-or-classic)), and the source prerequisites. If a source now has an integrated CDC page that isn't listed here, use it. If the docs can't be fetched, say so and follow the table above as written. For a source marked "none yet", don't try integrated CDC: offer standard CDC. Don't add settings the table doesn't list (for example, only set `channel: PREVIEW` where the table or docs page requires it). Then let the workspace correct you.

**The workspace's error wins over the docs and this skill.** For example, a workspace that enforces serverless rejects classic compute (a `serverless` left unset that defaults to classic, or `serverless: false`) with "You must use serverless compute in this workspace". Set `serverless: true` and retry. If it rejects `connector_type: CDC`, integrated CDC isn't enabled on that workspace (the Databricks account team turns it on): tell the user rather than falling back to standard CDC.

### Source prerequisites

The source setup is the same as for standard CDC:

- **SQL Server:** CDC or change tracking enabled on each table ([below](#sql-server-change-tracking-vs-cdc)), on the **primary** instance (no read replicas). SCD Type 2 needs CDC, not change tracking.
- **MySQL:** binary log on, with `binlog_format=ROW` and `binlog_row_image=FULL`.

Check through the existing connection with `remote_query` (read-only), using the same `--file` pattern as Step 0 in [3-query-based-connectors.md](3-query-based-connectors.md):

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

**TLS on the connection.** The CDC extractor enforces TLS. On MySQL, query-based ingestion through the same connection works without a trusted certificate and only CDC fails; on SQL Server, every query fails too (`FAILED_JDBC.CONNECTION`, see the connection table in [3-query-based-connectors.md](3-query-based-connectors.md)). A source with a self-signed certificate (a VM or container install of MySQL or SQL Server) fails at the start of the update: `Extractor FAILED`, with `INGESTION_GATEWAY_CONNECTION_ERROR.TLS_VALIDATION_FAILURE ... TLS is enforced but no server certificate provided. Provide a server certificate or set trustServerCertificate=true`. Add `"trustServerCertificate": "true"` to the connection's options with `databricks connections update <connection> --json @opts.json` (pass every option, including the password, from a private file), then start a new update. Managed services with trusted certificates don't need it.

A table with neither CDC nor change tracking can't be ingested with CDC: enable it at the source (ask first; it's a change to the customer's database), or use query-based for that table.

### Pipeline

One pipeline per source. Create the destination schema and a staging schema first; the pipeline creates a staging volume in the staging schema, so it needs `CREATE VOLUME` there.

These fields are stable: `connection_name`, `connector_type: CDC`, `objects`, and `data_staging_options`. Set `channel` from the current docs page, as above. Leave `serverless` out and read back what the pipeline ran on ([Compute: serverless or classic](#compute-serverless-or-classic)).

```yaml
resources:
  pipelines:
    sqlserver_cdc:
      name: <name>                     # <firstname><lastname>_<connection>_<epoch>, see SKILL.md
      catalog: ${var.catalog}
      schema: sqlserver_cdc_raw
      ingestion_definition:
        connection_name: my_sqlserver_connection
        connector_type: CDC
        objects:
          - table:
              source_catalog: <database>
              source_schema: dbo
              source_table: customers
              destination_catalog: ${var.catalog}
              destination_schema: sqlserver_cdc_raw
              table_configuration:
                scd_type: SCD_TYPE_1     # SCD_TYPE_2 needs SQL Server CDC, not change tracking
        data_staging_options:
          catalog_name: ${var.catalog}
          schema_name: sqlserver_cdc_staging
```

Tested on a serverless-only workspace (Azure, 2026-10), against containers on a cloud VM: SQL Server 2022 (change tracking on a table with a primary key, SQL Server CDC on one without), MySQL 8.4, and Oracle 23ai Free (integrated, `channel: PREVIEW`, `serverless: true`, `source_catalog` = the PDB, `FREEPDB1`, with the connection's `service_name` = the CDB's service; see [3-query-based-connectors.md](3-query-based-connectors.md) Step 1). Each took the snapshot, then applied 3 inserts, 2 updates, and 2 deletes per table within about 3 minutes while the update was still running. Tables without a primary key ended up with the same rows as the source.

For MySQL, address tables by `source_schema: <mysql-database>` with no `source_catalog`, and add `channel: PREVIEW` while the docs page requires it. The MySQL page said classic compute only, but the serverless-only workspace refused classic ("You must use serverless compute in this workspace"), and the retry with `serverless: true` ran correctly. The example leaves `serverless` out, as everywhere: add `serverless: true` only when the workspace asks for it, as that one did.

```yaml
    mysql_cdc:
      name: <name>
      channel: PREVIEW                 # while the MySQL page says it's required
      catalog: ${var.catalog}
      schema: mysql_cdc_raw
      ingestion_definition:
        connection_name: my_mysql_connection
        connector_type: CDC
        objects:
          - table:
              source_schema: <mysql-database>
              source_table: orders
              destination_catalog: ${var.catalog}
              destination_schema: mysql_cdc_raw
        data_staging_options:
          catalog_name: ${var.catalog}
          schema_name: mysql_cdc_staging
```

### How an update runs: smart closure

Integrated CDC updates are triggered, but they don't stop as soon as the backlog is empty. [Smart closure](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/smart-closure) decides when an update ends:

1. **Minimum runtime first.** No update ends before it.
2. **Then, once caught up,** it ends (logged as `lag-converged`).
3. **Or at the maximum runtime,** if it hasn't caught up (logged as `max-runtime-cap-hit`). The next update resumes from there.
4. **Or on a source schema change.** The update stops and a new one starts with the new schema.

Databricks sets and adjusts the minimum runtime, maximum runtime, and lag threshold, and they aren't configurable. Don't quote fixed durations. What it means in practice:

- **Each update can run much longer than a standard CDC ingestion run,** even with nothing to apply. That's expected, not a hang. In one test, a SQL Server update took about 21 minutes for a snapshot of about 6k rows, ending with "CDC extraction has COMPLETED. Caught up to the source." A MySQL source with no writes kept logging "MySQL binlog stall detected" and was still running after 79 minutes, because the lag couldn't converge on an idle source. Expect an idle source to run until the maximum runtime. In tests and demos, stop it yourself after a set time with `databricks pipelines stop <pipeline-id>`. That cancels the update, and the next update resumes from where it stopped.
- **Don't block on it.** Start the update with `databricks bundle run <key> --no-wait` (or `pipelines start-update`), tell the user it's running, and check it with `databricks pipelines get-update <pipeline-id> <update-id>` until it's `COMPLETED` or `FAILED`. If you have to stop before it finishes, report the update ID and how to check it.
- **What it's doing, and why it stopped:** extraction progress and the closure decision are logged as `operation_progress` and `cdc_extraction_details` events. A running update reports something like "CDC extraction is IN_PROGRESS, running for 5 minutes. Zero snapshots are pending. Will stop once the lag converges." Source-side conditions show up too, such as "MySQL binlog stall detected" on an idle source. Query by event type, not by message text, since the wording changes:

  ```sql
  SELECT timestamp, event_type, message FROM event_log('<pipeline-id>')
  WHERE event_type IN ('operation_progress', 'cdc_extraction_details', 'update_progress')
  ORDER BY timestamp DESC LIMIT 20
  ```

- **Schedule** it with a Jobs `pipeline_task`. Use the interval the smart closure page recommends (it currently suggests starting at 60 minutes). A trigger that fires while an update is still running is skipped. Continuous mode exists as a separate option; check its status in the docs.

### Behaviour that doesn't change between releases

- **The first update takes the initial snapshot,** then later updates apply changes.
- **Tables without a primary key** are keyed on all columns, so identical rows collapse: the table ends up with one row per distinct row. Use SCD Type 2 or add a key at the source if duplicates matter.
- **Immutable after create:** `connection_name` and `connector_type` can't be changed. Create a new pipeline instead.
- **Purged change logs** (CDC retention, change-tracking retention, or binlog expiry) before the pipeline reads them need a full refresh. Schedule runs well inside the retention period.

Report rows per table per run with the query in [5-troubleshooting-and-monitoring.md](5-troubleshooting-and-monitoring.md#rows-per-table-per-run-snapshot-inserts-upserts-deletes).

---

## The gateway pattern

Standard CDC splits into two pipelines (the compute options for each change between releases; check the connector's docs page):

```
          ┌───────────────────────┐
          │  Customer database    │
          │  (SQL Server, etc.)   │
          └──────────┬────────────┘
                     │  CDC / change tracking
                     ▼
          ┌───────────────────────┐
          │  Ingestion gateway    │   classic compute,
          │  (one pipeline)       │   runs in customer VPC
          └──────────┬────────────┘
                     │  change events
                     ▼
          ┌───────────────────────┐
          │  UC Volume staging    │   30-day retention by default
          └──────────┬────────────┘
                     │
                     ▼
          ┌───────────────────────┐
          │  Ingestion pipeline   │   serverless,
          │  (one pipeline)       │   applies CDC into Delta
          └──────────┬────────────┘
                     ▼
          ┌───────────────────────┐
          │  Delta tables in UC   │
          └───────────────────────┘
```

Why each piece:

- **Gateway** runs in the customer's network so the source database is never exposed to Databricks-managed compute. It reads the CDC / change-tracking stream and writes change events into a UC Volume.
- **Staging Volume** decouples the two pipelines: the gateway can run on its own cadence, and the ingestion pipeline can re-process from the Volume without re-reading the source.
- **Ingestion pipeline** is the serverless half — it applies the staged events to Delta with CDC semantics and handles schema evolution.

Trade-offs:

- Two pipelines, two pieces of state. Both must be healthy.
- Gateway compute is billed separately from the ingestion DBUs. It used to be classic only; leave `serverless` unset and read back what it ran on ([Compute: serverless or classic](#compute-serverless-or-classic)). See the [pricing page](https://www.databricks.com/product/pricing/lakeflow-connect) for current rates.
- Staging Volume retention is 30 days. Reprocessing further back requires a snapshot reload.

---

### Standard CDC on a serverless-only workspace (tested)

A classic gateway is rejected with "You must use serverless compute in this workspace". With `"serverless": true` on the gateway, SQL Server standard CDC worked: the gateway runs continuously (it starts as soon as it's created), the applier took the snapshot on its first run, and a second run applied 3 inserts, 2 updates, and 2 deletes per table. The applier is triggered: run it again after source changes, or schedule it. Stop the gateway when the test is done (`databricks pipelines stop <gateway-id>`); it keeps billing otherwise. On the same workspace, MySQL and PostgreSQL gateways were refused as not enabled ([feature gates](#workspace-feature-gates-all-database-sources)), while integrated MySQL CDC worked. Oracle has no standard (gateway) mode in the docs, only integrated.

---

## SQL Server: change tracking vs CDC

SQL Server offers two source mechanisms; pick one per database.

**Change Tracking (CT)** — lightweight. The source tracks "which rows changed since version X" but not the actual change history. The gateway re-reads changed rows from the base table.

- Lower overhead on the source.
- Adequate when downstream only needs the latest state per PK.
- Cannot reconstruct historical change order.

**Change Data Capture (CDC)** — full change log. The source writes inserts/updates/deletes into change tables that the gateway reads directly.

- Higher overhead on the source (separate change tables, log reader job).
- Required when downstream needs per-event history (audit, SCD2 from raw deltas, etc.).

Most pipelines start with CT and switch to CDC only when audit or SCD2 demands it.

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
      name: <base>_gw_<epoch>          # <firstname><lastname>_<connection>_gw_<epoch>, see SKILL.md
      gateway_definition:
        connection_name: my_sqlserver_connection
        gateway_storage_catalog: ${var.catalog}
        gateway_storage_schema: ingestion_staging
        gateway_storage_name: sqlserver_gateway_storage

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
              destination_schema: sqlserver_raw
```

The SDK Python equivalent uses `w.pipelines.create` twice — once with `gateway_definition`, once with `ingestion_definition` referencing the gateway's pipeline ID.

---

## SQL Server on-prem

Same setup as cloud, plus private networking from the gateway to the on-prem source:

- **Azure**: ExpressRoute or VPN gateway between the customer VNet and the on-prem network.
- **AWS**: Direct Connect or Site-to-Site VPN between the customer VPC and the on-prem network.
- **GCP**: Cloud Interconnect or Cloud VPN.

The gateway compute itself runs on Databricks-managed VPC infrastructure inside the customer's workspace, so the private link only needs to extend that far.

---

## Workspace feature gates (all database sources)

Each standard CDC source is enabled per workspace. When it isn't, the gateway create fails with `Ingestion source '<SOURCE>' is not supported in your workspace. Please contact Databricks support to enable this feature for your workspace. (403 PERMISSION_DENIED)`. This has been seen for `POSTGRESQL` and `MYSQL` on a workspace where query-based and integrated CDC for the same sources worked, and for `ORACLE` (integrated CDC) and `POSTGRESQL` (gateway) on a workspace where query-based ingestion from both worked. Report it, don't work around it, and offer the modes that weren't refused: for a refused gateway (standard CDC), integrated CDC or query-based; for a refused integrated CDC source, standard CDC where the source has it, else query-based (Oracle has no standard mode, so query-based). A failed `bundle deploy` can still have created the schemas declared before the pipelines: offer `bundle destroy` so nothing is left behind.

---

## Compute: serverless or classic

A workspace's compute policy wins over the docs: a workspace that requires serverless rejects classic compute with "You must use serverless compute in this workspace" (see the first failure below).

**Leave `serverless` unset on every pipeline and gateway, read back what the system chose, and change it if that fails.** The UI uses the default too; when the default fails, you change the setting yourself. Defaults change over time and differ by workspace, mode, and source, so don't set `serverless` in the bundle or the REST body unless the workspace or the docs page requires it. After the first update starts, read the choice. The update ID is the `update_id` that `start-update` returns, or `.latest_updates[0].update_id` from `databricks pipelines get <pipeline-id>`:

```bash
databricks pipelines get-update <pipeline-id> <update-id> --profile <PROFILE> -o json \
  | yq -p json -o tsv '[(.update.config.serverless | tostring), (.update.cluster_id // "none")]'
# true  none        serverless
# false <cluster>   classic, on its own cluster
# null  ...         not decided yet: read it again
```

`pipelines get` doesn't tell: its `spec.serverless` stays unset, echoing what you sent. **If the create fails on compute, or the update can't reach the source, find out which first (see the two failures below), then set the opposite explicitly and retry.** A wrong host or port, or a source down, gives the same connection error on either compute: flipping won't fix it. For a query-based pipeline the opposite of serverless is classic compute, which is Beta and needs a `clusters` block as well as `serverless: false` ([3-query-based-connectors.md](3-query-based-connectors.md)); change both in one step and say so. Tested 2026-10 on two AWS workspaces and an Azure workspace with classic compute:

| Workspace | `serverless` unset | Opposite |
|---|---|---|
| Classic compute available (AWS, Azure) | Query-based: `config.serverless=true`. Gateway: `false` (classic, its own cluster). Integrated CDC, MySQL and SQL Server (Azure): `false`, classic, its own cluster | Integrated CDC and gateway with `serverless: true`: ran serverless |
| Serverless-only (AWS) | Query-based: `true`. Gateway: refused at create with `You must use serverless compute in this workspace.` | Gateway with `serverless: true`: created and started (`config.serverless=true`) |
| Any | Standard CDC ingestion pipeline (the applier): not tested unset; with `serverless: true` it ran serverless on AWS and Azure | |

**Two different failures, at different times.** Tell them apart before changing anything:

- **The compute isn't allowed.** Seen at create, in seconds: `You must use serverless compute in this workspace.` (a gateway left unset on a serverless-only workspace). A classic cluster you aren't allowed to create (no `allow-cluster-create`, no usable cluster policy) should fail here too, before anything connects (not tested yet). Set the opposite and retry.
- **The compute starts but can't reach the source.** Seen as `Extractor STARTED`, then about a minute later `Extractor FAILED` with `INGESTION_GATEWAY_CONNECTION_ERROR.INCORRECT_SOURCE_HOSTNAME_OR_PORT_SPECIFIED_FAILURE ... Could not connect to the source database`, with the IP's last octet masked as `0`. On the Azure workspace, integrated CDC for MySQL and SQL Server ran on classic and failed this way. The host and port were right: the source's firewall allowed the serverless ranges but not the workspace's classic egress IP, and `remote_query` on the same connection, which runs on serverless, still worked. The same error also comes from a wrong host or port, so check before flipping: run `remote_query` on the same connection ([7-source-sql-through-connection.md](7-source-sql-through-connection.md)), which runs on serverless. If it reaches the source and `update.config.serverless` is `false`, the host is right and the classic egress IP is what's blocked: set `serverless: true` and retry, or have the classic egress IP added to the source's firewall and rerun as is. If `remote_query` fails the same way, fix the host, port, or network first.

**Classic takes longer to start than serverless, so expect the second failure later on classic.** In the event log (`WAITING_FOR_RESOURCES` until `INITIALIZING`), classic integrated CDC waited 7 min 40 s to 8 min 10 s for its cluster. Serverless pipelines and gateways on the same workspace waited 10 s to 1 min 20 s, and 3 min 50 s once, for the first serverless start there. `update.config.serverless` is set as soon as the update starts, so read it right away to know which wait to expect.

To know beforehand whether a workspace has classic compute, run `databricks clusters list-zones --profile <PROFILE>`. A list of zones means classic is there; on Azure, `Azure worker environment does not support zones` also means classic is there. `No such workerEnvironment 'serverless-<workspace id>'` (AWS) or `does not have any associated worker environment` (Azure) means a serverless-only workspace. Any other answer (seen on GCP) doesn't tell, so let the create decide. Classic also needs the `allow-cluster-create` entitlement or a cluster policy you can use.

---

## Database-specific gotchas

| Symptom | Likely cause | Fix |
|---|---|---|
| Gateway requires an instance type unavailable in the region | Default gateway cluster shape not stocked in the target region | Apply a cluster policy override on the gateway pipeline to pin a regionally-available instance type. |
| Snapshot-only mode silently disabled | Snapshot-only is not supported for CDC sources | Use CT instead, or accept incremental mode. |
| Pipeline state diverges from source after 30+ days | Staging Volume retention expired | Resnapshot the affected tables. Increase the Volume retention if reprocessing further back is a recurring need. |
| "Continuous mode not supported" error at create | Lakeflow Connect is triggered-only (no continuous mode) | Use `continuous: false` plus a Jobs schedule. |
| Gateway pipeline succeeds but ingestion pipeline shows no new data | Staging path mismatch between the two pipelines | Confirm `gateway_storage_*` on the gateway matches the staging path the ingestion pipeline reads from. |

---

## PostgreSQL CDC

Same gateway pattern as SQL Server. Postgres logical replication (`pgoutput`) replaces CT/CDC. Supports PostgreSQL 13+, including RDS / Aurora, Azure Database for PostgreSQL Flexible Server, and Cloud SQL.

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

-- One publication and one slot per pipeline pair, named after it: <slot_name> is <base>_<epoch>, the
-- schema's name (e.g. janedoe_my_pg_conn_1792351234). Never share either between pipelines.
CREATE PUBLICATION <slot_name>_pub FOR TABLE <schema>.<table1>, <schema>.<table2>;

-- Slot: only pgoutput is supported, and a slot has one consumer at a time. Create it after the pipelines
-- deploy, so a rejected pipeline doesn't leave an unused slot holding WAL. Slots have no owner: create it
-- as any role with REPLICATION (or rds_replication). No SET ROLE needed; on PG 16+ a non-superuser admin
-- usually can't SET ROLE to a role it created.
SELECT pg_create_logical_replication_slot('<slot_name>', 'pgoutput');
```

Server parameters:

- `max_slot_wal_keep_size` — set a finite value. `-1` lets a stalled slot keep WAL forever and fill the disk.
- `max_replication_slots` / `max_wal_senders` — at least one per replicated database on top of existing use.

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
        gateway_storage_schema: ingestion_staging
        gateway_storage_name: pg_gateway_storage

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
              destination_schema: pg_raw
```

Check the `source_configurations` shape against the [connector reference](https://docs.databricks.com/ingestion/lakeflow-connect/) for your CLI version — it is Public Preview and may change.

**Each pipeline needs its own publication and slot.** The gateway doesn't create either one, and a slot can't be shared. If all you have is the UC connection, create and drop the slot with the notebook in [7-source-sql-through-connection.md](7-source-sql-through-connection.md). `CREATE PUBLICATION` can't go through the connection, so the user runs that one statement.

### Verify the source before creating the gateway

```sql
SELECT rolname, rolreplication FROM pg_roles WHERE rolname = 'databricks_replication';
SELECT * FROM pg_publication WHERE pubname = '<slot_name>_pub';
SELECT slot_name, plugin, active, restart_lsn FROM pg_replication_slots;
SELECT c.relname, c.relreplident FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = '<schema>' AND c.relkind = 'r';   -- 'd' (DEFAULT) or 'f' (FULL)
```

### Postgres-specific gotchas

| Symptom | Likely cause | Fix |
|---|---|---|
| Source disk filling up | Gateway stopped or deleted; the slot keeps holding WAL | Restart the gateway, or drop that pipeline's slot. Set a finite `max_slot_wal_keep_size`. |
| Slot left behind after deleting the pipeline | Slots aren't dropped automatically | `SELECT pg_drop_replication_slot('<slot_name>');`, or `action: drop_slot` in [7-source-sql-through-connection.md](7-source-sql-through-connection.md). Drop only your own slot, by name. |
| Updates / deletes missing columns | `REPLICA IDENTITY DEFAULT` on a table without a PK or with TOASTed columns | `ALTER TABLE ... REPLICA IDENTITY FULL`. |
| Table not replicating | Not in the publication | `ALTER PUBLICATION <slot_name>_pub ADD TABLE <schema>.<table>;` |
| Gateway create fails: "Ingestion source 'POSTGRESQL' is not supported in your workspace" (403) | Postgres CDC isn't enabled on this workspace (query-based Postgres can still work) | Ask the Databricks account team or support to enable it. Don't work around it. If you already created a slot, drop it. The deploy may have created the destination and staging schemas before failing: offer `bundle destroy` to remove them. |
| Slot creation fails | Wrong role, or `wal_level` not `logical` | Create the slot as the `REPLICATION` role, after the publication, with `pgoutput`. |

Using Lakebase as the Postgres source for a demo: see [6-lakebase-as-postgres-source.md](6-lakebase-as-postgres-source.md). For CDC on Lakebase, try creating the publication first: if Lakebase refuses it ("CREATE PUBLICATION is not enabled on this Lakebase instance"), CDC can't run there yet, so use query-based.

---

## Other Public Preview database connectors (brief)

Use the [connector reference](https://docs.databricks.com/ingestion/lakeflow-connect/) for current setup steps:

- **MySQL CDC** — same gateway pattern; binlog replaces CT/CDC.
- **Oracle / Teradata / SQL Server / Postgres / MySQL / MariaDB query-based** — no gateway; see [3-query-based-connectors.md](3-query-based-connectors.md).
- **Snowflake / Redshift / Synapse / BigQuery (Foreign Catalog)** — Lakeflow Connect creates the foreign catalog and materializes the queried subset to Delta. Most useful for warehouse-to-lakehouse migration scenarios.
