# Database source behaviors and errors

Error text, codes and limits change between releases. For an error on a database source, read the source's current troubleshooting and limitations pages first, and use what they say:

| Source | Troubleshooting | Limitations | This skill |
|---|---|---|---|
| SQL Server | [sql-server-troubleshoot](https://docs.databricks.com/ingestion/lakeflow-connect/sql-server-troubleshoot) | [sql-server-limits](https://docs.databricks.com/ingestion/lakeflow-connect/sql-server-limits) | [sql-server-cdc.md](sql-server-cdc.md#source-behaviors-and-errors) |
| PostgreSQL | [postgresql-troubleshoot](https://docs.databricks.com/ingestion/lakeflow-connect/postgresql-troubleshoot) | [postgresql-limits](https://docs.databricks.com/ingestion/lakeflow-connect/postgresql-limits) | [postgresql-cdc.md](postgresql-cdc.md#source-behaviors-and-errors) |
| MySQL | [mysql-troubleshoot](https://docs.databricks.com/ingestion/lakeflow-connect/mysql-troubleshoot) | [mysql-limits](https://docs.databricks.com/ingestion/lakeflow-connect/mysql-limits) | [MySQL](#mysql) below |
| Oracle | [oracle-troubleshoot](https://docs.databricks.com/ingestion/lakeflow-connect/oracle-troubleshoot) | [oracle-limits](https://docs.databricks.com/ingestion/lakeflow-connect/oracle-limits) | [Oracle](#oracle) below |
| Query-based | — | [query-based-limits](https://docs.databricks.com/ingestion/lakeflow-connect/query-based-limits) | [query-based-pipeline.md](query-based-pipeline.md#gotchas) |

Each source's section keeps what follows from how the source database or the pipeline works: source settings, grants, logs and their retention, and changes that report no error. Names are placeholders (`<schema>.<table>`, `<user>`, `<database>`).

## Read the destination, not only the status

- An ingestion update can report `COMPLETED` before the gateway's snapshot finishes. Compare row counts only after the gateway's events show `SNAPSHOT_COMPLETED` for each table. The per-run query is in [rows-per-run.md](rows-per-run.md#rows-per-table-per-run-snapshot-inserts-upserts-deletes).
- A low lag doesn't show that rows were written. Count the destination rows, and read the table's history: `DESCRIBE HISTORY` shows `numTargetRowsInserted` for each MERGE version. If it's 0 across the versions, the snapshot wrote nothing.
- After every first load and full refresh, compare source and destination row counts per table.

## Gateway compute

| Symptom | Likely cause | Fix |
|---|---|---|
| At create: "The Gateway pipeline does not allow spark config [...]" | `spark_conf` sets `spark.databricks.cluster.profile` or `spark.master` | Remove those keys. |
| `Extractor FAILED` with `INGESTION_GATEWAY_CONNECTION_ERROR.INCORRECT_SOURCE_HOSTNAME_OR_PORT_SPECIFIED_FAILURE`: "Could not connect to the source database" | Classic gateway, and the source's firewall allows only the serverless ranges | Allow the workspace's classic egress IP at the source; it isn't in the published serverless ranges. Or set `serverless: true`. Rule out a wrong host or port first ([two failures](compute-and-feature-gates.md#compute-serverless-or-classic)). |
| The classic gateway stays `PENDING` at "Finding instances" | No capacity for the node type | Ask a workspace admin for a node type the workspace has capacity for, or run the gateway on serverless. |
| `You are not authorized to create instance pools` | Creating an instance pool as a non-admin | Ask a workspace admin for instance-pool permission, or run the gateway on serverless. |

## Table limits

The ingestion pipeline's hidden CDC staging table counts toward its table quota, so the most source tables is one fewer than the quota. Over it, analysis fails with a message that names the quota ("The number of tables in this pipeline (1001 tables) exceeds the quota of 1000 tables per pipeline"), and retries repeat it. Take the current limits from the source's limitations page and the [continuous and integrated CDC](https://docs.databricks.com/ingestion/lakeflow-connect/continuous-integrated-cdc) page. Create can accept more tables than the documented limits: keep each pipeline within them, and split larger sets across pipelines.

## MySQL

Read the current [MySQL troubleshooting](https://docs.databricks.com/ingestion/lakeflow-connect/mysql-troubleshoot) and [limitations](https://docs.databricks.com/ingestion/lakeflow-connect/mysql-limits) pages first: error text, schema-change handling (added, renamed and dropped columns) and limits change between releases. Below is what follows from how MySQL and the pipeline work. Source prerequisites (`binlog_format=ROW`, `binlog_row_image=FULL`): [integrated-cdc.md](integrated-cdc.md#source-prerequisites).

| Symptom | Likely cause | Fix |
|---|---|---|
| `SHOW REPLICAS` is empty while the pipeline reads the binlog | The pipeline reads the binlog as a client, not a replica | Count its sessions in `SHOW PROCESSLIST`: one `Binlog Dump` row per reader, with its address as host. The row can stay for minutes after the update ends; wait for it to close before a run you measure. Don't `KILL` it on a shared source: other pipelines and replicas show the same `Binlog Dump` command. |
| A table's flow fails with `MYSQL_BINLOG_FILE_DELETED` | The binlog files expired (`binlog_expire_logs_seconds`) before the pipeline read them | Full refresh the table, and keep the binlog well past the schedule interval. |
| Every ingestion update fails at analysis ("Failed to resolve flow" for the table's flows) | A source table was dropped | Remove the dropped table from the ingestion pipeline. |
| The flow reports `MISSING_TABLE_IN_SOURCE` and the update fails at analysis, with no privilege message; schema exploration lists one table fewer | `SELECT` revoked from the ingestion login on that table | Grant `SELECT` on the table again, then check that schema exploration lists it. |
| The added column lands at the end of the destination table, not in its named position | `ADD COLUMN ... FIRST` or `ADD COLUMN ... AFTER` | Select columns by name. |

## Oracle

Read the current [Oracle troubleshooting](https://docs.databricks.com/ingestion/lakeflow-connect/oracle-troubleshoot) and [limitations](https://docs.databricks.com/ingestion/lakeflow-connect/oracle-limits) pages first. They cover supplemental logging, listener services, multi-tenant access to change data, archive logs purged before processing, and skipped tables; error text and the grant list change between releases. Below is what follows from how Oracle and LogMiner work. Connection and `source_catalog`: [query-based-pipeline.md](query-based-pipeline.md#step-1--uc-connection).

| Symptom | Likely cause | Fix |
|---|---|---|
| `INGESTION_GATEWAY_INTERNAL_ERROR` ("cannot recover"), with no cause in the pipeline events; on the source, `DBMS_LOGMNR.ADD_LOGFILE` fails with `ORA-65040` | The connection uses a PDB's service name; LogMiner runs in the root container | Set the connection's `service_name` to the CDB's service, and name the PDB in each object's `source_catalog`. |
| `ORA-01045: Login denied` (`INGESTION_GATEWAY_CONNECTION_ERROR.SOURCE_AUTHENTICATION_FAILURE`) | The user lacks `CREATE SESSION` | Grant `CREATE SESSION`. Missing privileges show up one at a time: session, then the dictionary views, then table `SELECT`. |
| `ORA-00942: table or view does not exist` on a `V_$` view or a table (`GENERIC_SNAPSHOT_FAILURE`), or `INGESTION_GATEWAY_CDC_NOT_ENABLED`: "Requires following permissions : SELECT on <object>, ..." | The user lacks a dictionary grant or `SELECT` on the table | Apply the grants in [Configure Oracle](https://docs.databricks.com/ingestion/lakeflow-connect/oracle-integrated-setup), then grant each object the message lists by name (`EXECUTE` on `DBMS_LOGMNR` and `DBMS_LOGMNR_D`, `SELECT` on `DBA_SEGMENTS` and the `V_$` and `GV_$` views). `SELECT ANY DICTIONARY` doesn't satisfy the check. |
| Schema exploration finds 0 tables; the update stays `INITIALIZING` with every snapshot `SCHEDULED` and no error event | A restricted user without the dictionary grants | Apply the grants above, then check that exploration finds the tables before starting the update. |
| `MISSING_TABLE_IN_SOURCE`: "No catalog or schema was found for replication" | A common user without `SET CONTAINER` and `CONTAINER_DATA=ALL` | Grant both multi-container privileges, then restart the pipeline. |
| `ORACLE_ARCHIVE_LOG_FILES_UNAVAILABLE`, and the update can still complete with its flows failed | Archive logs removed before the pipeline read them | Raise the recovery-area retention, full refresh the named tables, and read the flow events rather than the update status. The removal affects every pipeline that reads the same container. |
| `TABLE_SKIPPED_DUE_TO_UNSUPPORTED_COLUMN_DATA_TYPE` (`XMLTYPE`); the table gets no snapshot and the update waits with it `SCHEDULED` | LogMiner skips a whole table that has an unsupported column type | Remove the table from the pipeline. |
| `[INGESTION_GATEWAY_TABLE_DROPPED]`, and every table in the pipeline stops | A source table dropped while the pipeline runs | Remove the dropped table from the ingestion pipeline. |
| `ORA-01017: invalid credential or not authorized; logon denied`, repeated on retry | The source password changed; the connection keeps the old one | Update the connection with the current password, then start an update. |
| A `NUMBER` column with no precision or scale lands as a string with the exact digits | No declared precision to map to a decimal | Cast in queries, or declare a precision and scale on the source. |
