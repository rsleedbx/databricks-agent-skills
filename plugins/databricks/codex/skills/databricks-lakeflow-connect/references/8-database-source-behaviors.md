# Database source behaviors and errors

What happens on each database source and on the gateway compute, with the error text as the product returns it. Names and IDs are placeholders (`<slot_name>`, `<schema>.<table>`, `<user>`, `<database>`). Each section ends with the engine version the behavior was tested on.

Per source:

- PostgreSQL: [8-database-source-behaviors-postgresql.md](8-database-source-behaviors-postgresql.md)
- MySQL: [8-database-source-behaviors-mysql.md](8-database-source-behaviors-mysql.md)
- SQL Server: [8-database-source-behaviors-sql-server.md](8-database-source-behaviors-sql-server.md)
- Oracle: [8-database-source-behaviors-oracle.md](8-database-source-behaviors-oracle.md)

## Read the destination, not only the status

- An ingestion update can report `COMPLETED` before the gateway's snapshot finishes. Compare row counts only after the gateway's events show `SNAPSHOT_COMPLETED` for each table. The per-run query is in [5-troubleshooting-and-monitoring.md](5a-rows-per-run.md#rows-per-table-per-run-snapshot-inserts-upserts-deletes).
- An integrated pipeline can report a low lag while the destination is empty. Count the destination rows, and read the table's history: `DESCRIBE HISTORY` shows `numTargetRowsInserted` for each MERGE version. If it's 0 across the versions, the snapshot wrote nothing.

## Gateway compute

| Condition | Error or symptom | What to do |
|---|---|---|
| `spark_conf` sets `spark.databricks.cluster.profile` or `spark.master` | At create: "The Gateway pipeline does not allow spark config [...]" | Remove those keys. |
| `num_workers: 0` | Accepted at create | On classic compute, a zero-worker gateway never completed an update. Check the classic egress first (next row). |
| Classic gateway, and the source's firewall allows only the serverless ranges | `Extractor FAILED` with `INGESTION_GATEWAY_CONNECTION_ERROR.INCORRECT_SOURCE_HOSTNAME_OR_PORT_SPECIFIED_FAILURE`: "Could not connect to the source database" | Allow the workspace's classic egress IP at the source. It isn't in the published serverless ranges. Or set `serverless: true`. |
| Classic standard gateway stays `PENDING` at "Finding instances" | No instances start | Ask a workspace admin for a node type the workspace has capacity for, or run the gateway on serverless. |
| Creating an instance pool as a non-admin | `You are not authorized to create instance pools` | Ask a workspace admin for instance-pool permission, or run the gateway on serverless. |
| `pipelines.gateway.realtime.maxConnections` set to a non-numeric value (such as `abc`) | The update fails at start: `_LEGACY_ERROR_TEMP_84_FAILED_TO_PARSE_PIPELINE_CONF` with `CLUSTER_SETUP_FAILURE.EXECUTION_SERVICE_INITIALIZATION_FAILED` on each cluster. No tables are created. | Set an integer. |
| `pipelines.gateway.realtime.maxConnections` out of range (-1, 0, 100000) | Accepted with no error, and the update completes | Set a positive integer sized for the source's connection limit. |

Tested 2026-10-25 on SQL Server 2022. The `maxConnections` rows: tested 2026-10-26 on SQL Server 2022 CU27.

## Table limits

| Limit | Behavior | What to do |
|---|---|---|
| Source tables in one ingestion pipeline (quota 1,000) | The hidden CDC staging table counts as one, so 999 source tables is the most. At 1,000, analysis fails: "The number of tables in this pipeline (1001 tables) exceeds the quota of 1000 tables per pipeline. Please reduce the number of tables defined in this pipeline." Retries repeat it. A gateway over the same tables keeps running. | Keep each ingestion pipeline at 999 source tables or fewer; split the rest over pipelines. |

Tested 2026-10-26 on SQL Server 2022 CU27.

The documented limits for integrated CDC are 50 tables for speed-optimized (`runMode` `SPEED`), 500 for scale-optimized (the default), and 300 for triggered ([continuous and integrated CDC](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/continuous-integrated-cdc)). Create doesn't check them: a SPEED pipeline with 450 tables and a pipeline with more than 300 tables were both accepted with no table-limit error. Keep a SPEED pipeline to 50 tables, and split larger sets across pipelines.

Tested 2026-10-25 on SQL Server 2022.
