# Database source behaviors and errors

What happens on each database source and on the gateway compute, with the error text as the product returns it. Names and IDs are placeholders (`<slot_name>`, `<schema>.<table>`, `<user>`, `<database>`).

Per source:

- PostgreSQL: [database-source-behaviors-postgresql.md](database-source-behaviors-postgresql.md)
- MySQL: [database-source-behaviors-mysql.md](database-source-behaviors-mysql.md)
- SQL Server: [database-source-behaviors-sql-server.md](database-source-behaviors-sql-server.md)
- Oracle: [database-source-behaviors-oracle.md](database-source-behaviors-oracle.md)

## Read the destination, not only the status

- An ingestion update can report `COMPLETED` before the gateway's snapshot finishes. Compare row counts only after the gateway's events show `SNAPSHOT_COMPLETED` for each table. The per-run query is in [troubleshooting-and-monitoring.md](rows-per-run.md#rows-per-table-per-run-snapshot-inserts-upserts-deletes).
- An integrated pipeline can report a low lag while the destination is empty. Count the destination rows, and read the table's history: `DESCRIBE HISTORY` shows `numTargetRowsInserted` for each MERGE version. If it's 0 across the versions, the snapshot wrote nothing.

## Gateway compute

| Symptom | Likely cause | Fix |
|---|---|---|
| At create: "The Gateway pipeline does not allow spark config [...]" | `spark_conf` sets `spark.databricks.cluster.profile` or `spark.master` | Remove those keys. |
| Accepted at create | `num_workers: 0` | On classic compute, a zero-worker gateway never completed an update. Check the classic egress first (next row). |
| `Extractor FAILED` with `INGESTION_GATEWAY_CONNECTION_ERROR.INCORRECT_SOURCE_HOSTNAME_OR_PORT_SPECIFIED_FAILURE`: "Could not connect to the source database" | Classic gateway, and the source's firewall allows only the serverless ranges | Allow the workspace's classic egress IP at the source. It isn't in the published serverless ranges. Or set `serverless: true`. |
| No instances start | Classic standard gateway stays `PENDING` at "Finding instances" | Ask a workspace admin for a node type the workspace has capacity for, or run the gateway on serverless. |
| `You are not authorized to create instance pools` | Creating an instance pool as a non-admin | Ask a workspace admin for instance-pool permission, or run the gateway on serverless. |
| The update fails at start: `_LEGACY_ERROR_TEMP_84_FAILED_TO_PARSE_PIPELINE_CONF` with `CLUSTER_SETUP_FAILURE.EXECUTION_SERVICE_INITIALIZATION_FAILED` on each cluster. No tables are created. | `pipelines.gateway.realtime.maxConnections` set to a non-numeric value (such as `abc`) | Set an integer. |
| Accepted with no error, and the update completes | `pipelines.gateway.realtime.maxConnections` out of range (-1, 0, 100000) | Set a positive integer sized for the source's connection limit. |

## Table limits

| Symptom | Likely cause | Fix |
|---|---|---|
| The hidden CDC staging table counts as one, so 999 source tables is the most. At 1,000, analysis fails: "The number of tables in this pipeline (1001 tables) exceeds the quota of 1000 tables per pipeline. Please reduce the number of tables defined in this pipeline." Retries repeat it. A gateway over the same tables keeps running. | Source tables in one ingestion pipeline (quota 1,000) | Keep each ingestion pipeline at 999 source tables or fewer; split the rest over pipelines. |

The documented limits for integrated CDC are 50 tables for speed-optimized (`runMode` `SPEED`), 500 for scale-optimized (the default), and 300 for triggered ([continuous and integrated CDC](https://docs.databricks.com/ingestion/lakeflow-connect/continuous-integrated-cdc)). Create doesn't check them: a SPEED pipeline with 450 tables and a pipeline with more than 300 tables were both accepted with no table-limit error. Keep a SPEED pipeline to 50 tables, and split larger sets across pipelines.

