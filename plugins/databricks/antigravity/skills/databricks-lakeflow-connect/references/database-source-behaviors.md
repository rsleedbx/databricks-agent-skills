# Database source behaviors and errors

Error text, codes and limits change between releases. For an error on a database source, read the source's current troubleshooting and limitations pages first, and use what they say:

| Source | Troubleshooting | Limitations | This skill |
|---|---|---|---|
| SQL Server | [sql-server-troubleshoot](https://docs.databricks.com/ingestion/lakeflow-connect/sql-server-troubleshoot) | [sql-server-limits](https://docs.databricks.com/ingestion/lakeflow-connect/sql-server-limits) | [database-source-behaviors-sql-server.md](database-source-behaviors-sql-server.md) |
| PostgreSQL | [postgresql-troubleshoot](https://docs.databricks.com/ingestion/lakeflow-connect/postgresql-troubleshoot) | [postgresql-limits](https://docs.databricks.com/ingestion/lakeflow-connect/postgresql-limits) | [database-source-behaviors-postgresql.md](database-source-behaviors-postgresql.md) |
| MySQL | [mysql-troubleshoot](https://docs.databricks.com/ingestion/lakeflow-connect/mysql-troubleshoot) | [mysql-limits](https://docs.databricks.com/ingestion/lakeflow-connect/mysql-limits) | [database-source-behaviors-mysql.md](database-source-behaviors-mysql.md) |
| Oracle | [oracle-troubleshoot](https://docs.databricks.com/ingestion/lakeflow-connect/oracle-troubleshoot) | [oracle-limits](https://docs.databricks.com/ingestion/lakeflow-connect/oracle-limits) | [database-source-behaviors-oracle.md](database-source-behaviors-oracle.md) |
| Query-based | — | [query-based-limits](https://docs.databricks.com/ingestion/lakeflow-connect/query-based-limits) | [query-based-pipeline.md](query-based-pipeline.md#gotchas) |

The per-source files keep what follows from how the source database or the pipeline works: source settings, grants, logs and their retention, and changes that report no error. Names are placeholders (`<schema>.<table>`, `<user>`, `<database>`).

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
