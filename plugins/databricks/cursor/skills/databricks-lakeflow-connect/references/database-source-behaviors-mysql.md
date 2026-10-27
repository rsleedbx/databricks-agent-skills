# MySQL source behaviors and errors

Read the current [MySQL troubleshooting](https://docs.databricks.com/ingestion/lakeflow-connect/mysql-troubleshoot) and [limitations](https://docs.databricks.com/ingestion/lakeflow-connect/mysql-limits) pages first: error text, schema-change handling (added, renamed and dropped columns) and limits change between releases. Below is what follows from how MySQL and the pipeline work. Shared behaviors (reading the destination, gateway compute, table limits): [database-source-behaviors.md](database-source-behaviors.md). Source prerequisites (`binlog_format=ROW`, `binlog_row_image=FULL`): [integrated-cdc.md](integrated-cdc.md#source-prerequisites).

| Symptom | Likely cause | Fix |
|---|---|---|
| `SHOW REPLICAS` is empty while the pipeline reads the binlog | The pipeline reads the binlog as a client, not a replica | Count its sessions in `SHOW PROCESSLIST`: one `Binlog Dump` row per reader, with its address as host. The row can stay for minutes after the update ends; wait for it, or `KILL <connection_id>`, before a run you measure. |
| A table's flow fails with `MYSQL_BINLOG_FILE_DELETED` | The binlog files expired (`binlog_expire_logs_seconds`) before the pipeline read them | Full refresh the table, and keep the binlog well past the schedule interval. |
| Every ingestion update fails at analysis ("Failed to resolve flow" for the table's flows) | A source table was dropped | Remove the dropped table from the ingestion pipeline. |
| The flow reports `MISSING_TABLE_IN_SOURCE` and the update fails at analysis, with no privilege message; schema exploration lists one table fewer | `SELECT` revoked from the ingestion login on that table | Grant `SELECT` on the table again, then check that schema exploration lists it. |
| The added column lands at the end of the destination table, not in its named position | `ADD COLUMN ... FIRST` or `ADD COLUMN ... AFTER` | Select columns by name. |
