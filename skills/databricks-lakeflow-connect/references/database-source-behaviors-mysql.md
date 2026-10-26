# MySQL source behaviors and errors

What happens on a MySQL source, with the error text as the product returns it. Names and IDs are placeholders (`<slot_name>`, `<schema>.<table>`, `<user>`, `<database>`). Shared behaviors (reading the destination, gateway compute, table limits): [database-source-behaviors.md](database-source-behaviors.md).

| Symptom | Likely cause | Fix |
|---|---|---|
| Each gateway holds one `Binlog Dump` row in `SHOW PROCESSLIST`, with the gateway's address as host. `SHOW REPLICAS` stays empty. | Checking which connections the gateway holds | Count gateway sessions in `SHOW PROCESSLIST`, not `SHOW REPLICAS`. |
| Its `Binlog Dump` row stays 5 to 25 minutes after the pipeline shows `IDLE` and the update is `CANCELED` | The gateway has stopped | Wait for it, or `KILL <connection_id>` on the source, before a run you measure. |
| The gateway's snapshot `SELECT` on a keyless table ran past the server's limit and wasn't ended. The gateway's pool sessions run with `max_execution_time` 0. | Bounding the snapshot with the server's `max_execution_time` | Don't use the server limit to bound a snapshot. Count the rows after `SNAPSHOT_COMPLETED`. |
| `COMPLETED`, with the destination short of the source count in a regular gap pattern. A full refresh gives the same count. | A keyless table | Count after the gateway's `SNAPSHOT_COMPLETED` event, then run the update again and count. If it's still short, record both counts with the gateway's events. A plain second update can reach the full count. |
| Its destination tables are dropped | Deleting an integrated pipeline | Copy the tables out first. |
| The column lands at the end of the destination table, not in the named position. Values are right by name. | Integrated CDC: `ADD COLUMN ... FIRST` or `ADD COLUMN ... AFTER` | Select columns by name. |
| The running update is cancelled, and a new one starts with cause `SCHEMA_CHANGE`. | Integrated CDC: a column added with `AFTER` | No full refresh needed; wait for the new update. |
| The ingestion update fails at analysis with "Failed to resolve flow" for the table's flows. The destination keeps the old name. | Integrated CDC: `RENAME COLUMN` | Don't rename a column on an integrated CDC table. No fix for a renamed column is established. |
| Applied at the source; the destination keeps the column, with NULL in new rows. No schema-change event. | Integrated CDC: `DROP COLUMN` | Check the destination after each drop. No fix verified. |
| Supported. The destination type stays `STRING`, and new rows carry the value. | `VARCHAR` length changed (`MODIFY COLUMN`) | None needed. |
| The operation event "Snapshot of <table> has ..." reaches `IN_PROGRESS` and never `COMPLETED`; the snapshot flows of the update stay `RUNNING` until the update ends. | Integrated CDC: waiting for a table's snapshot | Use the `flow_progress` `COMPLETED` event of the table's `extractor_..._snapshot_flow`, or count the rows. |
| Every ingestion update fails at analysis ("Failed to resolve flow" for the dropped table's flow) until the table is removed from the pipeline. | A source table dropped | Remove the dropped table from the ingestion pipeline. |
| The flow's own text ("Flow ... has failed because of a fatal error during realtime phase") is shown, not the catalog message. | Gateway flow reports `TABLE_DROPPED` or `MYSQL_BINLOG_FILE_DELETED` | Read the event type and the flow's table name from the gateway's events. |
| Schema exploration lists one table fewer. The flow reports `MISSING_TABLE_IN_SOURCE`; the update fails at analysis. No privilege message. | `SELECT` revoked from the ingestion login on one table | Grant `SELECT` on the table again, then check that schema exploration lists it. |

