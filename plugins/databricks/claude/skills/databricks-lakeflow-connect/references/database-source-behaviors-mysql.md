# MySQL source behaviors and errors

What happens on a MySQL source, with the error text as the product returns it. Names and IDs are placeholders (`<slot_name>`, `<schema>.<table>`, `<user>`, `<database>`). Shared behaviors (reading the destination, gateway compute, table limits): [database-source-behaviors.md](database-source-behaviors.md).

| Condition | Error or symptom | What to do |
|---|---|---|
| Checking which connections the gateway holds | Each gateway holds one `Binlog Dump` row in `SHOW PROCESSLIST`, with the gateway's address as host. `SHOW REPLICAS` stays empty. | Count gateway sessions in `SHOW PROCESSLIST`, not `SHOW REPLICAS`. |
| The gateway has stopped | Its `Binlog Dump` row stays 5 to 25 minutes after the pipeline shows `IDLE` and the update is `CANCELED` | Wait for it, or `KILL <connection_id>` on the source, before a run you measure. |
| Bounding the snapshot with the server's `max_execution_time` | The gateway's snapshot `SELECT` on a keyless table ran past the server's limit and wasn't ended. The gateway's pool sessions run with `max_execution_time` 0. | Don't use the server limit to bound a snapshot. Count the rows after `SNAPSHOT_COMPLETED`. |
| A keyless table | `COMPLETED`, with the destination short of the source count in a regular gap pattern. A full refresh gives the same count. | Count after the gateway's `SNAPSHOT_COMPLETED` event, then run the update again and count. If it's still short, record both counts with the gateway's events. A plain second update can reach the full count. |
| Deleting an integrated pipeline | Its destination tables are dropped | Copy the tables out first. |
| Integrated CDC: `ADD COLUMN ... FIRST` or `ADD COLUMN ... AFTER` | The column lands at the end of the destination table, not in the named position. Values are right by name. | Select columns by name. |
| Integrated CDC: a column added with `AFTER` | The running update is cancelled, and a new one starts with cause `SCHEMA_CHANGE`. | No full refresh needed; wait for the new update. |
| Integrated CDC: `RENAME COLUMN` | The ingestion update fails at analysis with "Failed to resolve flow" for the table's flows. The destination keeps the old name. | Don't rename a column on an integrated CDC table. No fix for a renamed column is established. |
| Integrated CDC: `DROP COLUMN` | Applied at the source; the destination keeps the column, with NULL in new rows. No schema-change event. | Check the destination after each drop. No fix verified. |
| `VARCHAR` length changed (`MODIFY COLUMN`) | Supported. The destination type stays `STRING`, and new rows carry the value. | None needed. |
| Integrated CDC: waiting for a table's snapshot | The operation event "Snapshot of <table> has ..." reaches `IN_PROGRESS` and never `COMPLETED`; the snapshot flows of the update stay `RUNNING` until the update ends. | Use the `flow_progress` `COMPLETED` event of the table's `extractor_..._snapshot_flow`, or count the rows. |
| A source table dropped | Every ingestion update fails at analysis ("Failed to resolve flow" for the dropped table's flow) until the table is removed from the pipeline. | Remove the dropped table from the ingestion pipeline. |
| Gateway flow reports `TABLE_DROPPED` or `MYSQL_BINLOG_FILE_DELETED` | The flow's own text ("Flow ... has failed because of a fatal error during realtime phase") is shown, not the catalog message. | Read the event type and the flow's table name from the gateway's events. |
| `SELECT` revoked from the ingestion login on one table | Schema exploration lists one table fewer. The flow reports `MISSING_TABLE_IN_SOURCE`; the update fails at analysis. No privilege message. | Grant `SELECT` on the table again, then check that schema exploration lists it. |

