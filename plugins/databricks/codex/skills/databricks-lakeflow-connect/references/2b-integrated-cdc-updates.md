# Integrated CDC: updates

What an integrated CDC update does, when it ends, and what holds across releases. Setup: [2a-integrated-cdc.md](2a-integrated-cdc.md).

## How an update runs: smart closure

Integrated CDC updates are triggered, but they don't stop as soon as the backlog is empty. [Smart closure](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/smart-closure) decides when an update ends:

1. **Minimum runtime first.** No update ends before it.
2. **Then, once caught up,** it ends (logged as `lag-converged`).
3. **Or at the maximum runtime,** if it hasn't caught up (logged as `max-runtime-cap-hit`). The next update resumes from there.
4. **Or on a source schema change.** The update stops and a new one starts with the new schema.

Databricks sets and adjusts the minimum runtime, maximum runtime, and lag threshold, and they aren't configurable. Don't quote fixed durations. What it means in practice:

- **Each update can run much longer than a standard CDC ingestion run,** even with nothing to apply. That's expected, not a hang. An update can end with the log line "CDC extraction has COMPLETED. Caught up to the source." On an idle MySQL source with no writes, the update logs "MySQL binlog stall detected" and runs until the maximum runtime, because the lag can't converge. To stop an update early, use `databricks pipelines stop <pipeline-id>`. That cancels the update, and the next update resumes from where it stopped.
- **Don't block on it.** Start the update with `databricks bundle run <key> --no-wait` (or `pipelines start-update`), tell the user it's running, and check it with `databricks pipelines get-update <pipeline-id> <update-id>` until it's `COMPLETED` or `FAILED`. If you have to stop before it finishes, report the update ID and how to check it.
- **What it's doing, and why it stopped:** extraction progress and the closure decision are logged as `operation_progress` and `cdc_extraction_details` events. A running update reports something like "CDC extraction is IN_PROGRESS, running for 5 minutes. Zero snapshots are pending. Will stop once the lag converges." Source-side conditions show up too, such as "MySQL binlog stall detected" on an idle source. Query by event type, not by message text, since the wording changes:

  ```sql
  SELECT timestamp, event_type, message FROM event_log('<pipeline-id>')
  WHERE event_type IN ('operation_progress', 'cdc_extraction_details', 'update_progress')
  ORDER BY timestamp DESC LIMIT 20
  ```

- **The pipeline also starts updates itself.** After a source schema change, it starts one with cause `SCHEMA_CHANGE`; after a failure, often one with cause `RETRY_ON_FAILURE`. While that update runs, `start-update` is refused ("An active update ... already exists"). Before you start another update or a full refresh, check what's running and wait for it:

  ```bash
  databricks pipelines list-updates <pipeline-id> --max-results 3 -o json | yq '.updates[] | [.update_id, .cause, .state]'
  ```

  On SQL Server (tested 2026-10), an update that failed right after an added column with `INGESTION_GATEWAY_INTERNAL_ERROR` and an empty "Extractor error:" was followed by the pipeline's own updates, and the column arrived without a full refresh. When it also reports `PIPELINE_NON_RETRYABLE_ANALYSIS_CLIENT_ERROR`, no retry follows: start the next update yourself.
- **Schedule** it with a Jobs `pipeline_task`. Use the interval the smart closure page recommends (it currently suggests starting at 60 minutes). A trigger that fires while an update is still running is skipped. Continuous mode exists as a separate option; check its status in the docs.

## Behaviour that doesn't change between releases

- **The first update takes the initial snapshot,** then later updates apply changes.
- **Tables without a primary key** are keyed on all columns, so identical rows collapse: the table ends up with one row per distinct row. Use SCD Type 2 or add a key at the source if duplicates matter.
- **Immutable after create:** `connection_name` and `connector_type` can't be changed. Create a new pipeline instead.
- **Purged change logs** (CDC retention, change-tracking retention, or binlog expiry) before the pipeline reads them need a full refresh. Schedule runs well inside the retention period.

Report rows per table per run with the query in [5a-rows-per-run.md](5a-rows-per-run.md#rows-per-table-per-run-snapshot-inserts-upserts-deletes).

---

## Other behaviors tested 2026-10

- **One table that stops doesn't stop the others,** but the update ends `FAILED` with `PIPELINE_NON_RETRYABLE_ANALYSIS_CLIENT_ERROR` ("… A new pipeline update will not be created") until that table is fixed or full-refreshed. Alert on the update's state, and read the table's error in the event log.
- **A pipeline can't switch between integrated CDC and a gateway:** `pipelines update` refuses it both ways, a second pipeline can't write to the first one's tables (`TABLE_ALREADY_MANAGED_BY_OTHER_PIPELINE`), and deleting a pipeline deletes its destination tables. Moving means a new pipeline and a full load.
- **Change data feed is on for destination tables,** readable with `table_changes()` from version 1 (version 0 has no change data: `DELTA_MISSING_CHANGE_DATA`). On an SCD type 2 table a delete shows as a closed row, not a `delete` change.
- **Many tables per pipeline:** create accepts more tables than the documented integrated CDC limits (50 for `SPEED`, 500 by default, 300 triggered; see [8-database-source-behaviors.md](8-database-source-behaviors.md#table-limits)). A 900-table SQL Server pipeline (one schema object per source schema) was accepted and loaded with no reinitialization timeout. Load time and change latency grow with the count, so split large table counts across pipelines.
