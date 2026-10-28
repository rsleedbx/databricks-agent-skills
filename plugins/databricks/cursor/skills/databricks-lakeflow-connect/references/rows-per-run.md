# Rows per table per run

The per-table rows-per-run report (the query is a script) and its column meanings, and the SCD2 count query. The troubleshooting index is [troubleshooting-and-monitoring.md](troubleshooting-and-monitoring.md).

## Rows per table per run (snapshot inserts, upserts, deletes)

Use this when the user asks what each run did: how many rows each table took in its snapshot, upserted, appended, or deleted, and whether that run was a snapshot, a full refresh, or incremental. It works for query-based, integrated CDC, and standard CDC (run it on the ingestion pipeline, `<base>_ap_<epoch>`) pipelines. It reads only the event log and returns in a few seconds.

Run [`scripts/rows_per_run.sql`](../scripts/rows_per_run.sql) from this skill's folder, with the pipeline ID as a parameter:

```bash
databricks experimental aitools tools query --file scripts/rows_per_run.sql \
  --param pipeline_id=<pipeline-id> --profile <PROFILE> -o csv
```

Pass the SQL with `--file`: an inline query that starts with a `--` comment is parsed as a CLI flag. For a bundle-deployed pipeline, get the ID with `databricks bundle summary -o json --profile <PROFILE>` (under `resources.pipelines.<key>.id`). `event_log()` works only for the owner of the pipeline's tables: anyone else gets `PERMISSION_DENIED: The event_log table-valued function can be called only by the owner`.

If the flow names don't match the patterns below (for example names with hyphens or backticks), the tables show as `full load`: read `origin.flow_name` in the raw event log and adapt the patterns in the script.

Show the result as one table per run (or one row per table for the latest run only, if that's what the user asked about). Columns:

| Column | Meaning |
|---|---|
| `pipeline_mode` | `integrated cdc`, `standard cdc`, `query-based upsert` (SCD1/SCD2), `query-based append` (`APPEND_ONLY`), or `query-based full load` (no cursor) |
| `run_type` | `snapshot`, `snapshot + changes` (integrated CDC: snapshot and change rows in one update), `incremental`, `full refresh`, `full load` (no cursor: counts are blank because that flow records none; count the table instead), `no new data`, or `none` (the run failed before any table started) |
| `snapshot_inserted` | Rows loaded by the initial snapshot or a full refresh |
| `upserted` | Change rows inserted or updated after the snapshot |
| `appended` | New rows on an `APPEND_ONLY` table after the snapshot |
| `deleted` | Deletes applied: source deletes for CDC, `deletion_condition` soft deletes for query-based |

Where the numbers come from:

- **Query-based:** each table has one flow, `<catalog>.<schema>.<catalog>_<schema>_<table>`. SCD tables add an `_upsert` suffix and report `num_upserted_rows` and `num_deleted_rows`. `APPEND_ONLY` tables have no suffix and report `num_output_rows`. There's no separate snapshot metric: a run counts as a snapshot when its first batch started with no saved cursor (`stream_progress` → `sources[0].startOffset.incrementalOffset`).
- **Integrated and standard CDC:** each table has a snapshot flow and a change flow. Integrated CDC prefixes them with `applier_` (`applier_<catalog>_<schema>_<table>_snapshot_flow` / `..._cdc_flow`). The standard CDC ingestion pipeline uses the same names without the prefix. The snapshot flow reports the snapshot rows, and the change flow reports `num_upserted_rows` and `num_deleted_rows`. The query skips the `extractor_*` flows (rows read from the source) and the shared `cdc_staging_table`.
- **Query-based, no cursor:** the table's flow is `<catalog>.<schema>.<table>` (type `SNAPSHOT_CHANGE`) and records no row counts, only timings. The report lists the table as `full load` with blank counts.
- **Hard-delete tracking:** a separate `<catalog>_<schema>_<table>_delete` flow reports the hard deletes. The query adds them to the table's `deleted`.
- **Skipped as well:** the per-table `_staging` flows and the internal `pipelines.flowTimeMetrics.missingFlowName` flow. They carry only timings.

Things to tell the user:

- **Running updates report partial counts.** An integrated CDC update keeps running until smart closure ends it (see [integrated CDC updates](integrated-cdc.md#how-an-update-runs-smart-closure)), so re-run the query after it completes.
- **Failed runs still report** what each table loaded before the failure. A run that failed before any table started shows one `(no table ran)` row; pull its error with the `level IN ('ERROR', 'WARN')` query in [troubleshooting-and-monitoring.md](troubleshooting-and-monitoring.md#where-to-look-first).
- **No primary key under CDC (integrated or standard):** the table is keyed on all columns, so identical source rows collapse. The snapshot count can be far below the source row count. Use SCD Type 2, or add a key at the source.
- **Query-based counts can exceed the real changes.** With a timestamp cursor, SCD1 and SCD2 re-read a short lookback window before the high-water mark and merge by key, so `upserted` can include rows that didn't change (for example, more upserts than inserts on an SCD1 table). On SCD2 tables it's much larger: about the table's row count plus the changes, on every incremental run, and soft deletes close the current version instead of counting as `deleted`. For an exact count, use the per-key query below for SCD2 and spot-check SCD1 against the source.

---

### Changes on an SCD2 table, counted from the table

For **query-based** SCD2 tables, every version carries `__START_AT` and `__END_AT`, which hold the cursor value, so they're timestamps when the cursor is a timestamp. Group by key to count what changed since a point in time. Use when the changes were made, or the start of the previous run. The query returns exactly the number of inserts, updates, and soft deletes the source made. For a numeric cursor, compare against a cursor value instead of `TIMESTAMP '<since>'`. For **CDC** SCD2 tables these columns hold the change-log sequence value (`sequence_by`), not a time, so this query doesn't apply as written. Check `DESCRIBE <table>` and compare against a sequence value.

```sql
WITH k AS (
  SELECT <key> AS key,
         min(__START_AT) AS first_start,
         count_if(__START_AT >= TIMESTAMP '<since>') AS started,
         count_if(__END_AT IS NULL) AS open_now,
         count_if(__END_AT >= TIMESTAMP '<since>') AS closed
  FROM <catalog>.<schema>.<table>
  GROUP BY <key>
)
SELECT count_if(first_start >= TIMESTAMP '<since>') AS inserted,
       count_if(first_start <  TIMESTAMP '<since>' AND started > 0) AS updated,
       count_if(closed > 0 AND open_now = 0) AS deleted
FROM k
```

For a composite key, use `struct(<col1>, <col2>) AS key`. Report these numbers for SCD2 tables next to the rows-per-run report, and say which one is the source-change count.
