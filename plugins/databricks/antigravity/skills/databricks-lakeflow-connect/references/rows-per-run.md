# Rows per table per run

The query and the column meanings for the per-table rows-per-run report, and the SCD2 count query. The troubleshooting index is [troubleshooting-and-monitoring.md](troubleshooting-and-monitoring.md).

## Rows per table per run (snapshot inserts, upserts, deletes)

Use this when the user asks what each run did: how many rows each table took in its snapshot, upserted, appended, or deleted, and whether that run was a snapshot, a full refresh, or incremental. It works for query-based, integrated CDC, and standard CDC (run it on the ingestion pipeline, `<base>_ap_<epoch>`) pipelines. It reads only the event log and returns in a few seconds.

Write the query below to `rows_per_run.sql` with your file tool, replacing `<pipeline-id>`. Then run it:

```bash
databricks experimental aitools tools query --file rows_per_run.sql --profile <PROFILE>
```

Pass the SQL with `--file`: an inline query that starts with a `--` comment is parsed as a CLI flag. For a bundle-deployed pipeline, get the ID with `databricks bundle summary -o json --profile <PROFILE>` (under `resources.pipelines.<key>.id`).

```sql
-- Rows per table per run: snapshot inserts, upserts, appends, deletes
WITH ev AS (
  SELECT * FROM event_log('<pipeline-id>')
),
runs AS (
  SELECT origin.update_id,
         min(timestamp) AS started,
         max_by(details:update_progress.state::string, timestamp)
           FILTER (WHERE event_type = 'update_progress') AS state,
         max(details:create_update.full_refresh::boolean) AS full_refresh_all,
         max(details:create_update.full_refresh_selection::string) AS full_refresh_sel
  FROM ev
  WHERE event_type IN ('create_update', 'update_progress')
  GROUP BY 1
),
flow_rows AS (
  -- Per-table flows, by pipeline type:
  --   query-based:     <catalog>.<schema>.<catalog>_<schema>_<table>[_upsert]   (_upsert = SCD1/SCD2, none = APPEND_ONLY)
  --   integrated CDC:  <catalog>.<schema>.applier_<catalog>_<schema>_<table>_{snapshot|cdc}_flow
  --   standard CDC:    <catalog>.<schema>.<catalog>_<schema>_<table>_{snapshot|cdc}_flow   (ingestion pipeline)
  --   query-based, no cursor (full load each run): <catalog>.<schema>.<table>; records no row counts
  --   query-based hard-delete tracking: <catalog>.<schema>.<catalog>_<schema>_<table>_delete (counted into <table>)
  -- Skips _staging, extractor_*, cdc_staging_table, and internal timing-only flows.
  -- Note: if flow names use hyphens or backticks, the prefix pattern may not match and they show as "full load".
  --       Inspect origin.flow_name in the raw event log to see the actual pattern, then adapt the regex.
  SELECT origin.update_id, origin.flow_name,
         CASE WHEN origin.flow_name RLIKE '.+_(snapshot|cdc)_flow$'
              THEN regexp_extract(origin.flow_name, '^.+\\.([^_.]+)(?:_(?:snapshot|cdc)_flow)?(?:_upsert)?$', 1)
              WHEN origin.flow_name RLIKE '.+_(?:upsert|delete)$'
              THEN regexp_extract(origin.flow_name, '^.+\\.([^_.]+)(?:_upsert|_delete)?$', 1)
              ELSE regexp_extract(origin.flow_name, '([^.]+)$', 1) END AS tbl,
         origin.flow_name RLIKE '\\.applier_' AS is_integrated,
         CASE WHEN NOT origin.flow_name RLIKE '^([^.]+)\\.([^.]+)\\.(applier_)?\\1_\\2_.+$' THEN 'full load'
              WHEN origin.flow_name LIKE '%\\_snapshot\\_flow' THEN 'cdc snapshot'
              WHEN origin.flow_name LIKE '%\\_delete' THEN 'hard delete'
              WHEN origin.flow_name LIKE '%\\_cdc\\_flow' THEN 'cdc changes'
              WHEN origin.flow_name LIKE '%\\_upsert' THEN 'upsert'
              ELSE 'append' END AS kind,
         coalesce(details:flow_progress.metrics.num_upserted_rows::bigint, 0) AS upserted,
         coalesce(details:flow_progress.metrics.num_output_rows::bigint, 0) AS output_rows,
         coalesce(details:flow_progress.metrics.num_deleted_rows::bigint, 0) AS deleted
  FROM ev
  WHERE event_type = 'flow_progress'
    AND origin.flow_name NOT LIKE '%\\_staging'                        -- per-table staging flows: timings only
    AND (origin.flow_name RLIKE '^([^.]+)\\.([^.]+)\\.(applier_)?\\1_\\2_.+$'
         OR (origin.flow_name RLIKE '^[^.]+\\.[^.]+\\.[^.]+$'                 -- no-cursor full-load flow
             AND origin.flow_name NOT LIKE 'pipelines.%'
             AND origin.flow_name NOT LIKE '%.cdc\\_staging\\_table'
             AND NOT origin.flow_name RLIKE '\\.(applier|extractor)_'))
),
qbc_offsets AS (
  -- query-based only: the first batch's start offset; no incrementalOffset means a snapshot
  SELECT origin.update_id, origin.flow_name,
         min_by(get_json_object(details:stream_progress.progress_json::string,
                                '$.sources[0].startOffset.incrementalOffset'), timestamp) AS start_cursor
  FROM ev
  WHERE event_type = 'stream_progress'
  GROUP BY 1, 2
),
per_table AS (
  SELECT f.update_id, f.tbl,
         max(f.kind IN ('cdc snapshot', 'cdc changes')) AS is_cdc,
         max(f.is_integrated) AS is_integrated,
         max(f.kind = 'append') AS is_append,
         max(f.kind = 'full load') AS is_full_load,
         -- query-based: did any batch run, and did it start without a saved cursor?
         max(o.flow_name IS NOT NULL AND f.kind <> 'hard delete') AS qbc_batch,
         max(o.flow_name IS NOT NULL AND o.start_cursor IS NULL AND f.kind <> 'hard delete') AS qbc_snapshot,
         sum(CASE WHEN f.kind = 'cdc snapshot' THEN f.upserted ELSE 0 END) AS cdc_snapshot_rows,
         sum(CASE WHEN f.kind IN ('cdc changes', 'upsert') THEN f.upserted ELSE 0 END) AS upserted,
         sum(CASE WHEN f.kind = 'append' THEN f.output_rows ELSE 0 END) AS appended,
         sum(f.deleted) AS deleted
  FROM flow_rows f
  LEFT JOIN qbc_offsets o ON o.update_id = f.update_id AND o.flow_name = f.flow_name
  GROUP BY 1, 2
),
labeled AS (
  SELECT r.started, r.update_id, r.state,
         t.tbl, t.is_cdc, t.is_integrated, t.is_append, t.is_full_load, t.qbc_batch, t.qbc_snapshot,
         t.cdc_snapshot_rows, t.upserted, t.appended, t.deleted,
         coalesce(r.full_refresh_all, false)
           OR coalesce(array_contains(from_json(r.full_refresh_sel, 'array<string>'), t.tbl), false) AS is_full_refresh
  FROM runs r
  LEFT JOIN per_table t ON t.update_id = r.update_id   -- keep runs that failed before any table started
)
SELECT date_format(started, 'yyyy-MM-dd HH:mm:ss') AS run_started,
       left(update_id, 8) AS update_id,
       state,
       coalesce(tbl, '(no table ran)') AS table_name,
       CASE WHEN tbl IS NULL THEN NULL
            WHEN is_cdc AND is_integrated THEN 'integrated cdc'
            WHEN is_cdc THEN 'standard cdc'
            WHEN is_append THEN 'query-based append'
            WHEN is_full_load THEN 'query-based full load'
            ELSE 'query-based upsert' END AS pipeline_mode,
       CASE WHEN tbl IS NULL THEN 'none'
            WHEN is_full_refresh THEN 'full refresh'
            WHEN is_full_load THEN 'full load (no cursor; no row counts in the event log)'
            WHEN is_cdc AND cdc_snapshot_rows > 0 AND upserted + deleted > 0 THEN 'snapshot + changes'
            WHEN is_cdc AND cdc_snapshot_rows > 0 THEN 'snapshot'
            WHEN is_cdc AND upserted + deleted > 0 THEN 'incremental'
            WHEN is_cdc THEN 'no new data'
            WHEN NOT qbc_batch THEN 'no new data'
            WHEN qbc_snapshot THEN 'snapshot'
            ELSE 'incremental' END AS run_type,
       -- query-based snapshots and full refreshes count their rows as snapshot inserts
       CASE WHEN is_full_load THEN NULL
            WHEN is_cdc THEN cdc_snapshot_rows
            WHEN is_full_refresh OR qbc_snapshot THEN upserted + appended
            ELSE 0 END AS snapshot_inserted,
       CASE WHEN is_full_load THEN NULL WHEN NOT is_cdc AND (is_full_refresh OR qbc_snapshot) THEN 0 ELSE upserted END AS upserted,
       CASE WHEN is_full_load THEN NULL WHEN NOT is_cdc AND (is_full_refresh OR qbc_snapshot) THEN 0 ELSE appended END AS appended,
       CASE WHEN is_full_load THEN NULL ELSE deleted END AS deleted
FROM labeled
ORDER BY started, table_name
```

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

- **Running updates report partial counts.** An integrated CDC update keeps running until smart closure ends it (see [integrated-cdc-updates.md](integrated-cdc-updates.md#how-an-update-runs-smart-closure)), so re-run the query after it completes.
- **Failed runs still report** what each table loaded before the failure. A run that failed before any table started shows one `(no table ran)` row; pull its error with the `level IN ('ERROR', 'WARN')` query above.
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
