# Troubleshooting and monitoring

This reference covers what to check when an ingestion pipeline misbehaves: where the logs live, the common error shapes, and the escalation path.

---

## Where to look first

Every Lakeflow Connect pipeline emits a structured event log. For SaaS and query-based pipelines that's the only artifact; for gateway-based database pipelines (CDC) you'll also want to inspect the gateway pipeline's events.

The event log is a Delta table on the pipeline. Query it through SQL:

```sql
SELECT timestamp, level, message, error
FROM event_log("<pipeline-id>")
WHERE level IN ('ERROR', 'WARN')
  AND timestamp > current_timestamp() - INTERVAL 1 DAY
ORDER BY timestamp DESC
LIMIT 50;
```

For event-log table conventions (filtering by `event_type`, joining with metrics, etc.), see **databricks-pipelines**.

**Gateway-based database pipelines have two event logs** — one for the gateway, one for the ingestion pipeline. A symptom on the ingestion side often has its root cause in the gateway side. When debugging database connectors, query both.

### Each update can run on a different runtime

Before comparing two updates that behaved differently, check what each ran on. Nothing in the pipeline has to change for the runtime to change between updates:

- **The version can change.** Pipelines are versionless: Databricks upgrades the runtime on its own, and reverts a pipeline to the previous version if an upgrade stops it from starting ([pipeline release notes](https://docs.databricks.com/release-notes/dlt/)). The `channel` setting (`CURRENT` or `PREVIEW`) picks which version line, not the exact version.
- **The CPU architecture can change.** Serverless updates run on x86 or ARM depending on capacity, so on the same version one update can run on x86 and the next on ARM (`aarch64` in the image name; no suffix means x86). There is no pipeline setting for it. Cost per update can differ between the two: if DBU usage steps up or down at the same time as the architecture changes, show support the image names of the updates before and after, since they can pin a pipeline or workspace to one architecture.
- **The settings can change.** Each update records the settings it ran with. Settings appear, disappear and change between runtime versions, and a setting you see in one update can be gone in a later one.

Each update's `runtime_details` event shows the version, channel and image, and its `runtime_configuration` the settings:

```sql
SELECT timestamp, origin.update_id,
  details:runtime_details.runtime_version.dbr_version AS dbr_version,
  details:runtime_details.runtime_version.channel     AS channel,
  details:runtime_details.runtime_version.image_key   AS image
FROM event_log("<pipeline-id>")
WHERE event_type = 'runtime_details'
ORDER BY timestamp DESC;
```

When a problem appears in one update and not in another, put this next to the problem: a change of version or image between the two is the first thing to rule out, and the first thing support will ask about.

---

## Common errors and resolutions

| Error / symptom | Likely cause | Resolution |
|---|---|---|
| `APPLY_CHANGES_FROM_SNAPSHOT_ERROR.DUPLICATE_KEY_VIOLATION` | Source snapshot contains duplicate values on the declared primary key. | Inspect the source for duplicate PKs (often a merged record or composite-key surprise). The connector won't auto-resolve — fix at the source or change the PK declaration. |
| `validate_only` update appears in pipeline run history | Expected. A dry-run validation run is logged alongside actual runs. | Filter `event_log` on `details:flow_progress.status != 'VALIDATING'` if the dry-runs are noisy. |
| SCD2 row count doesn't match raw source count | Expected. SCD2 multiplies rows per change (one row per version), so SCD2 row count >> source row count is normal. | Compare on PK count with `current = true` instead of total row count. |
| `NULL` values appear after switching SCD1 -> SCD2 | Expected. Pre-switch history is reconstructed as a single open version with `NULL`s for unknown deltas. | Re-snapshot the table if a clean SCD2 history is required from a specific point. |
| `GB ingested` >> source row size in the metrics | Expected for CDC sources. Change log columns, schema metadata, and per-batch overhead inflate ingested bytes. | Use source row count, not GB ingested, as the workload sizing signal. |
| Gateway pipeline fails: instance type unavailable in region | Default gateway cluster shape isn't stocked in the target region. | Apply a cluster policy override on the gateway pipeline to pin a regionally-available instance type. |
| Pipeline runs but the destination table never updates | UC `CONNECTION` not in `READY` state, OR destination schema missing. | `DESCRIBE CONNECTION <name>` — state must be `READY`. Verify the destination schema exists and the pipeline's service principal has `CREATE TABLE` + `MODIFY`. |
| OAuth U2M connection refreshes fail after weeks of working | Refresh token expired or revoked at the SaaS source. | Re-open the connection in Catalog Explorer and re-authorize. Plan for periodic re-auth if the SaaS source enforces a refresh-token lifetime. |
| Query-based: source updates never land | Cursor column not bumped on UPDATE, or NULL cursor values. | Add an update trigger on the cursor column and make it `NOT NULL`. See [query-based-connectors.md](query-based-connectors.md). |
| Query-based: every run reloads the full table | No `cursor_columns` set, or the cursor isn't monotonic. | Set a single monotonic cursor (prefer an update timestamp). |
| Query-based: deleted source rows remain | Hard `DELETE` at the source is invisible to a query-based pull. | Use soft deletes with `deletion_condition`, the Beta hard-delete option, or a CDC connector. |
| Postgres connection to Lakebase works, then fails about an hour later | The connection password is a Lakebase OAuth token (1-hour lifetime). | Use a native Postgres password role. See [lakebase-as-postgres-source.md](lakebase-as-postgres-source.md). |
| `start-update` refused: "An active update ... already exists" | The pipeline started its own update (cause `SCHEMA_CHANGE` after a source schema change, or `RETRY_ON_FAILURE` after a failure). | `databricks pipelines list-updates <pipeline-id> --max-results 3`; wait for that update to finish. Don't stack a full refresh on it. |
| Database CDC update fails with `INGESTION_GATEWAY_INTERNAL_ERROR` and an empty "Extractor error:" | Often a transient failure as the extractor starts, for example right after a schema change. | Check `list-updates`: the pipeline usually retries itself and recovers. If no retry follows (it also reports `PIPELINE_NON_RETRYABLE_ANALYSIS_CLIENT_ERROR`), start an update. If it fails the same way again, file a support ticket with the update IDs. |
| SQL Server `SCD_TYPE_2` table gets no changes, no error | The table uses change tracking; `SCD_TYPE_2` needs CDC. Its changes are held until the next schema change. | Check the table's capture ([sql-server-cdc.md](sql-server-cdc.md#sql-server-change-tracking-vs-cdc)). Enable CDC on it and run a full refresh of the table, or set it to `SCD_TYPE_1`. |
| A table has fewer rows than the source after the first load, no error | Rows lost in the initial snapshot. | Compare source and destination row counts per table after every first load and full refresh ([rows per table per run](rows-per-run.md#rows-per-table-per-run-snapshot-inserts-upserts-deletes)). If a table is short, full-refresh it once; if it's short again, file a support ticket with the table's key and column types. |
| Connector requires `channel: PREVIEW` at pipeline create | The connector runs only on the preview SDP runtime channel. | Set `channel: PREVIEW` as the connector's docs instruct — it selects the runtime channel, not the connector's GA status. GA connectors run on `CURRENT` (omit it); if gateway startup is slow, `CURRENT` is faster where supported. |
| `INGESTION_GATEWAY_CDC_ERROR.CDC_NOT_ENABLED`: "wal_level is set to 'replica'" (PostgreSQL) | `wal_level` isn't `logical` | Set `wal_level = logical`, restart, then start the update. See [database-source-behaviors-postgresql.md](database-source-behaviors-postgresql.md). |
| `INGESTION_GATEWAY_PG_SLOT_IS_INVALIDATED` (PostgreSQL) | The slot's WAL was removed past `max_slot_wal_keep_size` | Create the slot again, then full refresh; a full refresh doesn't recreate it. See [database-source-behaviors-postgresql.md](database-source-behaviors-postgresql.md). |
| `INGESTION_GATEWAY_CONNECTION_ERROR.TLS_CONNECTION_FAILURE` (PostgreSQL) | `verify-ca` with a self-signed server certificate | Add `"trustServerCertificate": "true"` to the connection's options, or use a CA certificate. See [database-source-behaviors-postgresql.md](database-source-behaviors-postgresql.md). |
| `INGESTION_GATEWAY_UNTRACEABLE_TABLE_MODIFICATION` (SQL Server) | `TRUNCATE` on a change-tracking table | Full refresh that table. See [database-source-behaviors-sql-server.md](database-source-behaviors-sql-server.md). |
| `INGESTION_GATEWAY_DDL_OBJECTS_MISSING` (SQL Server) | Setup utility upgraded without its DDL objects | Run the DDL objects script the message names, then full refresh the table. See [database-source-behaviors-sql-server.md](database-source-behaviors-sql-server.md). |
| `ORACLE_ARCHIVE_LOG_FILES_UNAVAILABLE` | Archive logs removed before the gateway read them | Raise the recovery-area retention, then full refresh the named tables. See [database-source-behaviors-oracle.md](database-source-behaviors-oracle.md). |
| `NO_CONNECTION_PARAMETER` (Oracle) | `source_catalog` missing from `connection_parameters` | Set `{"source_catalog": "<CDB service name>"}`, not the literal `CDB$ROOT`. See [database-source-behaviors-oracle.md](database-source-behaviors-oracle.md). |
| Lag looks healthy but the destination is empty | The lag doesn't show whether rows were written; for example on SQL Server after a failed first start | Count the destination rows and read `DESCRIBE HISTORY` (`numTargetRowsInserted`). See [database-source-behaviors.md](database-source-behaviors.md#read-the-destination-not-only-the-status). |
| `TABLE_SKIPPED_DUE_TO_UNSUPPORTED_COLUMN_DATA_TYPE` (Oracle CDC) | A table has an `XMLTYPE` column; LogMiner skips the whole table, and the update waits with its snapshot `SCHEDULED` | Remove the table from the pipeline. See [database-source-behaviors-oracle.md](database-source-behaviors-oracle.md). |
| `INGESTION_GATEWAY_TABLE_DROPPED` | A source table was dropped. The ingestion update fails at analysis and blocks every table in the pipeline. | Remove the dropped table from the ingestion pipeline. See [database-source-behaviors-oracle.md](database-source-behaviors-oracle.md). |
| "The number of tables in this pipeline (1001 tables) exceeds the quota of 1000 tables per pipeline" | The pipeline's hidden CDC staging table counts toward the quota, so 1,000 source tables is refused. Retries repeat the error. | Keep each ingestion pipeline at 999 source tables or fewer, and split the rest over pipelines. See [database-source-behaviors.md](database-source-behaviors.md#table-limits). |

---

## Escalation pointers

When the event log doesn't explain a failure:

1. **Public docs hub** — [Lakeflow Connect overview](https://docs.databricks.com/ingestion/lakeflow-connect) covers concepts and links to per-connector pages.
2. **Connector reference** — [per-connector setup](https://docs.databricks.com/ingestion/lakeflow-connect/) is the canonical source for current auth, limits, and supported objects per source.
3. **Workspace support** — file a support case from Help -> Contact Support inside the workspace; attach the pipeline ID and a relevant `event_log` extract.

For monitoring patterns beyond event-log queries (dashboards, alerting on pipeline state, SLAs), see **databricks-pipelines**.
