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

Error text and codes change between releases. For a database source, read its current troubleshooting page first ([links per source](database-source-behaviors.md)); the rows below follow from how the pipelines and sources work.

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
| Database CDC update fails with `INGESTION_GATEWAY_INTERNAL_ERROR` | Varies; see the source's troubleshooting page | Check `list-updates`: the pipeline often retries itself and recovers. If the update also reports `PIPELINE_NON_RETRYABLE_ANALYSIS_CLIENT_ERROR`, no retry follows: start an update. If it fails the same way again, file a support case with the update IDs. |
| SQL Server `SCD_TYPE_2` table gets no changes, no error | The table uses change tracking; `SCD_TYPE_2` needs CDC, and create doesn't check it. | Check the table's capture ([sql-server-cdc.md](sql-server-cdc.md#sql-server-change-tracking-vs-cdc)). Enable CDC on it and run a full refresh of the table, or set it to `SCD_TYPE_1`. |
| Connector requires `channel: PREVIEW` at pipeline create | The connector runs only on the preview SDP runtime channel. | Set `channel: PREVIEW` as the connector's docs instruct — it selects the runtime channel, not the connector's GA status. GA connectors run on `CURRENT` (omit it); if gateway startup is slow, `CURRENT` is faster where supported. |
| `CDC_NOT_ENABLED`: "wal_level is set to 'replica'" (PostgreSQL) | `wal_level` isn't `logical` | Set `wal_level = logical`, restart, then start the update. See [database-source-behaviors-postgresql.md](database-source-behaviors-postgresql.md). |
| `INGESTION_GATEWAY_PG_SLOT_IS_INVALIDATED` (PostgreSQL) | The slot's WAL was removed past `max_slot_wal_keep_size` | Create the slot again, then full refresh; a full refresh doesn't recreate it. |
| SQL Server `TRUNCATE` on a change-tracking table | Change tracking doesn't record a `TRUNCATE` | Full refresh that table after every `TRUNCATE` ([source table changes](sql-server-cdc.md#sql-server-change-tracking-vs-cdc)). |
| `ORACLE_ARCHIVE_LOG_FILES_UNAVAILABLE` | Archive logs removed before the pipeline read them | Raise the recovery-area retention, then full refresh the named tables. See [database-source-behaviors-oracle.md](database-source-behaviors-oracle.md). |
| `INGESTION_GATEWAY_TABLE_DROPPED`, or "Failed to resolve flow" for a table's flows | A source table was dropped. The update fails at analysis and blocks every table in the pipeline. | Remove the dropped table from the ingestion pipeline. |
| Lag looks healthy but the destination is empty | The lag doesn't show whether rows were written | Count the destination rows and read `DESCRIBE HISTORY` (`numTargetRowsInserted`). See [database-source-behaviors.md](database-source-behaviors.md#read-the-destination-not-only-the-status). |
| "The number of tables in this pipeline ... exceeds the quota of ... tables per pipeline" | The hidden CDC staging table counts toward the quota | Keep each pipeline one table under the quota, and split the rest over pipelines. See [table limits](database-source-behaviors.md#table-limits). |
| Any other database source error (`INGESTION_GATEWAY_*`, `ORA-*`, replication slots, grants, logs) | Source settings, grants, or log retention | Read the source's troubleshooting page, then its file in this skill: [database-source-behaviors.md](database-source-behaviors.md). |

---

## Escalation pointers

When the event log doesn't explain a failure:

1. **Public docs hub** — [Lakeflow Connect overview](https://docs.databricks.com/ingestion/lakeflow-connect) covers concepts and links to per-connector pages.
2. **Connector reference** — [per-connector setup](https://docs.databricks.com/ingestion/lakeflow-connect/) is the canonical source for current auth, limits, and supported objects per source.
3. **Workspace support** — file a support case from Help -> Contact Support inside the workspace; attach the pipeline ID and a relevant `event_log` extract.

For monitoring patterns beyond event-log queries (dashboards, alerting on pipeline state, SLAs), see **databricks-pipelines**.
