---
name: databricks-lakeflow-connect
description: "Build managed ingestion pipelines into Databricks using Lakeflow Connect. Use when ingesting from SaaS apps (Salesforce, Workday Reports, ServiceNow, Google Analytics 4, HubSpot, Confluence) or databases (SQL Server cloud and on-prem, integrated single-pipeline or standard gateway CDC; MySQL integrated CDC; PostgreSQL/MySQL standard CDC; query-based ingestion from Oracle, Teradata, SQL Server, PostgreSQL, MySQL, MariaDB using a cursor column) into Unity Catalog with serverless pipelines. Also use when demoing the Postgres connector with Lakebase as a stand-in PostgreSQL source."
compatibility: Requires databricks CLI (>= v0.294.0)
metadata:
  version: "0.2.0"
parent: databricks-core
---

# Lakeflow Connect

Build managed ingestion pipelines that pull from SaaS apps and databases into Unity Catalog Delta tables, governed end-to-end and powered by serverless Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables / DLT).

**Status:** mixed catalog — GA connectors for production use, plus Public Preview, Beta, and Private Preview connectors that expand over time. See the connector catalog below.

---

## What Is Lakeflow Connect?

Managed connectors for ingesting data from SaaS applications and databases. The resulting ingestion pipeline is governed by Unity Catalog and powered by serverless compute and Lakeflow Spark Declarative Pipelines.

Three frames to keep in mind:

- **Simple and low-maintenance** — no client code to write, no message bus to operate; connector + UC Connection + a serverless pipeline.
- **Unified with the lakehouse** — credentials stored in UC, output is governed Delta, runs on Jobs and SDP like any other workload.
- **Efficient incremental processing** — change tracking / CDC / schema evolution / retries are built in.

There are five architecture patterns:

1. **SaaS pull** — connector reads from an external SaaS via OAuth or API key, lands in a streaming Delta table.
2. **Integrated database CDC** (the default for CDC) — one pipeline reads the change log (SQL Server CDC / change tracking, MySQL binlog) inside each update and applies the changes into Delta. No separate gateway. Set with `connection_name` + `connector_type: CDC`. Source support, release stage, and compute options change often: check the current docs before building (see [2-database-connectors.md](references/2-database-connectors.md#integrated-cdc-default-vs-standard-cdc)). When this was written: SQL Server GA, MySQL Beta, PostgreSQL not available.
3. **Standard database CDC via gateway** — only when the user asks for standard / two-pipeline / gateway CDC, or the source has no integrated CDC. An always-on ingestion gateway stages change events to a UC Volume, and a serverless ingestion pipeline applies them as CDC into Delta.
4. **Query-based** — for sources without CDC enabled (Oracle / Teradata / SQL Server / PG / MySQL / MariaDB, or any Lakehouse Federation foreign catalog), a single serverless pipeline queries for rows past a cursor column's high-water mark on each run. No gateway, no staging Volume.
5. **Community connectors** — template-based, out of scope for this skill.

---

## Is Lakeflow Connect the right tool?

Decide this before you build. Lakeflow Connect is the managed **pull** path for SaaS apps and databases — it is not the answer for every ingestion intent.

| If the source is... | Use | Skill |
|---|---|---|
| A SaaS app or database with a managed connector (Salesforce, Workday, ServiceNow, GA4, HubSpot, Confluence, SQL Server, ...) | **Lakeflow Connect** | this skill |
| Files on cloud object storage (S3 / ADLS / GCS) | Auto Loader | **databricks-pipelines** |
| A source you want to query in place, no copy | Lakehouse Federation | — |
| An app or device that **pushes** events at you | Zerobus | **databricks-zerobus-ingest** |
| Lakebase Postgres, to get its data into UC | Lakehouse Sync (native) | **databricks-lakebase** |
| Lakebase Postgres as a stand-in for customer PostgreSQL in a **demo** | **Lakeflow Connect** (Postgres query-based) | this skill — [6-lakebase-as-postgres-source.md](references/6-lakebase-as-postgres-source.md) |
| A partner offering a Delta share | Delta Sharing | — |

Full reasoning, including the Federation-vs-Connect and Auto-Loader-vs-Connect trade-offs, is in [4-ingestion-decision-tree.md](references/4-ingestion-decision-tree.md).

---

## Connector catalog

Lakeflow Connect ships connectors at multiple release stages. **GA** and **Public Preview** connectors are production-supported; **Beta** and **Private Preview** are early-access and not production-supported.

### GA connectors

Full coverage in this skill, except rows whose Reference is a public docs page.

| Source | Type | Auth | Reference |
|--------|------|------|-----------|
| Salesforce (Sales / Service / etc.) | SaaS pull | OAuth U2M | [1-saas-connectors.md](references/1-saas-connectors.md) |
| Workday Reports (RaaS) | SaaS pull | OAuth refresh token / basic | [1-saas-connectors.md](references/1-saas-connectors.md) |
| ServiceNow | SaaS pull | OAuth U2M / basic | [1-saas-connectors.md](references/1-saas-connectors.md) |
| Google Analytics 4 | SaaS pull (via BigQuery) | Service-account JSON | [1-saas-connectors.md](references/1-saas-connectors.md) |
| HubSpot | SaaS pull | OAuth | [1-saas-connectors.md](references/1-saas-connectors.md) |
| Confluence | SaaS pull | OAuth | [1-saas-connectors.md](references/1-saas-connectors.md) |
| NetSuite | SaaS pull | Token-based authentication (TBA) | [NetSuite connector docs](https://docs.databricks.com/ingestion/lakeflow-connect/netsuite) |
| Dynamics 365 | SaaS pull (via Azure Synapse Link) | OAuth U2M | [Dynamics 365 connector docs](https://docs.databricks.com/ingestion/lakeflow-connect/d365) |
| SQL Server (cloud) | Database CDC: integrated (default) or standard (gateway) | DB user + change tracking / CDC | [2-database-connectors.md](references/2-database-connectors.md) |
| SQL Server (on-prem) | Database CDC | DB user + ExpressRoute / Direct Connect | [2-database-connectors.md](references/2-database-connectors.md) |

### Public Preview connectors

Production-supported. Configuration may evolve before GA. Rows that link to the public docs aren't covered in depth yet; see the [public connector reference](https://docs.databricks.com/ingestion/lakeflow-connect/) for current setup steps.

| Source | Type | Auth | Reference |
|--------|------|------|-----------|
| PostgreSQL CDC | Database CDC (standard, gateway); no integrated CDC yet | Replication user + logical replication | [2-database-connectors.md](references/2-database-connectors.md) |
| MySQL CDC | Integrated CDC (default; Beta when written) or standard (gateway) | DB user + binlog | [2-database-connectors.md](references/2-database-connectors.md#integrated-cdc-default-vs-standard-cdc) |
| Oracle / Teradata / SQL Server / PG / MySQL / MariaDB (query-based) | Database query, serverless, no gateway | Read-only DB user + cursor column | [3-query-based-connectors.md](references/3-query-based-connectors.md) |
| Snowflake / Redshift / Synapse / BigQuery (Foreign Catalog) | Database query | Foreign Catalog | [3-query-based-connectors.md](references/3-query-based-connectors.md) |
| SFTP | File pull | Key / password | [connector docs](https://docs.databricks.com/ingestion/lakeflow-connect/) |

### Beta and Private Preview

Early-access connectors are not production-supported. The list changes month to month; check the [public connector reference](https://docs.databricks.com/ingestion/lakeflow-connect/) for current availability.

For the Lakeflow-Connect-vs-Auto-Loader-vs-Federation-vs-Delta-Sharing decision, see [4-ingestion-decision-tree.md](references/4-ingestion-decision-tree.md).

---

## Required Tools

- **Databricks CLI v0.294.0+** for `databricks pipelines create` and `databricks connections create`. Verify with `databricks --version`.
- **Databricks SDK for Python** (`databricks-sdk>=0.85.0`) if you prefer SDK over CLI.
- **Declarative Automation Bundles** if authoring as IaC (recommended for any pipeline that ships to a customer environment).

No extra connector-specific SDK is needed. Lakeflow Connect reuses the pipelines API surface — pipelines are created with an `ingestion_definition` block instead of a `libraries` block, but the API and CLI are otherwise the same.

---

## Prerequisites

**A bare demo request means Lakebase + query-based.** If the user asks for a Lakeflow Connect demo and gives no source, connector, or CDC mode, assume Lakebase as the Postgres source with query-based ingestion. Follow [6-lakebase-as-postgres-source.md](references/6-lakebase-as-postgres-source.md) and propose its Step 0 defaults (still confirm once before creating).

**Default pipeline names.** If the user doesn't name the pipeline, call it `<firstname><lastname>-<connection-name>`:

- **First and last name:** `name.givenName` + `name.familyName` from `databricks current-user me`, lowercased, letters and digits only (Jane Doe → `janedoe`). If those are empty, use the part of `userName` before `@`, without dots.
- **Standard (two-pipeline) CDC:** the gateway is `<base>-gw` and the ingestion pipeline (the applier) is `<base>-ap`.
- **Query-based and integrated CDC:** a single pipeline, `<base>`.
- **A connection you create** without a name from the user: `<firstname><lastname>-<engine>-<target>` (`janedoe-mysql-awsvm`), the same first and last name as above.
- **A connection that already starts with `<firstname><lastname>-`** (as the source skills name them) is the base as is, without repeating the prefix: `janedoe-mysql-azvm`, not `janedoe-janedoe-mysql-azvm`.
- **If a name is taken,** append the next free number: `<base>-2`, `<base>-3`, ... For standard CDC, use the same number for both: `<base>-gw-2` / `<base>-ap-2`. Check with `databricks pipelines list-pipelines --filter "name LIKE '<base>%'"` and look at the exact names it returns.
- **In bundles, don't use `mode: development`:** it prefixes every name with `[dev <user>]`.
- **Bundle name:** use the pipeline name with `-` turned into `_` (e.g. `janedoe_my_pg_conn_2`). A bundle's name and target decide where its deploy state lives (`~/.bundle/<name>/<target>` in the workspace). Reusing another bundle's name from a new directory makes `bundle deploy` **delete and replace that bundle's pipelines**. Before the first deploy, check that `databricks workspace list /Workspace/Users/<user>/.bundle/<name>` doesn't exist.

**Ask before creating anything.** If the user hasn't named a source, ask which database or app to ingest from. For Postgres, offer two choices: an existing database (host, port, database, read-only user), or a Lakebase demo stand-in, which runs end to end with [6-lakebase-as-postgres-source.md](references/6-lakebase-as-postgres-source.md) (its Step 0 lists the inputs to collect). If the user names an existing UC connection but not the tables, list them first with Step 0 of [3-query-based-connectors.md](references/3-query-based-connectors.md), and propose tables from what you find. For full detail, introspect the whole database in one shot with [7-source-sql-through-connection.md](references/7-source-sql-through-connection.md) (all columns in one query, all keys and indexes in another, never table by table). If the connection sees more than one database, ask which one. Never stand up a stand-in source (a Lakebase project or branch, a new database, seed tables) on your own. Before a query-based pipeline, run the pre-flight checks in [3-query-based-connectors.md](references/3-query-based-connectors.md#pre-flight-checks-before-creating-the-pipeline-advise-the-product-doesnt-check): the product doesn't verify that a declared key is unique, that the cursor is indexed, or that it has no NULLs. Report what you find, and let the user skip it (for example for a snapshot or a test). Ask for every missing input in one message, list what you'll create (source, UC connection, target schema, pipeline, job), and wait for one "yes". Then run the whole setup without asking again.

Confirm before creating any pipeline:

1. **A Unity Catalog target** — catalog and schema must exist. **CDC pipelines (integrated and standard) can't use a Default Storage catalog**: create fails with "Default Storage for Unity Catalog is not supported on LakeFlow Connect pipelines". Query-based pipelines work there. A catalog is on Default Storage when its `storage_root` is under the URL of the system external location `__databricks_managed_storage_location` (`databricks external-locations get __databricks_managed_storage_location`). Check before building CDC, and if it is, ask for a catalog with its own managed location. A catalog with its own location can still fail at create with "Azure storage credential was not found. The access connector may have been deleted or recreated": its storage credential is broken, so use another catalog and tell the user. On a metastore with no storage root, `databricks catalogs create` fails, while SQL `CREATE CATALOG <name>` creates a Default Storage catalog: fine for query-based, not for CDC. Beyond that, the service principal or user creating the pipeline needs `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, and `MODIFY` on the target schema.
2. **A UC `CONNECTION` object** with credentials for the source. SaaS OAuth U2M connections must be created via the UI (Catalog Explorer); API-key, basic-auth, and database connections can be created via CLI / DAB. Creating one requires `CREATE CONNECTION` on the metastore.
3. **For CDC database connectors**: network reachability from the compute that reads the change log to the source database. That's the integrated CDC pipeline itself, or the gateway for standard CDC. Serverless or classic depends on the current docs and the workspace's compute policy. On-prem requires ExpressRoute (Azure) or Direct Connect (AWS).
4. **For query-based connectors**: network reachability from **serverless** compute to the source, plus a monotonic cursor column (e.g. `updated_at`, bumped on every update) on each table.
5. **For file connectors**: OAuth scope grants on the SaaS file repo (SharePoint / Google Drive).

---

## Minimal Example — Salesforce ingestion pipeline

The canonical authoring path is JSON to `databricks pipelines create --json`. (There is no SQL `CREATE TABLE … FROM CONNECTION` syntax for Lakeflow Connect — that syntax exists only for Lakehouse Federation, which is a different product.)

```bash
databricks pipelines create --json '{
  "name": "salesforce_to_uc",
  "ingestion_definition": {
    "connection_name": "my_salesforce_oauth_connection",
    "objects": [
      {"table": {"source_schema": "salesforce", "source_table": "Account",
                 "destination_catalog": "main", "destination_schema": "salesforce_raw"}},
      {"table": {"source_schema": "salesforce", "source_table": "Opportunity",
                 "destination_catalog": "main", "destination_schema": "salesforce_raw"}}
    ]
  }
}'
```

For a DAB-authored version (the production path), see [1-saas-connectors.md](references/1-saas-connectors.md).

---

## Running the pipeline

Once authored, deploy and trigger a run. The bundle path gives the cleanest run-by-key command:

```bash
databricks bundle deploy -t dev
databricks bundle run salesforce_ingestion           # KEY = the pipeline resource key in the bundle; waits by default
databricks bundle run salesforce_ingestion --no-wait
```

A pipeline created imperatively with `pipelines create --json` has no run-by-name CLI — start and poll an update by pipeline ID instead:

```bash
databricks pipelines start-update <pipeline-id>             # returns an update_id
databricks pipelines get-update  <pipeline-id> <update-id>  # poll one update's status
databricks pipelines list-updates <pipeline-id>             # recent updates and their states
```

That asymmetry is one more reason to author with a Declarative Automation Bundle.

---

## Detailed guides

| Topic | File | When to read |
|-------|------|--------------|
| SaaS connectors (Salesforce, Workday Reports, ServiceNow, GA4, HubSpot, Confluence) | [1-saas-connectors.md](references/1-saas-connectors.md) | Unified SaaS pattern, per-connector deltas, OAuth flows, DAB stubs |
| Database CDC connectors (SQL Server cloud + on-prem, PostgreSQL CDC) | [2-database-connectors.md](references/2-database-connectors.md) | Gateway pattern, change tracking vs CDC, Postgres logical replication, network setup |
| Query-based connectors (Oracle, Teradata, SQL Server, PG, MySQL, MariaDB) | [3-query-based-connectors.md](references/3-query-based-connectors.md) | No-gateway serverless pattern, cursor columns, soft deletes, SCD1/SCD2 |
| Ingestion decision tree | [4-ingestion-decision-tree.md](references/4-ingestion-decision-tree.md) | Lakeflow Connect vs Auto Loader vs Lakehouse Federation vs Delta Sharing |
| Troubleshooting and monitoring | [5-troubleshooting-and-monitoring.md](references/5-troubleshooting-and-monitoring.md) | Event log queries, common errors, escalation pointers |
| Lakebase as a Postgres source (demos) | [6-lakebase-as-postgres-source.md](references/6-lakebase-as-postgres-source.md) | End-to-end Postgres connector demo on Lakebase: password role, seed tables, pipeline, mutate-and-rerun script, teardown |
| Source-side SQL through a UC connection | [7-source-sql-through-connection.md](references/7-source-sql-through-connection.md) | Introspect a whole source database in one shot (all columns, all keys and indexes), optionally create a foreign catalog, generate test DML, and create / drop Postgres replication slots, using only the UC connection (no secrets) |

---

## Workflow

For each new ingestion pipeline:

1. **Pick the connector category** — SaaS / database CDC / database query-based / file / push — and read the matching reference file.
2. **Verify prerequisites** — UC target, source credentials, network path (for databases), region availability.
3. **Create the UC `CONNECTION`** — UI for OAuth U2M, CLI / DAB for everything else.
4. **Author the pipeline** — `databricks pipelines create --json` for one-offs, DAB YAML for anything shipping to a customer.
5. **Trigger the first run** and watch the event log; see [5-troubleshooting-and-monitoring.md](references/5-troubleshooting-and-monitoring.md) for the SQL.
6. **Schedule** the triggered pipeline with a Jobs `pipeline_task` (cron or interval). Lakeflow Connect supports triggered runs only — `continuous: false` selects triggered mode but is not itself a schedule, so the cadence comes from the Jobs trigger.

---

## Anti-patterns

Four forms that look plausible but fail — wrong vs. right:

**1. `CREATE TABLE ... FROM CONNECTION` is Lakehouse Federation, not Lakeflow Connect.**

```sql
-- WRONG: Federation syntax; no LFC equivalent exists
CREATE TABLE main.salesforce_raw.account FROM CONNECTION my_salesforce_conn;
```
```json
// RIGHT: author an ingestion_definition (see the Minimal Example above)
{"ingestion_definition": {"connection_name": "my_salesforce_conn", "objects": [/* ... */]}}
```

**2. An ingestion pipeline carries `ingestion_definition`, never a `libraries` block.**

```json
// WRONG: libraries is for a standard SDP pipeline running your notebooks/files
{"name": "salesforce_to_uc", "libraries": [{"notebook": {"path": "/Repos/.../ingest"}}]}
// RIGHT:
{"name": "salesforce_to_uc", "ingestion_definition": {"connection_name": "...", "objects": []}}
```

**3. `continuous: true` is rejected — Lakeflow Connect is triggered-only.**

```json
// WRONG: continuous mode fails at create
{"continuous": true, "ingestion_definition": {/* ... */}}
// RIGHT: continuous:false (or omit) + schedule with a Jobs pipeline_task
{"continuous": false, "ingestion_definition": {/* ... */}}
```

**4. A query-based cursor that isn't bumped on update silently misses changes.**

```yaml
# WRONG: an integer PK, or updated_at with only DEFAULT now(), never changes on UPDATE
query_based_connector_config: {cursor_columns: [order_id]}
# RIGHT: a NOT NULL timestamp bumped on every insert and update (trigger or app-set)
query_based_connector_config: {cursor_columns: [updated_at]}
```

---

## Important

- **Triggered only, no continuous mode** — pipelines run on a schedule or on-demand, never continuously. Check the connector reference for the latest status.
- **Compute-only billing** — Lakeflow Connect is billed in DBUs (no per-row fee). Standard CDC adds an always-on gateway on top of the ingestion pipeline. Integrated CDC is one pipeline whose updates run until smart closure ends them, so they can run longer than a standard CDC ingestion run. See the [pricing page](https://www.databricks.com/product/pricing/lakeflow-connect) for current rates.
- **Salesforce auth is OAuth U2M only** — no machine-to-machine, no basic auth. Connection creation requires a UI walk-through.
- **Database staging retention is 30 days** by default for standard CDC, in the UC Volume between the gateway and the ingestion pipeline. Integrated CDC stages in the volume set by `data_staging_options`; check its docs page for retention.
- **Limits per pipeline** — most SaaS connectors cap at 250 tables per pipeline. Split across multiple pipelines if needed.
- **This lands raw tables** — Lakeflow Connect writes source-faithful tables (the ingestion landing zone). Build the medallion Bronze/Silver/Gold transforms on top of them with **databricks-pipelines**.

---

## Key Concepts

- **UC `CONNECTION` is the credential anchor** — every Lakeflow Connect pipeline points at a UC connection. The connection owns the auth; the pipeline references it by name.
- **One pipeline, plus a gateway only for standard CDC** — SaaS, query-based, and integrated CDC connectors are a single pipeline. Standard CDC splits into an always-on gateway and an ingestion pipeline (Delta-bound). Compute (serverless or classic) for each depends on the current docs and the workspace's policy.
- **Query-based = cursor + latest state** — a query-based pipeline pulls rows whose cursor moved past the last high-water mark. It sees the latest state of each changed row per run, not every intermediate change, and misses hard deletes unless configured (use `deletion_condition` for soft deletes).
- **CDC and schema evolution are built in** — for sources that support change tracking or CDC, the connector applies changes incrementally and evolves the target schema. Data-type changes typically require a full snapshot reload.
- **Streaming Delta output** — destination tables are governed Delta tables; CDC sources are applied with change semantics (`APPLY CHANGES` / AUTO CDC, or `apply_changes_from_snapshot` for snapshot sources). Compatible with downstream materialized views and Spark streaming.
- **OAuth U2M is UI-only** — DAB / CLI cannot bootstrap OAuth U2M connections. Hand the one-time browser step to a human (Catalog Explorer > External Data > Connections > Create connection > pick the source > sign in), then resume once `databricks connections get <connection_name>` reports `READY`.

---

## Read every error and warning, then recover and continue

Lakeflow Connect changes quickly. Defaults, compute options, and which sources a workspace allows move from release to release, and the workspace's own message is usually more current than the docs or this skill. After every create, deploy, and update:

1. **Read the full message.** That's the CLI or API error, the bundle deploy output (warnings included), and for an update, the `ERROR` and `WARN` events in the event log (`databricks pipelines list-pipeline-events <pipeline-id>`). Don't stop at a status of `FAILED`.
2. **Decide what it asks for.** Usually one of four things:
   - **A setting to change.** For example, "You must use serverless compute in this workspace" means set `serverless: true` and redeploy. A `channel` or connector-type message means take the value it names.
   - **A missing grant**, for example "does not have CREATE CATALOG on Metastore". Report the privilege and who can grant it, and don't work around it.
   - **A feature that isn't enabled**, for example "Ingestion source '<SOURCE>' is not supported in your workspace" ([feature gates](references/2-database-connectors.md#workspace-feature-gates-all-database-sources)). Report it and offer the modes that work.
   - **A source-side fix:** network, credentials, CDC setup on the database.
3. **Fix that one thing, then retry the same step.** Change one thing at a time, so the next message tells you whether the fix worked. When the error gives a way forward, take it and move on to the next step; don't restart from the beginning.
4. **Clean up what a failed step left behind.** For example, a failed `bundle deploy` can still have created the schemas declared before the pipelines: offer `bundle destroy` ([feature gates](references/2-database-connectors.md#workspace-feature-gates-all-database-sources)). Report what's left, and which errors the workspace showed, in your summary.

---

## Common Issues

For common errors and their fixes — duplicate-key violations, watermark / cursor problems, schema evolution, gateway region availability, the `channel` runtime-channel setting, and pipelines that run but land no data — see [5-troubleshooting-and-monitoring.md](references/5-troubleshooting-and-monitoring.md), which also has the event-log queries to diagnose them.

---

## Related Skills

- **databricks-pipelines** — the SDP runtime that Lakeflow Connect pipelines run on. For Auto Loader and downstream pipeline patterns.
- **databricks-zerobus-ingest** — push-based gRPC ingestion. Sibling to Lakeflow Connect's pull-based connectors.
- **databricks-dabs** — author Lakeflow Connect pipelines as IaC.
- **databricks-unity-catalog** — managing catalogs, schemas, and the UC `CONNECTION` objects that LFC credentials live in.
- **databricks-jobs** — schedule ingestion pipelines with `pipeline_task`.
- **databricks-lakebase** — Lakebase projects, endpoints, and roles when using Lakebase as a demo Postgres source; Lakehouse Sync for real Lakebase → UC ingestion.
- **databricks-synthetic-data-gen** — generate realistic seed data for connector demos.

---

## Resources

- [Lakeflow Connect public docs hub](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect)
- [Connector reference (per-connector setup)](https://docs.databricks.com/ingestion/lakeflow-connect/)
- [Pricing](https://www.databricks.com/product/pricing/lakeflow-connect)
