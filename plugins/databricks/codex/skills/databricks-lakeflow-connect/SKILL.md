---
name: databricks-lakeflow-connect
description: "Build managed ingestion pipelines with Lakeflow Connect from SaaS apps (Salesforce, Workday, ServiceNow, ...) and databases (SQL Server, PostgreSQL, MySQL, Oracle; CDC or query-based) into Unity Catalog. Also for demoing the Postgres connector with Lakebase as the source."
compatibility: Requires databricks CLI (>= v0.294.0)
metadata:
  version: "0.3.0"
parent: databricks-core
---

# Lakeflow Connect

Build managed ingestion pipelines that pull from SaaS apps and databases into Unity Catalog Delta tables, governed end-to-end and powered by serverless Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables / DLT).

**Status:** mixed catalog — GA connectors for production use, plus Public Preview, Beta, and Private Preview connectors that expand over time. See the connector catalog below.

---

## What Is Lakeflow Connect?

Managed connectors for ingesting data from SaaS applications and databases. The resulting ingestion pipeline is governed by Unity Catalog and powered by serverless compute and Lakeflow Spark Declarative Pipelines.

There are five architecture patterns:

1. **SaaS pull** — reads an external SaaS via OAuth or API key into a streaming Delta table.
2. **Integrated database CDC** (the default for CDC) — one pipeline reads the change log (SQL Server change tracking / CDC, MySQL binlog, Oracle redo log) inside each update. No separate gateway pipeline. Check the current docs for source support and compute ([integrated-cdc.md](references/integrated-cdc.md#integrated-cdc-default-vs-standard-cdc)).
3. **Standard database CDC via gateway** — only when the user asks for standard or gateway CDC, or the source has no integrated CDC. An always-on gateway stages changes in a UC Volume ([standard-cdc-gateway.md](references/standard-cdc-gateway.md)).
4. **Query-based** — for any of these sources, with or without CDC (Oracle / Teradata / SQL Server / PG / MySQL / MariaDB, or a Federation foreign catalog): one serverless pipeline pulls rows past a cursor's high-water mark each run. No separate gateway pipeline.
5. **Community connectors** — template-based, out of scope for this skill.

---

## Is Lakeflow Connect the right tool?

Decide this before you build. Lakeflow Connect is the managed **pull** path for SaaS apps and databases.

| If the source is... | Use | Skill |
|---|---|---|
| A SaaS app or database with a managed connector (Salesforce, Workday, ServiceNow, GA4, HubSpot, Confluence, SQL Server, ...) | **Lakeflow Connect** | this skill |
| Files on cloud object storage (S3 / ADLS / GCS) | Auto Loader | **databricks-pipelines** |
| A source you want to query in place, no copy | Lakehouse Federation | — |
| An app or device that **pushes** events at you | Zerobus | **databricks-zerobus-ingest** |
| Lakebase Postgres, to get its data into UC | Lakehouse Sync (native) | **databricks-lakebase** |
| Lakebase Postgres as a stand-in for customer PostgreSQL in a **demo** | **Lakeflow Connect** (Postgres query-based) | this skill — [lakebase-as-postgres-source.md](references/lakebase-as-postgres-source.md) |
| A partner offering a Delta share | Delta Sharing | — |

SFTP and SharePoint look like file sources but go through Lakeflow Connect, not Auto Loader, which is for cloud object storage. Whether you need a copy decides Lakehouse Federation (query in place) or Lakeflow Connect (a governed copy).

---

## Connector catalog

**GA** and **Public Preview** connectors are production-supported; **Beta** and **Private Preview** are early-access and not production-supported.

### GA connectors

Full coverage in this skill, except rows whose Reference is a public docs page.

| Source | Type | Auth | Reference |
|--------|------|------|-----------|
| Salesforce (Sales / Service / etc.) | SaaS pull | OAuth U2M | [saas-connectors.md](references/saas-connectors.md) |
| Workday Reports (RaaS) | SaaS pull | OAuth refresh token / basic | [saas-connectors.md](references/saas-connectors.md) |
| ServiceNow | SaaS pull | OAuth U2M / basic | [saas-connectors.md](references/saas-connectors.md) |
| Google Analytics 4 | SaaS pull (via BigQuery) | Service-account JSON | [saas-connectors.md](references/saas-connectors.md) |
| HubSpot | SaaS pull | OAuth | [saas-connectors.md](references/saas-connectors.md) |
| Confluence | SaaS pull | OAuth | [saas-connectors.md](references/saas-connectors.md) |
| NetSuite | SaaS pull | Token-based authentication (TBA) | [NetSuite connector docs](https://docs.databricks.com/ingestion/lakeflow-connect/netsuite) |
| Dynamics 365 | SaaS pull (via Azure Synapse Link) | OAuth U2M | [Dynamics 365 connector docs](https://docs.databricks.com/ingestion/lakeflow-connect/d365) |
| SQL Server (cloud) | Database CDC: integrated (default) or standard (gateway) | DB user + change tracking / CDC | [sql-server-cdc.md](references/sql-server-cdc.md) |
| SQL Server (on-prem) | Database CDC | DB user + ExpressRoute / Direct Connect | [sql-server-cdc.md](references/sql-server-cdc.md) |
| MySQL (integrated CDC pipeline, no separate gateway pipeline) | Database CDC: integrated | DB user + binlog | [integrated-cdc.md](references/integrated-cdc.md) |
| Oracle / Teradata / SQL Server / PG / MySQL / MariaDB (query-based) | Database query, serverless, no separate gateway pipeline | Read-only DB user + cursor column | [query-based-connectors.md](references/query-based-connectors.md) |
| Snowflake / Redshift / Synapse / BigQuery (Foreign Catalog) | Database query | Foreign Catalog | [query-based-connectors.md](references/query-based-connectors.md) |

### Public Preview connectors

Production-supported; configuration may change before GA.

| Source | Type | Auth | Reference |
|--------|------|------|-----------|
| PostgreSQL CDC | Database CDC (standard, gateway); no integrated CDC yet | Replication user + logical replication | [postgresql-cdc.md](references/postgresql-cdc.md) |
| MySQL CDC (gateway-based) | Database CDC (standard, gateway) | DB user + binlog + gateway | [standard-cdc-gateway.md](references/standard-cdc-gateway.md) |
| SFTP | File pull | Key / password | [connector docs](https://docs.databricks.com/ingestion/lakeflow-connect/) |

### Beta and Private Preview

Not production-supported; check the [connector reference](https://docs.databricks.com/ingestion/lakeflow-connect/).

| Source | Type | Auth | Reference |
|--------|------|------|-----------|
| Oracle CDC | Integrated CDC (Beta, `channel: PREVIEW`); no standard mode | DB user; connection `service_name` = the CDB's service | [integrated-cdc.md](references/integrated-cdc.md#integrated-cdc-default-vs-standard-cdc) |

---

## Required Tools

- **Databricks CLI v0.294.0+** for `databricks pipelines create` and `databricks connections create` (`databricks --version`).
- **Databricks SDK for Python** (`databricks-sdk>=0.85.0`) or **Declarative Automation Bundles** (recommended for pipelines that ship to a customer environment). No connector-specific SDK is needed; the API and CLI match other pipelines, with `ingestion_definition` in place of `libraries`.

## Prerequisites

**Bare demo request.** With no source, connector, or CDC mode given, propose Lakebase as the Postgres source with query-based ingestion, once, and proceed when the user confirms. Follow [lakebase-as-postgres-source.md](references/lakebase-as-postgres-source.md). If the user names a source, skip this proposal and use that source.

**Names.** Ask the user for names; for a demo or test without them, one `<base>_<epoch>` for the pipeline, schema, job and bundle. Rules and the limits that hold for any name: [default names](references/prerequisite-checks.md#default-names).

**Where tables land and same-name tables:** [integrated-cdc.md](references/integrated-cdc.md#where-each-table-lands-what-lfc-names-what-you-choose).

**Ask before creating anything.** Ask which source to ingest from; for Postgres, offer an existing database or a Lakebase demo stand-in, and never create a stand-in yourself. Ask for every missing input in one message, list what you'll create, and wait for one "yes"; then run the whole setup without asking again. First confirm the checks in [prerequisite-checks.md](references/prerequisite-checks.md): a UC target catalog (not Default Storage for CDC), a UC `CONNECTION`, network reachability from the compute that reads the source, and for query-based the [pre-flight checks](references/query-based-connectors.md#pre-flight-checks-before-creating-the-pipeline-advise-the-product-doesnt-check).

---

## Running the pipeline

Minimal JSON for `databricks pipelines create`, a DAB-authored version, and how to run and poll an update: [authoring-and-running.md](references/authoring-and-running.md).

---

## Detailed guides

| File | When to read |
|------|--------------|
| [saas-connectors.md](references/saas-connectors.md) | SaaS connectors: per-connector deltas, OAuth, DAB stubs |
| [integrated-cdc.md](references/integrated-cdc.md) | Integrated CDC: setup, source prerequisites, pipeline YAML, table mapping, smart closure, self-started updates, behaviors |
| [standard-cdc-gateway.md](references/standard-cdc-gateway.md) | Standard (gateway) CDC: two pipelines, the gateway's compute, gotchas |
| [sql-server-cdc.md](references/sql-server-cdc.md) | SQL Server: change tracking vs CDC, `SCD_TYPE_2`, source table changes, setup, grants, capture instances, governor limits |
| [postgresql-cdc.md](references/postgresql-cdc.md) | PostgreSQL CDC: slot, publication, pipelines, gotchas; slot, `wal_level`, replica identity and TLS errors |
| [compute-and-feature-gates.md](references/compute-and-feature-gates.md) | Serverless vs classic compute, what ran, workspace feature gates |
| [query-based-connectors.md](references/query-based-connectors.md) | Query-based concepts: cursors, run behavior, shape table, pre-flight |
| [query-based-pipeline.md](references/query-based-pipeline.md) | Query-based: finding tables, keys and cursors; connection, pipeline, schedule, networking, gotchas |
| [troubleshooting-and-monitoring.md](references/troubleshooting-and-monitoring.md) | Event-log queries, runtime per update, common errors |
| [rows-per-run.md](references/rows-per-run.md) | Rows per table per run: [`scripts/rows_per_run.sql`](scripts/rows_per_run.sql), column meanings, SCD2 counts |
| [lakebase-as-postgres-source.md](references/lakebase-as-postgres-source.md) | Lakebase as a demo Postgres source, end to end; trying Postgres CDC on Lakebase |
| [source-sql-through-connection.md](references/source-sql-through-connection.md) | Introspecting a source through a UC connection; test DML; replication slots; the source SQL notebook |
| [database-source-behaviors.md](references/database-source-behaviors.md) | Docs troubleshooting pages per source; destination checks, gateway compute, table limits; MySQL and Oracle errors |
| [prerequisite-checks.md](references/prerequisite-checks.md) | Catalog, grants, connection and network checks; default names for demos and tests |
| [anti-patterns.md](references/anti-patterns.md) | Wrong and right forms of the four most common mistakes |
| [authoring-and-running.md](references/authoring-and-running.md) | Minimal `pipelines create` JSON, the DAB form, running and polling an update |

## Workflow

For each new ingestion pipeline:

1. **Pick the connector category** — SaaS / database CDC / database query-based / file / push — and read the matching reference file.
2. **Verify prerequisites** — UC target, source credentials, network path (for databases), region availability.
3. **Create the UC `CONNECTION`** — UI for OAuth U2M, CLI / DAB for everything else.
4. **Author the pipeline** — `databricks pipelines create --json` for one-offs, DAB YAML for anything shipping to a customer.
5. **Trigger the first run** and watch the event log ([troubleshooting-and-monitoring.md](references/troubleshooting-and-monitoring.md)).
6. **Schedule** it with a Jobs `pipeline_task` (cron or interval): `continuous: false` isn't a schedule. Integrated CDC: the interval in [smart closure](references/integrated-cdc.md#how-an-update-runs-smart-closure).

---

## Anti-patterns

Four forms that look plausible but fail. The wrong and right JSON and SQL: [anti-patterns.md](references/anti-patterns.md).

1. **Federation syntax** (`CREATE TABLE ... FROM CONNECTION`) ([anti-patterns](references/anti-patterns.md#1-federation-syntax)).
2. **A `libraries` block.** An ingestion pipeline carries `ingestion_definition`, never `libraries` ([anti-patterns](references/anti-patterns.md#2-a-libraries-block)).
3. **`continuous: true` on a triggered connector** ([anti-patterns](references/anti-patterns.md#3-continuous-true-on-a-triggered-connector)).
4. **A cursor that isn't bumped on update.** It silently misses changes ([anti-patterns](references/anti-patterns.md#4-a-cursor-that-isnt-bumped-on-update)).

## Important

- **Query-based and standard-CDC ingestion pipelines are triggered** — on a schedule or on demand. The standard-CDC gateway runs continuously from creation: stop it when you're done, since it bills and holds WAL. Integrated CDC has a continuous option; check its docs page.
- **Compute-only billing** — DBUs, no per-row fee; standard CDC adds an always-on gateway. Rates: the [pricing page](https://www.databricks.com/product/pricing/lakeflow-connect).
- **Salesforce auth is OAuth U2M only** — no machine-to-machine or basic auth.
- **Database staging retention is 30 days** by default for standard CDC. Integrated CDC stages in the volume set by `data_staging_options`; check its docs page for retention.
- **Limits per pipeline** — most SaaS connectors cap at 250 tables per pipeline. Split across pipelines if needed.
- **This lands raw tables** — build Bronze/Silver/Gold on top with **databricks-pipelines**.

## Key Concepts

- **UC `CONNECTION` is the credential anchor** — every pipeline points at a UC connection, which owns the auth.
- **One pipeline, plus a gateway only for standard CDC** — SaaS connectors are pure serverless; compute for database pipelines: [compute-and-feature-gates.md](references/compute-and-feature-gates.md).
- **Query-based = cursor + latest state** — a run pulls rows whose cursor moved past the high-water mark. It sees the latest state of each changed row, misses hard deletes unless configured, and uses `deletion_condition` for soft deletes.
- **CDC and schema evolution are built in** — changes are applied incrementally and the target schema evolves. Data-type changes typically require a full snapshot reload.
- **Streaming Delta output** — destination tables are governed Delta tables; CDC sources apply with change semantics (`APPLY CHANGES` / AUTO CDC, or `apply_changes_from_snapshot` for snapshot sources). Compatible with downstream materialized views and Spark streaming.
- **OAuth U2M is UI-only** — DAB / CLI cannot bootstrap it. Hand the browser step to a human (Catalog Explorer > External Data > Connections > Create connection > pick the source > sign in), then resume once `databricks connections get <connection_name>` reports `READY`.

## Common Issues

Error text, codes and limits change between releases, so the workspace's message and the current docs come before this skill. After every create, deploy, and update:

1. **Read the full message:** the CLI or API error, the bundle deploy output (warnings included), and the `ERROR` and `WARN` events (`databricks pipelines list-pipeline-events <pipeline-id>`). Don't stop at `FAILED`.
2. **For a database source, read its docs troubleshooting page** (links per source in [database-source-behaviors.md](references/database-source-behaviors.md)), then the source's file in this skill.
3. **Decide what it asks for:**
   - **A setting to change.** "You must use serverless compute in this workspace" means set `serverless: true` and redeploy. A `channel` or connector-type message means take the value it names.
   - **A missing grant**, for example "does not have CREATE CATALOG on Metastore". Report the privilege and who can grant it; don't work around it.
   - **A feature that isn't enabled**, for example "Ingestion source '<SOURCE>' is not supported in your workspace" ([feature gates](references/compute-and-feature-gates.md#workspace-feature-gates-all-database-sources)). Report it and offer the modes that work.
   - **A source-side fix:** network, credentials, or CDC setup on the database.
4. **Fix that one thing, then retry the same step.** When the error gives a way forward, take it and continue.
5. **Clean up what a failed step left behind.** A failed `bundle deploy` can leave schemas: offer `bundle destroy`. Report what's left and which errors the workspace showed.

Errors that follow from how the pipelines and sources work (duplicate keys, cursors, SCD2 counts, replication slots, change-log retention, dropped tables, table quotas), with the event-log queries to diagnose them: [troubleshooting-and-monitoring.md](references/troubleshooting-and-monitoring.md).

---

## Related Skills

- **databricks-pipelines** — the SDP runtime these pipelines run on. For Auto Loader and downstream pipeline patterns.
- **databricks-zerobus-ingest** — push-based gRPC ingestion. Sibling to Lakeflow Connect's pull-based connectors.
- **databricks-dabs** — author Lakeflow Connect pipelines as IaC.
- **databricks-unity-catalog** — managing catalogs, schemas, and the UC `CONNECTION` objects that LFC credentials live in.
- **databricks-jobs** — schedule ingestion pipelines with `pipeline_task`.
- **databricks-lakebase** — Lakebase projects and roles for a demo Postgres source; Lakehouse Sync for real Lakebase → UC ingestion.
- **databricks-synthetic-data-gen** — generate realistic seed data for connector demos.

---

## Resources

- [Lakeflow Connect public docs hub](https://docs.databricks.com/ingestion/lakeflow-connect)
- [Connector reference (per-connector setup)](https://docs.databricks.com/ingestion/lakeflow-connect/)
- [Pricing](https://www.databricks.com/product/pricing/lakeflow-connect)
