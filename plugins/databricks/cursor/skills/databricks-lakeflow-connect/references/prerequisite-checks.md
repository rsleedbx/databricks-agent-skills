# Prerequisite checks

Checks to run before any Lakeflow Connect pipeline is created. Asking first, and the short list: [SKILL.md](../SKILL.md#prerequisites).

**Where tables land, and same-name tables:** [integrated-cdc.md](integrated-cdc.md#where-each-table-lands-what-lfc-names-what-you-choose).

**Before you ask for the inputs:**

- **Postgres:** offer an existing database (host, port, database, read-only user), or a Lakebase demo stand-in, which runs end to end with [lakebase-as-postgres-source.md](lakebase-as-postgres-source.md) (its Step 0 lists the inputs to collect).
- **An existing UC connection but no tables named:** list them first with [Step 0](query-based-pipeline.md#step-0--find-the-source-tables-through-the-connection), and propose tables from what you find. For full detail, introspect the whole database in one shot with [source-sql-through-connection.md](source-sql-through-connection.md) (all columns in one query, all keys and indexes in another, never table by table). If the connection sees more than one database, ask which one.
- **Query-based:** run the [pre-flight checks](query-based-connectors.md#pre-flight-checks-before-creating-the-pipeline-advise-the-product-doesnt-check): the product doesn't verify that a declared key is unique, that the cursor is indexed, or that it has no NULLs. Report what you find, and let the user skip it (for example for a snapshot or a test).
- **The plan** lists what you'll create: source, UC connection, target schema, pipeline, job.

Confirm before creating any pipeline:

1. **A Unity Catalog target** — catalog and schema must exist. **CDC pipelines (integrated and standard) can't use a Default Storage catalog**: create fails with "Default Storage for Unity Catalog is not supported on LakeFlow Connect pipelines". Query-based pipelines work there. A catalog is on Default Storage when its `storage_root` is under the URL of the system external location `__databricks_managed_storage_location` (`databricks external-locations get __databricks_managed_storage_location`). Check before building CDC, and if it is, ask for a catalog with its own managed location. A catalog with its own location can still fail at create with "Azure storage credential was not found. The access connector may have been deleted or recreated": its storage credential is broken, so use another catalog and tell the user. On a metastore with no storage root, `databricks catalogs create` fails, while SQL `CREATE CATALOG <name>` creates a Default Storage catalog: fine for query-based, not for CDC. Beyond that, the service principal or user creating the pipeline needs `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, and `MODIFY` on the target schema.
2. **A UC `CONNECTION` object** with credentials for the source, created with the CLI or a bundle. Creating one requires `CREATE CONNECTION` on the metastore.
3. **For CDC database connectors**: network reachability from the compute that reads the change log to the source database. That's the integrated CDC pipeline itself, or the gateway for standard CDC. Serverless or classic depends on the current docs and the workspace's compute policy. On-prem requires ExpressRoute (Azure) or Direct Connect (AWS).
4. **For query-based connectors**: network reachability from **serverless** compute to the source, plus a monotonic cursor column (e.g. `updated_at`, bumped on every update) on each table.

## Default names

Ask the user for names. Without them:

- **Anything the user keeps:** propose `<source>_<purpose>` (for example `erp_orders`), with `_gw` / `_ap` for a standard CDC pair and `_staging` for the staging schema, and confirm it with the plan.
- **Demos and tests:** the pipeline, its target schema, its schedule job and its bundle share one name, `<base>_<epoch>` (written `<name>` in the references), whether they're created in the UI, with a bundle, or through the REST API:
  - **`<base>`** is `<firstname><lastname>_<connection-name>`, lowercased, with every character other than letters and digits turned into `_` (Jane Doe with connection `my-pg-conn` → `janedoe_my_pg_conn`).
  - **First and last name:** `name.givenName` + `name.familyName` from `databricks current-user me`, ASCII letters and digits only (Jane Doe → `janedoe`; accents are dropped). If those are empty, use the part of `userName` before `@`, without dots.
  - **`<epoch>`** is the seconds since the epoch, taken once (`date +%s`) when the user says yes to the plan, and reused for every object of that setup: `janedoe_my_pg_conn_1792351234` for the pipeline, the schema, the job, and the bundle. Write it down (in the bundle's `variables`, or the shell) rather than calling `date` again, so a UI step done later still matches. A new setup gets a new epoch; still check the bundle and the Postgres slot names below before creating them, in case an epoch is reused.
  - **Standard (two-pipeline) CDC:** the gateway is `<base>_gw_<epoch>` and the ingestion pipeline (the applier) `<base>_ap_<epoch>`; the schema and job are `<base>_<epoch>`, with the same epoch.
  - **Staging schema** (integrated `data_staging_options`, or the gateway's `gateway_storage_schema`): `<base>_<epoch>_staging`.
  - **A connection you create** without a name from the user: `<firstname><lastname>-<engine>-<target>` (`janedoe-mysql-awsvm`), with no epoch: connections are shared by many pipelines.
  - **A connection that already starts with `<firstname><lastname>` followed by `-` or `_`** (as the source skills name them) isn't prefixed again: connection `janedoe-mysql-azvm` gives `janedoe_mysql_azvm_<epoch>`, not `janedoe_janedoe_mysql_azvm_<epoch>`. A connection that only starts with the same letters, such as `janedoes_pg`, is still prefixed: `janedoe_janedoes_pg_<epoch>`.

Constraints that hold for any name:

- **Keep the name a Postgres slot takes (`<base>_<epoch>`, or the pipeline pair's name) to 59 characters or fewer.** The slot takes this name and its publication adds `_pub`, and Postgres silently truncates names longer than 63 characters (see [source-sql-through-connection.md](source-sql-through-connection.md)). Shorten the connection or purpose part if needed, never the epoch.
- **`mode: development` renames what this skill derives names from:** it prefixes every resource name with `[dev <user>]`, so the pipeline name no longer matches the slot, publication and schema names taken from it. Leave it out of targets that create these pipelines, or take the names from the deployed resources.
- **The bundle's name** decides where its deploy state lives (`~/.bundle/<name>/<target>` in the workspace), and reusing another bundle's name from a new directory makes `bundle deploy` **delete and replace that bundle's pipelines**. The epoch keeps a demo's name new; for any name, check that `databricks workspace list /Workspace/Users/<user>/.bundle/<name>` doesn't exist before the first deploy.
