# Lakebase as a Postgres source (demos)

Lakebase is real Postgres, so it can stand in for a customer's external PostgreSQL when demoing the Lakeflow Connect Postgres connectors. There's no external database to stand up, no cloud firewall to open, and the source lives in the same workspace.

**Use this only to demo the Postgres connector.** If the goal is actually to get Lakebase data into Unity Catalog, use Lakebase's native features instead — Lakehouse Sync (Lakebase → UC, SCD2 history tables) in the **databricks-lakebase** skill. They need no connector, no pipeline, and no cursor columns.

| Connector | Lakebase status |
|---|---|
| Postgres **query-based** | Works today. Follow this page. |
| Postgres **CDC** (gateway) | Rolling out. Try creating the publication ([Lakebase CDC](#postgres-cdc-on-lakebase-try-it-then-fall-back)); if it fails, use query-based. |

For Lakebase basics (projects, branches, endpoints, `databricks postgres` / `databricks psql`), load **databricks-lakebase** first.

---

## Step 0 — gather inputs, then confirm once

Collect these before running any create command. Use what the user already gave you; ask for the rest **in one message**, offering the defaults. Look up real options first so the user picks from a list (`databricks postgres list-projects`, `databricks catalogs list` filtered to `MANAGED_CATALOG`).

| Input | Default |
|---|---|
| CLI profile | none: never auto-select (see **databricks-core**) |
| Lakebase project: reuse an existing one, or create a new one | reuse `lfc-demo` if it exists, otherwise create it |
| Target UC catalog (a regular managed catalog, not the Lakebase database catalog) | none: the user must pick |
| Target schema | `<name>`, the same as the pipeline (unique, so two people's demos in one catalog don't write the same tables). Examples below write `<target_schema>`. |
| Postgres schema for this demo (also names the role and connection) | `<base>` (e.g. `mydb`): drives the role `<base>_reader` and connection `<firstname><lastname>-<base>-lakebase` |
| Pipeline, schema, job, and bundle name | `<name>` = `<firstname><lastname>_<connection>_<epoch>` (e.g. `janedoe_mydb_lakebase_1792351234`); see [Default names](prerequisite-checks.md#default-names) |
| Connector | query-based (Lakebase CDC: see [Lakebase CDC](#postgres-cdc-on-lakebase-try-it-then-fall-back)) |
| Seed the `<base>` tables from Step 3 | yes |
| Schedule job | no (run on demand) |

Then show the full list of what you'll create (project or branch, role, seed tables, UC connection, target schema, bundle) and wait for one "yes". After that, run Steps 1–6 straight through without asking again; stop only on an error you can't fix.

If the user supplied everything up front and asked you to deploy, the request is the confirmation: list what you're creating and proceed.

---

## Step 1 — pick a Lakebase project and branch

**Reuse or create, per Step 0.** Don't create a project or branch without the user's go-ahead; a new project starts billable compute. Use a dedicated project or branch so seeding and mutating tables doesn't touch anything real.

The default name `lfc-demo` doesn't mention a connector type, so the same project can serve the Postgres CDC demo once Lakebase supports it, and other demos that need a source database (for example Zerobus). **Name everything per demo, after its schema base name `<base>`.** In a shared project, each demo gets its own schema, reader role, and UC connection: `<base>`, `<base>_reader`, `<firstname><lastname>-<base>-lakebase`. For example, pick `<base>` = `mydb`, then the role is `mydb_reader` and the connection is `janedoe-mydb-lakebase`. Postgres reserves role names starting with `pg_`, so don't start `<base>` with `pg_`. If the schema is already taken, pick a new one rather than sharing it. Never reuse or `ALTER` another demo's role or delete its connection: resetting a role's password breaks every connection that uses it, so `role.sql` refuses an existing role unless you pass `-v rotate=yes` for this demo's own. List what's already there before you pick names:

```bash
databricks psql --project <project-id> --profile <PROFILE> -- -X -At \
  -c "SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg\_%' AND nspname <> 'information_schema';" \
  -c "SELECT rolname FROM pg_roles WHERE rolname LIKE '%\_reader';"
databricks connections list --profile <PROFILE>
```

```bash
# New project (auto-creates the production branch and a primary endpoint; the CLI waits for it)
databricks postgres create-project lfc-demo \
  --json '{"spec": {"display_name": "lfc-demo"}}' --profile <PROFILE>

# Existing project: find the branch and endpoint
databricks postgres list-projects --profile <PROFILE>
databricks postgres list-branches  projects/<project-id> --profile <PROFILE>
databricks postgres list-endpoints projects/<project-id>/branches/<branch-id> --profile <PROFILE>
databricks postgres get-endpoint   projects/<project-id>/branches/<branch-id>/endpoints/<endpoint-id> --profile <PROFILE>
```

Deleted projects are soft-deleted and keep their name reserved for about 7 days. If `create-project` fails because the name is taken by a deleted project, use a new name (e.g. add a `-2` suffix); don't purge or undelete it without asking.

Record the endpoint **host** and the database name (default `databricks_postgres`). Check the SQL connection works before going further:

```bash
databricks psql --project <project-id> --profile <PROFILE> -- -c "SELECT current_user, version();"
```

**Turn on native password logins.** New projects reject Postgres password logins by default, so the connector's login fails until you enable them:

```bash
databricks postgres update-project projects/<project-id> spec.enable_pg_native_login \
  --json '{"spec": {"enable_pg_native_login": true}}' --profile <PROFILE>
```

Avoid scale-to-zero computes for live demos — the first query after idle waits for a cold start.

---

## Step 2 — create a password role for the connector

**Don't use an OAuth token as the connection password.** Lakebase OAuth tokens expire after 1 hour, but the UC connection stores a fixed password. A demo built on a token works for the first run and fails an hour later.

Let Postgres generate the password, set it on the role, and write the Step 4 connection request body, all in one SQL file. The password never appears in a command line, a shell variable, or the conversation, and every command stays plain (no `$(...)`, pipes, or redirects), so it also runs under strict tool permissions. Write `role.sql` with your file tool:

```sql
-- role.sql: run with -v host=<lakebase-endpoint-host> -v base=<base> -v connname=<connection-name>
SELECT replace(gen_random_uuid()::text || gen_random_uuid()::text, '-', '') AS pw \gset
SELECT :'base' || '_reader' AS rolname \gset
SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'rolname') AS role_exists \gset
\set do_rotate false
\if :{?rotate}
SELECT :'rotate' = 'yes' AS do_rotate \gset
\endif
\if :role_exists
\if :do_rotate
ALTER ROLE :"rolname" WITH LOGIN PASSWORD :'pw';   -- this demo's own role, rotated on purpose
\else
\echo role :rolname already exists: pick a new base name, or pass -v rotate=yes to rotate the password of this demo role
DO $$ BEGIN RAISE EXCEPTION 'role already exists'; END $$;
\endif
\else
CREATE ROLE :"rolname" WITH LOGIN PASSWORD :'pw';
\endif
GRANT CONNECT ON DATABASE databricks_postgres TO :"rolname";
\o conn.json
SELECT json_build_object(
  'name', :'connname',
  'connection_type', 'POSTGRESQL',
  'options', json_build_object('host', :'host', 'port', '5432', 'user', :'rolname', 'password', :'pw'));
\o
```

```bash
umask 077
databricks psql --project <project-id> --profile <PROFILE> -- \
  -X -At -v ON_ERROR_STOP=1 -v host=<lakebase-endpoint-host> \
  -v base=<base> -v connname="<firstname><lastname>-<base>-lakebase" \
  -f role.sql
```

`\gset` stores a query result in a psql variable, and psql substitutes `:'pw'` in `-f` files (not in `-c` strings). `conn.json` holds the password: never print, `cat`, or open it with a file-read tool, and delete it in Step 4. If you can't delete it, leave it untouched and tell the user to delete it. If the role already exists, `role.sql` stops with an error rather than resetting a password another demo may use. To rotate this demo's own password on purpose, add `-v rotate=yes` (any other value, or none, refuses), then update the connection with the new `conn.json`. `umask 077` keeps `conn.json` readable only by you.

Schema and table grants come at the end of Step 3, once the schema exists. If password logins are rejected, check whether native Postgres password auth is enabled for the project (see the Lakebase connectivity docs).

---

## Step 3 — seed demo tables with a cursor and a soft-delete flag

Query-based ingestion needs an `updated_at` that is bumped on **every** update, and a flag column for deletes. See [query-based-connectors.md](query-based-connectors.md) for why.

Save the SQL below as `seed.sql` and run it the same way:

```bash
databricks psql --project <project-id> --profile <PROFILE> -- \
  -v ON_ERROR_STOP=1 -v base=<base> -v rolname="<base>_reader" -f seed.sql
```

```sql
CREATE SCHEMA IF NOT EXISTS :"base";

CREATE TABLE :"base".customers (
  customer_id  bigint PRIMARY KEY,
  name         text NOT NULL,
  tier         text NOT NULL,
  is_deleted   boolean NOT NULL DEFAULT false,
  updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE :"base".orders (
  order_id     bigint PRIMARY KEY,
  customer_id  bigint NOT NULL REFERENCES :"base".customers,
  amount       numeric(12,2) NOT NULL,
  status       text NOT NULL,
  is_deleted   boolean NOT NULL DEFAULT false,
  updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION :"base".set_updated_at() RETURNS trigger AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER customers_set_updated_at BEFORE UPDATE ON :"base".customers
  FOR EACH ROW EXECUTE FUNCTION :"base".set_updated_at();
CREATE TRIGGER orders_set_updated_at BEFORE UPDATE ON :"base".orders
  FOR EACH ROW EXECUTE FUNCTION :"base".set_updated_at();

INSERT INTO :"base".customers (customer_id, name, tier)
SELECT g, 'customer_' || g, (ARRAY['bronze','silver','gold'])[1 + g % 3]
FROM generate_series(1, 1000) g;

INSERT INTO :"base".orders (order_id, customer_id, amount, status)
SELECT g, 1 + (g % 1000), round((random() * 500)::numeric, 2), 'placed'
FROM generate_series(1, 10000) g;

-- Read-only access for the connector role from Step 2
GRANT USAGE ON SCHEMA :"base" TO :"rolname";
GRANT SELECT ON ALL TABLES IN SCHEMA :"base" TO :"rolname";
ALTER DEFAULT PRIVILEGES IN SCHEMA :"base" GRANT SELECT ON TABLES TO :"rolname";
```

Grant `SELECT` only — the connector never writes to the source. For more realistic data, use **databricks-synthetic-data-gen** and write to these tables.

---

## Step 4 — UC connection to Lakebase

Create the connection from the `conn.json` that Step 2 wrote, then delete the file whether the create succeeds or not. `conn.json` lands in the directory you ran psql from: delete it there with a plain `rm -f conn.json` (or `rm -f <dir>/conn.json`), and if you can't, tell the user its exact path.

```bash
databricks connections create --json @conn.json --profile <PROFILE>
rm -f conn.json
```

If the connection name is already taken, don't delete or reuse that connection: it may be a coworker's. Pick a new connection name (for example append `-2`) and re-run `role.sql` with it and `-v rotate=yes` (this demo's own role, created a moment ago), then create again. From here on, `<connection-name>` is the name you created, suffix included: use it in the bundle and in the teardown, never the name that was taken.

Lakebase requires TLS. Test the connection with a quick Federation query or `databricks connections get <connection-name>` before building the pipeline.

**Networking:** serverless must reach the Lakebase endpoint. If the workspace has serverless egress control (network policies), Lakebase IP access lists, or Private Link set up for Lakebase, allow the serverless path first.

**Use the foreign-connection form, not `ingest_from_uc_foreign_catalog`.** A Lakebase database catalog registered in UC is not a Lakehouse Federation foreign catalog.

---

## Step 5 — bundle: pipeline + schedule

Create the target schema (the pipeline doesn't create it):

```bash
databricks schemas create <target_schema> <catalog> --profile <PROFILE>
```

Write `databricks.yml` in a new, empty directory. Leave out the `jobs` block if the user didn't ask for a schedule.

```yaml
bundle:
  name: <name>               # <firstname><lastname>_<connection>_<epoch>: the same name as the pipeline, schema, and job

sync:
  exclude:
    - conn.json      # holds the connector password; never upload it, even if rm failed
    - scratch/**     # role.sql, seed.sql, mutate.sql, report queries

variables:
  catalog:
    description: Target UC catalog (a regular managed catalog)

targets:
  dev:
    default: true            # no mode: development, which would prefix the pipeline name

resources:
  pipelines:
    lakebase_qbc:
      name: <name>           # default naming, see prerequisite-checks.md#default-names
      catalog: ${var.catalog}
      schema: <target_schema>
      ingestion_definition:
        connection_name: <connection-name>   # the name Step 4 created
        objects:
          - table:
              source_catalog: databricks_postgres
              source_schema: <base>
              source_table: customers
              destination_catalog: ${var.catalog}
              destination_schema: <target_schema>
              table_configuration:
                scd_type: SCD_TYPE_2
                primary_keys: [customer_id]
                query_based_connector_config:
                  cursor_columns: [updated_at]
                  deletion_condition: "is_deleted = true"
          - table:
              source_catalog: databricks_postgres
              source_schema: <base>
              source_table: orders
              destination_catalog: ${var.catalog}
              destination_schema: <target_schema>
              table_configuration:
                scd_type: SCD_TYPE_1
                primary_keys: [order_id]
                query_based_connector_config:
                  cursor_columns: [updated_at]
                  deletion_condition: "is_deleted = true"

  jobs:
    lakebase_qbc_schedule:
      name: <name>           # the pipeline's name: jobs and pipelines are named separately
      trigger:
        periodic:
          interval: 15
          unit: MINUTES
      tasks:
        - task_key: ingest
          pipeline_task:
            pipeline_id: ${resources.pipelines.lakebase_qbc.id}
```

The destination catalog must be a regular UC catalog, not the Lakebase database catalog.

---

## Step 6 — run the demo

```bash
databricks bundle validate -t dev --var catalog=<catalog> --profile <PROFILE>
databricks bundle deploy   -t dev --var catalog=<catalog> --profile <PROFILE>
databricks bundle run lakebase_qbc -t dev --var catalog=<catalog> --profile <PROFILE>   # run 1: initial snapshot
```

`bundle run` waits for the update and exits non-zero if it fails. Check the row counts before calling it done (the SQL at the end of this step, or `databricks tables list <catalog> <target_schema>`).

Make changes in Lakebase (save as `mutate.sql` and run it with `databricks psql ... -- -v ON_ERROR_STOP=1 -v base=<base> -f mutate.sql`):

```sql
INSERT INTO :"base".customers (customer_id, name, tier) VALUES (1001, 'new_customer', 'gold');
UPDATE :"base".customers SET tier = 'gold' WHERE customer_id = 1;          -- SCD2 history
UPDATE :"base".orders    SET status = 'shipped' WHERE order_id <= 100;     -- SCD1 overwrite
UPDATE :"base".orders    SET is_deleted = true WHERE order_id = 200;       -- soft delete
```

```bash
databricks bundle run lakebase_qbc -t dev --var catalog=<catalog> --profile <PROFILE>   # run 2: incremental pull
```

Show the result:

```sql
-- SCD2: customer 1 now has a closed version and an open version
SELECT customer_id, tier, __START_AT, __END_AT
FROM <catalog>.<target_schema>.customers
WHERE customer_id = 1
ORDER BY __START_AT;

-- SCD1: shipped orders overwritten in place, deleted order gone
SELECT status, count(*) FROM <catalog>.<target_schema>.orders GROUP BY status;
SELECT * FROM <catalog>.<target_schema>.orders WHERE order_id = 200;   -- 0 rows
```

Then show what each run did, per table: run [`scripts/rows_per_run.sql`](../scripts/rows_per_run.sql) as in [rows-per-run.md](rows-per-run.md#rows-per-table-per-run-snapshot-inserts-upserts-deletes). Get the pipeline ID from `databricks bundle summary -t dev --var catalog=<catalog> -o json --profile <PROFILE>` (`resources.pipelines.lakebase_qbc.id`). For this demo, run 1 should show `snapshot` with 1,000 customers and 10,000 orders under `snapshot_inserted`. Run 2 should be `incremental`: `orders` shows about 100 upserts and 1 delete, matching the mutations above. It can be a few more than 100, because a timestamp cursor re-reads a short lookback window before the high-water mark; that's expected, not a pipeline problem. Put the counts next to the mutations so the audience can see each change arrive. If an SCD2 count (`customers`) is higher than the number of changes, say it's the connector's merge count, not the number of source changes.

**Talking points:** no CDC setup on the source; one serverless pipeline; the cursor and lookback window; QBC captures latest state per run, not every intermediate change — that's what the CDC connector adds.

---

## Step 7 — teardown

```bash
databricks bundle destroy -t dev --var catalog=<catalog> --profile <PROFILE>
databricks schemas delete <catalog>.<target_schema> --force --profile <PROFILE>   # created in Step 5, not by the bundle
databricks connections delete "<connection-name>" --profile <PROFILE>   # the name Step 4 created
rm -f conn.json
```

Save as `teardown.sql` and run it the same way:

```bash
databricks psql --project <project-id> --profile <PROFILE> -- \
  -v ON_ERROR_STOP=1 -v base=<base> -v rolname="<base>_reader" -f teardown.sql
```

```sql
DROP SCHEMA :"base" CASCADE;
REVOKE CONNECT ON DATABASE databricks_postgres FROM :"rolname";
DROP ROLE :"rolname";
```

Keep the `lfc-demo` project by default, since other demos reuse it; drop only this demo's schema and role. Delete the project only if the user asks. `databricks postgres delete-project` is a soft delete: the project name stays reserved for about 7 days, so the next demo needs a different name.

---

## Postgres CDC on Lakebase: try it, then fall back

Whether a Lakebase instance can run Postgres CDC, and what to clean up if the attempt fails.

Lakebase support for publications and replication slots is being rolled out, and there's no fixed date. Don't assume either way: when the user asks for Postgres CDC on Lakebase, **try creating this demo's publication**. If that fails, CDC can't run on this Lakebase yet. Tell the user and use query-based.

You're connected as the project owner through `databricks psql`, so you can run DDL directly here (unlike an external source behind a UC connection). Save the probe as `probe.sql` and run it as one `-f` file:

```bash
databricks psql --project <project-id> --profile <PROFILE> -- \
  -X -v ON_ERROR_STOP=1 -v base=<base> -v slot_name="<slot_name>" -f probe.sql
```

```sql
SHOW wal_level;
SELECT rolreplication FROM pg_roles WHERE rolname = current_user;
SELECT :'slot_name' || '_pub' AS pubname \gset
CREATE PUBLICATION :"pubname" FOR TABLE :"base".customers, :"base".orders;
```

How to read the result:

- **The `CREATE PUBLICATION` fails** with "[Databricks Auth] CREATE PUBLICATION is not enabled on this Lakebase instance": CDC isn't available on this Lakebase yet, even though `wal_level` is `logical` and the role has `REPLICATION` (both are true on current Lakebase). Use query-based. Nothing was created.
- **It succeeds:** keep the publication and continue with the Postgres CDC section in [postgresql-cdc.md](postgresql-cdc.md). `<slot_name>` is the pipeline pair's `<base>_<epoch>` (the pipelines are `<base>_gw_<epoch>` / `<base>_ap_<epoch>`; see [Default names](prerequisite-checks.md#default-names)). The publication is `<slot_name>_pub`. Set `REPLICA IDENTITY FULL` on both tables (`ALTER TABLE <base>.customers REPLICA IDENTITY FULL;`, the same for `<base>.orders`). Then deploy the pipelines, stop the gateway (it starts on deploy), create the slot with `SELECT pg_create_logical_replication_slot('<slot_name>', 'pgoutput');`, and start the gateway, the same order as in [postgresql-cdc.md](postgresql-cdc.md). If the slot or the pipelines fail, clean up in this order: `bundle destroy` (or delete this attempt's pipelines) so the gateway stops reading, then drop this attempt's slot and publication by name (`SELECT pg_drop_replication_slot('<slot_name>');`, `DROP PUBLICATION <slot_name>_pub;`), check that both are gone, report anything left, and fall back to query-based.

Lakebase-specific checks once CDC works:

- **Leave Lakebase's own slots alone** (for example `wal_proposer_slot` and `wal_retention_slot`). Only ever drop this demo's slot, by name.
- **The replication role needs a native password,** for the same reason as [Step 2](#step-2--create-a-password-role-for-the-connector).
- **The gateway must be able to reach the Lakebase endpoint** from the compute it runs on ([compute](compute-and-feature-gates.md#compute-serverless-or-classic)).
- **A stopped gateway lets WAL pile up on the slot.** Drop the slot and the publication at teardown.
- **Reuse the Step 3 tables** ([Step 3](#step-3--seed-demo-tables-with-a-cursor-and-a-soft-delete-flag)). Their `text` columns are TOAST-able, so they need `REPLICA IDENTITY FULL`: under `DEFAULT` the gateway fails with "needs FULL REPLICA IDENTITY" ([PostgreSQL behaviors](postgresql-cdc.md#source-behaviors-and-errors)). The `updated_at` / `is_deleted` columns are harmless under CDC. The demo moves from query-based to CDC by changing only the connection and pipelines.
