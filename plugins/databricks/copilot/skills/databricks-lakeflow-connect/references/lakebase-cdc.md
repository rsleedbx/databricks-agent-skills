# Lakebase Postgres CDC: try it, then fall back

Whether a Lakebase instance can run Postgres CDC, and what to clean up if the attempt fails. Query-based demo: [lakebase-as-postgres-source.md](lakebase-as-postgres-source.md).

Lakebase support for publications and replication slots is being rolled out, and there's no fixed date. Don't assume either way: when the user asks for Postgres CDC on Lakebase, **try creating this demo's publication**. If that fails, CDC can't run on this Lakebase yet. Tell the user and use query-based.

You're connected as the project owner through `databricks psql`, so you can run DDL directly here (unlike an external source behind a UC connection). Run the probe as one `-f` file:

Save as `probe.sql` and run it the same way:

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
- **It succeeds:** keep the publication and continue with the Postgres CDC section in [postgresql-cdc.md](postgresql-cdc.md). `<slot_name>` is the pipeline pair's `<base>_<epoch>` (the pipelines are `<base>_gw_<epoch>` / `<base>_ap_<epoch>`; see [Default names](default-names.md)). The publication is `<slot_name>_pub`. Then deploy the pipelines, stop the gateway (it starts on deploy), create the slot with `SELECT pg_create_logical_replication_slot('<slot_name>', 'pgoutput');`, and start the gateway, the same order as in [postgresql-cdc.md](postgresql-cdc.md). If the slot or the pipelines fail, drop what you created (`DROP PUBLICATION <slot_name>_pub;`, `SELECT pg_drop_replication_slot('<slot_name>');`) and fall back to query-based.

Lakebase-specific checks once CDC works:

- **Leave Lakebase's own slots alone** (for example `wal_proposer_slot` and `wal_retention_slot`). Only ever drop this demo's slot, by name.
- **The replication role needs a native password,** for the same reason as Step 2 of [lakebase-as-postgres-source.md](lakebase-as-postgres-source.md).
- **The gateway must be able to reach the Lakebase endpoint** from the compute it runs on ([compute](compute-and-feature-gates.md#compute-serverless-or-classic)).
- **A stopped gateway lets WAL pile up on the slot.** Drop the slot and the publication at teardown.
- **Reuse the Step 3 tables** of [lakebase-as-postgres-source.md](lakebase-as-postgres-source.md). `REPLICA IDENTITY DEFAULT` is fine since they have primary keys, and the `updated_at` / `is_deleted` columns are harmless under CDC. The demo moves from query-based to CDC by changing only the connection and pipelines.
