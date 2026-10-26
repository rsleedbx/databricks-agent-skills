# Source SQL notebook

A one-time notebook that lists tables, publications, and slots on a source, and creates or drops a Postgres replication slot, through a UC connection. Use it when listing everything at once or for slot work. The single-query path and the introspection queries are in [7-source-sql-through-connection.md](7-source-sql-through-connection.md).

## The notebook

Save as `source_sql.py` (the first line marks it as a Databricks notebook). It takes the connection type as a parameter, so look it up first:

```bash
databricks connections get <connection> --profile <PROFILE> -o json | yq -r .connection_type   # POSTGRESQL | MYSQL | SQLSERVER
```

```python
# Databricks notebook source
# Source-side SQL through a Unity Catalog connection (remote_query), for Lakeflow Connect setup and cleanup.
# Widgets:
#   connection  UC connection name
#   conn_type   connection type from `databricks connections get <name>`: POSTGRESQL | MYSQL | SQLSERVER
#   database    source database (blank for MySQL)
#   action      list | create_slot | drop_slot   (slot actions: PostgreSQL only)
#   slot_name   exact replication slot name for create_slot / drop_slot
import json

dbutils.widgets.text("connection", "")
dbutils.widgets.text("conn_type", "")
dbutils.widgets.text("database", "")
dbutils.widgets.dropdown("action", "list", ["list", "create_slot", "drop_slot"])
dbutils.widgets.text("slot_name", "")
conn = dbutils.widgets.get("connection")
database = dbutils.widgets.get("database")
action = dbutils.widgets.get("action")
slot_name = dbutils.widgets.get("slot_name").strip()
conn_type = dbutils.widgets.get("conn_type").upper()

import re
# These go into SQL literals: allow only plain identifier characters
for name, value in (("connection", conn), ("database", database)):
    if value and not re.fullmatch(r"[A-Za-z0-9_.\-]+", value):
        raise ValueError(f"{name} has unexpected characters: {value!r}")


def rq(query):
    """Run one query on the source through the UC connection; returns a list of dicts."""
    db_arg = f", database => '{database}'" if database else ""
    q = query.replace("\\", "\\\\").replace('"', '\\"')
    rows = spark.sql(f"SELECT * FROM remote_query('{conn}'{db_arg}, query => \"{q}\")").collect()
    return [{k: (str(v) if v is not None else None) for k, v in r.asDict().items()} for r in rows]


def pg_slots():
    return rq("SELECT slot_name, plugin, database, active, temporary FROM pg_replication_slots")  # no inactive_since: PG 17+ only


out = {"connection": conn, "type": conn_type, "database": database or None, "action": action}

if action in ("create_slot", "drop_slot"):
    if conn_type != "POSTGRESQL":
        raise ValueError(f"{action} is PostgreSQL only, connection type is {conn_type}")
    if not database:
        raise ValueError(f"{action} requires database to be set")
    if not re.fullmatch(r"[a-z0-9_]+", slot_name):
        raise ValueError("slot_name must be lowercase ASCII letters, digits, and underscores")
    slots = pg_slots()   # list once; each remote_query is a separate source session
    exists = any(s["slot_name"] == slot_name for s in slots)
    if action == "create_slot":
        out["result"] = "already exists" if exists else rq(
            f"SELECT slot_name, lsn FROM pg_create_logical_replication_slot('{slot_name}', 'pgoutput')")
    else:
        # Drop only the slot named here, and only if nothing is reading from it.
        active = [s for s in slots if s["slot_name"] == slot_name and str(s["active"]).lower() == "true"]
        if active:
            raise ValueError(f"slot {slot_name} is active; stop the gateway first")
        out["result"] = "not found" if not exists else rq(
            f"SELECT '{slot_name}' AS dropped FROM (SELECT pg_drop_replication_slot('{slot_name}')) d")
    out["slots"] = pg_slots()
else:
    if conn_type == "MYSQL":
        out["tables"] = rq("""
            SELECT table_schema, table_name FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('mysql', 'sys', 'information_schema', 'performance_schema')""")
    elif conn_type == "SQLSERVER":
        out["tables"] = rq("SELECT s.name AS table_schema, t.name AS table_name FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.is_ms_shipped = 0")
    elif conn_type == "POSTGRESQL":
        out["tables"] = rq("""
            SELECT table_schema, table_name FROM information_schema.tables
            WHERE table_type = 'BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema')""")
        out["publications"] = rq("""
            SELECT p.pubname, pg_get_userbyid(p.pubowner) AS owner,
                   (SELECT string_agg(schemaname || '.' || tablename, ', ') FROM pg_publication_tables t WHERE t.pubname = p.pubname) AS tables
            FROM pg_publication p""")
        out["slots"] = pg_slots()
    else:
        raise ValueError(f"unsupported connection type {conn_type}")
    out["table_count"] = len(out["tables"])

dbutils.notebook.exit(json.dumps(out))
```

## Running it

Upload once, then submit one-time runs. Several tasks in one submit run in parallel, or in order with `depends_on`.

```bash
NB=/Workspace/Users/<user>/.lfc_skill/source_sql
databricks workspace mkdirs /Workspace/Users/<user>/.lfc_skill --profile <PROFILE>
databricks workspace import $NB --file source_sql.py --format SOURCE --language PYTHON --overwrite --profile <PROFILE>
databricks jobs submit --json @submit.json --profile <PROFILE> -o json > run.json
```

`submit.json`, listing a Postgres source (use `"action": "create_slot"` or `"drop_slot"` with `"slot_name"` for slot work):

```json
{
  "run_name": "lfc-source-sql",
  "tasks": [
    {"task_key": "list", "notebook_task": {
      "notebook_path": "/Workspace/Users/<user>/.lfc_skill/source_sql",
      "base_parameters": {"connection": "<connection>", "conn_type": "POSTGRESQL", "database": "<database>", "action": "list"}}}
  ]
}
```

No cluster spec means it runs on serverless. `jobs submit` waits for the run (about a minute including startup). Read each task's result from its **task** run ID, not the parent run ID:

```bash
RID=$(yq -r .run_id run.json)
databricks jobs get-run $RID --profile <PROFILE> -o json | yq -r '.tasks[] | "\(.task_key)\t\(.run_id)\t\(.state.result_state)"'
databricks jobs get-run-output <task-run-id> --profile <PROFILE> -o json | yq -r '.notebook_output.result // .error'
```

If a task fails, `jobs submit` exits non-zero and `run.json` can be empty: find the run with `databricks jobs list-runs --limit 5 -o json` by `run_name`.
