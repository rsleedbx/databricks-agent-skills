# Anti-patterns

Four forms that look plausible but fail. Each shows the wrong form and the right one.

## 1. Federation syntax

`CREATE TABLE ... FROM CONNECTION` is Lakehouse Federation, not Lakeflow Connect.

```sql
-- WRONG: Federation syntax; no LFC equivalent exists
CREATE TABLE main.salesforce_raw.account FROM CONNECTION my_salesforce_conn;
```
```json
// RIGHT: author an ingestion_definition (see the Minimal Example in [authoring-and-running.md](authoring-and-running.md))
{"ingestion_definition": {"connection_name": "my_salesforce_conn", "objects": [/* ... */]}}
```

## 2. A `libraries` block

An ingestion pipeline carries `ingestion_definition`, never a `libraries` block.

```json
// WRONG: libraries is for a standard SDP pipeline running your notebooks/files
{"name": "salesforce_to_uc", "libraries": [{"notebook": {"path": "/Repos/.../ingest"}}]}
// RIGHT:
{"name": "salesforce_to_uc", "ingestion_definition": {"connection_name": "...", "objects": []}}
```

## 3. `continuous: true` on a triggered connector

`continuous: true` is rejected for query-based and standard-CDC ingestion pipelines — they are triggered-only. (Integrated CDC has its own continuous mode.)

```json
// WRONG: continuous mode fails at create
{"continuous": true, "ingestion_definition": {/* ... */}}
// RIGHT: continuous:false (or omit) + schedule with a Jobs pipeline_task
{"continuous": false, "ingestion_definition": {/* ... */}}
```

## 4. A cursor that isn't bumped on update

A query-based cursor that isn't bumped on update silently misses changes.

```yaml
# WRONG: an integer PK, or updated_at with only DEFAULT now(), never changes on UPDATE
query_based_connector_config: {cursor_columns: [order_id]}
# RIGHT: a NOT NULL timestamp bumped on every insert and update (trigger or app-set)
query_based_connector_config: {cursor_columns: [updated_at]}
```
