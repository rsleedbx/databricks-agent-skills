# Database changes through Zerobus: change log, silver, gold

Use this when a CDC producer pushes a database's row changes to Zerobus and the user wants current tables, history tables, or both. Zerobus lands the changes; Lakeflow Spark Declarative Pipelines (the `databricks-pipelines` skill) turns them into tables. Each pattern here was tested end to end, with silver compared row by row, and version by version, against the source.

## The shape

| Layer | What | Why |
|---|---|---|
| **Snapshot** | The source's tables as of one time T, in Parquet, in a Unity Catalog Volume (one folder per table) | A large first load doesn't go through Zerobus. The change log starts at T, so nothing is missed or loaded twice |
| **Bronze: one change log** | One append-only Delta table for every source table: the table name, the operation, and the key, the row after the change and the producer's position as `VARIANT`. `CLUSTER BY` the table name | A new source table or column needs no DDL. The log is never rewritten, so silver can be rebuilt from the snapshot and the log at any time |
| **Silver** | Per source table, a streaming table maintained by AUTO CDC: current rows (SCD type 1), every version (SCD type 2), or both | AUTO CDC handles out-of-order and duplicate events, deletes, and history |
| **Gold** | Materialized views over silver, only for readers who need them, clustered for those readers | Refreshed incrementally |

### The change-log table

```sql
CREATE TABLE <catalog>.<schema>.cdc_log (
  destination STRING NOT NULL,         -- the source table
  operation STRING NOT NULL,           -- create, update, delete
  idempotency_key STRING,
  key VARIANT, value VARIANT, source_position VARIANT, headers VARIANT)
CLUSTER BY (destination);
```

The service principal the producer writes as needs `USE CATALOG`, `USE SCHEMA`, and `SELECT` and `MODIFY` on the table ([authentication.md](authentication.md)). **Send each `VARIANT` field as a JSON-encoded string.** Zerobus parses the string into `VARIANT`. A nested JSON object in a `VARIANT` field is rejected: "Record decoder/encoder error: invalid type: map, expected a string" (`NonRetriableException`). Read the fields with paths: `value:after:<column>::<type>`, `key:<column>::<type>`.

Event size matters at volume. Drop what silver doesn't read, such as the before-image, transaction metadata and duplicate timestamps. In testing, that cut events from about 700 to about 340 bytes, half of what remained being the producer's position.

## Duplicates: Zerobus keeps them, and a key may not be unique

- **Zerobus doesn't deduplicate.** An `idempotency_key` column is just data. When a producer restarts, it may resend every change since its last saved position, and those land as new rows. In testing, a producer killed and restarted turned 5 changes into 9 rows.
- **Check that the producer's key is unique per change before relying on it.** One tested producer gave two different updates of the same row, made a few milliseconds apart, the same key. Deduplicating by that key alone would drop a real change.
- **SCD type 1 silver absorbs the replays.** They arrive in order, so the last version wins.
- **SCD type 2 silver doesn't.** A replay looks like the row changing back and forth: in testing, it added versions that never happened. Remove the replays first (below).

## Silver: current rows and every version, from the same views

Per source table, two views feed both targets. The snapshot is sequenced by T, and the changes by a value that orders each key's changes. Best is the row's commit time, if the producer sends it. Otherwise use the producer's own event time. That orders each key's changes when one producer process emits them in commit order, but it only compares correctly with T if the two clocks agree.

```sql
CREATE TEMPORARY VIEW orders_snapshot AS
SELECT CAST(id AS BIGINT) AS id, CAST(amount AS DECIMAL(12,2)) AS amount, <T in ns> AS _seq, 'read' AS _op
FROM STREAM read_files('/Volumes/<catalog>/<schema>/snapshot/orders', format => 'parquet');

CREATE TEMPORARY VIEW orders_changes AS
SELECT key:id::BIGINT AS id, value:after:amount::DECIMAL(12,2) AS amount, value:ts_ns::BIGINT AS _seq, operation AS _op
FROM STREAM(cdc_log) WHERE destination = '<prefix>.<schema>.orders';

-- current rows
CREATE OR REFRESH STREAMING TABLE orders CLUSTER BY (id);
CREATE FLOW orders_from_snapshot AS AUTO CDC INTO orders FROM STREAM(orders_snapshot)
  KEYS (id) SEQUENCE BY _seq COLUMNS * EXCEPT (_seq, _op) STORED AS SCD TYPE 1;
CREATE FLOW orders_from_changes AS AUTO CDC INTO orders FROM STREAM(orders_changes)
  KEYS (id) APPLY AS DELETE WHEN _op = 'delete' SEQUENCE BY _seq COLUMNS * EXCEPT (_seq, _op) STORED AS SCD TYPE 1;

-- every version: the same snapshot view, and the changes without replays
CREATE TEMPORARY VIEW orders_changes_dedup AS
SELECT key:id::BIGINT AS id, value:after:amount::DECIMAL(12,2) AS amount, value:ts_ns::BIGINT AS _seq, operation AS _op
FROM STREAM(cdc_log_dedup) WHERE destination = '<prefix>.<schema>.orders';
CREATE OR REFRESH STREAMING TABLE orders_history CLUSTER BY (id);
CREATE FLOW orders_history_from_snapshot AS AUTO CDC INTO orders_history FROM STREAM(orders_snapshot)
  KEYS (id) SEQUENCE BY _seq COLUMNS * EXCEPT (_seq, _op) STORED AS SCD TYPE 2;
CREATE FLOW orders_history_from_changes AS AUTO CDC INTO orders_history FROM STREAM(orders_changes_dedup)
  KEYS (id) APPLY AS DELETE WHEN _op = 'delete' SEQUENCE BY _seq COLUMNS * EXCEPT (_seq, _op) STORED AS SCD TYPE 2;
```

- **Take the key columns from `key`**, not `after`: a delete has no `after`.
- **Snapshot columns are cast to the silver type.** Check decimals in the snapshot files: one export tool wrote a DECIMAL column as text bytes under a DECIMAL annotation, which every reader decoded wrongly. Exporting such columns as strings and casting them in the snapshot view fixed it.
- **Build the column list from the source's catalog**, not by hand. A Unity Catalog connection with `remote_query` reads `information_schema.columns` and the primary key in one query, and the pipeline file is generated from that.

The deduplicated change log, in a Python file in the same pipeline. A replay has the same key and the same row image as the original, so match on both, within a watermark on the event time:

```python
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.temporary_view(name="cdc_log_dedup")
def cdc_log_dedup():
    return (spark.readStream.table("cdc_log")
            .withColumn("_event_time", F.timestamp_micros(F.expr("value:ts_ns::BIGINT DIV 1000")))
            .withColumn("_after", F.expr("TO_JSON(value:after)"))
            .withWatermark("_event_time", "10 minutes")
            .dropDuplicatesWithinWatermark(["destination", "idempotency_key", "operation", "_after"])
            .drop("_event_time", "_after"))
```

Size the watermark to how long a producer restart takes. A real change that repeats the same key and the same row image within the watermark is dropped too; that needs the same values written twice, faster than the key changes.

**Tested:** after inserts, updates, deletes, a delete and reinsert of one key, a reinsert of a deleted key, and a producer killed and restarted, every version in the SCD type 2 tables matched the source's own change history in commit order. The current versions (`__END_AT IS NULL`) and the SCD type 1 tables equaled the source.

## Gold

A materialized view over silver, for example each customer with the count and total of their orders, `CLUSTER BY` what readers filter on. It refreshes **incrementally**: the first update is a complete recompute, and later ones use `GROUP_AGGREGATE`. That works because AUTO CDC targets have row tracking and change data feed on. To see how a refresh ran, query `event_log('<pipeline id>')` for `event_type = 'planning_information'`. The pipeline events API returns those events without their details.

## When the source's schema changes

| Change | Bronze | Silver with the old flow | What brings silver back |
|---|---|---|---|
| Added column | Lands as a new `value:after` field, no DDL. The source may resend every row with the new column | Keeps running and leaves the column out, so nothing signals the change. Compare the fields in recent `value:after` with the schema to catch it | Regenerate the flows from the current schema. **A plain update isn't enough:** the resent rows are already behind the flow's checkpoint, so the column stays null. Full-refresh that table, then run a plain update |
| Dropped column | No DDL | **Fails**: the cast of the missing field stops the flow with `INVALID_VARIANT_CAST` | Same: regenerate, full-refresh that table, plain update |
| Retyped column (wider DECIMAL) | No DDL | Regenerate before values that need the new type arrive | Same |

- **An update with `full_refresh_selection` runs only the selected tables.** Include the table's history table, and run a plain update afterwards so gold and other downstream tables catch up. In testing, gold kept a stale value until that plain update.
- **A full refresh reads the snapshot and the whole change log again**, so keep both for as long as silver may need rebuilding. A column added after T isn't in the snapshot: select it as `CAST(NULL AS <type>)` in the snapshot view.

## Related

- [how-it-works.md](how-it-works.md#delivery-and-duplicates): at-least-once delivery
- [schema-management.md](schema-management.md): how Zerobus matches records to the table
- `databricks-pipelines` skill: AUTO CDC syntax, SCD type 2 queries, materialized views
- `databricks-lakeflow-connect` skill: managed connectors, when Databricks should pull from the database instead
