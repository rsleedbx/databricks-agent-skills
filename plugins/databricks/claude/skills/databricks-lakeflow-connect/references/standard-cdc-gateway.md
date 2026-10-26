# Standard CDC: the gateway pattern

Standard CDC runs two pipelines: an always-on ingestion gateway and an ingestion pipeline that applies its staged changes. Use it when the user asks for standard, two-pipeline, or gateway CDC, or when the source has no integrated CDC. Integrated CDC: [integrated-cdc.md](integrated-cdc.md).

## The gateway pattern

Gateway compute errors (`spark_conf`, `num_workers`, classic egress, instance pools): [database-source-behaviors.md](database-source-behaviors.md#gateway-compute).

Standard CDC splits into two pipelines (the compute options for each change between releases; check the connector's docs page):

```
          ┌───────────────────────┐
          │  Customer database    │
          │  (SQL Server, etc.)   │
          └──────────┬────────────┘
                     │  CDC / change tracking
                     ▼
          ┌───────────────────────┐
          │  Ingestion gateway    │   classic compute,
          │  (one pipeline)       │   runs in customer VPC
          └──────────┬────────────┘
                     │  change events
                     ▼
          ┌───────────────────────┐
          │  UC Volume staging    │   30-day retention by default
          └──────────┬────────────┘
                     │
                     ▼
          ┌───────────────────────┐
          │  Ingestion pipeline   │   serverless,
          │  (one pipeline)       │   applies CDC into Delta
          └──────────┬────────────┘
                     ▼
          ┌───────────────────────┐
          │  Delta tables in UC   │
          └───────────────────────┘
```

Why each piece:

- **Gateway** runs in the customer's network so the source database is never exposed to Databricks-managed compute. It reads the CDC / change-tracking stream and writes change events into a UC Volume.
- **Staging Volume** decouples the two pipelines: the gateway can run on its own cadence, and the ingestion pipeline can re-process from the Volume without re-reading the source.
- **Ingestion pipeline** is the serverless half — it applies the staged events to Delta with CDC semantics and handles schema evolution.

Trade-offs:

- Two pipelines, two pieces of state. Both must be healthy.
- Gateway compute is billed separately from the ingestion DBUs. Leave `serverless` unset and read back what it ran on ([Compute: serverless or classic](compute-and-feature-gates.md#compute-serverless-or-classic)). See the [pricing page](https://www.databricks.com/product/pricing/lakeflow-connect) for current rates.
- Staging Volume retention is 30 days. Reprocessing further back requires a snapshot reload.

---

## Standard CDC on a serverless-only workspace

A classic gateway is rejected with "You must use serverless compute in this workspace". With `"serverless": true` on the gateway, SQL Server standard CDC works: the gateway runs continuously (it starts as soon as it's created), the applier takes the snapshot on its first run, and later runs apply inserts, updates, and deletes. The applier is triggered: run it again after source changes, or schedule it. Stop the gateway when you're done (`databricks pipelines stop <gateway-id>`); it keeps billing otherwise. On the same workspace, MySQL and PostgreSQL gateways were refused as not enabled ([feature gates](compute-and-feature-gates.md#workspace-feature-gates-all-database-sources)), while integrated MySQL CDC worked. Oracle has no standard (gateway) mode in the docs, only integrated.

---

## Database-specific gotchas

| Symptom | Likely cause | Fix |
|---|---|---|
| Gateway requires an instance type unavailable in the region | Default gateway cluster shape not stocked in the target region | Apply a cluster policy override on the gateway pipeline to pin a regionally-available instance type. |
| Snapshot-only mode silently disabled | Snapshot-only is not supported for CDC sources | Use CT instead, or accept incremental mode. |
| Pipeline state diverges from source after 30+ days | Staging Volume retention expired | Resnapshot the affected tables. Increase the Volume retention if reprocessing further back is a recurring need. |
| "Continuous mode not supported" error at create | Query-based and standard-CDC ingestion pipelines are triggered-only (integrated CDC has a continuous mode) | Use `continuous: false` plus a Jobs schedule. |
| Gateway pipeline succeeds but ingestion pipeline shows no new data | Staging path mismatch between the two pipelines | Confirm `gateway_storage_*` on the gateway matches the staging path the ingestion pipeline reads from. |
