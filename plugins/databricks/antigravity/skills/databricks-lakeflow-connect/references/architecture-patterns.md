# Architecture patterns

What Lakeflow Connect gives you, and how data moves in each pattern. Use it to explain a choice to the user. Which compute each piece runs on changes between releases: read it back as in [compute-and-feature-gates.md](compute-and-feature-gates.md#compute-serverless-or-classic).

Three frames to keep in mind:

- **Simple and low-maintenance** — no client code to write, no message bus to operate; connector + UC Connection + a serverless pipeline (plus a gateway for standard CDC).
- **Unified with the lakehouse** — credentials stored in UC, output is governed Delta, runs on Jobs and SDP like any other workload.
- **Efficient incremental processing** — change tracking / CDC / schema evolution / retries are built in.

## SaaS pull

```
          ┌───────────────────────┐
          │  SaaS application     │
          │  (Salesforce, etc.)   │
          └──────────┬────────────┘
                     │  API, through the UC connection (OAuth / API key)
                     ▼
          ┌───────────────────────┐
          │  Ingestion pipeline   │   serverless, triggered
          │  (one pipeline)       │
          └──────────┬────────────┘
                     ▼
          ┌───────────────────────┐
          │  Delta tables in UC   │
          └───────────────────────┘
```

Setup: [saas-connectors.md](saas-connectors.md).

## Integrated database CDC

```
          ┌───────────────────────┐
          │  Customer database    │
          │  (SQL Server, MySQL,  │
          │   Oracle)             │
          └──────────┬────────────┘
                     │  change log, read inside each update
                     ▼
          ┌───────────────────────┐
          │  Ingestion pipeline   │   extracts and applies;
          │  (one pipeline)       │   triggered or continuous
          └──────┬────────────┬───┘
                 │            │
                 ▼            ▼
   ┌──────────────────┐  ┌───────────────────────┐
   │  Staging volume  │  │  Delta tables in UC   │
   │  (staging schema)│  └───────────────────────┘
   └──────────────────┘
```

No gateway: an update ends when smart closure decides. Setup: [integrated-cdc.md](integrated-cdc.md); updates: [integrated CDC updates](integrated-cdc.md#how-an-update-runs-smart-closure).

## Standard database CDC (gateway)

Two pipelines, an always-on gateway and a triggered ingestion pipeline, with a UC Volume between them. The diagram and setup: [standard-cdc-gateway.md](standard-cdc-gateway.md).

## Query-based

```
          ┌───────────────────────┐
          │  Customer database    │
          │  (no CDC needed)      │
          └──────────┬────────────┘
                     │  SELECT ... WHERE <cursor> > <high-water mark>,
                     │  through the UC connection
                     ▼
          ┌───────────────────────┐
          │  Ingestion pipeline   │   serverless, triggered;
          │  (one pipeline)       │   saves the high-water mark
          └──────────┬────────────┘
                     ▼
          ┌───────────────────────┐
          │  Delta tables in UC   │   latest state per run
          └───────────────────────┘
```

Concepts and cursor rules: [query-based-connectors.md](query-based-connectors.md).
