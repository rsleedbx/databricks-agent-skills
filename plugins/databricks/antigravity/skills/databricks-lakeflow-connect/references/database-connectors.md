# Database connectors

SQL Server (cloud and on-prem) is the GA database connector, as standard or integrated CDC. The MySQL integrated CDC pipeline, query-based and Foreign Catalog connectors are also GA; Postgres CDC and gateway-based MySQL CDC are Public Preview; Oracle integrated CDC is Beta. Release stages change often: check the [connector reference](https://docs.databricks.com/ingestion/lakeflow-connect/) for current status.

**Query-based connectors don't use a gateway** — they're a single serverless pipeline. See [query-based-connectors.md](query-based-connectors.md).

Read the file for the topic you need. Each file is a separate read.

| File | When to read |
|------|--------------|
| [integrated-cdc.md](integrated-cdc.md) | Choosing integrated or standard CDC, source prerequisites (CDC, change tracking, binlog), the integrated pipeline YAML, and how source tables map to destination tables |
| [integrated-cdc-updates.md](integrated-cdc-updates.md) | How long an integrated update runs (smart closure), updates the pipeline starts itself, and behaviors that hold across releases |
| [standard-cdc-gateway.md](standard-cdc-gateway.md) | Standard (gateway) CDC: the two-pipeline pattern, the gateway's compute, and database CDC gotchas |
| [sql-server-cdc.md](sql-server-cdc.md) | SQL Server change tracking vs CDC, `SCD_TYPE_2` capture rules, source table changes, cloud and on-prem setup |
| [postgresql-cdc.md](postgresql-cdc.md) | PostgreSQL CDC: source setup, slot and publication, pipelines, Postgres gotchas; other preview database connectors |
| [compute-and-feature-gates.md](compute-and-feature-gates.md) | Serverless or classic compute, reading back which one ran, and workspace feature gates |
