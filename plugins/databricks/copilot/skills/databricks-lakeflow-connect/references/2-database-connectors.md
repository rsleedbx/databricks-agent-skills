# Database connectors

SQL Server (cloud and on-prem) is the GA database connector, as standard or integrated CDC. Postgres CDC is Public Preview. MySQL and Oracle CDC are integrated CDC, Beta. Release stages change often: check the [connector reference](https://docs.databricks.com/ingestion/lakeflow-connect/) for current status.

**Query-based connectors don't use a gateway** — they're a single serverless pipeline. See [3-query-based-connectors.md](3-query-based-connectors.md).

Read the file for the topic you need. Each file is a separate read.

| File | When to read |
|------|--------------|
| [2a-integrated-cdc.md](2a-integrated-cdc.md) | Choosing integrated or standard CDC, source prerequisites (CDC, change tracking, binlog), the integrated pipeline YAML, and how source tables map to destination tables |
| [2b-integrated-cdc-updates.md](2b-integrated-cdc-updates.md) | How long an integrated update runs (smart closure), updates the pipeline starts itself, and behaviors that hold across releases |
| [2c-standard-cdc-gateway.md](2c-standard-cdc-gateway.md) | Standard (gateway) CDC: the two-pipeline pattern, serverless-only gateways, and database CDC gotchas |
| [2d-sql-server.md](2d-sql-server.md) | SQL Server change tracking vs CDC, `SCD_TYPE_2` capture rules, source table changes, cloud and on-prem setup |
| [2e-postgresql-cdc.md](2e-postgresql-cdc.md) | PostgreSQL CDC: source setup, slot and publication, pipelines, Postgres gotchas; other preview database connectors |
| [2f-compute-and-feature-gates.md](2f-compute-and-feature-gates.md) | Serverless or classic compute, reading back which one ran, and workspace feature gates |
