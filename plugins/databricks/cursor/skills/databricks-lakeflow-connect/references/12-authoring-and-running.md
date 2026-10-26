# Authoring and running a pipeline

The minimal `ingestion_definition` for `databricks pipelines create --json`, the DAB-authored form, and the commands to run and poll an update. The SaaS and database forms are in the files named in [SKILL.md](../SKILL.md#reference-index).

## Minimal Example — Salesforce ingestion pipeline

The canonical authoring path is JSON to `databricks pipelines create --json`. (There is no SQL `CREATE TABLE … FROM CONNECTION` syntax for Lakeflow Connect — that syntax exists only for Lakehouse Federation, which is a different product.)

```bash
databricks pipelines create --json '{
  "name": "salesforce_to_uc",
  "ingestion_definition": {
    "connection_name": "my_salesforce_oauth_connection",
    "objects": [
      {"table": {"source_schema": "salesforce", "source_table": "Account",
                 "destination_catalog": "main", "destination_schema": "salesforce_raw"}},
      {"table": {"source_schema": "salesforce", "source_table": "Opportunity",
                 "destination_catalog": "main", "destination_schema": "salesforce_raw"}}
    ]
  }
}'
```

For a DAB-authored version (the production path), see [1-saas-connectors.md](1-saas-connectors.md).

---

## Running the pipeline

With a bundle, deploy and run by resource key:

```bash
databricks bundle deploy -t dev
databricks bundle run salesforce_ingestion           # KEY = the pipeline resource key in the bundle; waits by default
databricks bundle run salesforce_ingestion --no-wait
```

A pipeline created with `pipelines create --json` has no run-by-name CLI. Start and poll an update by pipeline ID:

```bash
databricks pipelines start-update <pipeline-id>             # returns an update_id
databricks pipelines get-update  <pipeline-id> <update-id>  # poll one update's status
databricks pipelines list-updates <pipeline-id>             # recent updates and their states
```

That asymmetry is one more reason to author with a Declarative Automation Bundle.

---
