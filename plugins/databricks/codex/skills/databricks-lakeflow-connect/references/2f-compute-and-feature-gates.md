# Compute and workspace feature gates

Database pipelines and gateways run on serverless or classic compute, and each workspace enables database sources separately. Read back what ran, and report refused sources rather than working around them.

## Workspace feature gates (all database sources)

Each standard CDC source and integrated CDC source is enabled per workspace. When it isn't, the pipeline (or gateway) create fails with `Ingestion source '<SOURCE>' is not supported in your workspace. Please contact Databricks support to enable this feature for your workspace. (403 PERMISSION_DENIED)`. This has been seen for `POSTGRESQL` and `MYSQL` (standard / gateway) on a workspace where query-based and integrated CDC for the same sources worked, and for `ORACLE` and `POSTGRESQL` (integrated CDC) on a workspace where query-based ingestion from both worked. Report it, don't work around it, and offer the modes that weren't refused: for a refused gateway (standard CDC), integrated CDC or query-based; for a refused integrated CDC source, standard CDC where the source has it, else query-based (Oracle has no standard mode, so query-based). A failed `bundle deploy` can still have created the schemas declared before the pipelines: offer `bundle destroy` so nothing is left behind.

---

## Compute: serverless or classic

A workspace's compute policy wins over the docs: a workspace that requires serverless rejects classic compute with "You must use serverless compute in this workspace" (see the first failure below).

Gateway compute errors (`spark_conf` keys, `num_workers: 0`, the classic egress IP, "Finding instances", instance pools) and their fixes: [8-database-source-behaviors.md](8-database-source-behaviors.md#gateway-compute).

**Leave `serverless` unset on every pipeline and gateway, read back what the system chose, and change it if that fails.** The UI uses the default too; when the default fails, you change the setting yourself. Defaults change over time and differ by workspace, mode, and source, so don't set `serverless` in the bundle or the REST body unless the workspace or the docs page requires it. After the first update starts, read the choice. The update ID is the `update_id` that `start-update` returns, or `.latest_updates[0].update_id` from `databricks pipelines get <pipeline-id>`:

```bash
databricks pipelines get-update <pipeline-id> <update-id> --profile <PROFILE> -o json \
  | yq -p json -o tsv '[(.update.config.serverless | tostring), (.update.cluster_id // "none")]'
# true  none        serverless
# false <cluster>   classic, on its own cluster
# null  ...         not decided yet: read it again
```

`pipelines get` doesn't tell: its `spec.serverless` stays unset, echoing what you sent. **If the create fails on compute, or the update can't reach the source, find out which first (see the two failures below), then set the opposite explicitly and retry.** A wrong host or port, or a source down, gives the same connection error on either compute: flipping won't fix it. For a query-based pipeline the opposite of serverless is classic compute, which is Beta and needs a `clusters` block as well as `serverless: false` ([3-query-based-connectors.md](3-query-based-connectors.md)); change both in one step and say so. Results by workspace type:

| Workspace | `serverless` unset | Opposite |
|---|---|---|
| Classic compute available (AWS, Azure) | Query-based: `config.serverless=true`. Gateway: `false` (classic, its own cluster). Integrated CDC, MySQL and SQL Server (Azure): `false`, classic, its own cluster | Integrated CDC and gateway with `serverless: true`: ran serverless |
| Serverless-only (AWS) | Query-based: `true`. Gateway: refused at create with `You must use serverless compute in this workspace.` | Gateway with `serverless: true`: created and started (`config.serverless=true`) |
| Any | Standard CDC ingestion pipeline (the applier): not verified unset; with `serverless: true` it ran serverless on AWS and Azure | |

Tested 2026-10 on two AWS workspaces and an Azure workspace with classic compute.

**Two different failures, at different times.** Tell them apart before changing anything:

- **The compute isn't allowed.** Seen at create, in seconds: `You must use serverless compute in this workspace.` (a gateway left unset on a serverless-only workspace). A classic cluster you aren't allowed to create (no `allow-cluster-create`, no usable cluster policy) should fail here too, before anything connects. Not verified. Set the opposite and retry.
- **The compute starts but can't reach the source.** Seen as `Extractor STARTED`, then about a minute later `Extractor FAILED` with `INGESTION_GATEWAY_CONNECTION_ERROR.INCORRECT_SOURCE_HOSTNAME_OR_PORT_SPECIFIED_FAILURE ... Could not connect to the source database`, with the IP's last octet masked as `0`. On an Azure workspace, integrated CDC for MySQL and SQL Server on classic failed this way. With the host and port right, the source's firewall allowed the serverless ranges but not the workspace's classic egress IP, and `remote_query` on the same connection, which runs on serverless, still worked. The same error also comes from a wrong host or port, so check before flipping: run `remote_query` on the same connection ([7-source-sql-through-connection.md](7-source-sql-through-connection.md)), which runs on serverless. If it reaches the source and `update.config.serverless` is `false`, the host is right and the classic egress IP is what's blocked: set `serverless: true` and retry, or have the classic egress IP added to the source's firewall and rerun as is. If `remote_query` fails the same way, fix the host, port, or network first.

**Classic takes longer to start than serverless, so expect the second failure later on classic.** In the event log, an update waits in `WAITING_FOR_RESOURCES` until `INITIALIZING`, and classic waits for its cluster far longer than serverless does. `update.config.serverless` is set as soon as the update starts, so read it right away to know which wait to expect.

To know beforehand whether a workspace has classic compute, run `databricks clusters list-zones --profile <PROFILE>`. A list of zones means classic is there; on Azure, `Azure worker environment does not support zones` also means classic is there. `No such workerEnvironment 'serverless-<workspace id>'` (AWS) or `does not have any associated worker environment` (Azure) means a serverless-only workspace. Any other answer (seen on GCP) doesn't tell, so let the create decide. Classic also needs the `allow-cluster-create` entitlement or a cluster policy you can use.
