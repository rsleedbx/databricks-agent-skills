# Workspace readiness: previews, target catalog, and access

Before the first Lakeflow Connect pipeline in a workspace, three things usually need checking: the connector previews are on, there's a catalog the pipelines can write to, and the people who'll run the pipelines can log in and use it. Each is a few API calls. Everything here goes through the Databricks CLI and REST API, so nothing needs to be installed on the source side.

The scripts are in `<SKILL_ROOT>/scripts/` and need `databricks`, [`yq`](https://github.com/mikefarah/yq) v4 (the Go one), and `jq`.

## The one-command check: `ready.sh`

```bash
bash <SKILL_ROOT>/scripts/ready.sh <PROFILE>                                     # you, the workspace's default catalog
bash <SKILL_ROOT>/scripts/ready.sh <PROFILE> --catalog <catalog> --principal "account users"
bash <SKILL_ROOT>/scripts/ready.sh <PROFILE> --no-enable                         # read-only
bash <SKILL_ROOT>/scripts/ready.sh <PROFILE> --egress-file serverless-egress.txt   # keep the egress list; later runs show what changed
```

[`ready.sh`](../scripts/ready.sh) checks, in order: the profile logs in; the metastore's cloud and region (put the sources there, to avoid cross-cloud and inter-region egress); serverless SQL warehouses (for `remote_query`), serverless compute, and classic compute; the connector previews (step 1); a target catalog (step 2); access for you, and for `--principal`, through `check.sh` (step 4); the serverless egress ranges the source's firewall must allow, from [ip-ranges.json](https://www.databricks.com/networking/v1/ip-ranges.json); and the database connections already in the workspace.

It prints one line per check: `PASS`, `FAIL`, `WARN`, `INFO`, or `ASK`, with the next command for anything that needs fixing. **`ASK` means stop and ask the user**: the target catalog isn't writable, or there isn't one, and it lists the catalogs you can write to, each marked "CDC ok" or "Default Storage: query-based only". It exits 1 if anything is `FAIL` or `ASK`, and ends with a verdict per mode (query-based, CDC).

**The one change it makes:** as a workspace admin, it turns on a connector preview that's off, and prints a line saying so first. `--no-enable` makes it read-only. Turn a preview off again with the same patch and `"value": false`.

**The serverless egress ranges, for the source's firewall.** Check 7 lists them for the workspace's cloud and region, from the published [ip-ranges.json](https://www.databricks.com/networking/v1/ip-ranges.json) (outbound, IPv4), with the date the file was published. Give them to whoever manages the source's firewall. The list changes: as often as every 30 days, and new ranges become active as soon as 60 days after they're published. So keep it with `--egress-file <file>`, and rerun `ready.sh`, or `<SKILL_ROOT>/scripts/serverless-egress.sh <cloud> <region> --state <file>` on its own, at least monthly. When the list has changed, check 7 is a `WARN` and lists the ranges added (`+`, to allow now) and removed (`-`, to drop once nothing uses them); the script on its own exits 3. The region's entries can also hold control-plane NAT addresses: the file doesn't label them, so allow the whole list.

**Service tags don't replace the list for a database source.** On Azure, `AzureDatabricksServerless` (and regional tags such as `AzureDatabricksServerless.CentralUS`) is supported only in a network security perimeter's inbound rules for Azure Storage in the workspace's region ([docs](https://learn.microsoft.com/en-us/azure/databricks/security/network/serverless-network-security/serverless-firewall-config)), not for a VM's network security group or an Azure SQL or Flexible Server firewall. It also isn't the same set: in centralus (2026-10), the regional tag held 70 prefixes and covered 11 of the 31 published egress ranges. The `AzureDatabricks` tag is for classic compute's outbound traffic to the control plane, not for serverless reaching a source. Only Azure was checked; on AWS and GCP, use the published list. For a private path without allowlisting, the docs point to an outbound private link from serverless instead.

**Classic or serverless-only.** `ready.sh` reads this from `databricks clusters list-zones`. A list of zones means classic compute is there; on Azure, `Azure worker environment does not support zones` also means classic is there. `No such workerEnvironment 'serverless-<workspace id>'` (AWS) or `does not have any associated worker environment` (Azure) means a serverless-only workspace, where a gateway must run with `serverless: true`. Any other answer (seen on GCP) doesn't tell, so let the pipeline create decide. (`clusters list-node-types` answers on every workspace, so it can't tell.) Classic also needs the `allow-cluster-create` entitlement or a cluster policy you can use, which `ready.sh` checks. The classic egress IP itself isn't readable from the workspace. Two places show it without starting a cluster: the account API (`databricks account workspaces get <workspace-id>`, its network configuration and network connectivity settings), which needs account admin, and the cloud account that hosts the workspace's VPC or VNet (on AWS, `aws ec2 describe-nat-gateways` in that VPC). On the workspaces tested, neither was available to a workspace admin (2026-10): the account API answered `This API is disabled for users without account admin status`, and the workspace's VPC was in another cloud account. Otherwise, ask the workspace's administrators for it. Classic compute connects from the workspace's own egress (NAT) IP, which the source's firewall must allow. Leave `serverless` unset on every pipeline and gateway, read back what the system chose, and if one fails on compute or can't reach the source, set the opposite and retry: on a workspace with classic compute, integrated CDC and gateways can default to classic ([2-database-connectors.md](2-database-connectors.md#compute-serverless-or-classic)).

## 1. Connector previews (Settings API)

Each Lakeflow Connect connector in preview is a workspace setting. The CLI's `workspace-settings-v2` commands list, read, and change them (workspace admin); they page through the list themselves:

```bash
P=<PROFILE>
databricks workspace-settings-v2 list-workspace-settings-metadata --profile $P -o json \
  | yq -p json -o tsv '.[] | select(((.display_name // "") + " " + (.description // "")) | downcase | (test("lakeflow connect") or (test("join pushdown") and test("federat")))) | [.name, .display_name]'
databricks workspace-settings-v2 get-public-workspace-setting integrated_cdc_oracle_connector --profile $P -o json \
  | yq -p json 'with(select(has("boolean_val")); .v = .boolean_val.value) | with(select(has("boolean_val") | not); .v = .effective_boolean_val.value) | .v'
databricks workspace-settings-v2 patch-public-workspace-setting integrated_cdc_oracle_connector \
  --json '{"name": "integrated_cdc_oracle_connector", "boolean_val": {"value": true}}' --profile $P                 # enable
```

The same calls work through `databricks api get|patch /api/2.1/settings-metadata` and `/api/2.1/settings/<name>`; there, the list's `page_token` is the next setting's name, passed as is.

- About 65 of 275 settings matched on the workspaces checked. The ones that matter for database sources: `integrated_cdc_sql_server_connector`, `integrated_cdc_mysql_connector`, `integrated_cdc_oracle_connector` (integrated CDC per engine), `direct_cdc_connector`, `conn_cdc_col_select` (column selection for database sources), and `ds_v2_join_pushdown` (join pushdown for federated queries, off by default).
- A value is either `boolean_val` (set on this workspace) or `effective_boolean_val` (inherited). Read both; don't use a "first non-null" shortcut, because `false` counts as missing in `yq` and `jq` `//`.
- Some settings aren't writable through the public API yet ("not enabled for public api"): leave them and say so.
- **Not a setting:** standard CDC (gateway) per engine. A workspace without it refuses the gateway with `Ingestion source '<ENGINE>' is not supported in your workspace`; only the Databricks account team can enable it ([feature gates](2-database-connectors.md#workspace-feature-gates-all-database-sources)). Integrated CDC was turned on through its setting on two workspaces where the MySQL and PostgreSQL gateways stayed refused.

## 2. A target catalog

Query-based pipelines can write to any catalog. CDC pipelines (integrated and standard) need one with its **own managed storage**, not Default Storage, and a working storage credential: see [SKILL.md, Prerequisites](../SKILL.md#prerequisites), "A Unity Catalog target". Check a candidate:

```bash
databricks catalogs get <catalog> --profile $P -o json | yq -p json '{"isolation": .isolation_mode, "storage_root": .storage_root}'
databricks external-locations get __databricks_managed_storage_location --profile $P -o json | yq -p json .url   # Default Storage starts with this
```

The workspace's default catalog (`databricks settings default-namespace get --profile $P -o json`, `.namespace.value`) can be a candidate, but check it: on one workspace it was `system`, which pipelines can't write to (nor to `hive_metastore`). Otherwise create one with its own managed location, or use one the workspace's provisioning made for you (check `catalogs list` for catalogs where you have `ALL_PRIVILEGES`).

## 3. Access for everyone running pipelines in the workspace

Three separate grants, in this order: logging in (an assignment plus entitlements), seeing the catalog, using it.

**Log in: workspace assignment.** Assign the account-wide group `account users` (everyone in the workspace's Databricks account), or an account group (not a workspace-local group). A **workspace admin** can do it from the workspace itself, with no account login: the workspace answers account SCIM queries for its own account, and has its own assignment API:

```bash
P=<PROFILE>; G="account users"                                                     # or an account group's name
q=$(printf 'displayName eq "%s"' "$G" | jq -sRr @uri)
ID=$(databricks api get "/api/2.0/account/scim/v2/Groups?filter=$q" --profile $P -o json | yq -p json '.Resources[0].id')
databricks api get /api/2.0/preview/permissionassignments --profile $P -o json \
  | yq -p json -o tsv '.permission_assignments[] | [(.principal.display_name // .principal.user_name), (.permissions | join(","))]'   # who's in now
databricks api put /api/2.0/preview/permissionassignments/principals/$ID --json '{"permissions": ["USER"]}' --profile $P
```

**Assignment alone doesn't let anyone in.** What members can do comes from **entitlements**: `workspace-access` to open the workspace, `databricks-sql-access` for SQL, `allow-cluster-create` and so on. Members of `ADMIN` principals get them all through the built-in `admins` group; `USER` principals get only what the built-in `users` group or their own group has, and that's often nothing. Check, then give the entitlements to the assigned group itself:

```bash
q=$(printf 'displayName eq "%s"' users | jq -sRr @uri)
databricks api get "/api/2.0/preview/scim/v2/Groups?filter=$q&attributes=entitlements" --profile $P -o json \
  | yq -p json '[.Resources[0].entitlements[]?.value] | join(", ")'                # empty: USER members get nothing
cat > ent.json <<'EOF'
{"schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
 "Operations": [{"op": "add", "path": "entitlements", "value": [{"value": "workspace-access"}, {"value": "databricks-sql-access"}]}]}
EOF
databricks api patch /api/2.0/preview/scim/v2/Groups/$ID --json @ent.json --profile $P     # $ID: the assigned account group
```

Seen on an AWS and an Azure workspace: the built-in `users` group had no entitlements, and changing it was refused (`Entitlement modifications on the workspace-users and admins groups are not permitted for this workspace`). Assigning `account users` as `USER` and patching the entitlements onto `account users` itself worked (both accounts had fewer than 10,000 identities, see below): everyone in the account could open the workspace and use SQL without being an admin. Assigning named groups as `ADMIN` also works, because `admins` carries every entitlement, but it makes every member an admin.

An account admin can do the same from the account: `databricks account workspace-assignment update <workspace-id> <principal-id> --json '{"permissions": ["USER"]}' --profile <account-profile>`. `ADMIN` instead of `USER` makes every member a workspace admin.

**A workspace holds at most 10,000 users and service principals** ([docs](https://docs.databricks.com/aws/en/admin/users-groups/); also at most 5,000 groups). Assigning a group adds **all its members, nested ones included**, and the total of everything assigned counts. So `account users` works only in an account with fewer than 10,000 identities. In a larger account the assignment fails with `Cannot add group <id> to workspace: member count <n> exceeds workspace limit 10000`. Assign subgroups instead, for example the teams that run pipelines in the workspace, whose combined membership stays under the limit, and give those groups the entitlements. Check the size first: `databricks api get "/api/2.0/account/scim/v2/Users?count=1&attributes=id" --profile $P -o json | yq -p json .totalResults`.

Groups belong to an account, and workspaces can sit in different accounts: a team group present in one account can be missing in another. Check with the SCIM query above before assigning; `account users` exists in every account.

**See the catalog: binding.** An `ISOLATED` catalog is visible only in the workspaces it's bound to; an `OPEN` one in every workspace on its metastore. Bind each workspace that needs an `ISOLATED` catalog (from a workspace where the catalog is already visible):

```bash
databricks workspace-bindings get <catalog> --profile $P -o json        # the workspaces it's bound to
databricks workspace-bindings update <catalog> --json '{"assign_workspaces": [<workspace-id>]}' --profile $P
```

Bindings are per workspace, not per user, and never cross metastores: a catalog can be bound only to workspaces on its own metastore, which usually means the same cloud, region, and account. Bind the catalog's storage credential and external location the same way if people create tables or volumes in it.

**Use the catalog: grants**, to the same principal:

```sql
GRANT USE CATALOG, USE SCHEMA, CREATE SCHEMA, SELECT, MODIFY, CREATE TABLE ON CATALOG <catalog> TO `account users`;
GRANT USE CONNECTION ON CONNECTION <connection> TO `account users`;          -- for pipelines on a shared connection
```

`MANAGE` on the catalog also lets members bind more workspaces; leave it out if they shouldn't. Catalog privileges don't give workspace login, and workspace login doesn't show an `ISOLATED` catalog that isn't bound: all three are needed.

## 4. Check and grant per principal

What a user, a group, or `account users` needs to run Lakeflow Connect in a workspace (from the public pipeline docs, plus what testing showed):

| Where | Needs | For |
|---|---|---|
| Workspace | Unity Catalog; the connector's preview on (step 1) | every mode |
| Login | an assignment (directly or through a group) and `workspace-access`; `databricks-sql-access` for SQL and `remote_query` | every mode |
| Metastore | `CREATE CONNECTION` to create connections (else use existing ones); `CREATE CATALOG` for foreign catalogs | creating connections; query-based through a foreign catalog |
| Connection | `USE CONNECTION` (or owning it); `CREATE FOREIGN CATALOG` for a foreign catalog | every pipeline on it |
| Target catalog | `USE CATALOG`, `USE SCHEMA`, `CREATE SCHEMA` (or an existing schema with `CREATE TABLE`), `CREATE TABLE`; visible here (bound, if `ISOLATED`) | every mode |
| CDC | `CREATE VOLUME` (staging), on a catalog with its own storage (step 2) | integrated and standard CDC |
| Compute | a network path from serverless (or, for a classic gateway, the workspace's egress IP) to the source; for a classic gateway, cluster creation or a cluster policy | |

Two scripts, run as a workspace admin. `--modes` takes `qbc` (query-based), `fcqbc` (query-based through a foreign catalog), `icdc` (integrated CDC), and `scdc` (standard CDC):

```bash
S=<SKILL_ROOT>/scripts
bash $S/check.sh <PROFILE> "<user-email | group | account users>" --catalog <catalog> --connection <connection> --modes qbc,icdc,fcqbc
bash $S/grant.sh <PROFILE> "<principal>" --level cdc --catalog <catalog> --connection <connection> [--foreign-catalogs] [--create-connections]   # the plan
bash $S/grant.sh ... --apply                                                    # make the changes, then run check.sh again
```

[`check.sh`](../scripts/check.sh) is read-only. It prints one line per requirement (`PASS`, `FAIL`, `WARN`) with the command or SQL that fixes each failure, and exits 1 if anything fails. It works on effective privileges, so grants that come through a group count, and it counts ownership: owning a securable gives every privilege on it, but Unity Catalog's effective-privileges API doesn't report it. That includes the workspace's admins group (`_workspace_admins_<name>_<workspace id>`), which owns the catalog a workspace is created with. [`grant.sh`](../scripts/grant.sh) gives exactly the chosen level: `login` (assignment and entitlements), `qbc` (plus connection and catalog privileges), `cdc` (plus `CREATE VOLUME`). It prints its plan and changes nothing without `--apply`. Grants go through the Unity Catalog REST API, so no SQL warehouse is needed.

**Run as a non-admin**, the workspace returns no assignments and the account directory hides other people's groups, so `check.sh` doesn't count "can't read" as "no". When you check yourself, being logged in proves the assignment and `workspace-access`, and your groups come from `/api/2.0/preview/scim/v2/Me`; anything it can't see is a `WARN` ("not visible to a non-admin"), never a `FAIL`. Run it as a workspace admin to check other principals.

Seen on an Azure serverless-only and an AWS classic workspace: a workspace admin who owned the connection and the catalog passed every check. `account users` passed login (after step 3) and the metastore checks (where `account users` owned the metastore), and failed exactly the connection and catalog grants, each with its fix. `grant.sh --apply` for a user went through every call.
