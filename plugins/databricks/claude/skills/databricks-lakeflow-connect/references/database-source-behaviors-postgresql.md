# PostgreSQL source behaviors and errors

What happens on a PostgreSQL source, with the error text as the product returns it. Names and IDs are placeholders (`<slot_name>`, `<schema>.<table>`, `<user>`, `<database>`). Shared behaviors (reading the destination, gateway compute, table limits): [database-source-behaviors.md](database-source-behaviors.md).

| Symptom | Likely cause | Fix |
|---|---|---|
| `PostgreSQL slot name cannot be empty or null` at create | Pipeline created with an empty slot name | Set `slot_name` to the slot the pipeline will use. |
| `CDC_NOT_ENABLED`: `Logical replication slot <slot_name> doesn't exist with pgoutput plugin` | The slot doesn't exist when the first update runs | Create the slot before the first update, as a role with `REPLICATION` (or a superuser) in the database the connection opens: `SELECT pg_create_logical_replication_slot('<slot_name>', 'pgoutput');`. The gateway doesn't create it. |
| The same `CDC_NOT_ENABLED` message | The slot was dropped | Create the slot again by hand, then run a full refresh of the tables. A full refresh doesn't recreate the slot. |
| `INGESTION_GATEWAY_PG_SLOT_IS_INVALIDATED` on the next update | The slot was lost (WAL removed past `max_slot_wal_keep_size`) | Create the slot again, then full refresh. On PostgreSQL 16 a lost slot shows as `wal_status = 'lost'`; `invalidation_reason` exists from PostgreSQL 17. |
| Its named slot is still in `pg_replication_slots`; it stays listed after the delete | The pipeline was deleted | Drop it once the pipeline is gone: `SELECT pg_drop_replication_slot('<slot_name>');`. |
| `INGESTION_GATEWAY_CDC_NOT_ENABLED`: "does not have primary key or has a TOAST-able type column, and needs FULL REPLICA IDENTITY" | A primary key and a TOAST-able column (such as `text`) under `REPLICA IDENTITY DEFAULT` | `ALTER TABLE <schema>.<table> REPLICA IDENTITY FULL;`. If the message stays after the change, recreating the gateway and ingestion pipelines with the same settings cleared it. |
| `INGESTION_GATEWAY_CDC_ERROR.CDC_NOT_ENABLED`: "CDC is not enabled on User <user> ... wal_level is set to 'replica'. It should be set to 'logical' for CDC to work." The update can sit in `INITIALIZING` for 20 minutes or more with no events | `wal_level` is `replica` | Set `wal_level = logical`, restart the server, then start the update. |
| At server start: `FATAL: logical replication slot "<slot_name>" exists, but wal_level < logical`; the container exits | `wal_level` lowered while slots exist | Stop the gateway, drop the logical slots, then lower `wal_level`. PostgreSQL 16 doesn't invalidate the slots. |
| `INGESTION_GATEWAY_CONNECTION_ERROR.TLS_CONNECTION_FAILURE` | `verify-ca` SSL mode with a self-signed server certificate | Add `"trustServerCertificate": "true"` to the connection's options, or use a certificate from a CA. |

