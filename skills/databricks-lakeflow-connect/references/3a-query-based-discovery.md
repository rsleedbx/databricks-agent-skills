# Query-based discovery: find the source tables

Find the tables, keys, and cursor candidates through an existing connection before you create a query-based pipeline. Concepts: [3-query-based-connectors.md](3-query-based-connectors.md). Pipeline setup: [3b-query-based-pipeline.md](3b-query-based-pipeline.md).

## Step 0 — find the source tables through the connection

If the user names a connection but not the tables, look at the source through the connection with `remote_query`. It's read-only, needs no foreign catalog, and also proves that serverless can reach the source. Write each query to a `.sql` file with your file tool and run it with:

```bash
databricks experimental aitools tools query --file <file>.sql --profile <PROFILE> -o csv
```

Each query lists every table with its primary key and the date/time columns that could be the cursor. Pick tables that have a primary key and a NOT NULL cursor that changes on every update. Tables without a key need `APPEND_ONLY` (which needs a cursor) or SCD1 with no cursor (a full load each run). See [the shape table](3-query-based-connectors.md#what-each-source-shape-does-with-each-target-tested) and run the [pre-flight checks](3-query-based-connectors.md#pre-flight-checks-before-creating-the-pipeline-advise-the-product-doesnt-check) before creating the pipeline.

**PostgreSQL:** list databases first, then the tables in one database.

```sql
SELECT * FROM remote_query('<connection>', database => 'postgres', query => "SELECT datname FROM pg_database WHERE NOT datistemplate") ORDER BY 1
```

```sql
SELECT * FROM remote_query('<connection>', database => '<database>', query => "
  SELECT c.table_schema, c.table_name,
         (SELECT string_agg(k.column_name, ',' ORDER BY k.ordinal_position)
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage k
              ON k.constraint_schema = tc.constraint_schema AND k.constraint_name = tc.constraint_name
           WHERE tc.table_schema = c.table_schema AND tc.table_name = c.table_name
             AND tc.constraint_type = 'PRIMARY KEY') AS primary_key,
         string_agg(c.column_name || ' ' || c.data_type || CASE WHEN c.is_nullable = 'NO' THEN ' not null' ELSE '' END,
                    ', ' ORDER BY c.ordinal_position)
           FILTER (WHERE c.data_type LIKE 'timestamp%' OR c.data_type = 'date') AS cursor_candidates
    FROM information_schema.columns c
    JOIN information_schema.tables t
      ON t.table_schema = c.table_schema AND t.table_name = c.table_name AND t.table_type = 'BASE TABLE'
   WHERE c.table_schema NOT IN ('pg_catalog', 'information_schema')
   GROUP BY c.table_schema, c.table_name")
ORDER BY 1, 2
```

**MySQL:** a MySQL database is the schema level, so there's no `database =>` argument. `on update` marks a column that MySQL bumps on every update, which makes it a good cursor.

```sql
SELECT * FROM remote_query('<connection>', query => "
  SELECT c.table_schema, c.table_name,
         group_concat(CASE WHEN c.column_key = 'PRI' THEN c.column_name END ORDER BY c.ordinal_position) AS primary_key,
         group_concat(CASE WHEN c.data_type IN ('timestamp', 'datetime', 'date')
                           THEN concat(c.column_name, ' ', c.column_type, IF(c.is_nullable = 'NO', ' not null', ''),
                                       IF(c.extra LIKE '%on update%', ' on update', '')) END
                      ORDER BY c.ordinal_position) AS cursor_candidates
    FROM information_schema.columns c
    JOIN information_schema.tables t
      ON t.table_schema = c.table_schema AND t.table_name = c.table_name AND t.table_type = 'BASE TABLE'
   WHERE c.table_schema NOT IN ('mysql', 'sys', 'information_schema', 'performance_schema')
   GROUP BY c.table_schema, c.table_name")
ORDER BY 1, 2
```

**SQL Server:** list databases on `master`, then the tables in one database. Run the table query once per database; the connection's login may not be able to open every database.

```sql
SELECT * FROM remote_query('<connection>', database => 'master', query => "SELECT name FROM sys.databases WHERE database_id > 4") ORDER BY name
```

```sql
SELECT * FROM remote_query('<connection>', database => '<database>', query => "
  SELECT s.name AS table_schema, t.name AS table_name,
         (SELECT STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal)
            FROM sys.indexes i
            JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
            JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
           WHERE i.object_id = t.object_id AND i.is_primary_key = 1) AS primary_key,
         (SELECT STRING_AGG(c.name + ' ' + ty.name + CASE WHEN c.is_nullable = 0 THEN ' not null' ELSE '' END, ', ')
                 WITHIN GROUP (ORDER BY c.column_id)
            FROM sys.columns c JOIN sys.types ty ON ty.user_type_id = c.user_type_id
           WHERE c.object_id = t.object_id
             AND ty.name IN ('datetime', 'datetime2', 'datetimeoffset', 'smalldatetime', 'date', 'rowversion', 'timestamp')) AS cursor_candidates
    FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id
   WHERE t.is_ms_shipped = 0") ORDER BY table_schema, table_name
```

**Oracle:** run these queries through a connection whose `service_name` is the PDB (for example `FREEPDB1`); why, and which connection the pipeline uses instead, is in the Oracle row of the [Step 1 table](3b-query-based-pipeline.md#step-1--uc-connection). The query below passes no `service_name`. Names come back in uppercase, and the pipeline needs them that way.

```sql
SELECT * FROM remote_query('<pdb-connection>', query => "
  SELECT t.owner AS table_schema, t.table_name,
         (SELECT LISTAGG(cc.column_name, ',') WITHIN GROUP (ORDER BY cc.position)
            FROM all_constraints c JOIN all_cons_columns cc ON cc.owner = c.owner AND cc.constraint_name = c.constraint_name
           WHERE c.owner = t.owner AND c.table_name = t.table_name AND c.constraint_type = 'P') AS primary_key,
         (SELECT LISTAGG(col.column_name || ' ' || col.data_type || CASE WHEN col.nullable = 'N' THEN ' not null' END, ', ')
                 WITHIN GROUP (ORDER BY col.column_id)
            FROM all_tab_columns col
           WHERE col.owner = t.owner AND col.table_name = t.table_name
             AND (col.data_type = 'DATE' OR col.data_type LIKE 'TIMESTAMP%')) AS cursor_candidates
    FROM all_tables t
   WHERE t.owner NOT IN (SELECT username FROM all_users WHERE oracle_maintained = 'Y')") ORDER BY 1, 2
```

How these queries are written:

- **Outer string in double quotes,** so the source SQL can use single-quoted literals without escaping.
- **No `ORDER BY` inside `remote_query`.** It wraps the query in a subquery, and SQL Server rejects `ORDER BY` there. Sort in the outer query.
- **One file per query** and plain commands only. Don't loop over databases in a shell `for`: run one file per database.
