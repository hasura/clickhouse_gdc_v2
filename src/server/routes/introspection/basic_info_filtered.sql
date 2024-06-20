SELECT
    tables.table_name AS "name",
    tables.table_schema AS "table_schema",
    tables.table_type AS "table_type"
FROM INFORMATION_SCHEMA.TABLES AS tables
WHERE tables.table_catalog NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
AND tables.table_name IN {table_names:Array(String)}
FORMAT JSON;
