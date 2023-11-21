SELECT
    tables.table_name AS "name",
    tables.table_type AS "table_type"
FROM INFORMATION_SCHEMA.TABLES AS tables
WHERE tables.table_catalog = currentDatabase()
AND tables.table_type IN ('BASE TABLE', 'VIEW')
FORMAT JSON;
