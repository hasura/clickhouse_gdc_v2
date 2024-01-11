SELECT
    tables.table_name AS "name",
    tables.table_schema AS "table_schema",
    system_columns.primary_key AS "primary_key",
    tables.table_type AS "table_type",
    cast(
        columns.columns,
        'Array(Tuple(name String, column_type String, nullable Bool))'
    ) AS "columns"
FROM INFORMATION_SCHEMA.TABLES AS tables
LEFT JOIN (
    SELECT columns.table_catalog,
        columns.table_schema,
        columns.table_name,
        groupArray(
            tuple(
                columns.column_name,
                columns.data_type,
                toBool(columns.is_nullable)
            )
        ) AS "columns"
    FROM INFORMATION_SCHEMA.COLUMNS AS columns
    GROUP BY columns.table_catalog,
        columns.table_schema,
        columns.table_name
) AS columns USING (table_catalog, table_schema, table_name)
LEFT JOIN (
    SELECT system_columns.database,
        system_columns.table,
        groupArray(system_columns.name) AS "primary_key"
    FROM system.columns AS system_columns
    WHERE system_columns.is_in_primary_key = 1
    GROUP BY system_columns.database, system_columns.table
) AS system_columns ON system_columns.database = tables.table_schema
AND system_columns.table = tables.table_name
WHERE tables.table_catalog NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
AND tables.table_type IN ('BASE TABLE', 'VIEW')
AND tables.table_name IN {table_names:Array(String)}
FORMAT JSON;