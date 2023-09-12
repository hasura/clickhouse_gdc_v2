SELECT
    t.table_name AS "name",
    sc.primary_key AS "primary_key",
    toString(
        cast(
            t.table_type,
            'Enum(\'table\' = 1, \'view\' = 2)'
        )
    ) AS "table_type",
    cast(
        C .columns,
        'Array(Tuple(name String, column_type String, nullable Bool))'
    ) AS "columns"
FROM INFORMATION_SCHEMA.TABLES AS t
LEFT JOIN (
    SELECT C .table_catalog,
        C .table_schema,
        C .table_name,
        groupArray(
            tuple(
                C .column_name,
                C .data_type,
                toBool(C .is_nullable)
            )
        ) AS "columns"
    FROM INFORMATION_SCHEMA.COLUMNS AS C
    GROUP BY C .table_catalog,
        C .table_schema,
        C .table_name
) AS C USING (table_catalog, table_schema, table_name)
LEFT JOIN (
    SELECT sc.database,
        sc.table,
        groupArray(sc.name) AS "primary_key"
    FROM system.columns sc
    WHERE sc.is_in_primary_key = 1
    GROUP BY sc.database,
        sc.table
) AS sc ON sc.database = t.table_schema
AND sc.table = t.table_name
WHERE t.table_catalog = currentDatabase()
AND t.table_type IN (1, 2) -- table type is an enum, where tables and views are 1 and 2 respectively
FORMAT JSON;