SELECT
    t.table_name AS "name",
    toString(
        cast(
            t.table_type,
            'Enum(\'table\' = 1, \'view\' = 2)'
        )
    ) AS "table_type"
FROM INFORMATION_SCHEMA.TABLES AS t
WHERE t.table_catalog = currentDatabase()
AND t.table_type IN (1, 2) -- table type is an enum, where tables and views are 1 and 2 respectively
FORMAT JSON;
