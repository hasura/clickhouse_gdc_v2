SELECT toJSONString(
        cast(
            groupArray(
                tuple(t.name, t.primary_key, t.table_type, t.columns)
            ),
            'Array(Tuple(name String, primary_key Array(String), table_type String, columns Array(Tuple(name String, column_type String, nullable Bool))))'
        )
    ) AS "schema"
FROM (
        SELECT t.table_name AS "name",
            sc.primary_key AS "primary_key",
            toString(
                cast(
                    t.table_type,
                    'Enum(\'table\' = 1, \'view\' = 2)'
                )
            ) AS "table_type",
            c.columns AS "columns"
        FROM INFORMATION_SCHEMA.TABLES AS t
            LEFT JOIN (
                SELECT c.table_catalog,
                    c.table_schema,
                    c.table_name,
                    groupArray(
                        tuple(
                            c.column_name,
                            c.data_type,
                            toBool(c.is_nullable)
                        )
                    ) AS "columns"
                FROM INFORMATION_SCHEMA.COLUMNS AS c
                GROUP BY c .table_catalog,
                    c.table_schema,
                    c.table_name
            ) AS c USING (table_catalog, table_schema, table_name)
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
        WHERE t.table_schema = currentDatabase()
            AND t.table_type IN (1, 2) -- table type is an enum, where tables and views are 1 and 2 respectively
    ) AS t