SELECT toJSONString(
        cast(
            tuple(
                groupArray(
                    cast(
                        tuple(t.name, t.primary_key, t.type, t.columns),
                        'Tuple(name Array(String), primary_key Array(String), type String, columns Array(Tuple(name String, type String, nullable Bool)))'
                    )
                )
            ),
            'Tuple(tables Array(Tuple(name Array(String), primary_key Array(String), type String, columns Array(Tuple(name String, type String, nullable Bool)))))'
        )
    ) AS "schema"
FROM (
        SELECT array(t.table_name) AS "name",
            sc.primary_key AS "primary_key",
            toString(
                cast(
                    t.table_type,
                    'Enum(\'table\' = 1, \'view\' = 2)'
                )
            ) AS "type",
            c.columns AS "columns"
        FROM INFORMATION_SCHEMA.TABLES AS t
            LEFT JOIN (
                SELECT c.table_catalog,
                    c.table_schema,
                    c.table_name,
                    groupArray(
                        cast(
                            tuple(
                                c.column_name,
                                -- strip out surrounding `Nullable(T)
                                -- strip out precision "(n)" from types that support it, eg. DateTime(9)
                                -- this may need to be revised later, to instead create types for each available precision
                                replaceRegexpOne(
                                    replaceRegexpOne(
                                        c.data_type,'^Nullable\((.*)\)$', '\1'),
                                    '\(.*\)',
                                    ''
                                ),
                                toBool(c.is_nullable)
                            ),
                            'Tuple(name String, type String, nullable Bool)'
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