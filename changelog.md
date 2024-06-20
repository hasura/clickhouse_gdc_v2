# ChangeLog

## 2.40.0

- Support all table types (previously only supported BASE TABLE and VIEW)

## 2.37.2

- fix breaking change: don't use schema qualification for the `default` schema.
  > Supporting non-default schema meant qualifying tables, which also meant introducing the schema name in the table name array.
  > This was a breaking change for two reasons: older metadata needed to be updated to qualify tables with the `default` schema,
  > and it also changed the generated hasura types. This fix removes the `default` schema from the table name array, but keeps any other schemas.

## 2.37.1

- fix introspection 4: when introspecting for specific tables, allow qualified table names. Note we only filter on table names with no regards for schemas, this may need to be fixed later

## 2.36.0

- fix introspection 3: instead of only showing tables in the current database schema, show tables from all schema except `system` and `INFORMATION_SCHEMA`

## 2.35.2

- fix introspection 2: only query for base tables and views

## 2.35.1

- fix introspection
- implement database healthchecks
- cleanup to reduce compile time warnings

## 2.35.0

- Update docker build process to produce small, alpine-based images
- update version number to match HGE version. Going forward will try to keep major and minor version in sync. Patch version will be independent.

## 0.4.2

- Improve error handling requests to clickhouse db

## 0.4.1

- update gdc_rust_types
- remove GET /schema endpoint
- add post_schema capability

## 0.4.0

- Added changelog :)
- Compatibly with Hasura GraphQL Engine v2.33+, broke compatibility with older versions
- Using data types from external crate
- Implement v2.33 GDC features: schema filtering
