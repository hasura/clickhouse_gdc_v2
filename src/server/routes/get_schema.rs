use std::error::Error;

use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::{info_span, Instrument};

use crate::server::{
    api::{
        error_response::ErrorResponseType,
        query_request::ScalarType,
        schema_response::{ColumnInfo, ColumnType, SchemaResponse, TableInfo, TableType},
    },
    client::execute_query,
    config::{SourceConfig, SourceName},
    error::ServerError,
    Config,
};

#[axum_macros::debug_handler]
pub async fn get_schema(
    SourceName(_source_name): SourceName,
    SourceConfig(config): SourceConfig,
) -> Result<Json<SchemaResponse>, ServerError> {
    let introspection_sql = include_str!("../database_introspection.sql");

    let introspection: Vec<TableIntrospection> = execute_query(&config, introspection_sql).await?;

    let response = SchemaResponse {
        functions: None,
        object_types: None,
        tables: introspection
            .into_iter()
            .map(|table| {
                let TableIntrospection {
                    name: table_name,
                    table_type,
                    primary_key,
                    columns,
                } = table;

                Ok(TableInfo {
                    name: vec![aliased_table_name(&table_name, &config)],
                    description: None,
                    table_type: Some(table_type),
                    primary_key: Some(primary_key),
                    foreign_keys: None,
                    insertable: None,
                    updatable: None,
                    deletable: None,
                    columns: columns
                        .into_iter()
                        .map(|column| {
                            let ColumnIntrospection {
                                name: column_name,
                                column_type,
                                nullable,
                            } = column;

                            let scalar_type = clickhouse_parser::data_type(&column_type)
                                .map(|data_type| get_scalar_type(&data_type))
                                .unwrap_or(ScalarType::Complex);

                            Ok(ColumnInfo {
                                name: aliased_column_name(&table_name, &column_name, &config),
                                description: None,
                                nullable,
                                insertable: None,
                                updatable: None,
                                value_generated: None,
                                column_type: ColumnType::ScalarType(scalar_type),
                            })
                        })
                        .collect::<Result<_, ServerError>>()?,
                })
            })
            .collect::<Result<_, ServerError>>()?,
    };

    Ok(Json(response))
}

#[derive(Debug, Serialize, Deserialize)]
struct TableIntrospection {
    name: String,
    primary_key: Vec<String>,
    table_type: TableType,
    columns: Vec<ColumnIntrospection>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ColumnIntrospection {
    name: String,
    column_type: String,
    nullable: bool,
}

fn aliased_table_name(table_name: &str, config: &Config) -> String {
    if let Some(tables) = &config.tables {
        if let Some(table_config) = tables
            .iter()
            .find(|table_config| table_config.name == table_name)
        {
            if let Some(alias) = &table_config.alias {
                return alias.to_owned();
            }
        }
    }

    table_name.to_owned()
}

fn aliased_column_name(table_name: &str, column_name: &str, config: &Config) -> String {
    if let Some(tables) = &config.tables {
        if let Some(table_config) = tables
            .iter()
            .find(|table_config| table_config.name == table_name)
        {
            if let Some(columns) = &table_config.columns {
                if let Some(column_config) = columns
                    .iter()
                    .find(|column_config| column_config.name == column_name)
                {
                    if let Some(alias) = &column_config.alias {
                        return alias.to_owned();
                    }
                }
            }
        }
    }

    column_name.to_owned()
}

fn get_scalar_type(data_type: &DataType) -> ScalarType {
    let scalar_type = match data_type {
        DataType::Nullable(inner) => get_scalar_type(inner),
        DataType::Bool => ScalarType::Bool,
        DataType::String => ScalarType::String,
        DataType::FixedString(_) => ScalarType::String,
        DataType::UInt8 => ScalarType::UInt8,
        DataType::UInt16 => ScalarType::UInt16,
        DataType::UInt32 => ScalarType::UInt32,
        DataType::UInt64 => ScalarType::UInt64,
        DataType::UInt128 => ScalarType::UInt128,
        DataType::UInt256 => ScalarType::UInt256,
        DataType::Int8 => ScalarType::Int8,
        DataType::Int16 => ScalarType::Int16,
        DataType::Int32 => ScalarType::Int32,
        DataType::Int64 => ScalarType::Int64,
        DataType::Int128 => ScalarType::Int128,
        DataType::Int256 => ScalarType::Int256,
        DataType::Float32 => ScalarType::Float32,
        DataType::Float64 => ScalarType::Float64,
        DataType::Decimal(_, _) => ScalarType::Decimal,
        DataType::Decimal32(_) => ScalarType::Decimal,
        DataType::Decimal64(_) => ScalarType::Decimal,
        DataType::Decimal128(_) => ScalarType::Decimal,
        DataType::Decimal256(_) => ScalarType::Decimal,
        DataType::Date => ScalarType::Date,
        DataType::Date32 => ScalarType::Date32,
        DataType::DateTime(_) => ScalarType::DateTime,
        DataType::DateTime64(_, _) => ScalarType::DateTime64,
        DataType::Json => ScalarType::Json,
        DataType::Uuid => ScalarType::Uuid,
        DataType::IPv4 => ScalarType::IPv4,
        DataType::IPv6 => ScalarType::IPv6,
        DataType::LowCardinality(inner) => get_scalar_type(inner),
        DataType::Nested(_) => ScalarType::Complex,
        DataType::Array(_) => ScalarType::Complex,
        DataType::Map(_, _) => ScalarType::Complex,
        DataType::Tuple(_) => ScalarType::Complex,
        DataType::Enum(_) => ScalarType::String,
        DataType::Nothing => ScalarType::Complex,
    };

    scalar_type
}

#[derive(Debug, PartialEq)]
pub enum DataType {
    Nullable(Box<DataType>),
    Bool,
    String,
    FixedString(u32),
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    UInt256,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,
    Float32,
    Float64,
    Decimal(u32, u32),
    Decimal32(u32),
    Decimal64(u32),
    Decimal128(u32),
    Decimal256(u32),
    Date,
    Date32,
    DateTime(Option<String>),
    DateTime64(u32, Option<String>),
    Json,
    Uuid,
    IPv4,
    IPv6,
    LowCardinality(Box<DataType>),
    Nested(Vec<(String, DataType)>),
    Array(Box<DataType>),
    Map(Box<DataType>, Box<DataType>),
    Tuple(Vec<DataType>),
    Enum(Vec<(String, Option<u32>)>),
    Nothing,
}

peg::parser! {
  grammar clickhouse_parser() for str {
    pub rule data_type() -> DataType = nullable()
        / uint256()
        / uint128()
        / uint64()
        / uint32()
        / uint16()
        / uint8()
        / int256()
        / int128()
        / int64()
        / int32()
        / int16()
        / int8()
        / float32()
        / float64()
        / decimal256()
        / decimal128()
        / decimal64()
        / decimal32()
        / decimal()
        / bool()
        / string()
        / fixed_string()
        / date_time64()
        / date_time()
        / date32()
        / date()
        / json()
        / uuid()
        / ipv4()
        / ipv6()
        / low_cardinality()
        / nested()
        / array()
        / map()
        / tuple()
        / r#enum()
        / nothing()
    rule nullable() -> DataType = "Nullable(" t:data_type() ")" { DataType::Nullable(Box::new(t)) }
    rule uint8() -> DataType = "UInt8" { DataType::UInt8 }
    rule uint16() -> DataType = "UInt16" { DataType::UInt16 }
    rule uint32() -> DataType = "UInt32" { DataType::UInt32 }
    rule uint64() -> DataType = "UInt64" { DataType::UInt64 }
    rule uint128() -> DataType = "UInt128" { DataType::UInt128 }
    rule uint256() -> DataType = "UInt256" { DataType::UInt256 }
    rule int8() -> DataType = "Int8" { DataType::Int8 }
    rule int16() -> DataType = "Int16" { DataType::Int16 }
    rule int32() -> DataType = "Int32" { DataType::Int32 }
    rule int64() -> DataType = "Int64" { DataType::Int64 }
    rule int128() -> DataType = "Int128" { DataType::Int128 }
    rule int256() -> DataType = "Int256" { DataType::Int256 }
    rule float32() -> DataType = "Float32" { DataType::Float32 }
    rule float64() -> DataType = "Float64" { DataType::Float64 }
    rule decimal() -> DataType = "Decimal(" p:integer_value() ", " s:integer_value() ")" { DataType::Decimal(p, s) }
    rule decimal32() -> DataType = "Decimal32(" s:integer_value() ")" { DataType::Decimal32(s) }
    rule decimal64() -> DataType = "Decimal64(" s:integer_value() ")" { DataType::Decimal64(s) }
    rule decimal128() -> DataType = "Decimal128(" s:integer_value() ")" { DataType::Decimal128(s) }
    rule decimal256() -> DataType = "Decimal256(" s:integer_value() ")" { DataType::Decimal256(s) }
    rule bool() -> DataType = "Bool" { DataType::Bool }
    rule string() -> DataType = "String" { DataType::String }
    rule fixed_string() -> DataType = "FixedString(" n:integer_value() ")" { DataType::FixedString(n) }
    rule date() -> DataType = "Date" { DataType::Date }
    rule date32() -> DataType = "Date32" { DataType::Date32 }
    rule date_time() -> DataType = "DateTime" tz:("(" tz:string_value()? ")" { tz })? { DataType::DateTime(tz.flatten().map(|s| s.to_owned())) }
    rule date_time64() -> DataType = "DateTime64(" p:integer_value() tz:(", " tz:string_value()? { tz })? ")" { DataType::DateTime64(p, tz.flatten().map(|s| s.to_owned())) }
    rule json() -> DataType = "JSON" { DataType::Json }
    rule uuid() -> DataType = "UUID" { DataType::Uuid }
    rule ipv4() -> DataType = "IPv4" { DataType::IPv4 }
    rule ipv6() -> DataType = "IPv6" { DataType::IPv6 }
    rule low_cardinality() -> DataType = "LowCardinality(" t:data_type() ")" { DataType::LowCardinality(Box::new(t)) }
    rule nested() -> DataType =  "Nested(" e:((n:string_value() " " t:data_type() { (n, t)}) ** ", ") ")" { DataType::Nested(e) }
    rule array() -> DataType =  "Array(" t:data_type() ")" { DataType::Array(Box::new(t)) }
    rule map() -> DataType =  "Map(" k:data_type() ", " v:data_type() ")" { DataType::Map(Box::new(k), Box::new(v)) }
    rule tuple() -> DataType =  "Tuple(" t:(data_type() ** ", ")  ")" { DataType::Tuple(t) }
    rule r#enum() -> DataType = "Enum" ("8" / "16")?  "(" e:((n:string_value() i:(" = " i:integer_value() { i })? { (n, i) }) ** ", ") ")" { DataType::Enum(e)}
    rule nothing() -> DataType = "Nothing" { DataType::Nothing }

    rule integer_value() -> u32 = n:$(['0'..='9']+) {? n.parse().or(Err("u32")) }
    rule string_value() -> String = s:$([_]+) { s.to_string() }
  }
}

#[test]
fn test_parsing() {
    let data_types = vec![
        ("Int32", DataType::Int32),
        (
            "Nullable(Int32)",
            DataType::Nullable(Box::new(DataType::Int32)),
        ),
        (
            "Nullable(Date32)",
            DataType::Nullable(Box::new(DataType::Date32)),
        ),
        ("DateTime64(9)", DataType::DateTime64(9, None)),
        ("Float64", DataType::Float64),
        ("Date", DataType::Date),
        (
            "LowCardinality(String)",
            DataType::LowCardinality(Box::new(DataType::String)),
        ),
        (
            "Map(LowCardinality(String), String)",
            DataType::Map(
                Box::new(DataType::LowCardinality(Box::new(DataType::String))),
                Box::new(DataType::String),
            ),
        ),
        (
            "Array(DateTime64(9))",
            DataType::Array(Box::new(DataType::DateTime64(9, None))),
        ),
        (
            "Array(Map(LowCardinality(String), String))",
            DataType::Array(Box::new(DataType::Map(
                Box::new(DataType::LowCardinality(Box::new(DataType::String))),
                Box::new(DataType::String),
            ))),
        ),
        (
            "Tuple(String, Int32)",
            DataType::Tuple(vec![DataType::String, DataType::Int32]),
        ),
    ];

    for (s, t) in data_types {
        let parsed = clickhouse_parser::data_type(s);
        assert_eq!(parsed, Ok(t), "Able to parse correctly");
    }
}
