use std::str::FromStr;

use axum::Json;
use serde::{Deserialize, Serialize};
mod clickhouse_data_type;

use crate::server::{
    api::{
        query_request::ScalarType,
        schema_response::{ColumnInfo, ColumnType, SchemaResponse, TableInfo, TableType},
    },
    client::execute_query,
    config::{SourceConfig, SourceName},
    error::ServerError,
    routes::get_schema::clickhouse_data_type::Identifier,
    Config,
};

use self::clickhouse_data_type::ClickhouseDataType;

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

                            let scalar_type = ClickhouseDataType::from_str(&column_type)
                                .map(|data_type| get_scalar_type(&data_type))
                                .unwrap_or(ScalarType::Unknown);

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

fn get_scalar_type(data_type: &ClickhouseDataType) -> ScalarType {
    use ClickhouseDataType as CDT;
    let scalar_type = match data_type {
        CDT::Nullable(inner) => get_scalar_type(inner),
        CDT::Bool => ScalarType::Bool,
        CDT::String => ScalarType::String,
        CDT::FixedString(_) => ScalarType::String,
        CDT::UInt8 => ScalarType::UInt8,
        CDT::UInt16 => ScalarType::UInt16,
        CDT::UInt32 => ScalarType::UInt32,
        CDT::UInt64 => ScalarType::UInt64,
        CDT::UInt128 => ScalarType::UInt128,
        CDT::UInt256 => ScalarType::UInt256,
        CDT::Int8 => ScalarType::Int8,
        CDT::Int16 => ScalarType::Int16,
        CDT::Int32 => ScalarType::Int32,
        CDT::Int64 => ScalarType::Int64,
        CDT::Int128 => ScalarType::Int128,
        CDT::Int256 => ScalarType::Int256,
        CDT::Float32 => ScalarType::Float32,
        CDT::Float64 => ScalarType::Float64,
        CDT::Decimal { .. } => ScalarType::Decimal,
        CDT::Decimal32 { .. } => ScalarType::Decimal,
        CDT::Decimal64 { .. } => ScalarType::Decimal,
        CDT::Decimal128 { .. } => ScalarType::Decimal,
        CDT::Decimal256 { .. } => ScalarType::Decimal,
        CDT::Date => ScalarType::Date,
        CDT::Date32 => ScalarType::Date32,
        CDT::DateTime { .. } => ScalarType::DateTime,
        CDT::DateTime64 { .. } => ScalarType::DateTime64,
        CDT::Json => ScalarType::Json,
        CDT::Uuid => ScalarType::Uuid,
        CDT::IPv4 => ScalarType::IPv4,
        CDT::IPv6 => ScalarType::IPv6,
        CDT::LowCardinality(inner) => get_scalar_type(inner),
        CDT::Nested(_) => ScalarType::Unknown,
        CDT::Array(_) => ScalarType::Unknown,
        CDT::Map { .. } => ScalarType::Unknown,
        CDT::Tuple(_) => ScalarType::Unknown,
        CDT::Enum(_) => ScalarType::String,
        CDT::Nothing => ScalarType::Unknown,
        CDT::SimpleAggregateFunction {
            function: _,
            arguments,
        } => {
            // simple aggregates are stored in the same format as their first input type
            if let Some(data_type) = arguments.first() {
                get_scalar_type(data_type)
            } else {
                ScalarType::Unknown
            }
        }
        CDT::AggregateFunction {
            function,
            arguments,
        } => {
            let function_name = match &function.name {
                Identifier::DoubleQuoted(n) => n,
                Identifier::BacktickQuoted(n) => n,
                Identifier::Unquoted(n) => n,
            }
            .as_str();

            match (function_name, arguments.first()) {
                ("avg", Some(CDT::UInt8)) => ScalarType::AvgUInt8,
                ("avg", Some(CDT::UInt16)) => ScalarType::AvgUInt16,
                ("avg", Some(CDT::UInt32)) => ScalarType::AvgUInt32,
                ("avg", Some(CDT::UInt64)) => ScalarType::AvgUInt64,
                ("avg", Some(CDT::UInt128)) => ScalarType::AvgUInt128,
                ("avg", Some(CDT::UInt256)) => ScalarType::AvgUInt256,
                ("avg", Some(CDT::Int8)) => ScalarType::AvgInt8,
                ("avg", Some(CDT::Int16)) => ScalarType::AvgInt16,
                ("avg", Some(CDT::Int32)) => ScalarType::AvgInt32,
                ("avg", Some(CDT::Int64)) => ScalarType::AvgInt64,
                ("avg", Some(CDT::Int128)) => ScalarType::AvgInt128,
                ("avg", Some(CDT::Int256)) => ScalarType::AvgInt256,
                ("avg", Some(CDT::Float32)) => ScalarType::AvgFloat32,
                ("avg", Some(CDT::Float64)) => ScalarType::AvgFloat64,
                (
                    "avg",
                    Some(
                        CDT::Decimal { .. }
                        | CDT::Decimal32 { .. }
                        | CDT::Decimal64 { .. }
                        | CDT::Decimal128 { .. }
                        | CDT::Decimal256 { .. },
                    ),
                ) => ScalarType::AvgDecimal,
                ("sum", Some(CDT::UInt8)) => ScalarType::SumUInt8,
                ("sum", Some(CDT::UInt16)) => ScalarType::SumUInt16,
                ("sum", Some(CDT::UInt32)) => ScalarType::SumUInt32,
                ("sum", Some(CDT::UInt64)) => ScalarType::SumUInt64,
                ("sum", Some(CDT::UInt128)) => ScalarType::SumUInt128,
                ("sum", Some(CDT::UInt256)) => ScalarType::SumUInt256,
                ("sum", Some(CDT::Int8)) => ScalarType::SumInt8,
                ("sum", Some(CDT::Int16)) => ScalarType::SumInt16,
                ("sum", Some(CDT::Int32)) => ScalarType::SumInt32,
                ("sum", Some(CDT::Int64)) => ScalarType::SumInt64,
                ("sum", Some(CDT::Int128)) => ScalarType::SumInt128,
                ("sum", Some(CDT::Int256)) => ScalarType::SumInt256,
                ("sum", Some(CDT::Float32)) => ScalarType::SumFloat32,
                ("sum", Some(CDT::Float64)) => ScalarType::SumFloat64,
                (
                    "sum",
                    Some(
                        CDT::Decimal { .. }
                        | CDT::Decimal32 { .. }
                        | CDT::Decimal64 { .. }
                        | CDT::Decimal128 { .. }
                        | CDT::Decimal256 { .. },
                    ),
                ) => ScalarType::SumDecimal,
                ("max", Some(CDT::UInt8)) => ScalarType::MaxUInt8,
                ("max", Some(CDT::UInt16)) => ScalarType::MaxUInt16,
                ("max", Some(CDT::UInt32)) => ScalarType::MaxUInt32,
                ("max", Some(CDT::UInt64)) => ScalarType::MaxUInt64,
                ("max", Some(CDT::UInt128)) => ScalarType::MaxUInt128,
                ("max", Some(CDT::UInt256)) => ScalarType::MaxUInt256,
                ("max", Some(CDT::Int8)) => ScalarType::MaxInt8,
                ("max", Some(CDT::Int16)) => ScalarType::MaxInt16,
                ("max", Some(CDT::Int32)) => ScalarType::MaxInt32,
                ("max", Some(CDT::Int64)) => ScalarType::MaxInt64,
                ("max", Some(CDT::Int128)) => ScalarType::MaxInt128,
                ("max", Some(CDT::Int256)) => ScalarType::MaxInt256,
                ("max", Some(CDT::Float32)) => ScalarType::MaxFloat32,
                ("max", Some(CDT::Float64)) => ScalarType::MaxFloat64,
                (
                    "max",
                    Some(
                        CDT::Decimal { .. }
                        | CDT::Decimal32 { .. }
                        | CDT::Decimal64 { .. }
                        | CDT::Decimal128 { .. }
                        | CDT::Decimal256 { .. },
                    ),
                ) => ScalarType::MaxDecimal,
                ("min", Some(CDT::UInt8)) => ScalarType::MinUInt8,
                ("min", Some(CDT::UInt16)) => ScalarType::MinUInt16,
                ("min", Some(CDT::UInt32)) => ScalarType::MinUInt32,
                ("min", Some(CDT::UInt64)) => ScalarType::MinUInt64,
                ("min", Some(CDT::UInt128)) => ScalarType::MinUInt128,
                ("min", Some(CDT::UInt256)) => ScalarType::MinUInt256,
                ("min", Some(CDT::Int8)) => ScalarType::MinInt8,
                ("min", Some(CDT::Int16)) => ScalarType::MinInt16,
                ("min", Some(CDT::Int32)) => ScalarType::MinInt32,
                ("min", Some(CDT::Int64)) => ScalarType::MinInt64,
                ("min", Some(CDT::Int128)) => ScalarType::MinInt128,
                ("min", Some(CDT::Int256)) => ScalarType::MinInt256,
                ("min", Some(CDT::Float32)) => ScalarType::MinFloat32,
                ("min", Some(CDT::Float64)) => ScalarType::MinFloat64,
                (
                    "min",
                    Some(
                        CDT::Decimal { .. }
                        | CDT::Decimal32 { .. }
                        | CDT::Decimal64 { .. }
                        | CDT::Decimal128 { .. }
                        | CDT::Decimal256 { .. },
                    ),
                ) => ScalarType::MinDecimal,
                ("max", Some(CDT::Date)) => ScalarType::MaxDate,
                ("max", Some(CDT::Date32)) => ScalarType::MaxDate32,
                ("max", Some(CDT::DateTime { .. })) => ScalarType::MaxDateTime,
                ("max", Some(CDT::DateTime64 { .. })) => ScalarType::MaxDateTime64,
                ("min", Some(CDT::Date)) => ScalarType::MinDate,
                ("min", Some(CDT::Date32)) => ScalarType::MinDate32,
                ("min", Some(CDT::DateTime { .. })) => ScalarType::MinDateTime,
                ("min", Some(CDT::DateTime64 { .. })) => ScalarType::MinDateTime64,

                _ => ScalarType::Unknown,
            }
        }
    };

    scalar_type
}
