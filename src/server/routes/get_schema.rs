use std::{error::Error, str::FromStr};

use axum::Json;
use serde::{Deserialize, Serialize};
use tracing::{info_span, Instrument};
mod clickhouse_data_type;

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
            function,
            arguments,
        }
        | CDT::AggregateFunction {
            function,
            arguments,
        } => {
            if let Some(data_type) = arguments.first() {
                get_scalar_type(data_type)
            } else {
                ScalarType::Unknown
            }
        }
    };

    scalar_type
}
