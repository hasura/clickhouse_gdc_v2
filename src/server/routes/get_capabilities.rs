use axum::Json;
use indexmap::IndexMap;

use crate::server::{
    api::{
        capabilities_response::{
            Capabilities, CapabilitiesResponse, ColumnNullability, ComparisonCapabilities,
            DataSchemaCapabilities, GraphQlType, QueryCapabilities, ScalarTypeCapabilities,
            SubqueryComparisonCapabilities,
        },
        query_request::{ScalarType, SingleColumnAggregateFunction},
    },
    config::get_openapi_config_schema_response,
};

#[axum_macros::debug_handler]
pub async fn get_capabilities() -> Json<CapabilitiesResponse> {
    Json(CapabilitiesResponse {
        display_name: Some("Clickhouse".to_owned()),
        release_name: Some("Beta".to_string()),
        config_schemas: get_openapi_config_schema_response(),
        capabilities: Capabilities {
            comparisons: Some(ComparisonCapabilities {
                subquery: Some(SubqueryComparisonCapabilities {
                    supports_relations: Some(true),
                }),
            }),
            data_schema: Some(DataSchemaCapabilities {
                column_nullability: Some(ColumnNullability::NullableAndNonNullable),
                supports_foreign_keys: Some(false),
                supports_primary_keys: Some(true),
            }),
            datasets: None,
            explain: Some(serde_json::Value::Object(serde_json::Map::new())),
            metrics: None,
            relationships: Some(serde_json::Value::Object(serde_json::Map::new())),
            scalar_types: scalar_types(),
            subscriptions: None,
            mutations: None,
            queries: Some(QueryCapabilities {
                foreach: Some(serde_json::Value::Object(serde_json::Map::new())),
            }),
            raw: Some(serde_json::Value::Object(serde_json::Map::new())),
        },
    })
}

fn scalar_types() -> IndexMap<ScalarType, ScalarTypeCapabilities> {
    use ScalarType as ST;
    use SingleColumnAggregateFunction as CA;
    IndexMap::from_iter(
        vec![
            ST::Bool,
            ST::String,
            ST::FixedString,
            ST::UInt8,
            ST::UInt16,
            ST::UInt32,
            ST::UInt64,
            ST::UInt128,
            ST::UInt256,
            ST::Int8,
            ST::Int16,
            ST::Int32,
            ST::Int64,
            ST::Int128,
            ST::Int256,
            ST::Float32,
            ST::Float64,
            ST::Decimal,
            ST::Date,
            ST::Date32,
            ST::DateTime,
            ST::DateTime64,
            ST::Json,
            ST::Uuid,
            ST::IPv4,
            ST::IPv6,
            ST::Unknown,
        ]
        .into_iter()
        .map(|scalar_type| {
            let graphql_type = match &scalar_type {
                ST::Bool => GraphQlType::Boolean,
                ST::String => GraphQlType::String,
                ST::FixedString => GraphQlType::String,
                ST::UInt8 | ST::UInt16 | ST::UInt32 => GraphQlType::Int,
                ST::UInt64 | ST::UInt128 | ST::UInt256 => GraphQlType::String,
                ST::Int8 | ST::Int16 | ST::Int32 => GraphQlType::Int,
                ST::Int64 | ST::Int128 | ST::Int256 => GraphQlType::String,
                ST::Float32 => GraphQlType::Float,
                ST::Float64 => GraphQlType::Float,
                ST::Decimal => GraphQlType::String,
                ST::Date | ST::Date32 | ST::DateTime | ST::DateTime64 => GraphQlType::String,
                ST::Json => GraphQlType::String,
                ST::Uuid => GraphQlType::String,
                ST::IPv4 => GraphQlType::String,
                ST::IPv6 => GraphQlType::String,
                ST::Unknown => GraphQlType::String,
            };
            let aggregate_functions = match &scalar_type {
                ST::Bool => None,
                ST::String | ST::FixedString => Some(IndexMap::from_iter(vec![
                    (CA::Longest, ST::String),
                    (CA::Shortest, ST::String),
                    (CA::Max, ST::String),
                    (CA::Min, ST::String),
                ])),
                ST::UInt8 => Some(IndexMap::from_iter(vec![
                    (CA::Avg, ST::Float64),
                    (CA::Max, ST::UInt8),
                    (CA::Min, ST::UInt8),
                    (CA::StddevPop, ST::Float64),
                    (CA::StddevSamp, ST::Float64),
                    (CA::Sum, ST::UInt64),
                    (CA::VarPop, ST::Float64),
                    (CA::VarSamp, ST::Float64),
                ])),
                ST::UInt16 => Some(IndexMap::from_iter(vec![
                    (CA::Avg, ST::Float64),
                    (CA::Max, ST::UInt16),
                    (CA::Min, ST::UInt16),
                    (CA::StddevPop, ST::Float64),
                    (CA::StddevSamp, ST::Float64),
                    (CA::Sum, ST::UInt64),
                    (CA::VarPop, ST::Float64),
                    (CA::VarSamp, ST::Float64),
                ])),
                ST::UInt32 => Some(IndexMap::from_iter(vec![
                    (CA::Avg, ST::Float64),
                    (CA::Max, ST::UInt32),
                    (CA::Min, ST::UInt32),
                    (CA::StddevPop, ST::Float64),
                    (CA::StddevSamp, ST::Float64),
                    (CA::Sum, ST::UInt64),
                    (CA::VarPop, ST::Float64),
                    (CA::VarSamp, ST::Float64),
                ])),
                ST::UInt64 => Some(IndexMap::from_iter(vec![
                    (CA::Avg, ST::Float64),
                    (CA::Max, ST::UInt64),
                    (CA::Min, ST::UInt64),
                    (CA::StddevPop, ST::Float64),
                    (CA::StddevSamp, ST::Float64),
                    (CA::Sum, ST::UInt64),
                    (CA::VarPop, ST::Float64),
                    (CA::VarSamp, ST::Float64),
                ])),
                ST::UInt128 => Some(IndexMap::from_iter(vec![
                    (CA::Avg, ST::Float64),
                    (CA::Max, ST::UInt128),
                    (CA::Min, ST::UInt128),
                    (CA::StddevPop, ST::Float64),
                    (CA::StddevSamp, ST::Float64),
                    (CA::Sum, ST::UInt128),
                    (CA::VarPop, ST::Float64),
                    (CA::VarSamp, ST::Float64),
                ])),
                ST::UInt256 => Some(IndexMap::from_iter(vec![
                    (CA::Avg, ST::Float64),
                    (CA::Max, ST::UInt256),
                    (CA::Min, ST::UInt256),
                    (CA::StddevPop, ST::Float64),
                    (CA::StddevSamp, ST::Float64),
                    (CA::Sum, ST::UInt256),
                    (CA::VarPop, ST::Float64),
                    (CA::VarSamp, ST::Float64),
                ])),
                ST::Int8 => Some(IndexMap::from_iter(vec![
                    (CA::Avg, ST::Float64),
                    (CA::Max, ST::Int8),
                    (CA::Min, ST::Int8),
                    (CA::StddevPop, ST::Float64),
                    (CA::StddevSamp, ST::Float64),
                    (CA::Sum, ST::Int64),
                    (CA::VarPop, ST::Float64),
                    (CA::VarSamp, ST::Float64),
                ])),
                ST::Int16 => Some(IndexMap::from_iter(vec![
                    (CA::Avg, ST::Float64),
                    (CA::Max, ST::Int16),
                    (CA::Min, ST::Int16),
                    (CA::StddevPop, ST::Float64),
                    (CA::StddevSamp, ST::Float64),
                    (CA::Sum, ST::Int64),
                    (CA::VarPop, ST::Float64),
                    (CA::VarSamp, ST::Float64),
                ])),
                ST::Int32 => Some(IndexMap::from_iter(vec![
                    (CA::Avg, ST::Float64),
                    (CA::Max, ST::Int32),
                    (CA::Min, ST::Int32),
                    (CA::StddevPop, ST::Float64),
                    (CA::StddevSamp, ST::Float64),
                    (CA::Sum, ST::Int64),
                    (CA::VarPop, ST::Float64),
                    (CA::VarSamp, ST::Float64),
                ])),
                ST::Int64 => Some(IndexMap::from_iter(vec![
                    (CA::Avg, ST::Float64),
                    (CA::Max, ST::Int64),
                    (CA::Min, ST::Int64),
                    (CA::StddevPop, ST::Float64),
                    (CA::StddevSamp, ST::Float64),
                    (CA::Sum, ST::Int64),
                    (CA::VarPop, ST::Float64),
                    (CA::VarSamp, ST::Float64),
                ])),
                ST::Int128 => Some(IndexMap::from_iter(vec![
                    (CA::Avg, ST::Float64),
                    (CA::Max, ST::Int128),
                    (CA::Min, ST::Int128),
                    (CA::StddevPop, ST::Float64),
                    (CA::StddevSamp, ST::Float64),
                    (CA::Sum, ST::Int128),
                    (CA::VarPop, ST::Float64),
                    (CA::VarSamp, ST::Float64),
                ])),
                ST::Int256 => Some(IndexMap::from_iter(vec![
                    (CA::Avg, ST::Float64),
                    (CA::Max, ST::Int256),
                    (CA::Min, ST::Int256),
                    (CA::StddevPop, ST::Float64),
                    (CA::StddevSamp, ST::Float64),
                    (CA::Sum, ST::Int256),
                    (CA::VarPop, ST::Float64),
                    (CA::VarSamp, ST::Float64),
                ])),
                ST::Float32 => Some(IndexMap::from_iter(vec![
                    (CA::Avg, ST::Float64),
                    (CA::Max, ST::Float32),
                    (CA::Min, ST::Float32),
                    (CA::StddevPop, ST::Float32),
                    (CA::StddevSamp, ST::Float32),
                    (CA::Sum, ST::Float64),
                    (CA::VarPop, ST::Float64),
                    (CA::VarSamp, ST::Float64),
                ])),
                ST::Float64 => Some(IndexMap::from_iter(vec![
                    (CA::Avg, ST::Float64),
                    (CA::Max, ST::Float64),
                    (CA::Min, ST::Float64),
                    (CA::StddevPop, ST::Float64),
                    (CA::StddevSamp, ST::Float64),
                    (CA::Sum, ST::Float64),
                    (CA::VarPop, ST::Float64),
                    (CA::VarSamp, ST::Float64),
                ])),
                ST::Decimal => Some(IndexMap::from_iter(vec![
                    (CA::Avg, ST::Float64),
                    (CA::Max, ST::Decimal),
                    (CA::Min, ST::Decimal),
                    (CA::StddevPop, ST::Decimal),
                    (CA::StddevSamp, ST::Decimal),
                    (CA::Sum, ST::Decimal),
                    (CA::VarPop, ST::Decimal),
                    (CA::VarSamp, ST::Decimal),
                ])),
                ST::Date => Some(IndexMap::from_iter(vec![
                    (CA::Max, ST::Date),
                    (CA::Min, ST::Date),
                ])),
                ST::Date32 => Some(IndexMap::from_iter(vec![
                    (CA::Max, ST::Date32),
                    (CA::Min, ST::Date32),
                ])),
                ST::DateTime => Some(IndexMap::from_iter(vec![
                    (CA::Max, ST::DateTime),
                    (CA::Min, ST::DateTime),
                ])),
                ST::DateTime64 => Some(IndexMap::from_iter(vec![
                    (CA::Max, ST::DateTime64),
                    (CA::Min, ST::DateTime64),
                ])),
                ST::Json => None,
                ST::Uuid => None,
                ST::IPv4 => None,
                ST::IPv6 => None,
                ST::Unknown => None,
            };
            let comparison_operators = Some(IndexMap::from_iter(vec![]));
            let scalar_type_capabilities = ScalarTypeCapabilities {
                graphql_type,
                aggregate_functions,
                comparison_operators,
                update_column_operators: None,
            };

            (scalar_type, scalar_type_capabilities)
        }),
    )
}
