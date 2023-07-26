use axum::Json;
use indexmap::IndexMap;
use schemars::schema_for;

use crate::server::{
    api::{
        capabilities_response::{
            Capabilities, CapabilitiesResponse, ColumnNullability, ComparisonCapabilities,
            ConfigSchemaResponse, DataSchemaCapabilities, GraphQlType, QueryCapabilities,
            ScalarTypeCapabilities, SubqueryComparisonCapabilities,
        },
        query_request::{ScalarType, SingleColumnAggregateFunction},
    },
    config::Config,
};

#[axum_macros::debug_handler]
pub async fn get_capabilities() -> Json<CapabilitiesResponse> {
    Json(CapabilitiesResponse {
        display_name: Some("Rust Data Connector".to_owned()),
        release_name: Some("0.1.0".to_string()),
        config_schemas: ConfigSchemaResponse {
            config_schema: schema_for!(Config),
            other_schemas: IndexMap::new(),
        },
        capabilities: Capabilities {
            comparisons: Some(ComparisonCapabilities {
                // subquery: None,
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
                // foreach: None,
            }),
            raw: Some(serde_json::Value::Object(serde_json::Map::new())),
        },
    })
}

fn scalar_types() -> IndexMap<ScalarType, ScalarTypeCapabilities> {
    use ScalarType::*;
    use SingleColumnAggregateFunction::*;
    IndexMap::from_iter(
        vec![
            Bool,
            String,
            FixedString,
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
            Date,
            Date32,
            DateTime,
            DateTime64,
            Json,
            Uuid,
            IPv4,
            IPv6,
        ]
        .into_iter()
        .map(|scalar_type| {
            let graphql_type = match &scalar_type {
                Bool => GraphQlType::Boolean,
                String => GraphQlType::String,
                FixedString => GraphQlType::String,
                UInt8 | UInt16 | UInt32 => GraphQlType::Int,
                UInt64 | UInt128 | UInt256 => GraphQlType::String,
                Int8 | Int16 | Int32 => GraphQlType::Int,
                Int64 | Int128 | Int256 => GraphQlType::String,
                Float32 => GraphQlType::Float,
                Float64 => GraphQlType::Float,
                Date | Date32 | DateTime | DateTime64 => GraphQlType::String,
                Json => GraphQlType::String,
                Uuid => GraphQlType::String,
                IPv4 => GraphQlType::String,
                IPv6 => GraphQlType::String,
            };
            let aggregate_functions = match &scalar_type {
                Bool => None,
                String | FixedString => Some(IndexMap::from_iter(vec![
                    (Longest, String),
                    (Shortest, String),
                    (Max, String),
                    (Min, String),
                ])),
                UInt8 => Some(IndexMap::from_iter(vec![
                    (Max, UInt8),
                    (Min, UInt8),
                    (StddevPop, Float64),
                    (StddevSamp, Float64),
                    (Sum, UInt64),
                    (VarPop, Float64),
                    (VarSamp, Float64),
                ])),
                UInt16 => Some(IndexMap::from_iter(vec![
                    (Max, UInt16),
                    (Min, UInt16),
                    (StddevPop, Float64),
                    (StddevSamp, Float64),
                    (Sum, UInt64),
                    (VarPop, Float64),
                    (VarSamp, Float64),
                ])),
                UInt32 => Some(IndexMap::from_iter(vec![
                    (Max, UInt32),
                    (Min, UInt32),
                    (StddevPop, Float64),
                    (StddevSamp, Float64),
                    (Sum, UInt64),
                    (VarPop, Float64),
                    (VarSamp, Float64),
                ])),
                UInt64 => Some(IndexMap::from_iter(vec![
                    (Max, UInt64),
                    (Min, UInt64),
                    (StddevPop, Float64),
                    (StddevSamp, Float64),
                    (Sum, UInt64),
                    (VarPop, Float64),
                    (VarSamp, Float64),
                ])),
                UInt128 => Some(IndexMap::from_iter(vec![
                    (Max, UInt128),
                    (Min, UInt128),
                    (StddevPop, Float64),
                    (StddevSamp, Float64),
                    (Sum, UInt128),
                    (VarPop, Float64),
                    (VarSamp, Float64),
                ])),
                UInt256 => Some(IndexMap::from_iter(vec![
                    (Max, UInt256),
                    (Min, UInt256),
                    (StddevPop, Float64),
                    (StddevSamp, Float64),
                    (Sum, UInt256),
                    (VarPop, Float64),
                    (VarSamp, Float64),
                ])),
                Int8 => Some(IndexMap::from_iter(vec![
                    (Max, Int8),
                    (Min, Int8),
                    (StddevPop, Float64),
                    (StddevSamp, Float64),
                    (Sum, Int64),
                    (VarPop, Float64),
                    (VarSamp, Float64),
                ])),
                Int16 => Some(IndexMap::from_iter(vec![
                    (Max, Int16),
                    (Min, Int16),
                    (StddevPop, Float64),
                    (StddevSamp, Float64),
                    (Sum, Int64),
                    (VarPop, Float64),
                    (VarSamp, Float64),
                ])),
                Int32 => Some(IndexMap::from_iter(vec![
                    (Max, Int32),
                    (Min, Int32),
                    (StddevPop, Float64),
                    (StddevSamp, Float64),
                    (Sum, Int64),
                    (VarPop, Float64),
                    (VarSamp, Float64),
                ])),
                Int64 => Some(IndexMap::from_iter(vec![
                    (Max, Int64),
                    (Min, Int64),
                    (StddevPop, Float64),
                    (StddevSamp, Float64),
                    (Sum, Int64),
                    (VarPop, Float64),
                    (VarSamp, Float64),
                ])),
                Int128 => Some(IndexMap::from_iter(vec![
                    (Max, Int128),
                    (Min, Int128),
                    (StddevPop, Float64),
                    (StddevSamp, Float64),
                    (Sum, Int128),
                    (VarPop, Float64),
                    (VarSamp, Float64),
                ])),
                Int256 => Some(IndexMap::from_iter(vec![
                    (Max, Int256),
                    (Min, Int256),
                    (StddevPop, Float64),
                    (StddevSamp, Float64),
                    (Sum, Int256),
                    (VarPop, Float64),
                    (VarSamp, Float64),
                ])),
                Float32 => Some(IndexMap::from_iter(vec![
                    (Max, Float32),
                    (Min, Float32),
                    (StddevPop, Float32),
                    (StddevSamp, Float32),
                    (Sum, Float64),
                    (VarPop, Float64),
                    (VarSamp, Float64),
                ])),
                Float64 => Some(IndexMap::from_iter(vec![
                    (Max, Float64),
                    (Min, Float64),
                    (StddevPop, Float64),
                    (StddevSamp, Float64),
                    (Sum, Float64),
                    (VarPop, Float64),
                    (VarSamp, Float64),
                ])),
                Date => Some(IndexMap::from_iter(vec![(Max, Date), (Min, Date)])),
                Date32 => Some(IndexMap::from_iter(vec![(Max, Date32), (Min, Date32)])),
                DateTime => Some(IndexMap::from_iter(vec![(Max, DateTime), (Min, DateTime)])),
                DateTime64 => Some(IndexMap::from_iter(vec![
                    (Max, DateTime64),
                    (Min, DateTime64),
                ])),
                Json => None,
                Uuid => None,
                IPv4 => None,
                IPv6 => None,
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
