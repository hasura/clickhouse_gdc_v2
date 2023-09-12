use axum::Json;
use indexmap::IndexMap;

use gdc_rust_types::{
    Capabilities, CapabilitiesResponse, ColumnNullability, ComparisonCapabilities,
    DataSchemaCapabilities, GraphQlType, QueryCapabilities, ScalarTypeCapabilities,
    SubqueryComparisonCapabilities,
};
use strum::IntoEnumIterator;

use crate::server::{
    config::get_openapi_config_schema_response,
    schema::{ScalarType, SingleColumnAggregateFunction},
};

#[axum_macros::debug_handler]
pub async fn get_capabilities() -> Json<CapabilitiesResponse> {
    let package_version: &'static str = env!("CARGO_PKG_VERSION");
    let capabilities = CapabilitiesResponse {
        display_name: Some("Clickhouse".to_owned()),
        release_name: Some(package_version.to_string()),
        config_schemas: get_openapi_config_schema_response(),
        capabilities: Capabilities {
            licensing: None,
            comparisons: Some(ComparisonCapabilities {
                subquery: Some(SubqueryComparisonCapabilities {
                    supports_relations: Some(true),
                }),
            }),
            data_schema: Some(DataSchemaCapabilities {
                column_nullability: Some(ColumnNullability::NullableAndNonNullable),
                supports_schemaless_tables: None,
                supports_foreign_keys: Some(false),
                supports_primary_keys: Some(true),
            }),
            datasets: None,
            explain: Some(serde_json::Value::Object(serde_json::Map::new())),
            metrics: None,
            relationships: Some(serde_json::Value::Object(serde_json::Map::new())),
            scalar_types: Some(scalar_types()),
            subscriptions: None,
            mutations: None,
            queries: Some(QueryCapabilities {
                foreach: Some(serde_json::Value::Object(serde_json::Map::new())),
            }),
            raw: Some(serde_json::Value::Object(serde_json::Map::new())),
            user_defined_functions: None,
            interpolated_queries: None,
        },
    };
    Json(capabilities)
}

fn scalar_types() -> IndexMap<String, ScalarTypeCapabilities> {
    use ScalarType as ST;
    use SingleColumnAggregateFunction as CA;
    IndexMap::from_iter(ST::iter().map(|scalar_type| {
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
            ST::AvgUInt8 => GraphQlType::String,
            ST::AvgUInt16 => GraphQlType::String,
            ST::AvgUInt32 => GraphQlType::String,
            ST::AvgUInt64 => GraphQlType::String,
            ST::AvgUInt128 => GraphQlType::String,
            ST::AvgUInt256 => GraphQlType::String,
            ST::AvgInt8 => GraphQlType::String,
            ST::AvgInt16 => GraphQlType::String,
            ST::AvgInt32 => GraphQlType::String,
            ST::AvgInt64 => GraphQlType::String,
            ST::AvgInt128 => GraphQlType::String,
            ST::AvgInt256 => GraphQlType::String,
            ST::AvgFloat32 => GraphQlType::String,
            ST::AvgFloat64 => GraphQlType::String,
            ST::AvgDecimal => GraphQlType::String,
            ST::SumUInt8 => GraphQlType::String,
            ST::SumUInt16 => GraphQlType::String,
            ST::SumUInt32 => GraphQlType::String,
            ST::SumUInt64 => GraphQlType::String,
            ST::SumUInt128 => GraphQlType::String,
            ST::SumUInt256 => GraphQlType::String,
            ST::SumInt8 => GraphQlType::String,
            ST::SumInt16 => GraphQlType::String,
            ST::SumInt32 => GraphQlType::String,
            ST::SumInt64 => GraphQlType::String,
            ST::SumInt128 => GraphQlType::String,
            ST::SumInt256 => GraphQlType::String,
            ST::SumFloat32 => GraphQlType::String,
            ST::SumFloat64 => GraphQlType::String,
            ST::SumDecimal => GraphQlType::String,
            ST::MaxUInt8 => GraphQlType::String,
            ST::MaxUInt16 => GraphQlType::String,
            ST::MaxUInt32 => GraphQlType::String,
            ST::MaxUInt64 => GraphQlType::String,
            ST::MaxUInt128 => GraphQlType::String,
            ST::MaxUInt256 => GraphQlType::String,
            ST::MaxInt8 => GraphQlType::String,
            ST::MaxInt16 => GraphQlType::String,
            ST::MaxInt32 => GraphQlType::String,
            ST::MaxInt64 => GraphQlType::String,
            ST::MaxInt128 => GraphQlType::String,
            ST::MaxInt256 => GraphQlType::String,
            ST::MaxFloat32 => GraphQlType::String,
            ST::MaxFloat64 => GraphQlType::String,
            ST::MaxDecimal => GraphQlType::String,
            ST::MinUInt8 => GraphQlType::String,
            ST::MinUInt16 => GraphQlType::String,
            ST::MinUInt32 => GraphQlType::String,
            ST::MinUInt64 => GraphQlType::String,
            ST::MinUInt128 => GraphQlType::String,
            ST::MinUInt256 => GraphQlType::String,
            ST::MinInt8 => GraphQlType::String,
            ST::MinInt16 => GraphQlType::String,
            ST::MinInt32 => GraphQlType::String,
            ST::MinInt64 => GraphQlType::String,
            ST::MinInt128 => GraphQlType::String,
            ST::MinInt256 => GraphQlType::String,
            ST::MinFloat32 => GraphQlType::String,
            ST::MinFloat64 => GraphQlType::String,
            ST::MinDecimal => GraphQlType::String,
            ST::MaxDate => GraphQlType::String,
            ST::MaxDate32 => GraphQlType::String,
            ST::MaxDateTime => GraphQlType::String,
            ST::MaxDateTime64 => GraphQlType::String,
            ST::MinDate => GraphQlType::String,
            ST::MinDate32 => GraphQlType::String,
            ST::MinDateTime => GraphQlType::String,
            ST::MinDateTime64 => GraphQlType::String,
        };
        let aggregate_functions: Option<IndexMap<SingleColumnAggregateFunction, ScalarType>> =
            match &scalar_type {
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
                ST::AvgUInt8
                | ST::AvgUInt16
                | ST::AvgUInt32
                | ST::AvgUInt64
                | ST::AvgUInt128
                | ST::AvgUInt256
                | ST::AvgInt8
                | ST::AvgInt16
                | ST::AvgInt32
                | ST::AvgInt64
                | ST::AvgInt128
                | ST::AvgInt256
                | ST::AvgFloat32
                | ST::AvgFloat64
                | ST::AvgDecimal => Some(IndexMap::from_iter(vec![(CA::AvgMerge, ST::Float64)])),
                ST::SumUInt8 => Some(IndexMap::from_iter(vec![(CA::SumMerge, ST::UInt64)])),
                ST::SumUInt16 => Some(IndexMap::from_iter(vec![(CA::SumMerge, ST::UInt64)])),
                ST::SumUInt32 => Some(IndexMap::from_iter(vec![(CA::SumMerge, ST::UInt64)])),
                ST::SumUInt64 => Some(IndexMap::from_iter(vec![(CA::SumMerge, ST::UInt64)])),
                ST::SumUInt128 => Some(IndexMap::from_iter(vec![(CA::SumMerge, ST::UInt128)])),
                ST::SumUInt256 => Some(IndexMap::from_iter(vec![(CA::SumMerge, ST::UInt256)])),
                ST::SumInt8 => Some(IndexMap::from_iter(vec![(CA::SumMerge, ST::UInt64)])),
                ST::SumInt16 => Some(IndexMap::from_iter(vec![(CA::SumMerge, ST::UInt64)])),
                ST::SumInt32 => Some(IndexMap::from_iter(vec![(CA::SumMerge, ST::UInt64)])),
                ST::SumInt64 => Some(IndexMap::from_iter(vec![(CA::SumMerge, ST::UInt64)])),
                ST::SumInt128 => Some(IndexMap::from_iter(vec![(CA::SumMerge, ST::UInt128)])),
                ST::SumInt256 => Some(IndexMap::from_iter(vec![(CA::SumMerge, ST::UInt256)])),
                ST::SumFloat32 => Some(IndexMap::from_iter(vec![(CA::SumMerge, ST::Float64)])),
                ST::SumFloat64 => Some(IndexMap::from_iter(vec![(CA::SumMerge, ST::Float64)])),
                ST::SumDecimal => Some(IndexMap::from_iter(vec![(CA::SumMerge, ST::Decimal)])),
                ST::MaxUInt8 => Some(IndexMap::from_iter(vec![(CA::MaxMerge, ST::MaxUInt8)])),
                ST::MaxUInt16 => Some(IndexMap::from_iter(vec![(CA::MaxMerge, ST::MaxUInt16)])),
                ST::MaxUInt32 => Some(IndexMap::from_iter(vec![(CA::MaxMerge, ST::MaxUInt32)])),
                ST::MaxUInt64 => Some(IndexMap::from_iter(vec![(CA::MaxMerge, ST::MaxUInt64)])),
                ST::MaxUInt128 => Some(IndexMap::from_iter(vec![(CA::MaxMerge, ST::MaxUInt128)])),
                ST::MaxUInt256 => Some(IndexMap::from_iter(vec![(CA::MaxMerge, ST::MaxUInt256)])),
                ST::MaxInt8 => Some(IndexMap::from_iter(vec![(CA::MaxMerge, ST::MaxInt8)])),
                ST::MaxInt16 => Some(IndexMap::from_iter(vec![(CA::MaxMerge, ST::MaxInt16)])),
                ST::MaxInt32 => Some(IndexMap::from_iter(vec![(CA::MaxMerge, ST::MaxInt32)])),
                ST::MaxInt64 => Some(IndexMap::from_iter(vec![(CA::MaxMerge, ST::MaxInt64)])),
                ST::MaxInt128 => Some(IndexMap::from_iter(vec![(CA::MaxMerge, ST::MaxInt128)])),
                ST::MaxInt256 => Some(IndexMap::from_iter(vec![(CA::MaxMerge, ST::MaxInt256)])),
                ST::MaxFloat32 => Some(IndexMap::from_iter(vec![(CA::MaxMerge, ST::MaxFloat32)])),
                ST::MaxFloat64 => Some(IndexMap::from_iter(vec![(CA::MaxMerge, ST::MaxFloat64)])),
                ST::MaxDecimal => Some(IndexMap::from_iter(vec![(CA::MaxMerge, ST::MaxDecimal)])),
                ST::MinUInt8 => Some(IndexMap::from_iter(vec![(CA::MinMerge, ST::MinUInt8)])),
                ST::MinUInt16 => Some(IndexMap::from_iter(vec![(CA::MinMerge, ST::MinUInt16)])),
                ST::MinUInt32 => Some(IndexMap::from_iter(vec![(CA::MinMerge, ST::MinUInt32)])),
                ST::MinUInt64 => Some(IndexMap::from_iter(vec![(CA::MinMerge, ST::MinUInt64)])),
                ST::MinUInt128 => Some(IndexMap::from_iter(vec![(CA::MinMerge, ST::MinUInt128)])),
                ST::MinUInt256 => Some(IndexMap::from_iter(vec![(CA::MinMerge, ST::MinUInt256)])),
                ST::MinInt8 => Some(IndexMap::from_iter(vec![(CA::MinMerge, ST::MinInt8)])),
                ST::MinInt16 => Some(IndexMap::from_iter(vec![(CA::MinMerge, ST::MinInt16)])),
                ST::MinInt32 => Some(IndexMap::from_iter(vec![(CA::MinMerge, ST::MinInt32)])),
                ST::MinInt64 => Some(IndexMap::from_iter(vec![(CA::MinMerge, ST::MinInt64)])),
                ST::MinInt128 => Some(IndexMap::from_iter(vec![(CA::MinMerge, ST::MinInt128)])),
                ST::MinInt256 => Some(IndexMap::from_iter(vec![(CA::MinMerge, ST::MinInt256)])),
                ST::MinFloat32 => Some(IndexMap::from_iter(vec![(CA::MinMerge, ST::MinFloat32)])),
                ST::MinFloat64 => Some(IndexMap::from_iter(vec![(CA::MinMerge, ST::MinFloat64)])),
                ST::MinDecimal => Some(IndexMap::from_iter(vec![(CA::MinMerge, ST::MinDecimal)])),
                ST::MaxDate => Some(IndexMap::from_iter(vec![(CA::MaxMerge, ST::MaxDate)])),
                ST::MaxDate32 => Some(IndexMap::from_iter(vec![(CA::MaxMerge, ST::MaxDate32)])),
                ST::MaxDateTime => Some(IndexMap::from_iter(vec![(CA::MaxMerge, ST::MaxDateTime)])),
                ST::MaxDateTime64 => {
                    Some(IndexMap::from_iter(vec![(CA::MaxMerge, ST::MaxDateTime64)]))
                }
                ST::MinDate => Some(IndexMap::from_iter(vec![(CA::MinMerge, ST::MinDate)])),
                ST::MinDate32 => Some(IndexMap::from_iter(vec![(CA::MinMerge, ST::MinDate32)])),
                ST::MinDateTime => Some(IndexMap::from_iter(vec![(CA::MinMerge, ST::MinDateTime)])),
                ST::MinDateTime64 => {
                    Some(IndexMap::from_iter(vec![(CA::MinMerge, ST::MinDateTime64)]))
                }
                ST::Json => None,
                ST::Uuid => None,
                ST::IPv4 => None,
                ST::IPv6 => None,
                ST::Unknown => None,
            };
        let aggregate_functions = aggregate_functions.map(|aggregate_functions| {
            IndexMap::from_iter(aggregate_functions.into_iter().map(
                |(aggregate_function, result_type)| {
                    (aggregate_function.to_string(), result_type.to_string())
                },
            ))
        });
        let comparison_operators = Some(IndexMap::from_iter(vec![]));
        let scalar_type_capabilities = ScalarTypeCapabilities {
            graphql_type: Some(graphql_type),
            aggregate_functions,
            comparison_operators,
            update_column_operators: None,
        };

        (scalar_type.to_string(), scalar_type_capabilities)
    }))
}
