use std::collections::HashMap;

use super::query_request::{BinaryComparisonOperator, ScalarType, SingleColumnAggregateFunction};
use indexmap::IndexMap;
use openapi_type::openapiv3::Schema;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[skip_serializing_none]
#[derive(Debug, Deserialize, Serialize)]
#[serde(bound = "")]
pub struct CapabilitiesResponse {
    pub capabilities: Capabilities,
    pub config_schemas: ConfigSchemaResponse,
    pub display_name: Option<String>,
    pub release_name: Option<String>,
}

// other_schema will always be empty, as we won't use references
#[derive(Debug, Deserialize, Serialize)]
pub struct ConfigSchemaResponse {
    pub config_schema: Schema,
    pub other_schemas: HashMap<String, Schema>,
}

#[skip_serializing_none]
#[derive(Debug, Deserialize, Serialize)]
#[serde(bound = "")]
pub struct Capabilities {
    pub comparisons: Option<ComparisonCapabilities>,
    pub data_schema: Option<DataSchemaCapabilities>,
    pub datasets: Option<serde_json::Value>,
    pub explain: Option<serde_json::Value>,
    pub metrics: Option<serde_json::Value>,
    pub mutations: Option<MutationCapabilities>,
    pub queries: Option<QueryCapabilities>,
    pub raw: Option<serde_json::Value>,
    pub relationships: Option<serde_json::Value>,
    /// A map from scalar type names to their capabilities. Keys must be valid GraphQL names and must be defined as scalar types in the `graphql_schema`
    pub scalar_types: IndexMap<ScalarType, ScalarTypeCapabilities>,
    pub subscriptions: Option<serde_json::Value>,
}

#[skip_serializing_none]
#[derive(Debug, Deserialize, Serialize)]
pub struct ComparisonCapabilities {
    pub subquery: Option<SubqueryComparisonCapabilities>,
}

#[skip_serializing_none]
#[derive(Debug, Deserialize, Serialize)]

pub struct DataSchemaCapabilities {
    pub column_nullability: Option<ColumnNullability>,
    /// Whether tables can have foreign keys
    pub supports_foreign_keys: Option<bool>,
    /// Whether tables can have primary keys
    pub supports_primary_keys: Option<bool>,
}

#[skip_serializing_none]
#[derive(Debug, Deserialize, Serialize)]
pub struct MutationCapabilities {
    pub atomicity_support_level: Option<AtomicitySupportLevel>,
    pub delete: Option<serde_json::Value>,
    pub insert: Option<Box<InsertCapabilities>>,
    pub returning: Option<serde_json::Value>,
    pub update: Option<serde_json::Value>,
}

#[skip_serializing_none]
#[derive(Debug, Deserialize, Serialize)]
pub struct QueryCapabilities {
    pub foreach: Option<serde_json::Value>,
}

#[skip_serializing_none]
#[derive(Debug, Deserialize, Serialize)]
#[serde(bound = "")]
pub struct ScalarTypeCapabilities {
    /// A map from aggregate function names to their result types. Function and result type names must be valid GraphQL names. Result type names must be defined scalar types declared in ScalarTypesCapabilities.
    pub aggregate_functions: Option<IndexMap<SingleColumnAggregateFunction, ScalarType>>,
    /// A map from comparison operator names to their argument types. Operator and argument type names must be valid GraphQL names. Argument type names must be defined scalar types declared in ScalarTypesCapabilities.
    pub comparison_operators: Option<IndexMap<BinaryComparisonOperator, ScalarType>>,
    pub graphql_type: GraphQlType,
    /// A map from update column operator names to their definitions. Operator names must be valid GraphQL names.
    pub update_column_operators: Option<IndexMap<String, UpdateColumnOperatorDefinition>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnNullability {
    OnlyNullable,
    NullableAndNonNullable,
}

#[derive(Debug, Deserialize, Serialize)]
pub enum GraphQlType {
    Int,
    Float,
    String,
    Boolean,
    ID,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AtomicitySupportLevel {
    Row,
    SingleOperation,
    HomogeneousOperations,
    HeterogeneousOperations,
}

#[skip_serializing_none]
#[derive(Debug, Deserialize, Serialize)]
pub struct SubqueryComparisonCapabilities {
    /// Does the agent support comparisons that involve related tables (ie. joins)?
    pub supports_relations: Option<bool>,
}

#[skip_serializing_none]
#[derive(Debug, Deserialize, Serialize)]
pub struct InsertCapabilities {
    /// Whether or not nested inserts to related tables are supported
    pub supports_nested_inserts: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct UpdateColumnOperatorDefinition {
    pub argument_type: String,
}
