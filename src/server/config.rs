use schemars::schema::{RootSchema, SchemaObject};
use schemars::visit::{visit_schema_object, Visitor};
use schemars::{schema_for, JsonSchema};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Config {
    /// The url for your clickhouse database
    pub url: String,
    /// The clickhouse user name
    pub username: String,
    /// The clickhouse password
    pub password: String,
    /// Optional additional configuration for tables
    pub tables: Vec<TableConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TableConfig {
    /// The table name
    pub name: String,
    /// Optional alias for this table. Required if the table name is not a valid graphql name
    pub alias: String,
    /// Optional configuration for table columns
    pub columns: Vec<ColumnConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ColumnConfig {
    /// The column name
    pub name: String,
    /// Optional alias for this column. Required if the column name is not a valid graphql name
    pub alias: String,
}
use axum::{
    async_trait,
    extract::FromRequestParts,
    http::{request::Parts, HeaderName, StatusCode},
};

use super::api::capabilities_response::ConfigSchemaResponse;

static CONFIG_HEADER: HeaderName = HeaderName::from_static("x-hasura-dataconnector-config");
static SOURCE_HEADER: HeaderName = HeaderName::from_static("x-hasura-dataconnector-sourcename");

#[derive(Debug)]
pub struct SourceName(pub String);
#[derive(Debug)]
pub struct SourceConfig(pub Config);

#[async_trait]
impl<S: Send + Sync> FromRequestParts<S> for SourceName {
    type Rejection = StatusCode;
    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        if let Some(source_header) = parts.headers.get(&SOURCE_HEADER) {
            let source_name = source_header
                .to_str()
                .map_err(|_err| StatusCode::BAD_REQUEST)?;
            Ok(Self(source_name.to_owned()))
        } else {
            Err(StatusCode::BAD_REQUEST)
        }
    }
}

#[async_trait]
impl<S: Send + Sync> FromRequestParts<S> for SourceConfig {
    type Rejection = StatusCode;
    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        if let Some(config_header) = parts.headers.get(&CONFIG_HEADER) {
            let config: Config = serde_json::from_slice(config_header.as_bytes())
                .map_err(|_err| StatusCode::BAD_REQUEST)?;
            Ok(Self(config))
        } else {
            Err(StatusCode::BAD_REQUEST)
        }
    }
}

struct RenameReferences;

impl Visitor for RenameReferences {
    fn visit_schema_object(&mut self, schema: &mut SchemaObject) {
        // rename all references to point to `#/other_schemas`, per the GDC spec: https://github.com/hasura/graphql-engine-mono/blob/main/dc-agents/DOCUMENTATION.md#capabilities-and-configuration-schema
        schema.reference = schema
            .reference
            .as_mut()
            .map(|reference| reference.replace("#/definitions", "#/other_schemas"));

        visit_schema_object(self, schema);
    }
}

pub fn get_config_schema_response() -> ConfigSchemaResponse {
    let mut schema = schema_for!(Config);
    let mut visitor = RenameReferences;

    visitor.visit_root_schema(&mut schema);

    let RootSchema {
        schema: config_schema,
        meta_schema: _,
        definitions: other_schemas,
    } = schema;

    ConfigSchemaResponse {
        config_schema,
        other_schemas,
    }
}
