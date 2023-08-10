use std::collections::HashMap;
use std::error::Error;

use axum::{
    async_trait,
    extract::FromRequestParts,
    http::{request::Parts, HeaderName, StatusCode},
};
use openapi_type::openapiv3::ReferenceOr;
use openapi_type::{OpenapiSchema, OpenapiType};
use openapiv3_visit::VisitMut;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_with::skip_serializing_none;

use super::api::capabilities_response::ConfigSchemaResponse;

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, OpenapiType)]
pub struct Config {
    /// The url for your clickhouse database
    pub url: String,
    /// The clickhouse user name
    pub username: String,
    /// The clickhouse password
    pub password: String,
    /// Optional additional configuration for tables
    pub tables: Option<Vec<TableConfig>>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, OpenapiType)]
pub struct TableConfig {
    /// The table name
    pub name: String,
    /// Optional alias for this table. Required if the table name is not a valid graphql name
    pub alias: Option<String>,
    /// Optional configuration for table columns
    pub columns: Option<Vec<ColumnConfig>>,
}

#[skip_serializing_none]
#[derive(Debug, Clone, Serialize, Deserialize, OpenapiType)]
pub struct ColumnConfig {
    /// The column name
    pub name: String,
    /// Optional alias for this column. Required if the column name is not a valid graphql name
    pub alias: Option<String>,
}

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

struct RenamedSchema;

impl<'openapi> VisitMut<'openapi> for RenamedSchema {
    fn visit_reference_or_schema_mut(
        &mut self,
        node: &'openapi mut ReferenceOr<openapiv3::Schema>,
    ) {
        match node {
            ReferenceOr::Reference { reference } => {
                *reference = reference.replace("#/components/schemas", "#/other_schemas");
            }
            ReferenceOr::Item(_) => {}
        };
    }
    fn visit_reference_or_box_schema_mut(
        &mut self,
        node: &'openapi mut ReferenceOr<Box<openapiv3::Schema>>,
    ) {
        match node {
            ReferenceOr::Reference { reference } => {
                *reference = reference.replace("#/components/schemas", "#/other_schemas");
            }
            ReferenceOr::Item(_) => {}
        };
    }
}

pub fn get_openapi_config_schema_response() -> ConfigSchemaResponse {
    // let OpenapiSchema {
    //     schema,
    //     dependencies,
    //     ..
    // } = Config::schema();

    // let mut config_schema = schema;

    // RenamedSchema.visit_schema_mut(&mut config_schema);

    // let other_schemas = HashMap::from_iter(dependencies.into_iter().filter_map(|(dependency_name, schema)| {
    //     let OpenapiSchema {
    //         schema: mut other_schema,
    //         dependencies,
    //         ..
    //     } = schema;

    //     assert!(dependencies.is_empty(), "It's my understanding dependencies should always be empty for items already in dependencies");

    //     if config_schema.schema_data.title.as_ref().is_some_and(|config_schema_name| config_schema_name == &dependency_name) {
    //         None
    //     } else {
    //         RenamedSchema.visit_schema_mut(&mut other_schema);
    //         Some((dependency_name, other_schema))
    //     }
    // }));

    // ConfigSchemaResponse {
    //     config_schema,
    //     other_schemas,
    // }

    // above code generate config schema from config type.
    // however, we cannot specify fields as nullable using these libraries, so hardcoding the config schema
    // TODO: if config type is changed, config schema should be changed also.

    let config_schema_json = json!({
        "type": "object",
        "nullable": false,
        "properties": {
            "url": {
                "title": "URL",
                "description": "The url for your clickhouse database",
                "nullable": false,
                "type": "string"
            },
            "username": {
                "title": "Username",
                "description": "The clickhouse user name",
                "nullable": false,
                "type": "string"
            },
            "password": {
                "title": "Password",
                "description": "The clickhouse password",
                "nullable": false,
                "type": "string"
            },
            "tables": {
                "title": "Tables",
                "description": "Optional additional configuration for tables",
                "nullable": true,
                "type": "array",
                "items": {
                    "type": "object",
                    "nullable": true,
                    "properties": {
                        "name": {
                            "title": "Name",
                            "description": "The table name",
                            "nullable": false,
                            "type": "string"
                        },
                        "alias": {
                            "title": "Alias",
                            "description": "Optional alias for this table. Required if the table name is not a valid graphql name",
                            "nullable": true,
                            "type": "string"
                        },
                        "columns": {
                            "description": "Optional configuration for table columns",
                            "type": "array",
                            "nullable": true,
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {
                                        "title": "Name",
                                        "description": "The column name",
                                        "nullable": false,
                                        "type": "string"
                                    },
                                    "alias": {
                                        "title": "Alias",
                                        "description": "Optional alias for this column. Required if the column name is not a valid graphql name",
                                        "nullable": true,
                                        "type": "string"
                                    }
                                },
                                "required": ["name"]
                            }
                        }
                    },
                    "required": ["name"]
                }
            }
        },
        "required": ["url", "username", "password"]
    });

    ConfigSchemaResponse {
        config_schema: serde_json::from_value(config_schema_json).unwrap(),
        other_schemas: serde_json::from_str("{}").unwrap(),
    }
}

#[test]
fn generate_config_schema() -> Result<(), Box<dyn Error>> {
    let schema = serde_json::to_string(&get_openapi_config_schema_response());

    assert!(schema.is_ok(), "can generate config schema");
    Ok(())
}
