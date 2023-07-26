use axum::{Json, TypedHeader};

use axum_extra::extract::WithRejection;
use serde::{Deserialize, Serialize};

use crate::server::{
    api::schema_response::SchemaResponse,
    client::execute_clickhouse_request,
    config::{SourceConfig, SourceName},
    error::ServerError,
};

#[axum_macros::debug_handler]
pub async fn get_schema(
    SourceName(source_name): SourceName,
    SourceConfig(config): SourceConfig,
) -> Result<Json<SchemaResponse>, ServerError> {
    let introspection_sql = include_str!("../database_introspection.sql");

    // todo: this is awful and needs cleanup/abstraction
    let response = execute_clickhouse_request(&config, introspection_sql.to_owned()).await?;

    let response: SchemaResponse = serde_json::from_str(&response)?;

    Ok(Json(response))
}
