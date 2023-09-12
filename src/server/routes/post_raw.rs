use axum::Json;
use axum_extra::extract::WithRejection;
use gdc_rust_types::{RawRequest, RawResponse};
use indexmap::IndexMap;

use crate::server::{
    client::execute_query,
    config::{SourceConfig, SourceName},
    error::ServerError,
};

#[axum_macros::debug_handler]
pub async fn post_raw(
    SourceName(_source_name): SourceName,
    SourceConfig(config): SourceConfig,
    WithRejection(Json(request), _): WithRejection<Json<RawRequest>, ServerError>,
) -> Result<Json<RawResponse>, ServerError> {
    let query = request.query;

    let query = if query.contains("FORMAT JSON;") {
        query
    } else if query.contains(';') {
        query.replace(';', " FORMAT JSON;")
    } else {
        format!("{query} FORMAT JSON;")
    };

    let rows: Vec<IndexMap<String, serde_json::Value>> =
        execute_query(&config, &query, &vec![]).await?;

    let response = RawResponse { rows };

    Ok(Json(response))
}
