use std::{str::FromStr, vec};

use axum::Json;
use axum_extra::extract::WithRejection;
use indexmap::IndexMap;

use crate::server::{
    api::{raw_request::RawRequest, raw_response::RawResponse},
    client::execute_clickhouse_request,
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

    let response = execute_clickhouse_request(&config, query).await?;

    let response_json = serde_json::Value::from_str(&response)?;
    let row: IndexMap<String, serde_json::Value> =
        IndexMap::from_iter((0..1).map(|i| (i.to_string(), response_json.clone())));
    let rows = vec![row];

    let response = RawResponse { rows };

    Ok(Json(response))
}
