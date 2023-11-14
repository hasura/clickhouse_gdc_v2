use axum::{http::StatusCode, response::IntoResponse};

use crate::server::{
    client::ping,
    config::{SourceConfig, SourceName},
};

#[axum_macros::debug_handler]
pub async fn get_health(
    _source_name: Option<SourceName>,
    config: Option<SourceConfig>,
) -> impl IntoResponse {
    // todo: if source_name and config provided, check if that specific source is healthy

    if let Some(SourceConfig(config)) = config {
        if ping(&config).await.is_ok() {
            StatusCode::NO_CONTENT
        } else {
            StatusCode::GATEWAY_TIMEOUT
        }
    } else {
        StatusCode::NO_CONTENT
    }
}
