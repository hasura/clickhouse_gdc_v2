use axum::{
    http::{header, StatusCode},
    response::IntoResponse,
};

use crate::server::config::{SourceConfig, SourceName};

#[axum_macros::debug_handler]
pub async fn get_health(
    _source_name: Option<SourceName>,
    _config: Option<SourceConfig>,
) -> impl IntoResponse {
    // todo: if source_name and config provided, check if that specific source is healthy

    (
        StatusCode::NO_CONTENT,
        [(header::CONTENT_TYPE, "text/html")],
    )
}
