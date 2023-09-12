pub mod schema;
use axum::{
    routing::{get, post},
    Router,
};
use axum_tracing_opentelemetry::middleware::{OtelAxumLayer, OtelInResponseLayer};
mod client;
mod config;
mod error;
mod routes;
use self::{error::ServerError, routes::*};
pub use config::Config;

pub fn router() -> Router {
    Router::new()
        .route("/capabilities", get(get_capabilities))
        .route("/schema", post(post_schema))
        .route("/query", post(post_query))
        .route("/mutation", post(post_mutation))
        .route("/raw", post(post_raw))
        .route("/explain", post(post_explain))
        .fallback(fallback)
        // include trace context as header into the response
        .layer(OtelInResponseLayer)
        //start OpenTelemetry trace on incoming request
        .layer(OtelAxumLayer::default())
        .route("/health", get(get_health)) // request processed without span / trace
}

#[axum_macros::debug_handler]
async fn fallback(uri: axum::http::Uri) -> impl axum::response::IntoResponse {
    ServerError::NotFound(uri)
}
