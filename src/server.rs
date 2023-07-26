pub mod api;

use axum::{
    routing::{get, post},
    Router,
};

mod client;
mod config;
mod error;
mod routes;
use self::routes::*;

pub fn router() -> Router {
    Router::new()
        .route("/capabilities", get(get_capabilities))
        .route("/schema", get(get_schema))
        .route("/query", post(post_query))
        .route("/mutation", post(post_mutation))
        .route("/raw", post(post_raw))
        .route("/explain", post(post_explain))
        .route("/health", get(get_health))
}
