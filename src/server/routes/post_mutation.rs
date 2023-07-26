use axum::http::StatusCode;

#[axum_macros::debug_handler]
pub async fn post_mutation() -> StatusCode {
    StatusCode::NOT_IMPLEMENTED
}
