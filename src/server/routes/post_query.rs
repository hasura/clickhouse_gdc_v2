use axum::Json;
use axum_extra::extract::WithRejection;
use tracing::{info_span, Instrument};

use crate::{
    server::{
        api::{
            error_response::ErrorResponseType, query_request::QueryRequest,
            query_response::QueryResponse,
        },
        client::execute_query,
        config::{SourceConfig, SourceName},
        error::ServerError,
    },
    sql::{apply_aliases_to_query_request, QueryBuilder},
};

#[axum_macros::debug_handler]
pub async fn post_query(
    SourceName(_source_name): SourceName,
    SourceConfig(config): SourceConfig,
    WithRejection(Json(request), _): WithRejection<Json<QueryRequest>, ServerError>,
) -> Result<Json<QueryResponse>, ServerError> {
    let request = apply_aliases_to_query_request(request, &config)?;
    let statement = QueryBuilder::build_sql_statement(&request, false)?;

    let statement_string = statement.to_string();

    let rows: Vec<QueryResponse> = execute_query(&config, &statement_string)
        .instrument(info_span!("execute_query"))
        .await?;

    let response: QueryResponse =
        rows.first()
            .cloned()
            .ok_or_else(|| ServerError::UncaughtError {
                details: None,
                message: "The database returned no rows".to_string(),
                error_type: ErrorResponseType::UncaughtError,
            })?;

    Ok(Json(response))
}
