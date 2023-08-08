use axum::Json;
use axum_extra::extract::WithRejection;
use tracing::{info_span, Instrument};

use crate::{
    server::{
        api::{query_request::QueryRequest, query_response::QueryResponse},
        client::execute_clickhouse_request,
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
    let response = execute_clickhouse_request(&config, statement_string)
        .instrument(info_span!("execute_query"))
        .await?;

    let response: QueryResponse = serde_json::from_str(&response)?;

    Ok(Json(response))
}
