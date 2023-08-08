use axum::Json;
use axum_extra::extract::WithRejection;
use tracing::{info_span, Instrument};

use crate::{
    server::{
        api::{explain_response::ExplainResponse, query_request::QueryRequest},
        client::execute_clickhouse_request,
        config::{SourceConfig, SourceName},
        error::ServerError,
    },
    sql::{apply_aliases_to_query_request, QueryBuilder},
};

#[axum_macros::debug_handler]
pub async fn post_explain(
    SourceName(_source_name): SourceName,
    SourceConfig(config): SourceConfig,
    WithRejection(Json(request), _): WithRejection<Json<QueryRequest>, ServerError>,
) -> Result<Json<ExplainResponse>, ServerError> {
    let request = apply_aliases_to_query_request(request, &config)?;
    let statement = QueryBuilder::build_sql_statement(&request, false)?;
    let statement_string = statement.to_string();
    let explain_statement = format!("EXPLAIN {}", statement_string);

    let response = execute_clickhouse_request(&config, explain_statement.clone())
        .instrument(info_span!("get_query_plan"))
        .await?;

    let response = ExplainResponse {
        lines: vec![response],
        query: explain_statement,
    };

    Ok(Json(response))
}
