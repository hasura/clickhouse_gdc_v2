use axum::Json;
use axum_extra::extract::WithRejection;

use crate::{
    server::{
        api::{query_request::QueryRequest, query_response::QueryResponse},
        client::execute_clickhouse_request,
        config::{SourceConfig, SourceName},
        error::ServerError,
    },
    sql::QueryBuilder,
};

#[axum_macros::debug_handler]
pub async fn post_query(
    SourceName(_source_name): SourceName,
    SourceConfig(config): SourceConfig,
    WithRejection(Json(request), _): WithRejection<Json<QueryRequest>, ServerError>,
) -> Result<Json<QueryResponse>, ServerError> {
    // get_http_file(&source_name, &config, &request);
    // let (statement, _) = request.build_sql(false)?;
    let statement = QueryBuilder::build_sql_statement(&request, false)?;

    // todo: this is awful and needs cleanup/abstraction
    let statement_string = statement.to_string();
    let response = execute_clickhouse_request(&config, statement_string).await?;

    let response: QueryResponse = serde_json::from_str(&response)?;

    Ok(Json(response))
}
