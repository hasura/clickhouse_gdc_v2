use axum::Json;
use axum_extra::extract::WithRejection;
use gdc_rust_types::{ExplainResponse, QueryRequest};
use serde::{Deserialize, Serialize};
use tracing::{info_span, Instrument};

use crate::{
    server::{
        client::execute_query,
        config::{SourceConfig, SourceName},
        error::ServerError,
    },
    sql::QueryBuilder,
};

#[axum_macros::debug_handler]
pub async fn post_explain(
    SourceName(_source_name): SourceName,
    SourceConfig(config): SourceConfig,
    WithRejection(Json(request), _): WithRejection<Json<QueryRequest>, ServerError>,
) -> Result<Json<ExplainResponse>, ServerError> {
    let statement = QueryBuilder::build_sql_statement(&request, false, &config)?;
    let statement_string = statement.to_string();
    let explain_statement = format!("EXPLAIN {}", statement_string);

    let query_plan: Vec<ExplainRow> = execute_query(&config, &explain_statement, &vec![])
        .instrument(info_span!("get_query_plan"))
        .await?;

    let response = ExplainResponse {
        lines: query_plan.into_iter().map(|r| r.explain).collect(),
        query: explain_statement,
    };

    Ok(Json(response))
}

#[derive(Debug, Serialize, Deserialize)]
struct ExplainRow {
    explain: String,
}
