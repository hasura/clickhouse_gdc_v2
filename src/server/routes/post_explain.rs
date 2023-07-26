use axum::{Json, TypedHeader};
use axum_extra::extract::WithRejection;

use crate::{
    server::{
        api::{explain_response::ExplainResponse, query_request::QueryRequest},
        client::execute_clickhouse_request,
        config::{SourceConfig, SourceName},
        error::ServerError,
    },
    sql::QueryBuilder,
};

#[axum_macros::debug_handler]
pub async fn post_explain(
    SourceName(source_name): SourceName,
    SourceConfig(config): SourceConfig,
    WithRejection(Json(request), _): WithRejection<Json<QueryRequest>, ServerError>,
) -> Result<Json<ExplainResponse>, ServerError> {
    // let (statement, _) = request.build_sql(false)?;
    let statement = QueryBuilder::build_sql_statement(&request, false)?;
    let statement_string = statement.to_string();
    let explain_statement = format!("EXPLAIN {}", statement_string);

    let response = execute_clickhouse_request(&config, explain_statement.clone()).await?;

    // let response: QueryResponse = serde_json::from_value(response)?;
    let response = ExplainResponse {
        lines: vec![response],
        query: explain_statement,
    };

    Ok(Json(response))

    // let query = sqlx::query(&explain_statement);
    // let query = params.into_iter().fold(query, |query, bound_param| {
    //     match bound_param {
    //         BoundParam::Number(number) => query.bind(number.as_f64()),
    //         BoundParam::Value {
    //             value,
    //             value_type: _,
    //         } => match value {
    //             serde_json::Value::Number(number) => query.bind(number.as_f64()),
    //             serde_json::Value::String(string) => query.bind(string),
    //             serde_json::Value::Bool(boolean) => query.bind(boolean),
    //             // feels like a hack.
    //             serde_json::Value::Null => query.bind(None::<bool>),
    //             serde_json::Value::Array(array) => query.bind(sqlx::types::Json(array)),
    //             serde_json::Value::Object(object) => query.bind(sqlx::types::Json(object)),
    //         },
    //     }
    // });

    // BoundParam::Number(serde_json::Number { n }) => query.bind(n),
    // BoundParam::Value { value, value_type } => match value {
    //     serde_json::Value::Number(serde_json::Number { n }) => query.bind(n),
    //     serde_json::Value::String(s) => query.bind(s),
    //     serde_json::Value::Bool(b) => query.bind(b),
    //     serde_json::Value::Null => query.bind(()),
    //     serde_json::Value::Array(a) => query.bind(a),
    //     serde_json::Value::Object(o) => query.bind(o),
    // },

    // let pool = state.get_pool(&source_name, config).await?;

    // // capture db error
    // let plan = query
    //     .fetch_all(&pool)
    //     .await
    //     .map(|rows| {
    //         rows.iter()
    //             .map(|row| -> String { row.get(0) })
    //             .collect::<Vec<_>>()
    //     })
    //     .unwrap_or_else(|err| vec![err.to_string()]);

    // let mut lines = vec![
    //     "HGE Request".to_string(),
    //     request_string,
    //     "Database Query Plan".to_string(),
    // ];
    // lines.extend(plan);

    // let response = ExplainResponse {
    //     lines,
    //     query: explain_statement,
    // };

    // Ok(Json(response))
}
