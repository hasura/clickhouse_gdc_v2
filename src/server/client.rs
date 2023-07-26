use std::error::Error;

use super::config::Config;

pub async fn execute_clickhouse_request(
    config: &Config,
    statement: String,
) -> Result<String, Box<dyn Error>> {
    let client = reqwest::Client::new();
    let request = client
        .post(&config.url)
        .basic_auth(&config.username, Some(&config.password))
        .body(statement)
        .send()
        .await?;

    let response = request.text().await?;
    // hack: remove escaped single quotes, as these are not valid in json strings.
    // issue: https://github.com/ClickHouse/ClickHouse/issues/49348
    let response = response.replace(r#"\'"#, r#"'"#);
    let response = response.replace(r#"\\"#, r#"\"#);

    Ok(response)
}
