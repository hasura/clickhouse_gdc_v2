use std::error::Error;

use serde::{de::DeserializeOwned, Deserialize};

use super::config::Config;

pub type ClickhouseParameters = Vec<(String, String)>;

pub async fn execute_query<T: DeserializeOwned>(
    config: &Config,
    statement: &str,
    parameters: &ClickhouseParameters,
) -> Result<Vec<T>, Box<dyn Error>> {
    let client = reqwest::Client::new();
    let response = client
        .post(&config.url)
        .header("X-ClickHouse-User", &config.username)
        .header("X-ClickHouse-Key", &config.password)
        .query(parameters)
        .body(statement.to_owned())
        .send()
        .await?;

    if response.error_for_status_ref().is_err() {
        return Err(response.text().await?.into());
    }

    let payload: ClickHouseResponse<T> = response.json().await?;

    Ok(payload.data)
}

pub async fn ping(config: &Config) -> Result<(), Box<dyn Error>> {
    let last_char = config.url.chars().last();

    let url = if let Some('/') = last_char {
        format!("{}ping", config.url)
    } else {
        format!("{}/ping", config.url)
    };

    let client = reqwest::Client::new();
    let _request = client
        .get(&url)
        .header("X-ClickHouse-User", &config.username)
        .header("X-ClickHouse-Key", &config.password)
        .send()
        .await?;

    Ok(())
}

#[derive(Debug, Deserialize)]
struct ClickHouseResponse<T> {
    #[allow(dead_code)]
    meta: Vec<ClickHouseResponseMeta>,
    data: Vec<T>,
    #[allow(dead_code)]
    rows: u32,
    // unsure about the specification for this object, it's likely to be somewhat dynamic
    // keeping as an unspecified json value for now
    #[allow(dead_code)]
    statistics: serde_json::Value,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct ClickHouseResponseMeta {
    name: String,
    #[serde(rename = "type")]
    column_type: String,
}
