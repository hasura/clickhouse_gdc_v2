use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Config {
    pub username: String,
    pub password: String,
    pub url: String,
}
use axum::{
    async_trait,
    extract::FromRequestParts,
    http::{request::Parts, HeaderName, StatusCode},
};

static CONFIG_HEADER: HeaderName = HeaderName::from_static("x-hasura-dataconnector-config");
static SOURCE_HEADER: HeaderName = HeaderName::from_static("x-hasura-dataconnector-sourcename");

#[derive(Debug)]
pub struct SourceName(pub String);
#[derive(Debug)]
pub struct SourceConfig(pub Config);

#[async_trait]
impl<S: Send + Sync> FromRequestParts<S> for SourceName {
    type Rejection = StatusCode;
    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        if let Some(source_header) = parts.headers.get(&SOURCE_HEADER) {
            let source_name = source_header
                .to_str()
                .map_err(|_err| StatusCode::BAD_REQUEST)?;
            Ok(Self(source_name.to_owned()))
        } else {
            Err(StatusCode::BAD_REQUEST)
        }
    }
}

#[async_trait]
impl<S: Send + Sync> FromRequestParts<S> for SourceConfig {
    type Rejection = StatusCode;
    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        if let Some(config_header) = parts.headers.get(&CONFIG_HEADER) {
            let config: Config = serde_json::from_slice(config_header.as_bytes())
                .map_err(|_err| StatusCode::BAD_REQUEST)?;
            Ok(Self(config))
        } else {
            Err(StatusCode::BAD_REQUEST)
        }
    }
}
