use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    /// Error details
    pub details: Option<serde_json::Value>,
    /// Error message
    pub message: String,
    #[serde(rename = "type")]
    pub error_type: ErrorResponseType,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ErrorResponseType {
    #[serde(rename = "uncaught-error")]
    UncaughtError,
    #[serde(rename = "mutation-constraint-violation")]
    MutationConstraintViolation,
    #[serde(rename = "mutation-permission-check-failure")]
    MutationPermissionCheckFailure,
}
