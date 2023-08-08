use std::error::Error;

use axum::{
    extract::rejection::{JsonRejection, TypedHeaderRejection},
    http::{StatusCode, Uri},
    response::{IntoResponse, Response},
};
use axum_macros::FromRequest;

use crate::sql::QueryBuilderError;

use super::api::error_response::{ErrorResponse, ErrorResponseType};

pub enum ServerError {
    NotFound(Uri),
    UncaughtError {
        details: Option<serde_json::Value>,
        message: String,
        error_type: ErrorResponseType,
    },
}

#[derive(FromRequest)]
#[from_request(via(axum::Json), rejection(ServerError))]
pub struct Json<T>(pub T);

impl IntoResponse for ServerError {
    fn into_response(self) -> Response {
        match self {
            Self::UncaughtError {
                details,
                message,
                error_type,
            } => (
                StatusCode::BAD_REQUEST,
                axum::Json(ErrorResponse {
                    details,
                    message,
                    error_type,
                }),
            )
                .into_response(),
            Self::NotFound(uri) => (
                StatusCode::NOT_FOUND,
                format!("Path not found: {}", uri.path()),
            )
                .into_response(),
        }
    }
}

impl From<serde_json::Error> for ServerError {
    fn from(err: serde_json::Error) -> Self {
        Self::UncaughtError {
            details: None,
            message: err.to_string(),
            error_type: ErrorResponseType::UncaughtError,
        }
    }
}

impl From<std::io::Error> for ServerError {
    fn from(err: std::io::Error) -> Self {
        Self::UncaughtError {
            details: None,
            message: err.to_string(),
            error_type: ErrorResponseType::UncaughtError,
        }
    }
}

impl From<Box<dyn Error>> for ServerError {
    fn from(err: Box<dyn Error>) -> Self {
        Self::UncaughtError {
            details: None,
            message: err.to_string(),
            error_type: ErrorResponseType::UncaughtError,
        }
    }
}

impl From<QueryBuilderError> for ServerError {
    fn from(err: QueryBuilderError) -> Self {
        Self::UncaughtError {
            details: None,
            message: err.to_string(),
            error_type: ErrorResponseType::UncaughtError,
        }
    }
}

impl From<JsonRejection> for ServerError {
    fn from(err: JsonRejection) -> Self {
        Self::UncaughtError {
            details: None,
            message: err.to_string(),
            error_type: ErrorResponseType::UncaughtError,
        }
    }
}

impl From<TypedHeaderRejection> for ServerError {
    fn from(err: TypedHeaderRejection) -> Self {
        Self::UncaughtError {
            details: None,
            message: err.to_string(),
            error_type: ErrorResponseType::UncaughtError,
        }
    }
}
