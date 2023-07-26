use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RawResponse {
    /// The rows returned by the raw query.
    pub rows: Vec<IndexMap<String, serde_json::Value>>,
}
