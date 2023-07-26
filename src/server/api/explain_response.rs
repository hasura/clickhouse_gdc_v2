use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExplainResponse {
    /// Lines of the formatted explain plan response
    pub lines: Vec<String>,
    /// The generated query - i.e. SQL for a relational DB
    pub query: String,
}
