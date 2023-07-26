use serde::{Serialize, Deserialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RawRequest {
    /// A string representing a raw query
    pub query: String,
}
