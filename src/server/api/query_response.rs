use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_with::skip_serializing_none;

#[skip_serializing_none]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueryResponse {
    /// The results of the aggregates returned by the query
    aggregates: Option<IndexMap<String, Value>>,
    /// The rows returned by the query, corresponding to the query's fields
    rows: Option<Vec<IndexMap<String, Option<RowFieldValue>>>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RowFieldValue {
    RelationshipFieldValue(QueryResponse),
    ColumnFieldValue(Value),
}
