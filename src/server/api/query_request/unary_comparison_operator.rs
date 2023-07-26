use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Hash, Eq, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum UnaryComparisonOperator {
    IsNull,
}
