use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Hash, Eq, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum BinaryComparisonOperator {
    LessThan,
    LessThanOrEqual,
    Equal,
    GreaterThan,
    GreaterThanOrEqual,
}
