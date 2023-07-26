use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Hash, Eq, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum SingleColumnAggregateFunction {
    Max,
    Min,
    StddevPop,
    StddevSamp,
    Sum,
    VarPop,
    VarSamp,
    Longest,
    Shortest,
}
