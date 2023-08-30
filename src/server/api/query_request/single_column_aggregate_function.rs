use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Hash, Eq, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum SingleColumnAggregateFunction {
    Max,
    Min,
    Avg,
    StddevPop,
    StddevSamp,
    Sum,
    VarPop,
    VarSamp,
    Longest,
    Shortest,
    AvgMerge,
    SumMerge,
    MinMerge,
    MaxMerge,
}
