use serde::{Deserialize, Serialize};
use strum::{Display, EnumIter, EnumString};
pub mod clickhouse_data_type;

#[derive(
    Debug, Serialize, Deserialize, Hash, Eq, PartialEq, Clone, Display, EnumIter, EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum BinaryArrayComparisonOperator {
    In,
}

#[derive(
    Debug, Serialize, Deserialize, Hash, Eq, PartialEq, Clone, Display, EnumIter, EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum BinaryComparisonOperator {
    LessThan,
    LessThanOrEqual,
    Equal,
    GreaterThan,
    GreaterThanOrEqual,
}

#[derive(
    Debug, Serialize, Deserialize, Hash, Eq, PartialEq, Clone, Display, EnumIter, EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ScalarType {
    Bool,
    String,
    FixedString,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    UInt256,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,
    Float32,
    Float64,
    Decimal,
    Date,
    Date32,
    DateTime,
    DateTime64,
    #[strum(serialize = "JSON")]
    #[serde(rename = "JSON")]
    Json,
    #[strum(serialize = "UUID")]
    #[serde(rename = "UUID")]
    Uuid,
    IPv4,
    IPv6,
    AvgUInt8,
    AvgUInt16,
    AvgUInt32,
    AvgUInt64,
    AvgUInt128,
    AvgUInt256,
    AvgInt8,
    AvgInt16,
    AvgInt32,
    AvgInt64,
    AvgInt128,
    AvgInt256,
    AvgFloat32,
    AvgFloat64,
    AvgDecimal,
    SumUInt8,
    SumUInt16,
    SumUInt32,
    SumUInt64,
    SumUInt128,
    SumUInt256,
    SumInt8,
    SumInt16,
    SumInt32,
    SumInt64,
    SumInt128,
    SumInt256,
    SumFloat32,
    SumFloat64,
    SumDecimal,
    MaxUInt8,
    MaxUInt16,
    MaxUInt32,
    MaxUInt64,
    MaxUInt128,
    MaxUInt256,
    MaxInt8,
    MaxInt16,
    MaxInt32,
    MaxInt64,
    MaxInt128,
    MaxInt256,
    MaxFloat32,
    MaxFloat64,
    MaxDecimal,
    MinUInt8,
    MinUInt16,
    MinUInt32,
    MinUInt64,
    MinUInt128,
    MinUInt256,
    MinInt8,
    MinInt16,
    MinInt32,
    MinInt64,
    MinInt128,
    MinInt256,
    MinFloat32,
    MinFloat64,
    MinDecimal,
    MaxDate,
    MaxDate32,
    MaxDateTime,
    MaxDateTime64,
    MinDate,
    MinDate32,
    MinDateTime,
    MinDateTime64,
    Unknown,
}

#[derive(
    Debug, Serialize, Deserialize, Hash, Eq, PartialEq, Clone, Display, EnumIter, EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
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

#[derive(
    Debug, Serialize, Deserialize, Hash, Eq, PartialEq, Clone, Display, EnumIter, EnumString,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum UnaryComparisonOperator {
    IsNull,
}
