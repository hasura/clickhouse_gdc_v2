use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
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
    #[serde(rename = "JSON")]
    Json,
    #[serde(rename = "UUID")]
    Uuid,
    IPv4,
    IPv6,
    Unknown,
}
