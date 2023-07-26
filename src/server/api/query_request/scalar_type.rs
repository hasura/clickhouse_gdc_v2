use std::fmt::{Display, Formatter};

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
}
// impl Display for ClickhouseScalarType {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         match self {
//             Self::Bool => write!(f, "Bool"),
//             Self::String => write!(f, "String"),
//             Self::FixedString => write!(f, "FixedString"),
//             Self::UInt8 => write!(f, "UInt8"),
//             Self::UInt16 => write!(f, "UInt16"),
//             Self::UInt32 => write!(f, "UInt32"),
//             Self::UInt64 => write!(f, "UInt64"),
//             Self::UInt128 => write!(f, "UInt128"),
//             Self::UInt256 => write!(f, "UInt256"),
//             Self::Int8 => write!(f, "Int8"),
//             Self::Int16 => write!(f, "Int16"),
//             Self::Int32 => write!(f, "Int32"),
//             Self::Int64 => write!(f, "Int64"),
//             Self::Int128 => write!(f, "Int128"),
//             Self::Int256 => write!(f, "Int256"),
//             Self::Float32 => write!(f, "Float32"),
//             Self::Float64 => write!(f, "Float64"),
//             Self::Date => write!(f, "Date"),
//             Self::Date32 => write!(f, "Date32"),
//             Self::DateTime => write!(f, "DateTime"),
//             Self::DateTime64 => write!(f, "DateTime64"),
//             Self::Json => write!(f, "JSON"),
//             Self::Uuid => write!(f, "UUID"),
//             Self::IPv4 => write!(f, "IPv4"),
//             Self::IPv6 => write!(f, "IPv6"),
//         }
//     }
// }
// impl ScalarType for ClickhouseScalarType {
//     fn default_value(&self) -> Expr {
//         match self {
//             Self::Bool => Expr::Value(Value::Null),
//             Self::String => Expr::Value(Value::SingleQuotedString("".to_owned())),
//             Self::FixedString => Expr::Value(Value::SingleQuotedString("".to_owned())),
//             Self::UInt8 => Expr::Value(Value::Number("0".to_owned(), false)),
//             Self::UInt16 => Expr::Value(Value::Number("0".to_owned(), false)),
//             Self::UInt32 => Expr::Value(Value::Number("0".to_owned(), false)),
//             Self::UInt64 => Expr::Value(Value::Number("0".to_owned(), false)),
//             Self::UInt128 => Expr::Value(Value::Number("0".to_owned(), false)),
//             Self::UInt256 => Expr::Value(Value::Number("0".to_owned(), false)),
//             Self::Int8 => Expr::Value(Value::Number("0".to_owned(), false)),
//             Self::Int16 => Expr::Value(Value::Number("0".to_owned(), false)),
//             Self::Int32 => Expr::Value(Value::Number("0".to_owned(), false)),
//             Self::Int64 => Expr::Value(Value::Number("0".to_owned(), false)),
//             Self::Int128 => Expr::Value(Value::Number("0".to_owned(), false)),
//             Self::Int256 => Expr::Value(Value::Number("0".to_owned(), false)),
//             Self::Float32 => Expr::Value(Value::Number("0".to_owned(), false)),
//             Self::Float64 => Expr::Value(Value::Number("0".to_owned(), false)),
//             Self::Date => Expr::Value(Value::Null),
//             Self::Date32 => Expr::Value(Value::Null),
//             Self::DateTime => Expr::Value(Value::Null),
//             Self::DateTime64 => Expr::Value(Value::Null),
//             Self::Json => Expr::Value(Value::Null),
//             Self::Uuid => Expr::Value(Value::Null),
//             Self::IPv4 => Expr::Value(Value::Null),
//             Self::IPv6 => Expr::Value(Value::Null),
//         }
//     }
//     fn literal_expr(&self, value: serde_json::Value) -> Expr {
//         match value {
//             serde_json::Value::Number(number) => {
//                 Expr::Value(Value::Number(number.to_string(), false))
//             }
//             serde_json::Value::String(string) => Expr::Value(Value::SingleQuotedString(string)),
//             serde_json::Value::Bool(boolean) => Expr::Value(Value::Boolean(boolean)),
//             // feels like a hack.
//             serde_json::Value::Null => Expr::Value(Value::Null),
//             serde_json::Value::Array(_) => {
//                 Expr::Value(Value::SingleQuotedString(value.to_string()))
//             }
//             serde_json::Value::Object(_) => {
//                 Expr::Value(Value::SingleQuotedString(value.to_string()))
//             }
//         }
//     }
// }
