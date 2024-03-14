use std::{
    error::Error,
    fmt::{Display, Formatter},
};

#[derive(Debug)]
pub enum QueryBuilderError {
    Internal(String),
    NoRowsOrAggregates,
    RightHandColumnComparisonNotSupported(String),
    UnsupportedColumnComparisonPath(Vec<String>),
    TableMissing(Vec<String>),
    RelationshipMissingInTable(String, Vec<String>),
    MisshapenTableName(Vec<String>),
    UnknownScalarType(String),
    UnknownUnaryComparisonOperator(String),
    UnknownBinaryComparisonOperator(String),
    UnknownBinaryArrayComparisonOperator(String),
    UnknownSingleColumnAggregateFunction(String),
}

impl Display for QueryBuilderError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryBuilderError::Internal(message) => write!(f, "Internal error: {}", message),
            QueryBuilderError::NoRowsOrAggregates => {
                write!(f, "Query must have at least either fields or aggregates")
            }
            QueryBuilderError::RightHandColumnComparisonNotSupported(column) => write!(
                f,
                "Right hand column comparison is not supported, attempted to compare column \"{}\"",
                column
            ),
            QueryBuilderError::UnsupportedColumnComparisonPath(path) => {
                write!(f, "Unsupported column comparison path: {:?}", path)
            }
            QueryBuilderError::TableMissing(table) => write!(
                f,
                "Missing table {} from table relationships reference",
                table.join(".")
            ),
            QueryBuilderError::RelationshipMissingInTable(relationship, table) => write!(
                f,
                "Missing relationship {} in table {} in relationships reference",
                relationship,
                table.join(".")
            ),
            QueryBuilderError::MisshapenTableName(table) => write!(
                f,
                "Misshapen table name, expected an array with one or two string members, got {:?}",
                table
            ),
            QueryBuilderError::UnknownScalarType(name) => {
                write!(f, "Unknown Scalar Type: {name}")
            }
            QueryBuilderError::UnknownUnaryComparisonOperator(name) => {
                write!(f, "Unknown Unary Comparison Operator: {name}")
            }
            QueryBuilderError::UnknownBinaryComparisonOperator(name) => {
                write!(f, "Unknown Binary Comparison Operator: {name}")
            }
            QueryBuilderError::UnknownBinaryArrayComparisonOperator(name) => {
                write!(f, "Unknown Binary Array Comparison Operator: {name}")
            }
            QueryBuilderError::UnknownSingleColumnAggregateFunction(name) => {
                write!(f, "Unknown Single Column Aggregate Function: {name}")
            }
        }
    }
}
impl Error for QueryBuilderError {}
