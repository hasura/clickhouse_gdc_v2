use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use super::query_request::{ScalarType, TableName};

#[skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SchemaResponse {
    /// Available functions
    pub functions: Option<Vec<FunctionInfo>>,
    /// Object type definitions referenced in this schema
    pub object_types: Option<Vec<ObjectTypeDefinition>>,
    /// Available tables
    pub tables: Vec<TableInfo>,
}

#[skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
pub struct TableInfo {
    /// The columns of the table
    pub columns: Vec<ColumnInfo>,
    /// Whether or not existing rows can be deleted in the table
    pub deletable: Option<bool>,
    /// Description of the table
    pub description: Option<String>,
    /// Foreign key constraints
    pub foreign_keys: Option<IndexMap<String, Constraint>>,
    /// Whether or not new rows can be inserted into the table
    pub insertable: Option<bool>,
    /// The fully qualified name of a table, where the last item in the array is the table name and any earlier items represent the namespacing of the table name
    pub name: TableName,
    /// The primary key of the table
    pub primary_key: Option<Vec<String>>,
    #[serde(rename = "type")]
    pub table_type: Option<TableType>,
    /// Whether or not existing rows can be updated in the table
    pub updatable: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableType {
    Table,
    View,
}

#[skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnInfo {
    /// Column description
    pub description: Option<String>,
    /// Whether or not the column can be inserted into
    pub insertable: Option<bool>,
    /// Column name
    pub name: String,
    /// Is column nullable
    pub nullable: bool,
    #[serde(rename = "type")]
    pub column_type: ColumnType,
    /// Whether or not the column can be updated
    pub updatable: Option<bool>,
    pub value_generated: Option<ColumnValueGenerationStrategy>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ColumnType {
    NonScalar(ColumnTypeNonScalar),
    ScalarType(ScalarType),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ColumnTypeNonScalar {
    Object {
        name: String,
    },
    Array {
        element_type: Box<ColumnType>,
        nullable: bool,
    },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum ColumnValueGenerationStrategy {
    DefaultValue {},
    AutoIncrement {},
    UniqueIdentifier {},
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Constraint {
    /// The columns on which you want want to define the foreign key.
    pub column_mapping: IndexMap<String, String>,
    /// The fully qualified name of a table, where the last item in the array is the table name and any earlier items represent the namespacing of the table name
    pub foreign_table: TableName,
}

#[skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FunctionInfo {
    /// argument info - name/types
    args: Vec<FunctionInformationArgument>,
    /// Description of the table
    description: Option<String>,
    name: FunctionName,
    response_cardinality: FunctionCardinality,
    returns: FunctionReturnType,
    #[serde(rename = "type")]
    function_type: FunctionType,
}

pub type FunctionName = Vec<String>;

#[skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FunctionInformationArgument {
    // The name of the argument
    name: String,
    /// If the argument can be omitted
    optional: Option<bool>,
    scalar_type: ScalarType,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum FunctionReturnType {
    Table { table: TableName },
    Unknown {},
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FunctionCardinality {
    One,
    Many,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FunctionType {
    Read,
    Write,
}

#[skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
pub struct ObjectTypeDefinition {
    /// The columns of the type
    columns: Vec<ColumnInfo>,
    /// The description of the type
    description: Option<String>,
    /// The name of the type
    name: String,
}
