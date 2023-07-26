use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

use super::query_request::{ScalarType, TableName};

#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
pub struct SchemaResponse {
    /// Available tables
    pub tables: Vec<TableInfo>,
}

#[skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
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
#[serde(bound = "")]
pub enum TableType {
    Table,
    View,
}

#[skip_serializing_none]
#[derive(Debug, Serialize, Deserialize)]
#[serde(bound = "")]
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
    pub column_type: ScalarType,
    /// Whether or not the column can be updated
    pub updatable: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Constraint {
    /// The columns on which you want want to define the foreign key.
    pub column_mapping: IndexMap<String, String>,
    /// The fully qualified name of a table, where the last item in the array is the table name and any earlier items represent the namespacing of the table name
    pub foreign_table: TableName,
}
