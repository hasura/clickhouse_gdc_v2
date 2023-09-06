use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use serde_with::skip_serializing_none;

mod binary_array_comparison_operator;
mod binary_comparison_operator;
mod scalar_type;
mod single_column_aggregate_function;
mod unary_comparison_operator;

pub use binary_array_comparison_operator::BinaryArrayComparisonOperator;
pub use binary_comparison_operator::BinaryComparisonOperator;
pub use scalar_type::ScalarType;
pub use single_column_aggregate_function::SingleColumnAggregateFunction;
pub use unary_comparison_operator::UnaryComparisonOperator;

pub type ScalarValue = Value;

/// The fully qualified name of a table, where the last item in the array is the table name and any earlier items represent the namespacing of the table name
pub type TableName = Vec<String>;

pub type ColumnMapping = IndexMap<String, String>;

#[skip_serializing_none]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum QueryRequest {
    Table {
        /// If present, a list of columns and values for the columns that the query must be repeated for, applying the column values as a filter for each query.
        foreach: Option<Vec<ForEach>>,
        query: Query,
        table: TableName,
        /// The relationships between tables involved in the entire query request
        table_relationships: Vec<TableRelationships>,
    },
    Target {
        /// If present, a list of columns and values for the columns that the query must be repeated for, applying the column values as a filter for each query.
        foreach: Option<Vec<ForEach>>,
        query: Query,
        target: Target,
        /// The relationships between tables involved in the entire query request
        relationships: Vec<TableRelationships>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Target {
    Table {
        /// The fully qualified name of a table, where the last item in the array is the table name and any earlier items represent the namespacing of the table name
        name: TableName,
    },
    Interpolated {
        id: String,
    },
    Function {
        function: Vec<String>,
    },
}

pub type ForEach = IndexMap<String, ForEachValue>;

#[skip_serializing_none]
#[derive(Clone, Debug, Serialize, Deserialize)]

pub struct Query {
    /// Aggregate fields of the query
    pub aggregates: Option<Aggregates>,
    /// Optionally limit the maximum number of rows considered while applying aggregations. This limit does not apply to returned rows.
    pub aggregates_limit: Option<Number>,
    /// Fields of the query
    pub fields: Option<Fields>,
    /// Optionally limit to N results
    pub limit: Option<Number>,
    /// Optionally offset from the Nth result. This applies to both row and aggregation results.
    pub offset: Option<Number>,
    /// Optionally order the results by the value of one or more fields
    pub order_by: Option<OrderBy>,
    #[serde(rename = "where")]
    pub selection: Option<Expression>,
}

pub type Aggregates = IndexMap<String, Aggregate>;
pub type Fields = IndexMap<String, Field>;

#[derive(Clone, Debug, Serialize, Deserialize)]

pub struct ForEachValue {
    pub value: ScalarValue,
    pub value_type: ScalarType,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TableRelationships {
    /// A map of relationships from the source table to target tables. The key of the map is the relationship name
    pub relationships: IndexMap<String, Relationship>,
    pub source_table: TableName,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Relationship {
    Table {
        /// A mapping between columns on the source table to columns on the target table
        column_mapping: ColumnMapping,
        relationship_type: RelationshipType,
        /// The target of the relationship.
        /// For backwards compatibility with previous versions of dc-api we allow the alternative property name "target_table" and allow table names to be parsed into Target::TTable
        target_table: TableName,
    },
    Target {
        /// A mapping between columns on the source table to columns on the target table
        column_mapping: ColumnMapping,
        relationship_type: RelationshipType,
        /// The target of the relationship.
        /// For backwards compatibility with previous versions of dc-api we allow the alternative property name "target_table" and allow table names to be parsed into Target::TTable
        target: Target,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationshipType {
    Object,
    Array,
}

#[derive(Clone, Debug, Serialize, Deserialize)]

pub struct OrderBy {
    ///The elements to order by, in priority order
    pub elements: Vec<OrderByElement>,
    /// A map of relationships from the current query table to target tables. The key of the map is the relationship name. The relationships are used within the order by elements.
    pub relations: IndexMap<String, OrderByRelation>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]

pub struct OrderByElement {
    pub order_direction: OrderDirection,
    pub target: OrderByTarget,
    /// The relationship path from the current query table to the table that contains the target to order by. This is always non-empty for aggregate order by targets
    pub target_path: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum OrderByTarget {
    #[serde(rename = "star_count_aggregate")]
    StarCountAggregate,
    #[serde(rename = "single_column_aggregate")]
    SingleColumnAggregate {
        /// The column to apply the aggregation function to
        column: String,
        function: SingleColumnAggregateFunction,
        result_type: ScalarType,
    },
    #[serde(rename = "column")]
    Column { column: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderByRelation {
    /// Further relationships to follow from the relationship's target table. The key of the map is the relationship name.
    pub subrelations: IndexMap<String, OrderByRelation>,
    #[serde(rename = "where")]
    pub selection: Option<Expression>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Expression {
    #[serde(rename = "exists")]
    Exists {
        in_table: ExistsInTable,
        #[serde(rename = "where")]
        selection: Box<Expression>,
    },
    #[serde(rename = "binary_arr_op")]
    BinaryArrayComparisonOperator {
        column: ComparisonColumn,
        operator: BinaryArrayComparisonOperator,
        value_type: ScalarType,
        values: Vec<Value>,
    },
    #[serde(rename = "or")]
    Or { expressions: Vec<Expression> },
    #[serde(rename = "unary_op")]
    UnaryComparisonOperator {
        column: ComparisonColumn,
        operator: UnaryComparisonOperator,
    },
    #[serde(rename = "binary_op")]
    BinaryComparisonOperator {
        column: ComparisonColumn,
        operator: BinaryComparisonOperator,
        value: ComparisonValue,
    },
    #[serde(rename = "not")]
    Not { expression: Box<Expression> },
    #[serde(rename = "and")]
    And { expressions: Vec<Expression> },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ExistsInTable {
    #[serde(rename = "unrelated")]
    UnrelatedTable { table: TableName },
    #[serde(rename = "related")]
    RelatedTable { relationship: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ComparisonValue {
    #[serde(rename = "scalar")]
    ScalarValueComparison {
        value: ScalarValue,
        value_type: ScalarType,
    },
    #[serde(rename = "column")]
    AnotherColumnComparison { column: ComparisonColumn },
}

#[derive(Clone, Debug, Serialize, Deserialize)]

pub struct ComparisonColumn {
    pub column_type: ScalarType,
    /// The name of the column
    pub name: String,
    /// The relationship path from the current query table to the table that contains the specified column. Empty Vec means the current query table.
    pub path: Option<Vec<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Field {
    #[serde(rename = "relationship")]
    Relationship { query: Query, relationship: String },
    #[serde(rename = "column")]
    Column {
        column: String,
        column_type: ScalarType,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Aggregate {
    #[serde(rename = "column_count")]
    ColumnCount { column: String, distinct: bool },
    #[serde(rename = "single_column")]
    SingleColumn {
        column: String,
        function: SingleColumnAggregateFunction,
        result_type: ScalarType,
    },
    #[serde(rename = "star_count")]
    StarCount,
}
