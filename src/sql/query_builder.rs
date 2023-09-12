use std::{str::FromStr, vec};

use super::ast::{
    BinaryOperator, Expr, Function, FunctionArgExpr, Ident, Join, JoinConstraint, JoinOperator,
    LimitByExpr, ObjectName, OrderByExpr, Query, SelectItem, Statement, TableFactor,
    TableWithJoins, UnaryOperator, Value,
};
use indexmap::IndexMap;
pub mod aliasing;
mod error;
use crate::server::schema;
pub use error::QueryBuilderError;

pub enum BoundParam {
    Number(serde_json::Number),
    Value {
        value: serde_json::Value,
        value_type: schema::ScalarType,
    },
}

fn sql_function(name: &str, args: Vec<Expr>) -> Expr {
    Expr::Function(Function {
        name: ObjectName(vec![Ident::unquoted(name)]),
        args: args.into_iter().map(FunctionArgExpr::Expr).collect(),
        over: None,
        distinct: false,
    })
}

// we use the function name to alias aggregate columns when necessary.
// the name should be reasonable short, and a valid part of a sql identifier when quoted
fn function_name(function: &schema::SingleColumnAggregateFunction) -> &'static str {
    use schema::SingleColumnAggregateFunction as CA;
    match function {
        CA::Avg => "avg",
        CA::Max => "max",
        CA::Min => "min",
        CA::StddevPop => "stddevPop",
        CA::StddevSamp => "stddevSamp",
        CA::Sum => "sum",
        CA::VarPop => "varPop",
        CA::VarSamp => "varSamp",
        CA::Longest => "longest",
        CA::Shortest => "shortest",
        CA::AvgMerge => "avgMerge",
        CA::SumMerge => "sumMerge",
        CA::MinMerge => "minMerge",
        CA::MaxMerge => "maxMerge",
    }
}

fn and_reducer(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::And,
        right: Box::new(right),
    }
}
fn or_reducer(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(left),
        op: BinaryOperator::Or,
        right: Box::new(right),
    }
}

fn single_column_aggregate(function: &schema::SingleColumnAggregateFunction, column: Expr) -> Expr {
    use schema::SingleColumnAggregateFunction as CA;
    match function {
        CA::Avg => sql_function("avg", vec![column]),
        CA::Max => sql_function("max", vec![column]),
        CA::Min => sql_function("min", vec![column]),
        CA::StddevPop => sql_function("stddevPop", vec![column]),
        CA::StddevSamp => sql_function("stddevSamp", vec![column]),
        CA::Sum => sql_function("sum", vec![column]),
        CA::VarPop => sql_function("varPop", vec![column]),
        CA::VarSamp => sql_function("varSamp", vec![column]),
        CA::Longest => sql_function("max", vec![sql_function("length", vec![column])]),
        CA::Shortest => sql_function("min", vec![sql_function("length", vec![column])]),
        CA::AvgMerge => sql_function("avgMerge", vec![column]),
        CA::SumMerge => sql_function("sumMerge", vec![column]),
        CA::MinMerge => sql_function("minMerge", vec![column]),
        CA::MaxMerge => sql_function("maxMerge", vec![column]),
    }
}

fn root_foreach_row_type(query: &gdc_rust_types::Query) -> Result<String, QueryBuilderError> {
    Ok(format!("Array(Tuple(query {}))", query_object_type(query)?))
}
fn root_rows_type(
    fields: &IndexMap<String, gdc_rust_types::Field>,
) -> Result<String, QueryBuilderError> {
    Ok(format!("Array({})", rows_object_type(fields)?))
}
fn root_aggregates_type(
    aggregates: &IndexMap<String, gdc_rust_types::Aggregate>,
) -> Result<String, QueryBuilderError> {
    aggregates_object_type(aggregates)
}

fn query_object_type(query: &gdc_rust_types::Query) -> Result<String, QueryBuilderError> {
    Ok(match (&query.fields, &query.aggregates) {
        (None, None) => "Map(Nothing, Nothing)".to_owned(),
        (Some(fields), None) => {
            let fields_type = rows_object_type(fields)?;
            format!("Tuple(rows Array({}))", fields_type)
        }
        (None, Some(aggregates)) => {
            let aggregates_type = aggregates_object_type(aggregates)?;
            format!("Tuple(aggregates {})", aggregates_type)
        }
        (Some(fields), Some(aggregates)) => {
            let fields_type = rows_object_type(fields)?;
            let aggregates_type = aggregates_object_type(aggregates)?;
            format!(
                "Tuple(rows Array({}), aggregates {})",
                fields_type, aggregates_type
            )
        }
    })
}
fn rows_object_type(
    fields: &IndexMap<String, gdc_rust_types::Field>,
) -> Result<String, QueryBuilderError> {
    Ok(if fields.is_empty() {
        "Map(Nothing, Nothing)".to_string()
    } else {
        let field_types = fields
            .iter()
            .map(|(column_name, field)| {
                let field_type = match field {
                    gdc_rust_types::Field::Column {
                        column: _,
                        column_type,
                    } => type_cast_string(column_type)?,
                    gdc_rust_types::Field::Relationship {
                        query,
                        relationship: _,
                    } => query_object_type(query)?,
                    gdc_rust_types::Field::Object { .. } => {
                        return Err(QueryBuilderError::Internal(
                            "Object fields not supported".to_string(),
                        ))
                    }
                    gdc_rust_types::Field::Array { .. } => {
                        return Err(QueryBuilderError::Internal(
                            "Array fields not supported".to_string(),
                        ))
                    }
                };
                Ok(format!("\"{}\" {}", column_name, field_type))
            })
            .collect::<Result<Vec<_>, _>>()?;
        format!("Tuple({})", field_types.join(", "))
    })
}
fn aggregates_object_type(
    aggregates: &IndexMap<String, gdc_rust_types::Aggregate>,
) -> Result<String, QueryBuilderError> {
    Ok(if aggregates.is_empty() {
        "Map(Nothing, Nothing)".to_string()
    } else {
        let aggregates_types = aggregates
            .iter()
            .map(|(column_name, aggregate)| {
                let aggregate_type = match aggregate {
                    // note! casting from UInt64 to UInt32 here
                    // UInt64 is serialized as a JSON string, but test suite expects JSON numbers
                    // todo: once we are able to specify return type for these aggregates, update this cast to the correct type
                    gdc_rust_types::Aggregate::ColumnCount { .. } => "UInt32".to_owned(),
                    gdc_rust_types::Aggregate::StarCount {} => "UInt32".to_owned(),
                    gdc_rust_types::Aggregate::SingleColumn { result_type, .. } => {
                        type_cast_string(result_type)?
                    }
                };
                Ok(format!("\"{}\" {}", column_name, aggregate_type))
            })
            .collect::<Result<Vec<_>, _>>()?;
        format!("Tuple({})", aggregates_types.join(", "))
    })
}
/// given a scalar type, return the type for the variant of this type that is nullable
/// used when casting rows to named tuples, which is later used to cast to JSON
/// we always wrap the type name in Nullable() as we don't know if the underlying column is nulable or not
fn type_cast_string(scalar_type: &gdc_rust_types::ScalarType) -> Result<String, QueryBuilderError> {
    let scalar_type = schema::ScalarType::from_str(scalar_type)
        .map_err(|err| QueryBuilderError::UnknownScalarType(scalar_type.to_owned()))?;
    use schema::ScalarType as ST;
    Ok(match scalar_type {
        ST::Bool => "Nullable(Bool)",
        ST::String => "Nullable(String)",
        ST::FixedString => "Nullable(FixedString)",
        ST::UInt8 => "Nullable(UInt8)",
        ST::UInt16 => "Nullable(UInt16)",
        ST::UInt32 => "Nullable(UInt32)",
        ST::UInt64 => "Nullable(UInt64)",
        ST::UInt128 => "Nullable(UInt128)",
        ST::UInt256 => "Nullable(UInt256)",
        ST::Int8 => "Nullable(Int8)",
        ST::Int16 => "Nullable(Int16)",
        ST::Int32 => "Nullable(Int32)",
        ST::Int64 => "Nullable(Int64)",
        ST::Int128 => "Nullable(Int128)",
        ST::Int256 => "Nullable(Int256)",
        ST::Float32 => "Nullable(Float32)",
        ST::Float64 => "Nullable(Float64)",
        // casting decimal to string. Not sure if this is correct.
        // cannot cast to decimal without making a call on precision and scale
        // could go for max precision, but impossible to know scale
        ST::Decimal => "Nullable(String)",
        ST::Date => "Nullable(Date)",
        ST::Date32 => "Nullable(Date32)",
        ST::DateTime => "Nullable(DateTime)",
        ST::DateTime64 => "Nullable(DateTime64(9))",
        ST::Json => "Nullable(JSON)",
        ST::Uuid => "Nullable(UUID)",
        ST::IPv4 => "Nullable(IPv4)",
        ST::IPv6 => "Nullable(IPv6)",
        ST::Unknown => "Nullable(String)",
        // AggregateFunction types are not really meant to be looked at directly, casting to string for now
        ST::AvgUInt8 => "Nullable(String)",
        ST::AvgUInt16 => "Nullable(String)",
        ST::AvgUInt32 => "Nullable(String)",
        ST::AvgUInt64 => "Nullable(String)",
        ST::AvgUInt128 => "Nullable(String)",
        ST::AvgUInt256 => "Nullable(String)",
        ST::AvgInt8 => "Nullable(String)",
        ST::AvgInt16 => "Nullable(String)",
        ST::AvgInt32 => "Nullable(String)",
        ST::AvgInt64 => "Nullable(String)",
        ST::AvgInt128 => "Nullable(String)",
        ST::AvgInt256 => "Nullable(String)",
        ST::AvgFloat32 => "Nullable(String)",
        ST::AvgFloat64 => "Nullable(String)",
        ST::AvgDecimal => "Nullable(String)",
        ST::SumUInt8 => "Nullable(String)",
        ST::SumUInt16 => "Nullable(String)",
        ST::SumUInt32 => "Nullable(String)",
        ST::SumUInt64 => "Nullable(String)",
        ST::SumUInt128 => "Nullable(String)",
        ST::SumUInt256 => "Nullable(String)",
        ST::SumInt8 => "Nullable(String)",
        ST::SumInt16 => "Nullable(String)",
        ST::SumInt32 => "Nullable(String)",
        ST::SumInt64 => "Nullable(String)",
        ST::SumInt128 => "Nullable(String)",
        ST::SumInt256 => "Nullable(String)",
        ST::SumFloat32 => "Nullable(String)",
        ST::SumFloat64 => "Nullable(String)",
        ST::SumDecimal => "Nullable(String)",
        ST::MaxUInt8 => "Nullable(String)",
        ST::MaxUInt16 => "Nullable(String)",
        ST::MaxUInt32 => "Nullable(String)",
        ST::MaxUInt64 => "Nullable(String)",
        ST::MaxUInt128 => "Nullable(String)",
        ST::MaxUInt256 => "Nullable(String)",
        ST::MaxInt8 => "Nullable(String)",
        ST::MaxInt16 => "Nullable(String)",
        ST::MaxInt32 => "Nullable(String)",
        ST::MaxInt64 => "Nullable(String)",
        ST::MaxInt128 => "Nullable(String)",
        ST::MaxInt256 => "Nullable(String)",
        ST::MaxFloat32 => "Nullable(String)",
        ST::MaxFloat64 => "Nullable(String)",
        ST::MaxDecimal => "Nullable(String)",
        ST::MinUInt8 => "Nullable(String)",
        ST::MinUInt16 => "Nullable(String)",
        ST::MinUInt32 => "Nullable(String)",
        ST::MinUInt64 => "Nullable(String)",
        ST::MinUInt128 => "Nullable(String)",
        ST::MinUInt256 => "Nullable(String)",
        ST::MinInt8 => "Nullable(String)",
        ST::MinInt16 => "Nullable(String)",
        ST::MinInt32 => "Nullable(String)",
        ST::MinInt64 => "Nullable(String)",
        ST::MinInt128 => "Nullable(String)",
        ST::MinInt256 => "Nullable(String)",
        ST::MinFloat32 => "Nullable(String)",
        ST::MinFloat64 => "Nullable(String)",
        ST::MinDecimal => "Nullable(String)",
        ST::MaxDate => "Nullable(String)",
        ST::MaxDate32 => "Nullable(String)",
        ST::MaxDateTime => "Nullable(String)",
        ST::MaxDateTime64 => "Nullable(String)",
        ST::MinDate => "Nullable(String)",
        ST::MinDate32 => "Nullable(String)",
        ST::MinDateTime => "Nullable(String)",
        ST::MinDateTime64 => "Nullable(String)",
    }
    .to_owned())
}

pub struct QueryBuilder<'request> {
    request: &'request gdc_rust_types::QueryRequest,
    bind_params: bool,
    parameters: IndexMap<String, BoundParam>,
    parameter_index: i32,
}

fn get_target_table(
    target: &gdc_rust_types::Target,
) -> Result<&gdc_rust_types::TableName, QueryBuilderError> {
    match target {
        gdc_rust_types::Target::Table { name } => Ok(name),
        gdc_rust_types::Target::Interpolated { id } => Err(QueryBuilderError::Internal(
            "Interpolated targets not supported".to_string(),
        )),
        gdc_rust_types::Target::Function { name, arguments } => Err(QueryBuilderError::Internal(
            "Function targets not yet supported".to_string(),
        )),
    }
}

impl<'request> QueryBuilder<'request> {
    fn new(request: &'request gdc_rust_types::QueryRequest, bind_params: bool) -> Self {
        Self {
            request,
            bind_params,
            parameters: IndexMap::new(),
            parameter_index: 0,
        }
    }
    pub fn build_sql_statement(
        request: &'request gdc_rust_types::QueryRequest,
        bind_params: bool,
    ) -> Result<Statement, QueryBuilderError> {
        let mut builder = Self::new(request, bind_params);

        let query = builder.root_query()?;

        let statement = Statement(query);

        Ok(statement)
    }
    fn table_relationship(
        &self,
        table: &gdc_rust_types::TableName,
        relationship_name: &str,
    ) -> Result<&'request gdc_rust_types::Relationship, QueryBuilderError> {
        let source_table = self
            .request
            .relationships
            .iter()
            .find(|table_relationships| table_relationships.source_table == *table)
            .ok_or_else(|| QueryBuilderError::TableMissing(table.to_owned()))?;

        let relationship = source_table
            .relationships
            .get(relationship_name)
            .ok_or_else(|| {
                QueryBuilderError::RelationshipMissingInTable(
                    relationship_name.to_owned(),
                    table.to_owned(),
                )
            })?;

        Ok(relationship)
    }
    fn root_query(&mut self) -> Result<Query, QueryBuilderError> {
        let query = &self.request.query;
        let table = get_target_table(&self.request.target)?;

        let root_subquery = match &self.request.foreach {
            Some(foreach) => {
                // todo: verify that all objects of the foreach collection have the same keys.
                // fail gracefully if not
                // handle the case where there are no objects in the foreach collection. Unsure if this could happen at all?

                let foreach_obj: IndexMap<String, Vec<_>> =
                    foreach
                        .iter()
                        .fold(IndexMap::new(), |mut accumulator, foreach_row| {
                            for (key, value) in foreach_row.iter() {
                                if let Some(foreach_column) = accumulator.get_mut(key) {
                                    foreach_column.push(value.value.to_owned());
                                } else {
                                    accumulator
                                        .insert(key.to_owned(), vec![value.value.to_owned()]);
                                }
                            }
                            accumulator
                        });
                let foreach_obj_json_string = serde_json::to_string(&foreach_obj)
                    .map_err(|err| QueryBuilderError::Internal(err.to_string()))?;

                let foreach_expr = Function {
                    name: ObjectName(vec![Ident::unquoted("format")]),
                    args: vec![
                        FunctionArgExpr::Expr(Expr::Identifier(Ident::unquoted("JSONColumns"))),
                        FunctionArgExpr::Expr(Expr::Value(Value::SingleQuotedString(
                            foreach_obj_json_string,
                        ))),
                    ],
                    over: None,
                    distinct: false,
                };

                let foreach_table = TableFactor::TableFunction {
                    function: foreach_expr,
                    alias: Some(Ident::quoted("_foreach")),
                };
                let foreach_columns: Vec<_> = foreach[0].keys().collect();

                self.query_subquery(
                    table,
                    &vec![],
                    query,
                    Some((foreach_table, &foreach_columns)),
                )?
            }
            None => self.query_subquery(table, &vec![], query, None)?,
        };

        let query_expr =
            Expr::CompoundIdentifier(vec![Ident::quoted("_query"), Ident::quoted("query")]);

        let root_projection = if self.request.foreach.is_some() {
            vec![SelectItem::ExprWithAlias {
                expr: sql_function(
                    "cast",
                    vec![
                        sql_function(
                            "tupleElement",
                            vec![query_expr, Expr::Value(Value::Number("1".to_owned()))],
                        ),
                        Expr::Value(Value::SingleQuotedString(root_foreach_row_type(query)?)),
                    ],
                ),
                alias: Ident::quoted("rows"),
            }]
        } else {
            match (&query.fields, &query.aggregates) {
                (None, None) => vec![SelectItem::UnnamedExpr(Expr::Value(Value::Null))],
                (None, Some(aggregates)) => {
                    vec![SelectItem::ExprWithAlias {
                        expr: sql_function(
                            "cast",
                            vec![
                                sql_function(
                                    "tupleElement",
                                    vec![query_expr, Expr::Value(Value::Number("1".to_owned()))],
                                ),
                                Expr::Value(Value::SingleQuotedString(root_aggregates_type(
                                    aggregates,
                                )?)),
                            ],
                        ),
                        alias: Ident::quoted("aggregates"),
                    }]
                }
                (Some(fields), None) => {
                    vec![SelectItem::ExprWithAlias {
                        expr: sql_function(
                            "cast",
                            vec![
                                sql_function(
                                    "tupleElement",
                                    vec![query_expr, Expr::Value(Value::Number("1".to_owned()))],
                                ),
                                Expr::Value(Value::SingleQuotedString(root_rows_type(fields)?)),
                            ],
                        ),
                        alias: Ident::quoted("rows"),
                    }]
                }
                (Some(fields), Some(aggregates)) => {
                    vec![
                        SelectItem::ExprWithAlias {
                            expr: sql_function(
                                "cast",
                                vec![
                                    sql_function(
                                        "tupleElement",
                                        vec![
                                            query_expr.clone(),
                                            Expr::Value(Value::Number("1".to_owned())),
                                        ],
                                    ),
                                    Expr::Value(Value::SingleQuotedString(root_rows_type(fields)?)),
                                ],
                            ),
                            alias: Ident::quoted("rows"),
                        },
                        SelectItem::ExprWithAlias {
                            expr: sql_function(
                                "cast",
                                vec![
                                    sql_function(
                                        "tupleElement",
                                        vec![
                                            query_expr,
                                            Expr::Value(Value::Number("2".to_owned())),
                                        ],
                                    ),
                                    Expr::Value(Value::SingleQuotedString(root_aggregates_type(
                                        aggregates,
                                    )?)),
                                ],
                            ),
                            alias: Ident::quoted("aggregates"),
                        },
                    ]
                }
            }
        };

        let root_from = vec![TableWithJoins {
            relation: TableFactor::Derived {
                subquery: root_subquery,
                alias: Some(Ident::quoted("_query")),
            },
            joins: vec![],
        }];

        Ok(Query::new(root_projection).from(root_from))
    }
    fn query_subquery(
        &mut self,
        table: &gdc_rust_types::TableName,
        join_cols: &Vec<&String>,
        query: &gdc_rust_types::Query,
        foreach: Option<(TableFactor, &[&String])>,
    ) -> Result<Box<Query>, QueryBuilderError> {
        let foreach_columns = foreach
            .as_ref()
            .map(|(_, foreach_columns)| *foreach_columns);
        let (rows_subquery, rows_expr) = match &query.fields {
            None => (None, None),
            Some(fields) => {
                let rows_subquery =
                    self.rows_subquery(table, join_cols, fields, query, &foreach_columns)?;
                let rows_expr =
                    Expr::CompoundIdentifier(vec![Ident::quoted("_rows"), Ident::quoted("rows")]);
                (Some(rows_subquery), Some(rows_expr))
            }
        };
        let (aggregates_subquery, aggregates_expr) = match &query.aggregates {
            None => (None, None),
            Some(aggregates) => {
                let aggregates_subquery = self.aggregates_subquery(
                    table,
                    join_cols,
                    aggregates,
                    query,
                    &foreach_columns,
                )?;
                let aggregates_expr = Expr::CompoundIdentifier(vec![
                    Ident::quoted("_aggregates"),
                    Ident::quoted("aggregates"),
                ]);
                (Some(aggregates_subquery), Some(aggregates_expr))
            }
        };

        let query_expr = match (rows_expr, aggregates_expr) {
            (None, None) => sql_function("map", vec![]),
            (None, Some(aggregates_expr)) => sql_function("tuple", vec![aggregates_expr]),
            (Some(rows_expr), None) => sql_function("tuple", vec![rows_expr]),
            (Some(rows_expr), Some(aggregates_expr)) => {
                sql_function("tuple", vec![rows_expr, aggregates_expr])
            }
        };

        let base_expr = if foreach.is_some() {
            sql_function(
                "tuple",
                vec![sql_function(
                    "groupArray",
                    vec![sql_function("tuple", vec![query_expr])],
                )],
            )
        } else {
            query_expr
        };

        let base_select_item = SelectItem::ExprWithAlias {
            expr: base_expr,
            alias: Ident::quoted("query"),
        };

        let query_projection = vec![base_select_item]
            .into_iter()
            .chain(join_cols.iter().map(|col| SelectItem::ExprWithAlias {
                expr: Expr::CompoundIdentifier(vec![Ident::quoted(format!("_selection.{col}"))]),
                alias: Ident::quoted(format!("_selection.{col}")),
            }))
            .collect();

        // note: if rows not required. join not required either
        // also note: will need to change this cross join for subqueries that do have some kind of predicate
        let query_from = match foreach {
            Some((foreach_table, foreach_columns)) => {
                let rows_join = rows_subquery.map(|rows_subquery| {
                    let join_expr = foreach_columns
                        .iter()
                        .map(|&col| {
                            let left = Expr::CompoundIdentifier(vec![
                                Ident::quoted("_foreach"),
                                Ident::quoted(col),
                            ]);
                            let right = Expr::CompoundIdentifier(vec![
                                Ident::quoted("_rows"),
                                Ident::quoted(format!("_foreach.{}", col)),
                            ]);
                            Expr::BinaryOp {
                                left: Box::new(left),
                                op: BinaryOperator::Eq,
                                right: Box::new(right),
                            }
                        })
                        .reduce(and_reducer)
                        .unwrap_or(Expr::Value(Value::Boolean(true)));
                    Join {
                        relation: TableFactor::Derived {
                            subquery: rows_subquery,
                            alias: Some(Ident::quoted("_rows")),
                        },
                        join_operator: JoinOperator::LeftOuter(JoinConstraint::On(join_expr)),
                    }
                });
                let aggregates_join = aggregates_subquery.map(|aggregates_subquery| {
                    let join_expr = foreach_columns
                        .iter()
                        .map(|&col| {
                            let left = Expr::CompoundIdentifier(vec![
                                Ident::quoted("_foreach"),
                                Ident::quoted(col),
                            ]);
                            let right = Expr::CompoundIdentifier(vec![
                                Ident::quoted("_aggregates"),
                                Ident::quoted(format!("_foreach.{}", col)),
                            ]);
                            Expr::BinaryOp {
                                left: Box::new(left),
                                op: BinaryOperator::Eq,
                                right: Box::new(right),
                            }
                        })
                        .reduce(and_reducer)
                        .unwrap_or(Expr::Value(Value::Boolean(true)));
                    Join {
                        relation: TableFactor::Derived {
                            subquery: aggregates_subquery,
                            alias: Some(Ident::quoted("_aggregates")),
                        },
                        join_operator: JoinOperator::LeftOuter(JoinConstraint::On(join_expr)),
                    }
                });

                let joins = match (rows_join, aggregates_join) {
                    (None, None) => vec![],
                    (None, Some(aggregates_join)) => vec![aggregates_join],
                    (Some(rows_join), None) => vec![rows_join],
                    (Some(rows_join), Some(aggregates_join)) => vec![rows_join, aggregates_join],
                };

                vec![TableWithJoins {
                    relation: foreach_table,
                    joins,
                }]
            }
            None => match (rows_subquery, aggregates_subquery) {
                (None, None) => vec![],
                (None, Some(aggregates_subquery)) => vec![TableWithJoins {
                    relation: TableFactor::Derived {
                        subquery: aggregates_subquery,
                        alias: Some(Ident::quoted("_aggregates")),
                    },
                    joins: vec![],
                }],
                (Some(rows_subquery), None) => vec![TableWithJoins {
                    relation: TableFactor::Derived {
                        subquery: rows_subquery,
                        alias: Some(Ident::quoted("_rows")),
                    },
                    joins: vec![],
                }],
                (Some(rows_subquery), Some(aggregates_subquery)) => vec![TableWithJoins {
                    relation: TableFactor::Derived {
                        subquery: rows_subquery,
                        alias: Some(Ident::quoted("_rows")),
                    },
                    joins: vec![Join {
                        relation: TableFactor::Derived {
                            subquery: aggregates_subquery,
                            alias: Some(Ident::quoted("_aggregates")),
                        },
                        join_operator: if join_cols.is_empty() {
                            JoinOperator::CrossJoin
                        } else {
                            let cols = join_cols
                                .iter()
                                .map(|col| Ident::quoted(format!("_selection.{col}")))
                                .collect();
                            JoinOperator::FullOuter(JoinConstraint::Using(cols))
                        },
                    }],
                }],
            },
        };

        Ok(Query::new(query_projection).from(query_from).boxed())
    }
    fn rows_subquery(
        &mut self,
        table: &gdc_rust_types::TableName,
        join_cols: &[&String],
        fields: &IndexMap<String, gdc_rust_types::Field>,
        query: &gdc_rust_types::Query,
        foreach_columns: &Option<&[&String]>,
    ) -> Result<Box<Query>, QueryBuilderError> {
        let row_subquery = self.row_subquery(table, join_cols, fields, query, foreach_columns)?;

        let column_exprs = fields
            .iter()
            .map(|(alias, _)| {
                (
                    alias.clone(),
                    Expr::CompoundIdentifier(vec![
                        Ident::quoted("_row"),
                        Ident::quoted(format!("_projection.{alias}")),
                    ]),
                )
            })
            .collect::<Vec<_>>();

        let rows_projection = join_cols
            .iter()
            .map(|col| SelectItem::ExprWithAlias {
                expr: Expr::CompoundIdentifier(vec![
                    Ident::quoted("_row"),
                    Ident::quoted(format!("_selection.{col}")),
                ]),
                alias: Ident::quoted(format!("_selection.{col}")),
            })
            .chain(vec![SelectItem::ExprWithAlias {
                expr: if column_exprs.is_empty() {
                    sql_function("groupArray", vec![sql_function("map", vec![])])
                } else {
                    sql_function(
                        "groupArray",
                        vec![sql_function(
                            "tuple",
                            column_exprs.into_iter().map(|(_, expr)| expr).collect(),
                        )],
                    )
                },
                alias: Ident::quoted("rows"),
            }]);

        let rows_projection = if let Some(foreach_columns) = foreach_columns {
            rows_projection
                .chain(foreach_columns.iter().map(|col| {
                    SelectItem::UnnamedExpr(Expr::CompoundIdentifier(vec![
                        Ident::quoted("_row"),
                        Ident::quoted(format!("_foreach.{col}")),
                    ]))
                }))
                .collect()
        } else {
            rows_projection.collect()
        };

        let rows_from = vec![TableWithJoins {
            relation: TableFactor::Derived {
                subquery: row_subquery,
                alias: Some(Ident::quoted("_row")),
            },
            joins: vec![],
        }];

        let rows_group_by = join_cols.iter().map(|&col| {
            Expr::CompoundIdentifier(vec![
                Ident::quoted("_row"),
                Ident::quoted(format!("_selection.{col}")),
            ])
        });

        let rows_group_by = if let Some(foreach_columns) = foreach_columns {
            rows_group_by
                .chain(foreach_columns.iter().map(|col| {
                    Expr::CompoundIdentifier(vec![
                        Ident::quoted("_row"),
                        Ident::quoted(format!("_foreach.{col}")),
                    ])
                }))
                .collect()
        } else {
            rows_group_by.collect()
        };

        Ok(Query::new(rows_projection)
            .from(rows_from)
            .group_by(rows_group_by)
            .boxed())
    }
    fn row_subquery(
        &mut self,
        table: &gdc_rust_types::TableName,
        join_cols: &[&String],
        fields: &IndexMap<String, gdc_rust_types::Field>,
        query: &gdc_rust_types::Query,
        foreach_columns: &Option<&[&String]>,
    ) -> Result<Box<Query>, QueryBuilderError> {
        let selection_columns_expressions =
            join_cols.iter().map(|&col| SelectItem::ExprWithAlias {
                expr: Expr::CompoundIdentifier(vec![Ident::quoted("_origin"), Ident::quoted(col)]),
                alias: Ident::quoted(format!("_selection.{col}")),
            });

        let row_columns_expressions = fields
            .iter()
            .map(|(alias, field)| match field {
                gdc_rust_types::Field::Column {
                    column,
                    column_type,
                } => {
                    let identifier = Expr::CompoundIdentifier(vec![
                        Ident::quoted("_origin"),
                        Ident::quoted(column),
                    ]);
                    let column_type = schema::ScalarType::from_str(column_type).map_err(|_| {
                        QueryBuilderError::UnknownScalarType(column_type.to_owned())
                    })?;

                    let expr = match column_type {
                        schema::ScalarType::Unknown => {
                            sql_function("toJSONString", vec![identifier])
                        }
                        _ => identifier,
                    };
                    Ok(SelectItem::ExprWithAlias {
                        expr,
                        alias: Ident::quoted(format!("_projection.{alias}")),
                    })
                }
                gdc_rust_types::Field::Relationship { .. } => Ok(SelectItem::ExprWithAlias {
                    expr: Expr::CompoundIdentifier(vec![
                        Ident::quoted(format!("_rel.{alias}")),
                        Ident::quoted("query"),
                    ]),
                    alias: Ident::quoted(format!("_projection.{alias}")),
                }),
                gdc_rust_types::Field::Object { .. } => Err(QueryBuilderError::Internal(
                    "Object fields not supported".to_string(),
                )),
                gdc_rust_types::Field::Array { .. } => Err(QueryBuilderError::Internal(
                    "Array fields not supported".to_string(),
                )),
            })
            .collect::<Result<Vec<_>, _>>()?;

        let row_foreach_column_expressions = match foreach_columns {
            Some(foreach_columns) => foreach_columns
                .iter()
                .map(|&col| SelectItem::ExprWithAlias {
                    expr: Expr::CompoundIdentifier(vec![
                        Ident::quoted("_origin"),
                        Ident::quoted(col),
                    ]),
                    alias: Ident::quoted(format!("_foreach.{col}")),
                })
                .collect(),
            None => vec![],
        };

        let (row_order_by, order_by_joins) =
            self.order_by_expressions_joins(table, &query.order_by)?;

        let partition_cols = match foreach_columns {
            Some(foreach_columns) => join_cols.iter().chain(*foreach_columns).copied().collect(),
            None => join_cols.to_vec(),
        };

        let row_projection = selection_columns_expressions
            .chain(row_columns_expressions)
            .chain(row_foreach_column_expressions)
            .collect::<Vec<_>>();

        let row_projection = if row_projection.is_empty() {
            vec![SelectItem::UnnamedExpr(Expr::Value(Value::Null))]
        } else {
            row_projection
        };

        let (row_selection, exists_joins) = match &query.r#where {
            Some(expression) => {
                let mut exists_index = 0;
                let (expr, joins) = self.selection_expression(
                    expression,
                    &mut exists_index,
                    true,
                    "_origin",
                    table,
                )?;
                (Some(expr), joins)
            }
            None => (None, vec![]),
        };

        let relationship_joins = fields
            .iter()
            .filter_map(|(alias, field)| match field {
                gdc_rust_types::Field::Relationship {
                    query,
                    relationship,
                } => Some((alias, query, relationship)),
                _ => None,
            })
            .map(|(alias, query, relationship)| {
                let relationship = self.table_relationship(table, relationship)?;

                let join_expr = relationship
                    .column_mapping
                    .iter()
                    .map(|(source_col, target_col)| Expr::BinaryOp {
                        left: Box::new(Expr::CompoundIdentifier(vec![
                            Ident::quoted("_origin"),
                            Ident::quoted(source_col),
                        ])),
                        op: BinaryOperator::Eq,
                        right: Box::new(Expr::CompoundIdentifier(vec![
                            Ident::quoted(format!("_rel.{alias}")),
                            Ident::quoted(format!("_selection.{target_col}")),
                        ])),
                    })
                    .reduce(and_reducer)
                    .unwrap_or(Expr::Value(Value::Boolean(true)));

                let join_cols = &relationship.column_mapping.values().collect();

                let relationship_table = get_target_table(&relationship.target)?;

                Ok(Join {
                    relation: TableFactor::Derived {
                        subquery: self.query_subquery(
                            relationship_table,
                            join_cols,
                            query,
                            None,
                        )?,
                        alias: Some(Ident::quoted(format!("_rel.{alias}"))),
                    },
                    join_operator: JoinOperator::LeftOuter(JoinConstraint::On(join_expr)),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let row_from = vec![TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName(table.iter().map(Ident::quoted).collect()),
                alias: Some(Ident::quoted("_origin")),
            },
            joins: relationship_joins
                .into_iter()
                .chain(order_by_joins)
                .chain(exists_joins)
                .collect(),
        }];

        let partion_rows_by = partition_cols
            .into_iter()
            .map(|col| Expr::CompoundIdentifier(vec![Ident::quoted("_origin"), Ident::quoted(col)]))
            .collect::<Vec<_>>();

        let (limit_by, limit, offset) =
            self.limit_by_limit_offset(partion_rows_by, &query.limit, &query.offset);

        Ok(Query::new(row_projection)
            .from(row_from)
            .predicate(row_selection)
            .order_by(row_order_by)
            .limit_by(limit_by)
            .limit(limit)
            .offset(offset)
            .boxed())
    }
    fn aggregates_subquery(
        &mut self,
        table: &gdc_rust_types::TableName,
        join_cols: &[&String],
        aggregates: &IndexMap<String, gdc_rust_types::Aggregate>,
        query: &gdc_rust_types::Query,
        foreach_columns: &Option<&[&String]>,
    ) -> Result<Box<Query>, QueryBuilderError> {
        let aggregate_subquery =
            self.aggregate_subquery(table, join_cols, aggregates, query, foreach_columns)?;
        let column_exprs = aggregates
            .iter()
            .map(|(alias, field)| {
                let colum_expr = match field {
                    gdc_rust_types::Aggregate::StarCount {} => Expr::Function(Function {
                        name: ObjectName(vec![Ident::unquoted("COUNT")]),
                        args: vec![FunctionArgExpr::Wildcard],
                        over: None,
                        distinct: false,
                    }),
                    gdc_rust_types::Aggregate::ColumnCount {
                        column: _,
                        distinct,
                    } => {
                        let column = Expr::CompoundIdentifier(vec![
                            Ident::quoted("_row"),
                            Ident::quoted(format!("_projection.{alias}")),
                        ]);
                        Expr::Function(Function {
                            name: ObjectName(vec![Ident::unquoted("COUNT")]),
                            args: vec![FunctionArgExpr::Expr(column)],
                            over: None,
                            distinct: distinct.to_owned(),
                        })
                    }
                    gdc_rust_types::Aggregate::SingleColumn { function, .. } => {
                        let column = Expr::CompoundIdentifier(vec![
                            Ident::quoted("_row"),
                            Ident::quoted(format!("_projection.{alias}")),
                        ]);
                        let function = schema::SingleColumnAggregateFunction::from_str(function)
                            .map_err(|_| {
                                QueryBuilderError::UnknownSingleColumnAggregateFunction(
                                    function.to_owned(),
                                )
                            })?;
                        single_column_aggregate(&function, column)
                    }
                };

                Ok((alias.clone(), colum_expr))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let aggregates_projection = join_cols
            .iter()
            .map(|col| SelectItem::ExprWithAlias {
                expr: Expr::CompoundIdentifier(vec![
                    Ident::quoted("_row"),
                    Ident::quoted(format!("_selection.{col}")),
                ]),
                alias: Ident::quoted(format!("_selection.{col}")),
            })
            .chain(vec![SelectItem::ExprWithAlias {
                expr: if column_exprs.is_empty() {
                    sql_function("map", vec![])
                } else {
                    sql_function(
                        "tuple",
                        column_exprs.into_iter().map(|(_, expr)| expr).collect(),
                    )
                },
                alias: Ident::quoted("aggregates"),
            }]);

        let aggregates_projection = if let Some(foreach_columns) = foreach_columns {
            aggregates_projection
                .chain(foreach_columns.iter().map(|col| {
                    SelectItem::UnnamedExpr(Expr::CompoundIdentifier(vec![
                        Ident::quoted("_row"),
                        Ident::quoted(format!("_foreach.{col}")),
                    ]))
                }))
                .collect()
        } else {
            aggregates_projection.collect()
        };

        let aggregates_from = vec![TableWithJoins {
            relation: TableFactor::Derived {
                subquery: aggregate_subquery,
                alias: Some(Ident::quoted("_row")),
            },
            joins: vec![],
        }];

        let aggregates_group_by = join_cols.iter().map(|&col| {
            Expr::CompoundIdentifier(vec![
                Ident::quoted("_row"),
                Ident::quoted(format!("_selection.{col}")),
            ])
        });

        let aggregates_group_by = if let Some(foreach_columns) = foreach_columns {
            aggregates_group_by
                .chain(foreach_columns.iter().map(|col| {
                    Expr::CompoundIdentifier(vec![
                        Ident::quoted("_row"),
                        Ident::quoted(format!("_foreach.{col}")),
                    ])
                }))
                .collect()
        } else {
            aggregates_group_by.collect()
        };

        Ok(Query::new(aggregates_projection)
            .from(aggregates_from)
            .group_by(aggregates_group_by)
            .boxed())
    }
    fn aggregate_subquery(
        &mut self,
        table: &gdc_rust_types::TableName,
        join_cols: &[&String],
        aggregates: &IndexMap<String, gdc_rust_types::Aggregate>,
        query: &gdc_rust_types::Query,
        foreach_columns: &Option<&[&String]>,
    ) -> Result<Box<Query>, QueryBuilderError> {
        let selection_columns_expressions =
            join_cols.iter().map(|&col| SelectItem::ExprWithAlias {
                expr: Expr::CompoundIdentifier(vec![Ident::quoted("_origin"), Ident::quoted(col)]),
                alias: Ident::quoted(format!("_selection.{col}")),
            });

        let aggregate_columns_expressions =
            aggregates.iter().filter_map(|(alias, agg)| match agg {
                gdc_rust_types::Aggregate::ColumnCount { column, .. }
                | gdc_rust_types::Aggregate::SingleColumn { column, .. } => {
                    Some(SelectItem::ExprWithAlias {
                        expr: Expr::CompoundIdentifier(vec![
                            Ident::quoted("_origin"),
                            Ident::quoted(column),
                        ]),
                        alias: Ident::quoted(format!("_projection.{alias}")),
                    })
                }
                gdc_rust_types::Aggregate::StarCount {} => None,
            });

        let aggregate_foreach_column_expressions = match foreach_columns {
            Some(foreach_columns) => foreach_columns
                .iter()
                .map(|&col| SelectItem::ExprWithAlias {
                    expr: Expr::CompoundIdentifier(vec![
                        Ident::quoted("_origin"),
                        Ident::quoted(col),
                    ]),
                    alias: Ident::quoted(format!("_foreach.{col}")),
                })
                .collect(),
            None => vec![],
        };

        let (order_by, order_by_joins) = self.order_by_expressions_joins(table, &query.order_by)?;

        let partition_cols = match foreach_columns {
            Some(foreach_columns) => join_cols.iter().chain(*foreach_columns).copied().collect(),
            None => join_cols.to_vec(),
        };

        let aggregate_projection = selection_columns_expressions
            .chain(aggregate_columns_expressions)
            .chain(aggregate_foreach_column_expressions)
            .collect::<Vec<_>>();

        let aggregate_projection = if aggregate_projection.is_empty() {
            vec![SelectItem::UnnamedExpr(Expr::Value(Value::Null))]
        } else {
            aggregate_projection
        };

        let (aggregate_selection, exists_joins) = match &query.r#where {
            Some(expression) => {
                let mut exists_index = 0;
                let (expr, joins) = self.selection_expression(
                    expression,
                    &mut exists_index,
                    true,
                    "_origin",
                    table,
                )?;
                (Some(expr), joins)
            }
            None => (None, vec![]),
        };

        let aggregate_from = vec![TableWithJoins {
            relation: TableFactor::Table {
                name: ObjectName(table.iter().map(Ident::quoted).collect()),
                alias: Some(Ident::quoted("_origin")),
            },
            joins: exists_joins.into_iter().chain(order_by_joins).collect(),
        }];

        let partion_rows_by = partition_cols
            .into_iter()
            .map(|col| Expr::CompoundIdentifier(vec![Ident::quoted("_origin"), Ident::quoted(col)]))
            .collect::<Vec<_>>();

        let (limit_by, limit, offset) =
            self.limit_by_limit_offset(partion_rows_by, &query.aggregates_limit, &query.offset);

        Ok(Query::new(aggregate_projection)
            .from(aggregate_from)
            .predicate(aggregate_selection)
            .order_by(order_by)
            .limit_by(limit_by)
            .limit(limit)
            .offset(offset)
            .boxed())
    }
    fn order_by_expressions_joins(
        &mut self,
        table: &gdc_rust_types::TableName,
        order_by: &Option<gdc_rust_types::OrderBy>,
    ) -> Result<(Vec<OrderByExpr>, Vec<Join>), QueryBuilderError> {
        match order_by {
            None => Ok((vec![], vec![])),
            Some(order_by) => {
                // discard parent columns at the root level, since all columns are exposed on origin
                let (_, order_by_joins) =
                    self.order_by_joins(table, &vec![], &order_by.relations, order_by)?;

                let order_by = order_by
                    .elements
                    .iter()
                    .map(|element| {
                        let table_alias = if element.target_path.is_empty() {
                            "_origin".to_string()
                        } else {
                            format!("_ord.{}", element.target_path.join("."))
                        };
                        let column_alias = match &element.target {
                            gdc_rust_types::OrderByTarget::StarCountAggregate {} => {
                                "_count".to_string()
                            }
                            gdc_rust_types::OrderByTarget::SingleColumnAggregate {
                                column,
                                function,
                                result_type: _,
                            } => {
                                let function =
                                    schema::SingleColumnAggregateFunction::from_str(function)
                                        .map_err(|_| {
                                            QueryBuilderError::UnknownSingleColumnAggregateFunction(
                                                function.to_owned(),
                                            )
                                        })?;
                                format!("_agg.{}.{}", function_name(&function), column)
                            }

                            gdc_rust_types::OrderByTarget::Column { column } => {
                                let column = match column {
                                    gdc_rust_types::ColumnSelector::Compound(name) => {
                                        return Err(QueryBuilderError::Internal(format!(
                                            "Compound column selector not supported: {}",
                                            name.join(".")
                                        )))
                                    }
                                    gdc_rust_types::ColumnSelector::Name(name) => name,
                                };
                                if element.target_path.is_empty() {
                                    column.to_owned()
                                } else {
                                    format!("_col.{column}")
                                }
                            }
                        };

                        self.order_by_expr(&table_alias, &column_alias, element)
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok((order_by, order_by_joins))
            }
        }
    }
    fn order_by_expr(
        &mut self,
        table_alias: &str,
        column_alias: &str,
        order_by_element: &gdc_rust_types::OrderByElement,
    ) -> Result<OrderByExpr, QueryBuilderError> {
        let column = Expr::CompoundIdentifier(vec![
            Ident::quoted(table_alias),
            Ident::quoted(column_alias),
        ]);
        let expr = match &order_by_element.target {
            // default to sorting on 0 for count(*)
            gdc_rust_types::OrderByTarget::StarCountAggregate {} => sql_function(
                "COALESCE",
                vec![column, Expr::Value(Value::Number("0".to_owned()))],
            ),
            // sort on default value for aggregates
            gdc_rust_types::OrderByTarget::SingleColumnAggregate { result_type, .. } => {
                let result_type = schema::ScalarType::from_str(result_type)
                    .map_err(|_| QueryBuilderError::UnknownScalarType(result_type.to_owned()))?;
                use schema::ScalarType as ST;
                let default_sorting_value = match result_type {
                    ST::Bool => Value::Null,
                    ST::String | ST::FixedString => Value::SingleQuotedString("".to_owned()),
                    ST::UInt8
                    | ST::UInt16
                    | ST::UInt32
                    | ST::UInt64
                    | ST::UInt128
                    | ST::UInt256
                    | ST::Int8
                    | ST::Int16
                    | ST::Int32
                    | ST::Int64
                    | ST::Int128
                    | ST::Int256
                    | ST::Float32
                    | ST::Float64
                    | ST::Decimal => Value::Number("0".to_owned()),
                    ST::Date | ST::Date32 | ST::DateTime | ST::DateTime64 => Value::Null,
                    ST::Json => Value::Null,
                    ST::Uuid => Value::Null,
                    ST::IPv4 | ST::IPv6 => Value::Null,
                    ST::Unknown => Value::Null,
                    ST::AvgUInt8
                    | ST::AvgUInt16
                    | ST::AvgUInt32
                    | ST::AvgUInt64
                    | ST::AvgUInt128
                    | ST::AvgUInt256
                    | ST::AvgInt8
                    | ST::AvgInt16
                    | ST::AvgInt32
                    | ST::AvgInt64
                    | ST::AvgInt128
                    | ST::AvgInt256
                    | ST::AvgFloat32
                    | ST::AvgFloat64
                    | ST::AvgDecimal
                    | ST::SumUInt8
                    | ST::SumUInt16
                    | ST::SumUInt32
                    | ST::SumUInt64
                    | ST::SumUInt128
                    | ST::SumUInt256
                    | ST::SumInt8
                    | ST::SumInt16
                    | ST::SumInt32
                    | ST::SumInt64
                    | ST::SumInt128
                    | ST::SumInt256
                    | ST::SumFloat32
                    | ST::SumFloat64
                    | ST::SumDecimal
                    | ST::MaxUInt8
                    | ST::MaxUInt16
                    | ST::MaxUInt32
                    | ST::MaxUInt64
                    | ST::MaxUInt128
                    | ST::MaxUInt256
                    | ST::MaxInt8
                    | ST::MaxInt16
                    | ST::MaxInt32
                    | ST::MaxInt64
                    | ST::MaxInt128
                    | ST::MaxInt256
                    | ST::MaxFloat32
                    | ST::MaxFloat64
                    | ST::MaxDecimal
                    | ST::MinUInt8
                    | ST::MinUInt16
                    | ST::MinUInt32
                    | ST::MinUInt64
                    | ST::MinUInt128
                    | ST::MinUInt256
                    | ST::MinInt8
                    | ST::MinInt16
                    | ST::MinInt32
                    | ST::MinInt64
                    | ST::MinInt128
                    | ST::MinInt256
                    | ST::MinFloat32
                    | ST::MinFloat64
                    | ST::MinDecimal
                    | ST::MaxDate
                    | ST::MaxDate32
                    | ST::MaxDateTime
                    | ST::MaxDateTime64
                    | ST::MinDate
                    | ST::MinDate32
                    | ST::MinDateTime
                    | ST::MinDateTime64 => Value::Null,
                };
                sql_function("COALESCE", vec![column, Expr::Value(default_sorting_value)])
            }
            gdc_rust_types::OrderByTarget::Column { .. } => column,
        };
        Ok(OrderByExpr {
            expr,
            asc: Some(match order_by_element.order_direction {
                gdc_rust_types::OrderDirection::Asc => true,
                gdc_rust_types::OrderDirection::Desc => false,
            }),
            nulls_first: Some(match order_by_element.order_direction {
                gdc_rust_types::OrderDirection::Asc => false,
                gdc_rust_types::OrderDirection::Desc => true,
            }),
        })
    }
    fn order_by_joins(
        &mut self,
        table: &gdc_rust_types::TableName,
        source_path: &Vec<String>,
        relations: &IndexMap<String, gdc_rust_types::OrderByRelation>,
        order_by: &gdc_rust_types::OrderBy,
    ) -> Result<(Vec<String>, Vec<Join>), QueryBuilderError> {
        let mut joins = vec![];
        let mut parent_join_columns = vec![];
        let parent_alias = if source_path.is_empty() {
            "_origin".to_string()
        } else {
            format!("_ord.{}", source_path.join("."))
        };
        for (relationship_name, order_by_relation) in relations {
            let relationship = self.table_relationship(table, relationship_name)?;

            // parent table will need to expose these columns for this table to join on
            for column in relationship.column_mapping.keys() {
                if !parent_join_columns.contains(column) {
                    parent_join_columns.push(column.clone());
                }
            }

            let child_path = [&source_path[..], &[relationship_name.to_owned()]].concat();
            let child_alias = format!("_ord.{}", child_path.join("."));

            let relationship_table = get_target_table(&relationship.target)?;

            // child columns will be used by subsequent joins to join to this table
            let (child_columns, child_joins) = self.order_by_joins(
                relationship_table,
                &child_path,
                &order_by_relation.subrelations,
                order_by,
            )?;

            let mut projection_cols = IndexMap::new();
            let mut group_by_cols = IndexMap::new();

            for element in &order_by.elements {
                if element.target_path == child_path {
                    // add the column to the projection
                    let col_alias = match &element.target {
                        gdc_rust_types::OrderByTarget::StarCountAggregate {} => {
                            "_count".to_string()
                        }
                        gdc_rust_types::OrderByTarget::SingleColumnAggregate {
                            column,
                            function,
                            result_type: _,
                        } => {
                            let function = schema::SingleColumnAggregateFunction::from_str(
                                function,
                            )
                            .map_err(|_| {
                                QueryBuilderError::UnknownSingleColumnAggregateFunction(
                                    function.to_owned(),
                                )
                            })?;
                            format!("_agg.{}.{}", function_name(&function), column)
                        }

                        gdc_rust_types::OrderByTarget::Column { column } => {
                            let column = match column {
                                gdc_rust_types::ColumnSelector::Compound(name) => {
                                    return Err(QueryBuilderError::Internal(format!(
                                        "Compound column selector not supported: {}",
                                        name.join(".")
                                    )))
                                }
                                gdc_rust_types::ColumnSelector::Name(name) => name,
                            };
                            format!("_col.{column}")
                        }
                    };
                    let projection_expr = match &element.target {
                        gdc_rust_types::OrderByTarget::StarCountAggregate {} => {
                            Expr::Function(Function {
                                name: ObjectName(vec![Ident::unquoted("COUNT")]),
                                args: vec![FunctionArgExpr::Wildcard],
                                over: None,
                                distinct: false,
                            })
                        }
                        gdc_rust_types::OrderByTarget::SingleColumnAggregate {
                            column,
                            function,
                            result_type: _,
                        } => {
                            let column_expr = Expr::Identifier(Ident::quoted(column));
                            let function = schema::SingleColumnAggregateFunction::from_str(
                                function,
                            )
                            .map_err(|_| {
                                QueryBuilderError::UnknownSingleColumnAggregateFunction(
                                    function.to_owned(),
                                )
                            })?;
                            single_column_aggregate(&function, column_expr)
                        }
                        gdc_rust_types::OrderByTarget::Column { column } => {
                            let column = match column {
                                gdc_rust_types::ColumnSelector::Compound(name) => {
                                    return Err(QueryBuilderError::Internal(format!(
                                        "Compound column selector not supported: {}",
                                        name.join(".")
                                    )))
                                }
                                gdc_rust_types::ColumnSelector::Name(name) => name,
                            };
                            Expr::Identifier(Ident::quoted(column))
                        }
                    };
                    let projection_col = SelectItem::ExprWithAlias {
                        expr: projection_expr,
                        alias: Ident::quoted(&col_alias),
                    };
                    projection_cols.insert(col_alias, projection_col);
                    // add the column to the group by clause, if it's not an aggregate
                    if let gdc_rust_types::OrderByTarget::Column { column } = &element.target {
                        let column = match column {
                            gdc_rust_types::ColumnSelector::Compound(name) => {
                                return Err(QueryBuilderError::Internal(format!(
                                    "Compound column selector not supported: {}",
                                    name.join(".")
                                )))
                            }
                            gdc_rust_types::ColumnSelector::Name(name) => name,
                        };
                        let group_by_col = Expr::Identifier(Ident::quoted(column));
                        group_by_cols.insert(column, group_by_col);
                    }
                }
            }

            // add columns needed joining to the parent table to the projection and group by, if not duplicates
            for column in relationship.column_mapping.values() {
                let col_alias = format!("_col.{column}");
                if !projection_cols.contains_key(&col_alias) {
                    let projection_col = SelectItem::ExprWithAlias {
                        expr: Expr::Identifier(Ident::quoted(column)),
                        alias: Ident::quoted(col_alias.clone()),
                    };
                    projection_cols.insert(col_alias, projection_col);
                }
                if !group_by_cols.contains_key(column) {
                    let group_by_col = Expr::Identifier(Ident::quoted(column));
                    group_by_cols.insert(column, group_by_col);
                }
            }

            for column in &child_columns {
                let col_alias = format!("_col.{column}");
                if !projection_cols.contains_key(&col_alias) {
                    let projection_col = SelectItem::ExprWithAlias {
                        expr: Expr::Identifier(Ident::quoted(column)),
                        alias: Ident::quoted(col_alias.clone()),
                    };
                    projection_cols.insert(col_alias, projection_col);
                }
                if !group_by_cols.contains_key(column) {
                    let group_by_col = Expr::Identifier(Ident::quoted(column));
                    group_by_cols.insert(column, group_by_col);
                }
            }

            let (join_selection, exists_joins) = match &order_by_relation.r#where {
                Some(expression) => {
                    let mut exists_index = 0;
                    let (expr, joins) = self.selection_expression(
                        expression,
                        &mut exists_index,
                        true,
                        "_origin",
                        relationship_table,
                    )?;
                    (Some(expr), joins)
                }
                None => (None, vec![]),
            };

            // cols for join and ordering, aggregates
            let join_projection = projection_cols.into_values().collect();
            let join_from = vec![TableWithJoins {
                relation: TableFactor::Table {
                    name: ObjectName(
                        get_target_table(&relationship.target)?
                            .iter()
                            .map(Ident::quoted)
                            .collect(),
                    ),
                    alias: Some(Ident::quoted("_origin")),
                },
                joins: exists_joins,
            }];
            let join_group_by = group_by_cols.into_values().collect();

            let join_subquery = Query::new(join_projection)
                .from(join_from)
                .predicate(join_selection)
                .group_by(join_group_by)
                .boxed();

            let join = Join {
                relation: TableFactor::Derived {
                    subquery: join_subquery,
                    alias: Some(Ident::quoted(&child_alias)),
                },
                join_operator: JoinOperator::LeftOuter(JoinConstraint::On(
                    relationship
                        .column_mapping
                        .iter()
                        .map(|(source_col, target_col)| Expr::BinaryOp {
                            left: Box::new(Expr::CompoundIdentifier(vec![
                                Ident::quoted(parent_alias.clone()),
                                Ident::quoted(if source_path.is_empty() {
                                    source_col.clone()
                                } else {
                                    format!("_col.{source_col}")
                                }),
                            ])),
                            op: BinaryOperator::Eq,
                            right: Box::new(Expr::CompoundIdentifier(vec![
                                Ident::quoted(child_alias.clone()),
                                Ident::quoted(format!("_col.{target_col}")),
                            ])),
                        })
                        .reduce(and_reducer)
                        .unwrap_or(Expr::Value(Value::Boolean(true))),
                )),
            };

            joins.push(join);
            joins.extend(child_joins);
        }
        Ok((parent_join_columns, joins))
    }
    fn selection_expression(
        &mut self,
        expression: &gdc_rust_types::Expression,
        exists_index: &mut usize,
        origin: bool,
        table_alias: &str,
        table: &gdc_rust_types::TableName,
    ) -> Result<(Expr, Vec<Join>), QueryBuilderError> {
        match expression {
            gdc_rust_types::Expression::And { expressions } => {
                let exprs = expressions
                    .iter()
                    .map(|expression| {
                        self.selection_expression(
                            expression,
                            exists_index,
                            origin,
                            table_alias,
                            table,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let and_expr = exprs
                    .into_iter()
                    .reduce(|left, right| {
                        (
                            and_reducer(left.0, right.0),
                            left.1.into_iter().chain(right.1).collect(),
                        )
                    })
                    .map(|(expr, joins)| match expr {
                        Expr::BinaryOp {
                            op: BinaryOperator::And,
                            ..
                        } => (Expr::Nested(Box::new(expr)), joins),
                        _ => (expr, joins),
                    })
                    .unwrap_or_else(|| (Expr::Value(Value::Boolean(true)), vec![]));

                Ok(and_expr)
            }
            gdc_rust_types::Expression::Or { expressions } => {
                let exprs = expressions
                    .iter()
                    .map(|expression| {
                        self.selection_expression(
                            expression,
                            exists_index,
                            origin,
                            table_alias,
                            table,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let or_expr = exprs
                    .into_iter()
                    .reduce(|left, right| {
                        (
                            or_reducer(left.0, right.0),
                            left.1.into_iter().chain(right.1).collect(),
                        )
                    })
                    .map(|(expr, joins)| match expr {
                        Expr::BinaryOp {
                            op: BinaryOperator::Or,
                            ..
                        } => (Expr::Nested(Box::new(expr)), joins),
                        _ => (expr, joins),
                    })
                    .unwrap_or_else(|| (Expr::Value(Value::Boolean(false)), vec![]));

                Ok(or_expr)
            }

            gdc_rust_types::Expression::Not { expression } => {
                let (expr, joins) = self.selection_expression(
                    expression,
                    exists_index,
                    origin,
                    table_alias,
                    table,
                )?;
                let expr = Expr::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(Expr::Nested(Box::new(expr))),
                };
                Ok((expr, joins))
            }
            gdc_rust_types::Expression::ApplyUnaryComparison { column, operator } => {
                let expr = Box::new(self.comparison_column(table_alias, column)?);
                let expr = match operator {
                    gdc_rust_types::UnaryComparisonOperator::IsNull => {
                        Expr::IsNull(Box::new(Expr::Nested(expr)))
                    }
                    gdc_rust_types::UnaryComparisonOperator::Other(operator) => {
                        return Err(QueryBuilderError::UnknownUnaryComparisonOperator(
                            operator.to_owned(),
                        ))
                    }
                };
                Ok((expr, vec![]))
            }
            gdc_rust_types::Expression::ApplyBinaryComparison {
                column,
                operator,
                value,
            } => {
                let left = Box::new(self.comparison_column(table_alias, column)?);

                let right = match value {
                    gdc_rust_types::ComparisonValue::Scalar { value, value_type } => {
                        let value_type =
                            schema::ScalarType::from_str(value_type).map_err(|err| {
                                QueryBuilderError::UnknownScalarType(value_type.to_owned())
                            })?;
                        Box::new(self.bind_parameter(BoundParam::Value {
                            value: value.to_owned(),
                            value_type: value_type.to_owned(),
                        }))
                    }
                    gdc_rust_types::ComparisonValue::Column { column } => {
                        // technically, we could support column comparisons, but only if they don't cross relationships
                        // we can check the origin flag for this, to validate we're not traversing a relationship.
                        let name = match &column.name {
                            gdc_rust_types::ColumnSelector::Compound(name) => name.join("."),
                            gdc_rust_types::ColumnSelector::Name(name) => name.to_owned(),
                        };
                        return Err(QueryBuilderError::RightHandColumnComparisonNotSupported(
                            name,
                        ));
                    }
                };

                let op = match operator {
                    gdc_rust_types::BinaryComparisonOperator::LessThan => BinaryOperator::Lt,
                    gdc_rust_types::BinaryComparisonOperator::LessThanOrEqual => {
                        BinaryOperator::LtEq
                    }
                    gdc_rust_types::BinaryComparisonOperator::Equal => BinaryOperator::Eq,
                    gdc_rust_types::BinaryComparisonOperator::GreaterThan => BinaryOperator::Gt,
                    gdc_rust_types::BinaryComparisonOperator::GreaterThanOrEqual => {
                        BinaryOperator::GtEq
                    }
                    gdc_rust_types::BinaryComparisonOperator::Other(operator) => {
                        return Err(QueryBuilderError::UnknownBinaryComparisonOperator(
                            operator.to_owned(),
                        ))
                    }
                };

                let expr = Expr::BinaryOp { left, right, op };

                Ok((expr, vec![]))
            }
            gdc_rust_types::Expression::ApplyBinaryArrayComparison {
                column,
                operator,
                value_type,
                values,
            } => {
                let expr = Box::new(self.comparison_column(table_alias, column)?);
                let value_type = schema::ScalarType::from_str(value_type)
                    .map_err(|_err| QueryBuilderError::UnknownScalarType(value_type.to_owned()))?;
                let list = values
                    .iter()
                    .map(|value| {
                        self.bind_parameter(BoundParam::Value {
                            value: value.to_owned(),
                            value_type: value_type.to_owned(),
                        })
                    })
                    .collect();

                let expr = match operator {
                    gdc_rust_types::BinaryArrayComparisonOperator::In => {
                        Expr::InList { expr, list }
                    }
                    gdc_rust_types::BinaryArrayComparisonOperator::Other(operator) => {
                        return Err(QueryBuilderError::UnknownBinaryArrayComparisonOperator(
                            operator.to_owned(),
                        ))
                    }
                };
                Ok((expr, vec![]))
            }
            gdc_rust_types::Expression::Exists { in_table, r#where } => {
                // this should be marked as unused if all is fine.
                let selection: bool;
                if origin {
                    let join_alias = format!("_exists_{}", exists_index);
                    *exists_index += 1;

                    // assuming the only columns we care about are join columns.
                    // this may not be true if we support column comparison operators.
                    let (select_expr, join_expr, table_name, projection, group_by, limit) =
                        match in_table {
                            gdc_rust_types::ExistsInTable::Unrelated { table } => {
                                let left = Expr::CompoundIdentifier(vec![
                                    Ident::quoted(join_alias.clone()), // note: this is the alias of the join. Should be dynamic
                                    Ident::quoted("_exists"),
                                ]);
                                let right = Expr::Value(Value::Boolean(true));
                                let select_expr = Expr::BinaryOp {
                                    left: Box::new(left),
                                    op: BinaryOperator::Eq,
                                    right: Box::new(right),
                                };

                                let join_expr = Expr::Value(Value::Boolean(true));

                                let table_name = table;
                                let projection = vec![SelectItem::ExprWithAlias {
                                    expr: Expr::Value(Value::Boolean(true)),
                                    alias: Ident::quoted("_exists"),
                                }];
                                let group_by = vec![];
                                let limit = Some(1);
                                (
                                    select_expr,
                                    join_expr,
                                    table_name,
                                    projection,
                                    group_by,
                                    limit,
                                )
                            }
                            gdc_rust_types::ExistsInTable::Related { relationship } => {
                                let relationship = self.table_relationship(table, relationship)?;
                                let column_mappings: bool;
                                let relationship_table: bool;

                                let select_expr = relationship
                                    .column_mapping
                                    .iter()
                                    .map(|(source_col, target_col)| {
                                        let left = Expr::CompoundIdentifier(vec![
                                            Ident::quoted(join_alias.clone()), // note: this is the alias of the join. Should be dynamic
                                            Ident::quoted(target_col),
                                        ]);
                                        let right = Expr::CompoundIdentifier(vec![
                                            Ident::quoted(table_alias), // should be alias of parent table
                                            Ident::quoted(source_col),
                                        ]);
                                        Expr::BinaryOp {
                                            left: Box::new(left),
                                            op: BinaryOperator::Eq,
                                            right: Box::new(right),
                                        }
                                    })
                                    .reduce(and_reducer)
                                    .map(|expr| match expr {
                                        Expr::BinaryOp {
                                            op: BinaryOperator::And,
                                            ..
                                        } => Expr::Nested(Box::new(expr)),
                                        _ => expr,
                                    })
                                    .unwrap_or(Expr::Value(Value::Boolean(true)));
                                let join_expr = select_expr.clone();

                                let projection = relationship
                                    .column_mapping
                                    .iter()
                                    .map(|(_, target_col)| SelectItem::ExprWithAlias {
                                        expr: Expr::CompoundIdentifier(vec![
                                            Ident::quoted(join_alias.clone()),
                                            Ident::quoted(target_col),
                                        ]),
                                        alias: Ident::quoted(target_col),
                                    })
                                    .collect();
                                let group_by = relationship
                                    .column_mapping
                                    .iter()
                                    .map(|(_, target_col)| {
                                        Expr::CompoundIdentifier(vec![
                                            Ident::quoted(join_alias.clone()),
                                            Ident::quoted(target_col),
                                        ])
                                    })
                                    .collect();
                                let limit = None;

                                let relationship_table = match &relationship.target {
                                    gdc_rust_types::Target::Table { name } => name,
                                    gdc_rust_types::Target::Interpolated { id } => {
                                        return Err(QueryBuilderError::Internal(
                                            "Relationships to Interpolated tables not supported"
                                                .to_string(),
                                        ))
                                    }
                                    gdc_rust_types::Target::Function { name, arguments } => {
                                        return Err(QueryBuilderError::Internal(
                                            "Relationships to Function tables not supported"
                                                .to_string(),
                                        ))
                                    }
                                };

                                (
                                    select_expr,
                                    join_expr,
                                    relationship_table,
                                    projection,
                                    group_by,
                                    limit,
                                )
                            }
                        };

                    let mut subquery_exists_index = 0;

                    let (selection, joins) = self.selection_expression(
                        r#where,
                        &mut subquery_exists_index,
                        false,
                        &join_alias,
                        table_name,
                    )?;

                    let from = vec![TableWithJoins {
                        relation: TableFactor::Table {
                            name: ObjectName(table_name.iter().map(Ident::quoted).collect()),
                            alias: Some(Ident::quoted(join_alias.clone())),
                        },
                        joins,
                    }];

                    let subquery = Query::new(projection)
                        .from(from)
                        .predicate(Some(selection))
                        .group_by(group_by)
                        .limit(limit)
                        .boxed();

                    let join = Join {
                        join_operator: JoinOperator::LeftOuter(JoinConstraint::On(join_expr)),
                        relation: TableFactor::Derived {
                            subquery,
                            alias: Some(Ident::quoted(join_alias)),
                        },
                    };

                    Ok((select_expr, vec![join]))
                } else {
                    let join_alias = format!("{}.{}", table_alias, exists_index);
                    *exists_index += 1;

                    let (select_expr, join_expr, table_name) = match in_table {
                        gdc_rust_types::ExistsInTable::Unrelated { table } => {
                            let left = Expr::CompoundIdentifier(vec![
                                Ident::quoted(join_alias.clone()), // note: this is the alias of the join. Should be dynamic
                                Ident::quoted("_exists"),
                            ]);
                            let right = Expr::Value(Value::Boolean(true));
                            let select_expr = Expr::BinaryOp {
                                left: Box::new(left),
                                op: BinaryOperator::Eq,
                                right: Box::new(right),
                            };

                            let join_expr = Expr::Value(Value::Boolean(true));

                            let table_name = table;
                            (select_expr, join_expr, table_name)
                        }
                        gdc_rust_types::ExistsInTable::Related { relationship } => {
                            let relationship = self.table_relationship(table, relationship)?;

                            let select_expr = relationship
                                .column_mapping
                                .iter()
                                .map(|(source_col, target_col)| {
                                    let left = Expr::CompoundIdentifier(vec![
                                        Ident::quoted(join_alias.clone()), // note: this is the alias of the join. Should be dynamic
                                        Ident::quoted(target_col),
                                    ]);
                                    let right = Expr::CompoundIdentifier(vec![
                                        Ident::quoted(table_alias), // should be alias of parent table
                                        Ident::quoted(source_col),
                                    ]);
                                    Expr::BinaryOp {
                                        left: Box::new(left),
                                        op: BinaryOperator::Eq,
                                        right: Box::new(right),
                                    }
                                })
                                .reduce(and_reducer)
                                .map(|expr| match expr {
                                    Expr::BinaryOp {
                                        op: BinaryOperator::And,
                                        ..
                                    } => Expr::Nested(Box::new(expr)),
                                    _ => expr,
                                })
                                .unwrap_or(Expr::Value(Value::Boolean(true)));
                            let join_expr = select_expr.clone();

                            let relationship_table = get_target_table(&relationship.target)?;

                            (select_expr, join_expr, relationship_table)
                        }
                    };

                    let (selection, joins) = self.selection_expression(
                        r#where,
                        exists_index,
                        false,
                        &join_alias,
                        table_name,
                    )?;

                    let join = Join {
                        join_operator: JoinOperator::LeftOuter(JoinConstraint::On(join_expr)),
                        relation: TableFactor::Table {
                            name: ObjectName(table_name.iter().map(Ident::quoted).collect()),
                            alias: Some(Ident::quoted(join_alias)),
                        },
                    };

                    let joins = vec![join].into_iter().chain(joins).collect();

                    let select_expr = Expr::BinaryOp {
                        left: Box::new(select_expr),
                        op: BinaryOperator::And,
                        right: Box::new(selection),
                    };

                    Ok((select_expr, joins))
                }
            }
        }
    }
    fn comparison_column(
        &mut self,
        table_alias: &str,
        column: &gdc_rust_types::ComparisonColumn,
    ) -> Result<Expr, QueryBuilderError> {
        if let Some(path) = &column.path {
            if !path.is_empty() {
                return Err(QueryBuilderError::UnsupportedColumnComparisonPath(
                    path.to_owned(),
                ));
            }
        }

        let column_name = match &column.name {
            gdc_rust_types::ColumnSelector::Compound(name) => {
                return Err(QueryBuilderError::Internal(
                    "Compoud column selector not supported".to_string(),
                ))
            }
            gdc_rust_types::ColumnSelector::Name(name) => name,
        };

        let expr =
            Expr::CompoundIdentifier(vec![Ident::quoted(table_alias), Ident::quoted(column_name)]);

        Ok(expr)
    }
    fn bind_parameter(&mut self, param: BoundParam) -> Expr {
        if self.bind_params {
            let placeholder_string = format!("__placeholder__{}", self.parameter_index);
            self.parameter_index += 1;
            self.parameters.insert(placeholder_string.clone(), param);
            Expr::Value(Value::Placeholder(placeholder_string))
        } else {
            match param {
                BoundParam::Number(number) => Expr::Value(Value::Number(number.to_string())),
                BoundParam::Value {
                    value,
                    value_type: _,
                } => match value {
                    serde_json::Value::Number(number) => {
                        Expr::Value(Value::Number(number.to_string()))
                    }
                    serde_json::Value::String(string) => {
                        Expr::Value(Value::SingleQuotedString(string))
                    }
                    serde_json::Value::Bool(boolean) => Expr::Value(Value::Boolean(boolean)),
                    // feels like a hack.
                    serde_json::Value::Null => Expr::Value(Value::Null),
                    // note sure this works, should test
                    serde_json::Value::Array(_) => {
                        Expr::Value(Value::SingleQuotedString(value.to_string()))
                    }
                    serde_json::Value::Object(_) => {
                        Expr::Value(Value::SingleQuotedString(value.to_string()))
                    }
                },
            }
        }
    }
    fn limit_by_limit_offset(
        &self,
        partion_rows_by: Vec<Expr>,
        limit: &Option<u64>,
        offset: &Option<u64>,
    ) -> (Option<LimitByExpr>, Option<u64>, Option<u64>) {
        if partion_rows_by.is_empty() {
            (None, limit.to_owned(), offset.to_owned())
        } else {
            let limit_by = match (limit.as_ref(), offset.as_ref()) {
                (None, None) => None,
                (None, Some(offset)) => Some(LimitByExpr {
                    limit: u64::MAX,
                    offset: Some(offset.to_owned()),
                    by: partion_rows_by,
                }),
                (Some(limit), None) => Some(LimitByExpr {
                    limit: limit.to_owned(),
                    offset: None,
                    by: partion_rows_by,
                }),
                (Some(limit), Some(offset)) => Some(LimitByExpr {
                    limit: limit.to_owned(),
                    offset: Some(offset.to_owned()),
                    by: partion_rows_by,
                }),
            };
            (limit_by, None, None)
        }
    }
}
