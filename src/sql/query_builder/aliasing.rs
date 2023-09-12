use gdc_rust_types::{
    Aggregate, ColumnSelector, ComparisonValue, ExistsInTable, Expression, Field, OrderByRelation,
    OrderByTarget, Query, QueryRequest, TableName, TableRelationships, Target,
};
use indexmap::IndexMap;

use crate::server::Config;

use super::QueryBuilderError;

pub fn apply_aliases_to_query_request(
    mut request: QueryRequest,
    config: &Config,
) -> Result<QueryRequest, QueryBuilderError> {
    let QueryRequest {
        ref mut foreach,
        ref mut query,
        ref mut target,
        ref mut relationships,
        ref interpolated_queries,
    } = request;
    let request_table = match target {
        Target::Table { ref mut name } => name,
        Target::Interpolated { id } => {
            return Err(QueryBuilderError::Internal(
                "Interpolated targets not supported".to_string(),
            ))
        }
        Target::Function { name, arguments } => {
            return Err(QueryBuilderError::Internal(
                "Function targets not supported".to_string(),
            ))
        }
    };
    *request_table = aliased_table_name(request_table, config)?;

    for table_relationships in relationships.iter_mut() {
        table_relationships.source_table =
            aliased_table_name(&table_relationships.source_table, config)?;

        for relationship in table_relationships.relationships.values_mut() {
            let target_table = match relationship.target {
                Target::Table { ref mut name } => name,
                Target::Interpolated { .. } => {
                    return Err(QueryBuilderError::Internal(
                        "Interpolated targets not supported".to_string(),
                    ))
                }
                Target::Function { .. } => {
                    return Err(QueryBuilderError::Internal(
                        "Function targets not supported".to_string(),
                    ))
                }
            };
            *target_table = aliased_table_name(target_table, config)?;

            for (source_col, target_col) in
                relationship.column_mapping.drain(..).collect::<Vec<_>>()
            {
                relationship.column_mapping.insert(
                    aliased_column_name(&table_relationships.source_table, &source_col, config)?,
                    aliased_column_name(target_table, &target_col, config)?,
                );
            }
        }
    }

    if let Some(foreach) = foreach.as_mut() {
        for row in foreach.iter_mut() {
            for (column, value) in row.drain(..).collect::<Vec<_>>() {
                row.insert(aliased_column_name(request_table, &column, config)?, value);
            }
        }
    }

    apply_aliases_to_query(request_table, query, relationships, config)?;

    Ok(request)
}

fn apply_aliases_to_query(
    table: &TableName,
    query: &mut Query,
    relationships: &[TableRelationships],
    config: &Config,
) -> Result<(), QueryBuilderError> {
    if let Some(aggregates) = query.aggregates.as_mut() {
        for aggregate in aggregates.values_mut() {
            match aggregate {
                Aggregate::ColumnCount { column, .. } => {
                    *column = aliased_column_name(table, column, config)?;
                }
                Aggregate::SingleColumn { column, .. } => {
                    *column = aliased_column_name(table, column, config)?;
                }
                Aggregate::StarCount {} => {}
            }
        }
    }

    if let Some(fields) = query.fields.as_mut() {
        for field in fields.values_mut() {
            match field {
                Field::Column { column, .. } => {
                    *column = aliased_column_name(table, column, config)?;
                }
                Field::Relationship {
                    query,
                    relationship,
                } => {
                    let table = &relationship_target_table(table, relationship, relationships)?;
                    apply_aliases_to_query(table, query, relationships, config)?;
                }
                Field::Object { column, query } => todo!(),
                Field::Array {
                    field,
                    limit,
                    offset,
                    r#where,
                } => todo!(),
            }
        }
    }

    if let Some(expression) = query.r#where.as_mut() {
        apply_aliases_to_expression(table, expression, relationships, config)?;
    }

    if let Some(order_by) = query.order_by.as_mut() {
        for element in order_by.elements.iter_mut() {
            let table = element
                .target_path
                .iter()
                .try_fold(table, |table, relationship| {
                    relationship_target_table(table, relationship, relationships)
                })?;
            match &mut element.target {
                OrderByTarget::StarCountAggregate {} => {}
                OrderByTarget::SingleColumnAggregate { column, .. } => {
                    *column = aliased_column_name(table, column, config)?;
                }
                OrderByTarget::Column { column } => match column {
                    ColumnSelector::Compound(path) => {
                        return Err(QueryBuilderError::Internal(
                            "Compound column selectors not supported".to_string(),
                        ))
                    }
                    ColumnSelector::Name(ref mut name) => {
                        *name = aliased_column_name(table, name, config)?;
                    }
                },
            }
        }

        apply_aliases_to_order_by_relations(table, &mut order_by.relations, relationships, config)?;
    }

    Ok(())
}

fn apply_aliases_to_order_by_relations(
    table: &TableName,
    relations: &mut IndexMap<String, OrderByRelation>,
    table_relationships: &[TableRelationships],
    config: &Config,
) -> Result<(), QueryBuilderError> {
    for (relationship_name, relation) in relations.iter_mut() {
        let table = &relationship_target_table(table, relationship_name, table_relationships)?;

        if let Some(expression) = relation.r#where.as_mut() {
            apply_aliases_to_expression(table, expression, table_relationships, config)?;
        }

        apply_aliases_to_order_by_relations(
            table,
            &mut relation.subrelations,
            table_relationships,
            config,
        )?;
    }

    Ok(())
}

fn apply_aliases_to_expression(
    table: &TableName,
    expression: &mut Expression,
    table_relationships: &[TableRelationships],
    config: &Config,
) -> Result<(), QueryBuilderError> {
    match expression {
        Expression::And { expressions } => {
            for expression in expressions.iter_mut() {
                apply_aliases_to_expression(table, expression, table_relationships, config)?;
            }
        }
        Expression::Or { expressions } => {
            for expression in expressions.iter_mut() {
                apply_aliases_to_expression(table, expression, table_relationships, config)?;
            }
        }
        Expression::Not { expression } => {
            apply_aliases_to_expression(table, expression, table_relationships, config)?;
        }
        Expression::ApplyUnaryComparison { column, .. } => {
            // todo: consider column path. note we don't support this anyways so, perhaps don't bother?
            match &mut column.name {
                ColumnSelector::Compound(path) => {
                    return Err(QueryBuilderError::Internal(
                        "Compound column selectors not supported".to_string(),
                    ))
                }
                ColumnSelector::Name(name) => {
                    *name = aliased_column_name(table, name, config)?;
                }
            }
        }
        Expression::ApplyBinaryComparison { column, value, .. } => {
            // todo: consider column path. note we don't support this anyways so, perhaps don't bother?
            match &mut column.name {
                ColumnSelector::Compound(path) => {
                    return Err(QueryBuilderError::Internal(
                        "Compound column selectors not supported".to_string(),
                    ))
                }
                ColumnSelector::Name(ref mut name) => {
                    *name = aliased_column_name(table, name, config)?;
                }
            }
            match value {
                ComparisonValue::Scalar { .. } => {}
                ComparisonValue::Column { column } => {
                    // todo: consider column path. note we don't support this anyways so, perhaps don't bother?
                    match &mut column.name {
                        ColumnSelector::Compound(path) => {
                            return Err(QueryBuilderError::Internal(
                                "Compound column selectors not supported".to_string(),
                            ))
                        }
                        ColumnSelector::Name(ref mut name) => {
                            *name = aliased_column_name(table, name, config)?;
                        }
                    }
                }
            }
        }
        Expression::ApplyBinaryArrayComparison {
            column,
            operator: _,
            value_type: _,
            values: _,
        } => {
            // todo: consider column path. note we don't support this anyways so, perhaps don't bother?
            match &mut column.name {
                ColumnSelector::Compound(path) => {
                    return Err(QueryBuilderError::Internal(
                        "Compound column selectors not supported".to_string(),
                    ))
                }
                ColumnSelector::Name(ref mut name) => {
                    *name = aliased_column_name(table, name, config)?;
                }
            }
        }
        Expression::Exists { in_table, r#where } => {
            let table = match in_table {
                ExistsInTable::Unrelated { table } => table,
                ExistsInTable::Related { relationship } => {
                    relationship_target_table(table, relationship, table_relationships)?
                }
            };

            apply_aliases_to_expression(table, r#where, table_relationships, config)?;
        }
    }

    Ok(())
}

fn relationship_target_table<'a>(
    table: &TableName,
    relationship_name: &str,
    table_relationships: &'a [TableRelationships],
) -> Result<&'a TableName, QueryBuilderError> {
    let source_table = table_relationships
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

    let table_name = match &relationship.target {
        Target::Table { name } => name,
        Target::Interpolated { id: _ } => {
            return Err(QueryBuilderError::Internal(
                "Interpolated targets not supported".to_string(),
            ))
        }
        Target::Function {
            name: _,
            arguments: _,
        } => {
            return Err(QueryBuilderError::Internal(
                "Function targets not supported".to_string(),
            ))
        }
    };

    Ok(table_name)
}

fn aliased_table_name(table: &TableName, config: &Config) -> Result<TableName, QueryBuilderError> {
    let table_alias = match table.first() {
        Some(table_alias) if table.len() == 1 => table_alias,
        _ => return Err(QueryBuilderError::MisshapenTableName(table.to_owned())),
    };
    if let Some(tables) = &config.tables {
        if let Some(table_config) = tables.iter().find(|table_config| {
            table_config
                .alias
                .as_ref()
                .is_some_and(|alias| alias == table_alias)
        }) {
            return Ok(vec![table_config.name.to_owned()]);
        }
    }

    Ok(vec![table_alias.to_owned()])
}

fn aliased_column_name(
    table: &TableName,
    column: &String,
    config: &Config,
) -> Result<String, QueryBuilderError> {
    let table_alias = match table.first() {
        Some(table_alias) if table.len() == 1 => table_alias,
        _ => return Err(QueryBuilderError::MisshapenTableName(table.to_owned())),
    };

    if let Some(tables) = &config.tables {
        if let Some(table_config) = tables.iter().find(|table_config| {
            // Match on either table alias or table name. We don't expect or really support any overlapp between any names or aliases
            // This should make it easier for users who may not provide a table alias when intending to specify aliases for columns
            table_config
                .alias
                .as_ref()
                .is_some_and(|alias| alias == table_alias)
                || &table_config.name == table_alias
        }) {
            if let Some(columns) = &table_config.columns {
                if let Some(column_config) = columns.iter().find(|column_config| {
                    column_config
                        .alias
                        .as_ref()
                        .is_some_and(|alias| alias == column)
                }) {
                    return Ok(column_config.name.to_owned());
                }
            }
        }
    }

    Ok(column.to_owned())
}
