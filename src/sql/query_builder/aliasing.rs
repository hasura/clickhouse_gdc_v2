use std::{error::Error, mem};

use indexmap::IndexMap;

use crate::{
    server::{
        api::{
            query_request::{
                Aggregate, ComparisonValue, ExistsInTable, Expression, Field, OrderByRelation,
                OrderByTarget, Query, QueryRequest, Relationship, TableName, TableRelationships,
            },
            schema_response::SchemaResponse,
        },
        Config,
    },
    sql::ast::{Ident, ObjectName},
};

use super::QueryBuilderError;

pub fn apply_aliases_to_query_request(
    mut request: QueryRequest,
    config: &Config,
) -> Result<QueryRequest, QueryBuilderError> {
    request.table = aliased_table_name(&request.table, config)?;

    for table_relationships in request.table_relationships.iter_mut() {
        table_relationships.source_table =
            aliased_table_name(&table_relationships.source_table, config)?;

        for relationship in table_relationships.relationships.values_mut() {
            relationship.target_table = aliased_table_name(&relationship.target_table, config)?;

            for (source_col, target_col) in
                relationship.column_mapping.drain(..).collect::<Vec<_>>()
            {
                relationship.column_mapping.insert(
                    aliased_column_name(&table_relationships.source_table, &source_col, config)?,
                    aliased_column_name(&relationship.target_table, &target_col, config)?,
                );
            }
        }
    }

    if let Some(foreach) = request.foreach.as_mut() {
        for row in foreach.iter_mut() {
            for (column, value) in row.drain(..).collect::<Vec<_>>() {
                row.insert(aliased_column_name(&request.table, &column, config)?, value);
            }
        }
    }

    apply_aliases_to_query(
        &request.table,
        &mut request.query,
        &request.table_relationships,
        config,
    )?;

    Ok(request)
}

fn apply_aliases_to_query(
    table: &TableName,
    query: &mut Query,
    table_relationships: &[TableRelationships],
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
                Aggregate::StarCount => {}
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
                    let table =
                        &table_relationship(table, relationship, table_relationships)?.target_table;
                    apply_aliases_to_query(table, query, table_relationships, config)?;
                }
            }
        }
    }

    if let Some(expression) = query.selection.as_mut() {
        apply_aliases_to_expression(table, expression, table_relationships, config)?;
    }

    if let Some(order_by) = query.order_by.as_mut() {
        for element in order_by.elements.iter_mut() {
            let table = element
                .target_path
                .iter()
                .try_fold(table, |table, relationship| {
                    Ok(&table_relationship(table, relationship, table_relationships)?.target_table)
                })?;
            match &mut element.target {
                OrderByTarget::StarCountAggregate => {}
                OrderByTarget::SingleColumnAggregate { column, .. } => {
                    *column = aliased_column_name(table, column, config)?;
                }
                OrderByTarget::Column { column } => {
                    *column = aliased_column_name(table, column, config)?;
                }
            }
        }

        apply_aliases_to_order_by_relations(
            table,
            &mut order_by.relations,
            table_relationships,
            config,
        )?;
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
        let table =
            &table_relationship(table, relationship_name, table_relationships)?.target_table;

        if let Some(expression) = relation.selection.as_mut() {
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
        Expression::UnaryComparisonOperator { column, .. } => {
            // todo: consider column path. note we don't support this anyways so, perhaps don't bother?
            column.name = aliased_column_name(table, &column.name, config)?;
        }
        Expression::BinaryComparisonOperator { column, value, .. } => {
            // todo: consider column path. note we don't support this anyways so, perhaps don't bother?
            column.name = aliased_column_name(table, &column.name, config)?;
            match value {
                ComparisonValue::ScalarValueComparison { .. } => {}
                ComparisonValue::AnotherColumnComparison { column } => {
                    // todo: consider column path. note we don't support this anyways so, perhaps don't bother?
                    column.name = aliased_column_name(table, &column.name, config)?;
                }
            }
        }
        Expression::BinaryArrayComparisonOperator {
            column,
            operator,
            value_type,
            values,
        } => {
            // todo: consider column path. note we don't support this anyways so, perhaps don't bother?
            column.name = aliased_column_name(table, &column.name, config)?;
        }
        Expression::Exists {
            in_table,
            selection,
        } => {
            let table = match in_table {
                ExistsInTable::UnrelatedTable { table } => table,
                ExistsInTable::RelatedTable { relationship } => {
                    &table_relationship(table, relationship, table_relationships)?.target_table
                }
            };

            apply_aliases_to_expression(table, selection, table_relationships, config)?;
        }
    }

    Ok(())
}

fn table_relationship<'a>(
    table: &TableName,
    relationship_name: &str,
    table_relationships: &'a [TableRelationships],
) -> Result<&'a Relationship, QueryBuilderError> {
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

    Ok(relationship)
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
