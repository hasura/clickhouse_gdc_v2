use crate::{
    server::{Config, TableConfig},
    sql::ast::{Ident, ObjectName},
};

use super::QueryBuilderError;

pub struct TableContext<'a, 'b> {
    table_name: String,
    table_schema: String,
    pub table_alias: &'b gdc_rust_types::TableName,
    table_config: Option<&'a TableConfig>,
}

impl<'a, 'b> TableContext<'a, 'b> {
    pub fn try_from_target(
        target: &'b gdc_rust_types::Target,
        config: &'a Config,
    ) -> Result<Self, QueryBuilderError> {
        let table_alias = get_target_table(target)?;

        Self::try_from_name(table_alias, config)
    }
    pub fn try_from_name(
        table_alias: &'b gdc_rust_types::TableName,
        config: &'a Config,
    ) -> Result<Self, QueryBuilderError> {
        let default_schema = &"default".to_string();

        let (table_schema, table_name) = match &table_alias[..] {
            [table_name] => (default_schema, table_name),
            [table_schema, table_name] => (table_schema, table_name),
            _ => {
                return Err(QueryBuilderError::MisshapenTableName(
                    table_alias.to_owned(),
                ))
            }
        };

        let matching_table_name = |table_config: &TableConfig| match &table_config.alias {
            Some(alias) => alias == table_name,
            None => table_config.name == *table_name,
        };
        let matching_schema_name = |table_config: &TableConfig| match table_config.schema.as_ref() {
            Some(schema) => schema == table_schema,
            None => table_schema == default_schema,
        };

        let table_config = config.tables.as_ref().and_then(|tables| {
            tables.iter().find(|table_config| {
                matching_table_name(table_config) && matching_schema_name(table_config)
            })
        });

        let table_name = table_config
            .map(|table_config| table_config.name.to_owned())
            .unwrap_or_else(|| table_name.to_owned());

        let table_schema = table_schema.to_owned();

        Ok(Self {
            table_config,
            table_schema,
            table_alias,
            table_name,
        })
    }
    pub fn table_ident(&self) -> ObjectName {
        ObjectName(vec![
            Ident::quoted(&self.table_schema),
            Ident::quoted(&self.table_name),
        ])
    }
    pub fn column_ident(&self, colum_name: &str) -> Ident {
        self.table_config
            .and_then(|table_config| table_config.columns.as_ref())
            .and_then(|columns| {
                columns.iter().find(|column| {
                    column
                        .alias
                        .as_ref()
                        .is_some_and(|alias| alias == colum_name)
                })
            })
            .map(|column| Ident::quoted(&column.name))
            .unwrap_or_else(|| Ident::quoted(colum_name.to_owned()))
    }
}

fn get_target_table(
    target: &gdc_rust_types::Target,
) -> Result<&gdc_rust_types::TableName, QueryBuilderError> {
    match target {
        gdc_rust_types::Target::Table { name } => Ok(name),
        gdc_rust_types::Target::Interpolated { .. } => Err(QueryBuilderError::Internal(
            "Interpolated targets not supported".to_string(),
        )),
        gdc_rust_types::Target::Function { .. } => Err(QueryBuilderError::Internal(
            "Function targets not yet supported".to_string(),
        )),
    }
}
